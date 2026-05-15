#!/usr/bin/env python3
"""
render_compose.py — generate the local docker-compose stack from config/values.yml.

Reads:
  - config/values.yml — routers, cache config

Writes:
  - compose/docker-compose.yml
  - compose/configs/router-{id}.yml (one per router; stale ones get cleaned up)
  - compose/traefik/dynamic.yml (file-provider routes for each router/interface)

Routers + cache always use `build: { context: .., dockerfile: Dockerfile }`
tagged `smart-router:local`. The Dockerfile copies build/smartrouter from the
host (BINARY_PATH build arg) — caller is responsible for running `make build`
first.
"""

import sys
import re
from pathlib import Path

try:
    import yaml
except ImportError:
    sys.exit("❌ PyYAML required. Install with: pip install pyyaml")

REPO = Path(__file__).resolve().parents[1]
VALUES_FILE = REPO / "config/values.yml"
COMPOSE_DIR = REPO / "compose"
CONFIGS_DIR = COMPOSE_DIR / "configs"
TRAEFIK_DIR = COMPOSE_DIR / "traefik"


def load_values():
    if not VALUES_FILE.exists():
        sys.exit(f"❌ Values file not found: {VALUES_FILE}")
    with open(VALUES_FILE) as f:
        return yaml.safe_load(f) or {}


def rewrite_k8s_dns(s):
    """Strip in-cluster service DNS so URLs resolve via Docker DNS (bare service
    names). Inherited from the helm-driven repos — for `smart-router`'s own
    config/values.yml URLs typically reach the public internet so this is a no-op,
    but kept for parity in case someone reuses an internal URL."""
    if not isinstance(s, str):
        return s
    return re.sub(r"\.([\w-]+)\.svc\.cluster\.local", "", s)


def collect_interfaces(router):
    seen = []
    for node in router.get("nodes", []) or []:
        for ep in node.get("endpoints", []) or []:
            iface = ep.get("interface")
            if iface and iface not in seen:
                seen.append(iface)
    return seen


def expand_addons(ep):
    """Archive variant + others, then others alone (matches helm chart logic)."""
    has_archive = False
    others = []
    for addon in ep.get("addons", []) or []:
        if addon.lower() == "archive":
            has_archive = True
        else:
            others.append(addon)
    if has_archive:
        return [["archive"] + others, others]
    return [others]


def render_node_urls(node):
    out = []
    for ep in node.get("endpoints", []) or []:
        for addon_set in expand_addons(ep):
            entry = {"url": rewrite_k8s_dns(ep["url"])}
            if ep.get("internal_path"):
                entry["internal-path"] = ep["internal_path"]
            ac = ep.get("auth_config") or ep.get("auth-config")
            if ac:
                out_ac = {}
                for src, dst in [
                    ("auth_headers", "auth-headers"),
                    ("auth-headers", "auth-headers"),
                    ("auth_query", "auth-query"),
                    ("auth-query", "auth-query"),
                    ("use_tls", "use-tls"),
                    ("use-tls", "use-tls"),
                    ("key_pem", "key-pem"),
                    ("cert_pem", "cert-pem"),
                    ("ca_cert", "cacert-pem"),
                    ("allow_insecure", "allow-insecure"),
                ]:
                    if src in ac:
                        out_ac[dst] = ac[src]
                if out_ac:
                    entry["auth-config"] = out_ac
            timeout = ep.get("timeout") or node.get("timeout")
            if timeout:
                entry["timeout"] = timeout
            ipf = ep.get("ip_forwarding") if "ip_forwarding" in ep else node.get("ip_forwarding")
            if ipf:
                entry["ip-forwarding"] = ipf
            sv = node.get("skip_verifications")
            if sv:
                entry["skip-verifications"] = [s.strip() for s in str(sv).split(",")]
            if addon_set:
                entry["addons"] = [a.lower() for a in addon_set]
            out.append(entry)
    return out


def render_router_config(router):
    """Per-router smart-router config.yml (mirrors helm routers_configmap.yaml)."""
    interfaces = collect_interfaces(router)
    network_upper = router["network"].upper()

    endpoints = [
        {"chain-id": network_upper, "api-interface": iface, "network-address": f"0.0.0.0:300{i}"}
        for i, iface in enumerate(interfaces)
    ]

    def collect_rpcs(want_backup):
        out = []
        for node in router.get("nodes", []) or []:
            is_backup = bool(node.get("is_backup"))
            if is_backup != want_backup:
                continue
            entry = {
                "chain-id": network_upper,
                "api-interface": node["endpoints"][0]["interface"],
                "name": node["name"].lower().replace(" ", "-"),
            }
            if node.get("stake"):
                entry["stake"] = node["stake"]
            entry["node-urls"] = render_node_urls(node)
            out.append(entry)
        return out

    return {
        "endpoints": endpoints,
        "direct-rpc": collect_rpcs(False),
        "backup-direct-rpc": collect_rpcs(True),
        "metrics-listen-address": "0.0.0.0:7779",
    }, interfaces


def build_block():
    """Always-build block: unified root Dockerfile, fed the host-built binary."""
    return {
        "build": {
            "context": "..",
            "dockerfile": "Dockerfile",
            "args": {"BINARY_PATH": "build/smartrouter"},
        },
        "image": "smart-router:local",
    }


def build_router_service(router, router_idx, values, interfaces):
    rid = router["id"].lower()
    name = f"{rid}-router"
    misc = values["miscellaneous"]
    rcfg = misc["routers"]

    args = ["config.yml",
            "--geolocation", str(misc.get("geolocation", "1")),
            "--log-level", misc["log"]["level"],
            "--log-format", misc["log"]["format"]]
    if rcfg.get("cache", {}).get("enabled", True):
        args += ["--cache-be", "router-cache:20100"]
    args += [
        "--skip-websocket-verification=true",
        "--metrics-listen-address=:7779",
        "--optimizer-qos-listen",
        "--concurrent-providers=20",
        "--periodic-probe-providers-interval=120s",
        "--epoch-duration=15m",
        "--response-compression=gzip",
        "--enable-periodic-probe-providers=true",
        f"--max-sessions-per-provider={rcfg.get('maxSessionsPerProvider', 10000)}",
        f"--maximum-streams-per-connection={rcfg.get('maximumStreamsPerConnection', 1000)}",
        f"--set-relay-retry-limit={rcfg.get('setRelayRetryLimit', 2)}",
        f"--default-processing-timeout={rcfg.get('defaultProcessingTimeout', '30s')}",
        f"--min-relay-timeout={router.get('minRelayTimeout', rcfg.get('minRelayTimeout', '10s'))}",
    ]
    if int(rcfg.get("maxBatchRequestSize") or 0) > 0:
        args.append(f"--max-batch-request-size={rcfg['maxBatchRequestSize']}")
    args += [
        "--show-provider-address-in-metrics=true",
        f"--provider-optimizer-availability-weight={rcfg.get('providerOptimizerAvailabilityWeight', 0.3)}",
        f"--provider-optimizer-latency-weight={rcfg.get('providerOptimizerLatencyWeight', 0.3)}",
        f"--provider-optimizer-sync-weight={rcfg.get('providerOptimizerSyncWeight', 0.2)}",
        f"--provider-optimizer-stake-weight={rcfg.get('providerOptimizerStakeWeight', 0.2)}",
        f"--provider-optimizer-min-selection-chance={rcfg.get('providerOptimizerMinSelectionChance', 0.01)}",
        f"--probe-update-weight={rcfg.get('probeUpdateWeight', 0.25)}",
    ]
    if rcfg.get("enableSelectionStats"):
        args.append("--enable-selection-stats")
    if rcfg.get("enableGrpcCompression"):
        args.append("--enable-grpc-compression")
    # Static specs: the binary refuses to start without at least one source.
    # By default we pass the repo's bundled specs/ (bind-mounted at
    # /smart-router/specs); an optional remoteUrl lets routers load chains
    # that aren't checked in locally (e.g. COSMOSHUB).
    specs = misc.get("staticSpecs", {}) or {}
    local_path = specs.get("localPath", "/smart-router/specs")
    args += ["--use-static-spec", local_path]
    if specs.get("remoteUrl"):
        args += ["--use-static-spec", specs["remoteUrl"]]
    if specs.get("token"):
        args += ["--github-token", specs["token"]]
    for extra in rcfg.get("additionalFlags", []) or []:
        args.append(rewrite_k8s_dns(str(extra)))

    env = {"POD_NAME": name}

    # First interface exposed on host as 3000+idx; secondaries reachable via Traefik.
    host_port = 3000 + router_idx
    ports = [f"{host_port}:3000"]

    svc = build_block()
    svc.update({
        "container_name": name,
        "command": args,
        "environment": env,
        "ports": ports,
        "volumes": [
            f"./configs/router-{rid}.yml:/smart-router/config/config.yml:ro",
            "../specs:/smart-router/specs:ro",
        ],
        "depends_on": (["router-cache"] if rcfg.get("cache", {}).get("enabled", True) else []),
        "healthcheck": {
            "test": ["CMD", "wget", "-q", "-O", "-", "http://localhost:7779/metrics"],
            "interval": "10s",
            "timeout": "5s",
            "retries": 3,
            "start_period": "15s",
        },
        "networks": ["smartrouter"],
        "restart": "unless-stopped",
    })
    return name, svc


def build_cache_service(values):
    misc = values["miscellaneous"]
    c = misc["cache"]
    args = [
        "cache", "0.0.0.0:20100",
        "--expiration-multiplier", str(c.get("expiration_multiplier", 1.5)),
        "--expiration-non-finalized-multiplier", str(c.get("expiration_non_finalized_multiplier", 1.25)),
        "--max-items", str(c.get("max_items", 2147483647)),
        "--metrics_address", "0.0.0.0:7781",
        "--log_level", str(c.get("log_level", "error")),
    ]
    svc = build_block()
    svc.update({
        "container_name": "router-cache",
        "command": args,
        "healthcheck": {
            "test": ["CMD", "wget", "-q", "-O", "-", "http://localhost:7781/metrics"],
            "interval": "10s", "timeout": "5s", "retries": 3, "start_period": "15s",
        },
        "networks": ["smartrouter"],
        "restart": "unless-stopped",
    })
    return "router-cache", svc


def build_traefik_service():
    return "traefik", {
        "image": "traefik:v3.5",
        "container_name": "traefik",
        "command": [
            "--api.dashboard=true",
            "--api.insecure=true",
            "--providers.file.filename=/etc/traefik/dynamic.yml",
            "--providers.file.watch=true",
            "--entrypoints.web.address=:80",
        ],
        "ports": ["80:80", "8090:8080"],
        "volumes": ["./traefik/dynamic.yml:/etc/traefik/dynamic.yml:ro"],
        "networks": ["smartrouter"],
        "restart": "unless-stopped",
    }


def render_traefik_dynamic(routers_with_interfaces):
    """File-provider routes — one per (router, interface) pair, HTTP on :80."""
    cors_middleware = {
        "headers": {
            "accessControlAllowOriginList": ["*"],
            "accessControlAllowMethods": ["GET", "POST", "OPTIONS"],
            "accessControlAllowHeaders": ["*"],
            "accessControlMaxAge": 86400,
        },
    }

    http_routers = {}
    http_services = {}
    for router, interfaces in routers_with_interfaces:
        rid = router["id"].lower()
        for i, iface in enumerate(interfaces):
            rname = f"{rid}-{iface}"
            host = f"{rid}-{iface}.localhost"
            backend = f"{rid}-router:300{i}"
            http_services[rname] = {
                "loadBalancer": {"servers": [{"url": f"http://{backend}"}]},
            }
            http_routers[rname] = {
                "rule": f"Host(`{host}`)",
                "entryPoints": ["web"],
                "service": rname,
                "middlewares": ["router-cors"],
            }

    return {
        "http": {
            "routers": http_routers,
            "services": http_services,
            "middlewares": {"router-cors": cors_middleware},
        },
    }


def has_websocket_upstream(router):
    """True if any of this router's node URLs starts with ws:// or wss://."""
    for node in router.get("nodes", []) or []:
        for ep in node.get("endpoints", []) or []:
            url = (ep.get("url") or "").lower()
            if url.startswith("ws://") or url.startswith("wss://"):
                return True
    return False


# Per-interface example payloads — picked to be realistic and small so users
# can copy-paste them and see a real response. Methods that don't exist on the
# specific chain still validate the wiring end-to-end (error response confirms
# the request reached the smart-router and routed correctly).
_INTERFACE_EXAMPLES = {
    "jsonrpc": ("eth_blockNumber", "[]"),
    "tendermintrpc": ("status", "[]"),
}


def _example_payload(iface):
    method, params = _INTERFACE_EXAMPLES.get(iface, ("eth_blockNumber", "[]"))
    return '{"jsonrpc":"2.0","method":"' + method + '","params":' + params + ',"id":1}'


# REST endpoints are chain-specific; pick a real path that returns a useful
# response per network. Falls back to a placeholder for chains we don't know.
_REST_PATH_EXAMPLES = {
    "lava": "/lavanet/lava/spec/show_all_chains",
    "cosmoshub": "/cosmos/staking/v1beta1/validators",
}


def _example_rest_path(network):
    return _REST_PATH_EXAMPLES.get((network or "").lower(), "/<chain-specific-path>")


def render_usage(routers_with_interfaces):
    """Produce a human-readable cheat sheet of curl/wscat/grpcurl invocations,
    one block per (chain, interface). HTTP + WS go through Traefik on :80 with
    a Host header; gRPC bypasses Traefik (h2c routing is fragile) and hits the
    router's direct host port."""
    lines = ["Usage examples (HTTP/WS via Traefik on :80; gRPC via direct host port):", ""]
    for idx, (router, interfaces) in enumerate(routers_with_interfaces):
        rid = router["id"].lower()
        has_ws = has_websocket_upstream(router)
        host_port = 3000 + idx
        for iface in interfaces:
            host = f"{rid}-{iface}.localhost"
            lines.append(f"  {rid}-{iface}  (direct: localhost:{host_port})")
            if iface == "grpc":
                lines.append(
                    f"    grpcurl -plaintext localhost:{host_port} \\\n"
                    f"            cosmos.base.tendermint.v1beta1.Service.GetLatestBlock"
                )
            elif iface == "rest":
                lines.append(f"    curl http://{host}{_example_rest_path(router.get('network'))}")
            else:
                # jsonrpc + tendermintrpc both speak JSON-RPC over HTTP at the
                # root path; only the method name differs.
                lines.append(
                    f"    curl -X POST -H 'Content-Type: application/json' http://{host} \\\n"
                    f"         --data '{_example_payload(iface)}'"
                )
            if has_ws and iface == "jsonrpc":
                lines.append(
                    f"    wscat -c ws://127.0.0.1/ws -H \"Host: {host}\" \\\n"
                    f"          -x '{_example_payload(iface)}'"
                )
            lines.append("")
    return "\n".join(lines) + "\n"


def write_yaml(path, doc, header=None):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        if header:
            f.write(header)
        yaml.dump(doc, f, default_flow_style=False, sort_keys=False, width=200)


def main():
    values = load_values()

    services = {}
    router_names = []
    rendered_config_files = set()
    routers_with_interfaces = []

    for idx, router in enumerate(values.get("routers", []) or []):
        cfg, interfaces = render_router_config(router)
        rid = router["id"].lower()
        cfg_path = CONFIGS_DIR / f"router-{rid}.yml"
        write_yaml(cfg_path, cfg,
                   header="# GENERATED by scripts/render_compose.py — do not edit by hand.\n\n")
        rendered_config_files.add(cfg_path.name)
        name, svc = build_router_service(router, idx, values, interfaces)
        services[name] = svc
        router_names.append(name)
        routers_with_interfaces.append((router, interfaces))

    # Clean up stale router-*.yml from earlier renders (e.g. when a router is
    # removed from config/values.yml).
    if CONFIGS_DIR.exists():
        for p in CONFIGS_DIR.glob("router-*.yml"):
            if p.name not in rendered_config_files:
                p.unlink()

    if values.get("miscellaneous", {}).get("routers", {}).get("cache", {}).get("enabled", True):
        name, svc = build_cache_service(values)
        services[name] = svc

    name, svc = build_traefik_service()
    services[name] = svc

    # Project name kept distinct from `smart-router-standalone`'s compose project
    # (`smart-router`) so both stacks can coexist on the same docker host without
    # clobbering each other's containers/network.
    compose = {
        "name": "smart-router-local",
        "services": services,
        "networks": {"smartrouter": {"driver": "bridge"}},
    }

    header = ("# GENERATED by scripts/render_compose.py — do not edit by hand.\n"
              "# Source: config/values.yml\n"
              "# Re-render with: ./scripts/compose_up.sh --render-only\n\n")
    write_yaml(COMPOSE_DIR / "docker-compose.yml", compose, header=header)
    write_yaml(TRAEFIK_DIR / "dynamic.yml", render_traefik_dynamic(routers_with_interfaces),
               header="# GENERATED — Traefik file-provider routes (HTTP :80).\n\n")

    # compose_up.sh cats this at the end so users can copy-paste a working call.
    (COMPOSE_DIR / "usage.txt").write_text(render_usage(routers_with_interfaces))

    print(f"✓ Rendered compose stack ({len(router_names)} routers)")


if __name__ == "__main__":
    main()
