# smart-router config wizard

A Charm-based TUI that builds a smartrouter config and runs the local docker
compose stack — from "which chains?" to a running, health-verified router.

```bash
make wizard          # from the repo root (builds the router, then launches)
# or
cd tools/wizard && go run . --repo /path/to/smart-router
```

## Prerequisites

On launch the wizard runs an **OS-adaptive tool check** (step 0) before the
flow, so a missing dependency surfaces up front instead of when the run step
fails. Check it standalone anytime:

```bash
make wizard-preflight     # or: cd tools/wizard && go run . --preflight
go run . --skip-preflight # advanced: bypass the gate
```

| Tool | Tier | Used for | Install |
| --- | --- | --- | --- |
| `bash` | required | the render + up steps run via `bash -c`; `run.sh` is a bash script | macOS/Linux built-in |
| `envsubst` | required | expands `${VAR}` secrets from `.env` into the rendered config | GNU **gettext** — `brew install gettext` (mac) · `apt install gettext-base` (Debian) · `dnf install gettext` (Fedora) |
| `docker` + `docker compose` (v2 plugin) | required | builds and runs the stack (`docker compose … up`, **not** legacy `docker-compose`) | Docker Desktop (mac/Win) · Docker Engine + `docker-compose-plugin` (Linux) |
| `go` | optional | builds the `smartrouter` binary for health checks (skipped if `build/` has one; `make wizard` builds it first) | https://go.dev/dl/ |
| `gh` | optional | fetches chain specs via the GitHub API — falls back to a plain HTTPS download when absent | https://cli.github.com/ |

**Windows:** the run step needs `bash` + `envsubst`, which native cmd/PowerShell
don't provide. Preflight detects native Windows without a POSIX shell and steers
you to **WSL2** or **Git Bash** (Docker Desktop's WSL2 backend still powers
`docker compose`). A hard stop, not a warning — the bash-based run step can't
work otherwise.

## Flow

1. **Chains** — a family-tabbed, fuzzy-searchable multi-select over the live
   [`lava-specs`](https://github.com/magma-Devs/lava-specs) catalog. Families
   (**EVM · Cosmos · BTC · Other-L1 · Specialty**) and per-chain **icons** are
   **extrapolated from the docs** at runtime (`chains-data.js` + the published
   SVGs) — nothing is hand-maintained or vendored here. `tab` switches family,
   `/` filters, `space` toggles, `enter` confirms.
2. **Interfaces** — per chain (when it exposes more than one), pick which
   interface(s) to expose; ports are auto-assigned from 3360.
3. **Endpoints** — per listener, add upstream URL(s). Each is validated by the
   spec-driven **`smartrouter health`** command (latest-block + addon/extension
   + websocket checks), shown with a spinner. jsonrpc upstreams collect a paired
   `wss://` url (ETH1-derived specs require it). Addons can be **auto-detected**
   (health) or chosen explicitly. Optional per-endpoint **auth** (`${VAR}` in a
   gitignored `.env`).
4. **Backups · Cache · Dashboard** — optional `backup-direct-rpc` tier; the
   cache sidecar; the Prometheus + dashboard overlay. When the dashboard is
   enabled, the run output prints its access info: **UI <http://localhost:3000>,
   login `admin` / `password`**, API on `:8000`, Prometheus on `:9090`.
5. **Save** — writes `config/local/<name>.template.yml` + `.env` + the rendered
   `<name>.yml` (gitignored), lints it, then runs a full `health <config>` pass
   over everything (http + ws + addons) so a green wizard = a config that runs.
6. **Run** — prints the exact render + `up` + teardown commands, optionally runs
   `docker compose up --build`, then smoke-tests each listener via `health`. The
   `up` command layers a generated `<name>.compose.override.yml` (beside the
   config) that publishes exactly the ports this config's listeners bind — so the
   base compose keeps its example ports (3360-3362) and the wizard's ports are
   simply added on top. It also sets `SR_SPEC` when the catalog came from remote
   (see below) so the container resolves chains absent from the bundled `specs/`.

## `SR_SPEC` — which specs the router loads

The base compose passes `--use-static-spec ${SR_SPEC:-specs/}`. Two valid values:

| `SR_SPEC` | Meaning |
| --- | --- |
| `specs/` *(default)* | The lava-specs snapshot **bundled into the image**. Covers the chains in the repo's `specs/` dir (the bundled examples). |
| `https://github.com/magma-Devs/lava-specs/tree/main` | The **live lava-specs GitHub repo**. Resolves every chain in the catalog, including ones not bundled (e.g. `LAV1`). The router fetches + caches these at startup. |

The wizard sets it automatically: `specs/` when its chain catalog came from the
local source, the GitHub URL when it came from remote. You can override it by
hand on any `docker compose … up` (a local dir or a GitHub/GitLab repo URL — the
router accepts either, same as the `health` command).

## Icons

Chain icons come from the docs site
(`http://docs.magmadevs.com/assets/chains/<slug>.svg`) — fetched by URL,
rasterized in pure Go, cached under `$TMPDIR`, and rendered inline via the
**Kitty** graphics protocol (Kitty, Ghostty, WezTerm) or **iTerm2** inline
images. Other terminals fall back to a family glyph (◆ ⚛ ₿ ⬡ ✦). Force a mode
with `WIZARD_ICONS=glyph|kitty|iterm`.

## Layout

```
tools/wizard/                  (separate Go module — router go.mod untouched)
  main.go                      flow orchestration
  internal/
    catalog/   lava-specs fetch + transitive import-merge
    docs/      chains-data.js (eco + icon slug) — shared source of truth
    classify/  family from docs eco (+ structural fallback for base specs)
    icons/     docs SVG → raster → kitty/iterm/glyph
    health/    shells out to `smartrouter health`, parses the JSON envelope
    emit/      YAML render (+ auth ${VAR}, ws pairing, cache-be) + lint
    ui/        lipgloss theme, gradient ASCII logo, chain-picker, huh theme
    flow/      screens + state: interfaces, endpoints, save, run, smoke
```

## Build / test

```bash
make wizard-build    # build/sr-wizard, don't launch
make wizard-test     # go test ./... for the non-TUI packages
```

The wizard needs a `health`-capable `smartrouter` binary (built by `make wizard`
via the `build` dependency, or `$SMARTROUTER_BIN` / `build/smartrouter` / one on
`PATH`).

## Known limits

- Catalog, taxonomy, and icons are fetched from the network (lava-specs + docs);
  an offline run falls back to the bundled `specs/` and family glyphs.
- A chain not in the bundled `specs/` resolves only when `SR_SPEC` points at the
  remote lava-specs repo (the wizard sets this automatically for a remote
  catalog — see the `SR_SPEC` table above). A fully offline run is limited to the
  chains in the bundled `specs/`.
- `cross-validation:` is left as a documented TODO in the emitted config header.
```
