# Enterprise Licensing — Operator Guide

This guide is for operators of the **enterprise edition** of the smart router. Community edition users do not need a license file.

## Quick start

You receive a `license.key` file from Magma. Place it in the router's **working directory** (default `./license.key` resolves against the process's current working directory, NOT the binary's directory). The simplest layout — binary directory and working directory are the same:

```
/srv/smart-router/                ← cd here before launching, or set systemd
├── smartrouter-enterprise          WorkingDirectory=/srv/smart-router/
└── license.key       ← this file
```

For systemd, Docker, or any deployment where CWD is not the binary directory, use either `--license-file=PATH` or `$SMART_ROUTER_LICENSE_FILE` to point at the file's absolute path explicitly. Defaults work for one-off CLI invocations; explicit paths are recommended for production.

Start the router. If the license is valid, you'll see:

```
INF Smart Router ENTERPRISE Edition customer=acme expires=2027-04-30 days_until_expiry=365
```

If the license is missing or invalid, the binary refuses to start. **There is no community-mode fallback** — an enterprise binary without a license is a hard error.

## Where the license file is read from

In precedence order (highest first):

| Source | How to set | Use when |
|---|---|---|
| `--license-file=PATH` flag | `smartrouter --license-file=/etc/magma/license.key …` | One-off invocations, debugging |
| `$SMART_ROUTER_LICENSE_FILE` env var | `export SMART_ROUTER_LICENSE_FILE=/etc/magma/license.key` | systemd units, containers, anywhere a flag is awkward |
| `./license.key` (default) | Place file in the router process's working directory (NOT necessarily the binary's directory; align them via `cd` before launch or systemd `WorkingDirectory=`) | Simplest single-tenant deployments |

The chosen source is logged at startup so you can confirm which one fired:

```
INF Loading enterprise license source=$SMART_ROUTER_LICENSE_FILE=/etc/magma/license.key
INF Loading enterprise license source=--license-file=/tmp/test.key
INF Loading enterprise license source=default ./license.key
```

**Important:** if `$SMART_ROUTER_LICENSE_FILE` is set but points at a missing file, the binary errors — it does **not** fall back to the default. This is deliberate: silent fallback would hide the misconfiguration you set the env var to fix.

## Grace period (14 days)

Past the license's `expires_at`, the router enters a 14-day grace period. During grace:

- The router **continues to start and serve requests** (existing operations are not interrupted).
- Every startup logs an ERROR-level warning:
  ```
  ERR LICENSE IN GRACE PERIOD — expired 2026-04-30, stops accepting new starts on 2026-05-14
  ```
- A background watcher re-logs the warning hourly so the message stays visible.

After the grace window ends, the router **refuses to start** with:

```
FTL license expired on 2026-04-30 (grace period ended 2026-05-14) — re-issue license, replace the file, and restart
```

Plan license rotation to land **before** the original `expires_at` to avoid the grace-period log noise.

## Rotating to a new license

1. Receive the new `license.key` from Magma.
2. Replace the old file in place (`mv new-license.key /etc/magma/license.key`).
3. Restart the router. The new `expires_at` and `days_until_expiry` will appear in the startup banner.

No live reload — the license is read once at startup. A restart is required.

## Failure modes

Each failure produces a fatal error with `source=` showing which path was attempted, so you know which file to fix.

| Log message | Cause | Fix |
|---|---|---|
| `license file unreadable — enterprise binary cannot start without a valid license` | The file at `source=` doesn't exist, isn't readable, or is empty. | Check the path exists and is readable by the router process; replace with the file Magma sent you. |
| `license validation failed` | Envelope is malformed — wrong format, truncated, or signature doesn't verify against any known public key. | The file may have been corrupted in transit. Re-download from Magma. |
| `license expired on YYYY-MM-DD (grace period ended YYYY-MM-DD) — re-issue license, replace the file, and restart` | License is past expiry **and** past the 14-day grace window. | Contact Magma for a new license; replace `license.key` with the new file (see "Rotating to a new license") and restart. |
| `license invalid` | Defensive arm — license validated cleanly but with an unrecognized status code. Should not occur in normal operation. | File a bug report with the full log line. |

For warnings (not failures) like `license approaching expiry days_until_expiry=14`, contact Magma to issue a renewal — the router stays operational until expiry.

## Frequently confused

- **Setting `$SMART_ROUTER_LICENSE_FILE` to an empty string is the same as unsetting it.** Both fall back to `./license.key`.
- **The `--license-file` flag is on the router command, not subcommands.** `smartrouter cache`, `smartrouter version`, and `smartrouter test` ignore it (and don't validate licenses at all).
- **The license file is plain text** (a base64-encoded envelope). It's safe to `cat` for debugging; it does not contain a signing private key.
