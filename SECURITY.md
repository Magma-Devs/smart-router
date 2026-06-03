# Security Policy

## Reporting a vulnerability

If you discover a security vulnerability in smart-router, please **do not** open a public GitHub issue. Email <security@magmadevs.com> instead with:

- A description of the issue and its potential impact.
- Steps to reproduce.
- Affected version(s) — the output of `smartrouter version`.
- Any suggested mitigation.

We aim to acknowledge reports within 2 business days. Confirmed vulnerabilities are resolved within 90 days, coordinated with affected customers, and disclosed publicly via a GitHub Security Advisory after patches ship and a reasonable update window (typically 7–14 days).

## Supported versions

Only the latest released minor version line receives security patches. Subscribe to the [Releases page](https://github.com/Magma-Devs/smart-router/releases) for update notifications.

## Scope

In scope:

- The `smartrouter` binary and the published Docker image at `ghcr.io/magma-devs/smart-router`.
- Wire protocols smart-router exposes: JSON-RPC, REST, gRPC, Tendermint RPC, plus the `Smart-Router-*` and `Lava-*` HTTP metadata headers.
- The release pipeline configuration (`.goreleaser.yaml`, `docker/Dockerfile.release`, `.github/workflows/release.yml`).

Out of scope:

- Vulnerabilities in upstream RPC providers — smart-router is a relay, not the origin of trust.
- Configuration mistakes that expose the router to the public internet without authentication; smart-router is intended to run behind your edge.
