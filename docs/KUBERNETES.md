# Kubernetes

Smart Router runs on Kubernetes, and Magma Devs supports Kubernetes deployments. To run it on
your cluster, or for help with one you already run, [contact the team](https://magmadevs.com/contact).

What a cluster needs from the router:

- **Probes.** The router serves `/livez` and `/readyz` on its metrics server, which is off until
  `--metrics-listen-address` is set (for example `:7779`). `/livez` answers `200` while the
  process serves HTTP, so it suits a liveness probe. `/readyz` answers `503` while no chain is
  healthy, so it suits a readiness probe: the pod leaves the Service without restarting.
  [METRICS.md](METRICS.md) ("When they change") says what makes a chain healthy.
- **The health check stays on.** `--relays-health-enable` is on by default. With it off, `/readyz`
  answers `503` unconditionally.
- **The image.** `ghcr.io/magma-devs/smart-router:vX.Y.Z`, for `linux/amd64` and `linux/arm64`,
  carries the same binary as the release (see [Releases](../README.md#releases)).
