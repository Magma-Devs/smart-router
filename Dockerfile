# syntax=docker/dockerfile:1
#
# Thin runner image: copies a prebuilt smart-router binary into a minimal
# distroless base. Used by:
#   - CI (.github/workflows/smartrouter.yml + release.yml): per-arch binaries
#     are cross-compiled on the runner and downloaded to
#     artifacts/linux/<arch>/smartrouter; buildx assembles a multi-arch image.
#   - Local compose stack: `make build` (or scripts/compose_up.sh) produces
#     build/smartrouter; compose passes BINARY_PATH=build/smartrouter.

ARG RUNNER_IMAGE=gcr.io/distroless/static-debian12:debug

FROM ${RUNNER_IMAGE}

ARG TARGETARCH
ARG BINARY_PATH=artifacts/linux/${TARGETARCH}/smartrouter
COPY ${BINARY_PATH} /bin/smart-router

ENV HOME=/smart-router
WORKDIR ${HOME}

# smart router listener
EXPOSE 3360
# metrics
EXPOSE 7779

ENTRYPOINT ["smart-router"]
