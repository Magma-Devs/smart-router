# syntax=docker/dockerfile:1
#
# Local-dev image: builds the smartrouter binary from source inside Docker.
# Unlike ../Dockerfile.ci / ../Dockerfile.release (which copy a prebuilt
# binary), this one needs no host Go toolchain —
# `docker compose -f docker/docker-compose.yml up` builds and runs router +
# cache from the same image. Not used by CI/release.

FROM golang:1.26 AS build
WORKDIR /src

# Cache module downloads on an unchanged go.mod/go.sum.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .
# Static build so it runs on the distroless runtime. netgo avoids the libc
# resolver; -w -s strip debug info to shrink the binary.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -tags netgo -ldflags '-w -s' \
        -o /out/smartrouter ./cmd/smartrouter

FROM gcr.io/distroless/static-debian12:debug
COPY --from=build /out/smartrouter /bin/smart-router
# Bundled chain specs (--use-static-spec specs/) and example configs travel
# with the image so the router resolves the spec graph offline.
COPY --from=build /src/specs /smart-router/specs
COPY --from=build /src/config /smart-router/config

ENV HOME=/smart-router
WORKDIR $HOME

# router jsonrpc listener / metrics ; cache server / cache metrics
EXPOSE 3360 7779 20100 5555

ENTRYPOINT ["smart-router"]
