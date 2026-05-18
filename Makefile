#!/usr/bin/make -f

BINDIR ?= $(GOPATH)/bin

# Resolve version from git the same way CI does (smartrouter.yml's
# `Resolve version from git` step). Falls back to "dev" outside a git
# checkout.
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT  ?= $(shell git rev-parse HEAD 2>/dev/null || echo none)

# Flags match smartrouter.yml and .goreleaser.yaml so a locally-built
# binary is byte-identical to CI's for the same commit (modulo
# build-host differences in Go toolchain version).
LDFLAGS := -w -s \
	-X github.com/magma-Devs/smart-router/version.Version=$(VERSION) \
	-X github.com/magma-Devs/smart-router/version.Commit=$(COMMIT)

GOFLAGS := -mod=readonly -trimpath -ldflags '$(LDFLAGS)'

export CGO_ENABLED ?= 0
export GOAMD64     ?= v3
export GOARM64     ?= v8.2

# Install the router binary to $GOPATH/bin
install-all: install

install:
	go install $(GOFLAGS) ./cmd/smartrouter

install-smartrouter: install

# Build the router binary into build/
build-all: build

build:
	go build $(GOFLAGS) -o build/smartrouter ./cmd/smartrouter

BUILDX_BUILDER ?= smartrouter-builder
export BUILDX_BUILDER

# One-time-per-machine setup. Currently ensures a docker-container-
# driver buildx builder exists (needed for the multi-arch image build).
# The default `docker` driver doesn't support --platform / multi-arch,
# so without this snapshot fails with `unknown flag: --platform`.
# First run pulls moby/buildkit (~150MB) — about 30s; later runs no-op.
# Scoped to this invocation via BUILDX_BUILDER env so the user's global
# default builder is unchanged.
setup:
	@if ! docker buildx version >/dev/null 2>&1; then \
	  echo "ERROR: docker buildx is not installed."; \
	  echo "  Official install guide: https://docs.docker.com/build/buildx/install/"; \
	  echo ""; \
	  echo "  Common paths:"; \
	  echo "    - Docker Desktop (macOS / Windows / WSL2): buildx is included."; \
	  echo "    - docker-ce on Linux (Docker's official apt repo):"; \
	  echo "        sudo apt install docker-buildx-plugin"; \
	  echo "    - docker.io from Ubuntu repos: does NOT include buildx — install as a"; \
	  echo "      CLI plugin manually:"; \
	  echo "        mkdir -p ~/.docker/cli-plugins"; \
	  echo "        curl -L https://github.com/docker/buildx/releases/latest/download/buildx-\$$(curl -s https://api.github.com/repos/docker/buildx/releases/latest | grep tag_name | cut -d '\"' -f 4).linux-amd64 -o ~/.docker/cli-plugins/docker-buildx"; \
	  echo "        chmod +x ~/.docker/cli-plugins/docker-buildx"; \
	  exit 1; \
	fi
	@if ! docker buildx inspect $(BUILDX_BUILDER) >/dev/null 2>&1; then \
	  echo "Creating buildx builder '$(BUILDX_BUILDER)' (docker-container driver, pulls ~150MB moby/buildkit on first use)..."; \
	  docker buildx create --name $(BUILDX_BUILDER) --driver docker-container >/dev/null; \
	  docker buildx inspect --bootstrap $(BUILDX_BUILDER) >/dev/null; \
	fi

# Produce every release artifact locally (4 binaries, multi-arch Docker
# image, checksums) under dist/ without publishing. Requires GoReleaser
# (v2+) and Docker. Drives the same .goreleaser.yaml config CI uses, so
# dist/ matches what a real release would produce for this commit.
snapshot: setup
	goreleaser release --snapshot --clean --skip=publish

# Tests
test:
	go test ./... -count=1 -timeout 300s

test-short:
	go test ./protocol/rpcsmartrouter/... -count=1 -timeout 120s

# Maintenance
tidy:
	go mod tidy

lint:
	go vet ./...

clean:
	rm -rf build/ dist/

.PHONY: install install-all install-smartrouter build build-all setup snapshot test test-short tidy lint clean
