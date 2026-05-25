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
	-X github.com/Magma-Devs/smart-router/version.Version=$(VERSION) \
	-X github.com/Magma-Devs/smart-router/version.Commit=$(COMMIT)

GOFLAGS := -mod=readonly -trimpath -ldflags '$(LDFLAGS)'

export CGO_ENABLED ?= 0
export GOAMD64     ?= v3
export GOARM64     ?= v8.2

# ----------------------------------------------------------------------------
# Community build (default — no -tags enterprise).
# Produces the restrictive jsonrpc/HTTP/EVM-only binary. install / build /
# test targets are unchanged from prior sprints; existing scripts and CI
# workflows continue to work without modification.
# ----------------------------------------------------------------------------

# Install binary to $GOPATH/bin
install-all: install

install:
	go install $(GOFLAGS) ./cmd/smartrouter

install-smartrouter: install

# Build binary into build/
build-all: build

build:
	go build $(GOFLAGS) -o build/smartrouter ./cmd/smartrouter

# ----------------------------------------------------------------------------
# Enterprise build variants.
# The -tags enterprise flag compiles in license validation, full subscription
# managers, and all spec types. The license itself is read at runtime from
# --license-file / $SMART_ROUTER_LICENSE_FILE / ./license.key (Sprint 6 swapped
# from build-time //go:embed to runtime file-loading). Without the tag, the
# same source produces the community binary above.
#
# Output is build/smartrouter-enterprise so both binaries can sit side-by-
# side for symbol-isolation checks (go tool nm) and A/B testing.
# ----------------------------------------------------------------------------

build-enterprise:
	go build $(GOFLAGS) -tags enterprise -o build/smartrouter-enterprise ./cmd/smartrouter

install-enterprise:
	go install $(GOFLAGS) -tags enterprise ./cmd/smartrouter

test-enterprise:
	go test -tags enterprise ./... -count=1 -timeout 300s

# Convenience: build/test both editions in one invocation. Useful before
# committing a change that touches the gating system or build-tagged files.
build-both: build build-enterprise

test-both: test test-enterprise

# ----------------------------------------------------------------------------
# Release tooling (snapshot reproduction, changelog drafting).
# ----------------------------------------------------------------------------

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

# Produce every release artifact locally (binaries, multi-arch Docker
# image, checksums) under dist/ without publishing. Requires GoReleaser
# (v2+) and Docker. Drives the same .goreleaser.yaml config CI uses, so
# dist/ matches what a real release would produce for this commit.
snapshot: setup
	# --skip=sign: cosign keyless signing requires GitHub Actions OIDC,
	# which isn't available locally. CI (.github/workflows/release.yml)
	# does sign — see signs: in .goreleaser.yaml.
	IS_SNAPSHOT=1 goreleaser release --snapshot --clean --skip=publish --skip=sign

# Prepend a new release section to CHANGELOG.md for VERSION. Groups
# commits since the last v*.*.* tag by conventional-commit prefix and
# drafts the Highlights paragraph via Gemini Flash, then opens $EDITOR
# so you can review/edit before committing.
#
# This is only needed for local previews or hand-written special-case
# entries — CI does the same thing automatically on tag push. See
# RELEASING.md for the full release workflow.
#
# GEMINI_API_KEY is required to get a Highlights draft (the repo
# secret is only visible inside GitHub Actions — locally you need your
# own key from https://aistudio.google.com/apikey). Without it, the
# Highlights area is a TODO placeholder you'll need to fill in.
#
# Examples:
#   GEMINI_API_KEY=... make changelog VERSION=v1.0.0   # full draft
#   make changelog VERSION=v1.0.1 AI=0                 # explicitly skip LLM, TODO placeholder
#   make changelog VERSION=v1.0.1 EDIT=0               # don't open $EDITOR
changelog:
	@if ! echo "$(VERSION)" | grep -qE '^v[0-9]+\.[0-9]+\.[0-9]+(-[A-Za-z0-9.]+)?$$'; then \
	  echo "VERSION must be a semver tag like v1.0.0 or v1.0.0-rc1 (got: '$(VERSION)')" >&2; \
	  echo "Usage: make changelog VERSION=v1.0.0 [AI=0] [EDIT=0]" >&2; \
	  exit 1; \
	fi
	VERSION=$(VERSION) AI=$(AI) EDIT=$(EDIT) GEMINI_API_KEY=$(GEMINI_API_KEY) scripts/changelog-bump.sh

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

# Sprint 3.8 — source-level guard. Catches forbidden constructor calls
# at PR review via git grep. See scripts/check_gated_symbols.sh.
check-gates:
	@bash scripts/check_gated_symbols.sh

# Sprint 4.2 — post-build guard. Builds both editions and inspects the
# linked artifacts (symbols, strings, size delta, go mod graph) to catch
# leakage that source grep can't see. Complementary to check-gates — both
# are needed. See scripts/verify_binaries.sh.
verify-binaries:
	@bash scripts/verify_binaries.sh

clean:
	rm -rf build/ dist/

.PHONY: install install-all install-smartrouter build build-all \
        build-enterprise install-enterprise test-enterprise build-both test-both \
        setup snapshot changelog test test-short tidy lint check-gates verify-binaries clean
