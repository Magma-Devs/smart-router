#!/usr/bin/make -f

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

# Install the router binary to $GOPATH/bin (or $GOBIN)
install:
	go install $(GOFLAGS) ./cmd/smartrouter

# Build the router binary into build/
build:
	go build $(GOFLAGS) -o build/smartrouter ./cmd/smartrouter

# Interactive config wizard (separate go module under tools/wizard). Builds the
# router first so the wizard's spec-driven `health` checks have a binary, then
# launches the TUI. On launch the wizard runs an OS-adaptive prerequisite check
# (docker + compose v2, envsubst, bash) and hard-stops if a required tool is
# missing — native-Windows users are steered to WSL2/Git Bash. The wizard
# fetches chain specs + icons from the docs at runtime; nothing is vendored.
#
# Preflight runs BEFORE `make build` so a missing docker/envsubst hard-stops
# up front instead of after a full router compile. The launch still re-runs the
# gate (step 0) — this just fails fast.
wizard: wizard-preflight build
	@cd tools/wizard && go run . --repo $(CURDIR) --skip-preflight

# Check the wizard's external tool prerequisites for this OS and exit (no build,
# no TUI). Run this first if `make wizard` bails on a missing tool.
wizard-preflight:
	@cd tools/wizard && go run . --preflight

# Reprint the run command from the most recent wizard run (no router build, no
# TUI — just reads the saved record under ~/.config/smartrouter-wizard).
wizard-last:
	@cd tools/wizard && go run . --last

# Build the wizard binary into build/ without launching it.
wizard-build:
	cd tools/wizard && go build -o $(CURDIR)/build/sr-wizard .

# Test the wizard's non-TUI packages.
wizard-test:
	cd tools/wizard && go test ./...

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
# docs/RELEASING.md for the full release workflow.
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

clean:
	rm -rf build/ dist/

.PHONY: install build wizard wizard-preflight wizard-last wizard-build wizard-test setup snapshot changelog test test-short tidy lint clean
