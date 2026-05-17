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

# Produce every release artifact locally (4 binaries, multi-arch Docker
# image, checksums) under dist/ without publishing. Requires GoReleaser
# (v2+) and Docker. Drives the same .goreleaser.yaml config CI uses, so
# dist/ matches what a real release would produce for this commit.
snapshot:
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

.PHONY: install install-all install-smartrouter build build-all snapshot test test-short tidy lint clean
