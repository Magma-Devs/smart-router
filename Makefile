#!/usr/bin/make -f

BINDIR ?= $(GOPATH)/bin

# Install binary to $GOPATH/bin
install-all: install

install:
	go install -mod=readonly ./cmd/smartrouter

install-smartrouter: install

# Build binary into build/
build-all: build

build:
	go build -mod=readonly -o build/smartrouter ./cmd/smartrouter

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

# Sprint 3.8 — guard against new callers bypassing the §3.3.6 gating system.
# See scripts/check_gated_symbols.sh for the symbol allowlist.
check-gates:
	@bash scripts/check_gated_symbols.sh

clean:
	rm -rf build/

.PHONY: install install-all install-smartrouter build build-all test test-short tidy lint check-gates clean
