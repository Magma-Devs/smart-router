#!/usr/bin/make -f

BINDIR ?= $(GOPATH)/bin

# Install the router binary to $GOPATH/bin
install-all: install

install:
	go install -mod=readonly ./cmd/smartrouter

install-smartrouter: install

# Build the router binary into build/
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

clean:
	rm -rf build/

.PHONY: install install-all install-smartrouter build build-all test test-short tidy lint clean
