#!/usr/bin/make -f

BINDIR ?= $(GOPATH)/bin

.DEFAULT_GOAL := help

help:
	@echo "Usage: make <target>"
	@echo ""
	@echo "Build & install:"
	@echo "  build              Build smartrouter binary to build/"
	@echo "  build-all          Alias for build"
	@echo "  install            Install smartrouter to GOPATH/bin"
	@echo "  install-all        Alias for install"
	@echo ""
	@echo "Test & quality:"
	@echo "  test               Run all tests"
	@echo "  test-short         Run smart router tests only"
	@echo "  lint               Run go vet"
	@echo "  tidy               Run go mod tidy"
	@echo ""
	@echo "Local compose stack:"
	@echo "  compose-up         Build binary and bring up local stack"
	@echo "  compose-down       Tear down local stack"
	@echo "  compose-render     Render compose files without starting"
	@echo "  compose-reinstall  Tear down (drop volumes) then bring up"
	@echo "  compose-logs       Follow logs of running stack"
	@echo ""
	@echo "Maintenance:"
	@echo "  clean              Remove build artifacts"

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

# Local docker-compose stack
compose-up:
	./scripts/compose_up.sh

compose-down:
	./scripts/compose_down.sh

compose-render:
	./scripts/compose_up.sh --render-only

compose-reinstall:
	./scripts/compose_up.sh --reinstall

compose-logs:
	docker compose -f compose/docker-compose.yml logs -f

# Maintenance
tidy:
	go mod tidy

lint:
	go vet ./...

clean:
	rm -rf build/

.PHONY: help install install-all install-smartrouter build build-all test test-short compose-up compose-down compose-render compose-reinstall compose-logs tidy lint clean
