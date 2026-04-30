#!/usr/bin/make -f

BINDIR ?= $(GOPATH)/bin

# ----------------------------------------------------------------------------
# Community build (default — no -tags enterprise).
# Produces the restrictive jsonrpc/HTTP/EVM-only binary. install / build /
# test targets are unchanged from prior sprints; existing scripts and CI
# workflows continue to work without modification.
# ----------------------------------------------------------------------------

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
	go build -mod=readonly -tags enterprise -o build/smartrouter-enterprise ./cmd/smartrouter

install-enterprise:
	go install -mod=readonly -tags enterprise ./cmd/smartrouter

test-enterprise:
	go test -tags enterprise ./... -count=1 -timeout 300s

# Convenience: build/test both editions in one invocation. Useful before
# committing a change that touches the gating system or build-tagged files.
build-both: build build-enterprise

test-both: test test-enterprise

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
	rm -rf build/

.PHONY: install install-all install-smartrouter build build-all test test-short \
        build-enterprise install-enterprise test-enterprise build-both test-both \
        tidy lint check-gates verify-binaries clean
