SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c
.DELETE_ON_ERROR:
.SUFFIXES:
MAKEFLAGS += --no-print-directory --no-builtin-rules --no-builtin-variables
.DEFAULT_GOAL := help

# -trimpath keeps the checkout's path out of Go's cache keys, so every session shares one test cache (René, 4 Nov).
export GOFLAGS += -trimpath

# The cross build: a 32-bit target (ARM32 broke unnoticed once).
CROSS_GOOS ?= linux
CROSS_GOARCH ?= arm
TEST_TIMEOUT ?= 20m
# make old: two old tests, the public API surface and a tiered clean reopen against a full rebuild.
OLD_API_PKG ?= ./pkg/graph
OLD_API_TEST ?= TestGraphAccessor_StoreOpsConsumerSurfaceWorks
OLD_REOPEN_PKG ?= ./pkg/graph/store/tiered
OLD_REOPEN_TEST ?= TestTieredCleanReopenMatchesFullRebuild
# golangci-lint runs pinned in the go.mod toolchain image; named volumes keep its caches and binary.
GOLANGCI_LINT_VERSION ?= v2.13.2
GO_VERSION := $(shell awk '/^toolchain go[0-9]/{sub(/^go/, "", $$2); v=$$2} /^go [0-9]/{if (!v) v=$$2} END{print v}' go.mod)
GO_IMAGE ?= golang:$(GO_VERSION)
LINT_BIN := /go/bin/golangci-lint-$(GOLANGCI_LINT_VERSION)/golangci-lint
DOCKER_GO := docker run --rm -v "$(CURDIR)":/src:ro -w /src -v rho-tkg-gocache:/go \
	-v rho-tkg-buildcache:/root/.cache/go-build -v rho-tkg-lintcache:/root/.cache/golangci-lint $(GO_IMAGE)

.PHONY: help build test old lint gate release

help: ## this list (scripts/ holds the rest: race, measure, fuzz, gate-major)
	@grep -E '^[a-zA-Z0-9_-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-10s %s\n", $$1, $$2}'
build: ## compile everything, and for CROSS_GOOS/CROSS_GOARCH
	go build -o /dev/null ./...
	GOOS=$(CROSS_GOOS) GOARCH=$(CROSS_GOARCH) CGO_ENABLED=0 go build -o /dev/null ./...
test: ## the short tier, cached: only what a change reaches reruns
	go test -short -timeout $(TEST_TIMEOUT) ./...
old: ## two old tests that nothing broke (OLD_* above)
	go test -timeout $(TEST_TIMEOUT) -run '^$(OLD_API_TEST)$$' $(OLD_API_PKG)
	go test -timeout $(TEST_TIMEOUT) -run '^$(OLD_REOPEN_TEST)$$' $(OLD_REOPEN_PKG)
lint: ## go.mod tidy and verified; golangci-lint (gofmt, goimports, go vet, the linters)
	go mod tidy -diff
	go mod verify
	$(DOCKER_GO) sh -ec 'test -x $(LINT_BIN) || GOBIN=$(patsubst %/,%,$(dir $(LINT_BIN))) go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION); $(LINT_BIN) run ./...'
gate: ## the patch gate: build, lint, test one after another (no cache contention); a red run lists every red step
	$(MAKE) -k build lint test
release: ## the patch release: gate, tag, atomic push, race run after (scripts/release)
	go run ./scripts/release patch
