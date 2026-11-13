SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c
.DELETE_ON_ERROR:
MAKEFLAGS += --no-print-directory --no-builtin-rules
.DEFAULT_GOAL := help

# -trimpath keeps the checkout's path out of Go's cache keys, so every session shares one test cache (René, 4 Nov).
export GOFLAGS := -trimpath

GOLANGCI_LINT_VERSION ?= v2.13.2
# golangci-lint runs in the go.mod toolchain image, with named volumes for its caches and binary.
DOCKER_GO = docker run --rm -v "$(CURDIR)":/src -w /src -v rho-tkg-gocache:/go \
	-v rho-tkg-buildcache:/root/.cache/go-build -v rho-tkg-lintcache:/root/.cache/golangci-lint golang:$(shell awk '/^go /{print $$2}' go.mod)

.PHONY: help build test new old lint gate release

help: ## this list (scripts/ holds the rest: race, measure, fuzz, gate-major)
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-8s %s\n", $$1, $$2}'
build: ## compile everything, 32-bit too
	go build ./... && GOOS=linux GOARCH=386 CGO_ENABLED=0 go build ./...
test: ## the short tier, cached: only what a change reaches reruns
	go test -short -timeout 20m ./...
new: ## the new code's tests: make new PKG=./pkg/x RUN='TestA|TestB'; red when RUN matches nothing or a test skips
	$(if $(and $(PKG),$(RUN)),,$(error usage: make new PKG=./pkg/x RUN='TestA|TestB'))
	go test -timeout 10m -v -run '$(RUN)' $(PKG) | tee .git/make-new.out | grep -E '^(--- |ok|FAIL|panic:)|_test\.go:[0-9]+:'
	! grep -E 'no tests to run|^--- SKIP' .git/make-new.out
old: ## two old tests: the public API surface, a tiered clean reopen equals a full rebuild
	go test -timeout 10m -run '^TestGraphAccessor_StoreOpsConsumerSurfaceWorks$$' ./pkg/graph
	go test -timeout 10m -run '^TestTieredCleanReopenMatchesFullRebuild$$' ./pkg/graph/store/tiered
lint: ## golangci-lint (gofmt, goimports, go vet, the linters), go.mod tidy and verified
	go mod tidy -diff && go mod verify
	$(DOCKER_GO) sh -c 'golangci-lint version 2>/dev/null | grep -q " $(GOLANGCI_LINT_VERSION:v%=%) " || go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION); golangci-lint run ./...'
gate: ## the patch gate: build, lint, test at once; a red run lists every red step
	$(MAKE) -j -k -O build lint test
release: ## the patch release: gate, tag, atomic push, race run after (scripts/release)
	go run ./scripts/release patch
