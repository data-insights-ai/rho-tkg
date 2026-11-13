SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c
.DELETE_ON_ERROR:
MAKEFLAGS += --no-print-directory --no-builtin-rules
.DEFAULT_GOAL := help

# -trimpath keeps the checkout's path out of Go's cache keys, so every session shares one test cache (René, 4 Nov).
export GOFLAGS := -trimpath

RACE_LOCK ?= $(HOME)/.cache/race-gate.lock # one -race suite on the host at a time, shared with sigma
COVER_MIN ?= 80
GOLANGCI_LINT_VERSION ?= v2.13.2
GOSEC_VERSION ?= v2.29.0
GOVULNCHECK_VERSION ?= v1.7.0
# The pinned tools run in the go.mod toolchain image, with named volumes for their caches and binaries.
DOCKER_GO = docker run --rm -v "$(CURDIR)":/src -w /src -v rho-tkg-gocache:/go \
	-v rho-tkg-buildcache:/root/.cache/go-build -v rho-tkg-lintcache:/root/.cache/golangci-lint golang:$(shell awk '/^go /{print $$2}' go.mod)

.PHONY: help build test new old lint tidy gate race measure release gate-major test-major gosec vuln fuzz

help: ## this list
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-11s %s\n", $$1, $$2}'

# --- The development loop (AGENTS.md) ---------------------------------------
build: ## compile everything, 32-bit too
	go build ./... && GOOS=linux GOARCH=386 CGO_ENABLED=0 go build ./...
test: ## the short tier, cached: only what a change reaches reruns
	go test -short -timeout 20m ./...
new: ## the new code's tests: make new PKG=./pkg/x RUN='TestA|TestB'
	@scripts/new.sh
old: ## two old tests: the public API surface, a tiered clean reopen equals a full rebuild
	go test -timeout 10m -run '^TestGraphAccessor_StoreOpsConsumerSurfaceWorks$$' ./pkg/graph
	go test -timeout 10m -run '^TestTieredCleanReopenMatchesFullRebuild$$' ./pkg/graph/store/tiered
lint: ## golangci-lint (gofmt, goimports, go vet and the linters)
	$(DOCKER_GO) sh -c 'golangci-lint version 2>/dev/null | grep -q " $(GOLANGCI_LINT_VERSION:v%=%) " || go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION); golangci-lint run ./...'
tidy: ## go.mod and go.sum are tidy and verified
	go mod tidy -diff && go mod verify

# --- Gate and release -------------------------------------------------------
gate: ## the patch gate: build, lint, tidy, test at once; a red run lists every red step
	$(MAKE) -j -k -O build lint tidy test
race: ## -race over the short tier, under the host-wide race lock (release and cron start it)
	flock $(RACE_LOCK) go test -race -short -timeout 30m ./...
measure: ## heap and allocation bounds (//go:build measure), by hand on a quiet host
	scripts/measure.sh
release: ## the patch release: gate, tag, atomic push (scripts/release)
	go run ./scripts/release patch

# A major only (go run ./scripts/release major runs it): cold, -race, coverage floor, gosec, fuzz, vuln.
gate-major:
	$(MAKE) -j -k -O build lint tidy test-major gosec fuzz vuln
test-major:
	flock $(RACE_LOCK) go test -race -count=1 -shuffle=on -timeout 90m -coverpkg=./pkg/... -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out | awk -v min=$(COVER_MIN) '/^total:/ { sub("%","",$$3); print "coverage " $$3 "% (floor " min "%)"; exit $$3+0 < min }'
gosec:
	$(DOCKER_GO) sh -c 'go install github.com/securego/gosec/v2/cmd/gosec@$(GOSEC_VERSION) && gosec -quiet ./...'
vuln:
	$(DOCKER_GO) sh -c 'go install golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION) && govulncheck ./...'
fuzz:
	scripts/fuzz.sh
