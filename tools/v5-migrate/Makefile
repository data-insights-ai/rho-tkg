SHELL := /bin/bash
GO ?= go
.DEFAULT_GOAL := build
# Package-loading analyzers must resolve the same compiler as the Go recipes.
GO_DIR := $(shell dirname "$$(command -v "$(GO)")")
export PATH := $(GO_DIR):$(PATH)
export GOWORK := off
export GOTOOLCHAIN := local
export GOMAXPROCS := 2
export GOMEMLIMIT := 2GiB
export GOFLAGS := -p=1

GOLANGCI_LINT_VERSION ?= v2.13.2
GOSEC_VERSION ?= v2.29.0
GOVULNCHECK_VERSION ?= v1.7.0

.PHONY: version tools build test test-v test-race test-integration vet cover cover-gate fmt fmt-check lint security vulncheck lint-docker security-docker vulncheck-docker ci ci-docker clean
version:
	@test "$$($(GO) env GOVERSION)" = go1.26.9 || { echo "Go1.26.9 required" >&2; exit 1; }
build: version
	$(GO) build ./...
test: version
	$(GO) test -short -count=1 ./...
test-v: version
	$(GO) test -short -count=1 -v ./...
test-race: version
	$(GO) test -short -race -count=1 ./...
test-integration: version
	$(GO) test -count=1 ./...
vet: version
	$(GO) vet ./...
cover: version
	$(GO) test -short -count=1 -coverprofile=coverage.out ./...
	$(GO) tool cover -func=coverage.out
cover-gate: cover
	@$(GO) tool cover -func=coverage.out | awk '/^total:/ { total++; if ($$3+0 < 80) bad=1 } /PreflightNode|PreflightRel/ { direct++; if ($$3+0 < 80) bad=1 } END { if (total!=1 || direct!=2 || bad) { print "migration coverage gate requires total and both public functions >=80%"; exit 1 } }'
fmt: version
	$(GO) fmt ./...
fmt-check: version
	@test -z "$$($$($(GO) env GOROOT)/bin/gofmt -l *.go)"
tools: version
	$(GO) install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)
	$(GO) install github.com/securego/gosec/v2/cmd/gosec@$(GOSEC_VERSION)
	$(GO) install golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION)
lint: version
	golangci-lint run --config ../../.golangci.yml ./...
security: version
	gosec -quiet ./...
vulncheck: version
	govulncheck ./...
GO_VERSION = $(shell awk '/^go /{print $$2}' go.mod)
GO_IMAGE ?= golang:$(GO_VERSION)
DOCKER_GO = docker run --rm --cpus=2 --memory=4g -e GOTOOLCHAIN -e GOWORK -e GOMAXPROCS -e GOMEMLIMIT -e GOFLAGS \
	-v "$(abspath ../..)":/src -w /src/tools/v5-migrate \
	-v rho-tkg-gocache:/go -v rho-tkg-buildcache:/root/.cache/go-build $(GO_IMAGE)
lint-docker:
	$(DOCKER_GO) sh -c 'go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION) && golangci-lint run --config ../../.golangci.yml ./...'
security-docker:
	$(DOCKER_GO) sh -c 'go install github.com/securego/gosec/v2/cmd/gosec@$(GOSEC_VERSION) && gosec -quiet ./...'
vulncheck-docker:
	$(DOCKER_GO) sh -c 'go install golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION) && govulncheck ./...'
ci: fmt-check vet lint build test test-race security vulncheck cover-gate
ci-docker: fmt-check vet lint-docker build test test-race security-docker vulncheck-docker cover-gate
clean:
	rm -f coverage.out
