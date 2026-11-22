# Minimal rho-tkg Makefile (plain make = menu). GOFLAGS -trimpath; scripts/release for kind and tag push.

SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c
.DELETE_ON_ERROR:
.SUFFIXES:
MAKEFLAGS += --no-print-directory --no-builtin-rules --no-builtin-variables
.DEFAULT_GOAL := help
export GOFLAGS += -trimpath

CROSS_GOOS ?= linux
CROSS_GOARCH ?= arm
TEST_TIMEOUT ?= 20m
PKG ?=
RUN ?=
BENCH ?=
RACE_LOCK ?= $(HOME)/.cache/race-gate.lock
COVER_MIN ?= 80
GOSEC_VERSION ?= v2.29.0
GOVULNCHECK_VERSION ?= v1.7.0
FUZZTIME ?= 20s
OLD_API_PKG ?= ./pkg/graph
OLD_API_TEST ?= TestGraphAccessor_StoreOpsConsumerSurfaceWorks
OLD_REOPEN_PKG ?= ./pkg/graph/store/tiered
OLD_REOPEN_TEST ?= TestTieredCleanReopenMatchesFullRebuild
GOLANGCI_LINT_VERSION ?= v2.15.0
# lint Docker image: same toolchain as `go` on this machine (honours go.mod and GOTOOLCHAIN).
GO_VERSION := $(subst go,,$(shell go env GOVERSION))
GO_IMAGE ?= golang:$(GO_VERSION)
LINT_BIN := /go/bin/golangci-lint-$(GOLANGCI_LINT_VERSION)/golangci-lint
DOCKER_GO := docker run --rm -v "$(CURDIR)":/src:ro -w /src -v rho-tkg-gocache:/go \
	-v rho-tkg-buildcache:/root/.cache/go-build -v rho-tkg-lintcache:/root/.cache/golangci-lint $(GO_IMAGE)

.PHONY: help dev test-one old gate check race release release-major bench \
	build lint test gate-major fuzz

# help — Default goal: short index + every `# target —` comment in this file. Does not build or test.
help:
	@echo "  DEV        dev   test-one PKG=… [RUN=…]   old"
	@echo "  pre-push   gate | check   (same; handoff when green)"
	@echo "  release    release | release-major   (RH)"
	@echo "  optional   race   bench [BENCH=smoke|baseline|check]"
	@echo "  gate step  build | lint | test   (when gate fails on one)"
	@echo "  scripts    go run ./scripts/release kind | patch -dry-run | …"
	@echo ""
	@grep -hE '^# [a-z][a-z0-9_-]* — ' $(MAKEFILE_LIST) | awk -F' — ' '{printf "  %-18s %s\n", substr($$1,3), $$2}'

# dev — DEV: after an edit, before every commit: gofmt check, go vet, go test -short (Slow tests skip; Go's cache skips unchanged packages).
dev:
	@f=$$(gofmt -l $$(go list -f '{{.Dir}}' ./...)); test -z "$$f" || { echo "gofmt needed: $$f" >&2; exit 1; }; go vet ./... && go test -short -timeout $(TEST_TIMEOUT) ./...

# test-one — DEV: one package or test; PKG required (fails fast if missing); RUN optional; -short only.
test-one:
	@test -n "$(PKG)" || { echo 'usage: make test-one PKG=./pkg/... RUN=TestFoo' >&2; exit 2; }
	go test -short -timeout $(TEST_TIMEOUT) $(if $(RUN),-run '$(RUN)',) $(PKG)

# old — DEV: after new tests, two stable integration checks (public store surface + tiered clean reopen).
old:
	go test -timeout $(TEST_TIMEOUT) -run '^$(OLD_API_TEST)$$' $(OLD_API_PKG)
	go test -timeout $(TEST_TIMEOUT) -run '^$(OLD_REOPEN_TEST)$$' $(OLD_REOPEN_PKG)

# build — Compile every package, vet the measure and race tags (files behind a tag must keep compiling); last line cross-builds CROSS_* (gate step). Library only; no binary installed.
build:
	go build -o /dev/null ./...
	go vet -tags=measure ./... && go vet -tags=race ./...
	GOOS=$(CROSS_GOOS) GOARCH=$(CROSS_GOARCH) CGO_ENABLED=0 go build -o /dev/null ./...

# lint — Gate step: go.mod tidy -diff, verify, golangci-lint in Docker (report only, no --fix).
lint:
	go mod tidy -diff
	go mod verify
	$(DOCKER_GO) sh -ec 'test -x $(LINT_BIN) || GOBIN=$(patsubst %/,%,$(dir $(LINT_BIN))) go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION); $(LINT_BIN) run ./...'

# test — Gate step: the whole short tier, slow tests included (RHO_GATE=1; `make dev` is the fast dev run).
test:
	RHO_GATE=1 go test -short -timeout $(TEST_TIMEOUT) ./...

# gate — Pre-push before git push origin dev: make -k build lint test; lists every red step. Green gate does not push.
gate:
	$(MAKE) -k build lint test

# check — Same as gate; agents treat a task as done only when make check passes.
check: gate

# race — Optional by hand; flock shared with sigma. A patch release starts make race detached after push.
race:
	@mkdir -p "$(dir $(RACE_LOCK))"
	flock "$(RACE_LOCK)" env RHO_GATE=1 go test -race -short -timeout 30m ./...

# release — PROD patch (RH): scripts/release gate, tag, atomic push of main+dev+tag.
release:
	go run ./scripts/release patch

# release-major — PROD major (RH): make gate-major, then tag and push.
release-major:
	go run ./scripts/release major

# gate-major — Cold major gate only (release-major): -race -count=1, coverage floor, gosec, fuzz, govulncheck.
gate-major:
	@red=(); step() { echo "== $$1"; shift; "$$@" || red+=("$$1"); }; \
	step build $(MAKE) build; \
	step lint $(MAKE) lint; \
	step test-major bash -c 'flock "$(RACE_LOCK)" go test -race -count=1 -shuffle=on -timeout 90m -coverpkg=./pkg/... -coverprofile=coverage.out ./... && go tool cover -func=coverage.out | awk -v min="$(COVER_MIN)" '\''/^total:/ { sub("%","",$$3); print "coverage " $$3 "% (floor " min "%)"; exit $$3+0 < min }'\'''; \
	step gosec $(DOCKER_GO) sh -ec 'go install github.com/securego/gosec/v2/cmd/gosec@$(GOSEC_VERSION) && gosec -quiet ./...'; \
	step fuzz $(MAKE) fuzz; \
	step vuln $(DOCKER_GO) sh -ec 'go install golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION) && govulncheck ./...'; \
	(( $${#red[@]} == 0 )) || { echo "gate-major red: $${red[*]}"; exit 1; }

# fuzz — Trust-boundary harnesses (SECURITY.md); FUZZTIME each; used by gate-major, not patch gate.
fuzz:
	@t="$(FUZZTIME)"; \
	for f in FuzzImport FuzzApplyChange; do go test -run '^$$' -fuzz=$$f -fuzztime="$$t" ./pkg/graph/internal/core/; done; \
	for f in FuzzWireToNodeChecked FuzzWireToRelChecked FuzzUnmarshalNodeWireWithKeys; do \
		go test -run '^$$' -fuzz=$$f -fuzztime="$$t" ./pkg/graph/internal/storeutil/; done

# bench — Optional ./bench; BENCH=smoke|baseline|check or default informal ~0.3s; not in gate.
bench:
ifeq ($(BENCH),smoke)
	go test -bench=. -benchtime=1x -run '^$$' ./bench
else ifeq ($(BENCH),baseline)
	go test -bench=. -benchmem -benchtime=0.3s -count=1 -run '^$$' ./bench | tee bench/local-baseline.txt
else ifeq ($(BENCH),check)
	@test -f bench/local-baseline.txt || { echo 'run: make bench BENCH=baseline' >&2; exit 1; }
	go test -bench=. -benchmem -benchtime=0.3s -count=1 -run '^$$' ./bench | tee bench/local-current.txt
	bash bench/bench-compare.sh bench/local-baseline.txt bench/local-current.txt bench/local-benchstat.csv
else
	go test -bench=. -benchmem -benchtime=0.3s -count=1 -run '^$$' ./bench
endif