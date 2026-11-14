# rho-tkg — dev: Makefile + go test. One Go helper: scripts/release (tag/push, kind, presubmit).
# Everything else is Make recipes or plain go commands. See AGENTS.md § Scripts.
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
RACE_LOCK ?= $(HOME)/.cache/race-gate.lock
COVER_MIN ?= 80
GOSEC_VERSION ?= v2.29.0
GOVULNCHECK_VERSION ?= v1.7.0
FUZZTIME ?= 20s
OLD_API_PKG ?= ./pkg/graph
OLD_API_TEST ?= TestGraphAccessor_StoreOpsConsumerSurfaceWorks
OLD_REOPEN_PKG ?= ./pkg/graph/store/tiered
OLD_REOPEN_TEST ?= TestTieredCleanReopenMatchesFullRebuild
GOLANGCI_LINT_VERSION ?= v2.13.2
GO_VERSION := $(shell awk '/^toolchain go[0-9]/{sub(/^go/, "", $$2); v=$$2} /^go [0-9]/{if (!v) v=$$2} END{print v}' go.mod)
GO_IMAGE ?= golang:$(GO_VERSION)
LINT_BIN := /go/bin/golangci-lint-$(GOLANGCI_LINT_VERSION)/golangci-lint
DOCKER_GO := docker run --rm -v "$(CURDIR)":/src:ro -w /src -v rho-tkg-gocache:/go \
	-v rho-tkg-buildcache:/root/.cache/go-build -v rho-tkg-lintcache:/root/.cache/golangci-lint $(GO_IMAGE)

.PHONY: help setup test-one old presubmit gate race release release-minor release-major \
	dry-patch dry-minor kind bench bench-smoke bench-baseline bench-check fuzz measure gate-major \
	build lint test

define HELP_SECTION
	@echo ""
	@echo "  $(1)"
	@grep -hE '^[a-zA-Z0-9_.-]+:.*## $(2)' $(MAKEFILE_LIST) | awk -F':.*## ' '{d=$$2; sub(/^$(2)/, "", d); printf "    %-18s %s\n", $$1, d}'
endef

help: ## menu: does not build or test
	@echo "rho-tkg  |  DEV: test-one, old  |  optional: presubmit  |  pre-push: gate  |  release: René"
	$(call HELP_SECTION,clone,clone:)
	$(call HELP_SECTION,DEV:,DEV:)
	$(call HELP_SECTION,optional,optional:)
	$(call HELP_SECTION,GATE,GATE:)
	@echo ""
	@echo "  gate steps (if gate stops on one step)"
	@grep -hE '^# [a-z][a-z0-9_]* — ' $(MAKEFILE_LIST) | awk -F' — ' '{printf "    %-18s %s\n", substr($$1,3), $$2}'
	$(call HELP_SECTION,RELEASE,RELEASE:)
	$(call HELP_SECTION,bench,bench:)
	@echo ""
	@echo "  test-one: PKG=./pkg/...  RUN=TestName (optional)"
	@echo "  release:   go run ./scripts/release kind|presubmit|patch|..."
	@echo "  by hand:   make fuzz, make measure (not in gate)"

# --- clone ---
setup: ## clone: go mod download+verify once per worktree
	go mod download
	go mod verify

# --- DEV ---
test-one: ## DEV: one package; PKG required; RUN optional; -short
	@test -n "$(PKG)" || { echo 'usage: make test-one PKG=./pkg/graph/internal/core RUN=TestFoo' >&2; exit 2; }
	go test -short -timeout $(TEST_TIMEOUT) $(if $(RUN),-run '$(RUN)',) $(PKG)

old: ## DEV: two integration tests (store surface + tiered reopen)
	go test -timeout $(TEST_TIMEOUT) -run '^$(OLD_API_TEST)$$' $(OLD_API_PKG)
	go test -timeout $(TEST_TIMEOUT) -run '^$(OLD_REOPEN_TEST)$$' $(OLD_REOPEN_PKG)

# --- optional ---
presubmit: ## optional: affected vet+short since origin/main (go run ./scripts/release presubmit)
	go run ./scripts/release presubmit

race: ## optional: -race -short under flock (shared lock with sigma)
	@mkdir -p "$(dir $(RACE_LOCK))"
	flock "$(RACE_LOCK)" go test -race -short -timeout 30m ./...

# --- GATE steps (gate calls these) ---
# build — Compile every package; second line cross-builds. No install binary; library only.
build:
	go build -o /dev/null ./...
	GOOS=$(CROSS_GOOS) GOARCH=$(CROSS_GOARCH) CGO_ENABLED=0 go build -o /dev/null ./...

# lint — tidy -diff, verify, golangci-lint in Docker; report only, no --fix.
lint:
	go mod tidy -diff
	go mod verify
	$(DOCKER_GO) sh -ec 'test -x $(LINT_BIN) || GOBIN=$(patsubst %/,%,$(dir $(LINT_BIN))) go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION); $(LINT_BIN) run ./...'

# test — go test -short ./... (same as gate's test step).
test:
	go test -short -timeout $(TEST_TIMEOUT) ./...

gate: ## GATE: build, lint, test with -k (before git push origin dev)
	$(MAKE) -k build lint test

# --- RELEASE (René; git/tag/push in scripts/release only) ---
kind: ## RELEASE: patch|minor|major from diff since last tag
	go run ./scripts/release kind

dry-patch: ## RELEASE: preflight patch (no tag/push)
	go run ./scripts/release patch -dry-run

dry-minor: ## RELEASE: preflight minor (no tag/push)
	go run ./scripts/release minor -dry-run

release: ## RELEASE: patch gate, tag, atomic push
	go run ./scripts/release patch

release-minor: ## RELEASE: minor
	go run ./scripts/release minor

release-major: ## RELEASE: major (runs make gate-major, then tag)
	go run ./scripts/release major

# gate-major — cold major gate (release-major only): -race -count=1, coverage, gosec, fuzz, vuln.
gate-major:
	@red=(); step() { echo "== $$1"; shift; "$$@" || red+=("$$1"); }; \
	step build $(MAKE) build; \
	step lint $(MAKE) lint; \
	step test-major bash -c 'flock "$(RACE_LOCK)" go test -race -count=1 -shuffle=on -timeout 90m -coverpkg=./pkg/... -coverprofile=coverage.out ./... && go tool cover -func=coverage.out | awk -v min="$(COVER_MIN)" '\''/^total:/ { sub("%","",$$3); print "coverage " $$3 "% (floor " min "%)"; exit $$3+0 < min }'\'''; \
	step gosec $(DOCKER_GO) sh -ec 'go install github.com/securego/gosec/v2/cmd/gosec@$(GOSEC_VERSION) && gosec -quiet ./...'; \
	step fuzz $(MAKE) fuzz; \
	step vuln $(DOCKER_GO) sh -ec 'go install golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION) && govulncheck ./...'; \
	(( $${#red[@]} == 0 )) || { echo "gate-major red: $${red[*]}"; exit 1; }

# fuzz — trust-boundary harnesses (SECURITY.md); FUZZTIME each; major gate and by hand.
fuzz:
	@t="$(FUZZTIME)"; \
	for f in FuzzImport FuzzApplyChange; do go test -run '^$$' -fuzz=$$f -fuzztime="$$t" ./pkg/graph/internal/core/; done; \
	for f in FuzzWireToNodeChecked FuzzWireToRelChecked FuzzUnmarshalNodeWireWithKeys; do \
		go test -run '^$$' -fuzz=$$f -fuzztime="$$t" ./pkg/graph/internal/storeutil/; done

# measure — //go:build measure tests, one file per process; by hand only.
measure:
	@for f in $$(grep -rl '^//go:build measure' --include='*_test.go' pkg); do \
		tests=$$(grep -oE '^func Test\w+' "$$f" | cut -c6- | paste -sd'|'); \
		go test -tags measure -count=1 -v -run "^($$tests)$$" "./$${f%/*}"; \
	done

# --- bench (bench/bench-compare.sh is the shared comparator; not in gate) ---
bench-smoke: ## bench: every scenario once
	go test -bench=. -benchtime=1x -run '^$$' ./bench

bench: ## bench: informal suite (~0.3s per scenario)
	go test -bench=. -benchmem -benchtime=0.3s -count=1 -run '^$$' ./bench

bench-baseline: ## bench: write bench/local-baseline.txt (gitignored)
	go test -bench=. -benchmem -benchtime=0.3s -count=1 -run '^$$' ./bench | tee bench/local-baseline.txt

bench-check: ## bench: regress vs local baseline
	@test -f bench/local-baseline.txt || { echo 'bench-check: run make bench-baseline first' >&2; exit 1; }
	go test -bench=. -benchmem -benchtime=0.3s -count=1 -run '^$$' ./bench | tee bench/local-current.txt
	bash bench/bench-compare.sh bench/local-baseline.txt bench/local-current.txt bench/local-benchstat.csv