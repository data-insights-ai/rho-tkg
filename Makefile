SHELL := /bin/bash
MAKEFLAGS += --no-print-directory
.DEFAULT_GOAL := test

# -trimpath keeps the checkout's path out of Go's cache keys, so every
# session and gate shares the same test cache (René, 4 Nov: GOFLAGS=-trimpath everywhere).
export GOFLAGS := -trimpath

RACE_LOCK ?= $(HOME)/.cache/race-gate.lock   # one -race suite on the host at a time, shared with sigma
COVER_MIN ?= 80
FUZZTIME ?= 20s
GOLANGCI_LINT_VERSION ?= v2.13.2
GOSEC_VERSION ?= v2.29.0
GOVULNCHECK_VERSION ?= v1.7.0
# The pinned tools run in the go.mod toolchain image: Docker is always here, the tools are not.
# Named volumes keep its module, build and lint caches and the installed binaries.
DOCKER_GO = docker run --rm -v "$(CURDIR)":/src -w /src -v rho-tkg-gocache:/go \
	-v rho-tkg-buildcache:/root/.cache/go-build -v rho-tkg-lintcache:/root/.cache/golangci-lint golang:$(shell awk '/^go /{print $$2}' go.mod)

.PHONY: build test new old lint mod gosec vuln fuzz test-major test-long race measure cover presubmit kind \
	gate-patch gate-minor gate-major release-patch release-minor release-major bench bench-baseline bench-check bench-compare

# --- Checks (each one plain go, or the pinned tool in Docker) -----------------
build:    ## compiles everything, 32-bit too (ARM32 broke unnoticed once)
	go build ./... && GOOS=linux GOARCH=386 CGO_ENABLED=0 go build ./...
test:     ## the short tier, cached: only what a change reaches reruns
	go test -short -timeout 20m ./...
new:      ## the new code's few tests: make new PKG=./pkg/graph/store/tiered RUN='TestA|TestB' (cached)
	@test -n "$(PKG)" -a -n "$(RUN)" || { echo "usage: make new PKG=./pkg/... RUN='TestA|TestB'"; exit 2; }
	go test -short -timeout 10m -run '$(RUN)' $(PKG)
old:      ## two old tests that nothing broke: the public API surface, and a tiered clean reopen equals a full rebuild (cached)
	go test -timeout 10m -run '^TestGraphAccessor_StoreOpsConsumerSurfaceWorks$$' ./pkg/graph
	go test -timeout 10m -run '^TestTieredCleanReopenMatchesFullRebuild$$' ./pkg/graph/store/tiered
lint:     ## golangci-lint (.golangci.yml: gofmt, goimports, go vet and the linters)
	$(DOCKER_GO) sh -c 'golangci-lint version 2>/dev/null | grep -q " $(GOLANGCI_LINT_VERSION:v%=%) " || go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION); golangci-lint run ./...'
mod:      ## go.mod and go.sum are tidy and verified
	go mod tidy -diff && go mod verify
gosec:    ## static security analysis
	$(DOCKER_GO) sh -c 'go install github.com/securego/gosec/v2/cmd/gosec@$(GOSEC_VERSION) && gosec -quiet ./...'
vuln:     ## known vulnerabilities in the dependencies
	$(DOCKER_GO) sh -c 'go install golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION) && govulncheck ./...'
fuzz:     ## the trust-boundary fuzz harnesses (docs/SECURITY.md), FUZZTIME each
	for f in FuzzImport FuzzApplyChange; do go test -run '^$$' -fuzz=$$f -fuzztime=$(FUZZTIME) ./pkg/graph/internal/core/ || exit 1; done
	for f in FuzzWireToNodeChecked FuzzWireToRelChecked FuzzUnmarshalNodeWireWithKeys; do go test -run '^$$' -fuzz=$$f -fuzztime=$(FUZZTIME) ./pkg/graph/internal/storeutil/ || exit 1; done
test-major: ## cold, with -race, and the coverage floor on ./pkg/... from the same run
	flock $(RACE_LOCK) go test -race -count=1 -shuffle=on -timeout 90m -coverpkg=./pkg/... -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out | awk -v min=$(COVER_MIN) '/^total:/ { sub("%","",$$3); print "coverage " $$3 "% (floor " min "%)"; exit $$3+0 < min }'

# --- By hand, never before a tag ----------------------------------------------
race:     ## -race over the short tier; after a patch or minor push (started by the release) and from cron
	flock $(RACE_LOCK) go test -race -short -timeout 30m ./...
test-long: ## the long tier: without -short, without race
	go test -timeout 60m ./...
measure:  ## the measurement level (//go:build measure): heap and allocation bounds, on a quiet host
	for f in $$(grep -rl '^//go:build measure' --include='*_test.go' pkg); do go test -tags measure -count=1 -v -run "^($$(grep -oE '^func Test\w+' $$f | cut -c6- | paste -sd'|'))$$" ./$${f%/*} || exit 1; done
cover:    ## coverage report of the short tier: coverage.html
	go test -short -coverpkg=./pkg/... -coverprofile=coverage.out ./pkg/... && go tool cover -html=coverage.out -o coverage.html
presubmit: ## a builder's check: vet and short tests of the packages a change reaches (scripts/release)
	go run ./scripts/release test
kind:     ## the diff's kind since the last tag (patch, minor, major) and why
	go run ./scripts/release kind

# --- Gates (steps at once; -k: a red run lists every red step) and releases ---
gate-patch:
	$(MAKE) -j -k -O build lint mod test
gate-minor:
	$(MAKE) -j -k -O build lint mod test gosec
gate-major:
	$(MAKE) -j -k -O build lint mod test-major gosec fuzz vuln
release-patch release-minor release-major: ## preconditions, kind, CHANGELOG, the gate, tag, atomic push (scripts/release)
	go run ./scripts/release $(@:release-%=%)

# --- Benchmarks (docs/CONTRIBUTING.md § Benchmarks) ---------------------------
bench:    ## the cross-backend suite, every scenario briefly
	go test -bench=. -benchmem -benchtime=0.3s -count=1 ./bench
bench-baseline: ## this machine's baseline for bench-check (gitignored)
	go test -bench=. -benchmem -benchtime=0.3s -count=1 -run '^$$' ./bench | tee bench/local-baseline.txt
bench-check: ## fails on a regression against the baseline (bench/bench-check.sh)
	bench/bench-check.sh
bench-compare: ## the graph micro-benchmarks across revisions (bench/bench-compare-revisions.sh)
	bench/bench-compare-revisions.sh
