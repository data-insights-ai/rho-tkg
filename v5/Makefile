SHELL := /bin/bash
.DEFAULT_GOAL := build

.PHONY: build test test-v test-race test-integration cover cover-gate vet fmt fmt-check lint security vulncheck lint-docker security-docker vulncheck-docker check ci ci-docker clean

COVER_MIN ?= 80
GOLANGCI_LINT_VERSION ?= v2.13.2
GOSEC_VERSION ?= v2.29.0
GOVULNCHECK_VERSION ?= v1.7.0

build:
	go build ./...

test:
	go test -short -count=1 ./...

test-v:
	go test -short -count=1 -v ./...

test-race:
	go test -short -race -count=1 ./...

test-integration:
	go test -count=1 -run Integration ./...

# Reports belong to this module; root reports continue describing v4.
cover:
	go test -short -count=1 -coverpkg=./... -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html

cover-gate:
	go test -short -count=1 -coverpkg=./... -coverprofile=coverage.pkg.out ./...
	@total=$$(go tool cover -func=coverage.pkg.out | awk '/^total:/ {gsub("%","",$$3); print $$3}'); \
	awk -v t="$$total" -v min=$(COVER_MIN) 'BEGIN { if (t == "" || t+0 < min+0) { printf "v5 coverage gate: total=%s%% < min=%s%% or missing — failing\n", t, min; exit 1 } else { printf "v5 coverage gate: total=%s%% >= min=%s%% — ok\n", t, min } }'

vet:
	go vet ./...

# Include new untracked Go files, scoped to this module's working directory.
fmt:
	git ls-files --cached --others --exclude-standard -z '*.go' | xargs -0 gofmt -w

fmt-check:
	@unformatted=$$(git ls-files --cached --others --exclude-standard -z '*.go' | xargs -0 gofmt -l); \
		test -z "$$unformatted" || (echo "Files need formatting:"; echo "$$unformatted"; exit 1)

lint:
	golangci-lint run --config ../.golangci.yml ./...

security:
	gosec -quiet $$(go list -f '{{.Dir}}' ./...)

vulncheck:
	govulncheck $$(go list ./...)

GO_VERSION = $(shell awk '/^go /{print $$2}' go.mod)
GO_IMAGE ?= golang:$(GO_VERSION)
# Mount the repository so lint can read the shared configuration.
DOCKER_GO = docker run --rm -e GOTOOLCHAIN -e GOMAXPROCS -e GOFLAGS \
	-v "$(abspath ..)":/src -w /src/v5 \
	-v rho-tkg-gocache:/go -v rho-tkg-buildcache:/root/.cache/go-build $(GO_IMAGE)

lint-docker:
	$(DOCKER_GO) sh -c 'go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION) && golangci-lint run --config ../.golangci.yml ./...'

security-docker:
	$(DOCKER_GO) sh -c 'go install github.com/securego/gosec/v2/cmd/gosec@$(GOSEC_VERSION) && gosec -quiet ./...'

vulncheck-docker:
	$(DOCKER_GO) sh -c 'go install golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION) && govulncheck ./...'

check: vet build test
ci: fmt-check vet lint build test-race security vulncheck cover-gate
ci-docker: fmt-check vet lint-docker build test-race security-docker vulncheck-docker cover-gate

clean:
	rm -f coverage.out coverage.html coverage.pkg.out
