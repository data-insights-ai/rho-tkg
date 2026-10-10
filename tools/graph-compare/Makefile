SHELL := /bin/bash
.DEFAULT_GOAL := build
GO_BIN ?= go
REPRO_FLAGS ?=
CHECKS ?= build,normal,race,vet,cover,fmt,lint,security,vulncheck,exports
GOLANGCI_LINT_VERSION := v2.13.2
GOSEC_VERSION := v2.29.0
GOVULNCHECK_VERSION := v1.7.0
.PHONY: runner-tests build test test-v test-race test-integration vet cover cover-gate fmt fmt-check lint security vulncheck lint-docker security-docker vulncheck-docker tools ci ci-docker clean
runner-tests:
	python3 -B -m unittest -v test_reproduce
build:
	python3 -B reproduce.py --go "$(GO_BIN)" --checks build $(REPRO_FLAGS)
test test-v: runner-tests
	python3 -B reproduce.py --go "$(GO_BIN)" --checks normal $(REPRO_FLAGS)
test-race:
	python3 -B reproduce.py --go "$(GO_BIN)" --checks race $(REPRO_FLAGS)
test-integration:
	python3 -B reproduce.py --go "$(GO_BIN)" --checks exports $(REPRO_FLAGS)
vet:
	python3 -B reproduce.py --go "$(GO_BIN)" --checks vet $(REPRO_FLAGS)
cover cover-gate:
	python3 -B reproduce.py --go "$(GO_BIN)" --checks cover $(REPRO_FLAGS)
fmt:
	python3 -B reproduce.py --go "$(GO_BIN)" --format-tool
fmt-check:
	python3 -B reproduce.py --go "$(GO_BIN)" --checks fmt $(REPRO_FLAGS)
lint:
	python3 -B reproduce.py --go "$(GO_BIN)" --checks lint $(REPRO_FLAGS)
security:
	python3 -B reproduce.py --go "$(GO_BIN)" --checks security $(REPRO_FLAGS)
vulncheck:
	python3 -B reproduce.py --go "$(GO_BIN)" --checks vulncheck $(REPRO_FLAGS)
# Exact compiler is checked by the runner before the scanners are used.
# Provision pinned analyzers explicitly; never run this in correctness-only tests.
tools:
	GOTOOLCHAIN=local "$(GO_BIN)" install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)
	GOTOOLCHAIN=local "$(GO_BIN)" install github.com/securego/gosec/v2/cmd/gosec@$(GOSEC_VERSION)
	GOTOOLCHAIN=local "$(GO_BIN)" install golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION)
lint-docker:
	python3 -B reproduce.py --go "$(GO_BIN)" --checks lint --docker-scanners $(REPRO_FLAGS)
security-docker:
	python3 -B reproduce.py --go "$(GO_BIN)" --checks security --docker-scanners $(REPRO_FLAGS)
vulncheck-docker:
	python3 -B reproduce.py --go "$(GO_BIN)" --checks vulncheck --docker-scanners $(REPRO_FLAGS)
ci-docker: runner-tests
	python3 -B reproduce.py --go "$(GO_BIN)" --checks "$(CHECKS)" --docker-scanners $(REPRO_FLAGS)
ci: runner-tests
	python3 -B reproduce.py --go "$(GO_BIN)" --checks "$(CHECKS)" $(REPRO_FLAGS)
clean:
	@echo "Comparison outputs are caller-owned; clean does not delete evidence."
