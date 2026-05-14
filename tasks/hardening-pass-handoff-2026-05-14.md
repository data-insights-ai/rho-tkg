# TKG hardening pass handoff - 2026-05-14

Objective being pursued: review, harden, optimize, and improve maintainability
of the entire TKG library while preserving correctness and historical
performance characteristics.

## Verified in this pass

- Export/import record framing now rejects oversized export records before
  writing data that import cannot read.
  - Files: `pkg/graph/internal/core/export.go`,
    `pkg/graph/internal/core/export_test.go`.
  - Evidence: targeted core export/import tests passed.

- Sparse daily/weekly recurrence expansion avoids scanning every calendar day
  between `from` and `to`.
  - Files: `pkg/types/recurrence.go`, `pkg/types/recurrence_test.go`.
  - Evidence: `go test ./pkg/types -count=1 -parallel=2` passed as part of
    the bounded package run.

- TieredStore typed ID fanout no longer converts `AllNodeIDs` / `AllRelIDs`
  pages through raw `snowflake.ID` slices before merging and paginating.
  - Files: `pkg/graph/store/tiered/tieredstore_read.go`,
    `pkg/graph/store/tiered/tieredstore_read_bulk.go`,
    `pkg/graph/store/tiered/tieredstore_read_bulk_rel.go`.
  - Evidence: tiered merge/pagination and public ID query tests passed.

- Transient cold-shard read fanout now has direct ID-query coverage.
  - Files: `pkg/graph/store/tiered/tieredstore_read_fanout_test.go`,
    `pkg/graph/store/tiered/tieredstore_read_test.go`.
  - Evidence: `go test ./pkg/graph/store/tiered -count=1 -parallel=2`
    passed as part of the bounded package run.

- Full-tree stabilization was run after the bounded handoff work.
  - Evidence: `make test`, `make test-race`, `make test-integration`,
    `make cover`, `make ci`, and `git diff --check` passed on the tree
    committed as `cfa71c0`.
  - `make ci` covered `fmt-check`, `go vet`, `golangci-lint`, `go build`,
    short race tests, `gosec`, `govulncheck`, and `cover-gate`.
  - Coverage gate reported `total=88.6% >= min=80%`.
  - `govulncheck` reported 0 reachable vulnerabilities; it also reported 3
    vulnerabilities in required modules that current code does not call.

- The full-gate pass found and fixed one additional static-security issue.
  - File: `pkg/graph/internal/core/export.go`.
  - Issue: `gosec` G115 on an `int`/`uint64` conversion in
    `readExportRecordBytes`.
  - Fix: validate the bounded record length once, convert it to `int`, and use
    that checked value for body bounds and slicing.

## Verification commands run

```sh
env GOFLAGS=-p=1 GOCACHE=/private/tmp/tkg-gocache go test ./pkg/types ./pkg/graph/internal/storeutil ./pkg/graph/store/tiered -count=1 -parallel=2
env GOFLAGS=-p=1 GOCACHE=/private/tmp/tkg-gocache go test ./pkg/graph/internal/core -run 'Test(ReadExportRecord|WriteExportRecord|ExportRecordSizeGuard|MarshalAndWrite|WriteHistoryEntries|ExportGraph_|IOExportNilWriter|IOImportNilReader|TxExportNilWriter|ImportWithOptions_|ImportGraph|Export_)' -count=1 -parallel=2
git diff --check -- pkg/types/recurrence.go pkg/types/recurrence_test.go pkg/graph/internal/core/export.go pkg/graph/internal/core/export_test.go pkg/graph/store/tiered/tieredstore_read.go pkg/graph/store/tiered/tieredstore_read_bulk.go pkg/graph/store/tiered/tieredstore_read_bulk_rel.go pkg/graph/store/tiered/tieredstore_read_test.go pkg/graph/store/tiered/tieredstore_read_fanout_test.go
git diff --check
make test
make test-race
make test-integration
make cover
env PATH=/Users/markusnissl/go/bin:/usr/local/bin:/opt/homebrew/bin:/usr/bin:/bin:/usr/sbin:/sbin make ci
```

## Commit, branch, and workspace state

- Commit created: `cfa71c0 Harden TKG correctness and performance`.
- Branch checked out: `hardening/tkg-correctness-performance`.
- Branch tracking state after push: `hardening/tkg-correctness-performance`
  tracks `origin/hardening/tkg-correctness-performance`.
- Repository worktree was clean after the commit.
- Extra registered `.claude/worktrees/*` worktrees were removed, and stale
  worktree metadata for missing `/private/tmp/tkg-v3.1.16` and
  `/private/tmp/tkg-v3.1.20` worktrees was pruned.
- `git worktree list --porcelain` then showed only the main repository
  worktree.

## Completion audit

The full objective is not complete.

- Entire library bug/correctness review: partially covered by selected slices,
  direct tests, and full gates, but not by a line-by-line hostile audit of the
  whole tree.
- Performance regression review: partially covered by structural changes and
  bounded tests, not by a fresh cross-version benchmark sweep after the final
  commit.
- Allocation/copy/lock/full-row-read review: partially covered in recurrence,
  export framing, storeutil pagination, TieredStore fanout, rollback paths, and
  direct API/store/type coverage, but not exhaustively across every package.
- Scalability review: partially covered in recurrence expansion and TieredStore
  cold-shard fanout.
- API consistency and ownership boundaries: partially covered in store/type
  boundary changes and sub-API direct tests, not comprehensively audited across
  all sub-APIs.
- Weak tests: improved for touched behavior, but the public surface is not
  fully re-audited.
- Broad gates: run and passed after the final static-security fix. The passing
  gates are strong regression evidence, but they are not proof that every
  stated review dimension has been exhaustively completed.

## Suggested next high-impact pass

Run a real completion audit instead of more opportunistic micro-audits:

1. Build a package-by-package checklist for graph, store, and type-layer public
   surfaces.
2. Map each public method, sentinel branch, nil path, rollback path, and store
   capability behavior to a direct test or a deliberate non-test rationale.
3. Run a fresh cross-version benchmark comparison against the v3.1.16/v3.1.20
   baseline for mutation and read hot paths.
4. Inspect any remaining high-allocation, lock-heavy, or full-row-read paths
   surfaced by the benchmark/coverage audit.
5. Re-run `make ci`, `make cover`, `make test-integration`, and any targeted
   benchmark checks after fixes.
