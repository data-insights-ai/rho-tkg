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

## Verification commands run

```sh
env GOFLAGS=-p=1 GOCACHE=/private/tmp/tkg-gocache go test ./pkg/types ./pkg/graph/internal/storeutil ./pkg/graph/store/tiered -count=1 -parallel=2
env GOFLAGS=-p=1 GOCACHE=/private/tmp/tkg-gocache go test ./pkg/graph/internal/core -run 'Test(ReadExportRecord|WriteExportRecord|ExportRecordSizeGuard|MarshalAndWrite|WriteHistoryEntries|ExportGraph_|IOExportNilWriter|IOImportNilReader|TxExportNilWriter|ImportWithOptions_|ImportGraph|Export_)' -count=1 -parallel=2
git diff --check -- pkg/types/recurrence.go pkg/types/recurrence_test.go pkg/graph/internal/core/export.go pkg/graph/internal/core/export_test.go pkg/graph/store/tiered/tieredstore_read.go pkg/graph/store/tiered/tieredstore_read_bulk.go pkg/graph/store/tiered/tieredstore_read_bulk_rel.go pkg/graph/store/tiered/tieredstore_read_test.go pkg/graph/store/tiered/tieredstore_read_fanout_test.go
```

## Completion audit

The full objective is not complete.

- Entire library bug/correctness review: partially covered by selected slices,
  not the whole tree.
- Performance regression review: partially covered by structural changes and
  bounded tests, not by a fresh cross-version benchmark sweep.
- Allocation/copy/lock/full-row-read review: partially covered in recurrence,
  export framing, storeutil pagination, and TieredStore read fanout only.
- Scalability review: partially covered in recurrence expansion and TieredStore
  cold-shard fanout.
- API consistency and ownership boundaries: partially covered in store/type
  boundary changes, not comprehensively audited across all sub-APIs.
- Weak tests: improved for touched behavior, but the public surface is not
  fully re-audited.
- Broad gates: not run in this wrap-up because prior heavy runs caused
  system-level file-handle/PTY pressure. Use bounded `GOFLAGS=-p=1` runs until
  the environment is known healthy.

## Suggested next high-impact pass

Run a bounded release-style verification plan rather than more opportunistic
micro-audits:

1. `env GOFLAGS=-p=1 GOCACHE=/private/tmp/tkg-gocache go test ./pkg/types ./pkg/graph/internal/storeutil ./pkg/graph/store/memory ./pkg/graph/store/badger ./pkg/graph/store/tiered -count=1 -parallel=2`
2. `env GOFLAGS=-p=1 GOCACHE=/private/tmp/tkg-gocache go test ./pkg/graph/internal/core -count=1 -parallel=2`
3. If those pass and the machine is healthy, run a small benchmark comparison
   for mutation/read hot paths only.
4. Only after that consider `make cover`, `make test-race`, or integration
   targets.
