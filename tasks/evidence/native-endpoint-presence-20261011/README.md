# Native endpoint presence

Local patch on `codex/legal-native-endpoint-presence-20261011`, based on
published v4.50.1 (`09e58d36`), then rebased onto fetched remote main
`efbedc28170c6b04aa28a0e957fc16c4424100c6`. The upstream delta changes
only HANDOVER.md, docs/v5-analysis.md and tasks/todo.md.
Release metadata is prepared for v4.50.2 in this isolated branch.
No push, tag, release, or deployment.

## Contract

- `types.TemporalEndpoint{Value, Finite}` distinguishes finite zero from
  infinity. An unbounded start is negative infinity; an unbounded end is
  positive infinity. Unknown time is outside this type.
- Creation and updates accept `tkg_valid_bounds: types.TemporalRange` as a
  value. Scalar endpoints retain their existing defaults; mixed input is refused.
- `QueryOpts.ValidAtSet` activates epoch zero. `ValidWindow` supplies typed
  interval bounds. Timeline `Bounds()` returns exact endpoint presence.
- Typed retrospective corrections use `SetNodeVersionBounds` and
  `SetRelVersionBounds` on Temporal, GraphTx, BatchBuilder and ingest Session.
  The existing `temporal.Ops` interface stays unchanged; the new facade methods
  use an optional capability.
- Wire v4 carries endpoint presence. Writable open advances the store marker;
  the actual published v4.50.1 reader rejects it before reading rows.
  A downgrade needs a pre-upgrade backup. Temporal metadata remains outside
  rho's existing content integrity hash contract.
- Scalar column batches decline typed rows before emitting callbacks and
  allow consumers to use row paths. The current declared relationship segment
  format refuses typed endpoints before current/history mutation.

## Evidence

All probes use actual native APIs and storage backends with synthetic temporal
boundary facts; they do not assert legal truth.

- `make test-race`: passed, 342.57 seconds.
- `make cover`: passed, 166.91 seconds; total 85.3%.
- `go vet ./...`: passed, final run 1.64 seconds; `make fmt-check` passed.
- `make build` and `check-metakv-reap`: passed; lifecycle policy test 0.473 seconds.
- `golangci-lint run ./...`: passed after import-only cleanup; final cached run 23.51 seconds.
- Pinned Docker lint: passed, zero issues, 34.97 seconds.
- Pinned Docker security: passed, 20.95 seconds.
- Pinned Docker vulnerability scan: passed, no vulnerabilities found, 5.60 seconds.
- Complete candidate `make ci-docker`: passed, 378.11 seconds; package coverage
  88.5% against the required 80% floor. This includes formatting, vet, pinned
  Docker lint/security/vulnerability scans, build, full race, coverage and
  MetaKV lifecycle policy.
- Full, partial, delta, nested and fixed-tail codecs preserve endpoint presence.
- Four backend probes cover exact closed-zero ends, zero starts, indexed
  point/window queries, counts, pinned retraction, retrospective correction,
  transaction rollback, batch and all ingest modes.
- Badger reopen preserves the earlier belief and exact timeline endpoints.
- Two-phase predicate tests reject old labels/properties after a typed update
  starts at zero. Constraint probes preserve closed-zero endpoint expiry.
- The no-allocation presence accessors preserve the 104-byte metadata layout.
- Additional malformed creation cases cover the input helper at 96.3%.

## Remaining release gates

Confirm downstream Sigma and OA typed adapter results, approve and publish
v4.50.2 through the normal release path, then pin/build/test the consumers
without permanent local replacements. Recheck remote main before promotion.
Local main remains unchanged and clean at
`d6fff484782007beaef0efba1530d30f445e247d`; its newer remote head is the
integrated revision above.

## Integrated source identity

Before and after remote-main rebase, the complete `pkg` subtree is
`4f8071777806a99c16918273e0b8f4feeaca4e52`. The prior tested source commit
is `6c92c5a8d35e503a0fa13872b8cd7b8faee8a3f3`; the rebased source commit is
`0deaab94067ac49754e121fbfd6d02a1cd495d41`.
The v4.50.2 tag was absent at the read-only remote check.
Release documentation consistency passes against the candidate metadata.
