# Native endpoint presence

Local patch on `codex/legal-native-endpoint-presence-20261011`, based on
published v4.50.1 (`09e58d36`). No push, tag, release, or deployment.

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

Review/rebase against current main, finish security/vulnerability gates,
test fresh Sigma integration and the OA typed adapter, approve and publish a
version, then pin/build/test the consumer. Current main remained clean during
this work; its independently advanced head was `d6fff484782007beaef0efba1530d30f445e247d`.
