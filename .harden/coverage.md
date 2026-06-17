# Hardening coverage ledger

Tracks which surfaces have been adversarially attacked (with real tests that can
fail) vs. still untested/weak. Append-only discipline: never delete or weaken a
test to make a pass look clean.

## Hardened

### Wire decode trust boundary — msgpack → NodeWire/RelWire (pass 1, v4.9.2)
- **Attack:** native Go fuzzing (`FuzzWireToNodeChecked`/`FuzzWireToRelChecked`/
  `FuzzUnmarshalNodeWireWithKeys`) + a 4-lens adversarial decode audit.
- **Found (2 serious):**
  1. `msgpack.Unmarshal` PANICS via reflect (`SetString/SetInt using
     unaddressable value`) on a duplicate map key for the interface-typed
     `PropertyWire.Value` — ~17 hostile bytes crash any store-read/import.
  2. FATAL stack overflow (unrecoverable) decoding a deeply-nested array/map
     into `Value any`.
- **Fix:** `storeutil.SafeUnmarshal` = non-recursive `guardMsgpackDepth` (reject
  nesting > 64 before decode) + panic recover, returning `store.ErrCorruptWire`.
  Rerouted every NodeWire/RelWire/meta/custom-property decode in badger +
  storeutil + core import.
- **Regression:** `wire_fuzz_test.go` (3 fuzz targets + committed crasher seed),
  `safe_decode_test.go` (panic-recover, deep-nesting rejection, legit-depth pin).
- **Re-fuzzed post-fix:** ~17M execs, 0 crashes.

## Still untested / weak (attack next)

- **Tiered crash-fault injection:** process kill between cross-shard split-writes
  (E→R / R→E ordering), mid-flush, mid-cascade. Recovery/rollback correctness
  under partial writes is asserted only on the happy path.
- **Clock-skew vs rotation boundaries:** hot→warm rotation alignment, cold
  demotion, and `ShardWindow` edges under non-monotonic / skewed wall clock.
- **Import stream end-to-end fuzzing:** the export/import record framing
  (`import.go` tag/length/header loop) beyond the per-row wire decode now covered
  — truncation/bit-flip is covered by lesson-44 tests but the framing parser
  itself is not fuzzed.
- **Other decoders:** tiered catalog/registry/index metadata files
  (`registry_file.go`, `temporal_index_file.go`, `vector_index_file.go`) decode
  flat typed structs (reviewed: no interface/deeply-nestable field, outside the
  pass-1 bug class) but are not fuzzed; badger meta decodes routed through
  SafeUnmarshal but not independently fuzzed.
- **Property index / vector index** query paths under adversarial values
  (NaN/Inf/huge-dim) — partially covered by lesson 23/25, not exhaustively fuzzed.
- **Concurrency soak:** long-running mixed read/write/tx/batch under the race
  detector beyond the existing targeted race tests.
- **Resource exhaustion:** huge-but-valid inputs (max labels, max properties,
  giant blobs) for latency/alloc cliffs; `DisableAllocLimit` default behavior.

## Notes
- Full suite: `go test -short -count=1 ./...` (~68s). Race gate:
  `go test -short -race -count=1 ./...` (touched pkgs run per pass).
- Version source of truth: first `## [x.y.z]` heading in `CHANGELOG.md`; keep
  `AGENTS.md` `Status:` and `docs/architecture.md` `(vX.Y.Z)` in sync (lesson 41).
