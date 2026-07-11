# Security Policy

tkg/v4 is an embedded Go library (no network listener, no server process).
Its trust boundary is **untrusted bytes handed to the process** — a corrupt
on-disk row, a hostile `Import`/`ImportMerge` stream, or a replayed change-log
record from `Replication().ApplyChange` — not a network attack surface.

## Supported versions

Only the **latest minor release of the current major version (v4.x)** is
supported with security fixes. This is a fast-moving internal library;
`CHANGELOG.md` documents the full release history, and upgrading to the
latest v4.x tag is the supported path. There is no long-term-support branch
for older v4 minors, and v3.x is no longer maintained.

| Version | Supported |
|---|---|
| v4.x (latest) | Yes |
| < v4.x (older minors) | No — upgrade |
| v3.x | No |

## Reporting a vulnerability

Please report suspected security issues **privately**, not as a public GitHub
issue, to:

**security@data-insights.ai**

Please include:
- The affected version (or commit) and Go version.
- A minimal reproduction (a failing test is ideal — this repo already has
  fuzz harnesses you can extend, see below).
- The trust boundary crossed (e.g. "corrupt on-disk row", "hostile
  `Import` stream", "replayed change-log record"), since that determines how
  the fix is scoped.

We will acknowledge reports and work with you on a fix and disclosure
timeline before any public write-up.

## Hardening posture

The codebase treats every read of *persisted or externally-supplied* bytes as
a trust boundary and fails closed rather than panicking or crashing the
process. Pointers into the code for anyone auditing this library:

- **Depth-guarded, panic-recovering decode.** All untrusted msgpack decoding
  (persisted rows, import streams, change-log records) routes through
  `storeutil.SafeUnmarshal` (`pkg/graph/internal/storeutil/safe_decode.go`),
  which runs a non-recursive `guardMsgpackDepth` scan that rejects container
  nesting beyond a fixed bound **before** the decoder runs (the vmihailenco
  msgpack decoder fatally stack-overflows on deep nesting — unrecoverable by
  `recover()`), then recovers any remaining decoder panic (e.g. a reflect
  panic on a duplicate map key bound to an interface field). Both failure
  modes return the sentinel `store.ErrCorruptWire` instead of crashing the
  process. Audit recipe: `grep -rn 'msgpack.Unmarshal(' pkg/ | grep -v _test`
  — every non-test decode-from-bytes site should route through
  `SafeUnmarshal`, never call the raw decoder directly.
- **Import framing amplification bounds.** `pkg/graph/internal/core/import.go`
  reads each record body with a bounded copy instead of
  `make([]byte, declaredLength)`, so a small hostile stream that lies about a
  huge record length cannot force a huge allocation before any bytes are
  validated; replay/rollback preallocation derived from the (untrusted)
  export header's node/rel counts is likewise capped, independent of what the
  header claims. See `pkg/graph/internal/core/import_amplification_test.go`
  for the `runtime.TotalAlloc`-bounded regression tests.
- **Hash recompute-and-compare on import and replica apply.** Import
  (`pkg/graph/internal/core/import.go`) and change-log replica apply
  (`pkg/graph/internal/core/apply_record.go`) never trust a persisted or
  transmitted integrity hash at face value — they reconstruct each entity
  from its wire form and **recompute** the hash, comparing it against the
  claimed value before the row is admitted; a mismatch rolls back the whole
  replay (import) or rejects the record (apply). A full post-replay chain
  verification runs on top of the per-row check.
- **Fuzz harnesses.** Go native fuzzing exercises the decode and import
  boundaries directly:
  - `FuzzImport` (`pkg/graph/internal/core/import_fuzz_test.go`) — the
    end-to-end snapshot-import trust boundary.
  - `FuzzWireToNodeChecked`, `FuzzWireToRelChecked`,
    `FuzzUnmarshalNodeWireWithKeys`
    (`pkg/graph/internal/storeutil/wire_fuzz_test.go`) — the checked wire
    decode/validate path for nodes and relationships.

  Run any of them locally with, e.g.:

  ```bash
  go test -fuzz=FuzzImport -fuzztime=60s ./pkg/graph/internal/core/
  ```

## Not in scope

- Network-level attacks: this library has no listener, no RPC surface, and no
  authentication layer — that belongs to the consuming application.
- Vector search is brute-force k-NN, not a production ANN index; it is not a
  hardened attack surface distinct from the property/index read paths above.
- Log-shipped read replicas (`Config.ReadOnlyReplica`) are Phase 1 only:
  byte-exact apply of a trusted primary's change feed. There is no built-in
  authentication of the feed's origin or transport encryption — a consumer
  wiring replication across a network boundary is responsible for securing
  that transport.
