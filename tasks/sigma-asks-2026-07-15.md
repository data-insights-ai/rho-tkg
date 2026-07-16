# rho-tkg asks from sigma-tkgd (2026-07-15)

**NEVER commit this file to `main`** — integration-branch only, dropped at squash
(merge protocol). Tracks the consumer-driven upstream asks recorded 2026-07-15.

These are DISTINCT from the earlier V412-8 batch (asks 1–6: TxPin adjacency Pin
doors, `NodeMatchesValidTime`, error-surface, stats doc, `OnDrop`, `QueueDepth`)
— all of which already landed in v4.15.0/v4.15.1. Below are FOUR NEW asks. Each is
additive (new methods / helpers / a new accepted value type); none change existing
signatures.

**Priority:** Ask 1 (HIGH) > Asks 2 & 4 (MEDIUM) > Ask 3 (LOW).

---

## Ask 1 — `tx.GetOrCreateByKey` (tx-scoped atomic get-or-create) — HIGH

**Consumer:** cypher `MERGE (n:Label {key: value})` fast path (sigma V412-1). BLOCKED.

**Problem.** `nodes.API.GetOrCreateByKey(ctx, label, propertyKey, value, extraProps)`
is exactly the atomic get-or-create MERGE wants, but it lives on `g.Nodes()` and
**auto-commits its own write**. sigma's MERGE executor runs inside a
statement-scoped `g.Tx().Begin()` transaction. Calling the `g.Nodes()` door from
inside that tx commits the create immediately, OUTSIDE the statement tx — so once
MERGE is combined with `ON CREATE SET`, a second MERGE, or any later clause that
can fail, `tx.Rollback()` no longer undoes the create. Correctness regression →
sigma cannot adopt the door as-is.

**Ask.** A transaction-scoped variant that joins the caller's open tx and defers
its write to `tx.Commit()`, mirroring `tx.AddNode` / `tx.AddRelationshipByIDIfAbsent`:

```go
func (tx *GraphTx) GetOrCreateByKey(
    label, propertyKey string, value any, extraProps map[string]any,
) (node *types.Node, created bool, err error)
```

Same semantics as the `g.Nodes()` version (single value lock → exactly one create
under concurrency; `value` an indexable scalar; `extraProps` seed only a fresh
node; works with or without an active unique constraint), but the create
participates in the tx, is visible to later reads on the same tx (ghost-read
consistency), and is undone by `tx.Rollback()`.

**Why it matters:** without it, MERGE-on-unique-property stays a non-atomic
scan-then-`tx.AddNode` that only avoids duplicates when a unique CONSTRAINT is
present. The tx-scoped door makes MERGE atomic-and-duplicate-free even without a
constraint — the whole point of `GetOrCreateByKey`.

**rho-tkg triage (where this lands):** the standalone door is `NodeOps.GetOrCreateByKey`
(`internal/core`, value-stripe + `addNodeInternal` heldStripes). The tx path is
`GraphTx` in `internal/core/tx.go` (its `AddNode`/`AddRelationshipByIDIfAbsent`
already defer to commit). Implement `GraphTx.GetOrCreateByKey` reusing the same
value-stripe + property-index probe, routing the create through the tx's deferred
apply instead of the immediate `addNodeInternal`. Re-export via `pkg/graph` TxAPI.
Two-phase + concurrency tests (exactly-one-create under N goroutines; rollback undoes).

---

## Ask 2 — bitemporal valid-time INTERVAL door `NodesDuringTx` / `RelsDuringTx` — MEDIUM

**Consumer:** cypher `MATCH … AS OF SYSTEM TIME t BETWEEN a AND b` (sigma V412-7). Half-limited.

**Context.** The bitemporal POINT case is fixed: `AS OF SYSTEM TIME t AT TIME v`
resolves via `NodesAtTx(v, t)` / `RelsAtTx(v, t)`. The INTERVAL case has no
equivalent: `NodesDuring(a, b)` / `RelsDuring(a, b)` resolve valid-time overlap
but ignore transaction time, so `AS OF SYSTEM TIME t BETWEEN a AND b` is stuck on
the belief-head-plus-overlap-filter path the point case just moved off — same
multi-valid-version miss.

**Ask.** The bitemporal interval siblings of `NodesAtTx`/`RelsAtTx`:

```go
func (a *API) NodesDuringTx(from, to, txAt types.Instant) ([]*types.Node, error)
func (a *API) RelsDuringTx(from, to, txAt types.Instant) ([]*types.Relationship, error)
```

Semantics: every entity version whose valid window OVERLAPS `[from, to)` as known
at `txAt`. `txAt == 0` ⇒ equivalent to `NodesDuring(from, to)`.

**Why it matters:** closes the last bitemporal-correctness gap in Cypher's
`AS OF SYSTEM TIME … BETWEEN …`.

**rho-tkg triage:** the single resolution seam is `resolveNodeChain`/`resolveRelChain`
(`chain_resolver.go`) via a `chainProbe`. `NodesDuring` uses `probeInterval`;
`NodesAtTx` adds the `TxAt` visibility filter. `NodesDuringTx` = `probeInterval`
WITH the `TxAt` filter applied in the same seam (rule 17: also wire the generic
`QueryOpts` door — `ByLabel`/`ByType` with interval+`TxAt`). The
predicate-anywhere-in-interval rule (test rule 16) must hold: a version whose
window overlapped `[from,to)` earlier but not on the belief-head-at-txAt must
still match. Two-phase adversarial tests across memory/badger/tiered/**sharded**.

---

## Ask 4 — public `ChangeRecord` → entity-identity decode — MEDIUM

**Consumer:** durable Memgraph/Neo4j mirror re-platform (sigma V412-4). BLOCKED.

**Problem.** sigma's Memgraph mirror rides the lossy in-memory `AsyncEventBus`
(events dropped under backpressure). The durable replacement is
`g.Replication().Watch(ctx, fromLSN)` + a persisted LSN cursor (at-least-once,
survives restart). `Watch` yields `store.ChangeRecord{LSN, Tag, Payload []byte}`.
To translate a record into the mirror's Cypher, the consumer needs the **entity
identity**: for a delete, the Snowflake ID alone; for a put, enough to re-read
current state (the ID). `ChangeRecord.Tag` gives the kind
(NodePut/RelPut/NodeDelete/RelDelete), but the ID lives only inside `Payload`, a
msgpack `NodeWire`/`RelWire` decodable **only** via `internal/storeutil`
(not importable out-of-tree). `ApplyChange` consumes a record but targets another
*tkg* graph; it never surfaces identity for a foreign sink.

**Ask.** A public decode helper or typed accessor on `ChangeRecord`:

```go
// Option A — free function (pkg/graph/store or pkg/graph/replication).
func DecodeChangeIdentity(rec ChangeRecord) (kind EntityKind, id types.SnowflakeID, err error)

// Option B — methods on ChangeRecord.
func (r ChangeRecord) EntityKind() EntityKind          // Node | Rel
func (r ChangeRecord) EntityID() (types.SnowflakeID, error)
```

Identity + kind is enough (sigma re-reads for puts, `DETACH DELETE` for deletes).
Optional fuller `AsNode()/AsRelationship()` would let CDC consumers skip the re-read.

**Why it matters:** unblocks the durable, restart-safe outbound mirror. Until
then the mirror stays on the lossy event bus, and `Watch` is tkg→tkg only, not a
foreign CDC sink.

**rho-tkg triage:** record bodies are `storeutil.NodePutBody`/`RelPutBody` (put =
UNTOKENIZED `NodeToWireChecked`) and `NodeDeleteBody`/`RelDeleteBody` (carry the
`int64` ID directly). The delete case is trivial (ID is a top-level field). The
put case needs the wire's `ID` field only — a SHALLOW decode (no property-key
registry needed since `EntityID` reads just the ID). Add a public
`store.DecodeChangeIdentity` (or `ChangeRecord` methods) that switches on `Tag`,
`SafeUnmarshal`s the minimal header, returns kind+ID. Keep the heavy
`AsNode/AsRelationship` optional/behind the full wire decode (needs the caller's
registry for tokenized props — but put bodies are untokenized, so it's decodable).
Fail-closed with `ErrCorruptWire` at the trust boundary. Export `EntityKind`.

---

## Ask 3 — `time.Time` accepted as a property value — LOW (ergonomics)

**Consumer:** sigma DTO / Bolt value conversion (V412-6a). Nice-to-have.

**Ask.** Accept `time.Time` directly as a node/rel property value (store as the
existing typed-temporal/ISO representation), so callers don't pre-convert at every
boundary (DTO decode, Bolt decode). No correctness gap — removes boilerplate.

**rho-tkg triage:** the allowlist validator is `PropertySlice.Set` /
`validatePropertyValue` (`pkg/types/propertyslice.go`) — recursive type switch,
depth-limited. Add a `time.Time` case that canonicalizes to the stored form
(likely `Instant`/Unix-ms or ISO string — pick ONE canonical rep and document it,
matching how temporal shadow values store). Must round-trip through
`deepCopyValue` + the content hash (`internal/integrity.appendPropertyValue`) —
so the hash inputs need the `time.Time` case too, or it canonicalizes to an
already-handled type BEFORE hashing (cleaner: convert to `Instant` at `Set`, so
downstream sees no new type). Lowest priority.

---

### Cross-cutting notes
- All four additive; none change existing signatures.
- Ask 1 unblocks V412-1 (MERGE atomicity); Ask 2 completes V412-7 BETWEEN; Ask 4
  unblocks V412-4 (durable outbound mirror). Ask 3 is pure ergonomics.
- Any new temporal/history-aware door (Asks 1, 2) needs two-phase tests (rule 15)
  and the generic-door audit (rule 17), across memory/badger/tiered AND the new
  sharded backend.
