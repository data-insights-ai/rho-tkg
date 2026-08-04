# Plan — phase 2: relationship columns, persisted columns, zone maps before I/O

Proposed 2026-08-04, after v4.26.0. Written before any code, and grounded by
reading the existing machinery rather than assuming it.

Phase 1 (`plan-columnar.md`) shipped typed columns, validity columns + zone maps,
a typed scan door, and append-extend. This plan covers the three items that were
deliberately left out of it.

## What already exists (verified, not assumed)

| thing | state | why it matters here |
|---|---|---|
| `typeIdx map[uint16]map[types.RelID]struct{}` | exists on badger | EXACT mirror of `labelIdx`; rel-type membership is already in RAM |
| `types.NodeID` / `types.RelID` | both `snowflake.ID` | the snapshot machinery is structurally identical for both |
| `relEpoch`, `bumpRelEpoch`, `RelMutationEpoch` | exist | the invalidation counter for rels is already wired and bumped on every edge write |
| `CurrentWireFormatVersion = 2`, `verifyAndStampWireFormatVersion` | mature | fail-closed on newer, auto-raise on older, absent == v1 |
| `Config.TemporalIndexOnDisk` + `KeyTemporalIndex` | shipped | THE precedent for an on-disk index; see below |

**The `TemporalIndexOnDisk` precedent is the most important fact in this plan.**
Its own doc states the rule: *maintained alongside — never instead of — the RAM
structure; the RAM structure remains the sole authority for reads at runtime.* It
is opt-in, it is a REBUILD ACCELERATOR, and its rows are written in the SAME
`appendOps` call as the entity row for crash consistency.

## RC — relationship columns

**What is genuinely new:** a snapshot keyed by rel-type token, endpoint columns,
and a rel-side scan door. **What is not new:** membership (`typeIdx`),
invalidation (`relEpoch`), and the entire column structure.

### RC1 — generalise the snapshot over its ID type

`LabelDocValues` is keyed on `types.NodeID`. `RelID` is the same underlying type.
Two options, and the choice matters:

- **Generic** `DocValues[T ~int64]` — honest, no duplication, but touches every
  caller of the existing type.
- **Type-punning** (`types.NodeID(relID)`) — zero churn, and a trap: a rel
  snapshot whose vector is typed `NodeID` will eventually be handed to something
  expecting nodes, and nothing will complain.

**Decision: generic.** The punning option trades a one-time mechanical rename for
a permanent class of silent bug. Keep `LabelDocValues` as an alias of
`DocValues[types.NodeID]` so no existing caller changes.

### RC2 — endpoint columns

Start and end IDs are already int64. As columns they are free, and they are the
point of the whole exercise: a traversal aggregation can then read
`(start, end, weight)` as three typed arrays with zero entity materialisation.
Endpoint columns are ALWAYS built for a rel snapshot (unlike property columns,
which are requested), because every rel consumer needs them.

### RC3 — rel-side append-delta

Reuse the phase-1 discipline verbatim, including its hard-won part: **record on
the add seam, poison on the remove seam.** Find the rel equivalents of
`addNodePropertyKeyCounts` / `removeNodePropertyKeyCounts` FIRST; if the rel write
path has no such single seam (as the memory store's node path did not), fall back
to the memory-store inversion — **poison by default, opt in only at the insert
path** — rather than auditing call sites.

STOP CONDITION: if neither a two-seam nor a single-insert-path opt-in exists on
the rel write path, stop and report. Do not audit N call sites; that is the
failure mode phase 1 explicitly avoided.

### RC4 — `ScanRelColumns` capability

Mirror `NodeColumnScanCapability`: same batch shape plus `StartIDs`/`EndIDs`,
same refusal rules, same shared row fallback, same value-level A/B oracle against
the row path.

### RC acceptance

- A/B oracle: columnar vs row path agree value-for-value across rel-type,
  endpoint, property, absent and mixed-numeric cases.
- Routing probe: the fast path provably RAN (both paths return identical data, so
  equivalence alone proves nothing — phase 1's lesson).
- Append-extend probes including **delete-then-insert with no read between**, the
  case that only the poison catches.
- `-race` on both stores.

## CP — persisted columns (opt-in rebuild accelerator)

### The shape, and why it needs no migration

Follow `TemporalIndexOnDisk` exactly:

- **`Config.ColumnsOnDisk bool`**, default OFF. Off means today's behaviour, byte
  for byte.
- Persisted columns are **never a read authority**. A read still resolves against
  the in-RAM snapshot; the on-disk copy only avoids REBUILDING one.
- Every persisted column carries its **epoch stamp**. A stamp that does not match
  the label's current epoch means stale → discard and rebuild. This is the same
  gate the RAM cache already uses, so staleness is not a new failure mode.
- Corrupt or unreadable → discard and rebuild. Never fail a query.

**Therefore: no wire-format bump, and no migration path.** The keyspace is
additive; an older binary that does not know these keys simply never reads them,
and a newer binary treats anything unrecognised as "rebuild". This is the whole
reason to copy the `TemporalIndexOnDisk` shape rather than invent one — a format
whose worst case is "rebuild" does not need a compat contract.

CONTRAST, stated so it is not lost: an AUTHORITATIVE on-disk column WOULD need a
format bump, a compat marker and a migration path, because a misread would be a
wrong answer rather than a slow one. That design is explicitly rejected here.

### CP1 — column serialisation

Encode a built snapshot: ordinal vector, per-column typed arrays, presence
bitset, string dictionary, validity columns, zone map. Fixed-width arrays make
this nearly a memcpy; the dictionary is the only variable part.

### CP2 — write path

Written in the SAME `appendOps` batch as the triggering write, mirroring
`maintainTemporalIndexDiskAdd`, so a crash cannot leave a column that claims an
epoch the entity rows never reached.

### CP3 — read path

At build, if `ColumnsOnDisk` and a persisted column exists at the current epoch,
decode instead of re-reading every entity. Falls back to a full build otherwise.

### CP acceptance

- Decode(Encode(snapshot)) is **value-identical** to the original, including zone
  map decisions — the same oracle shape as `Extend`.
- A stale stamp, a truncated row and a corrupt dictionary each fall back to a
  rebuild and answer correctly.
- With `ColumnsOnDisk: false`, the on-disk keyspace is untouched (byte-for-byte
  no-op) — asserted, not assumed.
- Reopening a store with persisted columns answers identically to one without.

## ZM — zone maps before I/O

**Depends on CP and cannot be built first.** "Before I/O" means skipping the bulk
entity fetch during a build; to skip it you must consult the zone map before
reading, which means the zone map must be on disk. Building ZM against the RAM
zone map would be a no-op, since by then the I/O has already happened.

Once CP exists: a build carrying a valid-time predicate consults the persisted
per-block min/max and fetches only the blocks that can match.

**Honest bound on the win, measured in phase 1:** 98% of blocks skippable on a
time-clustered column, **0% on a scattered one**. Clustering is the natural case
(snapshots sort by ID, snowflakes ascend with mint time, unset `ValidFrom`
resolves to mint time) but backfilled data with arbitrary explicit bounds gets
nothing. ZM is worth doing only where CP is already on.

## Order, and the honest sizing

1. **RC** — independent of the other two, and the largest. Ships alone.
2. **CP** — independent of RC. Ships alone.
3. **ZM** — strictly after CP.

RC is comparable in size to all of phase 1. CP is smaller than it looks BECAUSE
of the accelerator framing. ZM is small once CP lands.

Each is independently shippable, and each must leave `golangci-lint` at 0,
`make cover-gate` above 80, and the full repo green under `-race`.

## What would make me stop and report instead of continuing

- RC3 finds no usable seam on the rel write path (see its STOP condition).
- CP's encode/decode oracle disagrees on any value — a persisted column that is
  not bit-identical to a rebuilt one is not a cache, it is a second source of
  truth, and the whole no-migration argument collapses.
- Any design pressure toward making persisted columns authoritative. That is a
  different project with a format bump, and it should be decided deliberately,
  not arrived at.
