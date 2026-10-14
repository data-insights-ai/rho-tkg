# Backlog 28 — state form of the column doors and counts: design

**Problem.** sigma reads pinned state as `{ValidAt: now, TxAt: pin}`. `CountByLabelAt` / `CountByTypeAt` and
`ScanNodeColumns` / `ScanRelColumns` answer it by the ByLabel/ByType row fold on every call (read every
candidate's row, resolve its chain): 1 M nodes on badger 13-14 s vs 1.4 s for unpinned columns. The as-of
column set (`DocValuesSnapshotAsOf`, key (label, txAt)) is a record door and does not apply.

**Key: (kind, label/type token, pin), no valid time in the key.** sigma's ValidAt is the wall clock, new on
every statement: a key holding validAt never hits and grows without bound. Instead cache the effective cut:
the label's effective-timeline segments at the pin (`forEachNodeEffectiveByLabels` /
`ForEachRelEffectiveByType`, item J plus the v4.51.2/55 scan machinery), whose contract is "for every t, the
segments containing t are exactly ByLabel(label, {ValidAt: t, TxAt: pin})". One cut answers every valid
instant: keep the segments with `from <= v < to` (to 0 = open). Rejected: (label, validAt, pin) keys
(explosion, no hit for "now"); a separate valid-end index cut (the timeline is that cut, built by the
resolver itself, so it cannot drift from it).

**What a cut holds** (struct of arrays, ID-ascending, an entity's segments contiguous). Per segment: id,
from, to (all a count reads), the winning row's raw ValidRange (and a relationship's endpoints) and, per
property key requested so far, a classified cell (1 B tag: absent / other / column kind; 8 B payload; strings
dictionary-coded). Node 40 B + 9 B per key and segment, rel 56 B + 9 B. A scan asking for a key the cut lacks
rebuilds it for the union of keys (the as-of set's rule). Built on the second ask for its key (a pin read once
costs the fold, as before, and no memory).

**Answers equal the row fold by construction.** Counts: the segments containing v, After/Limit in ID order as
the fold paginates. Scans: the same rows replayed through `store.ScanColumnsFromNodeSource` (new, sibling of
`ScanColumnsFromRelSource`), so kinds, Null, Other and `ErrMixedNumericColumn` follow the row driver's rules
(`TestColumnDrivers_NodeAndRelAgree`). Found while testing (red on main): the fold's own Ordinals at a pin
change after a later delete or update (the winning row's stored numbering), so temporal column scans now
carry Ordinals 0 on every path (experimental surface; sigma reads no ordinals at a coordinate). TxAt alone
is normalised to ValidAt = readNow, as the fold does. Fold as today: TxPin, intervals, Depth != all, a store
without the column capability (ok=false), any cut build error (a per-entity compaction or retention stub
below the pin). The graph-level gates still run first on every call.

**Invalidation: the as-of set's machinery, shared.** Cuts live in `asOfColumnCache` beside the as-of sets:
same epoch, bumped after every history rewrite at or below a pin (compaction, retention purge,
truncate/trim, import, ImportMerge, Reset, tx rollback, past-dated `*WithTx` writes, replica applies at or
below `maxPin`; backlog 21's coming rel purge must bump too), and the same `clear()` on exact erasure (cells
hold property values). Additions: (1) relationship replica applies feed the pin check (`noteWriteAt`), not
only the high-water mark, and a hard rel delete apply bumps; (2) a delete that clamps a scheduled future close
(backlog 25: the tombstone rewrites a valid end an earlier pin reads) bumps after its store write, primary and
replica, so the cut equals the fold, bug included, until 25 is fixed at its root. Forward writes do not
change the state at the pin (pin stability), so the cache survives write-active ingest.

**In-flight writes.** A cut is built only when every write stamped at or below the pin has landed: under a
`c.mu.TryLock` (never waits; the fold answers when it fails) (standalone writes run under the read lock; batches, ingest and applies under
the write lock) read `landed = min(lastInstant, openTxStart while a tx is open)`; build iff pin <= landed.
An update in flight also holds its entity lock, which the per-entity read takes; a create does not, hence
the barrier. `notePin(pin)` runs before the epoch is read; `put` drops a build whose epoch moved.

**Memory bound and eviction.** One byte budget per graph for all cuts, `stateCutBudget` = 256 MiB, LRU by
use; a cut over the budget answers its call and is not kept; stale-epoch cuts are dropped on the next put.
sigma's 1 M-node label with two keys is ~58 MB. Only callers of the state doors spend it; nothing persists.

**Cost after the build.** Count: one pass over the from/to arrays, no row read. Scan: the same pass plus the
cell replay. Build: one effective-timeline scan of the label, about one fold. `03-bench.txt`.
