package core

import (
	"encoding/binary"
	"fmt"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// instantFloorMeta is the MetaKV key holding the durable commit-clock floor
// watermark — the session's max lastInstant, persisted at Close and reseeded at
// open so NowTx()/c.now() stay above every persisted TxFrom even when a pre-close
// burst's monotonic floor outran the wall clock (lesson 71). Distinct from
// graphEpochMeta / replicaAppliedLSNMeta / idSlotLeaseMeta / asofTagsMeta.
const instantFloorMeta = "tx_instant_floor"

// seedInstantFloor raises the commit-clock floor to the durable watermark
// persisted by the previous session's Close (persistInstantFloor). This is what
// makes NowTx() reopen-safe: lastInstant resets to 0 on open, but a burst whose
// monotonic floor outran the wall would leave persisted TxFrom stamps ABOVE the
// reopened wall clock — without reseeding, NowTx()=c.now()=wall would under-cover
// them (and a fresh write could even be stamped BELOW an already-committed one,
// TX time going backwards). A missing/corrupt watermark is treated as absent: the
// floor stays wall-derived (no worse than before this fix, self-healing as the
// wall advances). No-op without MetaKV. Called from New during construction
// (single goroutine); lastInstant is touched only via its atomic CAS.
func (c *Core) seedInstantFloor() {
	mk, ok := c.store.(storepkg.MetaKVCapability)
	if !ok {
		return
	}
	v, err := mk.MetaGet(instantFloorMeta)
	if err != nil {
		// Could not READ the watermark — an IO / checksum / value-log fault, with
		// the stored value very possibly INTACT on disk. That is a different
		// failure from a readable-but-malformed blob and must be handled
		// differently: this session must not later OVERWRITE a floor it never
		// saw. The watermark is a monotone high-water mark, and persisting this
		// session's (lower) lastInstant over an unread one downgrades it
		// permanently — every later open then reseeds the regressed value and
		// NowTx() under-covers burst rows still in the store, which is the exact
		// anachronism lesson 71 exists to close.
		c.floorSeedUnreadable = true
		return
	}
	if len(v) != 8 {
		// Readable but malformed. This one we DO overwrite at Close — that is the
		// self-healing path the corrupt-watermark regression relies on.
		return
	}
	seeded := types.Instant(binary.BigEndian.Uint64(v))
	if seeded <= 0 {
		// Malformed: cannot be a real instant. Leave the clock wall-derived and
		// let Close REPLACE the garbage (the self-healing path).
		return
	}
	if !c.plausibleForeignStamp(seeded) {
		// Well-formed but unvalidatable against THIS host's wall clock — which
		// on a host whose RTC is decades behind is a statement about the clock,
		// not about the watermark. Treat it exactly like an unreadable value:
		// refuse to seed from it, and refuse to OVERWRITE it at Close, because
		// downgrading a monotone high-water mark silently drops committed rows
		// from every future AS-OF pin.
		c.floorSeedUnreadable = true
		return
	}
	c.advanceInstantFloor(seeded)
}

// plausibleForeignStamp bounds a transaction stamp that arrived from OUTSIDE
// this process against the host's wall clock, using the same tolerance
// advanceClockFloor applies to a caller-supplied HLC merge (~10 years —
// generous enough that no genuine clock skew is refused, tight enough to catch
// a unit/scale mixup or a corrupt blob).
//
// Three doors take untrusted stamps and all three bound them here: the durable
// watermark (bytes off disk), a full Import stream, and a delta ImportMerge
// stream. What differs is the RESPONSE, not the bound — see advanceImportedStamp.
//
// The bound is deliberately NOT inside advanceInstantFloor. That function also
// serves replica apply, which reproduces a primary's already-committed row;
// refusing there would leave NowTx() below durably stored data.
func (c *Core) plausibleForeignStamp(inst types.Instant) bool {
	// TWO-SIDED. An upper bound alone lets a NEGATIVE stamp through, and a
	// negative is not "safely in the past": RecordForeignIncoming stores
	// edge.TxFrom verbatim, so the row lands with negative transaction time,
	// which no AS-OF pin can ever reach — and TxFrom is outside the integrity
	// hash, so the chain verifiers still call the store healthy.
	return inst > 0 && int64(inst) <= c.clock().UnixMilli()+maxClockAdvanceSkewMillis
}

// checkImportedStamp is the IMPORT door's stamp bound. An export stream is
// untrusted input in exactly the way the durable watermark is — a truncated
// copy, a bit-rotted archive, a snapshot from a host with a broken RTC, or an
// attacker-supplied file — so its stamps are bounded like the seed's.
//
// It differs from the seed in what it does on refusal. The seed can silently
// fall back to a wall-derived clock because nothing has been stored yet. Here
// the row is already in the store, so silently declining to advance would leave
// NowTx() below durably stored data — the lesson-71 anachronism. The stamp is
// instead treated as what it is, corruption: the record is REJECTED, and
// Import's rollback unwinds the whole stream (see TestR9_ImportReplayError...).
// Loud and atomic beats silent and half-applied.
// It runs at DECODE time, before the wire can reach the store. Bounding it after
// the write was a real asymmetry with ImportMerge: the row (and, with the change
// log on, a change-feed event) was already published, so a stamp Import itself
// classified as corruption still escaped to replicas — whose apply door advances
// the clock unconditionally and correctly, having no way to know the record was
// about to be rejected upstream. Rollback unwinds the local store; it cannot
// unpublish. The advance itself still happens only AFTER a successful store.
func (c *Core) checkImportedStamp(inst types.Instant) error {
	if inst != 0 && !c.plausibleForeignStamp(inst) {
		return fmt.Errorf("%w: transaction stamp %d is implausibly far past this host's clock",
			ErrCorruptExport, int64(inst))
	}
	return nil
}

// persistInstantFloor writes the current commit-clock floor to the durable
// watermark so the next open can reseed it (seedInstantFloor). Called from Close
// before store.Close (which flushes it). No-op without MetaKV or when nothing was
// minted (floor 0). A clean shutdown restores the floor EXACTLY; after an unclean
// shutdown (no Close) the watermark is stale/absent and the floor falls back to
// the wall clock, self-healing as the wall advances past the drift. lastInstant
// is read atomically — a late straggler mutation racing Close cannot commit
// (closed is already set), so the persisted floor still covers every committed
// write.
func (c *Core) persistInstantFloor() error {
	if c.floorSeedUnreadable {
		// The open could not read the watermark, so we do not know what it holds.
		// Writing this session's floor over it could LOWER a durable high-water
		// mark. Leaving it untouched is strictly safer: a stale-but-higher value
		// only over-covers, while a downgraded one silently drops committed rows
		// from every future AS-OF pin.
		return nil
	}
	return c.writeInstantFloor()
}

// writeInstantFloor unconditionally writes the live floor to the durable
// watermark. It carries NO floorSeedUnreadable guard — callers decide whether
// the no-overwrite rule applies, because it does not apply everywhere.
func (c *Core) writeInstantFloor() error {
	mk, ok := c.store.(storepkg.MetaKVCapability)
	if !ok {
		return nil
	}
	floor := c.lastInstant.Load()
	if floor <= 0 {
		return nil
	}
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(floor))
	if err := mk.MetaSet(instantFloorMeta, buf); err != nil {
		return fmt.Errorf("graph: persist commit-clock floor: %w", err)
	}
	return nil
}

// restoreInstantFloorAfterReset re-persists the commit-clock floor after
// Admin.Reset() / a replica's ChangeClear apply wiped the MetaKV keyspace —
// instantFloorMeta's reapPolicyPreserve half (BACKLOG 13l). The commit clock is
// data-INDEPENDENT node state: like idSlotLeaseMeta, and unlike every Reap key,
// it describes THIS NODE'S transaction-time position, not the graph's (now
// erased) entities. Letting Clear wipe it would let transaction time go
// BACKWARDS across the Clear — a burst leaves the floor above the wall, the
// wipe drops the watermark, and after a reopen fresh writes are stamped below
// records the change log already emitted before the Clear.
//
// Unlike captureIDSlotLeaseForReset there is deliberately NO capture-before-Clear
// half, because the authoritative value is not the persisted blob: it is the
// in-memory lastInstant, which Clear never lowers and which seedInstantFloor
// already raised to >= the persisted watermark at open. Re-persisting the live
// value after the wipe therefore always restores a floor >= the one Clear
// removed. Caller must hold c.mu.Lock.
func (c *Core) restoreInstantFloorAfterReset() error {
	// Deliberately writeInstantFloor, NOT persistInstantFloor.
	//
	// persistInstantFloor declines to write when this session could not READ the
	// watermark at open, because the durable value may be intact and higher and
	// must not be downgraded. That rule is right at Close and WRONG here: Clear
	// has already destroyed the key, so there is no durable value left to
	// protect, and honouring the rule would cost exactly the thing it exists to
	// preserve — the watermark would simply be gone.
	if err := c.writeInstantFloor(); err != nil {
		return err
	}
	// And clear the flag. It means "this session never learned the durable
	// value, so it must not overwrite it" — no longer true, because this session
	// just WROTE it. Left set, the following Close declines to persist a
	// strictly higher floor, stranding every post-Reset write above the
	// watermark.
	c.floorSeedUnreadable = false
	return nil
}

// checkImportedRecordStamp is the DELTA-MERGE door's stamp bound.
//
// ImportMerge reads the same untrusted stream as Import, but applies each
// record through applyChangeRecordLocked — the REPLICA-APPLY door, which
// advances the commit-clock floor unconditionally. That is correct for a
// replica (the primary already committed the row, so declining would strand it
// above the pin) and wrong for a file. One function, two callers, two trust
// levels: trust is a property of the CALLER, not of applyChangeRecordLocked.
//
// So the bound is applied here, at the merge trust boundary, BEFORE the record
// can reach the clock — and a violation is classified exactly as every other
// corruption on this path (ErrCorruptExport), so mergeRollback unwinds the
// whole delta atomically.
func (c *Core) checkImportedRecordStamp(rec storepkg.ChangeRecord) error {
	// stamp == 0 means "this tag carries no transaction stamp" (ChangeMeta,
	// ChangeClear, ChangeRangePurge, ChangeForeignIncomingDelete, the history
	// truncations). There is nothing to bound, and rejecting it would misdiagnose
	// a perfectly valid record as corruption.
	if stamp := recordCommitStamp(rec); stamp != 0 && !c.plausibleForeignStamp(stamp) {
		return fmt.Errorf("%w: change record LSN %d carries transaction stamp %d, implausibly far past this host's clock",
			ErrCorruptExport, rec.LSN, int64(stamp))
	}
	return nil
}
