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
	c.advanceInstantFloor(types.Instant(binary.BigEndian.Uint64(v)))
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
	return c.persistInstantFloor()
}
