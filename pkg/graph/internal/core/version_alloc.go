package core

import (
	"errors"
	"fmt"
	"math"

	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// One version allocator for every row a write appends to an entity's chain
// (backlog 18). A bounded SetVersionInterval appends rows ABOVE the current
// row's version while the current row keeps the store's current slot, so
// "current.Version() + 1" is not a free version: an Update, CloseVersion,
// label change or property CAS that used it wrote a second row with the
// cascade row's version, and every later reader that orders a chain by
// version (the as-of door, the rollback trim, History) saw two rows with one
// version. Every appending door therefore allocates one above the HIGHEST
// version stored for the entity — current row and history — so versions are
// unique and strictly increasing in write order.
//
// The highest version is derived from the chain under the entity lock (no
// persisted counter, no store format change). Versions are allocated densely
// (each write takes max+1), so a row above the current version exists iff the
// version current+1 does: the common case costs one point read that misses;
// only an entity whose chain holds rows above the current row (a cascade that
// left the current row in place) reads its history to find the top.
//
// Density is an assumption, shared with the as-of fast paths (memory
// history[v+1], badger's point read of key current+1, the core fallback) and
// the GraphTx rollback snapshot. Every write path of this package keeps it. A
// gap directly above the current row — a direct Store.TruncateNodeHistory /
// TrimNodeHistoryFrom that removed version current+1 but kept a higher one, or
// a re-import over an earlier life compacted below its top, stored before
// backlog 38 (a re-import now starts above the chain's top) — makes all of
// them answer "no row above": the allocator then hands out current+1 again
// (unique, since that key is free, but below the surviving higher row, so the
// newest write is no longer the highest version), and the as-of door answers
// the current row where SelectAsOfWithCurrent over the whole chain would
// answer the higher row. Memory, badger and the core fallback agree with each
// other in that case (all probe current+1).

// nodeHasHistoryAbove reports whether node id has a history row at version
// v+1, i.e. (versions being dense) any row above v.
func (c *Core) nodeHasHistoryAbove(id types.NodeID, v uint32) (bool, error) {
	if v == math.MaxUint32 {
		return false, nil
	}
	if p, ok := c.store.(storepkg.HistoryPresenceCapability); ok {
		// An entity without history rows has none above v (a RAM lookup on
		// badger, no key read).
		if has, err := p.HasNodeHistory(id); err != nil || !has {
			return false, err
		}
	}
	_, err := c.getNodeVersion(id, v+1)
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, storepkg.ErrVersionNotFound):
		return false, nil
	default:
		return false, err
	}
}

// relHasHistoryAbove mirrors nodeHasHistoryAbove for relationships. A Model-A
// foreign-incoming stub's slot is foreign (ErrSlotNotLocal): it has no local
// history.
func (c *Core) relHasHistoryAbove(id types.RelID, v uint32) (bool, error) {
	if v == math.MaxUint32 {
		return false, nil
	}
	if p, ok := c.store.(storepkg.HistoryPresenceCapability); ok {
		if has, err := p.HasRelHistory(id); err != nil || !has {
			if errors.Is(err, storepkg.ErrSlotNotLocal) {
				return false, nil
			}
			return false, err
		}
	}
	_, err := c.getRelVersion(id, v+1)
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, storepkg.ErrVersionNotFound), errors.Is(err, storepkg.ErrSlotNotLocal):
		return false, nil
	default:
		return false, err
	}
}

// nextNodeVersion returns the version of the next row appended to node id,
// whose current row is current: one above the highest version on the chain.
// Caller holds the entity lock.
func (c *Core) nextNodeVersion(id types.NodeID, current *types.Node) (uint32, error) {
	top := current.Version()
	above, err := c.nodeHasHistoryAbove(id, top)
	if err != nil {
		return 0, err
	}
	if above {
		history, err := c.getNodeHistory(id)
		if err != nil {
			return 0, err
		}
		for _, h := range history {
			top = max(top, h.Version())
		}
	}
	return nextEntityVersion(top)
}

// nextRelVersion mirrors nextNodeVersion for relationships.
func (c *Core) nextRelVersion(id types.RelID, current *types.Relationship) (uint32, error) {
	top := current.Version()
	above, err := c.relHasHistoryAbove(id, top)
	if err != nil {
		return 0, err
	}
	if above {
		history, err := c.getRelHistory(id)
		if err != nil {
			return 0, err
		}
		for _, h := range history {
			top = max(top, h.Version())
		}
	}
	return nextEntityVersion(top)
}

// A re-import of a deleted ID (Nodes().Import, Rels().Import,
// Nodes().AddByIDIfAbsent and the GraphTx twins) continues the ID's chain
// (backlog 38). History is keyed by version: a re-import that restarted at
// version 0 had its row stored under the earlier life's version-0 key by the
// first write that moved it to history (an Update, CloseVersion, a cascade),
// and the earlier row was gone — a pin taken before that write answered
// differently after it, and the hash chain lost a link. The new life
// therefore starts one above the highest version on the chain, with that
// row's hash as its PrevHash (the chain verifies across lives), and it is
// recorded after every stamp of the chain. A row's life is the number of
// deletes recorded before its TxFrom (chainWriteOrder, chainLifeEnds): a
// re-import recorded at or before the delete would join the earlier life and
// read absent from the delete on in the valid-time doors while the as-of doors
// (newest by version) read it present, and no stamp could tell it apart from
// a cascade row of the earlier life once it is demoted. So a caller instant
// (tkg_tx_from) at or below a stamp of the chain is refused with ErrTxOrder —
// the ordering rule of UpdateWithTx / DeleteWithTx — and the plain door stamps
// the row one past the chain's stamps when they lie ahead of the clock (a
// delete of a row whose valid start lies ahead is stamped after it), on this
// row only, without moving the commit clock. Versions then grow in write order across lives and
// (life, version) order is version order. Chains stored before the fix (a
// re-import at version 0) are read by the same life rule and never rewritten.

// lifeStart is where a re-import of a deleted ID starts: its version, its
// predecessor hash, and the largest TxFrom / TxTo / DeletedAt on the chain.
// The zero value is a fresh ID (no history).
type lifeStart struct {
	version  uint32
	prevHash string
	maxStamp types.Instant
}

// lifeStartOf derives the lifeStart from an entity's history rows (its current
// slot is empty: the ID is being imported).
func lifeStartOf[T storeutil.TemporalRow](history []T, hash func(T) string) (lifeStart, error) {
	if len(history) == 0 {
		return lifeStart{}, nil
	}
	top := history[0]
	var ls lifeStart
	for _, h := range history {
		if h.Version() > top.Version() {
			top = h
		}
		if tm := h.Temporal(); tm != nil {
			ls.maxStamp = max(ls.maxStamp, tm.TxFrom, tm.TxTo, tm.DeletedAt)
		}
	}
	v, err := nextEntityVersion(top.Version())
	if err != nil {
		return lifeStart{}, err
	}
	ls.version, ls.prevHash = v, hash(top)
	return ls, nil
}

// checkCallerTx refuses, before anything is allocated, a caller instant t
// (tkg_tx_from) at or below a stamp of the chain with ErrTxOrder. t == 0 is
// the plain door: nothing to check (see txFrom).
func (l lifeStart) checkCallerTx(t types.Instant) error {
	if t != 0 && t <= l.maxStamp {
		return fmt.Errorf("%w: a re-import at t %d is not after the ID's recorded stamp %d", ErrTxOrder, t, l.maxStamp)
	}
	return nil
}

// txFrom is the plain door's stamp for the re-imported row: the clock reading
// now, or one past the chain's largest stamp when that lies ahead of the clock
// (a delete of a row whose valid start lies ahead is stamped after it,
// validInstantAfter). Like the delete's own stamp it is local to this row: the
// commit-clock floor is NOT raised — that stamp came from caller valid time,
// which must never pull the floor forward (maxCommitStamp), and an unbounded
// raise would stamp every later write of every entity decades ahead.
func (l lifeStart) txFrom(now types.Instant) types.Instant {
	return validInstantAfter(now, l.maxStamp)
}

// stubLifeStart continues a chain whose rows are all gone but whose
// compaction stub remains — a retention purge after a history compaction
// removes the entity's rows, not its stub: the new life starts one above the
// trimmed versions and links to the last trimmed hash, which is what
// Verify*Chain checks the oldest stored row against when a stub exists (a
// version-0 row under a stub fails it). Only a graph that ever compacted can
// hold a stub (watermark > 0), so the probe costs nothing elsewhere.
func (c *Core) stubLifeStart(load func() (compactionStub, bool, error)) (lifeStart, error) {
	if c.compactedThroughTx.Load() == 0 {
		return lifeStart{}, nil
	}
	s, ok, err := load()
	if err != nil || !ok {
		return lifeStart{}, err
	}
	v, err := nextEntityVersion(s.TrimmedThroughVersion)
	if err != nil {
		return lifeStart{}, err
	}
	return lifeStart{version: v, prevHash: s.LastTrimmedHash, maxStamp: types.Instant(s.LastTrimmedTxTo)}, nil
}

// nodeLifeStart returns where a re-import of node id starts. Caller holds the
// entity lock and has seen no current row.
func (c *Core) nodeLifeStart(id types.NodeID) (lifeStart, error) {
	stub := func() (compactionStub, bool, error) { return c.loadNodeCompactionStub(id) }
	if p, ok := c.store.(storepkg.HistoryPresenceCapability); ok {
		if has, err := p.HasNodeHistory(id); err != nil {
			return lifeStart{}, err
		} else if !has {
			return c.stubLifeStart(stub)
		}
	}
	history, err := c.getNodeHistory(id)
	if err != nil {
		return lifeStart{}, err
	}
	if len(history) == 0 {
		return c.stubLifeStart(stub)
	}
	return lifeStartOf(history, nodeIntegrityHash)
}

// relLifeStart mirrors nodeLifeStart for relationships. A slot that is not
// local holds no history here.
func (c *Core) relLifeStart(id types.RelID) (lifeStart, error) {
	stub := func() (compactionStub, bool, error) { return c.loadRelCompactionStub(id) }
	if p, ok := c.store.(storepkg.HistoryPresenceCapability); ok {
		if has, err := p.HasRelHistory(id); errors.Is(err, storepkg.ErrSlotNotLocal) {
			return lifeStart{}, nil
		} else if err != nil {
			return lifeStart{}, err
		} else if !has {
			return c.stubLifeStart(stub)
		}
	}
	history, err := c.getRelHistory(id)
	if errors.Is(err, storepkg.ErrSlotNotLocal) {
		return lifeStart{}, nil
	}
	if err != nil {
		return lifeStart{}, err
	}
	if len(history) == 0 {
		return c.stubLifeStart(stub)
	}
	return lifeStartOf(history, func(r *types.Relationship) string {
		if ig := r.Integrity(); ig != nil {
			return ig.Hash
		}
		return ""
	})
}
