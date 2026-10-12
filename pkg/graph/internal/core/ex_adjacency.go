package core

import (
	"errors"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Ex-adjacency (backlog 50). The store keeps, per node and direction, every
// relationship that named the node in a row its live adjacency no longer ties
// to it — written in the same batch as each relationship delete and each
// history row whose endpoints are not the live row's, by every door (Rels,
// GraphTx, batches, replica apply, IO.Import, ImportMerge, rollback), since all
// of them write through the store. The adjacency doors read a seed's
// ex-adjacency instead of folding all history (forEachRelExAdjacentID).
//
// A store written before the index existed lacks it, so the graph trusts it
// only once a scan at open has filled it (exAdjBuiltMeta). The store is stamped
// with a format marker releases without the index refuse (badger), so no older
// binary can delete relationships behind the marker.

// exAdjBuiltMeta marks a store whose ex-adjacency covers every relationship it
// holds: "1" once the backfill finished, "0" while it runs.
const exAdjBuiltMeta = "rel_exadj_built"

// backfillExAdjacencyBestEffort sets c.exAdj when the store's ex-adjacency is
// complete, running the one-time backfill first when the marker is missing.
// It does nothing (c.exAdj stays nil: the doors fold all history) for a store
// without the capability or MetaKV, for one that cannot be written (a
// read-only open: the marker probe fails before any scan), and when the scan
// fails.
func (c *Core) backfillExAdjacencyBestEffort(store storepkg.MandatoryStore) {
	ex, ok := store.(storepkg.ExAdjacencyCapability)
	if !ok || c.metaKV == nil {
		return
	}
	v, err := c.metaKV.MetaGet(exAdjBuiltMeta)
	if err != nil {
		return
	}
	if string(v) == "1" {
		c.exAdj = ex
		return
	}
	if err := c.metaKV.MetaSet(exAdjBuiltMeta, []byte("0")); err != nil {
		return // not writable
	}
	noted, err := c.backfillExAdjacency(ex)
	if err != nil {
		return
	}
	// The notes are buffered writes; they must be durable before the marker.
	if d, ok := store.(storepkg.DurableFlushCapability); ok && noted > 0 && d.DurableFlushSupported() {
		if err := d.DurableFlush(); err != nil {
			return
		}
	}
	if err := c.metaKV.MetaSet(exAdjBuiltMeta, []byte("1")); err != nil {
		return
	}
	c.exAdj = ex
}

// backfillExAdjacency streams every relationship with history and notes each
// distinct pair of endpoints its rows name other than its live row's (all of
// them for a deleted relationship). O(relationships with history), once.
func (c *Core) backfillExAdjacency(ex storepkg.ExAdjacencyCapability) (noted int, err error) {
	var ferr error
	err = c.store.ForEachRelHistoryID(func(id types.RelID) bool {
		var n int
		n, ferr = c.backfillExAdjacencyRel(ex, id)
		noted += n
		return ferr == nil
	})
	if ferr != nil {
		return noted, ferr
	}
	return noted, err
}

func (c *Core) backfillExAdjacencyRel(ex storepkg.ExAdjacencyCapability, id types.RelID) (int, error) {
	cur, err := c.getCurrentRelationship(id)
	switch {
	case errors.Is(err, storepkg.ErrRelNotFound):
		cur = nil
	case errors.Is(err, storepkg.ErrSlotNotLocal):
		return 0, nil
	case err != nil:
		return 0, err
	}
	history, err := c.getRelHistory(id)
	if err != nil {
		return 0, err
	}
	noted := make(map[[2]types.NodeID]struct{}, 1)
	for _, h := range history {
		ends := [2]types.NodeID{h.StartNodeID(), h.EndNodeID()}
		if cur != nil && ends == [2]types.NodeID{cur.StartNodeID(), cur.EndNodeID()} {
			continue
		}
		if _, dup := noted[ends]; dup {
			continue
		}
		noted[ends] = struct{}{}
		if err := ex.NoteExAdjacentRel(id, ends[0], ends[1]); err != nil {
			return len(noted), err
		}
	}
	return len(noted), nil
}
