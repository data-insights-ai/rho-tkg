package badger

import (
	"fmt"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

var _ storecontract.ExAdjacencyCapability = (*Store)(nil)

// Ex-adjacency (store.ExAdjacencyCapability, backlog 50): one key
// 0x0D | node | dir | rel (storeutil.ExAdjKey) per relationship that named the
// node in a row the live adjacency (0x05 / 0x06) no longer ties to it. Written
// into the same pending batch as the adjacency-key delete of every relationship
// delete (deleteRelByInfo, DeleteRelEntityAndOut, DeleteRelIncoming; not the
// placement removals of a move or a create rollback) and as every history row
// whose endpoints are not the live row's (PutRelVersion), so it is on disk
// exactly when the delete or the row is. Read by a prefix scan
// with the pending overlay, like the disk-resident adjacency. No RAM copy.

// exAdjLegOp is one ex-adjacency key write: set (a leg left the live adjacency)
// or delete (a purge removed the relationship).
func exAdjLegOp(op writeOpType, node snowflake.ID, incoming bool, rel snowflake.ID) writeOp {
	return writeOp{opType: op, key: storepkg.ExAdjKey(node, incoming, rel)}
}

// exAdjLegOps returns both legs of one row (start: outgoing, end: incoming).
func exAdjLegOps(op writeOpType, rel, start, end snowflake.ID) []writeOp {
	return []writeOp{exAdjLegOp(op, start, false, rel), exAdjLegOp(op, end, true, rel)}
}

// exAdjOpsForHistoryRowLocked returns the ex-adjacency writes a history row of
// rid needs: none when the live row names the same endpoints (the live
// adjacency reaches the row), both legs otherwise (no live row here, or an
// earlier life with other endpoints). Caller holds idxMu (R or W).
func (bs *Store) exAdjOpsForHistoryRowLocked(rid types.RelID, r *types.Relationship) []writeOp {
	start, end := r.StartNodeID(), r.EndNodeID()
	if _, live := bs.relIDs[rid]; live {
		if !bs.adjOnDisk {
			if e, ok := bs.outIdx[start][rid]; ok && e == end {
				return nil
			}
		} else if cur, err := bs.getRelLocked(rid); err == nil && cur.StartNodeID() == start && cur.EndNodeID() == end {
			return nil
		}
	}
	return exAdjLegOps(writeOpSet, rid.SnowflakeID(), start.SnowflakeID(), end.SnowflakeID())
}

// exAdjRelIDs returns nid's ex-adjacency in one direction: the committed keys
// overlaid with the pending ones (flushing ++ pending; a pending delete wins).
func (bs *Store) exAdjRelIDs(nid types.NodeID, incoming bool) ([]types.RelID, error) {
	prefix := storepkg.ExAdjPrefix(nid.SnowflakeID(), incoming)
	var adds, dels map[types.RelID]struct{}
	bs.rangePending(func(k string, op writeOp) {
		if len(k) != storepkg.SizeExAdj || !hasPrefix([]byte(k), prefix) {
			return
		}
		rid := types.RelID(storepkg.ParseIDFromKey([]byte(k), 10))
		if op.opType == writeOpSet {
			if adds == nil {
				adds = make(map[types.RelID]struct{})
			}
			adds[rid] = struct{}{}
			delete(dels, rid)
			return
		}
		if dels == nil {
			dels = make(map[types.RelID]struct{})
		}
		dels[rid] = struct{}{}
		delete(adds, rid)
	})
	var ids []types.RelID
	err := bs.view(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			key := it.Item().Key()
			if len(key) != storepkg.SizeExAdj {
				continue
			}
			rid := types.RelID(storepkg.ParseIDFromKey(key, 10))
			if _, del := dels[rid]; del {
				continue
			}
			delete(adds, rid)
			ids = append(ids, rid)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("graph: ex-adjacency scan: %w", err)
	}
	for rid := range adds {
		ids = append(ids, rid)
	}
	return ids, nil
}

// exAdjNodeDeleteOps returns deletes for every ex-adjacency key of nid, both
// directions (a node purge).
func (bs *Store) exAdjNodeDeleteOps(nid types.NodeID) ([]writeOp, error) {
	var ops []writeOp
	for _, incoming := range []bool{false, true} {
		ids, err := bs.exAdjRelIDs(nid, incoming)
		if err != nil {
			return nil, err
		}
		for _, rid := range ids {
			ops = append(ops, exAdjLegOp(writeOpDelete, nid.SnowflakeID(), incoming, rid.SnowflakeID()))
		}
	}
	return ops, nil
}

// ForEachExAdjacentRelID implements store.ExAdjacencyCapability.
func (bs *Store) ForEachExAdjacentRelID(nid types.NodeID, incoming bool, fn func(types.RelID) bool) error {
	if err := bs.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return errNilIterationCallback()
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return err
	}
	ids, err := bs.exAdjRelIDs(nid, incoming)
	if err != nil {
		return err
	}
	for _, id := range ids {
		if !fn(id) {
			return nil
		}
	}
	return nil
}

// NoteExAdjacentRel implements store.ExAdjacencyCapability: both legs of one
// row, buffered with the store's other writes (durable at the next flush).
func (bs *Store) NoteExAdjacentRel(id types.RelID, start, end types.NodeID) error {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	if err := storecontract.ValidateRelationshipIndexEntry(start, end, 1, id); err != nil {
		return err
	}
	bs.appendOps(exAdjLegOps(writeOpSet, id.SnowflakeID(), start.SnowflakeID(), end.SnowflakeID())...)
	return bs.flushIfNeeded()
}

// NoteExAdjacentLeg records one leg (a partitioned store routes each leg to
// the shard holding that endpoint's adjacency).
func (bs *Store) NoteExAdjacentLeg(nid types.NodeID, incoming bool, id types.RelID) error {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return err
	}
	if err := storecontract.ValidateRelID(id); err != nil {
		return err
	}
	bs.appendOps(exAdjLegOp(writeOpSet, nid.SnowflakeID(), incoming, id.SnowflakeID()))
	return bs.flushIfNeeded()
}
