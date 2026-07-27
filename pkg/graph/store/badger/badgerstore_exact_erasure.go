package badger

import (
	"fmt"

	badgerv4 "github.com/dgraph-io/badger/v4"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// ExactErase implements store.ExactErasureCapability for Badger. A preliminary
// flush establishes one committed base. The destructive transition then holds
// flushMu + idxMu.Lock through flushIndexLocked(true), so direct Store writers
// cannot recreate a declared ID between RAM teardown and durable commit.
func (bs *Store) ExactErase(req storecontract.ExactErasureRequest) (storecontract.ExactErasureResult, error) {
	var zero storecontract.ExactErasureResult
	if err := bs.checkWritable(); err != nil {
		return zero, err
	}
	if err := validateExactErasureRequest(req); err != nil {
		return zero, err
	}
	if err := bs.flush(); err != nil {
		return zero, err
	}

	bs.flushMu.Lock()
	defer bs.flushMu.Unlock()
	bs.idxMu.Lock()
	defer bs.idxMu.Unlock()
	if err := bs.checkWritable(); err != nil {
		return zero, err
	}
	if retained, err := bs.exactErasureChangeLogRetainedLocked(); err != nil {
		return zero, err
	} else if retained {
		return zero, storecontract.ErrExactErasureChangeLogRetained
	}

	relSet := make(map[types.RelID]struct{}, len(req.RelIDs))
	for _, id := range req.RelIDs {
		relSet[id] = struct{}{}
	}
	nodeSet := make(map[types.NodeID]struct{}, len(req.NodeIDs))
	for _, id := range req.NodeIDs {
		nodeSet[id] = struct{}{}
	}
	for _, nid := range req.NodeIDs {
		for _, incoming := range []bool{false, true} {
			ids, err := bs.adjacentRelIDsSnapshotLocked(nid, 0, incoming)
			if err != nil {
				return zero, err
			}
			for _, rid := range ids {
				if _, ok := relSet[rid]; !ok {
					return zero, fmt.Errorf("%w: node %d relationship %d",
						storecontract.ErrExactErasureRelationshipEscape, nid, rid)
				}
			}
		}
	}
	// Defend against a missing/corrupt adjacency leg: the live relationship row
	// remains the endpoint authority. A corrupt undeclared row makes the proof
	// impossible, so fail closed rather than assume it is unrelated.
	for rid := range bs.relIDs {
		if _, declared := relSet[rid]; declared {
			continue
		}
		rel, err := bs.getRelLocked(rid)
		if err != nil {
			return zero, fmt.Errorf("%w: cannot prove undeclared relationship %d endpoints: %v",
				storecontract.ErrExactErasureRelationshipEscape, rid, err)
		}
		_, touchesStart := nodeSet[rel.StartNodeID()]
		_, touchesEnd := nodeSet[rel.EndNodeID()]
		if touchesStart || touchesEnd {
			return zero, fmt.Errorf("%w: node endpoint relationship %d",
				storecontract.ErrExactErasureRelationshipEscape, rid)
		}
	}

	// Resolve every fallible prefix scan before the first mutation. The keys are
	// immutable under idxMu.Lock and can be appended during the apply phase.
	historyOps := make([]writeOp, 0)
	for _, nid := range req.NodeIDs {
		keys, _, err := bs.historyTruncateDeleteKeys(storepkg.HistNodePrefix(nid.SnowflakeID()), 0)
		if err != nil {
			return zero, err
		}
		for _, key := range keys {
			historyOps = append(historyOps, writeOp{opType: writeOpDelete, key: []byte(key)})
		}
	}
	for _, rid := range req.RelIDs {
		keys, _, err := bs.historyTruncateDeleteKeys(storepkg.HistRelPrefix(rid.SnowflakeID()), 0)
		if err != nil {
			return zero, err
		}
		for _, key := range keys {
			historyOps = append(historyOps, writeOp{opType: writeOpDelete, key: []byte(key)})
		}
	}

	result := storecontract.ExactErasureResult{}
	for _, rid := range req.RelIDs {
		if _, live := bs.relIDs[rid]; live {
			result.RelsRemoved++
			r, err := bs.getRelLocked(rid)
			if err == nil {
				bs.deleteRelByInfo(relDeleteInfoFromRelationship(r))
			} else {
				// A corrupt declared row is still erasure material. The
				// corruption path purges every type/adjacency index by ID.
				if purgeErr := bs.purgeOrphanRelIDLocked(rid); purgeErr != nil {
					return zero, purgeErr
				}
				bs.appendOps(writeOp{opType: writeOpDelete, key: storepkg.RelKey(rid.SnowflakeID())})
			}
		}
		// Always run the by-ID corruption arm after the ordinary current-row
		// teardown. It reaps any extra type/adjacency key not derivable from the
		// current row and is a no-op for the canonical keys already queued.
		if err := bs.purgeOrphanRelIDLocked(rid); err != nil {
			return zero, err
		}
		bs.purgeExactRelSidecarsLocked(rid)
	}

	for _, nid := range req.NodeIDs {
		if _, live := bs.nodeIDs[nid]; live {
			result.NodesRemoved++
			// Every declared adjacency was removed above. cascadeDeleteInner is
			// now the canonical node/current-index teardown and cannot widen
			// scope; a corrupt node takes its brute-force purge branch.
			n, _ := bs.getNodeLocked(nid)
			pf := cascadeDeletePrefetch{node: nodeDeleteInfo{node: n, rev: bs.nodeRevs[nid]}}
			_, _, fatalErr := bs.cascadeDeleteInner(nid, pf)
			if fatalErr != nil {
				return zero, fatalErr
			}
		}
		// The ordinary cascade intentionally leaves temporal/HNSW candidate
		// tombstones and removes property/label entries using only the current
		// row shape. Exact erasure follows it with the brute-force by-ID arm so
		// stale/corrupt and tombstoned index residue is physically rebuilt away.
		if err := bs.purgeExactNodeResidueLocked(nid); err != nil {
			return zero, err
		}
		bs.purgeExactNodeSidecarsLocked(nid)
	}

	bs.appendOps(historyOps...)
	for _, mw := range req.MetaWrites {
		key := storepkg.MetaKey(mw.Key)
		if mw.Value == nil {
			bs.appendOps(writeOp{opType: writeOpDelete, key: key})
		} else {
			bs.appendOps(writeOp{opType: writeOpSet, key: key, value: append([]byte(nil), mw.Value...)})
		}
	}

	// Accumulators contain raw extrema and non-decrementable HLL register
	// contributions. Reconstruct them solely from surviving current rows.
	if err := bs.rebuildExactErasurePlannerSketchesLocked(); err != nil {
		return zero, err
	}
	bs.docMu.Lock()
	bs.docColumns = nil
	bs.docColumnsMulti = nil
	bs.docMu.Unlock()
	bs.bumpNodeEpoch()
	bs.nodeEpochSalt.Add(1)
	bs.bumpRelEpoch()

	if err := bs.flushIndexLocked(true); err != nil {
		return zero, err
	}
	return result, nil
}

func validateExactErasureRequest(req storecontract.ExactErasureRequest) error {
	if len(req.NodeIDs) == 0 && len(req.RelIDs) == 0 {
		return storecontract.ErrInvalidStoreMutation
	}
	var prevNode types.NodeID
	for i, id := range req.NodeIDs {
		if err := storecontract.ValidateNodeID(id); err != nil {
			return err
		}
		if i > 0 && id <= prevNode {
			return storecontract.ErrInvalidStoreMutation
		}
		prevNode = id
	}
	var prevRel types.RelID
	for i, id := range req.RelIDs {
		if err := storecontract.ValidateRelID(id); err != nil {
			return err
		}
		if i > 0 && id <= prevRel {
			return storecontract.ErrInvalidStoreMutation
		}
		prevRel = id
	}
	return nil
}

// exactErasureChangeLogRetainedLocked checks recording state, every in-memory
// buffer, and the persisted 0x09 keyspace. This catches a store reopened with
// ChangeLog disabled after an earlier log-bearing run.
func (bs *Store) exactErasureChangeLogRetainedLocked() (bool, error) {
	if bs.logEnabled.Load() {
		return true, nil
	}
	bs.wbMu.Lock()
	buffered := len(bs.pendingLog) != 0 || bs.scopeActive || len(bs.scopeLog) != 0 || len(bs.scopedLogs) != 0
	bs.wbMu.Unlock()
	if buffered {
		return true, nil
	}
	retained := false
	err := bs.db.View(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		prefix := []byte{storepkg.KeyChangeLog}
		it.Seek(prefix)
		retained = it.ValidForPrefix(prefix)
		return nil
	})
	return retained, err
}

func (bs *Store) purgeExactRelSidecarsLocked(rid types.RelID) {
	raw := rid.SnowflakeID()
	bs.maintainRelPropertyIndexesPurge(raw)
	bs.maintainRelTypeTemporalIndexesPurge(raw)
	delete(bs.relValidIdx, rid)
	delete(bs.relBeliefWatermark, rid)
	bs.relStatsContrib.Delete(raw)
	bs.relTypeClassContrib.Delete(raw)
	for tok, members := range bs.relTypeTxMembers {
		delete(members, rid)
		if len(members) == 0 {
			delete(bs.relTypeTxMembers, tok)
		}
	}
}

func (bs *Store) purgeExactNodeSidecarsLocked(nid types.NodeID) {
	delete(bs.nodeBeliefWatermark, nid)
	for tok, members := range bs.labelTxMembers {
		delete(members, nid)
		if len(members) == 0 {
			delete(bs.labelTxMembers, tok)
		}
	}
}

// purgeExactNodeResidueLocked is the idempotent/current-row-absent arm.
func (bs *Store) purgeExactNodeResidueLocked(nid types.NodeID) error {
	raw := nid.SnowflakeID()
	ops := []writeOp{{opType: writeOpDelete, key: storepkg.NodeKey(raw)}}
	if bs.labelOnDisk {
		tokens, err := bs.nodeLabelTokensFromKeyspaceLocked(nid)
		if err != nil {
			return err
		}
		for _, tok := range tokens {
			ops = append(ops, writeOp{opType: writeOpDelete, key: storepkg.LabelIndexKey(tok, raw)})
		}
	} else {
		for tok, ids := range bs.labelIdx {
			if _, exists := ids[nid]; exists {
				delete(ids, nid)
				ops = append(ops, writeOp{opType: writeOpDelete, key: storepkg.LabelIndexKey(tok, raw)})
			}
			if len(ids) == 0 {
				delete(bs.labelIdx, tok)
			}
		}
	}
	ops = append(ops, bs.maintainPropertyIndexesPurge(raw)...)
	indexpkg.PurgeNodeFromAllTemporalIndexes(bs.temporalIndexes, raw)
	ops = append(ops, bs.maintainTemporalIndexDiskPurge(raw)...)
	indexpkg.PurgeNodeFromAllHighFrequencyIndexes(bs.hfIndexes, raw)
	indexpkg.PurgeNodeFromAllVectorIndexesExact(bs.vectorIndexes, raw)
	bs.nodeCache.MarkDeleted(raw)
	delete(bs.nodeIDs, nid)
	delete(bs.nodeHashes, nid)
	bs.deleteNodeRevLocked(nid)
	bs.appendOps(ops...)
	return nil
}

func (bs *Store) rebuildExactErasurePlannerSketchesLocked() error {
	if bs.disablePlannerStats {
		return nil
	}
	bs.propertyStats = make(map[indexpkg.PropertyIndexKey]*indexpkg.PropertyStatsAccumulator)
	for nid := range bs.nodeIDs {
		n, err := bs.getNodeLocked(nid)
		if err != nil {
			// Match loadIndexes' corruption tolerance: planner statistics are
			// optional and must never turn an otherwise complete erasure into
			// a post-mutation partial failure.
			continue
		}
		labelCount := n.LabelTokenCount()
		n.ForEachIndexablePropertyValueKey(func(propertyKey, valueKey string) bool {
			value, _ := n.GetProperty(propertyKey)
			for i := 0; i < labelCount; i++ {
				bs.adjustNodePropertyStatsOne(n.LabelTokenRawAt(i), propertyKey, valueKey, value, 1)
			}
			return true
		})
	}
	bs.relPropertyStats = make(map[indexpkg.RelPropertyIndexKey]*indexpkg.PropertyStatsAccumulator)
	for rid := range bs.relIDs {
		r, err := bs.getRelLocked(rid)
		if err != nil {
			continue
		}
		tok := r.TypeToken().Value()
		r.ForEachIndexablePropertyValueKey(func(propertyKey, valueKey string) bool {
			value, _ := r.GetProperty(propertyKey)
			bs.adjustRelPropertyStatsOne(tok, propertyKey, valueKey, value, 1)
			return true
		})
	}
	return nil
}

var _ storecontract.ExactErasureCapability = (*Store)(nil)
