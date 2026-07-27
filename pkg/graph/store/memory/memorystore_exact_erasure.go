package memory

import (
	"fmt"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// ExactErase implements store.ExactErasureCapability. The memory backend's
// single write lock is the commit boundary: every preflight completes before
// any map is changed, then the complete teardown is applied without a
// tombstone or change record.
func (ms *Store) ExactErase(req storecontract.ExactErasureRequest) (storecontract.ExactErasureResult, error) {
	var zero storecontract.ExactErasureResult
	if ms == nil {
		return zero, ErrNilStore
	}
	if err := validateExactErasureRequest(req); err != nil {
		return zero, err
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()
	if err := ms.checkOpenLocked(); err != nil {
		return zero, err
	}
	if ms.logEnabled || ms.logSeq != 0 || len(ms.changeLog) != 0 ||
		ms.scopeActive || len(ms.scopeLog) != 0 || len(ms.scopedLogs) != 0 {
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
		for rid := range ms.outIdx[nid] {
			if _, ok := relSet[rid]; !ok {
				return zero, fmt.Errorf("%w: node %d relationship %d",
					storecontract.ErrExactErasureRelationshipEscape, nid, rid)
			}
		}
		for rid := range ms.inIdx[nid] {
			if _, ok := relSet[rid]; !ok {
				return zero, fmt.Errorf("%w: node %d relationship %d",
					storecontract.ErrExactErasureRelationshipEscape, nid, rid)
			}
		}
	}
	// Adjacency is the fast proof, but legal erasure must also fail closed if a
	// corrupt store has lost an adjacency leg while the live relationship row
	// still touches the node. Fold live rows before any mutation.
	for rid, rel := range ms.rels {
		if _, declared := relSet[rid]; declared {
			continue
		}
		_, touchesStart := nodeSet[rel.StartNodeID()]
		_, touchesEnd := nodeSet[rel.EndNodeID()]
		if touchesStart || touchesEnd {
			return zero, fmt.Errorf("%w: node endpoint relationship %d",
				storecontract.ErrExactErasureRelationshipEscape, rid)
		}
	}

	result := storecontract.ExactErasureResult{}
	for _, rid := range req.RelIDs {
		if _, ok := ms.rels[rid]; ok {
			result.RelsRemoved++
		}
		if err := ms.deleteRelOrPurgeOrphanLocked(rid); err != nil {
			return zero, err // no ordinary error remains after preflight
		}
		delete(ms.relHistory, rid)
		delete(ms.relBeliefWatermark, rid)
		indexpkg.PurgeRelFromAllTemporalIndexes(ms.relTypeTemporalIndexes, rid.SnowflakeID())
		for tok, members := range ms.relTypeTxMembers {
			delete(members, rid)
			if len(members) == 0 {
				delete(ms.relTypeTxMembers, tok)
			}
		}
	}

	for _, nid := range req.NodeIDs {
		if n := ms.nodes[nid]; n != nil {
			result.NodesRemoved++
			ms.removeNodeLabelIndexes(nid, n)
			delete(ms.nodes, nid)
		}
		// Purge by ID from every index rather than relying only on the current
		// row, so an idempotent retry also reaps corruption/orphan residue.
		for tok, ids := range ms.labelIdx {
			delete(ids, nid)
			if len(ids) == 0 {
				delete(ms.labelIdx, tok)
			}
		}
		delete(ms.outIdx, nid)
		delete(ms.inIdx, nid)
		raw := nid.SnowflakeID()
		indexpkg.PurgeNodeFromAllPropertyIndexes(ms.propertyIndexes, raw)
		indexpkg.PurgeNodeFromAllCompositeIndexes(ms.compositeIndexes, raw)
		indexpkg.PurgeNodeFromAllTemporalIndexes(ms.temporalIndexes, raw)
		indexpkg.PurgeNodeFromAllHighFrequencyIndexes(ms.hfIndexes, raw)
		indexpkg.PurgeNodeFromAllVectorIndexesExact(ms.vectorIndexes, raw)
		delete(ms.nodeHistory, nid)
		delete(ms.nodeBeliefWatermark, nid)
		for tok, members := range ms.labelTxMembers {
			delete(members, nid)
			if len(members) == 0 {
				delete(ms.labelTxMembers, tok)
			}
		}
	}

	for _, mw := range req.MetaWrites {
		if mw.Value == nil {
			delete(ms.metaKV, mw.Key)
			continue
		}
		ms.metaKV[mw.Key] = append([]byte(nil), mw.Value...)
	}

	// Forget() cannot remove a HyperLogLog contribution and may retain erased
	// min/max values. Rebuild every planner accumulator from surviving live rows
	// so no property value or hash contribution remains resident.
	ms.rebuildPlannerStatsAfterExactErasureLocked()
	ms.docColumns = make(map[uint16]*indexpkg.LabelDocValues)
	ms.docColumnsMulti = make(map[string]*indexpkg.LabelDocValues)
	ms.bumpNodeEpoch()
	ms.bumpRelEpoch()
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

func (ms *Store) rebuildPlannerStatsAfterExactErasureLocked() {
	if ms.disablePlannerStats {
		return
	}
	ms.propertyKeyCounts = make(map[uint16]map[string]int)
	ms.propertyTypeClassCounts = make(map[uint16]map[string]*[types.NumPropertyTypeClasses]int64)
	ms.propertyStats = make(map[uint16]map[string]*indexpkg.PropertyStatsAccumulator)
	ms.relPropertyKeyCounts = make(map[uint16]map[string]int)
	ms.relPropertyTypeClassCounts = make(map[uint16]map[string]*[types.NumPropertyTypeClasses]int64)
	ms.relPropertyStats = make(map[uint16]map[string]*indexpkg.PropertyStatsAccumulator)
	for _, n := range ms.nodes {
		ms.adjustNodePropertyKeyCounts(n, 1)
	}
	for _, r := range ms.rels {
		ms.adjustRelPropertyTypeClassCounts(r, 1)
		ms.adjustRelPropertyKeyCounts(r, 1)
	}
}
