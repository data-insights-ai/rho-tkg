package memory

import (
	"fmt"
	"sort"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// ExactErasureRelationshipClosure resolves every relationship identity whose
// current row or any temporal version touches a declared node.
func (ms *Store) ExactErasureRelationshipClosure(
	req storecontract.ExactErasureClosureRequest,
) (storecontract.ExactErasureClosure, error) {
	var zero storecontract.ExactErasureClosure
	if ms == nil {
		return zero, ErrNilStore
	}
	if err := validateExactErasureClosureRequest(req); err != nil {
		return zero, err
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if err := ms.checkOpenLocked(); err != nil {
		return zero, err
	}
	return ms.exactErasureRelationshipClosureLocked(req.NodeIDs, req.Bounds)
}

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
	if len(req.NodeIDs) != 0 {
		closure, err := ms.exactErasureRelationshipClosureLocked(req.NodeIDs, req.Bounds)
		if err != nil {
			return zero, err
		}
		for _, rid := range closure.RelationshipIDs {
			if _, ok := relSet[rid]; !ok {
				return zero, fmt.Errorf("%w: node history relationship %d",
					storecontract.ErrExactErasureRelationshipEscape, rid)
			}
		}
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
	if len(req.NodeIDs) != 0 {
		if err := validateExactErasureBounds(req.Bounds); err != nil {
			return err
		}
		if len(req.RelIDs) > req.Bounds.MaxRelationshipIdentities {
			return storecontract.ErrExactErasureClosureLimit
		}
	}
	return nil
}

func validateExactErasureClosureRequest(req storecontract.ExactErasureClosureRequest) error {
	if len(req.NodeIDs) == 0 {
		return storecontract.ErrInvalidStoreMutation
	}
	var prev types.NodeID
	for i, id := range req.NodeIDs {
		if err := storecontract.ValidateNodeID(id); err != nil {
			return err
		}
		if i > 0 && id <= prev {
			return storecontract.ErrInvalidStoreMutation
		}
		prev = id
	}
	return validateExactErasureBounds(req.Bounds)
}

func validateExactErasureBounds(bounds storecontract.ExactErasureBounds) error {
	if bounds.MaxRelationshipIdentities <= 0 ||
		bounds.MaxRelationshipVersions <= 0 ||
		bounds.MaxEndpointNodeIdentities <= 0 {
		return storecontract.ErrInvalidStoreMutation
	}
	return nil
}

func (ms *Store) exactErasureRelationshipClosureLocked(
	nodeIDs []types.NodeID,
	bounds storecontract.ExactErasureBounds,
) (storecontract.ExactErasureClosure, error) {
	var zero storecontract.ExactErasureClosure
	nodeSet := make(map[types.NodeID]struct{}, len(nodeIDs))
	for _, id := range nodeIDs {
		nodeSet[id] = struct{}{}
	}
	matches := make(map[types.RelID]struct{})
	endpoints := make(map[types.NodeID]struct{})
	bindings := make(map[storecontract.ExactErasureRelationshipBinding]struct{})
	scanned := 0
	inspect := func(rid types.RelID, rel *types.Relationship) error {
		scanned++
		if scanned > bounds.MaxRelationshipVersions {
			return storecontract.ErrExactErasureClosureLimit
		}
		if rel == nil || rel.ID() != rid {
			return storecontract.ErrInvalidStoreMutation
		}
		_, start := nodeSet[rel.StartNodeID()]
		_, end := nodeSet[rel.EndNodeID()]
		if !start && !end {
			return nil
		}
		matches[rid] = struct{}{}
		if len(matches) > bounds.MaxRelationshipIdentities {
			return storecontract.ErrExactErasureClosureLimit
		}
		endpoints[rel.StartNodeID()] = struct{}{}
		endpoints[rel.EndNodeID()] = struct{}{}
		if len(endpoints) > bounds.MaxEndpointNodeIdentities {
			return storecontract.ErrExactErasureClosureLimit
		}
		integrity := rel.Integrity()
		integrityHash := ""
		if integrity != nil {
			integrityHash = integrity.Hash
		}
		bindings[storecontract.ExactErasureRelationshipBinding{
			RelationshipID: rid,
			TypeToken:      rel.TypeToken().Value(),
			StartNodeID:    rel.StartNodeID(),
			EndNodeID:      rel.EndNodeID(),
			Version:        rel.Version(),
			IntegrityHash:  integrityHash,
		}] = struct{}{}
		return nil
	}
	for rid, rel := range ms.rels {
		if err := inspect(rid, rel); err != nil {
			return zero, err
		}
	}
	for rid, versions := range ms.relHistory {
		for _, rel := range versions {
			if err := inspect(rid, rel); err != nil {
				return zero, err
			}
		}
	}
	result := storecontract.ExactErasureClosure{
		RelationshipIDs: make([]types.RelID, 0, len(matches)),
		EndpointNodeIDs: make([]types.NodeID, 0, len(endpoints)),
		Bindings:        make([]storecontract.ExactErasureRelationshipBinding, 0, len(bindings)),
	}
	for rid := range matches {
		result.RelationshipIDs = append(result.RelationshipIDs, rid)
	}
	for endpoint := range endpoints {
		result.EndpointNodeIDs = append(result.EndpointNodeIDs, endpoint)
	}
	for binding := range bindings {
		result.Bindings = append(result.Bindings, binding)
	}
	sort.Slice(result.RelationshipIDs, func(i, j int) bool {
		return result.RelationshipIDs[i] < result.RelationshipIDs[j]
	})
	sort.Slice(result.EndpointNodeIDs, func(i, j int) bool {
		return result.EndpointNodeIDs[i] < result.EndpointNodeIDs[j]
	})
	sort.Slice(result.Bindings, func(i, j int) bool {
		return exactErasureBindingLess(result.Bindings[i], result.Bindings[j])
	})
	return result, nil
}

func exactErasureBindingLess(
	left, right storecontract.ExactErasureRelationshipBinding,
) bool {
	if left.RelationshipID != right.RelationshipID {
		return left.RelationshipID < right.RelationshipID
	}
	if left.Version != right.Version {
		return left.Version < right.Version
	}
	if left.TypeToken != right.TypeToken {
		return left.TypeToken < right.TypeToken
	}
	if left.StartNodeID != right.StartNodeID {
		return left.StartNodeID < right.StartNodeID
	}
	if left.EndNodeID != right.EndNodeID {
		return left.EndNodeID < right.EndNodeID
	}
	return left.IntegrityHash < right.IntegrityHash
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
