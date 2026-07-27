package badger

import (
	"encoding/binary"
	"fmt"
	"sort"

	snowflake "github.com/bds421/rho-snowflake-2026"
	badgerv4 "github.com/dgraph-io/badger/v4"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// ExactErasureRelationshipClosure resolves every relationship identity whose
// current row or any temporal version touches a declared node. It takes the
// same backend exclusion as ExactErase so the returned set is one coherent
// planning snapshot; ExactErase still recomputes it at the destructive door.
func (bs *Store) ExactErasureRelationshipClosure(
	req storecontract.ExactErasureClosureRequest,
) (storecontract.ExactErasureClosure, error) {
	var zero storecontract.ExactErasureClosure
	if err := bs.checkWritable(); err != nil {
		return zero, err
	}
	if err := validateExactErasureClosureRequest(req); err != nil {
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
	return bs.exactErasureRelationshipClosureLocked(req.NodeIDs, req.Bounds)
}

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
	if len(req.NodeIDs) != 0 {
		closure, closureErr := bs.exactErasureRelationshipClosureLocked(req.NodeIDs, req.Bounds)
		if closureErr != nil {
			return zero, closureErr
		}
		for _, rid := range closure.RelationshipIDs {
			if _, ok := relSet[rid]; !ok {
				return zero, fmt.Errorf("%w: node history relationship %d",
					storecontract.ErrExactErasureRelationshipEscape, rid)
			}
		}
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

	// Build the entire immutable destructive plan before the first mutation.
	// Every Badger read/prefix scan lives in this phase. Once it succeeds, apply
	// only mutates RAM and appends already-resolved delete keys; the sole
	// remaining error door is the final durable WriteBatch commit.
	plan, err := bs.buildExactErasurePlanLocked(req)
	if err != nil {
		return zero, err
	}
	result := bs.applyExactErasurePlanLocked(plan)

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
	bs.rebuildExactErasurePlannerSketchesLocked()
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

type exactErasurePlan struct {
	historyOps    []writeOp
	relationships []exactErasureRelPlan
	nodes         []exactErasureNodePlan
}

type exactErasureRelPlan struct {
	id         types.RelID
	live       bool
	current    RelDeleteInfo
	hasCurrent bool
	indexKeys  [][]byte
}

type exactErasureNodePlan struct {
	id          types.NodeID
	live        bool
	current     *types.Node
	labelTokens []uint16
	residueOps  []writeOp
}

func (bs *Store) exactErasureScanTest(stage string, id uint64) error {
	if bs.exactErasureScanTestHook == nil {
		return nil
	}
	if err := bs.exactErasureScanTestHook(stage, id); err != nil {
		return fmt.Errorf("graph: exact erasure preflight %s: %w", stage, err)
	}
	return nil
}

// buildExactErasurePlanLocked resolves every fallible read and prefix scan
// while flushMu + idxMu exclude both direct writers and the background
// flusher. The returned keys and entity snapshots are owned by the plan and
// remain valid through apply.
func (bs *Store) buildExactErasurePlanLocked(
	req storecontract.ExactErasureRequest,
) (exactErasurePlan, error) {
	plan := exactErasurePlan{
		relationships: make([]exactErasureRelPlan, 0, len(req.RelIDs)),
		nodes:         make([]exactErasureNodePlan, 0, len(req.NodeIDs)),
	}

	for _, nid := range req.NodeIDs {
		if err := bs.exactErasureScanTest("node-history", uint64(nid.SnowflakeID())); err != nil {
			return exactErasurePlan{}, err
		}
		keys, _, err := bs.historyTruncateDeleteKeys(storepkg.HistNodePrefix(nid.SnowflakeID()), 0)
		if err != nil {
			return exactErasurePlan{}, err
		}
		for _, key := range keys {
			plan.historyOps = append(plan.historyOps, writeOp{
				opType: writeOpDelete,
				key:    []byte(key),
			})
		}
	}
	for _, rid := range req.RelIDs {
		if err := bs.exactErasureScanTest("relationship-history", uint64(rid.SnowflakeID())); err != nil {
			return exactErasurePlan{}, err
		}
		keys, _, err := bs.historyTruncateDeleteKeys(storepkg.HistRelPrefix(rid.SnowflakeID()), 0)
		if err != nil {
			return exactErasurePlan{}, err
		}
		for _, key := range keys {
			plan.historyOps = append(plan.historyOps, writeOp{
				opType: writeOpDelete,
				key:    []byte(key),
			})
		}
	}

	for _, rid := range req.RelIDs {
		relPlan := exactErasureRelPlan{id: rid}
		_, relPlan.live = bs.relIDs[rid]
		if relPlan.live {
			if err := bs.exactErasureScanTest("relationship-current", uint64(rid.SnowflakeID())); err != nil {
				return exactErasurePlan{}, err
			}
			if rel, err := bs.getRelLocked(rid); err == nil {
				relPlan.current = relDeleteInfoFromRelationship(rel)
				relPlan.hasCurrent = true
			}
		}
		if err := bs.exactErasureScanTest("relationship-index", uint64(rid.SnowflakeID())); err != nil {
			return exactErasurePlan{}, err
		}
		indexKeys, err := bs.relationshipIndexKeysForRel(rid.SnowflakeID())
		if err != nil {
			return exactErasurePlan{}, err
		}
		relPlan.indexKeys = indexKeys
		plan.relationships = append(plan.relationships, relPlan)
	}

	for _, nid := range req.NodeIDs {
		nodePlan := exactErasureNodePlan{id: nid}
		_, nodePlan.live = bs.nodeIDs[nid]
		if nodePlan.live {
			if err := bs.exactErasureScanTest("node-current", uint64(nid.SnowflakeID())); err != nil {
				return exactErasurePlan{}, err
			}
			if node, err := bs.getNodeLocked(nid); err == nil {
				nodePlan.current = node.DeepCopy()
			}
		}

		rawID := nid.SnowflakeID()
		nodePlan.residueOps = append(nodePlan.residueOps, writeOp{
			opType: writeOpDelete,
			key:    storepkg.NodeKey(rawID),
		})
		if err := bs.exactErasureScanTest("node-label-index", uint64(rawID)); err != nil {
			return exactErasurePlan{}, err
		}
		tokens, err := bs.nodeLabelTokensFromKeyspaceLocked(nid)
		if err != nil {
			return exactErasurePlan{}, err
		}
		sort.Slice(tokens, func(i, j int) bool { return tokens[i] < tokens[j] })
		nodePlan.labelTokens = tokens
		for _, token := range tokens {
			nodePlan.residueOps = append(nodePlan.residueOps, writeOp{
				opType: writeOpDelete,
				key:    storepkg.LabelIndexKey(token, rawID),
			})
		}

		if err := bs.exactErasureScanTest("node-property-index", uint64(rawID)); err != nil {
			return exactErasurePlan{}, err
		}
		propertyOps, err := bs.exactErasurePropertyIndexOpsLocked(rawID)
		if err != nil {
			return exactErasurePlan{}, err
		}
		nodePlan.residueOps = append(nodePlan.residueOps, propertyOps...)
		if err := bs.exactErasureScanTest("node-temporal-index", uint64(rawID)); err != nil {
			return exactErasurePlan{}, err
		}
		temporalOps, err := bs.exactErasureTemporalIndexOpsLocked(rawID)
		if err != nil {
			return exactErasurePlan{}, err
		}
		nodePlan.residueOps = append(nodePlan.residueOps, temporalOps...)
		plan.nodes = append(plan.nodes, nodePlan)
	}
	return plan, nil
}

func (bs *Store) exactErasurePropertyIndexOpsLocked(id snowflake.ID) ([]writeOp, error) {
	ops := make([]writeOp, 0)
	bs.rangePending(func(k string, op writeOp) {
		key := []byte(k)
		if len(key) < 8 || key[0] != storepkg.KeyPropertyIndex || op.opType != writeOpSet {
			return
		}
		if storepkg.PropertyIndexNodeIDFromKey(key) == id {
			ops = append(ops, writeOp{opType: writeOpDelete, key: key})
		}
	})
	err := bs.db.View(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		prefix := []byte{storepkg.KeyPropertyIndex}
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			key := it.Item().KeyCopy(nil)
			if len(key) >= 8 && storepkg.PropertyIndexNodeIDFromKey(key) == id {
				ops = append(ops, writeOp{opType: writeOpDelete, key: key})
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("graph: exact erasure property-index scan: %w", err)
	}
	return ops, nil
}

func (bs *Store) exactErasureTemporalIndexOpsLocked(id snowflake.ID) ([]writeOp, error) {
	ops := make([]writeOp, 0)
	bs.rangePending(func(k string, op writeOp) {
		key := []byte(k)
		if len(key) != storepkg.SizeTemporalIndexEntryKey ||
			key[0] != storepkg.KeyTemporalIndex ||
			op.opType != writeOpSet {
			return
		}
		if storepkg.TemporalIndexNodeIDFromKey(key) == id {
			ops = append(ops, writeOp{opType: writeOpDelete, key: key})
		}
	})
	err := bs.db.View(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		prefix := []byte{storepkg.KeyTemporalIndex}
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			key := it.Item().KeyCopy(nil)
			if len(key) == storepkg.SizeTemporalIndexEntryKey &&
				storepkg.TemporalIndexNodeIDFromKey(key) == id {
				ops = append(ops, writeOp{opType: writeOpDelete, key: key})
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("graph: exact erasure temporal-index scan: %w", err)
	}
	return ops, nil
}

func (bs *Store) applyExactErasurePlanLocked(plan exactErasurePlan) storecontract.ExactErasureResult {
	result := storecontract.ExactErasureResult{}
	for _, relPlan := range plan.relationships {
		if relPlan.live {
			result.RelsRemoved++
			if relPlan.hasCurrent {
				bs.deleteRelByInfo(relPlan.current)
			} else {
				bs.appendOps(writeOp{
					opType: writeOpDelete,
					key:    storepkg.RelKey(relPlan.id.SnowflakeID()),
				})
			}
		}
		bs.purgeOrphanRelIDLockedWithIndexKeys(relPlan.id, relPlan.indexKeys)
		bs.purgeExactRelSidecarsLocked(relPlan.id)
	}

	for _, nodePlan := range plan.nodes {
		if nodePlan.live {
			result.NodesRemoved++
			if nodePlan.current != nil {
				bs.applyExactCurrentNodeLocked(nodePlan.id, nodePlan.current)
			} else {
				bs.applyExactCorruptCurrentNodeLocked(nodePlan)
			}
		}
		bs.applyExactNodeResidueLocked(nodePlan)
		bs.purgeExactNodeSidecarsLocked(nodePlan.id)
	}
	bs.appendOps(plan.historyOps...)
	return result
}

func (bs *Store) applyExactCurrentNodeLocked(nid types.NodeID, node *types.Node) {
	rawID := nid.SnowflakeID()
	ops := []writeOp{{opType: writeOpDelete, key: storepkg.NodeKey(rawID)}}
	for _, token := range collectNodeLabelTokens(node) {
		ops = append(ops, writeOp{
			opType: writeOpDelete,
			key:    storepkg.LabelIndexKey(token, rawID),
		})
		if !bs.labelOnDisk {
			if members := bs.labelIdx[token]; members != nil {
				delete(members, nid)
				if len(members) == 0 {
					delete(bs.labelIdx, token)
				}
			}
		}
		bs.getOrCreateLabelCounter(token).Add(-1)
	}
	bs.removeNodePropertyKeyCounts(node)
	ops = append(ops, bs.maintainPropertyIndexesRemove(node, rawID)...)
	indexpkg.RemoveNodeFromTemporalIndexes(bs.temporalIndexes, node, rawID)
	ops = append(ops, bs.maintainTemporalIndexDiskRemove(node, rawID)...)
	indexpkg.RemoveNodeFromHighFrequencyIndexes(bs.hfIndexes, node, rawID)
	indexpkg.RemoveNodeFromVectorIndexes(bs.vectorIndexes, node, rawID)
	bs.nodeCache.MarkDeleted(rawID)
	delete(bs.nodeIDs, nid)
	delete(bs.nodeHashes, nid)
	bs.deleteNodeRevLocked(nid)
	bs.nodeCount.Add(-1)
	bs.appendOps(ops...)
}

func (bs *Store) applyExactCorruptCurrentNodeLocked(plan exactErasureNodePlan) {
	if bs.labelOnDisk {
		for _, token := range plan.labelTokens {
			bs.getOrCreateLabelCounter(token).Add(-1)
		}
	} else {
		for token, members := range bs.labelIdx {
			if _, present := members[plan.id]; !present {
				continue
			}
			delete(members, plan.id)
			if len(members) == 0 {
				delete(bs.labelIdx, token)
			}
			bs.getOrCreateLabelCounter(token).Add(-1)
		}
	}
	bs.nodeCount.Add(-1)
}

func (bs *Store) applyExactNodeResidueLocked(plan exactErasureNodePlan) {
	rawID := plan.id.SnowflakeID()
	if !bs.labelOnDisk {
		for token, members := range bs.labelIdx {
			delete(members, plan.id)
			if len(members) == 0 {
				delete(bs.labelIdx, token)
			}
		}
	}
	indexpkg.PurgeNodeFromAllCompositeIndexes(bs.compositeIndexes, rawID)
	if !bs.propIdxOnDisk {
		indexpkg.PurgeNodeFromAllPropertyIndexes(bs.propertyIndexes, rawID)
	}
	indexpkg.PurgeNodeFromAllTemporalIndexes(bs.temporalIndexes, rawID)
	indexpkg.PurgeNodeFromAllHighFrequencyIndexes(bs.hfIndexes, rawID)
	indexpkg.PurgeNodeFromAllVectorIndexesExact(bs.vectorIndexes, rawID)
	bs.nodeCache.MarkDeleted(rawID)
	delete(bs.nodeIDs, plan.id)
	delete(bs.nodeHashes, plan.id)
	bs.deleteNodeRevLocked(plan.id)
	bs.appendOps(plan.residueOps...)
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

func (bs *Store) exactErasureRelationshipClosureLocked(
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

	for rid := range bs.relIDs {
		rel, err := bs.getRelLocked(rid)
		if err != nil {
			return zero, fmt.Errorf("graph: exact erasure closure current relationship %d: %w", rid, err)
		}
		if err = inspect(rid, rel); err != nil {
			return zero, err
		}
	}

	overlayEntries, overlayDeletes := bs.pendingHistoryVersionOverlay(
		[]byte{storepkg.KeyHistRel}, 0,
	)
	inspectHistory := func(key, raw []byte) error {
		if len(key) != storepkg.SizeHistKey {
			return fmt.Errorf("%w: malformed relationship history key", storecontract.ErrCorruptWire)
		}
		rid := types.RelID(storepkg.ParseIDFromKey(key, 1))
		version := binary.BigEndian.Uint64(key[9:])
		rel, decodeErr := bs.historyRelTemporal(rid.SnowflakeID(), version, raw)
		if decodeErr != nil {
			return fmt.Errorf("graph: exact erasure closure relationship %d version %d: %w",
				rid, version, decodeErr)
		}
		return inspect(rid, rel)
	}
	err := bs.db.View(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = true
		it := txn.NewIterator(opts)
		defer it.Close()
		prefix := []byte{storepkg.KeyHistRel}
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := item.Key()
			if len(key) != storepkg.SizeHistKey {
				return fmt.Errorf("%w: malformed relationship history key", storecontract.ErrCorruptWire)
			}
			keyString := string(key)
			if _, deleted := overlayDeletes[keyString]; deleted {
				continue
			}
			raw, overlaid := overlayEntries[keyString]
			if overlaid {
				delete(overlayEntries, keyString)
			} else {
				var valueErr error
				raw, valueErr = item.ValueCopy(nil)
				if valueErr != nil {
					return valueErr
				}
			}
			if inspectErr := inspectHistory(key, raw); inspectErr != nil {
				return inspectErr
			}
		}
		return nil
	})
	if err != nil {
		return zero, err
	}
	for key, raw := range overlayEntries {
		if err = inspectHistory([]byte(key), raw); err != nil {
			return zero, err
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

func (bs *Store) rebuildExactErasurePlannerSketchesLocked() {
	if bs.disablePlannerStats {
		return
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
}

var _ storecontract.ExactErasureCapability = (*Store)(nil)
