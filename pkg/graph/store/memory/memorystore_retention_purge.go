package memory

import (
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// defaultRetentionPurgeChunk bounds one PurgeNodesByLabelBefore call's work.
const defaultRetentionPurgeChunk = 256

// LogRangePurge appends ONE ChangeRangePurge record (ADR-0008 R3) so a replica
// re-executes the predicate. No-op when the change-log is disabled. See
// store.RangePurgeLogCapability.
func (ms *Store) LogRangePurge(labelToken uint16, before types.Instant, mode uint8) error {
	if ms == nil {
		return ErrNilStore
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if err := ms.checkOpenLocked(); err != nil {
		return err
	}
	if !ms.logEnabled {
		return nil
	}
	payload, err := storeutil.MarshalChangeBody(storeutil.RangePurgeBody{
		LabelToken: labelToken,
		Before:     int64(before),
		Mode:       mode,
	})
	if err != nil {
		return err
	}
	ms.logChangeLocked(storecontract.ChangeRangePurge, payload)
	return nil
}

// PurgeNodesByLabelBefore is the memory mirror of the badger age purge (ADR-0008
// R2 / store.RetentionPurgeCapability). It hard-removes up to `chunk` nodes of a
// label whose IMMUTABLE snowflake mint-time is < before, together with each
// node's connected relationships (both adjacency legs → survivor endpoints stay
// phantom-free), ALL index entries, and the ENTIRE version history of the node
// and each removed relationship. It emits NO change-log record — the graph layer
// owns the single ChangeRangePurge + the retention watermark. The whole call runs
// under one ms.mu.Lock (the memory store has no chunked-batch machinery; the
// chunk bound keeps the critical section short) and is idempotent.
//
// Mirrors DeleteNodeCascade's index-teardown sequence exactly, PLUS history
// removal (delete never erases history; a purge does), and skips the per-entity
// change-log record the cascade would emit.
func (ms *Store) PurgeNodesByLabelBefore(labelToken uint16, before types.Instant, chunk int) (storecontract.RetentionPurgeResult, error) {
	if before <= 0 {
		return storecontract.RetentionPurgeResult{}, nil
	}
	// Mint-time is immutable; the node argument is unused.
	return ms.purgeNodesByLabel(labelToken, chunk, func(nid types.NodeID, _ *types.Node) bool {
		return storeutil.SnowflakeInstant(nid.SnowflakeID()) < before
	})
}

// PurgeNodesByLabelValidToBefore is the memory mirror of the badger ByValidTo purge
// (ADR-0008 R5 / store.RetentionPurgeByValidToCapability): it removes nodes whose
// current-version world-time validity ended before the boundary (ValidTo != 0 &&
// ValidTo < before). Because the memory store selects and removes under ONE lock,
// there is no mutable-predicate race — the selection reads the same live node state
// that is then removed. A node with an open interval (ValidTo == 0) is never purged.
func (ms *Store) PurgeNodesByLabelValidToBefore(labelToken uint16, before types.Instant, chunk int) (storecontract.RetentionPurgeResult, error) {
	if before <= 0 {
		return storecontract.RetentionPurgeResult{}, nil
	}
	return ms.purgeNodesByLabel(labelToken, chunk, func(_ types.NodeID, n *types.Node) bool {
		vt := n.Temporal().ValidTo
		return vt != 0 && vt < before
	})
}

// purgeNodesByLabel is the shared body for both memory purge modes; `qualifies`
// selects victims from the label's live nodes under the store lock.
func (ms *Store) purgeNodesByLabel(labelToken uint16, chunk int, qualifies func(types.NodeID, *types.Node) bool) (storecontract.RetentionPurgeResult, error) {
	var zero storecontract.RetentionPurgeResult
	if ms == nil {
		return zero, ErrNilStore
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()
	defer ms.bumpNodeEpoch()
	defer ms.bumpRelEpoch()

	if err := ms.checkOpenLocked(); err != nil {
		return zero, err
	}
	if chunk <= 0 {
		chunk = defaultRetentionPurgeChunk
	}

	// Select up to `chunk` qualifying victims. Map order is random — fine: the
	// purge is order-independent and `more` just tells the caller to loop; each
	// call removes SOME qualifying subset, so repeated calls drain the label.
	victims := make([]types.NodeID, 0, chunk)
	more := false
	for id := range ms.labelIdx[labelToken] {
		n, ok := ms.nodes[id]
		if !ok || !qualifies(id, n) {
			continue
		}
		if len(victims) >= chunk {
			more = true
			break
		}
		victims = append(victims, id)
	}

	nodesPurged, relsPurged := 0, 0
	purgedIDs := make([]types.NodeID, 0, len(victims))
	for _, nid := range victims {
		n, ok := ms.nodes[nid]
		if !ok {
			continue // concurrently gone
		}
		// Connected rels (dedup self-loops across out+in adjacency).
		relIDs := make(map[types.RelID]struct{})
		for relID := range ms.outIdx[nid] {
			relIDs[relID] = struct{}{}
		}
		for relID := range ms.inIdx[nid] {
			relIDs[relID] = struct{}{}
		}
		for relID := range relIDs {
			if _, live := ms.rels[relID]; live {
				relsPurged++ // count only rows actually removed (parity with badger)
			}
			if err := ms.deleteRelOrPurgeOrphanLocked(relID); err != nil {
				return zero, err
			}
			delete(ms.relHistory, relID) // purge the rel's whole history too
		}

		ms.removeNodeLabelIndexes(nid, n)
		ms.removeNodePropertyKeyCounts(n)
		rawID := nid.SnowflakeID()
		indexpkg.RemoveNodeFromPropertyIndexes(ms.propertyIndexes, n, rawID)
		ms.removeNodeFromCompositeIndexesLocked(n, rawID)
		indexpkg.RemoveNodeFromTemporalIndexes(ms.temporalIndexes, n, rawID)
		indexpkg.RemoveNodeFromHighFrequencyIndexes(ms.hfIndexes, n, rawID)
		indexpkg.RemoveNodeFromVectorIndexes(ms.vectorIndexes, n, rawID)
		delete(ms.nodes, nid)
		delete(ms.nodeHistory, nid) // purge the node's whole history too
		nodesPurged++
		purgedIDs = append(purgedIDs, nid)
	}

	return storecontract.RetentionPurgeResult{
		NodesPurged:   nodesPurged,
		RelsPurged:    relsPurged,
		More:          more,
		PurgedNodeIDs: purgedIDs,
	}, nil
}
