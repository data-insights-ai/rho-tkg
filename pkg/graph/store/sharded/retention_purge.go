package sharded

import (
	"sync"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var (
	_ storecontract.RetentionPurgeCapability          = (*Store)(nil)
	_ storecontract.RetentionPurgeByValidToCapability = (*Store)(nil)
	_ storecontract.RangePurgeLogCapability           = (*Store)(nil)
)

// PurgeNodesByLabelBefore hard-removes aged-out nodes of a label across every shard
// (ADR-0008 R4, ByAge). See purgeNodesFanOut for the two-phase mechanism.
func (s *Store) PurgeNodesByLabelBefore(labelToken uint16, before types.Instant, chunk int) (storecontract.RetentionPurgeResult, error) {
	return s.purgeNodesFanOut(func(shard *badger.Store) (storecontract.RetentionPurgeResult, error) {
		return shard.PurgeNodesByLabelBefore(labelToken, before, chunk)
	})
}

// PurgeNodesByLabelValidToBefore hard-removes nodes whose world-time validity ended
// before the boundary across every shard (ADR-0008 R5, ByValidTo). Same two-phase
// mechanism as ByAge (see purgeNodesFanOut) — only the per-shard predicate differs;
// each shard applies its own mutable-predicate re-confirm under its write lock.
func (s *Store) PurgeNodesByLabelValidToBefore(labelToken uint16, before types.Instant, chunk int) (storecontract.RetentionPurgeResult, error) {
	return s.purgeNodesFanOut(func(shard *badger.Store) (storecontract.RetentionPurgeResult, error) {
		return shard.PurgeNodesByLabelValidToBefore(labelToken, before, chunk)
	})
}

// purgeNodesFanOut is the shared two-phase cross-shard purge (ADR-0008 R4):
//
//	(1) Fan out `perShard` (parallel) — each shard removes its own qualifying nodes,
//	    their CO-LOCATED edges (rels minted in the node's slot, ADR-0007 §2, incl.
//	    the survivor inIdx cleanup), and all their history.
//	(2) Cross-shard sweep — for each purged node, remove any edge MINTED IN ANOTHER
//	    node's slot that points at it. Such an edge lives on a shard OTHER than the
//	    purged node's, so phase 1 cannot see it; left behind it would dangle
//	    (a phantom in the survivor's adjacency fold). This is the one edge class a
//	    per-shard label purge misses (an event-as-END cross-shard edge).
//
// Each shard batch is atomic; the whole operation is crash-recoverable-not-atomic
// (a re-run finishes, and the graph watermark makes any residue invisible), the
// same contract as the cross-shard cascade. Emits no per-entity record — the graph
// layer owns the single ChangeRangePurge.
func (s *Store) purgeNodesFanOut(perShard func(shard *badger.Store) (storecontract.RetentionPurgeResult, error)) (storecontract.RetentionPurgeResult, error) {
	var zero storecontract.RetentionPurgeResult
	if err := s.checkOpen(); err != nil {
		return zero, err
	}

	var mu sync.Mutex
	total := storecontract.RetentionPurgeResult{}
	var purgedIDs []types.NodeID
	ferr := s.forEachShardErr(func(_ int, shard *badger.Store) error {
		res, e := perShard(shard)
		if e != nil {
			return e
		}
		mu.Lock()
		total.NodesPurged += res.NodesPurged
		total.RelsPurged += res.RelsPurged
		if res.More {
			total.More = true
		}
		purgedIDs = append(purgedIDs, res.PurgedNodeIDs...)
		mu.Unlock()
		return nil
	})
	if ferr != nil {
		return total, ferr
	}

	// Phase 2: cross-shard edge sweep for every purged node.
	for _, pid := range purgedIDs {
		ownerIdx, oerr := s.shardIndexForNode(pid)
		if oerr != nil {
			return total, oerr
		}
		for idx, shard := range s.shards {
			if idx == ownerIdx {
				continue // co-located edges were removed by phase 1
			}
			n, e := shard.PurgeAdjacentRelsForNode(pid)
			if e != nil {
				return total, e
			}
			total.RelsPurged += n
		}
	}
	// Surface the purged IDs so the graph layer can reap UniqueForever owners.
	total.PurgedNodeIDs = purgedIDs
	return total, nil
}

// LogRangePurge appends ONE store-global ChangeRangePurge record on the anchor
// shard (ADR-0008 R4/R3) so a sharded replica re-executes the predicate across
// all of its own shards. No-op when the change-log is disabled.
func (s *Store) LogRangePurge(labelToken uint16, before types.Instant, mode uint8) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	return s.anchor().LogRangePurge(labelToken, before, mode)
}
