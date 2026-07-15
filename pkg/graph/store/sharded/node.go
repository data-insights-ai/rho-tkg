package sharded

import (
	"fmt"
	"sort"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// --- Node CRUD (point ops route by the node's slot) ---

func (s *Store) PutNode(n *types.Node) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeWrite(n); err != nil {
		return err
	}
	shard, err := s.shardForNodeID(n.InternalID())
	if err != nil {
		return err
	}
	return shard.PutNode(n)
}

func (s *Store) GetNode(id types.NodeID) (*types.Node, error) {
	if err := s.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateNodeID(id); err != nil {
		return nil, err
	}
	shard, err := s.shardForNodeID(id)
	if err != nil {
		return nil, err
	}
	return shard.GetNode(id)
}

func (s *Store) ReplaceNode(n *types.Node) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeWrite(n); err != nil {
		return err
	}
	shard, err := s.shardForNodeID(n.InternalID())
	if err != nil {
		return err
	}
	return shard.ReplaceNode(n)
}

// DeleteNode removes an UNCONNECTED node row. Connectivity is checked across ALL
// shards (a node's relationships live on the rel's slot, which may differ from
// the node's slot), so a node with any cross-shard edge is rejected.
func (s *Store) DeleteNode(id types.NodeID) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeID(id); err != nil {
		return err
	}
	shard, err := s.shardForNodeID(id)
	if err != nil {
		return err
	}
	connected, err := s.nodeConnectedAnyShard(id)
	if err != nil {
		return err
	}
	if connected {
		return fmt.Errorf("%w: node %d has connected relationships", ErrInvalidStoreMutation, id)
	}
	return shard.DeleteNode(id)
}

// DeleteNodeCascade removes the node and every relationship it participates in.
//
// ADR-0007 Risk 1 — a node and its rels may span shards and there is NO
// cross-shard WriteBatch. The cascade is therefore crash-recoverable rather
// than crash-atomic, and the ordering is the recovery contract:
//
//	(a) fold-collect ALL connected rel IDs across every shard (out ∪ in,
//	    self-loops deduped to a single delete);
//	(b) delete each rel on ITS OWN shard, in DETERMINISTIC ascending rel-ID
//	    order — each per-rel delete is a single-shard single-WriteBatch atomic
//	    operation, so a crash stops at a rel boundary that is reproducible
//	    (same order every run) and repair is decidable;
//	(c) delete the NODE row LAST. Because rels are removed first, a crash
//	    mid-cascade leaves DANGLING RELS (some already gone, some still live)
//	    but NEVER a dangling node with ghost edges: the node row is the last
//	    thing standing, so recovery/VerifyConsistency always finds a live node
//	    to re-drive the cascade from — never a rel pointing at a vanished node;
//	(d) an adjacency entry whose rel row is already gone (a torn prior run or
//	    index corruption) is purged so the final node delete does not see a
//	    phantom connection.
//
// The core layer holds entity locks over the whole neighborhood above this
// door, so it may rely on no CONCURRENT mutation of the same neighborhood — but
// NOT on crash atomicity, which this ordering + the verify door provide instead.
func (s *Store) DeleteNodeCascade(id types.NodeID) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeID(id); err != nil {
		return err
	}
	nodeShard, err := s.shardForNodeID(id)
	if err != nil {
		return err
	}
	if _, err := nodeShard.GetNode(id); err != nil {
		return err // ErrNodeNotFound if absent
	}

	relIDs, err := s.adjacentRelIDsAnyShard(id)
	if err != nil {
		return err
	}
	// (b) Deterministic ascending rel-ID order: a crash always stops at the same
	// boundary, so a partial cascade is reproducible and repair is decidable.
	sort.Slice(relIDs, func(i, j int) bool {
		return relIDs[i].SnowflakeID() < relIDs[j].SnowflakeID()
	})
	for _, rid := range relIDs {
		relShard, rerr := s.shardForRelID(rid)
		if rerr != nil {
			return rerr
		}
		derr := relShard.DeleteRelationship(rid)
		if derr == nil {
			continue
		}
		if !isRelNotFound(derr) {
			return derr
		}
		// (d) The adjacency fold pointed at a rel whose row is already gone —
		// a torn prior cascade or an index orphan. Purge the stale index
		// entries so the final DeleteNode does not reject on a phantom edge.
		if perr := relShard.PurgeOrphanRelationshipIndexes(rid); perr != nil {
			return perr
		}
	}
	// (c) Node row deleted LAST.
	return nodeShard.DeleteNode(id)
}

func (s *Store) RemoveNodeLabelToken(id types.NodeID, tok uint16, updatedNode *types.Node) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	shard, err := s.shardForNodeID(id)
	if err != nil {
		return err
	}
	return shard.RemoveNodeLabelToken(id, tok, updatedNode)
}

func (s *Store) AddNodeLabelToken(id types.NodeID, tok uint16, updatedNode *types.Node) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	shard, err := s.shardForNodeID(id)
	if err != nil {
		return err
	}
	return shard.AddNodeLabelToken(id, tok, updatedNode)
}

// --- Cross-shard connectivity helpers ---

// nodeConnectedAnyShard reports whether the node has any outgoing OR incoming
// adjacency entry on ANY shard.
func (s *Store) nodeConnectedAnyShard(id types.NodeID) (bool, error) {
	sid := id.SnowflakeID()
	var mu = make([]bool, len(s.shards))
	err := s.forEachShardErr(func(idx int, shard *badgerShard) error {
		if len(shard.OutgoingRelIDs(sid)) > 0 || len(shard.IncomingRelIDs(sid, 0)) > 0 {
			mu[idx] = true
		}
		return nil
	})
	if err != nil {
		return false, err
	}
	for _, c := range mu {
		if c {
			return true, nil
		}
	}
	return false, nil
}

// adjacentRelIDsAnyShard collects the deduplicated set of relationship IDs
// (outgoing ∪ incoming) touching the node across every shard.
func (s *Store) adjacentRelIDsAnyShard(id types.NodeID) ([]types.RelID, error) {
	sid := id.SnowflakeID()
	per := make([][]types.RelID, len(s.shards))
	err := s.forEachShardErr(func(idx int, shard *badgerShard) error {
		seen := make(map[types.RelID]struct{})
		var ids []types.RelID
		for _, rawID := range shard.OutgoingRelIDs(sid) {
			rid := types.RelID(rawID)
			if _, ok := seen[rid]; !ok {
				seen[rid] = struct{}{}
				ids = append(ids, rid)
			}
		}
		for _, rawID := range shard.IncomingRelIDs(sid, 0) {
			rid := types.RelID(rawID)
			if _, ok := seen[rid]; !ok {
				seen[rid] = struct{}{}
				ids = append(ids, rid)
			}
		}
		per[idx] = ids
		return nil
	})
	if err != nil {
		return nil, err
	}
	// Deduplicate across shards (a rel's out and in adjacency both live on the
	// rel's shard, so cross-shard dup is impossible today, but be defensive).
	seen := make(map[types.RelID]struct{})
	var out []types.RelID
	for _, ids := range per {
		for _, rid := range ids {
			if _, ok := seen[rid]; !ok {
				seen[rid] = struct{}{}
				out = append(out, rid)
			}
		}
	}
	return out, nil
}
