package sharded

import (
	"fmt"

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
// Relationships are collected by folding adjacency across all shards, deleted on
// their own shards, then the (now unconnected) node row is removed.
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
	for _, rid := range relIDs {
		relShard, rerr := s.shardForRelID(rid)
		if rerr != nil {
			return rerr
		}
		if derr := relShard.DeleteRelationship(rid); derr != nil {
			// Tolerate an already-removed rel (self-loop counted once, or a
			// concurrent delete): the node delete is what matters.
			if !isRelNotFound(derr) {
				return derr
			}
		}
	}
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
