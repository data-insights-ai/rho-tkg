package sharded

import (
	"errors"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// --- Version history (point ops route by entity slot) ---

func (s *Store) ReplaceNodeWithHistory(current *types.Node, prevVersion uint32, prevState *types.Node) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	shard, err := s.shardForNodeID(current.InternalID())
	if err != nil {
		return err
	}
	return shard.ReplaceNodeWithHistory(current, prevVersion, prevState)
}

func (s *Store) ReplaceRelWithHistory(current *types.Relationship, prevVersion uint32, prevState *types.Relationship) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	shard, err := s.shardForRelID(current.InternalID())
	if err != nil {
		return err
	}
	return shard.ReplaceRelWithHistory(current, prevVersion, prevState)
}

func (s *Store) PutNodeVersion(id types.NodeID, version uint32, n *types.Node) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	shard, err := s.shardForNodeID(id)
	if err != nil {
		return err
	}
	return shard.PutNodeVersion(id, version, n)
}

func (s *Store) GetNodeVersion(id types.NodeID, version uint32) (*types.Node, error) {
	if err := s.checkOpen(); err != nil {
		return nil, err
	}
	shard, err := s.shardForNodeID(id)
	if err != nil {
		return nil, err
	}
	return shard.GetNodeVersion(id, version)
}

func (s *Store) GetNodeHistory(id types.NodeID) ([]*types.Node, error) {
	if err := s.checkOpen(); err != nil {
		return nil, err
	}
	shard, err := s.shardForNodeID(id)
	if err != nil {
		return nil, err
	}
	return shard.GetNodeHistory(id)
}

func (s *Store) TruncateNodeHistory(id types.NodeID, keepVersions int) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	shard, err := s.shardForNodeID(id)
	if err != nil {
		return err
	}
	return shard.TruncateNodeHistory(id, keepVersions)
}

func (s *Store) PutRelVersion(id types.RelID, version uint32, r *types.Relationship) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	shard, err := s.shardForRelID(id)
	if err != nil {
		return err
	}
	return shard.PutRelVersion(id, version, r)
}

func (s *Store) GetRelVersion(id types.RelID, version uint32) (*types.Relationship, error) {
	if err := s.checkOpen(); err != nil {
		return nil, err
	}
	shard, err := s.shardForRelID(id)
	if err != nil {
		return nil, err
	}
	return shard.GetRelVersion(id, version)
}

func (s *Store) GetRelHistory(id types.RelID) ([]*types.Relationship, error) {
	if err := s.checkOpen(); err != nil {
		return nil, err
	}
	shard, err := s.shardForRelID(id)
	if err != nil {
		return nil, err
	}
	return shard.GetRelHistory(id)
}

func (s *Store) TruncateRelHistory(id types.RelID, keepVersions int) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	shard, err := s.shardForRelID(id)
	if err != nil {
		return err
	}
	return shard.TruncateRelHistory(id, keepVersions)
}

func (s *Store) RemoveNodeLabelTokenWithHistory(id types.NodeID, tok uint16, updatedNode *types.Node,
	prevVersion uint32, prevState *types.Node) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	shard, err := s.shardForNodeID(id)
	if err != nil {
		return err
	}
	return shard.RemoveNodeLabelTokenWithHistory(id, tok, updatedNode, prevVersion, prevState)
}

func (s *Store) AddNodeLabelTokenWithHistory(id types.NodeID, tok uint16, updatedNode *types.Node,
	prevVersion uint32, prevState *types.Node) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	shard, err := s.shardForNodeID(id)
	if err != nil {
		return err
	}
	return shard.AddNodeLabelTokenWithHistory(id, tok, updatedNode, prevVersion, prevState)
}

// DeleteNodeWithHistory writes the node tombstone on the node's shard and each
// relationship tombstone on the relationship's OWN shard. Relationships that
// happen to be co-located with the node go through the node-shard's atomic
// DeleteNodeWithHistory call (which validates them against local adjacency);
// cross-shard relationship tombstones are written per rel shard via
// DeleteRelWithHistory. Cross-shard atomicity is not available (ADR-0007 Risk 1);
// entity locks in the graph layer serialize the neighborhood.
func (s *Store) DeleteNodeWithHistory(id types.NodeID, prevNodeVersion uint32, nodeTombstone *types.Node, relTombstones []RelTombstone) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	nodeShard, err := s.shardForNodeID(id)
	if err != nil {
		return err
	}
	nodeIdx, err := s.shardIndexForNode(id)
	if err != nil {
		return err
	}

	var local []RelTombstone
	var remote []RelTombstone
	for _, rt := range relTombstones {
		idx, rerr := s.shardIndexForRel(rt.ID)
		if rerr != nil {
			if !errors.Is(rerr, ErrSlotNotLocal) {
				return rerr
			}
			// A rel tombstone whose rel-ID slot is FOREIGN is a Model-A incoming
			// half-edge stub (ADR-0010 §3.3): physically co-located on the END
			// node's (== this node's) shard, adjacency-only, with no version chain
			// to tombstone. Remove it now — BEFORE the node's own with-history
			// delete — so nodeShard.DeleteNodeWithHistory's tombstone validation and
			// cascade sweep see a stub-free adjacency. It replicates via a dedicated
			// ChangeForeignIncomingDelete (routed by END slot), never a rel-slot
			// ChangeRelDelete that a replica cannot route. Idempotent.
			if derr := nodeShard.DeleteRelationshipForeignIncoming(rt.ID); derr != nil && !isRelNotFound(derr) {
				return derr
			}
			continue
		}
		if idx == nodeIdx {
			local = append(local, rt)
		} else {
			remote = append(remote, rt)
		}
	}

	// Cross-shard rel tombstones first (each atomic on its own shard).
	for _, rt := range remote {
		relShard, rerr := s.shardForRelID(rt.ID)
		if rerr != nil {
			return rerr
		}
		if derr := relShard.DeleteRelWithHistory(rt.ID, rt.PrevVersion, rt.Tombstone); derr != nil {
			return derr
		}
	}

	// Node + co-located rel tombstones in one atomic call on the node's shard.
	return nodeShard.DeleteNodeWithHistory(id, prevNodeVersion, nodeTombstone, local)
}

func (s *Store) DeleteRelWithHistory(id types.RelID, prevVersion uint32, tombstone *types.Relationship) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	if err := storecontract.ValidateRelID(id); err != nil {
		return err
	}
	shard, err := s.shardForRelID(id)
	if err != nil {
		return err
	}
	return shard.DeleteRelWithHistory(id, prevVersion, tombstone)
}
