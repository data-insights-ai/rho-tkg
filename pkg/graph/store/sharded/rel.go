package sharded

import (
	"errors"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// --- Relationship CRUD ---
//
// ADR-0007 v1: a relationship row AND both its adjacency index entries (out +
// in) live on the REL ID's shard. Endpoint existence is validated on the
// endpoint's OWN shard (which may differ from the rel's shard for foreign-ID
// puts). Adjacency reads fold across all shards' local indexes.

func (s *Store) PutRelationship(r *types.Relationship) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	if err := storecontract.ValidateRelationshipWrite(r); err != nil {
		return err
	}
	relShard, err := s.shardForRelID(r.InternalID())
	if err != nil {
		return err
	}
	// Validate endpoints on their own shards (cross-shard reads).
	if err := s.requireNodeLive(r.StartNodeID()); err != nil {
		return err
	}
	if err := s.requireNodeLive(r.EndNodeID()); err != nil {
		return err
	}
	if relShard.HasRelID(r.ID().SnowflakeID()) {
		return ErrRelExists
	}
	// The rel entity + BOTH adjacency legs live on the rel's shard (ADR-0007), so
	// this is a single-shard atomic write via the co-located door — one WriteBatch,
	// one co-committed ChangeRelPut record (endpoints already validated cross-shard
	// above). The record emission is why this must NOT use the record-free partial
	// doors (PutRelEntityAndOut/PutRelIncoming) — a sharded rel create would
	// otherwise be invisible to a tailing replica.
	return relShard.PutRelationshipCoLocated(r)
}

func (s *Store) GetRelationship(id types.RelID) (*types.Relationship, error) {
	if err := s.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateRelID(id); err != nil {
		return nil, err
	}
	shard, err := s.shardForRelID(id)
	if err != nil {
		return nil, err
	}
	return shard.GetRelationship(id)
}

func (s *Store) ReplaceRelationship(r *types.Relationship) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	if err := storecontract.ValidateRelationshipWrite(r); err != nil {
		return err
	}
	shard, err := s.shardForRelID(r.InternalID())
	if err != nil {
		return err
	}
	return shard.ReplaceRelationship(r)
}

// DeleteRelationship removes the rel row + both adjacency entries — all on the
// rel's shard, so this is a single-shard delegation.
func (s *Store) DeleteRelationship(id types.RelID) error {
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
	return shard.DeleteRelationship(id)
}

// requireNodeLive returns ErrNodeNotFound if the node is absent from its shard.
func (s *Store) requireNodeLive(id types.NodeID) error {
	shard, err := s.shardForNodeID(id)
	if err != nil {
		return err
	}
	if !shard.HasNodeID(id.SnowflakeID()) {
		return ErrNodeNotFound
	}
	return nil
}

// --- Adjacency (parallel folds over all shards) ---

func (s *Store) OutgoingRelationships(nodeID types.NodeID, typeToken uint16) ([]*types.Relationship, error) {
	if err := s.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateNodeID(nodeID); err != nil {
		return nil, err
	}
	if err := s.requireNodeLive(nodeID); err != nil {
		return nil, err
	}
	return s.foldAdjacency(nodeID, typeToken, true)
}

func (s *Store) IncomingRelationships(nodeID types.NodeID, typeToken uint16) ([]*types.Relationship, error) {
	if err := s.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateNodeID(nodeID); err != nil {
		return nil, err
	}
	if err := s.requireNodeLive(nodeID); err != nil {
		return nil, err
	}
	return s.foldAdjacency(nodeID, typeToken, false)
}

// foldAdjacency collects a node's outgoing (or incoming) relationships across
// every shard. Each shard holds the entity co-located with its adjacency entry,
// so per shard we resolve the adjacency rel IDs against that same shard.
func (s *Store) foldAdjacency(nodeID types.NodeID, typeToken uint16, outgoing bool) ([]*types.Relationship, error) {
	sid := nodeID.SnowflakeID()
	per := make([][]*types.Relationship, len(s.shards))
	err := s.forEachShardErr(func(idx int, shard *badgerShard) error {
		var ids []types.RelID
		if outgoing {
			for _, id := range shard.OutgoingRelIDs(sid) {
				ids = append(ids, types.RelID(id))
			}
		} else {
			for _, id := range shard.IncomingRelIDs(sid, typeToken) {
				ids = append(ids, types.RelID(id))
			}
		}
		if len(ids) == 0 {
			return nil
		}
		rels, gerr := shard.GetRelationshipsByIDs(ids)
		if gerr != nil {
			// Tolerate index orphans (an adjacency entry whose entity was
			// concurrently removed): skip missing rather than failing the fold.
			if errors.Is(gerr, ErrRelNotFound) {
				rels = resolveRelsTolerant(shard, ids)
			} else {
				return gerr
			}
		}
		var matched []*types.Relationship
		for _, rr := range rels {
			if relationshipMatches(rr, nodeID, typeToken, outgoing) {
				matched = append(matched, rr)
			}
		}
		per[idx] = matched
		return nil
	})
	if err != nil {
		return nil, err
	}
	merged := mergeSortRels(per)
	if len(merged) == 0 {
		return nil, nil
	}
	return merged, nil
}

// resolveRelsTolerant resolves rel IDs one at a time, skipping any that are
// missing (index orphan / tombstone).
func resolveRelsTolerant(shard *badgerShard, ids []types.RelID) []*types.Relationship {
	var out []*types.Relationship
	for _, id := range ids {
		r, err := shard.GetRelationship(id)
		if err != nil {
			continue
		}
		out = append(out, r)
	}
	return out
}

func relationshipMatches(r *types.Relationship, nodeID types.NodeID, typeToken uint16, outgoing bool) bool {
	if r == nil {
		return false
	}
	if outgoing {
		if r.StartNodeID() != nodeID {
			return false
		}
	} else {
		if r.EndNodeID() != nodeID {
			return false
		}
	}
	return typeToken == 0 || r.HasTypeTokenRaw(typeToken)
}

func (s *Store) OutgoingRelationshipsForNodes(nodeIDs []types.NodeID, typeToken uint16) (map[types.NodeID][]*types.Relationship, error) {
	return s.foldAdjacencyForNodes(nodeIDs, typeToken, true)
}

func (s *Store) IncomingRelationshipsForNodes(nodeIDs []types.NodeID, typeToken uint16) (map[types.NodeID][]*types.Relationship, error) {
	return s.foldAdjacencyForNodes(nodeIDs, typeToken, false)
}

func (s *Store) foldAdjacencyForNodes(nodeIDs []types.NodeID, typeToken uint16, outgoing bool) (map[types.NodeID][]*types.Relationship, error) {
	if err := s.checkOpen(); err != nil {
		return nil, err
	}
	if len(nodeIDs) == 0 {
		return nil, nil
	}
	for _, id := range nodeIDs {
		if err := storecontract.ValidateNodeID(id); err != nil {
			return nil, err
		}
	}
	// All requested nodes must exist (all-or-error contract).
	seen := make(map[types.NodeID]struct{}, len(nodeIDs))
	for _, id := range nodeIDs {
		if _, done := seen[id]; done {
			continue
		}
		seen[id] = struct{}{}
		if err := s.requireNodeLive(id); err != nil {
			return nil, err
		}
	}
	result := make(map[types.NodeID][]*types.Relationship)
	for id := range seen {
		rels, err := s.foldAdjacency(id, typeToken, outgoing)
		if err != nil {
			return nil, err
		}
		if len(rels) > 0 {
			result[id] = rels
		}
	}
	if len(result) == 0 {
		return nil, nil
	}
	return result, nil
}
