package sharded

import (
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// ForEachExAdjacentRelID / NoteExAdjacentRel implement
// storecontract.ExAdjacencyCapability. A relationship's rows and
// both adjacency legs live on the shard owning its ID, and so does its
// ex-adjacency; a node's is spread over the shards like its adjacency, so a
// read fans out over every shard (as foldAdjacency does).
var _ storecontract.ExAdjacencyCapability = (*Store)(nil)
var _ storecontract.ExAdjacencyMeteredCapability = (*Store)(nil)

func (s *Store) ForEachExAdjacentRelID(nid types.NodeID, incoming bool, fn func(types.RelID) bool) error {
	return s.ForEachExAdjacentRelIDMetered(nid, incoming, nil, fn)
}

// ForEachExAdjacentRelIDMetered shares native admission across every slot.
func (s *Store) ForEachExAdjacentRelIDMetered(nid types.NodeID, incoming bool, t *storecontract.ReadTally, fn func(types.RelID) bool) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return storecontract.ErrInvalidStoreMutation
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return err
	}
	return s.forEachRelID(fn, func(shard *badgerShard, cb func(types.RelID) bool) error {
		return shard.ForEachExAdjacentRelIDMetered(nid, incoming, t, cb)
	})
}

func (s *Store) NoteExAdjacentRel(id types.RelID, start, end types.NodeID) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	if err := storecontract.ValidateRelationshipIndexEntry(start, end, 1, id); err != nil {
		return err
	}
	shard, err := s.shardForRelID(id)
	if err != nil {
		return err
	}
	return shard.NoteExAdjacentRel(id, start, end)
}
