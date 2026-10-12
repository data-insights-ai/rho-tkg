package sharded

import (
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var _ storecontract.ReadSnapshotCapability = (*Store)(nil)

// NewReadSnapshot returns a store.ReadSnapshot (backlog 55) that routes every
// read to the slot the ID routes to, as the store's own reads do, and reads
// it through that slot's badger snapshot (opened on the slot's first read):
// one transaction per slot a chunk of the scan touches.
func (s *Store) NewReadSnapshot() storecontract.ReadSnapshot {
	return &readSnapshot{s: s, slots: map[*badger.Store]storecontract.ReadSnapshot{}}
}

type readSnapshot struct {
	s     *Store
	slots map[*badger.Store]storecontract.ReadSnapshot
}

func (r *readSnapshot) slot(id int64) (storecontract.ReadSnapshot, error) {
	if err := r.s.checkOpen(); err != nil {
		return nil, err
	}
	shard, err := r.s.shardForID(types.NodeID(id).SnowflakeID())
	if err != nil {
		return nil, err
	}
	snap, ok := r.slots[shard]
	if !ok {
		snap = shard.NewReadSnapshot()
		r.slots[shard] = snap
	}
	return snap, nil
}

func (r *readSnapshot) GetNode(id types.NodeID) (*types.Node, error) {
	if err := storecontract.ValidateNodeID(id); err != nil {
		return nil, err
	}
	snap, err := r.slot(int64(id))
	if err != nil {
		return nil, err
	}
	return snap.GetNode(id)
}

func (r *readSnapshot) GetRelationship(id types.RelID) (*types.Relationship, error) {
	if err := storecontract.ValidateRelID(id); err != nil {
		return nil, err
	}
	snap, err := r.slot(int64(id))
	if err != nil {
		return nil, err
	}
	return snap.GetRelationship(id)
}

func (r *readSnapshot) HasNodeHistory(id types.NodeID) (bool, error) {
	snap, err := r.slot(int64(id))
	if err != nil {
		return false, err
	}
	return snap.HasNodeHistory(id)
}

func (r *readSnapshot) HasRelHistory(id types.RelID) (bool, error) {
	snap, err := r.slot(int64(id))
	if err != nil {
		return false, err
	}
	return snap.HasRelHistory(id)
}

func (r *readSnapshot) NodeHistoryTemporalMeta(id types.NodeID) ([]storecontract.VersionTemporalMeta, error) {
	snap, err := r.slot(int64(id))
	if err != nil {
		return nil, err
	}
	return snap.NodeHistoryTemporalMeta(id)
}

func (r *readSnapshot) RelHistoryTemporalMeta(id types.RelID) ([]storecontract.VersionTemporalMeta, error) {
	snap, err := r.slot(int64(id))
	if err != nil {
		return nil, err
	}
	return snap.RelHistoryTemporalMeta(id)
}

func (r *readSnapshot) GetNodeVersion(id types.NodeID, version uint32) (*types.Node, error) {
	snap, err := r.slot(int64(id))
	if err != nil {
		return nil, err
	}
	return snap.GetNodeVersion(id, version)
}

func (r *readSnapshot) GetRelVersion(id types.RelID, version uint32) (*types.Relationship, error) {
	snap, err := r.slot(int64(id))
	if err != nil {
		return nil, err
	}
	return snap.GetRelVersion(id, version)
}

func (r *readSnapshot) GetNodeHistory(id types.NodeID) ([]*types.Node, error) {
	snap, err := r.slot(int64(id))
	if err != nil {
		return nil, err
	}
	return snap.GetNodeHistory(id)
}

func (r *readSnapshot) GetRelHistory(id types.RelID) ([]*types.Relationship, error) {
	snap, err := r.slot(int64(id))
	if err != nil {
		return nil, err
	}
	return snap.GetRelHistory(id)
}

// Release releases every slot's snapshot.
func (r *readSnapshot) Release() {
	for _, snap := range r.slots {
		snap.Release()
	}
}

// Close closes every slot's snapshot.
func (r *readSnapshot) Close() {
	for shard, snap := range r.slots {
		snap.Close()
		delete(r.slots, shard)
	}
}
