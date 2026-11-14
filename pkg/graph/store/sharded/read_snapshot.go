package sharded

import (
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var _ storecontract.ReadSnapshotCapability = (*Store)(nil)

var _ storecontract.ReadSnapshotLendCapability = (*readSnapshot)(nil)

var _ storecontract.ReadSnapshotMeterCapability = (*readSnapshot)(nil)

var _ storecontract.ReadSnapshotAsOfWalkCapability = (*readSnapshot)(nil)

// NodeAsOfWalk delegates to the immutable owning slot with the snapshot's
// tally. The native owner admits every probe and decode before doing it.
func (r *readSnapshot) NodeAsOfWalk(id types.NodeID, txTime types.Instant) (*types.Node, bool, error) {
	if err := r.s.checkOpen(); err != nil {
		return nil, false, err
	}
	if err := storecontract.ValidateNodeID(id); err != nil {
		return nil, false, err
	}
	owner, err := r.s.shardForNodeID(id)
	if err != nil {
		return nil, false, err
	}
	n, err := owner.NodeAsOfMetered(id, txTime, r.tally)
	return n, true, err
}

// RelAsOfWalk is NodeAsOfWalk for a relationship's immutable owning slot.
func (r *readSnapshot) RelAsOfWalk(id types.RelID, txTime types.Instant) (*types.Relationship, bool, error) {
	if err := r.s.checkOpen(); err != nil {
		return nil, false, err
	}
	if err := storecontract.ValidateRelID(id); err != nil {
		return nil, false, err
	}
	owner, err := r.s.shardForRelID(id)
	if err != nil {
		return nil, false, err
	}
	rr, err := owner.RelAsOfMetered(id, txTime, r.tally)
	return rr, true, err
}

// NewReadSnapshot returns a store.ReadSnapshot that routes every
// read to the slot the ID routes to, as the store's own reads do, and reads
// it through that slot's badger snapshot (opened on the slot's first read):
// one transaction per slot a chunk of the scan touches.
func (s *Store) NewReadSnapshot() storecontract.ReadSnapshot {
	return &readSnapshot{s: s, slots: map[*badger.Store]storecontract.ReadSnapshot{}}
}

type readSnapshot struct {
	s     *Store
	slots map[*badger.Store]storecontract.ReadSnapshot
	tally *storecontract.ReadTally // RH list R2: every slot snapshot counts its decodes in it
}

// MeterReads implements store.ReadSnapshotMeterCapability: the snapshots of
// every slot, now and when opened later, count the bytes they decode in t.
func (r *readSnapshot) MeterReads(t *storecontract.ReadTally) {
	r.tally = t
	for _, snap := range r.slots {
		snap.(storecontract.ReadSnapshotMeterCapability).MeterReads(t)
	}
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
		if r.tally != nil {
			snap.(storecontract.ReadSnapshotMeterCapability).MeterReads(r.tally)
		}
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

// LendNode is GetNode lent (store.ReadSnapshotLendCapability), through the
// slot's badger snapshot.
func (r *readSnapshot) LendNode(id types.NodeID) (*types.Node, error) {
	if err := storecontract.ValidateNodeID(id); err != nil {
		return nil, err
	}
	snap, err := r.slot(int64(id))
	if err != nil {
		return nil, err
	}
	return snap.(storecontract.ReadSnapshotLendCapability).LendNode(id)
}

// LendRelationship is LendNode for relationships.
func (r *readSnapshot) LendRelationship(id types.RelID) (*types.Relationship, error) {
	if err := storecontract.ValidateRelID(id); err != nil {
		return nil, err
	}
	snap, err := r.slot(int64(id))
	if err != nil {
		return nil, err
	}
	return snap.(storecontract.ReadSnapshotLendCapability).LendRelationship(id)
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
