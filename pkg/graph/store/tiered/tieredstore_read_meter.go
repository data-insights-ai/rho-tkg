package tiered

import (
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Metered reads (RH list R2, DecodedBytes): a read snapshot that counts the
// bytes it decodes (store.ReadSnapshotMeterCapability) reads the reference
// shard through its one badger snapshot, and every other shard through a
// throwaway badger snapshot per read (a cold shard may close between reads, so
// none is held), each counting in the same tally. The routing is the store's
// own: the node reads through shardVia, the relationship reads through relVia.

// shardRelReads is what a tiered relationship read asks the shard it routed to.
type shardRelReads interface {
	GetRelationship(types.RelID) (*types.Relationship, error)
	LendRelationship(types.RelID) (*types.Relationship, error)
	RelHistoryTemporalMeta(types.RelID) ([]storecontract.VersionTemporalMeta, error)
	GetRelVersion(types.RelID, uint32) (*types.Relationship, error)
	GetRelHistory(types.RelID) ([]*types.Relationship, error)
}

// relVia maps a routed shard to what reads its relationships: the shard
// itself, or in a metered read snapshot a counting read.
type relVia func(*BadgerStore) shardRelReads

func directRelReads(s *BadgerStore) shardRelReads { return s }

var (
	_ shardNodeReads = meteredShard{}
	_ shardRelReads  = meteredShard{}
)

// meteredShard reads a shard through a throwaway badger read snapshot that
// counts the bytes it decodes in t: one transaction per read, as the shard's
// own methods open one. Reads that decode no row (presence) are the shard's.
type meteredShard struct {
	*BadgerStore
	t *storecontract.ReadTally
}

// snap opens the throwaway snapshot; the caller closes it.
func (m meteredShard) snap() storecontract.ReadSnapshot {
	s := m.BadgerStore.NewReadSnapshot()
	s.(storecontract.ReadSnapshotMeterCapability).MeterReads(m.t)
	return s
}

func (m meteredShard) GetNode(id types.NodeID) (*types.Node, error) {
	s := m.snap()
	defer s.Close()
	return s.GetNode(id)
}

func (m meteredShard) LendNode(id types.NodeID) (*types.Node, error) {
	s := m.snap()
	defer s.Close()
	return s.(storecontract.ReadSnapshotLendCapability).LendNode(id)
}

func (m meteredShard) NodeHistoryTemporalMeta(id types.NodeID) ([]storecontract.VersionTemporalMeta, error) {
	s := m.snap()
	defer s.Close()
	return s.NodeHistoryTemporalMeta(id)
}

func (m meteredShard) GetNodeVersion(id types.NodeID, version uint32) (*types.Node, error) {
	s := m.snap()
	defer s.Close()
	return s.GetNodeVersion(id, version)
}

func (m meteredShard) GetNodeHistory(id types.NodeID) ([]*types.Node, error) {
	s := m.snap()
	defer s.Close()
	return s.GetNodeHistory(id)
}

func (m meteredShard) GetRelationship(id types.RelID) (*types.Relationship, error) {
	s := m.snap()
	defer s.Close()
	return s.GetRelationship(id)
}

func (m meteredShard) LendRelationship(id types.RelID) (*types.Relationship, error) {
	s := m.snap()
	defer s.Close()
	return s.(storecontract.ReadSnapshotLendCapability).LendRelationship(id)
}

func (m meteredShard) RelHistoryTemporalMeta(id types.RelID) ([]storecontract.VersionTemporalMeta, error) {
	s := m.snap()
	defer s.Close()
	return s.RelHistoryTemporalMeta(id)
}

func (m meteredShard) GetRelVersion(id types.RelID, version uint32) (*types.Relationship, error) {
	s := m.snap()
	defer s.Close()
	return s.GetRelVersion(id, version)
}

func (m meteredShard) GetRelHistory(id types.RelID) ([]*types.Relationship, error) {
	s := m.snap()
	defer s.Close()
	return s.GetRelHistory(id)
}
