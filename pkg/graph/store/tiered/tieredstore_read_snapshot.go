package tiered

import (
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// store.ReadSnapshotCapability on tiered (row 121): an effective scan's run of
// node reads routes every read exactly as the store's method of the same name
// (move lock, shard checkout, archive and history-shard fallbacks), and reads
// the reference shard through ONE badger read snapshot for the run: one
// transaction and forward iterators instead of a transaction per read. The
// reference shard is opened with the store and closed only by Close, so the
// snapshot never outlives it (a run holds the graph's read lock, which Close
// waits for). Event shards and the archive are read through their own
// methods (a cold shard may close between reads). Relationship reads are the
// store's methods. Each read therefore answers what the store's method
// answers at that moment, as store.ReadSnapshot requires.

var _ storecontract.ReadSnapshotCapability = (*Store)(nil)

var _ storecontract.ReadSnapshotLendCapability = (*readSnapshot)(nil)

var _ storecontract.ReadSnapshotMeterCapability = (*readSnapshot)(nil)

// shardNodeReads is what a tiered node read asks the shard it routed to.
type shardNodeReads interface {
	GetNode(types.NodeID) (*types.Node, error)
	LendNode(types.NodeID) (*types.Node, error)
	HasNodeHistory(types.NodeID) (bool, error)
	NodeHistoryTemporalMeta(types.NodeID) ([]storecontract.VersionTemporalMeta, error)
	GetNodeVersion(types.NodeID, uint32) (*types.Node, error)
	GetNodeHistory(types.NodeID) ([]*types.Node, error)
}

// shardVia maps a routed shard to what reads it: the shard itself, or in a
// read snapshot the reference shard's badger snapshot.
type shardVia func(*BadgerStore) shardNodeReads

func directReads(s *BadgerStore) shardNodeReads { return s }

// NewReadSnapshot returns a store.ReadSnapshot over the reference shard.
func (ts *Store) NewReadSnapshot() storecontract.ReadSnapshot { return &readSnapshot{ts: ts} }

type readSnapshot struct {
	ts    *Store
	ref   storecontract.ReadSnapshot // the reference shard's, opened on its first read
	tally *storecontract.ReadTally   // RH list R2: set by MeterReads, every decode counts in it
}

// MeterReads implements store.ReadSnapshotMeterCapability: the reads of this
// snapshot, on every shard, count the bytes they decode in t before the decode
// (the reference shard's badger snapshot, and a throwaway one per read on any
// other shard). Presence reads decode no row and count nothing.
func (r *readSnapshot) MeterReads(t *storecontract.ReadTally) {
	r.tally = t
	if r.ref != nil {
		r.ref.(storecontract.ReadSnapshotMeterCapability).MeterReads(t)
	}
}

// refSnapshot is the reference shard's badger snapshot, opened on first use.
func (r *readSnapshot) refSnapshot(s *BadgerStore) storecontract.ReadSnapshot {
	if r.ref == nil {
		r.ref = s.NewReadSnapshot()
		if r.tally != nil {
			r.ref.(storecontract.ReadSnapshotMeterCapability).MeterReads(r.tally)
		}
	}
	return r.ref
}

// via reads the reference shard through the snapshot, any other directly (a
// metered snapshot: through a counting read).
func (r *readSnapshot) via(s *BadgerStore) shardNodeReads {
	if s == nil {
		return s
	}
	if s != r.ts.refShard {
		if r.tally != nil {
			return meteredShard{s, r.tally}
		}
		return s
	}
	return r.refSnapshot(s).(shardNodeReads)
}

// relVia is via for relationship reads, which read the reference shard through
// its snapshot only when metered: an unmetered snapshot reads them directly.
func (r *readSnapshot) relVia(s *BadgerStore) shardRelReads {
	if r.tally == nil || s == nil {
		return s
	}
	if s != r.ts.refShard {
		return meteredShard{s, r.tally}
	}
	return r.refSnapshot(s).(shardRelReads)
}

// A metered snapshot's reads charge each closed shard's open to its tally
// before the open (READBUDGET step 6, ShardOpens): every routed checkout and
// the whole-chain history search; an unmetered one (nil tally) charges nothing.

func (r *readSnapshot) GetNode(id types.NodeID) (*types.Node, error) {
	return r.ts.getNodeVia(id, r.via, r.tally, false)
}

func (r *readSnapshot) LendNode(id types.NodeID) (*types.Node, error) {
	return r.ts.getNodeVia(id, r.via, r.tally, true)
}

func (r *readSnapshot) HasNodeHistory(id types.NodeID) (bool, error) {
	return r.ts.hasNodeHistoryVia(id, r.via, r.tally)
}

func (r *readSnapshot) NodeHistoryTemporalMeta(id types.NodeID) ([]storecontract.VersionTemporalMeta, error) {
	return r.ts.nodeHistoryTemporalMetaVia(id, r.via, r.tally)
}

func (r *readSnapshot) GetNodeVersion(id types.NodeID, version uint32) (*types.Node, error) {
	return r.ts.getNodeVersionVia(id, version, r.via, r.tally)
}

// GetNodeHistory is the store's own; a metered snapshot reads each shard's
// rows through its counting reads (RH list R2).
func (r *readSnapshot) GetNodeHistory(id types.NodeID) ([]*types.Node, error) {
	if r.tally != nil {
		return r.ts.getNodeHistoryVia(id, r.via, r.tally)
	}
	return r.ts.GetNodeHistory(id)
}

func (r *readSnapshot) GetRelationship(id types.RelID) (*types.Relationship, error) {
	if r.tally != nil {
		return r.ts.getRelationshipVia(id, r.relVia, r.tally, false)
	}
	return r.ts.GetRelationship(id)
}

func (r *readSnapshot) LendRelationship(id types.RelID) (*types.Relationship, error) {
	if r.tally != nil {
		return r.ts.getRelationshipVia(id, r.relVia, r.tally, true)
	}
	return r.ts.LendRelationship(id)
}

func (r *readSnapshot) HasRelHistory(id types.RelID) (bool, error) {
	return r.ts.hasRelHistoryCharged(id, r.tally)
}

func (r *readSnapshot) RelHistoryTemporalMeta(id types.RelID) ([]storecontract.VersionTemporalMeta, error) {
	if r.tally != nil {
		return r.ts.relHistoryTemporalMetaVia(id, r.relVia, r.tally)
	}
	return r.ts.RelHistoryTemporalMeta(id)
}

func (r *readSnapshot) GetRelVersion(id types.RelID, version uint32) (*types.Relationship, error) {
	if r.tally != nil {
		return r.ts.getRelVersionVia(id, version, r.relVia, r.tally)
	}
	return r.ts.GetRelVersion(id, version)
}

func (r *readSnapshot) GetRelHistory(id types.RelID) ([]*types.Relationship, error) {
	if r.tally != nil {
		return r.ts.getRelHistoryVia(id, r.relVia, r.tally)
	}
	return r.ts.GetRelHistory(id)
}

// Release releases the reference shard's snapshot (its transaction and
// iterators); the next read opens a new one.
func (r *readSnapshot) Release() {
	if r.ref != nil {
		r.ref.Release()
	}
}

// Close is Release.
func (r *readSnapshot) Close() {
	if r.ref != nil {
		r.ref.Close()
		r.ref = nil
	}
}
