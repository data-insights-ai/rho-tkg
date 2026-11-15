package tiered

import (
	"errors"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var _ storecontract.ReadSnapshotAsOfWalkCapability = (*readSnapshot)(nil)

func (r *readSnapshot) NodeAsOfWalk(id types.NodeID, txTime types.Instant) (*types.Node, bool, error) {
	return r.ts.nodeAsOfWalkCharged(id, txTime, r.tally)
}

func (r *readSnapshot) RelAsOfWalk(id types.RelID, txTime types.Instant) (*types.Relationship, bool, error) {
	return r.ts.relAsOfWalkCharged(id, txTime, r.tally)
}

// nodeAsOfWalkCharged shares the plain walk's eligibility and revalidation.
// The owner remains pinned through the walk and every probe uses the same
// tally. No move lock spans the gap hook: an archive move must be observable
// by the final liveness check, which declines to the whole-chain fallback.
func (ts *Store) nodeAsOfWalkCharged(id types.NodeID, txTime types.Instant, t *storecontract.ReadTally) (*types.Node, bool, error) {
	return nodeHistoryOwnerRead(ts, id, t, func(owner *BadgerStore) (*types.Node, error) {
		return owner.NodeAsOfMetered(id, txTime, t)
	})
}

// nodeHistoryOwnerRead shares owner eligibility and final revalidation between
// as-of resolution and compact history proofs. Both require one complete owner.
func nodeHistoryOwnerRead[Row any](ts *Store, id types.NodeID, t *storecontract.ReadTally, read func(*BadgerStore) (*Row, error)) (*Row, bool, error) {
	if err := ts.checkOpen(); err != nil {
		return nil, false, err
	}
	if err := storecontract.ValidateNodeID(id); err != nil {
		return nil, false, err
	}
	owner, checkin, isArchive, err := ts.shardForNodeIDReadCharged(id, t)
	if err != nil {
		return nil, false, err
	}
	defer checkin()
	live := func() (bool, error) {
		here, err := nodeRowLiveCharged(owner, id, t)
		here, _ = ts.nodeLivesRoute(id, here, false)
		return here, err
	}
	here, err := live()
	if err != nil || !here || isArchive {
		return nil, false, err
	}
	return asOfOwnerWalk(ts, owner, t,
		func(s storecontract.ReadSnapshot) (bool, error) { return s.HasNodeHistory(id) },
		func() (*Row, error) { return read(owner) }, live)
}

// relAsOfWalkCharged uses the ordinary relationship route, including reference,
// archive, mint and cross-shard owners. Its admitted route probe supplies the
// initial liveness proof; split lives and archive history still decline.
func (ts *Store) relAsOfWalkCharged(id types.RelID, txTime types.Instant, t *storecontract.ReadTally) (*types.Relationship, bool, error) {
	return relHistoryOwnerRead(ts, id, t, func(owner *BadgerStore) (*types.Relationship, error) {
		return owner.RelAsOfMetered(id, txTime, t)
	})
}

// relHistoryOwnerRead is the relationship form of nodeHistoryOwnerRead.
func relHistoryOwnerRead[Row any](ts *Store, id types.RelID, t *storecontract.ReadTally, read func(*BadgerStore) (*Row, error)) (*Row, bool, error) {
	if err := ts.checkOpen(); err != nil {
		return nil, false, err
	}
	if err := storecontract.ValidateRelID(id); err != nil {
		return nil, false, err
	}
	owner, checkin, isArchive, found, err := ts.routeRelShardFor(id, routeRead, t)
	if err != nil {
		return nil, false, err
	}
	defer checkin()
	if found.err != nil {
		return nil, false, found.err
	}
	here, isArchive := ts.relLivesRoute(id, found.live, isArchive)
	if !here || isArchive {
		return nil, false, nil
	}
	live := func() (bool, error) {
		here, err := relRowLiveCharged(owner, id, t)
		here, _ = ts.relLivesRoute(id, here, false)
		return here, err
	}
	return asOfOwnerWalk(ts, owner, t,
		func(s storecontract.ReadSnapshot) (bool, error) { return s.HasRelHistory(id) },
		func() (*Row, error) { return read(owner) }, live)
}

var _ storecontract.ReadSnapshotHistoryBoundsCapability = (*readSnapshot)(nil)

type historyBounds struct {
	version uint32
	newest  types.Instant
}

func (r *readSnapshot) NodeHistoryBounds(id types.NodeID) (uint32, types.Instant, bool, error) {
	bounds, handled, err := nodeHistoryOwnerRead(r.ts, id, r.tally, func(owner *BadgerStore) (*historyBounds, error) {
		snap := owner.NewReadSnapshot()
		defer snap.Close()
		snap.(storecontract.ReadSnapshotMeterCapability).MeterReads(r.tally)
		version, newest, known, err := snap.(storecontract.ReadSnapshotHistoryBoundsCapability).NodeHistoryBounds(id)
		if err != nil || !known {
			return nil, err
		}
		return &historyBounds{version, newest}, nil
	})
	return unpackHistoryBounds(bounds, handled, err)
}

func (r *readSnapshot) RelHistoryBounds(id types.RelID) (uint32, types.Instant, bool, error) {
	bounds, handled, err := relHistoryOwnerRead(r.ts, id, r.tally, func(owner *BadgerStore) (*historyBounds, error) {
		snap := owner.NewReadSnapshot()
		defer snap.Close()
		snap.(storecontract.ReadSnapshotMeterCapability).MeterReads(r.tally)
		version, newest, known, err := snap.(storecontract.ReadSnapshotHistoryBoundsCapability).RelHistoryBounds(id)
		if err != nil || !known {
			return nil, err
		}
		return &historyBounds{version, newest}, nil
	})
	return unpackHistoryBounds(bounds, handled, err)
}

func unpackHistoryBounds(bounds *historyBounds, handled bool, err error) (uint32, types.Instant, bool, error) {
	if err != nil || !handled || bounds == nil {
		return 0, 0, false, err
	}
	return bounds.version, bounds.newest, true, nil
}

// asOfOwnerWalk runs only after the route proved one live owner. Reference
// entities may retain history in the archive, so ask its admitted snapshot
// before walking. Every checkout is pinned until its reads finish.
func asOfOwnerWalk[Row any](ts *Store, owner *BadgerStore, t *storecontract.ReadTally, archived func(storecontract.ReadSnapshot) (bool, error), read func() (*Row, error), live func() (bool, error)) (*Row, bool, error) {
	if owner == ts.refShard {
		archive, checkin, err := ts.checkoutArchiveCharged(t)
		if err != nil {
			return nil, false, err
		}
		defer checkin()
		if archive != nil {
			snap := archive.NewReadSnapshot()
			snap.(storecontract.ReadSnapshotMeterCapability).MeterReads(t)
			has, err := archived(snap)
			snap.Close()
			if err != nil || has {
				return nil, false, err
			}
		}
	}
	if ts.asOfWalkGapHook != nil {
		ts.asOfWalkGapHook()
	}
	row, err := read()
	if errors.Is(err, storecontract.ErrReadBudget) {
		return nil, true, err
	}
	if here, liveErr := live(); liveErr != nil || !here {
		return nil, false, liveErr
	}
	return row, true, err
}

// These liveness probes own their Candidate charge. The shard snapshot owns
// the bytes decoded by a cache miss; the row is lent and never copied here.
func nodeRowLiveCharged(s *BadgerStore, id types.NodeID, t *storecontract.ReadTally) (bool, error) {
	if s == nil {
		return false, nil
	}
	if err := t.Candidates(1); err != nil {
		return false, err
	}
	if !s.HasNodeID(id.SnowflakeID()) {
		return false, nil
	}
	var err error
	if t == nil {
		_, err = s.LendNode(id)
	} else {
		_, err = (meteredShard{s, t}).LendNode(id)
	}
	if errors.Is(err, ErrNodeNotFound) {
		return false, nil
	}
	return err == nil, err
}

func relRowLiveCharged(s *BadgerStore, id types.RelID, t *storecontract.ReadTally) (bool, error) {
	if s == nil {
		return false, nil
	}
	if err := t.Candidates(1); err != nil {
		return false, err
	}
	if !s.HasRelID(id.SnowflakeID()) {
		return false, nil
	}
	var err error
	if t == nil {
		_, err = s.LendRelationship(id)
	} else {
		_, err = (meteredShard{s, t}).LendRelationship(id)
	}
	if errors.Is(err, ErrRelNotFound) {
		return false, nil
	}
	return err == nil, err
}
