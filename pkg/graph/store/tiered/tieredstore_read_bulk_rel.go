package tiered

import (
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Relationship-side bulk queries.
// Mirror layout of the node-side methods in tieredstore_read_bulk.go.

// AllRelationships shares AllNodes's memory caveat (BACKLOG 19j,
// tieredstore_read_bulk.go): every shard's full result is materialized
// concurrently and merged before opts.Limit/opts.After is applied, so peak
// memory is unbounded in the total relationship count, not reduced by
// pagination. ForEachRelID (below) is a genuine O(one-shard) streaming
// alternative for ID-only enumeration with no depth filtering.
func (ts *Store) AllRelationships(opts QueryOpts) ([]*types.Relationship, error) {
	if err := ts.checkOpen(); err != nil {
		return nil, err
	}
	if err := validateQueryOpts(opts); err != nil {
		return nil, err
	}
	ts.mu.RLock()
	eventShards := ts.eventShardSnapshot(opts.Depth)
	ts.mu.RUnlock()

	ref, refCheckin, err := ts.checkoutRefShard()
	if err != nil {
		return nil, err
	}
	refRels, err := ref.AllRelationships(stripDepth(opts))
	refCheckin()
	if err != nil {
		return nil, err
	}

	// refArchive parity: see AllNodes above. Depth-gated to DepthAll —
	// archive is the coldest tier of reference data and must not surface
	// in DepthHot/DepthWarm queries.
	var archiveRels []*types.Relationship
	if opts.Depth == DepthAll {
		archive, archiveCheckin, archiveErr := ts.checkoutArchive()
		if archiveErr != nil {
			return nil, archiveErr
		}
		if archive != nil {
			archiveRels, err = archive.AllRelationships(stripDepth(opts))
			archiveCheckin()
			if err != nil {
				return nil, err
			}
		}
	}

	type result struct {
		rels []*types.Relationship
		err  error
	}
	results := make([]result, len(eventShards))
	queryEventShards(eventShards, func(i int, es *EventShard) {
		store, release, err := es.checkoutStoreForRead(ts)
		if err != nil {
			results[i].err = err
			return
		}
		defer release()
		results[i].rels, results[i].err = store.AllRelationships(stripDepth(opts))
	})

	var slices [][]*types.Relationship
	if len(refRels) > 0 {
		slices = append(slices, refRels)
	}
	if len(archiveRels) > 0 {
		slices = append(slices, archiveRels)
	}
	for _, r := range results {
		if r.err != nil {
			return nil, r.err
		}
		if len(r.rels) > 0 {
			slices = append(slices, r.rels)
		}
	}

	merged := mergeRelSlices(slices)
	return applyRelPagination(merged, opts), nil
}

func (ts *Store) RelationshipCount() (int, error) {
	if err := ts.checkOpen(); err != nil {
		return 0, err
	}
	ts.mu.RLock()
	eventShards := ts.eventShardSnapshot(DepthAll)
	ts.mu.RUnlock()

	total := 0
	ref, refCheckin, err := ts.checkoutRefShard()
	if err != nil {
		return 0, err
	}
	n, err := ref.RelationshipCount()
	refCheckin()
	if err != nil {
		return 0, err
	}
	total += n

	// refArchive parity: archived rels count toward the public total.
	archive, archiveCheckin, archiveErr := ts.checkoutArchive()
	if archiveErr != nil {
		return 0, archiveErr
	}
	if archive != nil {
		ar, err := archive.RelationshipCount()
		archiveCheckin()
		if err != nil {
			return 0, err
		}
		total += ar
	}

	type result struct {
		count int
		err   error
	}
	results := make([]result, len(eventShards))
	queryEventShards(eventShards, func(i int, es *EventShard) {
		store, release, err := es.checkoutStoreForRead(ts)
		if err != nil {
			results[i].err = err
			return
		}
		defer release()
		results[i].count, results[i].err = store.RelationshipCount()
	})
	for _, r := range results {
		if r.err != nil {
			return 0, r.err
		}
		total += r.count
	}
	return total, nil
}

func (ts *Store) RelCountByType(token uint16) (int, error) {
	if err := ts.checkOpen(); err != nil {
		return 0, err
	}
	if err := storecontract.ValidateRelTypeToken(token); err != nil {
		return 0, err
	}
	ts.mu.RLock()
	eventShards := ts.eventShardSnapshot(DepthAll)
	ts.mu.RUnlock()

	total := 0
	ref, refCheckin, err := ts.checkoutRefShard()
	if err != nil {
		return 0, err
	}
	n, err := ref.RelCountByType(token)
	refCheckin()
	if err != nil {
		return 0, err
	}
	total += n

	// refArchive parity: archived rels of this type count toward total.
	archive, archiveCheckin, archiveErr := ts.checkoutArchive()
	if archiveErr != nil {
		return 0, archiveErr
	}
	if archive != nil {
		an, err := archive.RelCountByType(token)
		archiveCheckin()
		if err != nil {
			return 0, err
		}
		total += an
	}

	type result struct {
		count int
		err   error
	}
	results := make([]result, len(eventShards))
	queryEventShards(eventShards, func(i int, es *EventShard) {
		store, release, err := es.checkoutStoreForRead(ts)
		if err != nil {
			results[i].err = err
			return
		}
		defer release()
		results[i].count, results[i].err = store.RelCountByType(token)
	})
	for _, r := range results {
		if r.err != nil {
			return 0, r.err
		}
		total += r.count
	}
	return total, nil
}

// AllRelIDs mirrors AllRelationships's memory caveat (BACKLOG 19j) — see
// AllNodes's doc comment (tieredstore_read_bulk.go) for the full writeup.
func (ts *Store) AllRelIDs(opts QueryOpts) ([]types.RelID, error) {
	if err := ts.checkOpen(); err != nil {
		return nil, err
	}
	if err := validateQueryOpts(opts); err != nil {
		return nil, err
	}
	ts.mu.RLock()
	eventShards := ts.eventShardSnapshot(opts.Depth)
	ts.mu.RUnlock()

	ref, refCheckin, err := ts.checkoutRefShard()
	if err != nil {
		return nil, err
	}
	refTyped, err := ref.AllRelIDs(stripDepth(opts))
	refCheckin()
	if err != nil {
		return nil, err
	}
	refIDs := refTyped

	// refArchive parity: see AllNodeIDs above. Depth-gated to DepthAll.
	var archiveIDs []types.RelID
	if opts.Depth == DepthAll {
		archive, archiveCheckin, archiveErr := ts.checkoutArchive()
		if archiveErr != nil {
			return nil, archiveErr
		}
		if archive != nil {
			typed, err := archive.AllRelIDs(stripDepth(opts))
			archiveCheckin()
			if err != nil {
				return nil, err
			}
			archiveIDs = typed
		}
	}

	type result struct {
		ids []types.RelID
		err error
	}
	results := make([]result, len(eventShards))
	queryEventShards(eventShards, func(i int, es *EventShard) {
		store, release, err := es.checkoutStoreForRead(ts)
		if err != nil {
			results[i].err = err
			return
		}
		defer release()
		typed, err := store.AllRelIDs(stripDepth(opts))
		results[i].ids = typed
		results[i].err = err
	})

	var slices [][]types.RelID
	if len(refIDs) > 0 {
		slices = append(slices, refIDs)
	}
	if len(archiveIDs) > 0 {
		slices = append(slices, archiveIDs)
	}
	for _, r := range results {
		if r.err != nil {
			return nil, r.err
		}
		if len(r.ids) > 0 {
			slices = append(slices, r.ids)
		}
	}

	merged := mergeRelIDSlices(slices)
	return applyRelIDPagination(merged, opts), nil
}

func (ts *Store) ForEachRelID(fn func(types.RelID) bool) error {
	if err := ts.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return errNilIterationCallback()
	}
	ids := make([]types.RelID, 0)
	ref, refCheckin, err := ts.checkoutRefShard()
	if err != nil {
		return err
	}
	if err := ref.ForEachRelID(func(id types.RelID) bool {
		ids = append(ids, id)
		return true
	}); err != nil {
		refCheckin()
		return err
	}
	refCheckin()
	for _, id := range ids {
		if !fn(id) {
			return nil
		}
	}

	archive, archiveCheckin, archiveErr := ts.checkoutArchive()
	if archiveErr != nil {
		return archiveErr
	}
	if archive != nil {
		ids = ids[:0]
		err := archive.ForEachRelID(func(id types.RelID) bool {
			ids = append(ids, id)
			return true
		})
		archiveCheckin()
		if err != nil {
			return err
		}
		for _, id := range ids {
			if !fn(id) {
				return nil
			}
		}
	}

	ts.mu.RLock()
	shards := ts.eventShardSnapshot(DepthAll)
	ts.mu.RUnlock()

	for _, es := range shards {
		store, release, err := es.checkoutStoreForRead(ts)
		if err != nil {
			return err
		}
		ids = ids[:0]
		err = store.ForEachRelID(func(id types.RelID) bool {
			ids = append(ids, id)
			return true
		})
		release()
		if err != nil {
			return err
		}
		for _, id := range ids {
			if !fn(id) {
				return nil
			}
		}
	}
	return nil
}
