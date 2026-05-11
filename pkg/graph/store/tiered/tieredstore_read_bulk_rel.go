package tiered

import (
	"sync"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storecontract "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// Relationship-side bulk queries (R5-F9 split out from tieredstore_read_bulk.go).
// Mirror layout of the node-side methods left in tieredstore_read_bulk.go.

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

	refRels, err := ts.refShard.AllRelationships(stripDepth(opts))
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
	var wg sync.WaitGroup
	for i, es := range eventShards {
		wg.Add(1)
		go func(i int, es *EventShard) {
			defer wg.Done()
			store, err := es.checkoutStore(ts)
			if err != nil {
				results[i].err = err
				return
			}
			defer es.checkinStore()
			results[i].rels, results[i].err = store.AllRelationships(stripDepth(opts))
		}(i, es)
	}
	wg.Wait()

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
	n, err := ts.refShard.RelationshipCount()
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

	for _, es := range eventShards {
		store, err := es.checkoutStore(ts)
		if err != nil {
			return 0, err
		}
		n, err := store.RelationshipCount()
		es.checkinStore()
		if err != nil {
			return 0, err
		}
		total += n
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
	n, err := ts.refShard.RelCountByType(token)
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

	for _, es := range eventShards {
		store, err := es.checkoutStore(ts)
		if err != nil {
			return 0, err
		}
		n, err := store.RelCountByType(token)
		es.checkinStore()
		if err != nil {
			return 0, err
		}
		total += n
	}
	return total, nil
}

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

	refTyped, err := ts.refShard.AllRelIDs(stripDepth(opts))
	if err != nil {
		return nil, err
	}
	refIDs := relIDsToRaw(refTyped)

	// refArchive parity: see AllNodeIDs above. Depth-gated to DepthAll.
	var archiveIDs []snowflake.ID
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
			archiveIDs = relIDsToRaw(typed)
		}
	}

	type result struct {
		ids []snowflake.ID
		err error
	}
	results := make([]result, len(eventShards))
	var wg sync.WaitGroup
	for i, es := range eventShards {
		wg.Add(1)
		go func(i int, es *EventShard) {
			defer wg.Done()
			store, err := es.checkoutStore(ts)
			if err != nil {
				results[i].err = err
				return
			}
			defer es.checkinStore()
			typed, err := store.AllRelIDs(stripDepth(opts))
			results[i].ids = relIDsToRaw(typed)
			results[i].err = err
		}(i, es)
	}
	wg.Wait()

	var slices [][]snowflake.ID
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

	merged := mergeIDSlices(slices)
	paginated := applyIDPagination(merged, opts)
	return rawToRelIDs(paginated), nil
}

func (ts *Store) ForEachRelID(fn func(types.RelID) bool) error {
	if err := ts.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return errNilIterationCallback()
	}
	ids := make([]types.RelID, 0)
	if err := ts.refShard.ForEachRelID(func(id types.RelID) bool {
		ids = append(ids, id)
		return true
	}); err != nil {
		return err
	}
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
		store, err := es.checkoutStore(ts)
		if err != nil {
			return err
		}
		ids = ids[:0]
		err = store.ForEachRelID(func(id types.RelID) bool {
			ids = append(ids, id)
			return true
		})
		es.checkinStore()
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
