package tiered

import (
	"sync"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// --- Label/type queries ---

func (ts *Store) NodesByLabel(token uint16, opts QueryOpts) ([]*types.Node, error) {
	if ts.ontology.ClassifyByToken(token) == ClassReference {
		// Reference labels live on refShard + refArchive. Without merging
		// archive results, archived reference entities silently disappear
		// from NodesByLabel even though GetNode still finds them.
		//
		// Depth gating: archive is the coldest tier of reference data and
		// must NOT surface in DepthHot/DepthWarm — those callers explicitly
		// asked to exclude colder tiers. Only DepthAll (zero value, default)
		// includes archive content. Mirrors the AllNodes / AllRelationships
		// gating policy.
		refNodes, err := ts.refShard.NodesByLabel(token, stripDepth(opts))
		if err != nil {
			return nil, err
		}
		if opts.Depth != DepthAll {
			return applyNodePagination(refNodes, opts), nil
		}
		archive, archiveCheckin, archiveErr := ts.checkoutArchive()
		if archiveErr != nil {
			return nil, archiveErr
		}
		if archive == nil {
			return refNodes, nil
		}
		archiveNodes, err := archive.NodesByLabel(token, stripDepth(opts))
		archiveCheckin()
		if err != nil {
			return nil, err
		}
		merged := mergeNodeSlices([][]*types.Node{refNodes, archiveNodes})
		return applyNodePagination(merged, opts), nil
	}
	// Event label: fan out across all event shards matching depth.
	ts.mu.RLock()
	eventShards := ts.eventShardSnapshot(opts.Depth)
	ts.mu.RUnlock()

	type result struct {
		nodes []*types.Node
		err   error
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
			results[i].nodes, results[i].err = store.NodesByLabel(token, stripDepth(opts))
		}(i, es)
	}
	wg.Wait()

	var slices [][]*types.Node
	for _, r := range results {
		if r.err != nil {
			return nil, r.err
		}
		if len(r.nodes) > 0 {
			slices = append(slices, r.nodes)
		}
	}

	merged := mergeNodeSlices(slices)
	return applyNodePagination(merged, opts), nil
}

func (ts *Store) RelationshipsByType(token uint16, opts QueryOpts) ([]*types.Relationship, error) {
	ts.mu.RLock()
	eventShards := ts.eventShardSnapshot(opts.Depth)
	ts.mu.RUnlock()

	refRels, err := ts.refShard.RelationshipsByType(token, stripDepth(opts))
	if err != nil {
		return nil, err
	}

	// refArchive parity: archived rels (migrated together with their
	// reference endpoints) carry the same type token. Depth-gated to
	// DepthAll — archive is the coldest tier of reference data and must
	// not surface in DepthHot/DepthWarm queries. Mirrors NodesByLabel /
	// AllRelationships gating policy.
	var archiveRels []*types.Relationship
	if opts.Depth == DepthAll {
		archive, archiveCheckin, archiveErr := ts.checkoutArchive()
		if archiveErr != nil {
			return nil, archiveErr
		}
		if archive != nil {
			archiveRels, err = archive.RelationshipsByType(token, stripDepth(opts))
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
			results[i].rels, results[i].err = store.RelationshipsByType(token, stripDepth(opts))
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

// --- Property indexes ---

func (ts *Store) NodesByLabelAndProperty(labelToken uint16, key string, value any, opts QueryOpts) ([]*types.Node, error) {
	if ts.ontology.ClassifyByToken(labelToken) == ClassReference {
		// Reference labels live on refShard + refArchive — see NodesByLabel.
		// Depth-gated: archive is excluded from DepthHot/DepthWarm.
		refNodes, err := ts.refShard.NodesByLabelAndProperty(labelToken, key, value, stripDepth(opts))
		if err != nil {
			return nil, err
		}
		if opts.Depth != DepthAll {
			return applyNodePagination(refNodes, opts), nil
		}
		archive, archiveCheckin, archiveErr := ts.checkoutArchive()
		if archiveErr != nil {
			return nil, archiveErr
		}
		if archive == nil {
			return refNodes, nil
		}
		archiveNodes, err := archive.NodesByLabelAndProperty(labelToken, key, value, stripDepth(opts))
		archiveCheckin()
		if err != nil {
			return nil, err
		}
		merged := mergeNodeSlices([][]*types.Node{refNodes, archiveNodes})
		return applyNodePagination(merged, opts), nil
	}
	// Event label: fan out across all event shards matching depth.
	ts.mu.RLock()
	eventShards := ts.eventShardSnapshot(opts.Depth)
	ts.mu.RUnlock()

	type result struct {
		nodes []*types.Node
		err   error
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
			results[i].nodes, results[i].err = store.NodesByLabelAndProperty(labelToken, key, value, stripDepth(opts))
		}(i, es)
	}
	wg.Wait()

	var slices [][]*types.Node
	for _, r := range results {
		if r.err != nil {
			return nil, r.err
		}
		if len(r.nodes) > 0 {
			slices = append(slices, r.nodes)
		}
	}

	merged := mergeNodeSlices(slices)
	return applyNodePagination(merged, opts), nil
}
