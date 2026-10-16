package tiered

import (
	"fmt"
	"slices"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// --- Label/type queries ---

func (ts *Store) NodesByLabel(token uint16, opts QueryOpts) ([]*types.Node, error) {
	ts.noteTypeScan("NodesByLabel")
	if err := ts.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateLabelToken(token); err != nil {
		return nil, err
	}
	if err := validateQueryOpts(opts); err != nil {
		return nil, err
	}
	// Label queries ask for membership, not ownership: a reference-primary node
	// may carry an event extra label and vice versa, so query every relevant tier.
	move := ts.lockMoveRead() // a node moved mid-read would show twice or not at all
	defer move.unlock()
	ref, refCheckin, err := ts.checkoutRefShard()
	if err != nil {
		return nil, err
	}
	refNodes, err := ref.NodesByLabel(token, stripDepth(opts))
	refCheckin()
	if err != nil {
		return nil, err
	}

	var archiveNodes []*types.Node
	if opts.Depth == DepthAll {
		ts.refArchiveGapHook()
		archive, archiveCheckin, archiveErr := ts.checkoutArchive()
		if archiveErr != nil {
			return nil, archiveErr
		}
		if archive != nil {
			archiveNodes, err = archive.NodesByLabel(token, stripDepth(opts))
			archiveCheckin()
			if err != nil {
				return nil, err
			}
		}
	}
	move.unlock()

	ts.mu.RLock()
	eventShards := ts.eventShardSnapshot(opts.Depth)
	ts.mu.RUnlock()

	type result struct {
		nodes []*types.Node
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
		results[i].nodes, results[i].err = store.NodesByLabel(token, stripDepth(opts))
	})

	var slices [][]*types.Node
	if len(refNodes) > 0 {
		slices = append(slices, refNodes)
	}
	if len(archiveNodes) > 0 {
		slices = append(slices, archiveNodes)
	}
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
	ts.noteTypeScan("RelationshipsByType")
	if err := ts.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateRelTypeToken(token); err != nil {
		return nil, err
	}
	if err := validateQueryOpts(opts); err != nil {
		return nil, err
	}
	ts.mu.RLock()
	eventShards := ts.eventShardSnapshot(opts.Depth)
	ts.mu.RUnlock()

	move := ts.lockMoveRead() // a node moved mid-read would show twice or not at all
	defer move.unlock()
	ref, refCheckin, err := ts.checkoutRefShard()
	if err != nil {
		return nil, err
	}
	refRels, err := ref.RelationshipsByType(token, stripDepth(opts))
	refCheckin()
	if err != nil {
		return nil, err
	}

	// refArchive parity: archived rels carry the same type token; DepthAll only
	// (as NodesByLabel / AllRelationships).
	var archiveRels []*types.Relationship
	if opts.Depth == DepthAll {
		ts.refArchiveGapHook()
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
	move.unlock()

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
		results[i].rels, results[i].err = store.RelationshipsByType(token, stripDepth(opts))
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

// --- Property indexes ---

func (ts *Store) NodesByLabelAndProperty(labelToken uint16, key string, value any, opts QueryOpts) ([]*types.Node, error) {
	if err := ts.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateIndexPropertyKey(key); err != nil {
		return nil, err
	}
	if err := validateQueryOpts(opts); err != nil {
		return nil, err
	}
	if err := types.ValidatePropertyValue(value); err != nil {
		return nil, fmt.Errorf("graph: nodes by label and property value: %w", err)
	}

	move := ts.lockMoveRead() // a node moved mid-read would show twice or not at all
	defer move.unlock()
	ref, refCheckin, err := ts.checkoutRefShard()
	if err != nil {
		return nil, err
	}
	elemOpts, _, err := ref.PropertyIndexOptions(labelToken, key)
	var refNodes []*types.Node
	if err == nil {
		refNodes, err = shardNodesByLabelAndProperty(ref, elemOpts.Elements, labelToken, key, value, stripDepth(opts))
	}
	refCheckin()
	if err != nil {
		return nil, err
	}

	var archiveNodes []*types.Node
	if opts.Depth == DepthAll {
		ts.refArchiveGapHook()
		archive, archiveCheckin, archiveErr := ts.checkoutArchive()
		if archiveErr != nil {
			return nil, archiveErr
		}
		if archive != nil {
			archiveNodes, err = shardNodesByLabelAndProperty(archive, elemOpts.Elements, labelToken, key, value, stripDepth(opts))
			archiveCheckin()
			if err != nil {
				return nil, err
			}
		}
	}
	move.unlock()

	ts.mu.RLock()
	eventShards := ts.eventShardSnapshot(opts.Depth)
	ts.mu.RUnlock()

	type result struct {
		nodes []*types.Node
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
		results[i].nodes, results[i].err = shardNodesByLabelAndProperty(store, elemOpts.Elements, labelToken, key, value, stripDepth(opts))
	})

	var slices [][]*types.Node
	if len(refNodes) > 0 {
		slices = append(slices, refNodes)
	}
	if len(archiveNodes) > 0 {
		slices = append(slices, archiveNodes)
	}
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

// shardNodesByLabelAndProperty is one shard's part of a node property lookup
// under the anchor's index semantics (elements: the anchor's index is an
// Elements index, backlog 63): the shard's own door when shardPropertyDoor
// says it answers the same, else a label scan with the anchor's match (a shard
// without its copy of an Elements index, or with a stale copy).
func shardNodesByLabelAndProperty(store *BadgerStore, elements bool, labelToken uint16, key string, value any, opts QueryOpts) ([]*types.Node, error) {
	have, has, err := store.PropertyIndexOptions(labelToken, key)
	if err != nil {
		return nil, err
	}
	if shardPropertyDoor(have, has, elements) {
		return store.NodesByLabelAndProperty(labelToken, key, value, opts)
	}
	targetKey := indexpkg.PropertyValueKey(value)
	if targetKey == "" {
		return nil, nil
	}
	nodes, err := store.NodesByLabel(labelToken, QueryOpts{After: opts.After})
	if err != nil {
		return nil, err
	}
	temporal := opts.HasValidTimeFilter()
	return slices.DeleteFunc(nodes, func(n *types.Node) bool {
		return !n.HasPropertyIndexKey(key, elements, targetKey) ||
			(temporal && !storeutil.MatchesTemporalFilter(n.ID().SnowflakeID(), n.Temporal(), opts))
	}), nil
}
