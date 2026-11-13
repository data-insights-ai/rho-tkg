package tiered

import (
	"slices"

	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// --- Label/type queries ---

func (ts *Store) NodesByLabel(token uint16, opts QueryOpts) ([]*types.Node, error) {
	return ts.nodesByLabel(token, opts, false)
}

var _ storecontract.NodesByLabelMeteredCapability = (*Store)(nil)

// NodesByLabelMetered implements store.NodesByLabelMeteredCapability: the
// reference shard, the archive and each event shard count the members of the
// label they list, before they read a row (badger.Store.NodesByLabelMetered),
// each closed shard is charged to the meter before it opens (it was already,
// READBUDGET step 6), and the first refusal stops the shards after it: the
// reference shard is read first and refuses before the archive or an event
// shard is touched, the event shards stop at their next check once one has
// refused (ReadMeter.Exceeded).
func (ts *Store) NodesByLabelMetered(token uint16, opts QueryOpts) ([]*types.Node, error) {
	return ts.nodesByLabel(token, opts, true)
}

func (ts *Store) nodesByLabel(token uint16, opts QueryOpts, metered bool) ([]*types.Node, error) {
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
	byLabel := func(shard *BadgerStore) ([]*types.Node, error) {
		if metered {
			return shard.NodesByLabelMetered(token, stripDepth(opts))
		}
		return shard.NodesByLabel(token, stripDepth(opts))
	}
	// Label queries ask for membership, not ownership: a reference-primary node
	// may carry an event extra label and vice versa, so query every relevant tier.
	move := ts.lockMoveRead() // a node moved mid-read would show twice or not at all
	defer move.unlock()
	ref, refCheckin, err := ts.checkoutRefShard()
	if err != nil {
		return nil, err
	}
	refNodes, err := byLabel(ref)
	refCheckin()
	if err != nil {
		return nil, err
	}

	var archiveNodes []*types.Node
	if opts.Depth == DepthAll {
		ts.refArchiveGapHook()
		archive, archiveCheckin, archiveErr := ts.checkoutArchiveCharged(opts.ReadTally()) // a budget's shard opens (READBUDGET step 6)
		if archiveErr != nil {
			return nil, archiveErr
		}
		if archive != nil {
			archiveNodes, err = byLabel(archive)
			archiveCheckin()
			if err != nil {
				return nil, err
			}
		}
	}
	move.unlock()

	ts.mu.RLock()
	eventShards := ts.eventShardsWithLabel(opts.Depth, token)
	ts.mu.RUnlock()

	type result struct {
		nodes []*types.Node
		err   error
	}
	results := make([]result, len(eventShards))
	queryEventShards(eventShards, func(i int, es *EventShard) {
		store, release, err := es.checkoutBudgeted(ts, opts.ReadTally())
		if err != nil {
			results[i].err = err
			return
		}
		defer release()
		results[i].nodes, results[i].err = byLabel(store)
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
	eventShards := ts.eventShardsWithRelType(opts.Depth, token)
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
		store, release, err := es.checkoutStoreReadOnly(ts)
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

// CountsPropertyQueryCandidates marks store.PropertyQueryCandidatesMeteredCapability:
// each shard counts its index's matches (badger) or its label (the fallback
// scan) before it reads a row.
func (ts *Store) CountsPropertyQueryCandidates() {}

func (ts *Store) NodesByLabelAndProperty(labelToken uint16, key string, value any, opts QueryOpts) ([]*types.Node, error) {
	return ts.nodesByLabelPropertyKeys(labelToken, key, opts, func() ([]string, error) {
		return storeutil.ValueIndexKeys(value, "nodes by label and property")
	})
}

var _ storecontract.PropertyKeysLookupCapability = (*Store)(nil)

// NodesByLabelAndPropertyKeys implements
// storecontract.PropertyKeysLookupCapability: one fold over the reference,
// archive and event shards, each shard asked once with every key (round 11;
// before, the graph asked once per numeric form and each ask walked every
// shard).
func (ts *Store) NodesByLabelAndPropertyKeys(labelToken uint16, key string, valueKeys []string, opts QueryOpts) ([]*types.Node, error) {
	return ts.nodesByLabelPropertyKeys(labelToken, key, opts, func() ([]string, error) { return valueKeys, nil })
}

var _ storecontract.NodePropertyKeysIDsCapability = (*Store)(nil)

// NodeIDsByLabelAndPropertyKeys implements
// storecontract.NodePropertyKeysIDsCapability: NodesByLabelAndPropertyKeys'
// IDs over the same shards, each shard's from its index without a row read
// where the shard answers so (badger NodeIDsByLabelAndPropertyKeys), else
// the IDs of its loaded rows (ELEMPROBE), filtered as there.
func (ts *Store) NodeIDsByLabelAndPropertyKeys(labelToken uint16, key string, valueKeys []string, opts QueryOpts) ([]types.NodeID, bool, error) {
	part := func(store *BadgerStore, elements bool, match storeutil.IndexKeyMatch, shardOpts QueryOpts) ([]types.NodeID, error) {
		if have, has, err := store.PropertyIndexOptions(labelToken, key); err != nil {
			return nil, err
		} else if shardPropertyDoor(have, has, elements) {
			ids, ok, err := store.NodeIDsByLabelAndPropertyKeys(labelToken, key, match.Keys(), shardOpts)
			if err != nil || ok {
				return ids, err
			}
		}
		nodes, err := shardNodesByLabelAndProperty(store, elements, labelToken, key, match, shardOpts)
		if err != nil {
			return nil, err
		}
		ids := make([]types.NodeID, len(nodes))
		for i, n := range nodes {
			ids[i] = n.ID()
		}
		return ids, nil
	}
	ids, err := propertyKeysFanOut(ts, labelToken, key, opts, func() ([]string, error) { return valueKeys, nil }, part, part,
		func(parts [][]types.NodeID) []types.NodeID {
			return storeutil.PaginateNodeIDs(mergeNodeIDSlices(parts), opts.After, opts.Limit)
		})
	if err != nil {
		return nil, false, err
	}
	return ids, true, nil
}

// nodesByLabelPropertyKeys is the body of both doors; keysOf runs after the
// store, token and option checks (a single value is validated there).
func (ts *Store) nodesByLabelPropertyKeys(labelToken uint16, key string, opts QueryOpts, keysOf func() ([]string, error)) ([]*types.Node, error) {
	return ts.nodesByLabelPropertyKeysWith(labelToken, key, opts, keysOf, func(ref *BadgerStore, elements bool, match storeutil.IndexKeyMatch, shardOpts QueryOpts) ([]*types.Node, error) {
		return shardNodesByLabelAndProperty(ref, elements, labelToken, key, match, shardOpts)
	})
}

// nodesByLabelPropertyKeysWith is nodesByLabelPropertyKeys reading the
// reference shard's part with refPart (the pruning lookup's, VALIDENDPRUNE).
func (ts *Store) nodesByLabelPropertyKeysWith(labelToken uint16, key string, opts QueryOpts, keysOf func() ([]string, error),
	refPart func(ref *BadgerStore, elements bool, match storeutil.IndexKeyMatch, shardOpts QueryOpts) ([]*types.Node, error)) ([]*types.Node, error) {
	return propertyKeysFanOut(ts, labelToken, key, opts, keysOf, refPart, func(store *BadgerStore, elements bool, match storeutil.IndexKeyMatch, shardOpts QueryOpts) ([]*types.Node, error) {
		return shardNodesByLabelAndProperty(store, elements, labelToken, key, match, shardOpts)
	}, func(parts [][]*types.Node) []*types.Node { return applyNodePagination(mergeNodeSlices(parts), opts) })
}

// propertyKeysFanOut is the fold of a node property lookup over the
// reference, archive and event shards: refPart reads the reference shard's
// part, shardPart the others', merge unites the sorted parts and pages them.
func propertyKeysFanOut[T any](ts *Store, labelToken uint16, key string, opts QueryOpts, keysOf func() ([]string, error),
	refPart, shardPart func(store *BadgerStore, elements bool, match storeutil.IndexKeyMatch, shardOpts QueryOpts) ([]T, error),
	merge func(parts [][]T) []T) ([]T, error) {
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
	targetKeys, err := keysOf()
	if err != nil {
		return nil, err
	}
	match := storeutil.DistinctIndexKeyMatch(targetKeys)
	if targetKeys = match.Keys(); len(targetKeys) == 0 {
		return nil, nil
	}

	move := ts.lockMoveRead() // a node moved mid-read would show twice or not at all
	defer move.unlock()
	ref, refCheckin, err := ts.checkoutRefShard()
	if err != nil {
		return nil, err
	}
	elemOpts, _, err := ref.PropertyIndexOptions(labelToken, key)
	var refNodes []T
	if err == nil {
		refNodes, err = refPart(ref, elemOpts.Elements, match, stripDepth(opts))
	}
	refCheckin()
	if err != nil {
		return nil, err
	}

	var archiveNodes []T
	if opts.Depth == DepthAll {
		ts.refArchiveGapHook()
		archive, archiveCheckin, archiveErr := ts.checkoutArchiveCharged(opts.ReadTally()) // a budget's shard opens (READBUDGET step 6)
		if archiveErr != nil {
			return nil, archiveErr
		}
		if archive != nil {
			archiveNodes, err = shardPart(archive, elemOpts.Elements, match, stripDepth(opts))
			archiveCheckin()
			if err != nil {
				return nil, err
			}
		}
	}
	move.unlock()

	ts.mu.RLock()
	eventShards := ts.eventShardsWithLabel(opts.Depth, labelToken)
	ts.mu.RUnlock()

	type result struct {
		nodes []T
		err   error
	}
	results := make([]result, len(eventShards))
	queryEventShards(eventShards, func(i int, es *EventShard) {
		store, release, err := es.checkoutBudgeted(ts, opts.ReadTally())
		if err != nil {
			results[i].err = err
			return
		}
		defer release()
		results[i].nodes, results[i].err = shardPart(store, elemOpts.Elements, match, stripDepth(opts))
	})

	var parts [][]T
	if len(refNodes) > 0 {
		parts = append(parts, refNodes)
	}
	if len(archiveNodes) > 0 {
		parts = append(parts, archiveNodes)
	}
	for _, r := range results {
		if r.err != nil {
			return nil, r.err
		}
		if len(r.nodes) > 0 {
			parts = append(parts, r.nodes)
		}
	}

	return merge(parts), nil
}

// shardNodesByLabelAndProperty is one shard's part of a node property lookup
// over match's index keys under the anchor's index
// semantics (elements: the anchor's index is an Elements index, backlog 63):
// the shard's own door when shardPropertyDoor says it answers the same, else
// a label scan with the anchor's match (a shard without its copy of an
// Elements index, or with a stale copy).
func shardNodesByLabelAndProperty(store *BadgerStore, elements bool, labelToken uint16, key string, match storeutil.IndexKeyMatch, opts QueryOpts) ([]*types.Node, error) {
	have, has, err := store.PropertyIndexOptions(labelToken, key)
	if err != nil {
		return nil, err
	}
	if shardPropertyDoor(have, has, elements) {
		return store.NodesByLabelAndPropertyKeys(labelToken, key, match.Keys(), opts)
	}
	if m, _ := opts.ReadMeter(); m != nil { // the shard's label counts before the fallback scan reads a row
		n, err := store.NodeCountByLabel(labelToken)
		if err != nil {
			return nil, err
		}
		if err := opts.AddCandidates(n); err != nil {
			return nil, err
		}
	}
	nodes, err := store.NodesByLabel(labelToken, QueryOpts{After: opts.After}.WithReadMeter(opts.ReadMeter())) // the fallback scan decodes under the same budget
	if err != nil {
		return nil, err
	}
	temporal := opts.HasValidTimeFilter()
	return slices.DeleteFunc(nodes, func(n *types.Node) bool {
		return !match.Node(n, key, elements) ||
			(temporal && !storeutil.MatchesTemporalFilter(store.ValidAxis(), n.ID().SnowflakeID(), n.Temporal(), opts))
	}), nil
}
