package tiered

import (
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var (
	_ storecontract.PropertyIndexValidEndCapability       = (*Store)(nil)
	_ storecontract.PropertyIndexValidEndListedCapability = (*Store)(nil)
)

// validEndSrcRef tags a probe of the reference shard: the only shard a
// pruning lookup prunes and refines (VALIDENDPRUNE: sigma's derived rows live
// there). Archive and event shard nodes are read whole (Src 0).
const validEndSrcRef = 1

// NodesByLabelAndPropertyKeysValidEnd implements
// store.PropertyIndexValidEndCapability: NodesByLabelAndPropertyKeys with the
// reference shard's pruning lookup. nil probes when no node is refinable.
func (ts *Store) NodesByLabelAndPropertyKeysValidEnd(labelToken uint16, key string, valueKeys []string, opts QueryOpts) ([]*types.Node, []storecontract.ValidEndProbe, error) {
	var refProbes map[types.NodeID]storecontract.ValidEndProbe
	nodes, err := ts.nodesByLabelPropertyKeysWith(labelToken, key, opts, func() ([]string, error) { return valueKeys, nil },
		// The reference shard's own index options decide elements, so its
		// index always answers (shardPropertyDoor).
		func(ref *BadgerStore, _ bool, match storeutil.IndexKeyMatch, shardOpts QueryOpts) ([]*types.Node, error) {
			nodes, probes, err := ref.NodesByLabelAndPropertyKeysValidEnd(labelToken, key, match.Keys(), shardOpts)
			if err != nil || probes == nil {
				return nodes, err
			}
			refProbes = make(map[types.NodeID]storecontract.ValidEndProbe, len(nodes))
			for i, n := range nodes {
				p := probes[i]
				p.Src = validEndSrcRef
				refProbes[n.ID()] = p
			}
			return nodes, nil
		})
	if err != nil || refProbes == nil {
		return nodes, nil, err
	}
	probes := make([]storecontract.ValidEndProbe, len(nodes))
	for i, n := range nodes {
		probes[i] = refProbes[n.ID()] // zero (Src 0) for another shard's node
	}
	return nodes, probes, nil
}

// RefineNodePropertyValidEnd implements store.PropertyIndexValidEndCapability:
// a reference shard probe's refine goes to the reference shard, any other is
// dropped.
func (ts *Store) RefineNodePropertyValidEnd(n *types.Node, probe storecontract.ValidEndProbe, end types.Instant) {
	if probe.Src != validEndSrcRef || ts.checkOpen() != nil {
		return
	}
	ref, checkin, err := ts.checkoutRefShard()
	if err != nil {
		return
	}
	defer checkin()
	ref.RefineNodePropertyValidEnd(n, probe, end)
}

// PropertyIndexValidEndListed implements
// store.PropertyIndexValidEndListedCapability: a reference label's definition
// the reference shard trusts (an event label's nodes are read whole on their
// shards).
func (ts *Store) PropertyIndexValidEndListed(labelToken uint16, key string) bool {
	names := ts.labelNames.Load()
	if names == nil || !ts.IsReferenceLabel(names.Resolve(labelToken)) {
		return false
	}
	ref, checkin, err := ts.checkoutRefShard()
	if err != nil {
		return false
	}
	defer checkin()
	return ref.PropertyIndexValidEndListed(labelToken, key)
}

// PropertyIndexValidEndStats implements store.PropertyIndexValidEndCapability:
// the reference shard's counts.
func (ts *Store) PropertyIndexValidEndStats() storecontract.PropertyIndexValidEndStats {
	ref, checkin, err := ts.checkoutRefShard()
	if err != nil {
		return storecontract.PropertyIndexValidEndStats{}
	}
	defer checkin()
	return ref.PropertyIndexValidEndStats()
}
