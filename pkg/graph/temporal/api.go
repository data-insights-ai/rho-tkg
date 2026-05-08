// API accessors for the Graph temporal-query surface (point-in-time, interval,
// bitemporal, snapshot/diff, Allen). Lives in the same package as the temporal
// types it returns (GraphSnapshot, SnapshotDiff, …) — collapsed in v3.4.0
// post-cleanup from the previous pkg/graph/temporalapi sibling.
package temporal

import (
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// Ops is the subset of *core.TempOps the temporal sub-API forwards to.
type Ops interface {
	// Point-in-time
	NodeAt(id types.NodeID, t types.Instant) (*types.Node, error)
	NodesAt(t types.Instant) ([]*types.Node, error)
	NodesByLabelAt(label string, t types.Instant) ([]*types.Node, error)
	RelAt(id types.RelID, t types.Instant) (*types.Relationship, error)
	RelationshipsAt(t types.Instant) ([]*types.Relationship, error)
	RelationshipsByTypeAt(relType string, t types.Instant) ([]*types.Relationship, error)
	NeighborsAt(nodeID types.NodeID, t types.Instant) ([]*types.Node, error)
	NodesByLabelPropertyAt(label, key string, value any, t types.Instant) ([]*types.Node, error)
	RelsByTypePropertyAt(relType, key string, value any, t types.Instant) ([]*types.Relationship, error)

	// Interval
	NodesDuring(start, end types.Instant) ([]*types.Node, error)
	RelationshipsDuring(start, end types.Instant) ([]*types.Relationship, error)
	NodesByLabelPropertyDuring(label, key string, value any, start, end types.Instant) ([]*types.Node, error)
	RelsByTypePropertyDuring(relType, key string, value any, start, end types.Instant) ([]*types.Relationship, error)

	// Bitemporal (transaction time)
	NodeAsOf(id types.NodeID, txTime types.Instant) (*types.Node, error)
	RelAsOf(id types.RelID, txTime types.Instant) (*types.Relationship, error)
	NodesAsOf(txTime types.Instant) ([]*types.Node, error)
	RelsAsOf(txTime types.Instant) ([]*types.Relationship, error)

	// Snapshot/Diff
	Snapshot(t types.Instant) (*GraphSnapshot, error)
	Diff(t1, t2 types.Instant) (*SnapshotDiff, error)
	DiffCallback(t1, t2 types.Instant, h DiffHandlers) error

	// Allen's interval algebra
	NodeInterval(n *types.Node) (start, end types.Instant, err error)
	RelInterval(r *types.Relationship) (start, end types.Instant, err error)
	RelateNodes(a, b *types.Node) (types.AllenRelation, error)
	RelateRels(a, b *types.Relationship) (types.AllenRelation, error)
}

// API is the temporal sub-API accessor.
type API struct{ ops Ops }

// New constructs a temporal sub-API.
func New(ops Ops) *API { return &API{ops: ops} }

// NodeAt returns the version of the node valid at t.
func (a *API) NodeAt(id types.NodeID, t types.Instant) (*types.Node, error) {
	return a.ops.NodeAt(id, t)
}

// NodesAt returns the set of nodes valid at t.
func (a *API) NodesAt(t types.Instant) ([]*types.Node, error) { return a.ops.NodesAt(t) }

// NodesByLabelAt returns nodes labeled at t.
func (a *API) NodesByLabelAt(label string, t types.Instant) ([]*types.Node, error) {
	return a.ops.NodesByLabelAt(label, t)
}

// RelAt returns the version of the relationship valid at t.
func (a *API) RelAt(id types.RelID, t types.Instant) (*types.Relationship, error) {
	return a.ops.RelAt(id, t)
}

// RelationshipsAt returns relationships valid at t.
func (a *API) RelationshipsAt(t types.Instant) ([]*types.Relationship, error) {
	return a.ops.RelationshipsAt(t)
}

// RelationshipsByTypeAt returns relationships of relType valid at t.
func (a *API) RelationshipsByTypeAt(relType string, t types.Instant) ([]*types.Relationship, error) {
	return a.ops.RelationshipsByTypeAt(relType, t)
}

// NeighborsAt returns the neighbours of nodeID valid at t.
func (a *API) NeighborsAt(nodeID types.NodeID, t types.Instant) ([]*types.Node, error) {
	return a.ops.NeighborsAt(nodeID, t)
}

// NodesByLabelPropertyAt returns nodes labeled with the given (label,key,value) at t.
func (a *API) NodesByLabelPropertyAt(label, key string, value any, t types.Instant) ([]*types.Node, error) {
	return a.ops.NodesByLabelPropertyAt(label, key, value, t)
}

// RelsByTypePropertyAt returns relationships of relType with property at t.
func (a *API) RelsByTypePropertyAt(relType, key string, value any, t types.Instant) ([]*types.Relationship, error) {
	return a.ops.RelsByTypePropertyAt(relType, key, value, t)
}

// NodesDuring returns nodes whose validity overlaps [start,end).
func (a *API) NodesDuring(start, end types.Instant) ([]*types.Node, error) {
	return a.ops.NodesDuring(start, end)
}

// RelationshipsDuring returns relationships whose validity overlaps [start,end).
func (a *API) RelationshipsDuring(start, end types.Instant) ([]*types.Relationship, error) {
	return a.ops.RelationshipsDuring(start, end)
}

// NodesByLabelPropertyDuring returns nodes labeled with property during [start,end).
func (a *API) NodesByLabelPropertyDuring(label, key string, value any, start, end types.Instant) ([]*types.Node, error) {
	return a.ops.NodesByLabelPropertyDuring(label, key, value, start, end)
}

// RelsByTypePropertyDuring returns relationships of relType with property during [start,end).
func (a *API) RelsByTypePropertyDuring(relType, key string, value any, start, end types.Instant) ([]*types.Relationship, error) {
	return a.ops.RelsByTypePropertyDuring(relType, key, value, start, end)
}

// NodeAsOf returns the version of node id known at transaction time txTime.
func (a *API) NodeAsOf(id types.NodeID, txTime types.Instant) (*types.Node, error) {
	return a.ops.NodeAsOf(id, txTime)
}

// RelAsOf returns the version of relationship id known at transaction time txTime.
func (a *API) RelAsOf(id types.RelID, txTime types.Instant) (*types.Relationship, error) {
	return a.ops.RelAsOf(id, txTime)
}

// NodesAsOf returns nodes known at transaction time txTime.
func (a *API) NodesAsOf(txTime types.Instant) ([]*types.Node, error) {
	return a.ops.NodesAsOf(txTime)
}

// RelsAsOf returns relationships known at transaction time txTime.
func (a *API) RelsAsOf(txTime types.Instant) ([]*types.Relationship, error) {
	return a.ops.RelsAsOf(txTime)
}

// Snapshot captures the graph state at t.
func (a *API) Snapshot(t types.Instant) (*GraphSnapshot, error) { return a.ops.Snapshot(t) }

// Diff returns the snapshot diff between t1 and t2.
func (a *API) Diff(t1, t2 types.Instant) (*SnapshotDiff, error) {
	return a.ops.Diff(t1, t2)
}

// DiffCallback streams the diff between t1 and t2 through the given handlers.
func (a *API) DiffCallback(t1, t2 types.Instant, h DiffHandlers) error {
	return a.ops.DiffCallback(t1, t2, h)
}

// NodeInterval returns the interval [valid_from, valid_to) for n.
func (a *API) NodeInterval(n *types.Node) (start, end types.Instant, err error) {
	return a.ops.NodeInterval(n)
}

// RelInterval returns the interval [valid_from, valid_to) for r.
func (a *API) RelInterval(r *types.Relationship) (start, end types.Instant, err error) {
	return a.ops.RelInterval(r)
}

// RelateNodes returns the Allen relation between the validity intervals of x and y.
func (a *API) RelateNodes(x, y *types.Node) (types.AllenRelation, error) {
	return a.ops.RelateNodes(x, y)
}

// RelateRels returns the Allen relation between the validity intervals of x and y.
func (a *API) RelateRels(x, y *types.Relationship) (types.AllenRelation, error) {
	return a.ops.RelateRels(x, y)
}
