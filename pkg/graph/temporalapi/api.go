// Package temporalapi is a sub-API accessor exposing the Graph temporal-query
// surface (point-in-time, interval, bitemporal, snapshot/diff, Allen). The
// temporal types themselves live in pkg/graph/temporal.
package temporalapi

import (
	temporalpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/temporal"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// Core is the subset of *graph.Graph methods the temporal sub-API forwards to.
type Core interface {
	// Point-in-time
	GetNodeAt(id types.NodeID, t types.Instant) (*types.Node, error)
	GetNodesValidAt(t types.Instant) ([]*types.Node, error)
	GetNodesByLabelValidAt(label string, t types.Instant) ([]*types.Node, error)
	GetRelAt(id types.RelID, t types.Instant) (*types.Relationship, error)
	GetRelationshipsValidAt(t types.Instant) ([]*types.Relationship, error)
	GetRelationshipsByTypeValidAt(relType string, t types.Instant) ([]*types.Relationship, error)
	GetNeighborsValidAt(nodeID types.NodeID, t types.Instant) ([]*types.Node, error)
	NodesByLabelPropertyAndTime(label, key string, value any, t types.Instant) ([]*types.Node, error)
	RelationshipsByTypePropertyAndTime(relType, key string, value any, t types.Instant) ([]*types.Relationship, error)

	// Interval
	GetNodesValidDuring(start, end types.Instant) ([]*types.Node, error)
	GetRelationshipsValidDuring(start, end types.Instant) ([]*types.Relationship, error)
	NodesByLabelPropertyDuring(label, key string, value any, start, end types.Instant) ([]*types.Node, error)
	RelationshipsByTypePropertyDuring(relType, key string, value any, start, end types.Instant) ([]*types.Relationship, error)

	// Bitemporal (transaction time)
	GetNodeAsOf(id types.NodeID, txTime types.Instant) (*types.Node, error)
	GetRelAsOf(id types.RelID, txTime types.Instant) (*types.Relationship, error)
	GetNodesAsOf(txTime types.Instant) ([]*types.Node, error)
	GetRelsAsOf(txTime types.Instant) ([]*types.Relationship, error)

	// Snapshot/Diff
	Snapshot(t types.Instant) (*temporalpkg.GraphSnapshot, error)
	DiffSnapshots(t1, t2 types.Instant) (*temporalpkg.SnapshotDiff, error)
	DiffSnapshotsCallback(t1, t2 types.Instant, h temporalpkg.DiffHandlers) error

	// Allen's interval algebra
	NodeInterval(n *types.Node) (start, end types.Instant, err error)
	RelInterval(r *types.Relationship) (start, end types.Instant, err error)
	RelateNodes(a, b *types.Node) (types.AllenRelation, error)
	RelateRels(a, b *types.Relationship) (types.AllenRelation, error)
}

// API is the temporal sub-API accessor.
type API struct{ c Core }

// New constructs a temporal sub-API.
func New(c Core) *API { return &API{c: c} }

// NodeAt returns the version of the node valid at t. Forwards to Graph.GetNodeAt.
func (a *API) NodeAt(id types.NodeID, t types.Instant) (*types.Node, error) {
	return a.c.GetNodeAt(id, t)
}

// NodesAt returns all nodes valid at t. Forwards to Graph.GetNodesValidAt.
func (a *API) NodesAt(t types.Instant) ([]*types.Node, error) { return a.c.GetNodesValidAt(t) }

// NodesByLabelAt returns nodes carrying label valid at t. Forwards to Graph.GetNodesByLabelValidAt.
func (a *API) NodesByLabelAt(label string, t types.Instant) ([]*types.Node, error) {
	return a.c.GetNodesByLabelValidAt(label, t)
}

// RelAt returns the version of the relationship valid at t. Forwards to Graph.GetRelAt.
func (a *API) RelAt(id types.RelID, t types.Instant) (*types.Relationship, error) {
	return a.c.GetRelAt(id, t)
}

// RelationshipsAt returns all relationships valid at t. Forwards to Graph.GetRelationshipsValidAt.
func (a *API) RelationshipsAt(t types.Instant) ([]*types.Relationship, error) {
	return a.c.GetRelationshipsValidAt(t)
}

// RelationshipsByTypeAt returns relationships of relType valid at t. Forwards to Graph.GetRelationshipsByTypeValidAt.
func (a *API) RelationshipsByTypeAt(relType string, t types.Instant) ([]*types.Relationship, error) {
	return a.c.GetRelationshipsByTypeValidAt(relType, t)
}

// NeighborsAt returns the neighbors of nodeID valid at t. Forwards to Graph.GetNeighborsValidAt.
func (a *API) NeighborsAt(nodeID types.NodeID, t types.Instant) ([]*types.Node, error) {
	return a.c.GetNeighborsValidAt(nodeID, t)
}

// NodesByLabelPropertyAt returns nodes with label/key=value valid at t. Forwards to Graph.NodesByLabelPropertyAndTime.
func (a *API) NodesByLabelPropertyAt(label, key string, value any, t types.Instant) ([]*types.Node, error) {
	return a.c.NodesByLabelPropertyAndTime(label, key, value, t)
}

// RelsByTypePropertyAt returns relationships of relType with key=value valid at t. Forwards to Graph.RelationshipsByTypePropertyAndTime.
func (a *API) RelsByTypePropertyAt(relType, key string, value any, t types.Instant) ([]*types.Relationship, error) {
	return a.c.RelationshipsByTypePropertyAndTime(relType, key, value, t)
}

// NodesDuring returns nodes valid during [start, end). Forwards to Graph.GetNodesValidDuring.
func (a *API) NodesDuring(start, end types.Instant) ([]*types.Node, error) {
	return a.c.GetNodesValidDuring(start, end)
}

// RelationshipsDuring returns relationships valid during [start, end). Forwards to Graph.GetRelationshipsValidDuring.
func (a *API) RelationshipsDuring(start, end types.Instant) ([]*types.Relationship, error) {
	return a.c.GetRelationshipsValidDuring(start, end)
}

// NodesByLabelPropertyDuring returns nodes with label/key=value during [start, end). Forwards to Graph.NodesByLabelPropertyDuring.
func (a *API) NodesByLabelPropertyDuring(label, key string, value any, start, end types.Instant) ([]*types.Node, error) {
	return a.c.NodesByLabelPropertyDuring(label, key, value, start, end)
}

// RelsByTypePropertyDuring returns relationships of relType with key=value during [start, end). Forwards to Graph.RelationshipsByTypePropertyDuring.
func (a *API) RelsByTypePropertyDuring(relType, key string, value any, start, end types.Instant) ([]*types.Relationship, error) {
	return a.c.RelationshipsByTypePropertyDuring(relType, key, value, start, end)
}

// NodeAsOf returns the node version that was current at transaction time txTime. Forwards to Graph.GetNodeAsOf.
func (a *API) NodeAsOf(id types.NodeID, txTime types.Instant) (*types.Node, error) {
	return a.c.GetNodeAsOf(id, txTime)
}

// RelAsOf returns the relationship version that was current at transaction time txTime. Forwards to Graph.GetRelAsOf.
func (a *API) RelAsOf(id types.RelID, txTime types.Instant) (*types.Relationship, error) {
	return a.c.GetRelAsOf(id, txTime)
}

// NodesAsOf returns all node versions that were current at transaction time txTime. Forwards to Graph.GetNodesAsOf.
func (a *API) NodesAsOf(txTime types.Instant) ([]*types.Node, error) {
	return a.c.GetNodesAsOf(txTime)
}

// RelsAsOf returns all relationship versions that were current at transaction time txTime. Forwards to Graph.GetRelsAsOf.
func (a *API) RelsAsOf(txTime types.Instant) ([]*types.Relationship, error) {
	return a.c.GetRelsAsOf(txTime)
}

// Snapshot returns the full graph state at t. Forwards to Graph.Snapshot.
func (a *API) Snapshot(t types.Instant) (*temporalpkg.GraphSnapshot, error) { return a.c.Snapshot(t) }

// Diff compares two snapshots taken at t1 and t2. Forwards to Graph.DiffSnapshots.
func (a *API) Diff(t1, t2 types.Instant) (*temporalpkg.SnapshotDiff, error) {
	return a.c.DiffSnapshots(t1, t2)
}

// DiffCallback streams entity changes between t1 and t2 via the supplied
// handlers instead of materialising both snapshots in RAM. RAM is bounded
// by O(|distinct entity IDs|) — the dedup ID set — plus one entity version
// pair at a time. nil handler fields are skipped; returning a non-nil
// error from any handler aborts iteration and propagates the error.
// Forwards to Graph.DiffSnapshotsCallback.
func (a *API) DiffCallback(t1, t2 types.Instant, h temporalpkg.DiffHandlers) error {
	return a.c.DiffSnapshotsCallback(t1, t2, h)
}

// NodeInterval returns the validity interval of a node version. Forwards to Graph.NodeInterval.
func (a *API) NodeInterval(n *types.Node) (start, end types.Instant, err error) {
	return a.c.NodeInterval(n)
}

// RelInterval returns the validity interval of a relationship version. Forwards to Graph.RelInterval.
func (a *API) RelInterval(r *types.Relationship) (start, end types.Instant, err error) {
	return a.c.RelInterval(r)
}

// RelateNodes returns the Allen interval relation between two node versions. Forwards to Graph.RelateNodes.
func (a *API) RelateNodes(x, y *types.Node) (types.AllenRelation, error) {
	return a.c.RelateNodes(x, y)
}

// RelateRels returns the Allen interval relation between two relationship versions. Forwards to Graph.RelateRels.
func (a *API) RelateRels(x, y *types.Relationship) (types.AllenRelation, error) {
	return a.c.RelateRels(x, y)
}
