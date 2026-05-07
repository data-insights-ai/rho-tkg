// Package nodes is a sub-API accessor exposing the node-management surface of
// a Graph. The package declares a local interface (Core) listing only the
// methods the API forwards; *graph.Graph satisfies it implicitly. This avoids
// an import cycle with pkg/graph while still letting customers write
// g.Nodes.Add(...) for discoverability.
package nodes

import (
	"context"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// Core is the subset of *graph.Graph methods the nodes sub-API forwards to.
// *graph.Graph implements this interface implicitly.
type Core interface {
	AddNode(labels []string, props map[string]any) (*types.Node, error)
	AddNodeWithContext(ctx context.Context, labels []string, props map[string]any) (*types.Node, error)
	GetNode(id types.NodeID) (*types.Node, error)
	GetNodeWithContext(ctx context.Context, id types.NodeID) (*types.Node, error)
	GetNodesByIDs(ids []types.NodeID) ([]*types.Node, error)
	UpdateNode(id types.NodeID, updates map[string]any) (*types.Node, error)
	UpdateNodeWithContext(ctx context.Context, id types.NodeID, updates map[string]any) (*types.Node, error)
	UpdateNodeInPlace(id types.NodeID, updates map[string]any) (*types.Node, error)
	UpdateNodeInPlaceWithContext(ctx context.Context, id types.NodeID, updates map[string]any) (*types.Node, error)
	DeleteNode(id types.NodeID) error
	DeleteNodeWithContext(ctx context.Context, id types.NodeID) error
	ImportNodeWithID(ctx context.Context, id types.NodeID, labels []string, props map[string]any) (*types.Node, error)

	// Reads
	AllNodes(opts storepkg.QueryOpts) ([]*types.Node, error)
	NodesByLabel(label string, opts storepkg.QueryOpts) ([]*types.Node, error)
	NodesByLabelAndProperty(label, key string, value any, opts storepkg.QueryOpts) ([]*types.Node, error)
	NodeCount() (int, error)
	NodeCountByLabel(label string) (int, error)

	// Properties
	SetNodeProperty(id types.NodeID, key string, value any) error
	DeleteNodeProperty(id types.NodeID, key string) error
	CompareAndSetProperty(id types.NodeID, key string, expected, newVal any) (bool, error)
	CompareAndSetPropertyWithContext(ctx context.Context, id types.NodeID, key string, expected, newVal any) (bool, error)

	// Labels
	AddNodeLabel(id types.NodeID, label string) error
	RemoveNodeLabel(id types.NodeID, label string) error
	NodeHasLabel(n *types.Node, label string) bool
	NodeLabels(n *types.Node) []string
	NodePrimaryLabel(n *types.Node) string

	// History/version chain
	CloseNodeVersion(id types.NodeID, t types.Instant) error
	GetNodeHistory(id types.NodeID) ([]*types.Node, error)
	GetNextNodeVersion(id types.NodeID, version uint32) (*types.Node, error)
	GetPreviousNodeVersion(id types.NodeID, version uint32) (*types.Node, error)

	// ID generation
	NextNodeID() types.NodeID
}

// API is the nodes sub-API accessor.
type API struct {
	c Core
}

// New constructs a nodes sub-API. Used internally by Graph.New.
func New(c Core) *API {
	return &API{c: c}
}

// Add creates a new node. Forwards to Graph.AddNode.
func (a *API) Add(labels []string, props map[string]any) (*types.Node, error) {
	return a.c.AddNode(labels, props)
}

// AddWithContext creates a new node honoring ctx. Forwards to Graph.AddNodeWithContext.
func (a *API) AddWithContext(ctx context.Context, labels []string, props map[string]any) (*types.Node, error) {
	return a.c.AddNodeWithContext(ctx, labels, props)
}

// Get returns the node with the given ID. Forwards to Graph.GetNode.
func (a *API) Get(id types.NodeID) (*types.Node, error) { return a.c.GetNode(id) }

// GetWithContext returns the node honoring ctx. Forwards to Graph.GetNodeWithContext.
func (a *API) GetWithContext(ctx context.Context, id types.NodeID) (*types.Node, error) {
	return a.c.GetNodeWithContext(ctx, id)
}

// GetByIDs returns nodes for the given IDs. Forwards to Graph.GetNodesByIDs.
func (a *API) GetByIDs(ids []types.NodeID) ([]*types.Node, error) { return a.c.GetNodesByIDs(ids) }

// Update updates a node's labels/properties. Forwards to Graph.UpdateNode.
func (a *API) Update(id types.NodeID, updates map[string]any) (*types.Node, error) {
	return a.c.UpdateNode(id, updates)
}

// UpdateWithContext updates a node honoring ctx. Forwards to Graph.UpdateNodeWithContext.
func (a *API) UpdateWithContext(ctx context.Context, id types.NodeID, updates map[string]any) (*types.Node, error) {
	return a.c.UpdateNodeWithContext(ctx, id, updates)
}

// UpdateInPlace updates a node in place. Forwards to Graph.UpdateNodeInPlace.
func (a *API) UpdateInPlace(id types.NodeID, updates map[string]any) (*types.Node, error) {
	return a.c.UpdateNodeInPlace(id, updates)
}

// UpdateInPlaceWithContext updates a node in place honoring ctx. Forwards to Graph.UpdateNodeInPlaceWithContext.
func (a *API) UpdateInPlaceWithContext(ctx context.Context, id types.NodeID, updates map[string]any) (*types.Node, error) {
	return a.c.UpdateNodeInPlaceWithContext(ctx, id, updates)
}

// Delete deletes the node. Forwards to Graph.DeleteNode.
func (a *API) Delete(id types.NodeID) error { return a.c.DeleteNode(id) }

// DeleteWithContext deletes the node honoring ctx. Forwards to Graph.DeleteNodeWithContext.
func (a *API) DeleteWithContext(ctx context.Context, id types.NodeID) error {
	return a.c.DeleteNodeWithContext(ctx, id)
}

// Import imports a node with a caller-supplied ID. Forwards to Graph.ImportNodeWithID.
func (a *API) Import(ctx context.Context, id types.NodeID, labels []string, props map[string]any) (*types.Node, error) {
	return a.c.ImportNodeWithID(ctx, id, labels, props)
}

// All returns all nodes matching opts. Forwards to Graph.AllNodes.
func (a *API) All(opts storepkg.QueryOpts) ([]*types.Node, error) { return a.c.AllNodes(opts) }

// ByLabel returns nodes carrying the given label. Forwards to Graph.NodesByLabel.
func (a *API) ByLabel(label string, opts storepkg.QueryOpts) ([]*types.Node, error) {
	return a.c.NodesByLabel(label, opts)
}

// ByLabelAndProperty returns nodes carrying the label whose property matches value. Forwards to Graph.NodesByLabelAndProperty.
func (a *API) ByLabelAndProperty(label, key string, value any, opts storepkg.QueryOpts) ([]*types.Node, error) {
	return a.c.NodesByLabelAndProperty(label, key, value, opts)
}

// Count returns the total node count. Forwards to Graph.NodeCount.
func (a *API) Count() (int, error) { return a.c.NodeCount() }

// CountByLabel returns the count of nodes carrying the label. Forwards to Graph.NodeCountByLabel.
func (a *API) CountByLabel(label string) (int, error) { return a.c.NodeCountByLabel(label) }

// SetProperty sets a single property. Forwards to Graph.SetNodeProperty.
func (a *API) SetProperty(id types.NodeID, key string, value any) error {
	return a.c.SetNodeProperty(id, key, value)
}

// DeleteProperty deletes a single property. Forwards to Graph.DeleteNodeProperty.
func (a *API) DeleteProperty(id types.NodeID, key string) error {
	return a.c.DeleteNodeProperty(id, key)
}

// CompareAndSetProperty atomically updates a property. Forwards to Graph.CompareAndSetProperty.
func (a *API) CompareAndSetProperty(id types.NodeID, key string, expected, newVal any) (bool, error) {
	return a.c.CompareAndSetProperty(id, key, expected, newVal)
}

// CompareAndSetPropertyWithContext atomically updates a property honoring ctx. Forwards to Graph.CompareAndSetPropertyWithContext.
func (a *API) CompareAndSetPropertyWithContext(ctx context.Context, id types.NodeID, key string, expected, newVal any) (bool, error) {
	return a.c.CompareAndSetPropertyWithContext(ctx, id, key, expected, newVal)
}

// AddLabel adds a label to a node. Forwards to Graph.AddNodeLabel.
func (a *API) AddLabel(id types.NodeID, label string) error { return a.c.AddNodeLabel(id, label) }

// RemoveLabel removes a label from a node. Forwards to Graph.RemoveNodeLabel.
func (a *API) RemoveLabel(id types.NodeID, label string) error {
	return a.c.RemoveNodeLabel(id, label)
}

// HasLabel reports whether the node carries the label. Forwards to Graph.NodeHasLabel.
func (a *API) HasLabel(n *types.Node, label string) bool { return a.c.NodeHasLabel(n, label) }

// Labels returns the node's labels. Forwards to Graph.NodeLabels.
func (a *API) Labels(n *types.Node) []string { return a.c.NodeLabels(n) }

// PrimaryLabel returns the node's primary label. Forwards to Graph.NodePrimaryLabel.
func (a *API) PrimaryLabel(n *types.Node) string { return a.c.NodePrimaryLabel(n) }

// CloseVersion closes the open-ended ValidTo of the current node version. Forwards to Graph.CloseNodeVersion.
func (a *API) CloseVersion(id types.NodeID, t types.Instant) error {
	return a.c.CloseNodeVersion(id, t)
}

// History returns the version chain for a node. Forwards to Graph.GetNodeHistory.
func (a *API) History(id types.NodeID) ([]*types.Node, error) { return a.c.GetNodeHistory(id) }

// NextVersion returns the next version for the given node. Forwards to Graph.GetNextNodeVersion.
func (a *API) NextVersion(id types.NodeID, version uint32) (*types.Node, error) {
	return a.c.GetNextNodeVersion(id, version)
}

// PreviousVersion returns the previous version for the given node. Forwards to Graph.GetPreviousNodeVersion.
func (a *API) PreviousVersion(id types.NodeID, version uint32) (*types.Node, error) {
	return a.c.GetPreviousNodeVersion(id, version)
}

// NextID generates and returns the next node ID. Forwards to Graph.NextNodeID.
func (a *API) NextID() types.NodeID { return a.c.NextNodeID() }
