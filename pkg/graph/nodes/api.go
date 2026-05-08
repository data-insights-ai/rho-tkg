// Package nodes is a sub-API accessor exposing the node-management surface of
// a Graph. The package declares a local interface (Ops) listing the methods
// the API forwards; *core.NodeOps implements it implicitly.
package nodes

import (
	"context"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// Ops is the subset of *core.NodeOps the nodes sub-API forwards to.
type Ops interface {
	Add(labels []string, props map[string]any) (*types.Node, error)
	AddWithContext(ctx context.Context, labels []string, props map[string]any) (*types.Node, error)
	Get(id types.NodeID) (*types.Node, error)
	GetWithContext(ctx context.Context, id types.NodeID) (*types.Node, error)
	GetByIDs(ids []types.NodeID) ([]*types.Node, error)
	Update(id types.NodeID, updates map[string]any) (*types.Node, error)
	UpdateWithContext(ctx context.Context, id types.NodeID, updates map[string]any) (*types.Node, error)
	UpdateInPlace(id types.NodeID, updates map[string]any) (*types.Node, error)
	UpdateInPlaceWithContext(ctx context.Context, id types.NodeID, updates map[string]any) (*types.Node, error)
	Delete(id types.NodeID) error
	DeleteWithContext(ctx context.Context, id types.NodeID) error
	Import(ctx context.Context, id types.NodeID, labels []string, props map[string]any) (*types.Node, error)

	All(opts storepkg.QueryOpts) ([]*types.Node, error)
	ByLabel(label string, opts storepkg.QueryOpts) ([]*types.Node, error)
	ByLabelAndProperty(label, key string, value any, opts storepkg.QueryOpts) ([]*types.Node, error)
	Count() (int, error)
	CountByLabel(label string) (int, error)

	SetProperty(id types.NodeID, key string, value any) error
	DeleteProperty(id types.NodeID, key string) error
	CompareAndSetProperty(id types.NodeID, key string, expected, newVal any) (bool, error)
	CompareAndSetPropertyWithContext(ctx context.Context, id types.NodeID, key string, expected, newVal any) (bool, error)

	AddLabel(id types.NodeID, label string) error
	RemoveLabel(id types.NodeID, label string) error
	HasLabel(n *types.Node, label string) bool
	Labels(n *types.Node) []string
	PrimaryLabel(n *types.Node) string

	CloseVersion(id types.NodeID, t types.Instant) error
	History(id types.NodeID) ([]*types.Node, error)
	NextVersion(id types.NodeID, version uint32) (*types.Node, error)
	PreviousVersion(id types.NodeID, version uint32) (*types.Node, error)

	NextID() types.NodeID
}

// API is the nodes sub-API accessor.
type API struct {
	ops Ops
}

// New constructs a nodes sub-API.
func New(ops Ops) *API { return &API{ops: ops} }

// Add creates a new node.
func (a *API) Add(labels []string, props map[string]any) (*types.Node, error) {
	return a.ops.Add(labels, props)
}

// AddWithContext creates a new node honoring ctx.
func (a *API) AddWithContext(ctx context.Context, labels []string, props map[string]any) (*types.Node, error) {
	return a.ops.AddWithContext(ctx, labels, props)
}

// Get returns the node with the given ID.
func (a *API) Get(id types.NodeID) (*types.Node, error) { return a.ops.Get(id) }

// GetWithContext returns the node honoring ctx.
func (a *API) GetWithContext(ctx context.Context, id types.NodeID) (*types.Node, error) {
	return a.ops.GetWithContext(ctx, id)
}

// GetByIDs returns nodes for the given IDs.
func (a *API) GetByIDs(ids []types.NodeID) ([]*types.Node, error) { return a.ops.GetByIDs(ids) }

// Update updates a node's labels/properties.
func (a *API) Update(id types.NodeID, updates map[string]any) (*types.Node, error) {
	return a.ops.Update(id, updates)
}

// UpdateWithContext updates a node honoring ctx.
func (a *API) UpdateWithContext(ctx context.Context, id types.NodeID, updates map[string]any) (*types.Node, error) {
	return a.ops.UpdateWithContext(ctx, id, updates)
}

// UpdateInPlace updates a node in place.
func (a *API) UpdateInPlace(id types.NodeID, updates map[string]any) (*types.Node, error) {
	return a.ops.UpdateInPlace(id, updates)
}

// UpdateInPlaceWithContext updates a node in place honoring ctx.
func (a *API) UpdateInPlaceWithContext(ctx context.Context, id types.NodeID, updates map[string]any) (*types.Node, error) {
	return a.ops.UpdateInPlaceWithContext(ctx, id, updates)
}

// Delete deletes the node.
func (a *API) Delete(id types.NodeID) error { return a.ops.Delete(id) }

// DeleteWithContext deletes the node honoring ctx.
func (a *API) DeleteWithContext(ctx context.Context, id types.NodeID) error {
	return a.ops.DeleteWithContext(ctx, id)
}

// Import imports a node with a caller-supplied ID.
func (a *API) Import(ctx context.Context, id types.NodeID, labels []string, props map[string]any) (*types.Node, error) {
	return a.ops.Import(ctx, id, labels, props)
}

// All returns all nodes matching opts.
func (a *API) All(opts storepkg.QueryOpts) ([]*types.Node, error) { return a.ops.All(opts) }

// ByLabel returns nodes carrying the given label.
func (a *API) ByLabel(label string, opts storepkg.QueryOpts) ([]*types.Node, error) {
	return a.ops.ByLabel(label, opts)
}

// ByLabelAndProperty returns nodes carrying the label whose property matches value.
func (a *API) ByLabelAndProperty(label, key string, value any, opts storepkg.QueryOpts) ([]*types.Node, error) {
	return a.ops.ByLabelAndProperty(label, key, value, opts)
}

// Count returns the total node count.
func (a *API) Count() (int, error) { return a.ops.Count() }

// CountByLabel returns the count of nodes carrying the label.
func (a *API) CountByLabel(label string) (int, error) { return a.ops.CountByLabel(label) }

// SetProperty sets a single property.
func (a *API) SetProperty(id types.NodeID, key string, value any) error {
	return a.ops.SetProperty(id, key, value)
}

// DeleteProperty deletes a single property.
func (a *API) DeleteProperty(id types.NodeID, key string) error {
	return a.ops.DeleteProperty(id, key)
}

// CompareAndSetProperty atomically updates a property.
func (a *API) CompareAndSetProperty(id types.NodeID, key string, expected, newVal any) (bool, error) {
	return a.ops.CompareAndSetProperty(id, key, expected, newVal)
}

// CompareAndSetPropertyWithContext atomically updates a property honoring ctx.
func (a *API) CompareAndSetPropertyWithContext(ctx context.Context, id types.NodeID, key string, expected, newVal any) (bool, error) {
	return a.ops.CompareAndSetPropertyWithContext(ctx, id, key, expected, newVal)
}

// AddLabel adds a label to a node.
func (a *API) AddLabel(id types.NodeID, label string) error { return a.ops.AddLabel(id, label) }

// RemoveLabel removes a label from a node.
func (a *API) RemoveLabel(id types.NodeID, label string) error {
	return a.ops.RemoveLabel(id, label)
}

// HasLabel reports whether the node carries the label.
func (a *API) HasLabel(n *types.Node, label string) bool { return a.ops.HasLabel(n, label) }

// Labels returns the node's labels.
func (a *API) Labels(n *types.Node) []string { return a.ops.Labels(n) }

// PrimaryLabel returns the node's primary label.
func (a *API) PrimaryLabel(n *types.Node) string { return a.ops.PrimaryLabel(n) }

// CloseVersion closes the open-ended ValidTo of the current node version.
func (a *API) CloseVersion(id types.NodeID, t types.Instant) error {
	return a.ops.CloseVersion(id, t)
}

// History returns the version chain for a node.
func (a *API) History(id types.NodeID) ([]*types.Node, error) { return a.ops.History(id) }

// NextVersion returns the next version for the given node.
func (a *API) NextVersion(id types.NodeID, version uint32) (*types.Node, error) {
	return a.ops.NextVersion(id, version)
}

// PreviousVersion returns the previous version for the given node.
func (a *API) PreviousVersion(id types.NodeID, version uint32) (*types.Node, error) {
	return a.ops.PreviousVersion(id, version)
}

// NextID generates and returns the next node ID.
func (a *API) NextID() types.NodeID { return a.ops.NextID() }
