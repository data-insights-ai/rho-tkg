// Package nodes is a sub-API accessor exposing the node-management surface of
// a Graph. The package declares a local interface (Ops) listing the methods
// the API forwards; *core.NodeOps implements it implicitly.
//
// API 4.0 change: methods that previously came in (Foo, FooWithContext) pairs
// (Add, Get, Update, UpdateInPlace, Delete, CompareAndSetProperty) collapsed
// into a single context-aware method each. Pass context.Background() for the
// previous no-context behavior. The historical *WithContext variants no
// longer exist.
package nodes

import (
	"context"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/grapherr"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// Ops is the subset of *core.NodeOps the nodes sub-API forwards to.
type Ops interface {
	Add(ctx context.Context, labels []string, props map[string]any) (*types.Node, error)
	Get(ctx context.Context, id types.NodeID) (*types.Node, error)
	GetByIDs(ids []types.NodeID) ([]*types.Node, error)
	Update(ctx context.Context, id types.NodeID, updates map[string]any) (*types.Node, error)
	UpdateInPlace(ctx context.Context, id types.NodeID, updates map[string]any) (*types.Node, error)
	Delete(ctx context.Context, id types.NodeID) error
	Import(ctx context.Context, id types.NodeID, labels []string, props map[string]any) (*types.Node, error)

	All(opts storepkg.QueryOpts) ([]*types.Node, error)
	ByLabel(label string, opts storepkg.QueryOpts) ([]*types.Node, error)
	ByLabelAndProperty(label, key string, value any, opts storepkg.QueryOpts) ([]*types.Node, error)
	Count() (int, error)
	CountByLabel(label string) (int, error)

	SetProperty(id types.NodeID, key string, value any) error
	DeleteProperty(id types.NodeID, key string) error
	CompareAndSetProperty(ctx context.Context, id types.NodeID, key string, expected, newVal any) (bool, error)

	AddLabel(id types.NodeID, label string) error
	RemoveLabel(id types.NodeID, label string) error
	HasLabel(n *types.Node, label string) bool
	Labels(n *types.Node) []string
	PrimaryLabel(n *types.Node) string

	CloseVersion(id types.NodeID, t types.Instant) error
	History(id types.NodeID) ([]*types.Node, error)
	VersionAfter(id types.NodeID, version uint32) (*types.Node, error)
	VersionBefore(id types.NodeID, version uint32) (*types.Node, error)

	NextID() types.NodeID
}

// API is the nodes sub-API accessor.
type API struct {
	ops Ops
	ok  bool
}

// New constructs a nodes sub-API.
func New(ops Ops) *API { return &API{ops: ops, ok: !grapherr.IsNil(ops)} }

func (a *API) ready() (Ops, error) {
	if a == nil || !a.ok {
		return nil, grapherr.ErrNilGraph
	}
	return a.ops, nil
}

// Add creates a new node honoring ctx.
func (a *API) Add(ctx context.Context, labels []string, props map[string]any) (*types.Node, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.Add(ctx, labels, props)
}

// Get returns the node with the given ID, honoring ctx.
func (a *API) Get(ctx context.Context, id types.NodeID) (*types.Node, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.Get(ctx, id)
}

// GetByIDs returns nodes for the given IDs.
func (a *API) GetByIDs(ids []types.NodeID) ([]*types.Node, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.GetByIDs(ids)
}

// Update updates a node's labels/properties honoring ctx.
func (a *API) Update(ctx context.Context, id types.NodeID, updates map[string]any) (*types.Node, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.Update(ctx, id, updates)
}

// UpdateInPlace updates a node in place honoring ctx (no version chain entry).
func (a *API) UpdateInPlace(ctx context.Context, id types.NodeID, updates map[string]any) (*types.Node, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.UpdateInPlace(ctx, id, updates)
}

// Delete deletes the node honoring ctx.
func (a *API) Delete(ctx context.Context, id types.NodeID) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.Delete(ctx, id)
}

// Import imports a node with a caller-supplied ID honoring ctx.
func (a *API) Import(ctx context.Context, id types.NodeID, labels []string, props map[string]any) (*types.Node, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.Import(ctx, id, labels, props)
}

// All returns all nodes matching opts.
func (a *API) All(opts storepkg.QueryOpts) ([]*types.Node, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.All(opts)
}

// ByLabel returns nodes carrying the given label.
func (a *API) ByLabel(label string, opts storepkg.QueryOpts) ([]*types.Node, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.ByLabel(label, opts)
}

// ByLabelAndProperty returns nodes carrying the label whose property matches value.
func (a *API) ByLabelAndProperty(label, key string, value any, opts storepkg.QueryOpts) ([]*types.Node, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.ByLabelAndProperty(label, key, value, opts)
}

// Count returns the total node count.
func (a *API) Count() (int, error) {
	ops, err := a.ready()
	if err != nil {
		return 0, err
	}
	return ops.Count()
}

// CountByLabel returns the count of nodes carrying the label.
func (a *API) CountByLabel(label string) (int, error) {
	ops, err := a.ready()
	if err != nil {
		return 0, err
	}
	return ops.CountByLabel(label)
}

// SetProperty sets a single property (internally uses context.Background()).
func (a *API) SetProperty(id types.NodeID, key string, value any) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.SetProperty(id, key, value)
}

// DeleteProperty deletes a single property (internally uses context.Background()).
func (a *API) DeleteProperty(id types.NodeID, key string) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.DeleteProperty(id, key)
}

// CompareAndSetProperty atomically updates a property honoring ctx.
func (a *API) CompareAndSetProperty(ctx context.Context, id types.NodeID, key string, expected, newVal any) (bool, error) {
	ops, err := a.ready()
	if err != nil {
		return false, err
	}
	return ops.CompareAndSetProperty(ctx, id, key, expected, newVal)
}

// AddLabel adds a label to a node.
func (a *API) AddLabel(id types.NodeID, label string) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.AddLabel(id, label)
}

// RemoveLabel removes a label from a node.
func (a *API) RemoveLabel(id types.NodeID, label string) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.RemoveLabel(id, label)
}

// HasLabel reports whether the node carries the label.
func (a *API) HasLabel(n *types.Node, label string) bool {
	if a == nil || !a.ok {
		return false
	}
	return a.ops.HasLabel(n, label)
}

// Labels returns the node's labels.
func (a *API) Labels(n *types.Node) []string {
	if a == nil || !a.ok {
		return nil
	}
	return cloneStrings(a.ops.Labels(n))
}

// PrimaryLabel returns the node's primary label.
func (a *API) PrimaryLabel(n *types.Node) string {
	if a == nil || !a.ok {
		return ""
	}
	return a.ops.PrimaryLabel(n)
}

// CloseVersion closes the open-ended ValidTo of the current node version.
func (a *API) CloseVersion(id types.NodeID, t types.Instant) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.CloseVersion(id, t)
}

// History returns the version chain for a node.
func (a *API) History(id types.NodeID) ([]*types.Node, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.History(id)
}

// VersionAfter returns the next version for the given node.
func (a *API) VersionAfter(id types.NodeID, version uint32) (*types.Node, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.VersionAfter(id, version)
}

// VersionBefore returns the previous version for the given node.
func (a *API) VersionBefore(id types.NodeID, version uint32) (*types.Node, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.VersionBefore(id, version)
}

// NextID generates and returns the next node ID.
func (a *API) NextID() types.NodeID {
	if a == nil || !a.ok {
		return 0
	}
	return a.ops.NextID()
}

func cloneStrings(values []string) []string {
	if values == nil {
		return nil
	}
	out := make([]string, len(values))
	copy(out, values)
	return out
}
