// Package rels is a sub-API accessor exposing the relationship-management
// surface of a Graph. The package declares a local Ops interface listing the
// methods it forwards to; *core.RelOps satisfies it implicitly.
package rels

import (
	"context"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/grapherr"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// Ops is the subset of *core.RelOps the rels sub-API forwards to.
type Ops interface {
	Add(typeName string, startNode, endNode *types.Node, props map[string]any) (*types.Relationship, error)
	AddWithContext(ctx context.Context, typeName string, startNode, endNode *types.Node, props map[string]any) (*types.Relationship, error)
	AddByID(typeName string, startID, endID types.NodeID, props map[string]any) (*types.Relationship, error)
	AddByIDWithContext(ctx context.Context, typeName string, startID, endID types.NodeID, props map[string]any) (*types.Relationship, error)
	AddByIDIfAbsent(typeName string, startID, endID types.NodeID, props map[string]any) (*types.Relationship, bool, error)
	AddByIDIfAbsentWithContext(ctx context.Context, typeName string, startID, endID types.NodeID, props map[string]any) (*types.Relationship, bool, error)
	Get(id types.RelID) (*types.Relationship, error)
	GetWithContext(ctx context.Context, id types.RelID) (*types.Relationship, error)
	GetByIDs(ids []types.RelID) ([]*types.Relationship, error)
	Update(id types.RelID, updates map[string]any) (*types.Relationship, error)
	UpdateWithContext(ctx context.Context, id types.RelID, updates map[string]any) (*types.Relationship, error)
	UpdateInPlace(id types.RelID, updates map[string]any) (*types.Relationship, error)
	UpdateInPlaceWithContext(ctx context.Context, id types.RelID, updates map[string]any) (*types.Relationship, error)
	Delete(id types.RelID) error
	DeleteWithContext(ctx context.Context, id types.RelID) error
	Import(ctx context.Context, id types.RelID, typeName string, startNode, endNode *types.Node, props map[string]any) (*types.Relationship, error)

	All(opts storepkg.QueryOpts) ([]*types.Relationship, error)
	ByType(typeName string, opts storepkg.QueryOpts) ([]*types.Relationship, error)
	Count() (int, error)
	CountByType(typeName string) (int, error)

	Outgoing(nodeID types.NodeID, typeName string) ([]*types.Relationship, error)
	Incoming(nodeID types.NodeID, typeName string) ([]*types.Relationship, error)
	OutgoingForNodes(nodeIDs []types.NodeID, typeName string) (map[types.NodeID][]*types.Relationship, error)
	IncomingForNodes(nodeIDs []types.NodeID, typeName string) (map[types.NodeID][]*types.Relationship, error)

	SetProperty(id types.RelID, key string, value any) error
	DeleteProperty(id types.RelID, key string) error

	HasType(r *types.Relationship, typ string) bool
	Type(r *types.Relationship) string

	CloseVersion(id types.RelID, t types.Instant) error
	History(id types.RelID) ([]*types.Relationship, error)
	NextVersion(id types.RelID, version uint32) (*types.Relationship, error)
	PreviousVersion(id types.RelID, version uint32) (*types.Relationship, error)

	NextID() types.RelID
}

// API is the rels sub-API accessor.
type API struct {
	ops Ops
	ok  bool
}

// New constructs a rels sub-API.
func New(ops Ops) *API { return &API{ops: ops, ok: !grapherr.IsNil(ops)} }

func (a *API) ready() (Ops, error) {
	if a == nil || !a.ok {
		return nil, grapherr.ErrNilGraph
	}
	return a.ops, nil
}

// Add creates a relationship from start to end.
func (a *API) Add(typeName string, startNode, endNode *types.Node, props map[string]any) (*types.Relationship, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.Add(typeName, startNode, endNode, props)
}

// AddWithContext creates a relationship honoring ctx.
func (a *API) AddWithContext(ctx context.Context, typeName string, startNode, endNode *types.Node, props map[string]any) (*types.Relationship, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.AddWithContext(ctx, typeName, startNode, endNode, props)
}

// AddByID creates a relationship by node IDs.
// It verifies live endpoints, captures endpoint hashes, and enforces graph
// constraints like Add; the difference is only the endpoint input form.
func (a *API) AddByID(typeName string, startID, endID types.NodeID, props map[string]any) (*types.Relationship, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.AddByID(typeName, startID, endID, props)
}

// AddByIDWithContext creates a relationship by node IDs honoring ctx.
// Same endpoint and constraint behaviour as AddByID.
func (a *API) AddByIDWithContext(ctx context.Context, typeName string, startID, endID types.NodeID, props map[string]any) (*types.Relationship, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.AddByIDWithContext(ctx, typeName, startID, endID, props)
}

// AddByIDIfAbsent creates a relationship if the (start,end,type) triple does not already exist.
// Same constraint behaviour as AddByID: the configured constraint set
// is always enforced when present.
func (a *API) AddByIDIfAbsent(typeName string, startID, endID types.NodeID, props map[string]any) (*types.Relationship, bool, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, false, err
	}
	return ops.AddByIDIfAbsent(typeName, startID, endID, props)
}

// AddByIDIfAbsentWithContext is the context-aware variant of AddByIDIfAbsent.
// Same constraint behaviour as AddByID.
func (a *API) AddByIDIfAbsentWithContext(ctx context.Context, typeName string, startID, endID types.NodeID, props map[string]any) (*types.Relationship, bool, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, false, err
	}
	return ops.AddByIDIfAbsentWithContext(ctx, typeName, startID, endID, props)
}

// Get returns the relationship with the given ID.
func (a *API) Get(id types.RelID) (*types.Relationship, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.Get(id)
}

// GetWithContext returns the relationship honoring ctx.
func (a *API) GetWithContext(ctx context.Context, id types.RelID) (*types.Relationship, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.GetWithContext(ctx, id)
}

// GetByIDs returns relationships for the given IDs.
func (a *API) GetByIDs(ids []types.RelID) ([]*types.Relationship, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.GetByIDs(ids)
}

// Update updates a relationship's properties.
func (a *API) Update(id types.RelID, updates map[string]any) (*types.Relationship, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.Update(id, updates)
}

// UpdateWithContext updates a relationship honoring ctx.
func (a *API) UpdateWithContext(ctx context.Context, id types.RelID, updates map[string]any) (*types.Relationship, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.UpdateWithContext(ctx, id, updates)
}

// UpdateInPlace updates a relationship in place (no version branch).
func (a *API) UpdateInPlace(id types.RelID, updates map[string]any) (*types.Relationship, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.UpdateInPlace(id, updates)
}

// UpdateInPlaceWithContext updates a relationship in place honoring ctx.
func (a *API) UpdateInPlaceWithContext(ctx context.Context, id types.RelID, updates map[string]any) (*types.Relationship, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.UpdateInPlaceWithContext(ctx, id, updates)
}

// Delete deletes the relationship.
func (a *API) Delete(id types.RelID) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.Delete(id)
}

// DeleteWithContext deletes the relationship honoring ctx.
func (a *API) DeleteWithContext(ctx context.Context, id types.RelID) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.DeleteWithContext(ctx, id)
}

// Import imports a relationship with a caller-supplied ID.
func (a *API) Import(ctx context.Context, id types.RelID, typeName string, startNode, endNode *types.Node, props map[string]any) (*types.Relationship, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.Import(ctx, id, typeName, startNode, endNode, props)
}

// All returns all relationships matching opts.
func (a *API) All(opts storepkg.QueryOpts) ([]*types.Relationship, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.All(opts)
}

// ByType returns relationships of the given type.
func (a *API) ByType(typeName string, opts storepkg.QueryOpts) ([]*types.Relationship, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.ByType(typeName, opts)
}

// Count returns the total relationship count.
func (a *API) Count() (int, error) {
	ops, err := a.ready()
	if err != nil {
		return 0, err
	}
	return ops.Count()
}

// CountByType returns the count of relationships of the given type.
func (a *API) CountByType(typeName string) (int, error) {
	ops, err := a.ready()
	if err != nil {
		return 0, err
	}
	return ops.CountByType(typeName)
}

// Outgoing returns outgoing relationships from nodeID, optionally filtered by type.
func (a *API) Outgoing(nodeID types.NodeID, typeName string) ([]*types.Relationship, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.Outgoing(nodeID, typeName)
}

// Incoming returns incoming relationships to nodeID, optionally filtered by type.
func (a *API) Incoming(nodeID types.NodeID, typeName string) ([]*types.Relationship, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.Incoming(nodeID, typeName)
}

// OutgoingForNodes returns outgoing relationships for the given set of node IDs.
func (a *API) OutgoingForNodes(nodeIDs []types.NodeID, typeName string) (map[types.NodeID][]*types.Relationship, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.OutgoingForNodes(nodeIDs, typeName)
}

// IncomingForNodes returns incoming relationships for the given set of node IDs.
func (a *API) IncomingForNodes(nodeIDs []types.NodeID, typeName string) (map[types.NodeID][]*types.Relationship, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.IncomingForNodes(nodeIDs, typeName)
}

// SetProperty sets a single property on the relationship.
func (a *API) SetProperty(id types.RelID, key string, value any) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.SetProperty(id, key, value)
}

// DeleteProperty deletes a single property from the relationship.
func (a *API) DeleteProperty(id types.RelID, key string) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.DeleteProperty(id, key)
}

// HasType reports whether the relationship has the given type name.
func (a *API) HasType(r *types.Relationship, typ string) bool {
	if a == nil || !a.ok {
		return false
	}
	return a.ops.HasType(r, typ)
}

// Type returns the relationship's type name.
func (a *API) Type(r *types.Relationship) string {
	if a == nil || !a.ok {
		return ""
	}
	return a.ops.Type(r)
}

// CloseVersion closes the open-ended ValidTo of the current relationship version.
func (a *API) CloseVersion(id types.RelID, t types.Instant) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.CloseVersion(id, t)
}

// History returns the version chain for a relationship.
func (a *API) History(id types.RelID) ([]*types.Relationship, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.History(id)
}

// NextVersion returns the next version for the given relationship.
func (a *API) NextVersion(id types.RelID, version uint32) (*types.Relationship, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.NextVersion(id, version)
}

// PreviousVersion returns the previous version for the given relationship.
func (a *API) PreviousVersion(id types.RelID, version uint32) (*types.Relationship, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.PreviousVersion(id, version)
}

// NextID generates and returns the next relationship ID.
func (a *API) NextID() types.RelID {
	if a == nil || !a.ok {
		return 0
	}
	return a.ops.NextID()
}
