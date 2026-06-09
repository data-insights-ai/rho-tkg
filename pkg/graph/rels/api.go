// Package rels is a sub-API accessor exposing the relationship-management
// surface of a Graph. The package declares a local Ops interface listing the
// methods it forwards to; *core.RelOps satisfies it implicitly.
//
// API 4.0 change: methods that previously came in (Foo, FooWithContext) pairs
// (Add, AddByID, AddByIDIfAbsent, Get, Update, UpdateInPlace, Delete,
// CompareAndSetProperty) collapsed into a single context-aware method each.
// Pass context.Background() for the previous no-context behavior. The
// historical *WithContext variants no longer exist.
package rels

import (
	"context"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/grapherr"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Ops is the subset of *core.RelOps the rels sub-API forwards to.
type Ops interface {
	Add(ctx context.Context, typeName string, startNode, endNode *types.Node, props map[string]any) (*types.Relationship, error)
	AddByID(ctx context.Context, typeName string, startID, endID types.NodeID, props map[string]any) (*types.Relationship, error)
	AddByIDIfAbsent(ctx context.Context, typeName string, startID, endID types.NodeID, props map[string]any) (*types.Relationship, bool, error)
	Get(ctx context.Context, id types.RelID) (*types.Relationship, error)
	GetByIDs(ids []types.RelID) ([]*types.Relationship, error)
	Update(ctx context.Context, id types.RelID, updates map[string]any) (*types.Relationship, error)
	UpdateInPlace(ctx context.Context, id types.RelID, updates map[string]any) (*types.Relationship, error)
	Delete(ctx context.Context, id types.RelID) error
	Import(ctx context.Context, id types.RelID, typeName string, startNode, endNode *types.Node, props map[string]any) (*types.Relationship, error)

	All(opts storepkg.QueryOpts) ([]*types.Relationship, error)
	ByType(typeName string, opts storepkg.QueryOpts) ([]*types.Relationship, error)
	Count() (int, error)
	CountByType(typeName string) (int, error)

	Outgoing(nodeID types.NodeID, typeName string) ([]*types.Relationship, error)
	Incoming(nodeID types.NodeID, typeName string) ([]*types.Relationship, error)
	OutgoingForNodes(nodeIDs []types.NodeID, typeName string) (map[types.NodeID][]*types.Relationship, error)
	IncomingForNodes(nodeIDs []types.NodeID, typeName string) (map[types.NodeID][]*types.Relationship, error)
	OutgoingDegree(nodeID types.NodeID, typeName string) (int, error)
	IncomingDegree(nodeID types.NodeID, typeName string) (int, error)

	SetProperty(ctx context.Context, id types.RelID, key string, value any) error
	DeleteProperty(ctx context.Context, id types.RelID, key string) error
	CompareAndSetProperty(ctx context.Context, id types.RelID, key string, expected, newVal any) (bool, error)

	HasType(r *types.Relationship, typ string) bool
	Type(r *types.Relationship) string

	CloseVersion(ctx context.Context, id types.RelID, t types.Instant) error
	History(id types.RelID) ([]*types.Relationship, error)
	VersionAfter(id types.RelID, version uint32) (*types.Relationship, error)
	VersionBefore(id types.RelID, version uint32) (*types.Relationship, error)

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

// Add creates a relationship honoring ctx.
func (a *API) Add(ctx context.Context, typeName string, startNode, endNode *types.Node, props map[string]any) (*types.Relationship, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.Add(ctx, typeName, startNode, endNode, props)
}

// AddByID creates a relationship by node IDs honoring ctx.
// It verifies live endpoints, captures endpoint hashes, and enforces graph
// constraints like Add; the difference is only the endpoint input form.
func (a *API) AddByID(ctx context.Context, typeName string, startID, endID types.NodeID, props map[string]any) (*types.Relationship, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.AddByID(ctx, typeName, startID, endID, props)
}

// AddByIDIfAbsent creates a relationship if the (start,end,type) triple does
// not already exist, honoring ctx. Same constraint behaviour as AddByID.
func (a *API) AddByIDIfAbsent(ctx context.Context, typeName string, startID, endID types.NodeID, props map[string]any) (*types.Relationship, bool, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, false, err
	}
	return ops.AddByIDIfAbsent(ctx, typeName, startID, endID, props)
}

// Get returns the relationship with the given ID honoring ctx.
func (a *API) Get(ctx context.Context, id types.RelID) (*types.Relationship, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.Get(ctx, id)
}

// GetByIDs returns relationships for the given IDs.
func (a *API) GetByIDs(ids []types.RelID) ([]*types.Relationship, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.GetByIDs(ids)
}

// Update updates a relationship's properties honoring ctx.
func (a *API) Update(ctx context.Context, id types.RelID, updates map[string]any) (*types.Relationship, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.Update(ctx, id, updates)
}

// UpdateInPlace updates a relationship in place honoring ctx (no version branch).
func (a *API) UpdateInPlace(ctx context.Context, id types.RelID, updates map[string]any) (*types.Relationship, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.UpdateInPlace(ctx, id, updates)
}

// Delete deletes the relationship honoring ctx.
func (a *API) Delete(ctx context.Context, id types.RelID) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.Delete(ctx, id)
}

// Import imports a relationship with a caller-supplied ID honoring ctx.
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

// OutgoingDegree returns the count of outgoing relationships from nodeID
// (optionally type-filtered) without materializing them. O(1)/O(degree) on the
// adjacency index via the store's DegreeCapability, with a len(Outgoing) fallback.
func (a *API) OutgoingDegree(nodeID types.NodeID, typeName string) (int, error) {
	ops, err := a.ready()
	if err != nil {
		return 0, err
	}
	return ops.OutgoingDegree(nodeID, typeName)
}

// IncomingDegree returns the count of incoming relationships to nodeID
// (optionally type-filtered) without materializing them. See OutgoingDegree.
func (a *API) IncomingDegree(nodeID types.NodeID, typeName string) (int, error) {
	ops, err := a.ready()
	if err != nil {
		return 0, err
	}
	return ops.IncomingDegree(nodeID, typeName)
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

// SetProperty sets a single property on the relationship honoring ctx.
func (a *API) SetProperty(ctx context.Context, id types.RelID, key string, value any) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.SetProperty(ctx, id, key, value)
}

// DeleteProperty deletes a single property from the relationship honoring ctx.
func (a *API) DeleteProperty(ctx context.Context, id types.RelID, key string) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.DeleteProperty(ctx, id, key)
}

// CompareAndSetProperty atomically updates a relationship property honoring ctx.
func (a *API) CompareAndSetProperty(ctx context.Context, id types.RelID, key string, expected, newVal any) (bool, error) {
	ops, err := a.ready()
	if err != nil {
		return false, err
	}
	return ops.CompareAndSetProperty(ctx, id, key, expected, newVal)
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

// CloseVersion closes the open-ended ValidTo of the current relationship version, honoring ctx.
func (a *API) CloseVersion(ctx context.Context, id types.RelID, t types.Instant) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.CloseVersion(ctx, id, t)
}

// History returns the version chain for a relationship.
func (a *API) History(id types.RelID) ([]*types.Relationship, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.History(id)
}

// VersionAfter returns the next version for the given relationship.
func (a *API) VersionAfter(id types.RelID, version uint32) (*types.Relationship, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.VersionAfter(id, version)
}

// VersionBefore returns the previous version for the given relationship.
func (a *API) VersionBefore(id types.RelID, version uint32) (*types.Relationship, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.VersionBefore(id, version)
}

// NextID generates and returns the next relationship ID.
func (a *API) NextID() types.RelID {
	if a == nil || !a.ok {
		return 0
	}
	return a.ops.NextID()
}
