// Package rels is a sub-API accessor exposing the relationship-management
// surface of a Graph. The package declares a local Core interface listing the
// methods it forwards to; *graph.Graph satisfies it implicitly.
package rels

import (
	"context"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// Core is the subset of *graph.Graph methods the rels sub-API forwards to.
type Core interface {
	AddRelationship(typeName string, startNode, endNode *types.Node, props map[string]any) (*types.Relationship, error)
	AddRelationshipWithContext(ctx context.Context, typeName string, startNode, endNode *types.Node, props map[string]any) (*types.Relationship, error)
	AddRelationshipByID(typeName string, startID, endID types.NodeID, props map[string]any) (*types.Relationship, error)
	AddRelationshipByIDWithContext(ctx context.Context, typeName string, startID, endID types.NodeID, props map[string]any) (*types.Relationship, error)
	AddRelationshipByIDIfAbsent(typeName string, startID, endID types.NodeID, props map[string]any) (*types.Relationship, bool, error)
	AddRelationshipByIDIfAbsentWithContext(ctx context.Context, typeName string, startID, endID types.NodeID, props map[string]any) (*types.Relationship, bool, error)
	GetRelationship(id types.RelID) (*types.Relationship, error)
	GetRelationshipWithContext(ctx context.Context, id types.RelID) (*types.Relationship, error)
	GetRelationshipsByIDs(ids []types.RelID) ([]*types.Relationship, error)
	UpdateRelationship(id types.RelID, updates map[string]any) (*types.Relationship, error)
	UpdateRelationshipWithContext(ctx context.Context, id types.RelID, updates map[string]any) (*types.Relationship, error)
	UpdateRelInPlace(id types.RelID, updates map[string]any) (*types.Relationship, error)
	UpdateRelInPlaceWithContext(ctx context.Context, id types.RelID, updates map[string]any) (*types.Relationship, error)
	DeleteRelationship(id types.RelID) error
	DeleteRelationshipWithContext(ctx context.Context, id types.RelID) error
	ImportRelationshipWithID(ctx context.Context, id types.RelID, typeName string, startNode, endNode *types.Node, props map[string]any) (*types.Relationship, error)

	AllRelationships(opts storepkg.QueryOpts) ([]*types.Relationship, error)
	RelationshipsByType(typeName string, opts storepkg.QueryOpts) ([]*types.Relationship, error)
	RelationshipCount() (int, error)
	RelCountByType(typeName string) (int, error)

	OutgoingRelationships(nodeID types.NodeID, typeName string) ([]*types.Relationship, error)
	IncomingRelationships(nodeID types.NodeID, typeName string) ([]*types.Relationship, error)
	OutgoingRelationshipsForNodes(nodeIDs []types.NodeID, typeName string) (map[types.NodeID][]*types.Relationship, error)
	IncomingRelationshipsForNodes(nodeIDs []types.NodeID, typeName string) (map[types.NodeID][]*types.Relationship, error)

	SetRelationshipProperty(id types.RelID, key string, value any) error
	DeleteRelationshipProperty(id types.RelID, key string) error

	RelationshipHasType(r *types.Relationship, typ string) bool
	RelationshipType(r *types.Relationship) string

	CloseRelVersion(id types.RelID, t types.Instant) error
	GetRelHistory(id types.RelID) ([]*types.Relationship, error)
	GetNextRelVersion(id types.RelID, version uint32) (*types.Relationship, error)
	GetPreviousRelVersion(id types.RelID, version uint32) (*types.Relationship, error)

	NextRelID() types.RelID
}

// API is the rels sub-API accessor.
type API struct{ c Core }

// New constructs a rels sub-API.
func New(c Core) *API { return &API{c: c} }

// Add creates a relationship from start to end. Forwards to Graph.AddRelationship.
func (a *API) Add(typeName string, startNode, endNode *types.Node, props map[string]any) (*types.Relationship, error) {
	return a.c.AddRelationship(typeName, startNode, endNode, props)
}

// AddWithContext creates a relationship honoring ctx. Forwards to Graph.AddRelationshipWithContext.
func (a *API) AddWithContext(ctx context.Context, typeName string, startNode, endNode *types.Node, props map[string]any) (*types.Relationship, error) {
	return a.c.AddRelationshipWithContext(ctx, typeName, startNode, endNode, props)
}

// AddByID creates a relationship by node IDs. Forwards to Graph.AddRelationshipByID.
func (a *API) AddByID(typeName string, startID, endID types.NodeID, props map[string]any) (*types.Relationship, error) {
	return a.c.AddRelationshipByID(typeName, startID, endID, props)
}

// AddByIDWithContext creates a relationship by node IDs honoring ctx. Forwards to Graph.AddRelationshipByIDWithContext.
func (a *API) AddByIDWithContext(ctx context.Context, typeName string, startID, endID types.NodeID, props map[string]any) (*types.Relationship, error) {
	return a.c.AddRelationshipByIDWithContext(ctx, typeName, startID, endID, props)
}

// AddByIDIfAbsent creates a relationship only if it does not already exist. Forwards to Graph.AddRelationshipByIDIfAbsent.
func (a *API) AddByIDIfAbsent(typeName string, startID, endID types.NodeID, props map[string]any) (*types.Relationship, bool, error) {
	return a.c.AddRelationshipByIDIfAbsent(typeName, startID, endID, props)
}

// AddByIDIfAbsentWithContext creates a relationship only if absent honoring ctx. Forwards to Graph.AddRelationshipByIDIfAbsentWithContext.
func (a *API) AddByIDIfAbsentWithContext(ctx context.Context, typeName string, startID, endID types.NodeID, props map[string]any) (*types.Relationship, bool, error) {
	return a.c.AddRelationshipByIDIfAbsentWithContext(ctx, typeName, startID, endID, props)
}

// Get returns the relationship by ID. Forwards to Graph.GetRelationship.
func (a *API) Get(id types.RelID) (*types.Relationship, error) { return a.c.GetRelationship(id) }

// GetWithContext returns the relationship honoring ctx. Forwards to Graph.GetRelationshipWithContext.
func (a *API) GetWithContext(ctx context.Context, id types.RelID) (*types.Relationship, error) {
	return a.c.GetRelationshipWithContext(ctx, id)
}

// GetByIDs returns relationships for the given IDs. Forwards to Graph.GetRelationshipsByIDs.
func (a *API) GetByIDs(ids []types.RelID) ([]*types.Relationship, error) {
	return a.c.GetRelationshipsByIDs(ids)
}

// Update updates relationship properties. Forwards to Graph.UpdateRelationship.
func (a *API) Update(id types.RelID, updates map[string]any) (*types.Relationship, error) {
	return a.c.UpdateRelationship(id, updates)
}

// UpdateWithContext updates a relationship honoring ctx. Forwards to Graph.UpdateRelationshipWithContext.
func (a *API) UpdateWithContext(ctx context.Context, id types.RelID, updates map[string]any) (*types.Relationship, error) {
	return a.c.UpdateRelationshipWithContext(ctx, id, updates)
}

// UpdateInPlace updates a relationship in place. Forwards to Graph.UpdateRelInPlace.
func (a *API) UpdateInPlace(id types.RelID, updates map[string]any) (*types.Relationship, error) {
	return a.c.UpdateRelInPlace(id, updates)
}

// UpdateInPlaceWithContext updates a relationship in place honoring ctx. Forwards to Graph.UpdateRelInPlaceWithContext.
func (a *API) UpdateInPlaceWithContext(ctx context.Context, id types.RelID, updates map[string]any) (*types.Relationship, error) {
	return a.c.UpdateRelInPlaceWithContext(ctx, id, updates)
}

// Delete deletes the relationship. Forwards to Graph.DeleteRelationship.
func (a *API) Delete(id types.RelID) error { return a.c.DeleteRelationship(id) }

// DeleteWithContext deletes the relationship honoring ctx. Forwards to Graph.DeleteRelationshipWithContext.
func (a *API) DeleteWithContext(ctx context.Context, id types.RelID) error {
	return a.c.DeleteRelationshipWithContext(ctx, id)
}

// Import imports a relationship with a caller-supplied ID. Forwards to Graph.ImportRelationshipWithID.
func (a *API) Import(ctx context.Context, id types.RelID, typeName string, startNode, endNode *types.Node, props map[string]any) (*types.Relationship, error) {
	return a.c.ImportRelationshipWithID(ctx, id, typeName, startNode, endNode, props)
}

// All returns all relationships matching opts. Forwards to Graph.AllRelationships.
func (a *API) All(opts storepkg.QueryOpts) ([]*types.Relationship, error) {
	return a.c.AllRelationships(opts)
}

// ByType returns relationships of the given type. Forwards to Graph.RelationshipsByType.
func (a *API) ByType(typeName string, opts storepkg.QueryOpts) ([]*types.Relationship, error) {
	return a.c.RelationshipsByType(typeName, opts)
}

// Count returns the total relationship count. Forwards to Graph.RelationshipCount.
func (a *API) Count() (int, error) { return a.c.RelationshipCount() }

// CountByType returns the count of relationships of the given type. Forwards to Graph.RelCountByType.
func (a *API) CountByType(typeName string) (int, error) { return a.c.RelCountByType(typeName) }

// Outgoing returns outgoing relationships from a node. Forwards to Graph.OutgoingRelationships.
func (a *API) Outgoing(nodeID types.NodeID, typeName string) ([]*types.Relationship, error) {
	return a.c.OutgoingRelationships(nodeID, typeName)
}

// Incoming returns incoming relationships to a node. Forwards to Graph.IncomingRelationships.
func (a *API) Incoming(nodeID types.NodeID, typeName string) ([]*types.Relationship, error) {
	return a.c.IncomingRelationships(nodeID, typeName)
}

// OutgoingForNodes returns outgoing relationships for a batch of nodes. Forwards to Graph.OutgoingRelationshipsForNodes.
func (a *API) OutgoingForNodes(nodeIDs []types.NodeID, typeName string) (map[types.NodeID][]*types.Relationship, error) {
	return a.c.OutgoingRelationshipsForNodes(nodeIDs, typeName)
}

// IncomingForNodes returns incoming relationships for a batch of nodes. Forwards to Graph.IncomingRelationshipsForNodes.
func (a *API) IncomingForNodes(nodeIDs []types.NodeID, typeName string) (map[types.NodeID][]*types.Relationship, error) {
	return a.c.IncomingRelationshipsForNodes(nodeIDs, typeName)
}

// SetProperty sets a single relationship property. Forwards to Graph.SetRelationshipProperty.
func (a *API) SetProperty(id types.RelID, key string, value any) error {
	return a.c.SetRelationshipProperty(id, key, value)
}

// DeleteProperty deletes a single relationship property. Forwards to Graph.DeleteRelationshipProperty.
func (a *API) DeleteProperty(id types.RelID, key string) error {
	return a.c.DeleteRelationshipProperty(id, key)
}

// HasType reports whether the relationship is of the given type. Forwards to Graph.RelationshipHasType.
func (a *API) HasType(r *types.Relationship, typ string) bool { return a.c.RelationshipHasType(r, typ) }

// Type returns the relationship's type name. Forwards to Graph.RelationshipType.
func (a *API) Type(r *types.Relationship) string { return a.c.RelationshipType(r) }

// CloseVersion closes the open-ended ValidTo of the current relationship version. Forwards to Graph.CloseRelVersion.
func (a *API) CloseVersion(id types.RelID, t types.Instant) error {
	return a.c.CloseRelVersion(id, t)
}

// History returns the version chain for a relationship. Forwards to Graph.GetRelHistory.
func (a *API) History(id types.RelID) ([]*types.Relationship, error) {
	return a.c.GetRelHistory(id)
}

// NextVersion returns the next version of the relationship. Forwards to Graph.GetNextRelVersion.
func (a *API) NextVersion(id types.RelID, version uint32) (*types.Relationship, error) {
	return a.c.GetNextRelVersion(id, version)
}

// PreviousVersion returns the previous version of the relationship. Forwards to Graph.GetPreviousRelVersion.
func (a *API) PreviousVersion(id types.RelID, version uint32) (*types.Relationship, error) {
	return a.c.GetPreviousRelVersion(id, version)
}

// NextID generates and returns the next relationship ID. Forwards to Graph.NextRelID.
func (a *API) NextID() types.RelID { return a.c.NextRelID() }
