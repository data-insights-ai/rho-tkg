// Package indexapi is a sub-API accessor exposing index-management methods
// (property indexes, vector indexes, high-frequency indexes, vector search,
// IndexProvider registration). The underlying types live in pkg/graph/index;
// the sub-API is named indexapi to avoid the directory-name collision.
package indexapi

import (
	"time"

	indexpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/index"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// Core is the subset of *graph.Graph methods the index sub-API forwards to.
type Core interface {
	CreatePropertyIndex(label, propertyKey string) error
	DropPropertyIndex(label, propertyKey string) error
	CreateHighFrequencyIndex(label string, bucketSize time.Duration) error
	DropHighFrequencyIndex(label string) error
	CreateTemporalIndex(label string) error
	DropTemporalIndex(label string) error
	CreateVectorIndex(label, propertyKey string, dims int, metric storepkg.DistanceMetric) error
	DropVectorIndex(label, propertyKey string) error
	SearchNearestNodes(label, propertyKey string, query []float32, k int, opts storepkg.QueryOpts) ([]*types.Node, error)
	RegisterIndexProvider(p indexpkg.IndexProvider) error
	RegisterLegacyIndexProvider(p indexpkg.LegacyIndexProvider) error
	UnregisterIndexProvider(name string) error
	IndexProviders() []string
}

// API is the index sub-API accessor.
type API struct{ c Core }

// New constructs an index sub-API.
func New(c Core) *API { return &API{c: c} }

// CreateProperty creates a property index. Forwards to Graph.CreatePropertyIndex.
func (a *API) CreateProperty(label, propertyKey string) error {
	return a.c.CreatePropertyIndex(label, propertyKey)
}

// DropProperty drops a property index. Forwards to Graph.DropPropertyIndex.
func (a *API) DropProperty(label, propertyKey string) error {
	return a.c.DropPropertyIndex(label, propertyKey)
}

// CreateHighFrequency creates a high-frequency temporal index. Forwards to Graph.CreateHighFrequencyIndex.
func (a *API) CreateHighFrequency(label string, bucketSize time.Duration) error {
	return a.c.CreateHighFrequencyIndex(label, bucketSize)
}

// DropHighFrequency drops a high-frequency temporal index. Forwards to Graph.DropHighFrequencyIndex.
func (a *API) DropHighFrequency(label string) error { return a.c.DropHighFrequencyIndex(label) }

// CreateTemporal creates a temporal interval index for a label. Forwards to Graph.CreateTemporalIndex.
func (a *API) CreateTemporal(label string) error { return a.c.CreateTemporalIndex(label) }

// DropTemporal drops a temporal interval index. Forwards to Graph.DropTemporalIndex.
func (a *API) DropTemporal(label string) error { return a.c.DropTemporalIndex(label) }

// CreateVector creates a vector (kNN) index. Forwards to Graph.CreateVectorIndex.
func (a *API) CreateVector(label, propertyKey string, dims int, metric storepkg.DistanceMetric) error {
	return a.c.CreateVectorIndex(label, propertyKey, dims, metric)
}

// DropVector drops a vector index. Forwards to Graph.DropVectorIndex.
func (a *API) DropVector(label, propertyKey string) error {
	return a.c.DropVectorIndex(label, propertyKey)
}

// SearchNearest returns the k nearest nodes to query in the vector index. Forwards to Graph.SearchNearestNodes.
func (a *API) SearchNearest(label, propertyKey string, query []float32, k int, opts storepkg.QueryOpts) ([]*types.Node, error) {
	return a.c.SearchNearestNodes(label, propertyKey, query, k, opts)
}

// RegisterProvider registers a new IndexProvider. Forwards to Graph.RegisterIndexProvider.
func (a *API) RegisterProvider(p indexpkg.IndexProvider) error {
	return a.c.RegisterIndexProvider(p)
}

// RegisterLegacyProvider registers a legacy index provider. Forwards to Graph.RegisterLegacyIndexProvider.
func (a *API) RegisterLegacyProvider(p indexpkg.LegacyIndexProvider) error {
	return a.c.RegisterLegacyIndexProvider(p)
}

// UnregisterProvider unregisters an index provider by name. Forwards to Graph.UnregisterIndexProvider.
func (a *API) UnregisterProvider(name string) error { return a.c.UnregisterIndexProvider(name) }

// Providers lists registered IndexProvider names. Forwards to Graph.IndexProviders.
func (a *API) Providers() []string { return a.c.IndexProviders() }
