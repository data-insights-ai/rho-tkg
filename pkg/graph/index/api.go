// API accessors for index management (property indexes, vector indexes,
// high-frequency indexes, vector search, IndexProvider registration). Lives in
// the same package as IndexProvider et al — collapsed in v3.4.0 post-cleanup
// from the previous pkg/graph/indexapi sibling.
package index

import (
	"time"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// Ops is the subset of *core.IndexOps the index sub-API forwards to.
type Ops interface {
	CreateProperty(label, propertyKey string) error
	DropProperty(label, propertyKey string) error
	CreateHighFrequency(label string, bucketSize time.Duration) error
	DropHighFrequency(label string) error
	CreateTemporal(label string) error
	DropTemporal(label string) error
	CreateVector(label, propertyKey string, dims int, metric storepkg.DistanceMetric) error
	DropVector(label, propertyKey string) error
	SearchNearest(label, propertyKey string, query []float32, k int, opts storepkg.QueryOpts) ([]*types.Node, error)
	RegisterProvider(p IndexProvider) error
	RegisterLegacyProvider(p LegacyIndexProvider) error
	UnregisterProvider(name string) error
	Providers() []string
}

// API is the index sub-API accessor.
type API struct{ ops Ops }

// New constructs an index sub-API.
func New(ops Ops) *API { return &API{ops: ops} }

// CreateProperty creates a property index.
func (a *API) CreateProperty(label, propertyKey string) error {
	return a.ops.CreateProperty(label, propertyKey)
}

// DropProperty drops a property index.
func (a *API) DropProperty(label, propertyKey string) error {
	return a.ops.DropProperty(label, propertyKey)
}

// CreateHighFrequency creates a high-frequency temporal index.
func (a *API) CreateHighFrequency(label string, bucketSize time.Duration) error {
	return a.ops.CreateHighFrequency(label, bucketSize)
}

// DropHighFrequency drops a high-frequency temporal index.
func (a *API) DropHighFrequency(label string) error { return a.ops.DropHighFrequency(label) }

// CreateTemporal creates a temporal interval index for a label.
func (a *API) CreateTemporal(label string) error { return a.ops.CreateTemporal(label) }

// DropTemporal drops a temporal interval index.
func (a *API) DropTemporal(label string) error { return a.ops.DropTemporal(label) }

// CreateVector creates a vector (kNN) index.
func (a *API) CreateVector(label, propertyKey string, dims int, metric storepkg.DistanceMetric) error {
	return a.ops.CreateVector(label, propertyKey, dims, metric)
}

// DropVector drops a vector index.
func (a *API) DropVector(label, propertyKey string) error {
	return a.ops.DropVector(label, propertyKey)
}

// SearchNearest returns the k nearest nodes to query in the vector index.
func (a *API) SearchNearest(label, propertyKey string, query []float32, k int, opts storepkg.QueryOpts) ([]*types.Node, error) {
	return a.ops.SearchNearest(label, propertyKey, query, k, opts)
}

// RegisterProvider registers a new IndexProvider.
func (a *API) RegisterProvider(p IndexProvider) error {
	return a.ops.RegisterProvider(p)
}

// RegisterLegacyProvider registers a legacy index provider.
func (a *API) RegisterLegacyProvider(p LegacyIndexProvider) error {
	return a.ops.RegisterLegacyProvider(p)
}

// UnregisterProvider unregisters an index provider by name.
func (a *API) UnregisterProvider(name string) error { return a.ops.UnregisterProvider(name) }

// Providers lists registered IndexProvider names.
func (a *API) Providers() []string { return a.ops.Providers() }
