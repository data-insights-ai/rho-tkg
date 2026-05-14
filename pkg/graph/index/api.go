// API accessors for index management (property indexes, vector indexes,
// high-frequency indexes, vector search, IndexProvider registration). Lives in
// the same package as IndexProvider et al — collapsed in v3.4.0 post-cleanup
// from the previous pkg/graph/indexapi sibling.
package index

import (
	"time"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/grapherr"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// Ops is the subset of *core.IndexOps the index sub-API forwards to.
type Ops interface {
	CreateProperty(label, propertyKey string) error
	DeleteProperty(label, propertyKey string) error
	CreateHighFrequency(label string, bucketSize time.Duration) error
	DeleteHighFrequency(label string) error
	CreateTemporal(label string) error
	DeleteTemporal(label string) error
	CreateVector(label, propertyKey string, dims int, metric storepkg.DistanceMetric) error
	DeleteVector(label, propertyKey string) error
	SearchNearest(label, propertyKey string, query []float32, k int, opts storepkg.QueryOpts) ([]*types.Node, error)
	RegisterProvider(p IndexProvider) error
	UnregisterProvider(name string) error
	Providers() []string
}

// API is the index sub-API accessor.
type API struct {
	ops Ops
	ok  bool
}

// New constructs an index sub-API.
func New(ops Ops) *API { return &API{ops: ops, ok: !grapherr.IsNil(ops)} }

func (a *API) ready() (Ops, error) {
	if a == nil || !a.ok {
		return nil, grapherr.ErrNilGraph
	}
	return a.ops, nil
}

// CreateProperty creates a property index.
func (a *API) CreateProperty(label, propertyKey string) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.CreateProperty(label, propertyKey)
}

// DropProperty drops a property index.
func (a *API) DeleteProperty(label, propertyKey string) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.DeleteProperty(label, propertyKey)
}

// CreateHighFrequency creates a high-frequency temporal index.
func (a *API) CreateHighFrequency(label string, bucketSize time.Duration) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.CreateHighFrequency(label, bucketSize)
}

// DropHighFrequency drops a high-frequency temporal index.
func (a *API) DeleteHighFrequency(label string) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.DeleteHighFrequency(label)
}

// CreateTemporal creates a temporal interval index for a label.
func (a *API) CreateTemporal(label string) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.CreateTemporal(label)
}

// DropTemporal drops a temporal interval index.
func (a *API) DeleteTemporal(label string) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.DeleteTemporal(label)
}

// CreateVector creates a vector (kNN) index. Existing indexed vectors with
// non-finite coordinates are rejected with ErrInvalidVectorValue.
func (a *API) CreateVector(label, propertyKey string, dims int, metric storepkg.DistanceMetric) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.CreateVector(label, propertyKey, dims, metric)
}

// DropVector drops a vector index.
func (a *API) DeleteVector(label, propertyKey string) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.DeleteVector(label, propertyKey)
}

// SearchNearest returns the k nearest nodes to query in the vector index.
// Queries containing NaN or infinity are rejected with ErrInvalidVectorValue.
func (a *API) SearchNearest(label, propertyKey string, query []float32, k int, opts storepkg.QueryOpts) ([]*types.Node, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.SearchNearest(label, propertyKey, query, k, opts)
}

// RegisterProvider registers a new IndexProvider.
func (a *API) RegisterProvider(p IndexProvider) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.RegisterProvider(p)
}

// UnregisterProvider unregisters an index provider by name.
func (a *API) UnregisterProvider(name string) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.UnregisterProvider(name)
}

// Providers lists registered IndexProvider names.
func (a *API) Providers() []string {
	if a == nil || !a.ok {
		return nil
	}
	return cloneStrings(a.ops.Providers())
}

func cloneStrings(values []string) []string {
	if values == nil {
		return nil
	}
	out := make([]string, len(values))
	copy(out, values)
	return out
}
