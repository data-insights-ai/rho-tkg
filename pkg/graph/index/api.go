// API accessors for index management (property indexes, vector indexes,
// high-frequency indexes, vector search, IndexProvider registration). Lives in
// the same package as IndexProvider et al — collapsed in v3.4.0 post-cleanup
// from the previous pkg/graph/indexapi sibling.
package index

import (
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/apiutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/grapherr"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Ops is the subset of *core.IndexOps the index sub-API forwards to.
type Ops interface {
	CreateProperty(label, propertyKey string) error
	DeleteProperty(label, propertyKey string) error
	HasProperty(label, propertyKey string) (bool, error)
	CreateRelProperty(typeName, propertyKey string) error
	DeleteRelProperty(typeName, propertyKey string) error
	HasRelProperty(typeName, propertyKey string) (bool, error)
	CreateComposite(label string, keys []string) error
	DeleteComposite(label string, keys []string) error
	HasComposite(label string, keys []string) (bool, error)
	ListComposites(label string) ([][]string, error)
	CreateHighFrequency(label string, bucketSize time.Duration) error
	DeleteHighFrequency(label string) error
	CreateTemporal(label string) error
	DeleteTemporal(label string) error
	HasTemporal(label string) (bool, error)
	CreateRelTemporal(typeName string) error
	DeleteRelTemporal(typeName string) error
	CreateVector(label, propertyKey string, dims int, metric storepkg.DistanceMetric) error
	CreateVectorWithOptions(label, propertyKey string, dims int, metric storepkg.DistanceMetric, opts storepkg.VectorIndexOptions) error
	DeleteVector(label, propertyKey string) error
	VectorIndexInfo(label, propertyKey string) (storepkg.VectorIndexInfo, bool, error)
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

// DeleteProperty drops a property index.
func (a *API) DeleteProperty(label, propertyKey string) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.DeleteProperty(label, propertyKey)
}

// HasProperty reports whether a single-key property index exists on (label,
// propertyKey) (BACKLOG 21b) — a planner existence door mirroring
// HasComposite, so a query planner can prove the single-key accelerated path
// exists before routing an equality/range predicate to it instead of a label
// scan + post-filter. Unregistered labels return false. There is NO
// index-DDL epoch/invalidation signal, so call it per plan. Backends without
// property-index introspection return store.ErrCapabilityNotSupported.
func (a *API) HasProperty(label, propertyKey string) (bool, error) {
	ops, err := a.ready()
	if err != nil {
		return false, err
	}
	return ops.HasProperty(label, propertyKey)
}

// CreateRelProperty creates a relationship property index on the given rel type
// and property key (K3b). Returns store.ErrRelPropertyIndexUnsupported on the
// tiered store, which declines rel property index creation.
func (a *API) CreateRelProperty(typeName, propertyKey string) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.CreateRelProperty(typeName, propertyKey)
}

// DeleteRelProperty drops a relationship property index.
func (a *API) DeleteRelProperty(typeName, propertyKey string) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.DeleteRelProperty(typeName, propertyKey)
}

// HasRelProperty reports whether a relationship property index exists on
// (typeName, propertyKey) (BACKLOG 21b) — the rel-side mirror of
// HasProperty. Unregistered relationship types return false. Backends
// without rel-property-index introspection (tiered) return
// store.ErrCapabilityNotSupported.
func (a *API) HasRelProperty(typeName, propertyKey string) (bool, error) {
	ops, err := a.ready()
	if err != nil {
		return false, err
	}
	return ops.HasRelProperty(typeName, propertyKey)
}

// CreateComposite creates a composite property index over the declared,
// ORDER-PRESERVING keys (2..4) under one label — EQUALITY-only in v1 (no
// partial-prefix or range semantics). See docs/query-planners.md "Composite
// property indexes" for planner guidance on when this beats a single-key
// index + post-filter.
func (a *API) CreateComposite(label string, keys []string) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.CreateComposite(label, keys)
}

// DeleteComposite drops a composite property index declared over the exact
// ordered keys.
func (a *API) DeleteComposite(label string, keys []string) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.DeleteComposite(label, keys)
}

// HasComposite reports whether a composite index definition exists on label
// whose declared key SET equals keys — ORDER-INSENSITIVE, duplicates in keys
// ignored; exactly the match rule the NodesByLabelAndProperties query door
// uses to decide index-vs-label-scan. A query planner calls this BEFORE
// routing a multi-property equality match through ByLabelAndProperties, so a
// missing definition keeps the single-key property-index plan instead of
// silently regressing to a label scan + post-filter. Unregistered labels
// return false. O(definitions on the label); there is NO index-DDL
// epoch/invalidation signal, so call it per plan rather than caching across
// DDL you do not control. Backends without composite-index introspection
// (tiered, wrappers) return store.ErrCapabilityNotSupported.
func (a *API) HasComposite(label string, keys []string) (bool, error) {
	ops, err := a.ready()
	if err != nil {
		return false, err
	}
	return ops.HasComposite(label, keys)
}

// ListComposites returns the DECLARED, ORDER-PRESERVING key tuple of every
// composite index registered under label (one entry per definition; distinct
// orderings of the same key set are distinct definitions and are both
// listed). Unregistered labels return an empty slice. The returned slices
// are caller-owned copies. See HasComposite for the caching guidance.
func (a *API) ListComposites(label string) ([][]string, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.ListComposites(label)
}

// CreateHighFrequency creates a high-frequency temporal index.
func (a *API) CreateHighFrequency(label string, bucketSize time.Duration) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.CreateHighFrequency(label, bucketSize)
}

// DeleteHighFrequency drops a high-frequency temporal index.
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

// DeleteTemporal drops a temporal interval index.
func (a *API) DeleteTemporal(label string) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.DeleteTemporal(label)
}

// HasTemporal reports whether a temporal interval index exists on label
// (BACKLOG 21b). Reports the interval-index KIND specifically — a
// high-frequency index on the same label (CreateHighFrequency) does not
// count. Unregistered labels return false. Backends without temporal-index
// introspection return store.ErrCapabilityNotSupported.
func (a *API) HasTemporal(label string) (bool, error) {
	ops, err := a.ready()
	if err != nil {
		return false, err
	}
	return ops.HasTemporal(label)
}

// CreateRelTemporal creates a temporal interval index on relationships with
// the given rel type (BACKLOG 21c). Returns storepkg.ErrCapabilityNotSupported
// on stores that decline the capability (tiered, sharded).
func (a *API) CreateRelTemporal(typeName string) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.CreateRelTemporal(typeName)
}

// DeleteRelTemporal drops a relationship-type temporal interval index.
func (a *API) DeleteRelTemporal(typeName string) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.DeleteRelTemporal(typeName)
}

// CreateVector creates a vector (kNN) index. Existing indexed vectors with
// non-finite coordinates are rejected with ErrInvalidVectorValue.
//
// The index defaults to an approximate HNSW engine (see CLAUDE.md "Vector
// Indexes" for the recall target and tuning); use CreateVectorWithOptions
// for the brute-force escape hatch or HNSW tuning.
//
// Persistence: vector index DEFINITIONS survive restart, but the index
// entries do not — on reopen the index is rebuilt by scanning every node
// carrying the indexed label/property. For large graphs this is the dominant
// restart cost; budget reopen latency accordingly (and prefer recomputable
// embeddings — there is no persistent per-entity vector cache).
func (a *API) CreateVector(label, propertyKey string, dims int, metric storepkg.DistanceMetric) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.CreateVector(label, propertyKey, dims, metric)
}

// CreateVectorWithOptions is CreateVector with additional control over the
// search engine (opts.UseBruteForce) and HNSW tuning (opts.M /
// EfConstruction / EfSearch). A zero-value opts is identical to
// CreateVector (documented HNSW defaults: M=16, EfConstruction=200,
// EfSearch=64).
func (a *API) CreateVectorWithOptions(label, propertyKey string, dims int, metric storepkg.DistanceMetric, opts storepkg.VectorIndexOptions) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.CreateVectorWithOptions(label, propertyKey, dims, metric, opts)
}

// DeleteVector drops a vector index.
func (a *API) DeleteVector(label, propertyKey string) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.DeleteVector(label, propertyKey)
}

// VectorIndexInfo returns the declared configuration (dims, metric, engine,
// HNSW tuning) of the vector index on (label, propertyKey), or (zero value,
// false, nil) if none exists (BACKLOG 21b) — lets a planner validate a query
// vector's dimensionality and pick the right engine BEFORE issuing
// SearchNearest, rather than discovering a mismatch as a runtime error.
// Unregistered labels return (zero value, false, nil). Backends without
// vector-index introspection return store.ErrCapabilityNotSupported.
func (a *API) VectorIndexInfo(label, propertyKey string) (storepkg.VectorIndexInfo, bool, error) {
	ops, err := a.ready()
	if err != nil {
		return storepkg.VectorIndexInfo{}, false, err
	}
	return ops.VectorIndexInfo(label, propertyKey)
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
	return apiutil.CloneSlice(a.ops.Providers())
}
