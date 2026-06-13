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

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/grapherr"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
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
	AddByIDIfAbsent(ctx context.Context, id types.NodeID, labels []string, props map[string]any) (*types.Node, bool, error)

	All(opts storepkg.QueryOpts) ([]*types.Node, error)
	ByLabel(label string, opts storepkg.QueryOpts) ([]*types.Node, error)
	ForEachByLabel(label string, opts storepkg.QueryOpts, fn func(*types.Node) bool) error
	ForEachByLabelPropertyRange(label, propKey string, min, max float64, inclMin, inclMax bool, opts storepkg.QueryOpts, fn func(*types.Node) bool) error
	RangeCardinality(label, propKey string, min, max float64, inclMin, inclMax bool, opts storepkg.QueryOpts) (int64, bool, error)
	ForEachDocValues(label string, propKeys []string, fn func(types.NodeID, []any, []bool) bool) (uint64, bool, error)
	ForEachDocValuesMulti(labels []string, propKeys []string, fn func(types.NodeID, []any, []bool) bool) (uint64, bool, error)
	DocValuesSnapshot(label string, propKeys []string) (types.NodeColumnReader, uint64, bool, error)
	NodeMutationEpoch() uint64
	ByLabelAndProperty(label, key string, value any, opts storepkg.QueryOpts) ([]*types.Node, error)
	Count() (int, error)
	CountByLabel(label string) (int, error)

	SetProperty(ctx context.Context, id types.NodeID, key string, value any) error
	DeleteProperty(ctx context.Context, id types.NodeID, key string) error
	CompareAndSetProperty(ctx context.Context, id types.NodeID, key string, expected, newVal any) (bool, error)

	AddLabel(ctx context.Context, id types.NodeID, label string) error
	RemoveLabel(ctx context.Context, id types.NodeID, label string) error
	HasLabel(n *types.Node, label string) bool
	Labels(n *types.Node) []string
	PrimaryLabel(n *types.Node) string

	CloseVersion(ctx context.Context, id types.NodeID, t types.Instant) error
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

// AddByIDIfAbsent creates a node with the supplied id and labels+props if no
// node with that id already exists; if a node is already present at that id
// it returns the existing node with created=false and no error. Mirror of
// Rels.AddByIDIfAbsent for Node/Rel parity (S3). Returns ErrZeroID or
// ErrInvalidID for invalid IDs (same as Import).
func (a *API) AddByIDIfAbsent(ctx context.Context, id types.NodeID, labels []string, props map[string]any) (*types.Node, bool, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, false, err
	}
	return ops.AddByIDIfAbsent(ctx, id, labels, props)
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

// ForEachByLabel streams nodes carrying the given label to fn in
// snowflake-ID order without materializing the result slice; fn returning
// false stops early. Isolation is relaxed relative to ByLabel: rows are
// fetched and fn runs without graph locks held (fn may call back into the
// graph; concurrent writes are neither blocked nor observed atomically).
// Rows are shared frozen pointers — never mutate them.
func (a *API) ForEachByLabel(label string, opts storepkg.QueryOpts, fn func(*types.Node) bool) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.ForEachByLabel(label, opts, fn)
}

// ForEachByLabelPropertyRange streams nodes whose NUMERIC propKey value
// lies within [min, max] from the property index's ordered view (ordered range view).
// The view over-selects (float64 sort keys, ulp-widened bounds) — fn must
// re-check its predicate exactly. Returns graph.ErrIndexNotFound when no
// usable ordered view exists; callers fall back to a label scan. Same
// relaxed isolation and frozen-row contract as ForEachByLabel.
func (a *API) ForEachByLabelPropertyRange(label, propKey string, min, max float64, inclMin, inclMax bool, opts storepkg.QueryOpts, fn func(*types.Node) bool) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.ForEachByLabelPropertyRange(label, propKey, min, max, inclMin, inclMax, opts, fn)
}

// RangeCardinality returns the count of the label's nodes whose numeric propKey
// value lies within [min, max] (inclusivity per flags), summed from the property
// index's sorted per-value bucket sizes (R1) — O(distinct values in range), NO
// node scan. The second return is exact: false means the index declined (absent /
// poisoned by an integer past 2^53 / temporal opts) and the caller must
// scan-and-count. Fractional values and bounds are counted exactly. The bounds
// must already capture the WHOLE predicate. See core.NodeOps.RangeCardinality.
func (a *API) RangeCardinality(label, propKey string, min, max float64, inclMin, inclMax bool, opts storepkg.QueryOpts) (int64, bool, error) {
	ops, err := a.ready()
	if err != nil {
		return 0, false, err
	}
	return ops.RangeCardinality(label, propKey, min, max, inclMin, inclMax, opts)
}

// ForEachDocValues (X5) streams the requested property columns for a label's nodes
// from a cached columnar snapshot, avoiding the per-node fetch+decode in grouped
// aggregation. ok=false means the column path is unusable (no capability, unknown/
// empty/over-cap label, or a non-buildable property) and the caller must fall back.
// gen is the snapshot's node-mutation epoch; re-check NodeMutationEpoch()==gen
// after consuming the rows to detect a concurrent writer (Gate 2).
// See core.NodeOps.ForEachDocValues.
func (a *API) ForEachDocValues(label string, propKeys []string, fn func(types.NodeID, []any, []bool) bool) (uint64, bool, error) {
	ops, err := a.ready()
	if err != nil {
		return 0, false, err
	}
	return ops.ForEachDocValues(label, propKeys, fn)
}

// ForEachDocValuesMulti (X5) streams the requested property columns for a LABEL
// INTERSECTION (multi-label patterns like (p:A:B)) from a cached columnar snapshot
// over the intersection membership, avoiding the per-node fetch+decode. ok=false
// means the column path is unusable (no capability, an unknown label, an empty/
// over-cap intersection, or a non-buildable property) and the caller falls back.
// See core.NodeOps.ForEachDocValuesMulti.
func (a *API) ForEachDocValuesMulti(labels []string, propKeys []string, fn func(types.NodeID, []any, []bool) bool) (uint64, bool, error) {
	ops, err := a.ready()
	if err != nil {
		return 0, false, err
	}
	return ops.ForEachDocValuesMulti(labels, propKeys, fn)
}

// DocValuesSnapshot (X5) returns a random-access point-lookup handle over a label's
// cached column snapshot — the expand-aggregation target side (fetch b's properties
// by ID without materializing b). ok=false means unusable (no capability, unknown/
// empty/over-cap label, or an unbuildable requested property — critique Trap B) and
// the caller falls back. See core.NodeOps.DocValuesSnapshot.
func (a *API) DocValuesSnapshot(label string, propKeys []string) (types.NodeColumnReader, uint64, bool, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, 0, false, err
	}
	return ops.DocValuesSnapshot(label, propKeys)
}

// NodeMutationEpoch returns the store's node-mutation epoch (0 if the backend
// lacks the DocValues capability) — the consumer's Gate-2 staleness check.
func (a *API) NodeMutationEpoch() uint64 {
	ops, err := a.ready()
	if err != nil {
		return 0
	}
	return ops.NodeMutationEpoch()
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

// SetProperty sets a single property honoring ctx.
func (a *API) SetProperty(ctx context.Context, id types.NodeID, key string, value any) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.SetProperty(ctx, id, key, value)
}

// DeleteProperty deletes a single property honoring ctx.
func (a *API) DeleteProperty(ctx context.Context, id types.NodeID, key string) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.DeleteProperty(ctx, id, key)
}

// CompareAndSetProperty atomically updates a property honoring ctx.
func (a *API) CompareAndSetProperty(ctx context.Context, id types.NodeID, key string, expected, newVal any) (bool, error) {
	ops, err := a.ready()
	if err != nil {
		return false, err
	}
	return ops.CompareAndSetProperty(ctx, id, key, expected, newVal)
}

// AddLabel adds a label to a node, honoring ctx.
func (a *API) AddLabel(ctx context.Context, id types.NodeID, label string) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.AddLabel(ctx, id, label)
}

// RemoveLabel removes a label from a node, honoring ctx.
func (a *API) RemoveLabel(ctx context.Context, id types.NodeID, label string) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.RemoveLabel(ctx, id, label)
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

// CloseVersion closes the open-ended ValidTo of the current node version, honoring ctx.
func (a *API) CloseVersion(ctx context.Context, id types.NodeID, t types.Instant) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.CloseVersion(ctx, id, t)
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
