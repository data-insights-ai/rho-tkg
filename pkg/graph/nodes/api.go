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
	"iter"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/grapherr"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Ops is the subset of *core.NodeOps the nodes sub-API forwards to.
type Ops interface {
	Add(ctx context.Context, labels []string, props map[string]any) (*types.Node, error)
	AddWithTx(ctx context.Context, labels []string, props map[string]any, txFrom types.Instant) (*types.Node, error)
	Get(ctx context.Context, id types.NodeID) (*types.Node, error)
	GetByIDs(ids []types.NodeID) ([]*types.Node, error)
	Update(ctx context.Context, id types.NodeID, updates map[string]any) (*types.Node, error)
	UpdateInPlace(ctx context.Context, id types.NodeID, updates map[string]any) (*types.Node, error)
	Delete(ctx context.Context, id types.NodeID) error
	Import(ctx context.Context, id types.NodeID, labels []string, props map[string]any) (*types.Node, error)
	AddByIDIfAbsent(ctx context.Context, id types.NodeID, labels []string, props map[string]any) (*types.Node, bool, error)
	GetOrCreateByKey(ctx context.Context, label, propertyKey string, value any, extraProps map[string]any) (*types.Node, bool, error)

	All(opts storepkg.QueryOpts) ([]*types.Node, error)
	ForEach(opts storepkg.QueryOpts, fn func(*types.Node) bool) error
	ByLabel(label string, opts storepkg.QueryOpts) ([]*types.Node, error)
	ForEachByLabel(label string, opts storepkg.QueryOpts, fn func(*types.Node) bool) error
	ForEachByLabelPropertyRange(label, propKey string, min, max float64, inclMin, inclMax bool, opts storepkg.QueryOpts, fn func(*types.Node) bool) error
	ForEachByLabelPropertyRangeOrdered(label, propKey string, min, max float64, inclMin, inclMax, desc bool, opts storepkg.QueryOpts, fn func(*types.Node) bool) error
	ForEachByLabelPropertyPrefix(label, propKey, prefix string, desc bool, opts storepkg.QueryOpts, fn func(*types.Node) bool) error
	RangeCardinality(label, propKey string, min, max float64, inclMin, inclMax bool, opts storepkg.QueryOpts) (int64, bool, error)
	ForEachDocValues(label string, propKeys []string, fn func(types.NodeID, []any, []bool) bool) (uint64, bool, error)
	ForEachDocValuesMulti(labels []string, propKeys []string, fn func(types.NodeID, []any, []bool) bool) (uint64, bool, error)
	DocValuesSnapshot(label string, propKeys []string) (types.NodeColumnReader, uint64, bool, error)
	DocValuesSnapshotAsOf(label string, propKeys []string, txAt types.Instant) (types.NodeColumnReader, uint64, bool, error)
	ForEachDocValuesAsOf(label string, propKeys []string, txAt types.Instant, fn func(types.NodeID, []any, []bool) bool) (uint64, bool, error)
	NodeMutationEpoch() uint64
	NodeLabelMutationEpoch(label string) uint64
	ByLabelAndProperty(label, key string, value any, opts storepkg.QueryOpts) ([]*types.Node, error)
	ByLabelAndProperties(label string, values map[string]any, opts storepkg.QueryOpts) ([]*types.Node, error)
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

// AddWithTx creates a node like Add but stamps the caller-supplied txFrom as
// its transaction time (backfill). Requires the graph to be opened with
// Config.AllowTxBackfill; see NodeOps.AddWithTx.
func (a *API) AddWithTx(ctx context.Context, labels []string, props map[string]any, txFrom types.Instant) (*types.Node, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.AddWithTx(ctx, labels, props, txFrom)
}

// GetOrCreateByKey atomically returns the current node carrying value for
// (label, propertyKey), or creates one if absent — under a single value lock so
// concurrent callers with the same key produce exactly one create. The bool
// reports whether THIS call created the node. Works with or without an active
// unique constraint on (label, propertyKey); value must be an indexable scalar
// (floats rejected with ErrUniqueUnsupportedType). extraProps seed a freshly
// created node (the keyed property wins) and are ignored on a hit. The returned
// node is a mutable, independent copy (Get semantics).
func (a *API) GetOrCreateByKey(ctx context.Context, label, propertyKey string, value any, extraProps map[string]any) (*types.Node, bool, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, false, err
	}
	return ops.GetOrCreateByKey(ctx, label, propertyKey, value, extraProps)
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
// Rels.AddByIDIfAbsent for Node/Rel parity. Returns ErrZeroID or
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

// ForEach streams all nodes matching opts to fn without materializing the full
// result slice when the backend can provide a current-state ID scan. fn returning
// false stops early.
func (a *API) ForEach(opts storepkg.QueryOpts, fn func(*types.Node) bool) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.ForEach(opts, fn)
}

// Iter returns a Go 1.23+ range-over-func iterator over nodes matching opts,
// built directly on top of ForEach (no new scan machinery) — same row set,
// same order, same temporal/paginated fallback to All that ForEach itself
// takes for the given opts (see ForEach's doc). Differences from ForEach:
//
//   - ctx is checked once per row (non-blocking, before each yield); on
//     cancellation the iterator yields (nil, ctx.Err()) exactly once and
//     stops the underlying scan.
//   - Any internal scan error yields (nil, err) exactly once and stops.
//   - Breaking out of the range loop stops the underlying ForEach scan
//     immediately; no further rows are fetched.
//
// Row ownership mirrors ForEach exactly, and it is NOT uniform across opts:
// a plain current-state, unpaginated scan (ForEach's fast path) hands each
// row to fn as an independent, already-mutable copy, while a temporal filter
// or Limit/After pagination (ForEach's fallback to All) hands back shared
// FROZEN rows on a trusted backend — DeepCopy before mutating in that case.
// When in doubt, treat every yielded row as read-only and DeepCopy before
// mutating.
//
// A nil/zero-value API yields a single (nil, grapherr.ErrNilGraph).
func (a *API) Iter(ctx context.Context, opts storepkg.QueryOpts) iter.Seq2[*types.Node, error] {
	return func(yield func(*types.Node, error) bool) {
		ops, err := a.ready()
		if err != nil {
			yield(nil, err)
			return
		}
		iterateForEach(ctx, yield, func(fn func(*types.Node) bool) error {
			return ops.ForEach(opts, fn)
		})
	}
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

// ForEachByLabelPropertyRangeOrdered streams nodes whose NUMERIC propKey value
// lies within [min, max] in CONTRACTUAL VALUE ORDER — ascending, or
// descending when desc, with ties broken by node ID ascending — the ordered /
// top-k access path for ORDER BY prop [LIMIT k]. fn returning false stops the
// scan at the index level, pushing a LIMIT down. The ordered view over-selects
// (float64 sort keys, ulp-widened bounds), so fn MUST re-check its predicate
// with exact comparison semantics.
//
// Non-temporal opts use the index-backed fast path and return
// graph.ErrIndexNotFound when no usable ordered view exists for (label, propKey).
// A TEMPORAL QueryOpts combination is served by a sound full fold (values resolved
// at the pin, sorted by value-at-t) — correct over a past belief/valid state,
// needs no index, O(N log N). Same relaxed isolation and frozen-row contract as
// ForEachByLabel.
func (a *API) ForEachByLabelPropertyRangeOrdered(label, propKey string, min, max float64, inclMin, inclMax, desc bool, opts storepkg.QueryOpts, fn func(*types.Node) bool) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.ForEachByLabelPropertyRangeOrdered(label, propKey, min, max, inclMin, inclMax, desc, opts, fn)
}

// ForEachByLabelPropertyPrefix streams nodes whose STRING propKey value begins
// with prefix in CONTRACTUAL VALUE ORDER — lexicographic ascending, or descending
// when desc, with ties broken by node ID ascending — the string prefix /
// `STARTS WITH` access path. fn returning false stops the scan at the index level,
// pushing an ORDER BY ... LIMIT k down. An empty prefix matches every string value
// of the property. Unlike the numeric range doors the string ordered view is
// EXACT, so rows already satisfy the prefix.
//
// Non-temporal opts use the index-backed fast path and return
// graph.ErrIndexNotFound when no usable ordered view exists for (label, propKey).
// A TEMPORAL QueryOpts combination is served by a sound full fold (values resolved
// at the pin, sorted lexicographically) — correct over a past belief/valid state,
// needs no index. Same relaxed isolation and frozen-row contract as ForEachByLabel.
func (a *API) ForEachByLabelPropertyPrefix(label, propKey, prefix string, desc bool, opts storepkg.QueryOpts, fn func(*types.Node) bool) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.ForEachByLabelPropertyPrefix(label, propKey, prefix, desc, opts, fn)
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

// DocValuesSnapshot returns a random-access point-lookup handle over a label's
// cached column snapshot — the expand-aggregation target side (fetch b's properties
// by ID without materializing b). ok=false means unusable (no capability, unknown/
// empty/over-cap label, or an unbuildable requested property) and
// the caller falls back. See core.NodeOps.DocValuesSnapshot.
func (a *API) DocValuesSnapshot(label string, propKeys []string) (types.NodeColumnReader, uint64, bool, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, 0, false, err
	}
	return ops.DocValuesSnapshot(label, propKeys)
}

// DocValuesSnapshotAsOf is the transaction-time (AS OF) analogue of
// DocValuesSnapshot: a columnar handle over a label's members as believed at txAt
// (a knowledge-time / TxPin belief-state pin) — the time-travel aggregation
// target. It reuses the pinned ByLabel resolver, so it works on EVERY backend
// (including tiered/sharded, which decline the current-state scanner) and is not
// cached (a past pin is immutable + one-shot). ok=false means a requested property
// is not a uniform column at txAt (caller falls back). A pin before a retention
// watermark returns ErrRetentionExpired. gen is DELIBERATELY 0 — an as-of snapshot
// is a frozen point-in-time read with no current-state staleness signal, so do
// NOT gen-recheck it (see core.NodeOps.DocValuesSnapshotAsOf).
func (a *API) DocValuesSnapshotAsOf(label string, propKeys []string, txAt types.Instant) (types.NodeColumnReader, uint64, bool, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, 0, false, err
	}
	return ops.DocValuesSnapshotAsOf(label, propKeys, txAt)
}

// ForEachDocValuesAsOf streams the label's members as believed at txAt, invoking
// fn(id, vals, present) per row without materializing a node — the streaming
// (AS OF) analogue of ForEachDocValues, and the time-travel aggregation target.
// fn returning false stops the scan. ok=false means a requested property is not a
// uniform column at txAt (caller falls back). Works on every backend;
// retention-guarded; gen is deliberately 0. See core.NodeOps.ForEachDocValuesAsOf.
func (a *API) ForEachDocValuesAsOf(label string, propKeys []string, txAt types.Instant, fn func(types.NodeID, []any, []bool) bool) (uint64, bool, error) {
	ops, err := a.ready()
	if err != nil {
		return 0, false, err
	}
	return ops.ForEachDocValuesAsOf(label, propKeys, txAt, fn)
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

// NodeLabelMutationEpoch returns the PER-LABEL node-mutation epoch (0 if the backend
// lacks the DocValues capability or the label is unknown) — the value a single-label
// DocValues result returns as gen (BACKLOG 4b). It advances only when a node carrying
// THIS label is written, not on unrelated-label writes, so a Gate-2 re-check on a
// single-label aggregate should use this (not NodeMutationEpoch) to avoid discarding a
// still-valid result after an unrelated-label write.
func (a *API) NodeLabelMutationEpoch(label string) uint64 {
	ops, err := a.ready()
	if err != nil {
		return 0
	}
	return ops.NodeLabelMutationEpoch(label)
}

// ByLabelAndProperty returns nodes carrying the label whose property matches value.
func (a *API) ByLabelAndProperty(label, key string, value any, opts storepkg.QueryOpts) ([]*types.Node, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.ByLabelAndProperty(label, key, value, opts)
}

// ByLabelAndProperties returns nodes carrying the label whose properties
// match EVERY (key, value) pair in values (AND-conjunction, EQUALITY-only in
// v1). Accelerated by a composite property index (see
// Index().CreateComposite) whose declared key set equals values' keys;
// otherwise answered via a label-scan + post-filter fallback — see
// docs/query-planners.md "Composite property indexes".
func (a *API) ByLabelAndProperties(label string, values map[string]any, opts storepkg.QueryOpts) ([]*types.Node, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.ByLabelAndProperties(label, values, opts)
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

// iterateForEach drives a Go 1.23+ range-over-func Seq2 from an
// already-validated ForEach-shaped streaming primitive (scan): ctx is checked
// once per row (non-blocking, before each yield) and the scan stops
// immediately either on ctx cancellation — yielding (zero, ctx.Err()) exactly
// once — or when the consumer's yield returns false (a normal early stop,
// nothing further is yielded). Any error the scan itself returns (and did not
// already surface via a per-row ctx check) is yielded once at the end.
func iterateForEach[T any](ctx context.Context, yield func(T, error) bool, scan func(fn func(T) bool) error) {
	var zero T
	if err := ctx.Err(); err != nil {
		yield(zero, err)
		return
	}
	stopped := false
	err := scan(func(v T) bool {
		if cErr := ctx.Err(); cErr != nil {
			stopped = true
			yield(zero, cErr)
			return false
		}
		if !yield(v, nil) {
			stopped = true
			return false
		}
		return true
	})
	if stopped {
		return
	}
	if err != nil {
		yield(zero, err)
	}
}
