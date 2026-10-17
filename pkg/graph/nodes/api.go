// Package nodes is a sub-API accessor exposing the node-management surface of
// a Graph. The package declares a local interface (Ops) listing the methods
// the API forwards; *core.NodeOps implements it implicitly.
//
// Each method takes a context (Add, Get, Update, UpdateInPlace, Delete,
// CompareAndSetProperty); pass context.Background() when there is none.
package nodes

import (
	"context"
	"iter"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/apiutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/grapherr"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Ops is the subset of *core.NodeOps the nodes sub-API forwards to.
type Ops interface {
	Add(ctx context.Context, labels []string, props map[string]any) (*types.Node, error)
	AddWithTx(ctx context.Context, labels []string, props map[string]any, txFrom types.Instant) (*types.Node, error)
	Get(ctx context.Context, id types.NodeID) (*types.Node, error)
	Lend(ctx context.Context, id types.NodeID) (*types.Node, error)
	LendByIDs(ctx context.Context, ids []types.NodeID, fn func(i int, row *types.Node) bool) error
	MaxOrdinal() (uint32, bool, error)
	ScanKeepsOrder(label string) (bool, error)
	DocValuesColumn(label, propertyKey string) (storepkg.DocValuesColumn, bool, error)
	GetByIDs(ids []types.NodeID) ([]*types.Node, error)
	Update(ctx context.Context, id types.NodeID, updates map[string]any) (*types.Node, error)
	UpdateWithTx(ctx context.Context, id types.NodeID, updates map[string]any, txFrom types.Instant) (*types.Node, error)
	UpdateInPlace(ctx context.Context, id types.NodeID, updates map[string]any) (*types.Node, error)
	Delete(ctx context.Context, id types.NodeID) error
	DeleteWithTx(ctx context.Context, id types.NodeID, txTo types.Instant) error
	Retract(ctx context.Context, id types.NodeID) error
	RetractWithTx(ctx context.Context, id types.NodeID, txTo types.Instant) error
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
	RangeCardinalityAt(label, propKey string, min, max float64, inclMin, inclMax bool, opts storepkg.QueryOpts) (int64, bool, error)
	ForEachByLabelPropertyRangeAnyVersion(label, key string, min, max float64, inclMin, inclMax bool, pin types.Instant, fn func(*types.Node) bool) error
	RangeCardinalityAnyVersion(label, key string, min, max float64, inclMin, inclMax bool, pin types.Instant) (storepkg.RangeAnyVersionCount, bool, error)
	CountTimelineByLabel(label string, pin types.Instant, opts storepkg.CountTimelineOpts) ([]storepkg.CountSegment, bool, error)
	ValidHullByLabel(label string, pin types.Instant, opts storepkg.CountTimelineOpts) (storepkg.ValidHull, bool, error)
	ForEachByIDsAnyVersion(ids []types.NodeID, pin types.Instant, fn func(*types.Node) bool) error
	ForEachByLabelOrdinals(label string, set []uint64, opts storepkg.QueryOpts, fn func(*types.Node) bool) error
	ForEachIDByLabelPropertyRangeOrdered(ctx context.Context, label, propKey string, min, max float64, inclMin, inclMax, desc bool, opts storepkg.QueryOpts, fn func(types.NodeID, any) bool) error
	ForEachIDByLabelPropertyPrefix(ctx context.Context, label, propKey, prefix string, desc bool, opts storepkg.QueryOpts, fn func(types.NodeID, any) bool) error
	CountByLabelPropertyValue(label, propKey string, value any, opts storepkg.QueryOpts) (int64, bool, error)
	ForEachDocValues(label string, propKeys []string, fn func(types.NodeID, []any, []bool) bool) (uint64, bool, error)
	ForEachDocValuesMulti(labels []string, propKeys []string, fn func(types.NodeID, []any, []bool) bool) (uint64, bool, error)
	ForEachDocValuesOrdinal(label string, propKeys []string, fn func(types.NodeID, uint32, []any, []bool) bool) (uint64, bool, error)
	ForEachDocValuesMultiOrdinal(labels []string, propKeys []string, fn func(types.NodeID, uint32, []any, []bool) bool) (uint64, bool, error)
	DocValuesSnapshot(label string, propKeys []string) (types.NodeColumnReader, uint64, bool, error)
	DocValuesSnapshotMulti(labels []string, propKeys []string) (types.NodeColumnReader, uint64, bool, error)
	DocValuesSnapshotOrdinal(label string, propKeys []string) (types.NodeColumnChunkReader, uint64, bool, error)
	DocValuesSnapshotMultiOrdinal(labels []string, propKeys []string) (types.NodeColumnChunkReader, uint64, bool, error)
	DocValuesSnapshotAsOf(label string, propKeys []string, txAt types.Instant) (types.NodeColumnReader, uint64, bool, error)
	ForEachDocValuesAsOf(label string, propKeys []string, txAt types.Instant, fn func(types.NodeID, []any, []bool) bool) (uint64, bool, error)
	NodeMutationEpoch() uint64
	NodeLabelMutationEpoch(label string) uint64
	ByLabelAndProperty(label, key string, value any, opts storepkg.QueryOpts) ([]*types.Node, error)
	ByLabelAndProperties(label string, values map[string]any, opts storepkg.QueryOpts) ([]*types.Node, error)
	Count() (int, error)
	CountByLabel(label string) (int, error)
	CountByLabelAt(label string, opts storepkg.QueryOpts) (int, error)

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
	HasHistory(id types.NodeID) (bool, error)
	LatestStamps(id types.NodeID) (txFrom, txTo types.Instant, deleted bool, err error)
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
//
// Scope of the atomicity guarantee: it holds against other GetOrCreateByKey
// callers, and against writes to a value under an ACTIVE unique constraint —
// but NOT against a plain concurrent Add/Update writing the same value when
// no unique constraint exists on (label, propertyKey); such a write never
// takes the value lock and can still land a second node with the value. Add
// a unique constraint on (label, propertyKey) for protection against every
// writer, not just other GetOrCreateByKey callers.
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

// Lend returns the node's current row like Get, without Get's copy: the
// store's own frozen row (the shared pointer label scans hand out). The row
// is never written after it is lent, so it may be held and read from any
// goroutine; it is a snapshot at the read and does not follow later writes
// (re-check NodeMutationEpoch to know whether it is still current). It is
// frozen: error-returning mutators fail with types.ErrFrozenNode, the others
// panic, DeepCopy thaws. On a store other than memory, badger or tiered the
// row is a frozen copy. Errors as Get.
func (a *API) Lend(ctx context.Context, id types.NodeID) (*types.Node, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.Lend(ctx, id)
}

// LendByIDs lends the current row of each node in ids as Lend does (frozen,
// no copy where Lend makes none) and calls fn(i, row) for each ID that names
// a node, i its position in ids; an ID naming none is skipped (fn is not
// called for it). Calls come in ascending ID order (equal IDs in ids'
// order); fn returning false stops; fn runs with no lock held. The IDs are
// read in chunks of 4,096, each under one read: badger reads a chunk's rows
// missing from its cache through one snapshot in key order (not cached),
// memory under one lock, other stores per ID. The context is checked before
// each chunk. Safe for concurrent calls. Errors: an error for a nil fn,
// ErrInvalidStoreMutation for a non-positive ID (before any read),
// ErrGraphClosed, the context's error, a store read error.
func (a *API) LendByIDs(ctx context.Context, ids []types.NodeID, fn func(i int, row *types.Node) bool) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.LendByIDs(ctx, ids, fn)
}

// MaxOrdinal returns the largest dense node ordinal handed out
// (types.Node.Ordinal; size an ordinal-indexed array max+1) and ok=false when
// the store assigns none (tiered, badger with DisableOrdinals, external
// stores; every row's Ordinal is then 0). Ordinals are stable while an entity
// has a current row, never reused, and not persisted: see
// store.OrdinalCapability.
func (a *API) MaxOrdinal() (uint32, bool, error) {
	ops, err := a.ready()
	if err != nil {
		return 0, false, err
	}
	return ops.MaxOrdinal()
}

// ScanKeepsOrder reports whether a current-state ForEachByLabel scan of the
// label walks a member list the store keeps in ascending ID order, so the
// scan neither collects nor sorts the label's IDs (memory; badger with its RAM
// label index). False where a scan collects and sorts (badger with
// LabelIndexOnDisk, tiered, sharded, external stores) and for a scan with a
// temporal filter. A query planner charges a scan's sort only where it is
// false. See store.ScanOrderCapability.
func (a *API) ScanKeepsOrder(label string) (bool, error) {
	ops, err := a.ready()
	if err != nil {
		return false, err
	}
	return ops.ScanKeepsOrder(label)
}

// DocValuesColumn states, without building it, what ForEachDocValues /
// DocValuesSnapshot build for the key on the label's nodes: a numeric or a
// string column, or none (mixed or unsupported values, an empty or over-cap
// label, no column path). Derived from the store's exact value-class counters
// (store.DocValuesColumnOf); valid until the next node write. ok=false when the
// store keeps no such counters.
func (a *API) DocValuesColumn(label, propertyKey string) (storepkg.DocValuesColumn, bool, error) {
	ops, err := a.ready()
	if err != nil {
		return storepkg.DocValuesNone, false, err
	}
	return ops.DocValuesColumn(label, propertyKey)
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
//
// An update that sets tkg_valid_to without tkg_valid_from closes the current
// version as CloseVersion does: the version keeps its start and ends at
// tkg_valid_to; an end at or before that start returns ErrInvalidTimeRange and
// writes nothing (backlog 65).
func (a *API) Update(ctx context.Context, id types.NodeID, updates map[string]any) (*types.Node, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.Update(ctx, id, updates)
}

// UpdateWithTx updates the node like Update but stamps the caller-supplied
// txFrom as the transaction instant of the change: the superseded version's
// TxTo and the new version's TxFrom and UpdatedAt are txFrom instead of the
// system clock. Requires Config.AllowTxBackfill (ErrTxBackfillDisabled);
// txFrom must be a positive instant not in the future (ErrInvalidTxFrom) and
// must follow every stamp recorded for the node and the current version's
// start, and the update must change something (ErrTxOrder, which wraps
// ErrInvalidTxFrom). The commit clock is not moved.
func (a *API) UpdateWithTx(ctx context.Context, id types.NodeID, updates map[string]any, txFrom types.Instant) (*types.Node, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.UpdateWithTx(ctx, id, updates, txFrom)
}

// UpdateInPlace updates a node in place honoring ctx (no version chain entry).
//
// A tkg_valid_to at or before the row's effective start returns
// ErrInvalidTimeRange (backlog 65).
func (a *API) UpdateInPlace(ctx context.Context, id types.NodeID, updates map[string]any) (*types.Node, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.UpdateInPlace(ctx, id, updates)
}

// Delete deletes the node honoring ctx.
//
// Changes in v4.63.0 (backlogs 4, 7, 25): the delete is stamped at the commit
// clock also for a row whose start lies ahead, and a ValidTo it does not write
// (a recorded close at or after the delete, the open end of a row starting at
// or after it) is kept in a tombstone marked Temporal().DeleteKeptValidTo,
// whose validity ends at DeletedAt.
func (a *API) Delete(ctx context.Context, id types.NodeID) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.Delete(ctx, id)
}

// DeleteWithTx deletes the node and its relationships like Delete but ends
// their belief at the caller-supplied transaction instant txTo: the node's and
// every cascaded relationship's tombstone carry TxTo = DeletedAt = txTo, so a
// read pinned before txTo still sees them and a read pinned at or after it
// does not. Requires Config.AllowTxBackfill (ErrTxBackfillDisabled); txTo must
// be a positive instant not in the future (ErrInvalidTxFrom) and must follow
// every stamp recorded for the node and for each cascaded relationship and
// their current versions' starts; a recorded close (ValidTo) at or after txTo
// on any of them refuses (ErrTxOrder, which wraps ErrInvalidTxFrom). A refusal
// changes nothing. The commit clock is not moved.
//
// Changes in v4.63.0 (backlog 7): a recorded close at or after txTo no longer
// refuses; the tombstone keeps it (Temporal().DeleteKeptValidTo) for reads
// pinned before txTo.
func (a *API) DeleteWithTx(ctx context.Context, id types.NodeID, txTo types.Instant) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.DeleteWithTx(ctx, id, txTo)
}

// Retract says "this node was never true" (backlog 43): it removes the node
// and its relationships like Delete, but marks every tombstone of the cascade
// as a retraction, so a read pinned at or after the retraction instant T
// finds the node and those relationships at NO valid time (after a Delete the
// past stays readable at later pins), while a read pinned before T answers
// exactly as before. Delete ends validity; Retract ends belief. History keeps
// the tombstones (Temporal().Retracted == true), HasHistory stays true,
// LatestStamps reports deleted with txTo >= T, and current-state reads lose
// the node as after a Delete. Errors: ErrNodeNotFound for an unknown ID; a
// node already deleted or retracted is refused with an error matching both
// ErrEntityDeleted and ErrNodeNotFound.
func (a *API) Retract(ctx context.Context, id types.NodeID) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.Retract(ctx, id)
}

// RetractWithTx is Retract at the caller-supplied transaction instant txTo,
// with DeleteWithTx's contract: Config.AllowTxBackfill (ErrTxBackfillDisabled);
// txTo positive and not in the future (ErrInvalidTxFrom); after every stamp
// recorded for the node and each cascaded relationship and their current
// versions' starts, and no recorded close at or after txTo (ErrTxOrder, which
// wraps ErrInvalidTxFrom). A refusal changes nothing; the commit clock is not
// moved.
func (a *API) RetractWithTx(ctx context.Context, id types.NodeID, txTo types.Instant) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.RetractWithTx(ctx, id, txTo)
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
		apiutil.IterateForEach(ctx, yield, func(fn func(*types.Node) bool) error {
			return ops.ForEach(opts, fn)
		})
	}
}

// IterByLabel is the iter.Seq2 form of ForEachByLabel (BACKLOG 3 — the streaming
// whole-node door for a one-shot MATCH (n:L) RETURN n): it ranges over the label's
// nodes without materializing the result slice, riding the same bulk/parallel decode
// substrate as ByLabel so the scan is fast while peak memory stays O(1) nodes. Row
// ownership + isolation are identical to ForEachByLabel (shared frozen rows on a
// trusted backend — DeepCopy before mutating; a temporal filter or a backend without
// the streaming capability falls back to a materialized ByLabel then streams). A
// nil/zero-value API yields a single (nil, grapherr.ErrNilGraph).
func (a *API) IterByLabel(ctx context.Context, label string, opts storepkg.QueryOpts) iter.Seq2[*types.Node, error] {
	return func(yield func(*types.Node, error) bool) {
		ops, err := a.ready()
		if err != nil {
			yield(nil, err)
			return
		}
		apiutil.IterateForEach(ctx, yield, func(fn func(*types.Node) bool) error {
			return ops.ForEachByLabel(label, opts, fn)
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
// relaxed isolation and frozen-row contract as ForEachByLabel. A temporal
// opts (ValidAt, ValidStart+ValidEnd, TxAt, TxPin) is served without an index
// by a fold over ByLabel(label, opts): the nodes whose value in their version
// under opts is in range, in ID order.
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
// Sharded merges its slots' walks lazily in the same order (a LIMIT reads
// about k rows per slot), tiered reads each shard's rows in range and merges
// them (a LIMIT bounds what fn gets, not what is read); a part without a
// usable index fails the walk (since sigma round 10, sharded and tiered refused
// before).
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
// Sharded merges its slots' walks lazily in the same order (a LIMIT reads
// about k rows per slot), tiered reads each shard's rows in range and merges
// them (a LIMIT bounds what fn gets, not what is read); a part without a
// usable index fails the walk (since sigma round 10, sharded and tiered refused
// before).
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
// value lies within [min, max] (inclusivity per flags) from the property index,
// NO node scan. Cost depends on how the index was created: with
// PropertyIndexOptions.RangeCounts two prefix sums over the
// per-value counts, O(log chunks) plus at most 512 counts each, independent of
// the range's width; without it (the default) a walk over the range's distinct
// values summing their bucket sizes, O(distinct values in range). The second
// return is exact: false (count 0) when the door does not answer — no index, a
// backend without the count (tiered, badger with PropertyIndexOnDisk), an index
// poisoned by an integer past 2^53, or temporal opts — and the caller must
// scan-and-count. A NaN bound counts 0. Fractional values and bounds are
// counted exactly. The bounds must already capture the WHOLE predicate. See
// core.NodeOps.RangeCardinality.
func (a *API) RangeCardinality(label, propKey string, min, max float64, inclMin, inclMax bool, opts storepkg.QueryOpts) (int64, bool, error) {
	ops, err := a.ready()
	if err != nil {
		return 0, false, err
	}
	return ops.RangeCardinality(label, propKey, min, max, inclMin, inclMax, opts)
}

// RangeCardinalityAt is RangeCardinality at a temporal coordinate (round 8
// request 2): the number of versions ForEachByLabelPropertyRange returns under
// opts (each node's version ByLabel resolves) whose value, a float32 widened
// to float64, lies within [min, max] per the inclusivity flags (NaN never;
// After and Limit ignored). Counted from the declared property index's
// membership sidecar: only the nodes whose rows ever carried a value in the
// range are resolved, no label fold. exact=false with no error declines (the
// caller folds and counts): no temporal filter in opts (use RangeCardinality),
// invalid opts, a NaN bound, no property index on (label, propKey), a store
// without store.NodePropertyTxMemberRangeCapability (tiered, sharded, external
// stores), a depth-scoped read on a store with depth-aware history, an
// unknown label.
func (a *API) RangeCardinalityAt(label, propKey string, min, max float64, inclMin, inclMax bool, opts storepkg.QueryOpts) (int64, bool, error) {
	ops, err := a.ready()
	if err != nil {
		return 0, false, err
	}
	return ops.RangeCardinalityAt(label, propKey, min, max, inclMin, inclMax, opts)
}

// ForEachByLabelPropertyRangeAnyVersion visits, once each and in ascending ID
// order, the nodes carrying label of which some row recorded at pin (every
// valid-time version known at pin, those of nodes deleted before pin
// included, retracted lives dropped: the rows ByLabelAndProperty(label, key,
// v, opts) matches under opts = {ValidStart: types.UnboundedValidFrom,
// ValidEnd: MaxInt64, TxAt: pin}) holds a numeric value of key within
// [min, max] per the flags (a float32 widened, NaN never in range;
// sigma-tkgd round 9 R9-1). The visited
// set is the union over every value v in range of ByLabelAndProperty's
// matches at those opts. fn receives the newest such row of the node (the
// caller resolves the node's effective timeline itself); false stops. pin <=
// 0 or a NaN bound visits nothing. Served from the declared property index's
// membership sidecar (candidates pruned by the pin, each node's rows tested;
// no label fold). Errors: ErrIndexNotFound without a property index on the
// pair (or one being built), store.ErrCapabilityNotSupported where the
// sidecar range cannot serve (tiered, sharded, external stores, an Elements
// index), the label and key errors of the range doors, ErrNilCallback.
func (a *API) ForEachByLabelPropertyRangeAnyVersion(label, key string, min, max float64, inclMin, inclMax bool, pin types.Instant, fn func(*types.Node) bool) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.ForEachByLabelPropertyRangeAnyVersion(label, key, min, max, inclMin, inclMax, pin, fn)
}

// RangeCardinalityAnyVersion counts what ForEachByLabelPropertyRangeAnyVersion
// visits: Entities (the nodes) and Rows (their rows recorded at pin whose
// value is in range). exact=false declines where the visit door would fail
// (no index, an index being built, tiered, sharded, external stores, an
// Elements index); it never walks the label. pin <= 0 or a NaN bound: zero,
// exact.
func (a *API) RangeCardinalityAnyVersion(label, key string, min, max float64, inclMin, inclMax bool, pin types.Instant) (storepkg.RangeAnyVersionCount, bool, error) {
	ops, err := a.ready()
	if err != nil {
		return storepkg.RangeAnyVersionCount{}, false, err
	}
	return ops.RangeCardinalityAnyVersion(label, key, min, max, inclMin, inclMax, pin)
}

// CountTimelineByLabel is the number of label's members holding at each
// valid instant as recorded at pin (sigma-tkgd round 9 R9-2): maximal
// segments [From, To) of constant Count > 0, ascending, adjacent segments of
// equal Count merged, instants where no member holds left out. A member
// holds over its effective timeline's segments carrying the label at pin
// (NodeEffectiveTimeline), so it counts once at each instant: for every
// valid instant t the count equals CountByLabelAt(label, {ValidAt: t, TxAt:
// pin}). From == math.MinInt64 is an open start, To == 0 an open end. pin 0
// reads the current state (the commit clock). opts.UntimedFromMinusInf
// starts a member's untimed first segment at -inf (see
// store.CountTimelineOpts). Built once per (label, pin, option) from the
// label's effective scan and cached until a write changes the state at pin;
// exact=false declines (opts.MaxSegments exceeded, writers moving the
// label's state across three builds, a store whose history the scan cannot
// read). An unknown label: empty, exact.
//
// opts.CurrentRows (round 10, pin must be 0) counts the label's current rows
// instead, each over its own valid interval: the rows ForEachByLabel(label,
// QueryOpts{}) hands out, a member's earlier versions left out (see
// store.CountTimelineOpts.CurrentRows; not cached).
func (a *API) CountTimelineByLabel(label string, pin types.Instant, opts storepkg.CountTimelineOpts) ([]storepkg.CountSegment, bool, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, false, err
	}
	return ops.CountTimelineByLabel(label, pin, opts)
}

// ValidHullByLabel is the hull of the valid instants at which label's
// members hold (sigma-tkgd round 10 request 2a): the first segment's From
// and the last segment's end of CountTimelineByLabel(label, pin, opts), as a
// store.ValidHull with explicit Empty and OpenEnd flags (From
// types.UnboundedValidFrom: some member holds from -inf). Same pin, options
// (opts.MaxSegments does not apply), cache and declines as
// CountTimelineByLabel: after the first call at a pin it is answered from
// the cached timeline in O(1) until a write changes the state at the pin;
// the first call costs the timeline's build (one effective scan of the
// label). With opts.CurrentRows: the hull of the current rows, one pass over
// them per call.
func (a *API) ValidHullByLabel(label string, pin types.Instant, opts storepkg.CountTimelineOpts) (storepkg.ValidHull, bool, error) {
	ops, err := a.ready()
	if err != nil {
		return storepkg.ValidHull{}, false, err
	}
	return ops.ValidHullByLabel(label, pin, opts)
}

// ForEachByLabelOrdinals streams the nodes ForEachByLabel(label, opts)
// streams, restricted to the ID set given as a bitset over dense ordinals
// (round 8 request 4): bit k of set[k/64] selects the node whose current
// ordinal is k (n.Ordinal(); size the set from MaxOrdinal), current or at a
// temporal coordinate, ascending ID order, fn false stops, After and Limit
// as for ForEachByLabel. A node without a current row has no ordinal and is
// never selected, at a pin too. Where the store tells ordinals from RAM and
// lists members (badger, sharded) only the selected rows are read and, at a
// coordinate, only the selected candidates resolved; memory filters its
// in-RAM rows. Same isolation and frozen-row contract as ForEachByLabel.
// Errors: those of ForEachByLabel, store.ErrCapabilityNotSupported on a
// store without ordinals (tiered, badger DisableOrdinals).
func (a *API) ForEachByLabelOrdinals(label string, set []uint64, opts storepkg.QueryOpts, fn func(*types.Node) bool) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.ForEachByLabelOrdinals(label, set, opts, fn)
}

// ForEachIDByLabelPropertyRangeOrdered streams (ID, value) of the label's
// nodes whose numeric propKey value lies within [min, max] (inclusivity
// applied exactly) in value order (descending when desc), ties by ID
// ascending, from the property index alone: no node row is read. The value is
// the stored one in its stored type. fn returning false stops; fn runs with
// no lock held; the context is checked every 4,096 entries. ErrIndexNotFound
// (reported, never a slower walk) for temporal opts, a store without the
// door (tiered, sharded, external), badger PropertyIndexOnDisk, no index, an
// Elements index, an index being built or one holding an integer past 2^53.
func (a *API) ForEachIDByLabelPropertyRangeOrdered(ctx context.Context, label, propKey string, min, max float64, inclMin, inclMax, desc bool, opts storepkg.QueryOpts, fn func(types.NodeID, any) bool) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.ForEachIDByLabelPropertyRangeOrdered(ctx, label, propKey, min, max, inclMin, inclMax, desc, opts, fn)
}

// ForEachIDByLabelPropertyPrefix is the string twin of
// ForEachIDByLabelPropertyRangeOrdered: (ID, value) of the label's nodes
// whose string value begins with prefix, in byte order (descending when
// desc), ties by ID ascending, from the index alone. Same contract.
func (a *API) ForEachIDByLabelPropertyPrefix(ctx context.Context, label, propKey, prefix string, desc bool, opts storepkg.QueryOpts, fn func(types.NodeID, any) bool) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.ForEachIDByLabelPropertyPrefix(ctx, label, propKey, prefix, desc, opts, fn)
}

// CountByLabelPropertyValue counts the label's nodes whose propKey value equals
// value under the property equality ByLabelAndProperty uses (so it is
// len(ByLabelAndProperty)), from the property index's value sets, no node
// read. exact=false with no error declines (count
// otherwise): no property index on (label, propKey), a store without the
// capability (tiered, external stores), badger with PropertyIndexOnDisk
// (nodes), an index being built, an Elements index, a temporal value, a list
// or map, an unknown label, or a
// temporal QueryOpts. exact tells a planner whether exact counts are
// available for (label, propKey).
func (a *API) CountByLabelPropertyValue(label, propKey string, value any, opts storepkg.QueryOpts) (int64, bool, error) {
	ops, err := a.ready()
	if err != nil {
		return 0, false, err
	}
	return ops.CountByLabelPropertyValue(label, propKey, value, opts)
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
// means the column path is unusable (no capability, an unknown label, an
// over-cap intersection, or a non-buildable property) and the caller falls back.
// An empty intersection of known labels is served: ok=true, no rows.
// See core.NodeOps.ForEachDocValuesMulti.
func (a *API) ForEachDocValuesMulti(labels []string, propKeys []string, fn func(types.NodeID, []any, []bool) bool) (uint64, bool, error) {
	ops, err := a.ready()
	if err != nil {
		return 0, false, err
	}
	return ops.ForEachDocValuesMulti(labels, propKeys, fn)
}

// ForEachDocValuesOrdinal is ForEachDocValues with each row's dense node
// ordinal (types.Node.Ordinal) beside its ID: same rows, order and gen.
// ok=false where ForEachDocValues declines and where the store numbers no
// node (tiered, external stores, badger with DisableOrdinals); the caller then
// uses ForEachDocValues. See core.NodeOps.ForEachDocValuesOrdinal.
func (a *API) ForEachDocValuesOrdinal(label string, propKeys []string, fn func(id types.NodeID, ordinal uint32, vals []any, present []bool) bool) (uint64, bool, error) {
	ops, err := a.ready()
	if err != nil {
		return 0, false, err
	}
	return ops.ForEachDocValuesOrdinal(label, propKeys, fn)
}

// ForEachDocValuesMultiOrdinal is ForEachDocValuesMulti with each row's node
// ordinal, under ForEachDocValuesOrdinal's contract. See
// core.NodeOps.ForEachDocValuesMultiOrdinal.
func (a *API) ForEachDocValuesMultiOrdinal(labels []string, propKeys []string, fn func(id types.NodeID, ordinal uint32, vals []any, present []bool) bool) (uint64, bool, error) {
	ops, err := a.ready()
	if err != nil {
		return 0, false, err
	}
	return ops.ForEachDocValuesMultiOrdinal(labels, propKeys, fn)
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

// ForEachByIDsAnyVersion streams, for each node of ids, every row recorded at
// pin (sigma-tkgd round 10 request 6): every valid-time version known at the
// pin, those of a node deleted before the pin included, a retracted life
// dropped — the rows the any-version equality reads
// (ByLabelAndProperty(..., {ValidStart: types.UnboundedValidFrom, ValidEnd:
// MaxInt64, TxAt: pin})), whatever their labels. Nodes in ascending ID order,
// a repeated ID once, an unknown ID or one created after the pin nothing; a
// node's rows newest first. The set is read in key order through the
// effective scans' path (one store snapshot per chunk of 256 IDs on badger,
// each node under its entity lock); fn runs without graph locks and gets the
// store's shared frozen rows (DeepCopy to mutate); false stops. Errors: pin
// <= 0 ErrInvalidTimeRange, a pin above the commit clock ErrTxPinTooNew, an
// invalid ID ErrInvalidStoreMutation (before any row), a node whose history
// was compacted or purged below the pin as NodeEffectiveTimeline fails,
// ErrNilCallback.
func (a *API) ForEachByIDsAnyVersion(ids []types.NodeID, pin types.Instant, fn func(*types.Node) bool) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.ForEachByIDsAnyVersion(ids, pin, fn)
}

// DocValuesSnapshotMulti is DocValuesSnapshot over a LABEL INTERSECTION
// (sigma-tkgd round 10 request 5a): the members and values
// ForEachDocValuesMulti streams, as a by-position snapshot that reads typed
// chunks with the snapshot's dictionary codes (type-assert
// types.NodeColumnChunkReader; memory, badger, sharded, tiered). Same gen
// and decline contract as ForEachDocValuesMulti (an empty intersection of
// known labels is served as an empty snapshot).
func (a *API) DocValuesSnapshotMulti(labels []string, propKeys []string) (types.NodeColumnReader, uint64, bool, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, 0, false, err
	}
	return ops.DocValuesSnapshotMulti(labels, propKeys)
}

// DocValuesSnapshotOrdinal is DocValuesSnapshot as typed chunks whose
// Ordinals carry each member's dense node ordinal beside its ID (round 10
// request 5b): the rows, order, ordinals and gen of ForEachDocValuesOrdinal.
// ok=false where ForEachDocValuesOrdinal declines (tiered, external stores,
// badger DisableOrdinals).
func (a *API) DocValuesSnapshotOrdinal(label string, propKeys []string) (types.NodeColumnChunkReader, uint64, bool, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, 0, false, err
	}
	return ops.DocValuesSnapshotOrdinal(label, propKeys)
}

// DocValuesSnapshotMultiOrdinal is DocValuesSnapshotMulti with ordinals
// (ForEachDocValuesMultiOrdinal's rows), under DocValuesSnapshotOrdinal's
// contract.
func (a *API) DocValuesSnapshotMultiOrdinal(labels []string, propKeys []string) (types.NodeColumnChunkReader, uint64, bool, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, 0, false, err
	}
	return ops.DocValuesSnapshotMultiOrdinal(labels, propKeys)
}

// DocValuesSnapshotAsOf is the transaction-time (AS OF) analogue of
// DocValuesSnapshot: a columnar handle over a label's members as believed at txAt
// (a knowledge-time / TxPin belief-state pin) — the time-travel aggregation
// target. It reuses the pinned ByLabel resolver, so it works on EVERY backend
// (including those without the current-state scanner) and is not
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

// ByLabelAndProperty returns nodes carrying the label whose property matches
// value under the property equality (types.EqualIndexValues: a number matches
// every stored width of the same exact value, 3 = int8(3) = 3.0 =
// float32(3); NaN and nil match nothing; since v4.57.1, before which the
// stored width had to match).
func (a *API) ByLabelAndProperty(label, key string, value any, opts storepkg.QueryOpts) ([]*types.Node, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.ByLabelAndProperty(label, key, value, opts)
}

// ByLabelAndProperties returns nodes carrying the label whose properties
// match EVERY (key, value) pair in values (AND-conjunction, EQUALITY-only in
// v1, each pair under the property equality as ByLabelAndProperty). Accelerated by a composite property index (see
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

// CountByLabelAt returns len(ByLabel(label, opts)) without building the
// nodes: the exact count at the read coordinate in opts (valid time, a
// transaction-time pin, Depth, After, Limit). The label's counter when opts
// has no temporal filter, the as-of column set cached for the label and pin
// when one is; with TxAt (and ValidAt) the label's state cut at TxAt, cached
// for the label and pin from the second read on (no row read then);
// otherwise a pass over ByLabel's candidates that decides a
// candidate on its current row where that row alone answers. See
// core.NodeOps.CountByLabelAt.
func (a *API) CountByLabelAt(label string, opts storepkg.QueryOpts) (int, error) {
	ops, err := a.ready()
	if err != nil {
		return 0, err
	}
	return ops.CountByLabelAt(label, opts)
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

// CompareAndSetProperty atomically updates a property honoring ctx. A numeric
// expected matches under the property equality (int64(42) matches a stored
// int(42), a NaN matches nothing; since v4.57.1), any other by
// types.PropertyValueEqual.
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
	return apiutil.CloneSlice(a.ops.Labels(n))
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
//
// The chain holds the rows as written, including an excise's void pieces
// (Temporal().Void, Temporal().ExciseNodeValidity): a void row asserts that
// the node is absent over its [ValidFrom, ValidTo), it is not a state of the
// node. A consumer that reads raw History intervals must skip void rows (or
// read the effective timeline, NodeEffectiveTimeline, which never returns
// them).
func (a *API) History(id types.NodeID) ([]*types.Node, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.History(id)
}

// HasHistory reports whether the node has at least one history row (it was
// updated, cascaded, closed or deleted and the store still holds that history),
// without reading the rows: it equals len(History(id)) > 0 at every moment.
// False for a node with only its current row or an unknown ID. A badger store
// answers from a RAM set of the IDs with history, built by one key-only scan on
// the first call. Errors: ErrNilGraph, ErrGraphClosed, an invalid ID
// (ErrInvalidStoreMutation).
func (a *API) HasHistory(id types.NodeID) (bool, error) {
	ops, err := a.ready()
	if err != nil {
		return false, err
	}
	return ops.HasHistory(id)
}

// LatestStamps returns the node's newest transaction stamps over ALL its rows
// (the current row and every history row: superseded versions, rows a
// version-interval cascade appended after the current row's TxFrom, the
// delete tombstone) without reading the history: txFrom is the largest
// TxFrom, txTo the largest TxTo or DeletedAt (0 when no row was ended),
// deleted is true when the node has rows but no current row (hard-deleted; a
// re-imported ID whose newest life is live is not deleted). It equals the
// fold of Get and History at every moment. Errors: ErrNodeNotFound for an ID
// without any row, ErrNilGraph, ErrGraphClosed, an invalid ID
// (ErrInvalidStoreMutation).
func (a *API) LatestStamps(id types.NodeID) (txFrom, txTo types.Instant, deleted bool, err error) {
	ops, err := a.ready()
	if err != nil {
		return 0, 0, false, err
	}
	return ops.LatestStamps(id)
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
