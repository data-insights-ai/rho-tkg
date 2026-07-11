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
	"iter"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/grapherr"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Ops is the subset of *core.RelOps the rels sub-API forwards to.
type Ops interface {
	Add(ctx context.Context, typeName string, startNode, endNode *types.Node, props map[string]any) (*types.Relationship, error)
	AddWithTx(ctx context.Context, typeName string, startNode, endNode *types.Node, props map[string]any, txFrom types.Instant) (*types.Relationship, error)
	AddByID(ctx context.Context, typeName string, startID, endID types.NodeID, props map[string]any) (*types.Relationship, error)
	AddByIDIfAbsent(ctx context.Context, typeName string, startID, endID types.NodeID, props map[string]any) (*types.Relationship, bool, error)
	Get(ctx context.Context, id types.RelID) (*types.Relationship, error)
	GetByIDs(ids []types.RelID) ([]*types.Relationship, error)
	Update(ctx context.Context, id types.RelID, updates map[string]any) (*types.Relationship, error)
	UpdateInPlace(ctx context.Context, id types.RelID, updates map[string]any) (*types.Relationship, error)
	Delete(ctx context.Context, id types.RelID) error
	Import(ctx context.Context, id types.RelID, typeName string, startNode, endNode *types.Node, props map[string]any) (*types.Relationship, error)

	All(opts storepkg.QueryOpts) ([]*types.Relationship, error)
	ForEach(opts storepkg.QueryOpts, fn func(*types.Relationship) bool) error
	ByType(typeName string, opts storepkg.QueryOpts) ([]*types.Relationship, error)
	ForEachByType(typeName string, opts storepkg.QueryOpts, fn func(*types.Relationship) bool) error
	ByTypeAndProperty(typeName, key string, value any, opts storepkg.QueryOpts) ([]*types.Relationship, error)
	ForEachByTypePropertyRange(typeName, propKey string, min, max float64, inclMin, inclMax bool, opts storepkg.QueryOpts, fn func(*types.Relationship) bool) error
	Count() (int, error)
	CountByType(typeName string) (int, error)

	Outgoing(nodeID types.NodeID, typeName string) ([]*types.Relationship, error)
	Incoming(nodeID types.NodeID, typeName string) ([]*types.Relationship, error)
	ForEachOutgoing(nodeID types.NodeID, typeName string, fn func(*types.Relationship) bool) error
	ForEachIncoming(nodeID types.NodeID, typeName string, fn func(*types.Relationship) bool) error
	ForEachAdjacentEndpoint(nodeID types.NodeID, typeName string, incoming bool, fn func(rel types.RelID, other types.NodeID) bool) error
	ForEachAdjacentEndpointAt(nodeID types.NodeID, typeName string, incoming bool, opts storepkg.QueryOpts, fn func(rel types.RelID, other types.NodeID) bool) error
	ForEachAdjacentRelAt(nodeID types.NodeID, typeName string, incoming bool, opts storepkg.QueryOpts, fn func(*types.Relationship) bool) error
	OutgoingForNodes(nodeIDs []types.NodeID, typeName string) (map[types.NodeID][]*types.Relationship, error)
	IncomingForNodes(nodeIDs []types.NodeID, typeName string) (map[types.NodeID][]*types.Relationship, error)
	OutgoingForNodesAtTx(nodeIDs []types.NodeID, typeName string, txAt types.Instant) (map[types.NodeID][]*types.Relationship, error)
	IncomingForNodesAtTx(nodeIDs []types.NodeID, typeName string, txAt types.Instant) (map[types.NodeID][]*types.Relationship, error)
	OutgoingForNodesAtPin(nodeIDs []types.NodeID, typeName string, pin types.Instant) (map[types.NodeID][]*types.Relationship, error)
	IncomingForNodesAtPin(nodeIDs []types.NodeID, typeName string, pin types.Instant) (map[types.NodeID][]*types.Relationship, error)
	OutgoingDegree(nodeID types.NodeID, typeName string) (int, error)
	IncomingDegree(nodeID types.NodeID, typeName string) (int, error)
	RelMutationEpoch() uint64

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

// AddWithTx creates a relationship like Add but stamps the caller-supplied
// txFrom as its transaction time (backfill). Requires the graph to be opened
// with Config.AllowTxBackfill; see RelOps.AddWithTx and §4.1.
func (a *API) AddWithTx(ctx context.Context, typeName string, startNode, endNode *types.Node, props map[string]any, txFrom types.Instant) (*types.Relationship, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.AddWithTx(ctx, typeName, startNode, endNode, props, txFrom)
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

// ForEach streams all relationships matching opts to fn without materializing
// the full result slice when the backend can provide a current-state ID scan.
// fn returning false stops early. Mirror of nodes.API.ForEach for Node/Rel
// parity; see core.RelOps.ForEach for the exact fallback/isolation contract.
func (a *API) ForEach(opts storepkg.QueryOpts, fn func(*types.Relationship) bool) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.ForEach(opts, fn)
}

// Iter returns a Go 1.23+ range-over-func iterator over relationships
// matching opts, built directly on top of ForEach (no new scan machinery) —
// same row set, same order, same temporal/paginated fallback to All that
// ForEach itself takes for the given opts (see ForEach's doc). Differences
// from ForEach:
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
func (a *API) Iter(ctx context.Context, opts storepkg.QueryOpts) iter.Seq2[*types.Relationship, error] {
	return func(yield func(*types.Relationship, error) bool) {
		ops, err := a.ready()
		if err != nil {
			yield(nil, err)
			return
		}
		iterateForEach(ctx, yield, func(fn func(*types.Relationship) bool) error {
			return ops.ForEach(opts, fn)
		})
	}
}

// ByType returns relationships of the given type.
func (a *API) ByType(typeName string, opts storepkg.QueryOpts) ([]*types.Relationship, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.ByType(typeName, opts)
}

// ForEachByType streams relationships of the given type to fn in
// snowflake-ID order; fn returning false stops the scan early. The
// streaming sibling of ByType for scan consumers that must not materialize
// the full result slice. RELAXED isolation: rows are fetched and fn runs
// without graph locks held — concurrent writers are neither blocked nor
// observed atomically. Rows are shared frozen pointers; fn must not mutate
// them.
func (a *API) ForEachByType(typeName string, opts storepkg.QueryOpts, fn func(*types.Relationship) bool) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.ForEachByType(typeName, opts, fn)
}

// ByTypeAndProperty returns relationships matching the rel type and property
// value (K3b) — the relationship mirror of nodes.API.ByLabelAndProperty. Uses
// the store-level rel property index for O(matches) lookup when one exists;
// otherwise falls back to a type-scan + property filter, so it works on every
// backend (including the tiered store, which declines rel-property-index
// CREATION but still answers this query). Temporal opts fold current + history.
func (a *API) ByTypeAndProperty(typeName, key string, value any, opts storepkg.QueryOpts) ([]*types.Relationship, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.ByTypeAndProperty(typeName, key, value, opts)
}

// ForEachByTypePropertyRange streams relationships whose NUMERIC propKey value
// lies within [min, max] (per the inclusivity flags) to fn in snowflake-ID
// order (K3b) — the relationship mirror of
// nodes.API.ForEachByLabelPropertyRange. Candidates come from the rel property
// index's ordered numeric view, which OVER-SELECTS by design, so fn must
// re-check its predicate with exact comparison semantics. Returns
// store.ErrIndexNotFound when no usable rel property index exists for
// (type, propKey) — callers fall back to a type scan.
func (a *API) ForEachByTypePropertyRange(typeName, propKey string, min, max float64, inclMin, inclMax bool, opts storepkg.QueryOpts, fn func(*types.Relationship) bool) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.ForEachByTypePropertyRange(typeName, propKey, min, max, inclMin, inclMax, opts, fn)
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

// ForEachOutgoing streams outgoing relationships from nodeID (optionally
// type-filtered; empty typeName means all types) to fn in snowflake-ID
// order; fn returning false stops the scan early. The streaming sibling of
// Outgoing for hub-degree adjacency consumers — same relaxed isolation and
// frozen-row contract as ForEachByType.
func (a *API) ForEachOutgoing(nodeID types.NodeID, typeName string, fn func(*types.Relationship) bool) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.ForEachOutgoing(nodeID, typeName, fn)
}

// ForEachIncoming is ForEachOutgoing for the incoming direction.
func (a *API) ForEachIncoming(nodeID types.NodeID, typeName string, fn func(*types.Relationship) bool) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.ForEachIncoming(nodeID, typeName, fn)
}

// OutgoingIter returns a Go 1.23+ range-over-func iterator over nodeID's
// outgoing relationships (optionally type-filtered; empty typeName means all
// types), built directly on top of ForEachOutgoing (no new scan machinery) —
// same rows, same order, same relaxed isolation and frozen-row contract. ctx
// is checked once per row; cancellation yields (nil, ctx.Err()) exactly once
// and stops the scan, same as Iter. Breaking out of the range loop stops the
// underlying scan immediately. A nil/zero-value API yields a single
// (nil, grapherr.ErrNilGraph).
func (a *API) OutgoingIter(ctx context.Context, nodeID types.NodeID, typeName string) iter.Seq2[*types.Relationship, error] {
	return func(yield func(*types.Relationship, error) bool) {
		ops, err := a.ready()
		if err != nil {
			yield(nil, err)
			return
		}
		iterateForEach(ctx, yield, func(fn func(*types.Relationship) bool) error {
			return ops.ForEachOutgoing(nodeID, typeName, fn)
		})
	}
}

// IncomingIter is OutgoingIter for the incoming direction, built on
// ForEachIncoming.
func (a *API) IncomingIter(ctx context.Context, nodeID types.NodeID, typeName string) iter.Seq2[*types.Relationship, error] {
	return func(yield func(*types.Relationship, error) bool) {
		ops, err := a.ready()
		if err != nil {
			yield(nil, err)
			return
		}
		iterateForEach(ctx, yield, func(fn func(*types.Relationship) bool) error {
			return ops.ForEachIncoming(nodeID, typeName, fn)
		})
	}
}

// ForEachAdjacentEndpoint streams (relID, otherEndpoint) for nodeID's adjacency
// in the given direction WITHOUT decoding relationship rows — for traversals
// that need only the neighbour, not the relationship's properties. fn returning
// false stops the scan. See core.RelOps.ForEachAdjacentEndpoint.
func (a *API) ForEachAdjacentEndpoint(nodeID types.NodeID, typeName string, incoming bool, fn func(rel types.RelID, other types.NodeID) bool) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.ForEachAdjacentEndpoint(nodeID, typeName, incoming, fn)
}

// ForEachAdjacentEndpointAt streams (relID, otherEndpoint) for nodeID's
// adjacency in the given direction, yielding only edges valid under the opts
// temporal filter and rejecting expired edges from inline valid-time stamps
// WITHOUT decoding relationship rows (OPT15). fn returning false stops the scan.
// See core.RelOps.ForEachAdjacentEndpointAt.
func (a *API) ForEachAdjacentEndpointAt(nodeID types.NodeID, typeName string, incoming bool, opts storepkg.QueryOpts, fn func(rel types.RelID, other types.NodeID) bool) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.ForEachAdjacentEndpointAt(nodeID, typeName, incoming, opts, fn)
}

// ForEachAdjacentRelAt streams the DECODED relationships for nodeID's adjacency
// in the given direction, yielding only edges valid under the opts temporal
// filter and skipping the decode of inline-stamp-rejected edges (OPT15). fn
// returning false stops the scan. See core.RelOps.ForEachAdjacentRelAt.
func (a *API) ForEachAdjacentRelAt(nodeID types.NodeID, typeName string, incoming bool, opts storepkg.QueryOpts, fn func(*types.Relationship) bool) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.ForEachAdjacentRelAt(nodeID, typeName, incoming, opts, fn)
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

// RelMutationEpoch returns the store's relationship-mutation epoch (0 if the backend
// lacks the capability) — the X5 expand-aggregation column path's Gate-2 adjacency
// staleness check. See core.RelOps.RelMutationEpoch.
func (a *API) RelMutationEpoch() uint64 {
	ops, err := a.ready()
	if err != nil {
		return 0
	}
	return ops.RelMutationEpoch()
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

// OutgoingForNodesAtTx is the bitemporal (transaction-time-pinned) counterpart
// of OutgoingForNodes: it resolves each candidate relationship's version at
// txAt through the adjacency index instead of paying a full history-aware
// ByType scan.
//
// This door agrees with the TxAt-pinned BITEMPORAL door (QueryOpts{TxAt: txAt})
// filtered by endpoint — which valid-filters at wall-now when no valid-time
// opts are set — NOT with a belief-state pin: an edge whose valid interval lies
// wholly in the past (a CloseVersion-ed edge, or a width-1 [t, t+1) point-event
// edge) is SILENTLY DROPPED even though it was believed at txAt. For pure
// knowledge-time (AS-OF-SYSTEM-TIME) semantics use OutgoingForNodesAtPin /
// IncomingForNodesAtPin instead. Rel endpoints are immutable, so a relationship
// deleted after txAt is still visible (delete is a transaction-time tombstone),
// one created after txAt is invisible, and a backfilled relationship (AddWithTx)
// is visible from its backfilled TxFrom onward. txAt == 0 delegates to
// OutgoingForNodes verbatim (no TX filter, no caller churn).
// See core.RelOps.OutgoingForNodesAtTx.
func (a *API) OutgoingForNodesAtTx(nodeIDs []types.NodeID, typeName string, txAt types.Instant) (map[types.NodeID][]*types.Relationship, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.OutgoingForNodesAtTx(nodeIDs, typeName, txAt)
}

// IncomingForNodesAtTx is the bitemporal counterpart of IncomingForNodes. See
// OutgoingForNodesAtTx for the resolution semantics.
func (a *API) IncomingForNodesAtTx(nodeIDs []types.NodeID, typeName string, txAt types.Instant) (map[types.NodeID][]*types.Relationship, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.IncomingForNodesAtTx(nodeIDs, typeName, txAt)
}

// OutgoingForNodesAtPin is the pure knowledge-time (belief-state) counterpart of
// OutgoingForNodes: it returns every outgoing relationship BELIEVED at the
// transaction-time pin, with NO valid-time filtering — the door to use for
// AS-OF-SYSTEM-TIME semantics. It agrees with ByType(QueryOpts{TxPin: pin})
// filtered by endpoint BY CONSTRUCTION (both funnel through the same as-of
// resolver). Unlike OutgoingForNodesAtTx it returns past-valid facts, width-1
// point events, and unset-valid_from (snowflake-fallback) edges believed at the
// pin. An edge hard-deleted after the pin is still visible; one created after
// the pin is invisible; a backfilled edge (AddWithTx) is visible from its
// backfilled TxFrom onward.
//
// SEED TOLERANCE — a seed hard-deleted after the pin is ACCEPTED (its pre-delete
// edges are returned via the deleted-rel fold), and a seed absent from the
// belief state at the pin is skipped silently (no ErrNodeNotFound, matching
// ByType{TxPin} filtered by endpoint). pin == 0 delegates to OutgoingForNodes
// verbatim. See core.RelOps.OutgoingForNodesAtPin.
func (a *API) OutgoingForNodesAtPin(nodeIDs []types.NodeID, typeName string, pin types.Instant) (map[types.NodeID][]*types.Relationship, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.OutgoingForNodesAtPin(nodeIDs, typeName, pin)
}

// IncomingForNodesAtPin is the belief-state counterpart of IncomingForNodes. See
// OutgoingForNodesAtPin for the resolution semantics and seed-tolerance contract.
func (a *API) IncomingForNodesAtPin(nodeIDs []types.NodeID, typeName string, pin types.Instant) (map[types.NodeID][]*types.Relationship, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.IncomingForNodesAtPin(nodeIDs, typeName, pin)
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

// iterateForEach drives a Go 1.23+ range-over-func Seq2 from an
// already-validated ForEach-shaped streaming primitive (scan): ctx is checked
// once per row (non-blocking, before each yield) and the scan stops
// immediately either on ctx cancellation — yielding (zero, ctx.Err()) exactly
// once — or when the consumer's yield returns false (a normal early stop,
// nothing further is yielded). Any error the scan itself returns (and did not
// already surface via a per-row ctx check) is yielded once at the end. Shared
// by Iter / OutgoingIter / IncomingIter; kept unexported and duplicated
// (rather than shared via a new package) to stay inside this WP's scope.
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
