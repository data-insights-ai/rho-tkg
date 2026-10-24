// API accessors for the Graph temporal-query surface (point-in-time, interval,
// bitemporal, snapshot/diff, Allen). Lives in the same package as the temporal
// types it returns (GraphSnapshot, SnapshotDiff, …).
package temporal

import (
	"context"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/grapherr"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Ops is the subset of *core.TempOps the temporal sub-API forwards to.
type Ops interface {
	// Point-in-time
	NodeAt(id types.NodeID, t types.Instant) (*types.Node, error)
	NodesAt(t types.Instant) ([]*types.Node, error)
	NodesByLabelAt(label string, t types.Instant) ([]*types.Node, error)
	RelAt(id types.RelID, t types.Instant) (*types.Relationship, error)
	RelsAt(t types.Instant) ([]*types.Relationship, error)
	RelsByTypeAt(relType string, t types.Instant) ([]*types.Relationship, error)
	NeighborsAt(nodeID types.NodeID, t types.Instant) ([]*types.Node, error)
	OutgoingRelsAt(nodeID types.NodeID, t types.Instant) ([]*types.Relationship, error)
	IncomingRelsAt(nodeID types.NodeID, t types.Instant) ([]*types.Relationship, error)
	NodesByLabelPropertyAt(label, key string, value any, t types.Instant) ([]*types.Node, error)
	RelsByTypePropertyAt(relType, key string, value any, t types.Instant) ([]*types.Relationship, error)

	// Interval
	NodesDuring(start, end types.Instant) ([]*types.Node, error)
	RelsDuring(start, end types.Instant) ([]*types.Relationship, error)
	NodesByLabelPropertyDuring(label, key string, value any, start, end types.Instant) ([]*types.Node, error)
	RelsByTypePropertyDuring(relType, key string, value any, start, end types.Instant) ([]*types.Relationship, error)
	NodesRelating(from, to types.Instant, rels types.AllenRelationSet) ([]*types.Node, error)
	RelsRelating(from, to types.Instant, rels types.AllenRelationSet) ([]*types.Relationship, error)

	// Bitemporal (transaction time)
	NowTx() (types.Instant, error)
	CommittedTx() (types.Instant, error)
	PeekTx() (types.Instant, error)
	AdvanceClock(to types.Instant) (types.Instant, error)
	NodeAsOf(id types.NodeID, txTime types.Instant) (*types.Node, error)
	RelAsOf(id types.RelID, txTime types.Instant) (*types.Relationship, error)
	NodesAsOf(txTime types.Instant) ([]*types.Node, error)
	RelsAsOf(txTime types.Instant) ([]*types.Relationship, error)

	// Bitemporal point-in-time (valid time AT a given transaction time).
	// txAt == 0 means "no TX filter" — semantically equivalent to NodeAt /
	// NodesAt / RelAt / RelsAt respectively.
	NodeAtTx(id types.NodeID, validAt, txAt types.Instant) (*types.Node, error)
	RelAtTx(id types.RelID, validAt, txAt types.Instant) (*types.Relationship, error)
	NodesAtTxByIDs(ids []types.NodeID, validAt, txAt types.Instant) ([]*types.Node, error)
	RelsAtTxByIDs(ids []types.RelID, validAt, txAt types.Instant) ([]*types.Relationship, error)
	NodeAtOpts(id types.NodeID, opts storepkg.QueryOpts) (*types.Node, error)
	RelAtOpts(id types.RelID, opts storepkg.QueryOpts) (*types.Relationship, error)
	NodesAtOptsByIDs(ids []types.NodeID, opts storepkg.QueryOpts) ([]*types.Node, error)
	RelsAtOptsByIDs(ids []types.RelID, opts storepkg.QueryOpts) ([]*types.Relationship, error)
	NodesAtTx(validAt, txAt types.Instant) ([]*types.Node, error)
	RelsAtTx(validAt, txAt types.Instant) ([]*types.Relationship, error)
	NodesDuringTx(from, to, txAt types.Instant) ([]*types.Node, error)
	RelsDuringTx(from, to, txAt types.Instant) ([]*types.Relationship, error)

	// Effective timeline: NodeAtTx / RelAtTx for every valid instant at once.
	NodeEffectiveTimeline(id types.NodeID, pin types.Instant) ([]NodeSegment, error)
	RelEffectiveTimeline(id types.RelID, pin types.Instant) ([]RelSegment, error)
	NodeEffectiveTimelines(id types.NodeID, pins []types.Instant) ([][]NodeSegment, error)
	RelEffectiveTimelines(id types.RelID, pins []types.Instant) ([][]RelSegment, error)
	// The lowest pin the point reads answer (row 104).
	NodeLowestAnsweredPin(id types.NodeID) (types.Instant, error)
	NodesLowestAnsweredPins(ids []types.NodeID) ([]types.Instant, error)
	RelLowestAnsweredPin(id types.RelID) (types.Instant, error)
	RelsLowestAnsweredPins(ids []types.RelID) ([]types.Instant, error)
	ForEachNodeEffectiveByLabel(label string, pin types.Instant, fn func(NodeSegment) bool) error
	ForEachRelEffectiveByType(typeName string, pin types.Instant, fn func(RelSegment) bool) error
	ForEachNodeEffective(pin types.Instant, fn func(NodeSegment) bool) error
	ForEachRelEffective(pin types.Instant, fn func(RelSegment) bool) error
	NodesEffectiveByIDs(ids []types.NodeID, pin types.Instant, fn func(NodeSegment) bool) error
	RelsEffectiveByIDs(ids []types.RelID, pin types.Instant, fn func(RelSegment) bool) error
	ForEachRelEffectiveAtNodes(nodeIDs []types.NodeID, dir Direction, relTypes []string, pin types.Instant, fn func(RelSegment) bool) error
	ForEachNodeEffectiveByLabels(labels []string, pin types.Instant, fn func(NodeSegment) bool) error
	ForEachNodeEffectiveByLabelAndProperties(ctx context.Context, label string, props map[string]any, pin types.Instant, opts EffectivePageOpts, fn func(types.NodeID, []NodeSegment) bool) (types.NodeID, error)
	ForEachRelEffectiveByTypeFrom(typeName string, pin, validFrom types.Instant, fn func(RelSegment) bool) error
	ForEachNodeEffectiveByLabelsFrom(labels []string, pin, validFrom types.Instant, fn func(NodeSegment) bool) error
	ForEachNodeEffectiveByLabelsWindow(labels []string, pin types.Instant, window Window, fn func(NodeSegment) bool) error
	ForEachRelEffectiveByTypeWindow(typeName string, pin types.Instant, window Window, fn func(RelSegment) bool) error
	ForEachRelEffectiveAtNodesWindow(nodeIDs []types.NodeID, dir Direction, relTypes []string, pin types.Instant, window Window, fn func(RelSegment) bool) error
	ScanRelEffectiveColumnsAtNodes(nodeIDs []types.NodeID, dir Direction, relTypes []string, pin types.Instant, window Window, props []string, fn func(*storepkg.RelColumnBatch) bool) error
	RelsEffectivePage(ctx context.Context, pin Pin, f Filter, after types.RelID, limit int) ([]RelSegment, types.RelID, error)
	RelEffectiveTimelinesByProperties(ctx context.Context, typeName string, keys []string, tuples [][]any, pin Pin) ([][]RelSegment, error)
	NodesEffectivePage(ctx context.Context, pin Pin, f Filter, after types.NodeID, limit int) ([]NodeSegment, types.NodeID, error)

	// Cascade / timeline edit: an append-only valid-time correction. props is
	// a patch (nil deletes a key) applied to the state valid in each piece of
	// [validFrom, validTo) as believed before the call; the newer belief wins
	// on overlap at read time. See SetNodeVersionInterval on API.
	SetNodeVersionInterval(ctx context.Context, id types.NodeID, validFrom, validTo types.Instant, props map[string]any) (*types.Node, error)
	SetRelVersionInterval(ctx context.Context, id types.RelID, validFrom, validTo types.Instant, props map[string]any) (*types.Relationship, error)
	// The same correction recorded at a caller transaction instant txFrom
	// (Config.AllowTxBackfill). See SetNodeVersionIntervalWithTx on API.
	SetNodeVersionIntervalWithTx(ctx context.Context, id types.NodeID, validFrom, validTo types.Instant, props map[string]any, txFrom types.Instant) (*types.Node, error)
	SetRelVersionIntervalWithTx(ctx context.Context, id types.RelID, validFrom, validTo types.Instant, props map[string]any, txFrom types.Instant) (*types.Relationship, error)
	// Valid-time excise: the entity is absent over [a, b). See
	// ExciseNodeValidity on API.
	ExciseNodeValidity(ctx context.Context, id types.NodeID, a, b types.Instant) error
	ExciseRelValidity(ctx context.Context, id types.RelID, a, b types.Instant) error
	ExciseNodeValidityWithTx(ctx context.Context, id types.NodeID, a, b, txFrom types.Instant) error
	ExciseRelValidityWithTx(ctx context.Context, id types.RelID, a, b, txFrom types.Instant) error

	// Snapshot/Diff
	Snapshot(t types.Instant) (*GraphSnapshot, error)
	Diff(t1, t2 types.Instant) (*SnapshotDiff, error)
	DiffCallback(t1, t2 types.Instant, h DiffHandlers) error

	// Allen's interval algebra
	NodeInterval(n *types.Node) (start, end types.Instant, err error)
	RelInterval(r *types.Relationship) (start, end types.Instant, err error)
	RelateNodes(a, b *types.Node) (types.AllenRelation, error)
	RelateRels(a, b *types.Relationship) (types.AllenRelation, error)

	// Canonical valid-time predicate for a materialized node/relationship.
	NodeMatchesValidTime(n *types.Node, opts storepkg.QueryOpts) bool
	RelMatchesValidTime(r *types.Relationship, opts storepkg.QueryOpts) bool

	// Named knowledge-time (Erkenntniszeit) marks. The instant is a
	// transaction time used with AS OF SYSTEM TIME / TxAt.
	TagAsOf(name string, at types.Instant) error
	ResolveAsOf(name string) (types.Instant, bool, error)
	AsOfTags() (map[string]types.Instant, error)
	RemoveAsOfTag(name string) error
}

// API is the temporal sub-API accessor.
type API struct {
	ops Ops
	ok  bool
}

// New constructs a temporal sub-API.
func New(ops Ops) *API { return &API{ops: ops, ok: !grapherr.IsNil(ops)} }

func (a *API) ready() (Ops, error) {
	if a == nil || !a.ok {
		return nil, grapherr.ErrNilGraph
	}
	return a.ops, nil
}

// NodeAt returns the version of the node valid at t.
func (a *API) NodeAt(id types.NodeID, t types.Instant) (*types.Node, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.NodeAt(id, t)
}

// NodesAt returns the set of nodes valid at t.
func (a *API) NodesAt(t types.Instant) ([]*types.Node, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.NodesAt(t)
}

// NodesByLabelAt returns nodes labeled at t.
func (a *API) NodesByLabelAt(label string, t types.Instant) ([]*types.Node, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.NodesByLabelAt(label, t)
}

// RelAt returns the version of the relationship valid at t.
func (a *API) RelAt(id types.RelID, t types.Instant) (*types.Relationship, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.RelAt(id, t)
}

// RelsAt returns relationships valid at t.
//
// DECLARED view: the relationship's own asserted validity only, NOT masked
// by endpoint validity; use Snapshot/NeighborsAt/OutgoingRelsAt for the
// effective graph view.
func (a *API) RelsAt(t types.Instant) ([]*types.Relationship, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.RelsAt(t)
}

// RelsByTypeAt returns relationships of relType valid at t.
//
// DECLARED view: the relationship's own asserted validity only, NOT masked
// by endpoint validity; use Snapshot/NeighborsAt/OutgoingRelsAt for the
// effective graph view.
func (a *API) RelsByTypeAt(relType string, t types.Instant) ([]*types.Relationship, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.RelsByTypeAt(relType, t)
}

// NeighborsAt returns the neighbours of nodeID valid at t. EFFECTIVE view: a
// neighbour is reached only via a relationship valid at t whose both endpoints
// are valid at t (same rule as Snapshot, Diff, OutgoingRelsAt). History-aware:
// returns neighbours connected via rels (current or deleted) that were valid
// at t, and earlier lives of rels re-imported with other endpoints. The fold
// reads the node's ex-adjacency (O(its ever-degree), not the total history)
// when the store implements ExAdjacencyCapability — every in-tree backend
// does. Same characteristic applies to OutgoingRelsAt and IncomingRelsAt.
func (a *API) NeighborsAt(nodeID types.NodeID, t types.Instant) ([]*types.Node, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.NeighborsAt(nodeID, t)
}

// OutgoingRelsAt returns relationships where nodeID is the start endpoint and
// the relationship was valid at t. EFFECTIVE view: a relationship is returned
// only if its version is valid at t AND both endpoints are valid at t — the same
// rule as Snapshot, Diff and NeighborsAt, so they agree on every edge. The
// declared row doors (RelsAt, RelsDuring, Rels().ByType/ForEachAdjacentRelAt
// with QueryOpts) are not endpoint-masked. History-aware: includes deleted rels
// that were valid at t and returns the version-at-t of each. Sorted by rel ID.
// Returns ErrNoVersionValidAt / ErrNodeNotFound if nodeID was not valid at t.
func (a *API) OutgoingRelsAt(nodeID types.NodeID, t types.Instant) ([]*types.Relationship, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.OutgoingRelsAt(nodeID, t)
}

// IncomingRelsAt returns relationships where nodeID is the end endpoint and
// the relationship was valid at t. EFFECTIVE view (start endpoint must also be
// valid at t). Mirror of OutgoingRelsAt.
func (a *API) IncomingRelsAt(nodeID types.NodeID, t types.Instant) ([]*types.Relationship, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.IncomingRelsAt(nodeID, t)
}

// NodesByLabelPropertyAt returns nodes labeled with the given (label,key,value) at t.
func (a *API) NodesByLabelPropertyAt(label, key string, value any, t types.Instant) ([]*types.Node, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.NodesByLabelPropertyAt(label, key, value, t)
}

// RelsByTypePropertyAt returns relationships of relType with property at t.
//
// DECLARED view: the relationship's own asserted validity only, NOT masked
// by endpoint validity; use Snapshot/NeighborsAt/OutgoingRelsAt for the
// effective graph view.
func (a *API) RelsByTypePropertyAt(relType, key string, value any, t types.Instant) ([]*types.Relationship, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.RelsByTypePropertyAt(relType, key, value, t)
}

// NodesDuring returns nodes whose validity overlaps [start,end).
func (a *API) NodesDuring(start, end types.Instant) ([]*types.Node, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.NodesDuring(start, end)
}

// RelsDuring returns relationships whose validity overlaps [start,end).
//
// DECLARED view: the relationship's own asserted validity only, NOT masked
// by endpoint validity; use Snapshot/NeighborsAt/OutgoingRelsAt for the
// effective graph view.
func (a *API) RelsDuring(start, end types.Instant) ([]*types.Relationship, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.RelsDuring(start, end)
}

// NodesRelating returns nodes having a version whose valid-interval has an Allen
// relation to the query interval [from,to) that is a member of rels. History-aware
// and predicate-anywhere; to == 0 is an open (+∞) query interval; an empty rels
// set yields an empty result. See core.TempOps.NodesRelating.
func (a *API) NodesRelating(from, to types.Instant, rels types.AllenRelationSet) ([]*types.Node, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.NodesRelating(from, to, rels)
}

// RelsRelating is the relationship mirror of NodesRelating.
//
// DECLARED view: the relationship's own asserted validity only, NOT masked
// by endpoint validity; use Snapshot/NeighborsAt/OutgoingRelsAt for the
// effective graph view.
func (a *API) RelsRelating(from, to types.Instant, rels types.AllenRelationSet) ([]*types.Relationship, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.RelsRelating(from, to, rels)
}

// NodesByLabelPropertyDuring returns nodes labeled with property during [start,end).
func (a *API) NodesByLabelPropertyDuring(label, key string, value any, start, end types.Instant) ([]*types.Node, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.NodesByLabelPropertyDuring(label, key, value, start, end)
}

// RelsByTypePropertyDuring returns relationships of relType with property during [start,end).
//
// DECLARED view: the relationship's own asserted validity only, NOT masked
// by endpoint validity; use Snapshot/NeighborsAt/OutgoingRelsAt for the
// effective graph view.
func (a *API) RelsByTypePropertyDuring(relType, key string, value any, start, end types.Instant) ([]*types.Relationship, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.RelsByTypePropertyDuring(relType, key, value, start, end)
}

// NodeAsOf returns the newest row of node id recorded by transaction time
// txTime (absent once it was superseded or deleted by then). After a bounded
// SetNodeVersionInterval that left the current row in place this is the
// correction row, not Get's row; for the node's state at valid instant t as
// believed at a pin use NodeAtTx(id, t, pin) or ByLabel with ValidAt+TxAt.
func (a *API) NodeAsOf(id types.NodeID, txTime types.Instant) (*types.Node, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.NodeAsOf(id, txTime)
}

// RelAsOf returns the newest row of relationship id recorded by transaction
// time txTime — NodeAsOf's rule; for the state at valid instant t as believed
// at a pin use RelAtTx(id, t, pin) or ByType with ValidAt+TxAt.
func (a *API) RelAsOf(id types.RelID, txTime types.Instant) (*types.Relationship, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.RelAsOf(id, txTime)
}

// NowTx returns the current transaction-time instant — the pin to pass to the
// AS-OF reads (QueryOpts.TxAt / NodeAtTx / NodesAsOf) or to name via TagAsOf, to
// snapshot everything committed so far. Every committed entity has TxFrom <=
// NowTx(); every later mutation is stamped strictly greater. Reading it advances
// the commit clock by one tick (reserving the instant), which is what makes the
// separation strict and the value correct across a Close/reopen, and after a
// crash above every row the store kept (store.CommitFloorCapability) — a bare
// wall-clock pin is unsafe because a burst of mutations can outrun the wall.
//
//	pin, _ := g.Temporal().NowTx()
//	_ = g.Temporal().TagAsOf("snapshot", pin) // address it by name later
func (a *API) NowTx() (types.Instant, error) {
	ops, err := a.ready()
	if err != nil {
		return 0, err
	}
	return ops.NowTx()
}

// CommittedTx returns a transaction-time pin at which every write is
// committed: the open transaction's StartInstant while one is open, else a
// fresh NowTx. A read pinned there (QueryOpts.TxPin, or TxAt with a valid
// time) sees no uncommitted transaction write and is repeatable while
// transactions commit, roll back or start. Not covered: a privileged backfill
// and a standalone (non-transaction) mutation in flight when the pin is taken.
// See core.TempOps.CommittedTx.
func (a *API) CommittedTx() (types.Instant, error) {
	ops, err := a.ready()
	if err != nil {
		return 0, err
	}
	return ops.CommittedTx()
}

// PeekTx returns the current transaction-clock value WITHOUT reserving an instant —
// the non-burning, observability-only sibling of NowTx for metrics/polling loops
// (which would inflate the commit clock if they called NowTx). It is NOT a sound
// as-of pin: the value can coincide with a concurrent write's stamp, so pinning a
// read at PeekTx() includes/excludes that write nondeterministically. For a sound
// pin use NowTx(), a value returned BY a write, or a named TagAsOf.
func (a *API) PeekTx() (types.Instant, error) {
	ops, err := a.ready()
	if err != nil {
		return 0, err
	}
	return ops.PeekTx()
}

// AdvanceClock raises the transaction-clock floor to at least `to`, returning the
// resulting floor (a no-op returning the current floor if `to` is not ahead).
// The clock never moves backward. It is the Hybrid-Logical-Clock merge seam for a
// distributed deployment: a coordinator that observes a peer machine's
// transaction timestamp calls this before stamping local writes, so subsequent
// local TxFrom values are >= every peer timestamp seen — a causal cross-machine
// order without a central sequencer. rho-tkg owns only the monotonic bump.
func (a *API) AdvanceClock(to types.Instant) (types.Instant, error) {
	ops, err := a.ready()
	if err != nil {
		return 0, err
	}
	return ops.AdvanceClock(to)
}

// NodesAsOf returns nodes known at transaction time txTime.
func (a *API) NodesAsOf(txTime types.Instant) ([]*types.Node, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.NodesAsOf(txTime)
}

// RelsAsOf returns relationships known at transaction time txTime.
//
// DECLARED view: the relationship's own asserted validity only, NOT masked
// by endpoint validity; use Snapshot/NeighborsAt/OutgoingRelsAt for the
// effective graph view.
func (a *API) RelsAsOf(txTime types.Instant) ([]*types.Relationship, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.RelsAsOf(txTime)
}

// NodeAtOpts is NodeAtTx with explicit presence (round 11): opts.ValidAt is
// the valid instant (absent: now) and opts.TxAt the transaction-time pin, read
// as every generic door reads them (store.QueryOpts.HasValidAt / HasTxAt): a
// present pin at 0 is the instant 0, before every recorded row (so nothing is
// valid: ErrNoVersionValidAt), an absent pin is none. For one entity it
// answers what Nodes().ByLabel / All with the same opts answer. Only the point
// form: a TxPin (use NodeAsOf), a window, a page, a depth or IncludeEclipsed
// is ErrConflictingTemporalOpts.
func (a *API) NodeAtOpts(id types.NodeID, opts storepkg.QueryOpts) (*types.Node, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.NodeAtOpts(id, opts)
}

// RelAtOpts is NodeAtOpts for relationships (RelAtTx's gates; DECLARED view,
// not masked by endpoint validity).
func (a *API) RelAtOpts(id types.RelID, opts storepkg.QueryOpts) (*types.Relationship, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.RelAtOpts(id, opts)
}

// NodesAtOptsByIDs is NodeAtOpts for a set of IDs, laid out as NodesAtTxByIDs
// (one answer per ID, aligned, nil where no version is valid); a pin above the
// commit clock is ErrTxPinTooNew.
func (a *API) NodesAtOptsByIDs(ids []types.NodeID, opts storepkg.QueryOpts) ([]*types.Node, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.NodesAtOptsByIDs(ids, opts)
}

// RelsAtOptsByIDs is NodesAtOptsByIDs for relationships.
func (a *API) RelsAtOptsByIDs(ids []types.RelID, opts storepkg.QueryOpts) ([]*types.Relationship, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.RelsAtOptsByIDs(ids, opts)
}

// NodeAtTx returns the node version valid at validAt as known at txAt
// (bitemporal point query). txAt == 0 is equivalent to NodeAt(id, validAt) (no
// pin); a pin at the instant 0 is NodeAtOpts with TxAtSet.
func (a *API) NodeAtTx(id types.NodeID, validAt, txAt types.Instant) (*types.Node, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.NodeAtTx(id, validAt, txAt)
}

// RelAtTx is the relationship counterpart of NodeAtTx.
func (a *API) RelAtTx(id types.RelID, validAt, txAt types.Instant) (*types.Relationship, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.RelAtTx(id, validAt, txAt)
}

// NodesAtTxByIDs is NodeAtTx for many IDs at one (validAt, txAt): the result
// is aligned to ids (same length and order; a repeated id is answered at each
// position and the rows may alias), an element is nil where NodeAtTx returns
// ErrNodeNotFound or ErrNoVersionValidAt, every other element is the row
// NodeAtTx returns. Rows at repeated positions are the same pointer;
// DeepCopy before mutating. Each distinct ID runs NodeAtTx's gates and body
// with its current row read per ID; the graph read lock is held once per 256
// distinct IDs, through one store read snapshot per call where the store
// offers one (badger, sharded). Batch, ingest and admin writers can land
// between two such chunks, so each ID is read at one moment but the call as a
// whole is not one snapshot: pin txAt at or below a value a write or NowTx
// handed out for a repeatable answer. Only IDs whose current row does not
// answer read their history. Empty ids returns an empty slice.
//
// Errors (the call fails, no rows): ErrNilGraph, ErrGraphClosed,
// ErrInvalidStoreMutation (an invalid id, before anything is read),
// ErrInvalidTimeRange (txAt == 0: a pin is required, unlike NodeAtTx's
// txAt == 0; a pin at the instant 0 is NodesAtOptsByIDs; a negative pin is an
// ordinary pin before every recorded row, all nil, round 11), ErrTxPinTooNew
// (txAt above the commit clock), and a member's
// ErrHistoryCompacted / ErrRetentionExpired (NodeAtTx's per-ID gates).
func (a *API) NodesAtTxByIDs(ids []types.NodeID, validAt, txAt types.Instant) ([]*types.Node, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.NodesAtTxByIDs(ids, validAt, txAt)
}

// RelsAtTxByIDs is NodesAtTxByIDs for relationships: each element is the row
// RelAtTx returns, nil where it returns ErrRelNotFound or
// ErrNoVersionValidAt; RelAtTx's gates. DECLARED view: not masked by
// endpoint validity.
func (a *API) RelsAtTxByIDs(ids []types.RelID, validAt, txAt types.Instant) ([]*types.Relationship, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.RelsAtTxByIDs(ids, validAt, txAt)
}

// NodeEffectiveTimeline returns node id's state over valid time as recorded
// at transaction-time pin: NodeSegments ascending by ValidFrom, half-open,
// non-overlapping, gaps omitted, adjacent segments holding different rows.
// For every valid instant t the segment containing t holds the row
// NodeAtTx(id, t, pin) returns; none contains t where NodeAtTx answers
// ErrNoVersionValidAt. ValidFrom is the effective start, never 0 (the ID's
// TxFrom for a row without a recorded valid-from, backlog 62); a deleted
// node's last
// segment ends at the delete instant; a node created after the pin returns
// nil, nil. Segments may share a row pointer; DeepCopy before mutating.
//
// NodeAsOf is the record door (the newest row recorded by a pin); this is the
// state door's interval form. A pin at or below 0 is an ordinary pin before
// every recorded row (every TxFrom is positive): nil, nil, as for a node
// created after the pin (round 11; it was ErrInvalidTimeRange). Errors:
// ErrNilGraph, ErrGraphClosed, ErrTxPinTooNew (pin above the commit clock),
// ErrHistoryCompacted, ErrRetentionExpired, ErrNodeNotFound,
// ErrInvalidStoreMutation (invalid ID).
func (a *API) NodeEffectiveTimeline(id types.NodeID, pin types.Instant) ([]NodeSegment, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.NodeEffectiveTimeline(id, pin)
}

// RelEffectiveTimeline is NodeEffectiveTimeline for relationships (the row
// RelAtTx(id, t, pin) returns; ErrRelNotFound for an unknown ID). DECLARED
// view: not masked by endpoint validity.
func (a *API) RelEffectiveTimeline(id types.RelID, pin types.Instant) ([]RelSegment, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.RelEffectiveTimeline(id, pin)
}

// NodeEffectiveTimelines returns NodeEffectiveTimeline(id, pins[i]) at index
// i for every pin, read from ONE walk of the node's history chain: the
// history rows read are about versions + segments, not pins × versions
// (row 98: 511 versions at 511 pins read 524,288 rows through 511 singular
// calls). The result is aligned with pins: unsorted and repeated pins are
// allowed, and each answer is the singular door's at that pin (nil for a pin
// at or below 0 or before the node's creation). Empty pins: an empty result,
// nothing read. Every pin is checked before a row is read: one pin above the
// commit clock (ErrTxPinTooNew), below compacted knowledge
// (ErrHistoryCompacted) or below a retention watermark (ErrRetentionExpired)
// refuses the whole call with no result. Other errors as
// NodeEffectiveTimeline. The head-pin cache is neither read nor filled.
func (a *API) NodeEffectiveTimelines(id types.NodeID, pins []types.Instant) ([][]NodeSegment, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.NodeEffectiveTimelines(id, pins)
}

// RelEffectiveTimelines is NodeEffectiveTimelines for relationships
// (RelEffectiveTimeline at every pin; ErrRelNotFound for an unknown ID).
func (a *API) RelEffectiveTimelines(id types.RelID, pins []types.Instant) ([][]RelSegment, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.RelEffectiveTimelines(id, pins)
}

// NodeLowestAnsweredPin returns the lowest transaction pin at which the
// node's point reads (NodeEffectiveTimeline(s), NodeAsOf, NodeAtTx) answer
// without ErrHistoryCompacted / ErrRetentionExpired: max(the node's
// compaction boundary, the highest retention watermark of its labels).
// Every positive pin at or above it is answered, every pin below it refused;
// it may lie above the commit clock. 0: every positive pin answers (never
// compacted, no watermark on its labels). An unknown or retention-purged
// node has nothing to consult: 0, and the reads answer ErrNodeNotFound at
// every pin — a purge removes the whole node, also at pins that answered it
// before. Pins at or below 0 are not covered. Errors: ErrGraphClosed,
// ErrInvalidStoreMutation (invalid ID).
func (a *API) NodeLowestAnsweredPin(id types.NodeID) (types.Instant, error) {
	ops, err := a.ready()
	if err != nil {
		return 0, err
	}
	return ops.NodeLowestAnsweredPin(id)
}

// NodesLowestAnsweredPins is NodeLowestAnsweredPin for every id: a slice
// aligned with ids (unsorted and repeated ids allowed) under one graph read
// lock. One invalid id refuses the whole call with no result; empty ids
// answer an empty result.
func (a *API) NodesLowestAnsweredPins(ids []types.NodeID) ([]types.Instant, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.NodesLowestAnsweredPins(ids)
}

// RelLowestAnsweredPin is NodeLowestAnsweredPin for a relationship: max(its
// compaction boundary, its type's retention watermark — unless it was minted
// at or after every relationship watermark — and the node-family retention
// maximum, which every relationship point read checks). An unknown or purged
// relationship minted below the relationship maximum gets that maximum (its
// type is unknown); at and above it the reads answer ErrRelNotFound.
func (a *API) RelLowestAnsweredPin(id types.RelID) (types.Instant, error) {
	ops, err := a.ready()
	if err != nil {
		return 0, err
	}
	return ops.RelLowestAnsweredPin(id)
}

// RelsLowestAnsweredPins is NodesLowestAnsweredPins for relationships.
func (a *API) RelsLowestAnsweredPins(ids []types.RelID) ([]types.Instant, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.RelsLowestAnsweredPins(ids)
}

// ForEachNodeEffectiveByLabel streams the effective timeline at pin of every
// node that carried label in a row recorded by the pin — including nodes
// deleted before the pin, which ByLabel{TxPin} drops — limited to the segments
// whose row carries the label: at every valid instant t the segments
// containing t are exactly ByLabel(label, {ValidAt: t, TxAt: pin}). Segments of
// one node are contiguous and ascending; node order is unspecified; fn
// returning false stops the scan. fn runs without graph locks. Errors: those
// of NodeEffectiveTimeline's pin, ErrNilCallback, an invalid label; the
// compaction / retention watermarks fail the whole scan.
func (a *API) ForEachNodeEffectiveByLabel(label string, pin types.Instant, fn func(NodeSegment) bool) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.ForEachNodeEffectiveByLabel(label, pin, fn)
}

// ForEachRelEffectiveByType is ForEachNodeEffectiveByLabel for relationships
// of a type (segments equal ByType(typeName, {ValidAt: t, TxAt: pin}) at every
// t; each listed relationship streams its whole timeline).
func (a *API) ForEachRelEffectiveByType(typeName string, pin types.Instant, fn func(RelSegment) bool) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.ForEachRelEffectiveByType(typeName, pin, fn)
}

// ForEachNodeEffective streams the effective timeline at pin of every node
// recorded by the pin — live, closed, deleted before or after the pin; none
// created after it: each node's segments are its NodeEffectiveTimeline(id,
// pin), contiguous and ascending; node order is unspecified; fn returning false stops; fn runs
// without graph locks. Errors and gates: ForEachNodeEffectiveByLabel's.
func (a *API) ForEachNodeEffective(pin types.Instant, fn func(NodeSegment) bool) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.ForEachNodeEffective(pin, fn)
}

// ForEachRelEffective is ForEachNodeEffective for relationships (segments
// equal each relationship's RelEffectiveTimeline). DECLARED view: not masked
// by endpoint validity.
func (a *API) ForEachRelEffective(pin types.Instant, fn func(RelSegment) bool) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.ForEachRelEffective(pin, fn)
}

// NodesEffectiveByIDs streams NodeEffectiveTimeline(id, pin) for each id in
// the order of ids, a repeated id once, an unknown id (or one created after
// the pin) skipped; fn returning false stops. The per-entity door batched:
// its per-entity compaction / retention gates fail the call; an invalid id
// (ErrInvalidStoreMutation) is refused before any segment.
func (a *API) NodesEffectiveByIDs(ids []types.NodeID, pin types.Instant, fn func(NodeSegment) bool) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.NodesEffectiveByIDs(ids, pin, fn)
}

// RelsEffectiveByIDs is NodesEffectiveByIDs for relationships.
func (a *API) RelsEffectiveByIDs(ids []types.RelID, pin types.Instant, fn func(RelSegment) bool) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.RelsEffectiveByIDs(ids, pin, fn)
}

// ForEachRelEffectiveAtNodes streams the effective timelines at pin of the
// relationships with a seed at the dir endpoint (Outgoing: start node,
// Incoming: end node), limited to the segments whose row has a seed there and,
// when relTypes is not empty, one of those types. Relationships deleted before
// the pin are included; each relationship streams once even when several
// seeds or both endpoints match; repeated seeds count once; unknown seeds and
// types contribute nothing; fn returning false stops. DECLARED view. Errors:
// ForEachRelEffectiveByType's, ErrInvalidDirection, ErrInvalidStoreMutation
// for an invalid seed.
func (a *API) ForEachRelEffectiveAtNodes(nodeIDs []types.NodeID, dir Direction, relTypes []string, pin types.Instant, fn func(RelSegment) bool) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.ForEachRelEffectiveAtNodes(nodeIDs, dir, relTypes, pin, fn)
}

// ForEachNodeEffectiveByLabels is ForEachNodeEffectiveByLabel for several
// labels: each node that carried one of them streams once, limited to the
// segments whose row carries at least one.
func (a *API) ForEachNodeEffectiveByLabels(labels []string, pin types.Instant, fn func(NodeSegment) bool) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.ForEachNodeEffectiveByLabels(labels, pin, fn)
}

// ForEachRelEffectiveByTypeFrom is ForEachRelEffectiveByType restricted to the
// relationships still effective at or after validFrom: a relationship streams
// (its whole timeline, as ForEachRelEffectiveByType streams it) iff one of its
// segments ends at or after validFrom (ValidTo >= validFrom: the half-open
// segment ending exactly at validFrom counts) or is open — an untimed
// relationship never deleted always streams, a deleted one ends at its delete
// instant, a retracted life never streams. validFrom at or below every end
// streams what ForEachRelEffectiveByType streams. Errors:
// ForEachRelEffectiveByType's.
//
// Cost on memory and badger: the relationships of the type whose valid end is
// at or after validFrom plus those written after pin, from a RAM valid-end
// index the store keeps per queried type. The first call for a type reads its
// ever-members' temporal fields once (and builds the K1 membership sidecar if
// no pinned scan did); after that every write of the type is folded in, and
// writes of other types change nothing. A reopen tracks it again on the next
// call. Tiered keeps the index
// per shard and unions the shards' candidates (a closed cold shard is opened;
// an entity whose rows bound its end loosely, such as a valid-time close, is
// read on every call). Sharded answers the same through the unbounded door's
// candidates (not accelerated).
func (a *API) ForEachRelEffectiveByTypeFrom(typeName string, pin, validFrom types.Instant, fn func(RelSegment) bool) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.ForEachRelEffectiveByTypeFrom(typeName, pin, validFrom, fn)
}

// ForEachNodeEffectiveByLabelsFrom is ForEachNodeEffectiveByLabels restricted
// to the nodes whose segments carrying one of the labels include one ending at
// or after validFrom, or an open one (ForEachRelEffectiveByTypeFrom's rule and
// cost; a node is bounded by its whole timeline, so one that dropped the
// labels but lives on is read and skipped).
func (a *API) ForEachNodeEffectiveByLabelsFrom(labels []string, pin, validFrom types.Instant, fn func(NodeSegment) bool) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.ForEachNodeEffectiveByLabelsFrom(labels, pin, validFrom, fn)
}

// ForEachNodeEffectiveByLabelsWindow is ForEachNodeEffectiveByLabels limited
// to the segments that meet window (sigma-tkgd round 10 request 2b): for
// every valid instant t inside the window the streamed segments containing t
// are exactly ForEachNodeEffectiveByLabels' (each node's segments carrying
// one of the labels, as recorded at pin); a segment outside the window is
// not streamed, so a node's streamed segments need not be contiguous. The
// window is a list of disjoint half-open intervals, ascending (Window); the
// empty window streams nothing. Candidates come from the valid-end index as
// for ForEachNodeEffectiveByLabelsFrom(labels, pin, window[0].From), so
// entities that end before the window are not read. Gates and errors:
// ForEachNodeEffectiveByLabels', ErrInvalidTimeRange for a window that is
// not ascending and disjoint.
func (a *API) ForEachNodeEffectiveByLabelsWindow(labels []string, pin types.Instant, window Window, fn func(NodeSegment) bool) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.ForEachNodeEffectiveByLabelsWindow(labels, pin, window, fn)
}

// ForEachRelEffectiveByTypeWindow is ForEachRelEffectiveByType limited to
// the segments that meet window, as ForEachNodeEffectiveByLabelsWindow
// (candidates as ForEachRelEffectiveByTypeFrom from window[0].From).
// DECLARED view: not masked by endpoint validity.
func (a *API) ForEachRelEffectiveByTypeWindow(typeName string, pin types.Instant, window Window, fn func(RelSegment) bool) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.ForEachRelEffectiveByTypeWindow(typeName, pin, window, fn)
}

// ForEachRelEffectiveAtNodesWindow is ForEachRelEffectiveAtNodes (a lookup by
// start or end node) limited to the segments that meet window, as
// ForEachNodeEffectiveByLabelsWindow. Gates and errors:
// ForEachRelEffectiveAtNodes' and the window's ErrInvalidTimeRange.
func (a *API) ForEachRelEffectiveAtNodesWindow(nodeIDs []types.NodeID, dir Direction, relTypes []string, pin types.Instant, window Window, fn func(RelSegment) bool) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.ForEachRelEffectiveAtNodesWindow(nodeIDs, dir, relTypes, pin, window, fn)
}

// ScanRelEffectiveColumnsAtNodes is the typed form of
// ForEachRelEffectiveAtNodesWindow (sigma-tkgd round 10 request 1): the same
// segments as store.RelColumnBatch rows (IDs, StartIDs, EndIDs, the
// requested properties typed by the shared column rules, and each segment's
// own valid span in ValidFrom / ValidTo, ValidTo 0 open), so a lookup by
// start or end node at a pin reads typed values without boxing them one by
// one. A relationship appears once per streamed segment. Ordinals are 0 (a
// read coordinate numbers no row). RelType is the type when relTypes names
// one, else empty (the rows mix the named types). A column holding integers
// and floats refuses with ErrMixedNumericColumn, as every column scan does.
// fn must not retain the batch.
func (a *API) ScanRelEffectiveColumnsAtNodes(nodeIDs []types.NodeID, dir Direction, relTypes []string, pin types.Instant, window Window, props []string, fn func(*storepkg.RelColumnBatch) bool) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.ScanRelEffectiveColumnsAtNodes(nodeIDs, dir, relTypes, pin, window, props, fn)
}

// NodesAtTx returns nodes valid at validAt as known at txAt.
// txAt == 0 is equivalent to NodesAt(validAt).
func (a *API) NodesAtTx(validAt, txAt types.Instant) ([]*types.Node, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.NodesAtTx(validAt, txAt)
}

// RelsAtTx is the relationship counterpart of NodesAtTx.
//
// DECLARED view: the relationship's own asserted validity only, NOT masked
// by endpoint validity; use Snapshot/NeighborsAt/OutgoingRelsAt for the
// effective graph view.
func (a *API) RelsAtTx(validAt, txAt types.Instant) ([]*types.Relationship, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.RelsAtTx(validAt, txAt)
}

// NodesDuringTx returns every node version whose valid window overlaps [from, to)
// as known at txAt (transaction time) — the bitemporal-interval sibling of
// NodesDuring / NodesAtTx. txAt == 0 is equivalent to NodesDuring(from, to).
func (a *API) NodesDuringTx(from, to, txAt types.Instant) ([]*types.Node, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.NodesDuringTx(from, to, txAt)
}

// RelsDuringTx is the relationship counterpart of NodesDuringTx.
//
// DECLARED view: the relationship's own asserted validity only, NOT masked
// by endpoint validity; use Snapshot/NeighborsAt/OutgoingRelsAt for the
// effective graph view.
func (a *API) RelsDuringTx(from, to, txAt types.Instant) ([]*types.Relationship, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.RelsDuringTx(from, to, txAt)
}

// SetNodeVersionInterval records a valid-time correction for node id over
// [validFrom, validTo) (validTo open: 0 on the legacy valid axis,
// types.InfiniteValidTo on the literal axis of an UntimedWholeAxis store,
// where 0 is the instant 0; round 11). props is a patch (a nil
// value deletes the key) applied to the state valid at each instant of the
// interval as believed before the call — unnamed properties keep their
// then-valid values, not the current ones. Append-only: reads pinned to an
// earlier transaction time still see the uncorrected belief. Returns the
// appended version covering validFrom.
//
// Unique constraints judge the patch like an update: an open-ended call
// (validTo open) replaces the current row, so a constrained value another
// current node holds is refused (UniqueCurrent and UniqueForever); a bounded
// call leaves the current row's value, so only UniqueForever (a value owned by
// another entity) refuses it. A refusal returns ErrUniqueViolation and appends
// nothing; a float on a constrained key returns ErrUniqueUnsupportedType.
func (a *API) SetNodeVersionInterval(ctx context.Context, id types.NodeID, validFrom, validTo types.Instant, props map[string]any) (*types.Node, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.SetNodeVersionInterval(ctx, id, validFrom, validTo, props)
}

// SetRelVersionInterval is the relationship counterpart of SetNodeVersionInterval.
func (a *API) SetRelVersionInterval(ctx context.Context, id types.RelID, validFrom, validTo types.Instant, props map[string]any) (*types.Relationship, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.SetRelVersionInterval(ctx, id, validFrom, validTo, props)
}

// SetNodeVersionIntervalWithTx records the correction of
// SetNodeVersionInterval at the caller's transaction instant txFrom (a replay
// whose records carry their own knowledge time, e.g. a burst that grows from
// [vs, ve) to [vs, ve')): every row it appends carries TxFrom = UpdatedAt =
// txFrom, so reads pinned before txFrom keep the uncorrected belief and reads
// pinned at or after it see the correction. Requires Config.AllowTxBackfill
// (ErrTxBackfillDisabled); txFrom must be a positive instant not in the future
// (ErrInvalidTxFrom) and after every TxFrom and TxTo
// recorded on the node's chain, also on rows outside [validFrom, validTo)
// (ErrTxOrder, wrapping ErrInvalidTxFrom). validFrom may lie after txFrom. A
// hard-deleted node returns ErrEntityDeleted. The commit clock is not advanced
// by txFrom. A refusal writes nothing.
func (a *API) SetNodeVersionIntervalWithTx(ctx context.Context, id types.NodeID, validFrom, validTo types.Instant, props map[string]any, txFrom types.Instant) (*types.Node, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.SetNodeVersionIntervalWithTx(ctx, id, validFrom, validTo, props, txFrom)
}

// SetRelVersionIntervalWithTx is the relationship counterpart of
// SetNodeVersionIntervalWithTx.
func (a *API) SetRelVersionIntervalWithTx(ctx context.Context, id types.RelID, validFrom, validTo types.Instant, props map[string]any, txFrom types.Instant) (*types.Relationship, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.SetRelVersionIntervalWithTx(ctx, id, validFrom, validTo, props, txFrom)
}

// ExciseNodeValidity ends node id's validity over [a, b) (sigma R7): reads
// with a valid instant in [a, b) answer ErrNoVersionValidAt (NodeAt,
// NodeAtTx, the label and property doors, the effective timeline), interval
// reads no longer see the node there, and reads outside [a, b) keep their
// answers. It splits the version valid over [a, b) — the current one, open or
// closed, or a historical one, also several — and is append-only: the rows it
// appends (void pieces over the parts of [a, b) where a version was valid, and
// a resumption from b) all carry one transaction instant, recorded under the
// node's entity lock, so pins before the call see the unsplit version.
// History() returns the void pieces (Temporal().Void); they never hold the
// current slot and never answer NodeAsOf. A later SetNodeVersionInterval over
// [a, b) fills the hole again.
//
// b == 0 (the valid axis's open end) excises the open tail [a, open) (v4.76.0,
// OPENEXCISE): nothing is valid from a on at later pins, no resumption is
// written and no row is ended in transaction time, so older beliefs do not
// read again; current-state reads (Get, ByLabel without a valid instant) keep
// the current row, as after a bounded excise. Its void piece is a new row
// shape (row format 7, store marker 10: a release before v4.76.0 refuses the
// store).
//
// Refusals, nothing written: a == 0 or, with a bounded b, a >= b
// (ErrInvalidTimeRange; a negative a, types.UnboundedValidFrom
// included, is a real start since sigma round 10); no version valid anywhere in [a, b)
// (ErrNoVersionValidAt); an unknown node (ErrNodeNotFound); a hard-deleted one
// (ErrEntityDeleted). Twins with the same rules: GraphTx.ExciseNodeValidity
// and BatchBuilder.ExciseNodeValidity.
func (a *API) ExciseNodeValidity(ctx context.Context, id types.NodeID, from, to types.Instant) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.ExciseNodeValidity(ctx, id, from, to)
}

// ExciseRelValidity is the relationship counterpart of ExciseNodeValidity.
func (a *API) ExciseRelValidity(ctx context.Context, id types.RelID, from, to types.Instant) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.ExciseRelValidity(ctx, id, from, to)
}

// ExciseNodeValidityWithTx is ExciseNodeValidity recorded at the caller's
// transaction instant txFrom, with the rules of SetNodeVersionIntervalWithTx:
// Config.AllowTxBackfill (ErrTxBackfillDisabled); txFrom a positive instant
// not in the future (ErrInvalidTxFrom) and after every TxFrom and TxTo of the
// node's chain (ErrTxOrder). The commit clock is not advanced. Twins:
// GraphTx.ExciseNodeValidityWithTx and BatchBuilder.ExciseNodeValidityWithTx
// (whole-unit pre-flight: one refusal refuses the batch).
func (a *API) ExciseNodeValidityWithTx(ctx context.Context, id types.NodeID, from, to, txFrom types.Instant) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.ExciseNodeValidityWithTx(ctx, id, from, to, txFrom)
}

// ExciseRelValidityWithTx is the relationship counterpart of
// ExciseNodeValidityWithTx.
func (a *API) ExciseRelValidityWithTx(ctx context.Context, id types.RelID, from, to, txFrom types.Instant) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.ExciseRelValidityWithTx(ctx, id, from, to, txFrom)
}

// Snapshot captures the graph state at t. EFFECTIVE view: a relationship is
// included only if it and both its endpoints are valid at t (RelsAt(t) is the
// declared, unmasked row view and may return more).
func (a *API) Snapshot(t types.Instant) (*GraphSnapshot, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.Snapshot(t)
}

// Diff returns the snapshot diff between t1 and t2. Relationship presence at
// each instant uses the EFFECTIVE rule of Snapshot.
func (a *API) Diff(t1, t2 types.Instant) (*SnapshotDiff, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.Diff(t1, t2)
}

// DiffCallback streams the diff between t1 and t2 through the given handlers.
// Relationship presence uses the EFFECTIVE rule of Snapshot.
func (a *API) DiffCallback(t1, t2 types.Instant, h DiffHandlers) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.DiffCallback(t1, t2, h)
}

// NodeInterval returns the interval [valid_from, valid_to) for n.
//
// A row without a valid-from starts at its TxFrom (backlog 62).
func (a *API) NodeInterval(n *types.Node) (start, end types.Instant, err error) {
	ops, err := a.ready()
	if err != nil {
		return 0, 0, err
	}
	return ops.NodeInterval(n)
}

// RelInterval returns the interval [valid_from, valid_to) for r.
//
// A row without a valid-from starts at its TxFrom (backlog 62).
func (a *API) RelInterval(r *types.Relationship) (start, end types.Instant, err error) {
	ops, err := a.ready()
	if err != nil {
		return 0, 0, err
	}
	return ops.RelInterval(r)
}

// NodeMatchesValidTime reports whether n passes the valid-time filter in opts
// (ValidAt point, ValidStart/ValidEnd interval, or no filter) using the
// canonical effective-valid-from predicate shared with the store push-down and
// the RelMatchesValidTime path. Returns false (no match) on a not-ready graph
// or nil node — a post-filter consumer treats either as "excluded", which is
// the safe default. See core.TempOps.NodeMatchesValidTime.
//
// A row without a valid-from starts at its TxFrom (backlog 62).
func (a *API) NodeMatchesValidTime(n *types.Node, opts storepkg.QueryOpts) bool {
	ops, err := a.ready()
	if err != nil {
		return false
	}
	return ops.NodeMatchesValidTime(n, opts)
}

// RelMatchesValidTime reports whether r passes the valid-time filter in opts
// (ValidAt point, ValidStart/ValidEnd interval, or no filter) using the
// canonical effective-valid-from predicate shared with the store push-down and
// the node path. Returns false (no match) on a not-ready graph or nil rel — a
// post-filter consumer treats either as "excluded", which is the safe default.
// See core.TempOps.RelMatchesValidTime.
//
// A row without a valid-from starts at its TxFrom (backlog 62).
func (a *API) RelMatchesValidTime(r *types.Relationship, opts storepkg.QueryOpts) bool {
	ops, err := a.ready()
	if err != nil {
		return false
	}
	return ops.RelMatchesValidTime(r, opts)
}

// RelateNodes returns the Allen relation between the validity intervals of x and y
// (as NodeInterval: a row without a valid-from starts at its TxFrom, backlog 62).
func (a *API) RelateNodes(x, y *types.Node) (types.AllenRelation, error) {
	ops, err := a.ready()
	if err != nil {
		return 0, err
	}
	return ops.RelateNodes(x, y)
}

// RelateRels returns the Allen relation between the validity intervals of x and y
// (as RelInterval).
func (a *API) RelateRels(x, y *types.Relationship) (types.AllenRelation, error) {
	ops, err := a.ready()
	if err != nil {
		return 0, err
	}
	return ops.RelateRels(x, y)
}

// TagAsOf registers (or overwrites) a named knowledge-time (Erkenntniszeit)
// mark: name → a transaction-time instant addressable via AS OF SYSTEM TIME /
// TxAt. See core.TempOps.TagAsOf.
func (a *API) TagAsOf(name string, at types.Instant) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.TagAsOf(name, at)
}

// ResolveAsOf returns the transaction-time instant registered under name; ok is
// false (nil error) when no such tag exists.
func (a *API) ResolveAsOf(name string) (types.Instant, bool, error) {
	ops, err := a.ready()
	if err != nil {
		return 0, false, err
	}
	return ops.ResolveAsOf(name)
}

// AsOfTags returns a snapshot copy of every registered name → instant mark.
func (a *API) AsOfTags() (map[string]types.Instant, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.AsOfTags()
}

// RemoveAsOfTag deletes a named mark (idempotent).
func (a *API) RemoveAsOfTag(name string) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.RemoveAsOfTag(name)
}
