// API accessors for the Graph temporal-query surface (point-in-time, interval,
// bitemporal, snapshot/diff, Allen). Lives in the same package as the temporal
// types it returns (GraphSnapshot, SnapshotDiff, …) — collapsed in v3.4.0
// post-cleanup from the previous pkg/graph/temporalapi sibling.
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

	// Bitemporal (transaction time)
	NowTx() (types.Instant, error)
	NodeAsOf(id types.NodeID, txTime types.Instant) (*types.Node, error)
	RelAsOf(id types.RelID, txTime types.Instant) (*types.Relationship, error)
	NodesAsOf(txTime types.Instant) ([]*types.Node, error)
	RelsAsOf(txTime types.Instant) ([]*types.Relationship, error)

	// Bitemporal point-in-time (valid time AT a given transaction time).
	// txAt == 0 means "no TX filter" — semantically equivalent to NodeAt /
	// NodesAt / RelAt / RelsAt respectively.
	NodeAtTx(id types.NodeID, validAt, txAt types.Instant) (*types.Node, error)
	RelAtTx(id types.RelID, validAt, txAt types.Instant) (*types.Relationship, error)
	NodesAtTx(validAt, txAt types.Instant) ([]*types.Node, error)
	RelsAtTx(validAt, txAt types.Instant) ([]*types.Relationship, error)
	NodesDuringTx(from, to, txAt types.Instant) ([]*types.Node, error)
	RelsDuringTx(from, to, txAt types.Instant) ([]*types.Relationship, error)

	// Cascade / timeline edit. Convenience wrapper that calls Update with
	// tkg_valid_from / tkg_valid_to in the props map. Adjacent versions'
	// effective intervals tile via the resolver's vEnd-from-next.ValidFrom
	// derivation. Mid-history overlap is not handled by the MVP.
	SetNodeVersionInterval(ctx context.Context, id types.NodeID, validFrom, validTo types.Instant, props map[string]any) (*types.Node, error)
	SetRelVersionInterval(ctx context.Context, id types.RelID, validFrom, validTo types.Instant, props map[string]any) (*types.Relationship, error)

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

	// Named knowledge-time (Erkenntniszeit) marks (§4.2). The instant is a
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
func (a *API) RelsAt(t types.Instant) ([]*types.Relationship, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.RelsAt(t)
}

// RelsByTypeAt returns relationships of relType valid at t.
func (a *API) RelsByTypeAt(relType string, t types.Instant) ([]*types.Relationship, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.RelsByTypeAt(relType, t)
}

// NeighborsAt returns the neighbours of nodeID valid at t. History-aware:
// returns neighbours connected via rels (current or deleted) that were valid
// at t. The deleted-rel fold scales with the number of deleted relationships,
// not the total history size, when the underlying store implements
// DeletedIterationCapability — every in-tree backend (memory, badger, tiered)
// does. Same characteristic applies to OutgoingRelsAt and IncomingRelsAt.
func (a *API) NeighborsAt(nodeID types.NodeID, t types.Instant) ([]*types.Node, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.NeighborsAt(nodeID, t)
}

// OutgoingRelsAt returns relationships where nodeID is the start endpoint and
// the relationship was valid at t. History-aware: includes deleted rels that
// were valid at t and returns the version-at-t of each. Sorted by rel ID.
// Returns ErrNodeNotFound if nodeID was not valid at t.
func (a *API) OutgoingRelsAt(nodeID types.NodeID, t types.Instant) ([]*types.Relationship, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.OutgoingRelsAt(nodeID, t)
}

// IncomingRelsAt returns relationships where nodeID is the end endpoint and
// the relationship was valid at t. Mirror of OutgoingRelsAt.
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
func (a *API) RelsDuring(start, end types.Instant) ([]*types.Relationship, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.RelsDuring(start, end)
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
func (a *API) RelsByTypePropertyDuring(relType, key string, value any, start, end types.Instant) ([]*types.Relationship, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.RelsByTypePropertyDuring(relType, key, value, start, end)
}

// NodeAsOf returns the version of node id known at transaction time txTime.
func (a *API) NodeAsOf(id types.NodeID, txTime types.Instant) (*types.Node, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.NodeAsOf(id, txTime)
}

// RelAsOf returns the version of relationship id known at transaction time txTime.
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
// separation strict and the value correct across a Close/reopen — a bare
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

// NodesAsOf returns nodes known at transaction time txTime.
func (a *API) NodesAsOf(txTime types.Instant) ([]*types.Node, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.NodesAsOf(txTime)
}

// RelsAsOf returns relationships known at transaction time txTime.
func (a *API) RelsAsOf(txTime types.Instant) ([]*types.Relationship, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.RelsAsOf(txTime)
}

// NodeAtTx returns the node version valid at validAt as known at txAt
// (bitemporal point query). txAt == 0 is equivalent to NodeAt(id, validAt).
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
func (a *API) RelsDuringTx(from, to, txAt types.Instant) ([]*types.Relationship, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.RelsDuringTx(from, to, txAt)
}

// SetNodeVersionInterval declares node id is in state props for [validFrom, validTo).
// validTo == 0 means open-ended. Adjacent versions' intervals tile via the resolver.
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

// Snapshot captures the graph state at t.
func (a *API) Snapshot(t types.Instant) (*GraphSnapshot, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.Snapshot(t)
}

// Diff returns the snapshot diff between t1 and t2.
func (a *API) Diff(t1, t2 types.Instant) (*SnapshotDiff, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.Diff(t1, t2)
}

// DiffCallback streams the diff between t1 and t2 through the given handlers.
func (a *API) DiffCallback(t1, t2 types.Instant, h DiffHandlers) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.DiffCallback(t1, t2, h)
}

// NodeInterval returns the interval [valid_from, valid_to) for n.
func (a *API) NodeInterval(n *types.Node) (start, end types.Instant, err error) {
	ops, err := a.ready()
	if err != nil {
		return 0, 0, err
	}
	return ops.NodeInterval(n)
}

// RelInterval returns the interval [valid_from, valid_to) for r.
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
func (a *API) RelMatchesValidTime(r *types.Relationship, opts storepkg.QueryOpts) bool {
	ops, err := a.ready()
	if err != nil {
		return false
	}
	return ops.RelMatchesValidTime(r, opts)
}

// RelateNodes returns the Allen relation between the validity intervals of x and y.
func (a *API) RelateNodes(x, y *types.Node) (types.AllenRelation, error) {
	ops, err := a.ready()
	if err != nil {
		return 0, err
	}
	return ops.RelateNodes(x, y)
}

// RelateRels returns the Allen relation between the validity intervals of x and y.
func (a *API) RelateRels(x, y *types.Relationship) (types.AllenRelation, error) {
	ops, err := a.ready()
	if err != nil {
		return 0, err
	}
	return ops.RelateRels(x, y)
}

// TagAsOf registers (or overwrites) a named knowledge-time (Erkenntniszeit)
// mark: name → a transaction-time instant addressable via AS OF SYSTEM TIME /
// TxAt. See core.TempOps.TagAsOf and §4.2.
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
