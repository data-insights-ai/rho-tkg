package temporal

import (
	"context"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// ChainTop is the mark NodeChangeFloorCtx hands out for a node's chain at a
// pin: the state a read at that pin saw, as the next call compares it. The
// zero value is no mark. A caller keeps it beside its read and passes it back
// unchanged; its fields are rho's to interpret.
type ChainTop struct {
	Updated types.Instant // the current row's UpdatedAt
	Gen     uint64        // the graph's rewrite generation (an open, an import, a Clear: a new one)
	Version uint32        // the highest version recorded by the pin
	Cur     uint32        // the current row's version
}

// changeFloorOps is the optional part of Ops NodeChangeFloorCtx reaches (the
// graph's own ops implement it).
type changeFloorOps interface {
	NodeChangeFloorCtx(ctx context.Context, id types.NodeID, after types.Instant, afterTop ChainTop, pin types.Instant) (types.Instant, ChainTop, error)
}

// NodeChangeFloorCtx reports how far down valid time node id's effective
// timeline changed between a caller's earlier read and pin. after and afterTop
// are that read's mark: the pin it read at and the ChainTop a call of this
// door at that pin returned just before the read (the zero ChainTop: no mark).
//
// floor > types.UnboundedValidFrom is proven: (1) the timeline at after read
// now is the one the mark's read returned; (2) at every valid instant below
// floor the timelines at after and at pin have the same winner (same version,
// properties, labels and valid bounds; the row's TxTo may since be stamped).
// So with Old = NodeEffectiveTimelineFrom(id, floor, after) and New =
// NodeEffectiveTimelineFrom(id, floor, pin), timeline(pin) is timeline(after)
// without Old's segments, plus New's, as segment lists. A segment belongs to
// the tail when it is open or ends at or after floor (the From doors' rule);
// floor need not be a segment bound. floor == types.InfiniteValidTo: nothing
// changed, read nothing. floor == types.UnboundedValidFrom: not proven, read
// the whole timeline at pin.
//
// Proven needs: the store's newest-first history walk
// (store.HistorySuffixCapability: memory, badger, tiered with the node's
// history on one shard; sharded and a replica are never proven); a chain
// with no delete or retraction stamp whose TxFrom never falls as the version
// rises (the write doors refuse a chain whose TxFrom falls; a store holding
// one is unproven for good); the mark's top row still the newest the pin
// after records (a backfill at or below after since the mark is unproven);
// the mark's current row not rewritten in place (UpdateInPlace); the same
// rewrite generation (no import, merge, untimed conversion, history
// compaction or reopen since the mark; a Clear or an exact erasure removes
// the node: store.ErrNodeNotFound). Corrections over several pieces are
// proven: floor is the lowest valid start among the rows written since the
// mark.
//
// top is the mark for the read at pin that follows; a zero top makes the
// next call unproven. A write landing between this call and that read is
// answered unproven or with a floor at or below it by the next call, never
// missed. Marks taken inside a GraphTx that rolls back are invalid.
//
// Cost: the current row, the history rows recorded after `after` plus one
// (two when the current row is the newest recorded by after; rows newer
// than pin among them), at most one point read of the mark's current
// version when it moved to history, and every history row when the store
// does not show the chain ordered (badger and tiered once after a
// correction demoted the current row: the door then checks the order on
// the rows read and answers from them, and the walk resolves the store's
// bit); each charged to ctx's read budget as
// NodeEffectiveTimelineFromCtx charges them (one candidate, HistoryExamined
// and DecodedBytes per row). Never a fold.
//
// Errors, as NodeEffectiveTimelineFromCtx at the same pins:
// ErrInvalidTimeRange (after > pin), ErrTxPinTooNew, the compaction and
// retention refusals for after or pin, store.ErrNodeNotFound (no current row
// and no history), the read budget's *store.ReadBudgetError, ctx's error;
// with an error floor and top are no answer. An Ops without the door
// answers unproven and a zero top.
func (a *API) NodeChangeFloorCtx(ctx context.Context, id types.NodeID, after types.Instant, afterTop ChainTop, pin types.Instant) (types.Instant, ChainTop, error) {
	ops, err := a.ready()
	if err != nil {
		return 0, ChainTop{}, err
	}
	if m, ok := ops.(changeFloorOps); ok {
		return m.NodeChangeFloorCtx(ctx, id, after, afterTop, pin)
	}
	if err := asOfWithoutBudget(ctx, "NodeChangeFloor"); err != nil {
		return 0, ChainTop{}, err
	}
	return types.UnboundedValidFrom, ChainTop{}, nil
}
