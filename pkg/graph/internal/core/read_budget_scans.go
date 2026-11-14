package core

import (
	"context"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/grapherr"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// The effective scans under a read budget (storepkg.WithReadBudget, RH list
// R2, sigma's group B). A scan streams, so it has no partial answer to withhold:
// it charges before each entity it reads and stops at the first refusal, and
// the callbacks it made before that stand (a refused scan has handed out a
// prefix of the plain scan's segments, none after the refusal).
//
// What a budgeted scan counts:
//   - Candidates: one per entity, before the entity is read (the candidate
//     list the scan gathers is not bounded by it: it is built first);
//   - BufferBytes: 8 per listed candidate, once the gather has built the list
//     and before the first entity is read;
//   - ShardOpens (tiered): each closed shard the gather (the label's or the
//     type's current members and its ever-members: storepkg.MemberGatherMeteredCapability)
//     and the entity reads (the metered read snapshot) are about to open;
//   - HistoryExamined and DecodedBytes: the history versions and rows each
//     entity's read looks at and decodes, through the store's metered read
//     snapshot (the memory store, which has none, counts the versions of an
//     entity's chain);
//   - Segments: each segment, before it is handed to fn.
// Under a Segments or BufferBytes bound the scan reads one entity at a time
// (ReadMeter.BoundsOutput), so a refusal comes before the next entity is read.
// The From and Window doors take their candidates from the store's valid-end
// index where it keeps one and can count its first call per token after an open
// (storepkg.RelValidEndMeteredCapability: that call reads every ever-member's
// temporal fields, and counts one candidate per entity, its history versions
// and stored bytes before it reads them, and each closed shard it opens); a
// store without the index, or whose index cannot count, reads the candidates of
// the unbounded scan, each counted. Either way they stream what the rule keeps:
// the plain door's segments and order.

// scanRun is the read budget of one scan; a nil *scanRun is the plain scan and
// every method is then a no-op.
type scanRun struct {
	meter *storepkg.ReadMeter
	door  string
	// refuse is the first refusal a segment charge raised: the stream stops
	// when it is set and the scan returns it.
	refuse error
	// admitted is set when a pre-pass has already counted every candidate the
	// stream reads (RelsEffectiveByIDs' retention gate), so admit counts nothing.
	admitted bool
}

// scanRunFor is the shared start of a Ctx scan: ctx is checked first, then a
// budget the store cannot count is refused; a context without a budget gives a
// nil run (the plain scan).
func (c *Core) scanRunFor(ctx context.Context, door string) (*scanRun, error) {
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}
	if err := c.budgetSupported(ctx, door); err != nil {
		return nil, err
	}
	m := storepkg.ReadMeterFrom(ctx)
	if m == nil {
		return nil, nil
	}
	return &scanRun{meter: m, door: door}, nil
}

func (r *scanRun) tally() *storepkg.ReadTally {
	if r == nil {
		return nil
	}
	return r.meter.Tally(r.door)
}

// chunk is how many entities the scan reads per hold of the graph read lock:
// one under an output bound, so each entity's segments are charged and handed
// to fn before the next one is read.
func (r *scanRun) chunk() int {
	if r != nil && r.meter.BoundsOutput() {
		return 1
	}
	return effScanChunk
}

// admit counts one candidate, before its entity is read.
func (r *scanRun) admit() error {
	if r == nil || r.admitted {
		return nil
	}
	return r.meter.AddCandidates(r.door, 1)
}

// holdList counts the candidate list of n IDs the scan holds (8 bytes each).
func (r *scanRun) holdList(n int) error {
	if r == nil {
		return nil
	}
	return r.meter.AddBufferBytes(r.door, n*bufSliceRef)
}

// listCap is how many candidates the scan may list at once when it asks for n:
// n, or one more than the candidate room the meter has left when that is less,
// so a listing never holds more than the one candidate that would pass the
// limit (a nil run lists n).
func (r *scanRun) listCap(n int) int {
	if r == nil {
		return n
	}
	if room := r.tally().CandidateRoom(); room < uint64(max(n, 0)) { // #nosec G115 -- a count
		return int(room) + 1 // #nosec G115 -- below n
	}
	return n
}

// admitSegments counts n segments the scan is about to hand out, one at a
// time, so a refusal names the segment that passes the limit.
func (r *scanRun) admitSegments(n int) error {
	if r == nil {
		return nil
	}
	for range n {
		if err := r.meter.AddSegments(r.door, 1); err != nil {
			return err
		}
	}
	return nil
}

// pagedReads are the entity reads of a paged door, which reads many chunks of
// entities through one snapshot (none: the store has no snapshot): under a
// budget the snapshot counts the rows it decodes and the history it looks at in
// the run's tally (a store without a meter-aware snapshot, memory, counts an
// entity's chain versions through the reads' tally). closeSnap releases it.
func (c *Core) pagedReads(run *scanRun) (rd entityReads, closeSnap func()) {
	tally := run.tally()
	rd, closeSnap = entityReads{c: c, scan: true, tally: tally}, func() {}
	if c.readSnapshots != nil {
		rd.snap = c.readSnapshots.NewReadSnapshot()
		closeSnap = func() { rd.snap.Close() }
		if tally != nil {
			if m, ok := rd.snap.(storepkg.ReadSnapshotMeterCapability); ok {
				m.MeterReads(tally)
			}
		}
	}
	return rd, closeSnap
}

// holdBytes counts n bytes the scan is about to hold in a buffer (BufferBytes).
func (r *scanRun) holdBytes(n int) error {
	if r == nil {
		return nil
	}
	return r.meter.AddBufferBytes(r.door, n)
}

// opts is o carrying the scan's meter, for the gather's member enumerations.
func (r *scanRun) opts(o storepkg.QueryOpts) storepkg.QueryOpts {
	if r == nil {
		return o
	}
	return o.WithReadMeter(r.meter, r.door)
}

// result is the scan's error: a segment refusal that stopped the stream wins
// over the stream's own (nil) result.
func (r *scanRun) result(err error) error {
	if r != nil && r.refuse != nil {
		return r.refuse
	}
	return err
}

// scanSegments wraps fn so each segment is counted (Segments) before it is
// handed on; a refusal is kept in r and stops the stream.
func scanSegments[S any](r *scanRun, fn func(S) bool) func(S) bool {
	if r == nil {
		return fn
	}
	return func(s S) bool {
		if err := r.meter.AddSegments(r.door, 1); err != nil {
			r.refuse = err
			return false
		}
		return fn(s)
	}
}

// nodeIDsByLabelUnder is members.NodeIDsByLabel, its shard opens counted in t
// where the store offers storepkg.MemberGatherMeteredCapability (t nil: plain).
func nodeIDsByLabelUnder(members storepkg.MemberIDsCapability, tok uint16, t *storepkg.ReadTally) ([]types.NodeID, error) {
	if m, ok := members.(storepkg.MemberGatherMeteredCapability); ok && t != nil {
		return m.NodeIDsByLabelMetered(tok, t)
	}
	return members.NodeIDsByLabel(tok)
}

// relIDsByTypeUnder is nodeIDsByLabelUnder for a relationship type.
func relIDsByTypeUnder(members storepkg.MemberIDsCapability, tok uint16, t *storepkg.ReadTally) ([]types.RelID, error) {
	if m, ok := members.(storepkg.MemberGatherMeteredCapability); ok && t != nil {
		return m.RelIDsByTypeMetered(tok, t)
	}
	return members.RelIDsByType(tok)
}

// forEachLabelTxMemberUnder is members.ForEachLabelTxMember with its shard
// opens counted in t where the store offers the metered form.
func forEachLabelTxMemberUnder(members storepkg.LabelTxMembershipCapability, tok uint16, t *storepkg.ReadTally, fn func(types.NodeID, types.Instant) bool) error {
	if m, ok := members.(storepkg.MemberGatherMeteredCapability); ok && t != nil {
		return m.ForEachLabelTxMemberMetered(tok, t, fn)
	}
	return members.ForEachLabelTxMember(tok, fn)
}

// forEachRelTypeTxMemberUnder is forEachLabelTxMemberUnder for a type.
func forEachRelTypeTxMemberUnder(members storepkg.RelTypeTxMembershipCapability, tok uint16, t *storepkg.ReadTally, fn func(types.RelID, types.Instant) bool) error {
	if m, ok := members.(storepkg.MemberGatherMeteredCapability); ok && t != nil {
		return m.ForEachRelTypeTxMemberMetered(tok, t, fn)
	}
	return members.ForEachRelTypeTxMember(tok, fn)
}

// foldOpensRefused refuses a candidate gather that walks the history IDs of
// every shard of a tiered store (no usable membership sidecar) under a budget
// that bounds ShardOpens: the walk opens closed shards it cannot count, so it
// is refused before it starts (Limit "unsupported"), as the property doors'
// history fold is. Any other read passes.
func (c *Core) foldOpensRefused(opts storepkg.QueryOpts) error {
	m, door := opts.ReadMeter()
	if m == nil || !m.BoundsShardOpens() {
		return nil
	}
	if _, opensShards := c.store.(*tiered.Store); !opensShards {
		return nil
	}
	return &storepkg.ReadBudgetError{Door: door, Limit: "unsupported", Work: m.Work()}
}

// ForEachNodeEffectiveByLabelCtx is ForEachNodeEffectiveByLabel under ctx's
// read budget (temporal.API.ForEachNodeEffectiveByLabelCtx).
func (t *TempOps) ForEachNodeEffectiveByLabelCtx(ctx context.Context, label string, pin types.Instant, fn func(temporal.NodeSegment) bool) error {
	run, err := t.c.scanRunFor(ctx, "ForEachNodeEffectiveByLabel")
	if err != nil {
		return err
	}
	return t.c.forEachNodeEffectiveByLabelsRun(run, []string{label}, recordedPin(pin), fn)
}

// ForEachNodeEffectiveByLabelsCtx is ForEachNodeEffectiveByLabels under ctx's
// read budget.
func (t *TempOps) ForEachNodeEffectiveByLabelsCtx(ctx context.Context, labels []string, pin types.Instant, fn func(temporal.NodeSegment) bool) error {
	run, err := t.c.scanRunFor(ctx, "ForEachNodeEffectiveByLabels")
	if err != nil {
		return err
	}
	return t.c.forEachNodeEffectiveByLabelsRun(run, labels, recordedPin(pin), fn)
}

// ForEachRelEffectiveByTypeCtx is ForEachRelEffectiveByType under ctx's read
// budget.
func (t *TempOps) ForEachRelEffectiveByTypeCtx(ctx context.Context, typeName string, pin types.Instant, fn func(temporal.RelSegment) bool) error {
	run, err := t.c.scanRunFor(ctx, "ForEachRelEffectiveByType")
	if err != nil {
		return err
	}
	return t.c.forEachRelEffectiveByTypeRun(run, typeName, recordedPin(pin), fn)
}

// ForEachNodeEffectiveByLabelsFromCtx is ForEachNodeEffectiveByLabelsFrom under
// ctx's read budget: without one it is the plain door; with one it reads the
// valid-end index's candidates (the unbounded scan's where the store has no
// countable index), each counted, and streams what the From rule keeps.
func (t *TempOps) ForEachNodeEffectiveByLabelsFromCtx(ctx context.Context, labels []string, pin, validFrom types.Instant, fn func(temporal.NodeSegment) bool) error {
	run, err := t.c.scanRunFor(ctx, "ForEachNodeEffectiveByLabelsFrom")
	if err != nil {
		return err
	}
	if run == nil {
		return t.ForEachNodeEffectiveByLabelsFrom(labels, pin, validFrom, fn)
	}
	pin = recordedPin(pin)
	if err := t.c.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return grapherr.ErrNilCallback
	}
	return t.c.nodeEffectiveByLabelsBounded(run, labels, pin, validFrom, func(keep func(temporal.NodeSegment) bool) func([]temporal.NodeSegment) bool {
		return pickFrom(t.c.axis(), validFrom, nodeSegTo, keep, scanSegments(run, fn))
	})
}

// ForEachNodeEffectiveByLabelsWindowCtx is ForEachNodeEffectiveByLabelsWindow
// under ctx's read budget (temporal.API.ForEachNodeEffectiveByLabelsWindowCtx):
// the From doors' candidates and counts, the segments kept by the window.
func (t *TempOps) ForEachNodeEffectiveByLabelsWindowCtx(ctx context.Context, labels []string, pin types.Instant, window temporal.Window, fn func(temporal.NodeSegment) bool) error {
	run, err := t.c.scanRunFor(ctx, "ForEachNodeEffectiveByLabelsWindow")
	if err != nil {
		return err
	}
	return t.forEachNodeEffectiveByLabelsWindowRun(run, labels, recordedPin(pin), window, fn)
}

// ForEachRelEffectiveByTypeWindowCtx is ForEachRelEffectiveByTypeWindow under
// ctx's read budget.
func (t *TempOps) ForEachRelEffectiveByTypeWindowCtx(ctx context.Context, typeName string, pin types.Instant, window temporal.Window, fn func(temporal.RelSegment) bool) error {
	run, err := t.c.scanRunFor(ctx, "ForEachRelEffectiveByTypeWindow")
	if err != nil {
		return err
	}
	return t.forEachRelEffectiveByTypeWindowRun(run, typeName, recordedPin(pin), window, fn)
}

// ForEachRelEffectiveByTypeFromCtx is ForEachRelEffectiveByTypeFrom under ctx's
// read budget, as ForEachNodeEffectiveByLabelsFromCtx.
func (t *TempOps) ForEachRelEffectiveByTypeFromCtx(ctx context.Context, typeName string, pin, validFrom types.Instant, fn func(temporal.RelSegment) bool) error {
	run, err := t.c.scanRunFor(ctx, "ForEachRelEffectiveByTypeFrom")
	if err != nil {
		return err
	}
	if run == nil {
		return t.ForEachRelEffectiveByTypeFrom(typeName, pin, validFrom, fn)
	}
	pin = recordedPin(pin)
	if err := t.c.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return grapherr.ErrNilCallback
	}
	return t.c.relEffectiveByTypeBounded(run, typeName, pin, validFrom, func(keep func(temporal.RelSegment) bool) func([]temporal.RelSegment) bool {
		return pickFrom(t.c.axis(), validFrom, relSegTo, keep, scanSegments(run, fn))
	})
}
