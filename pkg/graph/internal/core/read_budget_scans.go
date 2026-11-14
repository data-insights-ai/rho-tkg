package core

import (
	"context"
	"math"

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
//   - Candidates: one per entity, before the entity is read; and the gather that
//     lists them is held to the candidate room as it lists (R2 late counts,
//     gatherScan): the label's (type's) current members are checked against the
//     room by the store's own count before the store lists them, the ever-members
//     past them as the sidecar or the history fold yields them, the list of
//     several labels as it unites them, so a scan whose candidates pass the
//     room is refused before its list is built, with the members it saw counted
//     as work done and no callback made. A tiered store under a ShardOpens
//     bound is the exception for the first check (its count may open a shard no
//     counter sees), and the history fold of several labels on a store without
//     the membership sidecar builds its ID union in the store's own fold
//     (forEachNodeCandidateIDByDepth) before the gather sees it;
//   - BufferBytes: 8 per listed candidate, before each joins the list where the
//     gather builds it (the by-endpoint scans) and else once the gather has
//     built it, before the first entity is read;
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

// gatherKind says who counts the candidates a label or type gather lists
// (R2 late counts), so that the gather itself holds its list to the candidate
// room as it grows instead of building it first.
type gatherKind uint8

const (
	// gatherFree: the caller counts nothing of the list as a whole (a paged door
	// counts the entities of its page, a read without a budget nothing).
	gatherFree gatherKind = iota
	// gatherScan: the caller counts one candidate per list entry when it reads
	// the entity (the effective scans), so the list must fit the candidate room.
	gatherScan
	// gatherListed: as gatherScan, and the store has counted the label's current
	// members where it listed them (the temporal ByLabel read), so only the
	// members past them count against the room.
	gatherListed
)

// fits refuses a gather that holds n candidates, the first current of which are
// the label's current members: the entries the caller counts when it reads them
// must fit the candidate room t has left. A gather past it is refused as it
// grows, before its list is completed: the budget error counts the candidates it
// holds (ReadTally.CandidatesAhead, which counts nothing within the room; what
// it counts is what the list needed, not work done, and ReadBudgetError has no
// field to say so). A nil t, a meter without a candidate limit and gatherFree
// refuse nothing.
func (k gatherKind) fits(t *storepkg.ReadTally, current, n int) error {
	switch k {
	case gatherScan:
		return t.CandidatesAhead(n)
	case gatherListed:
		return t.CandidatesAhead(n - current)
	}
	return nil
}

// hold is the gather's allowance in the stores (storepkg.GatherHold, R2 round
// 3): the ever-members the stores may copy for this gather, over every shard,
// before the read is refused, so a store refuses a set it cannot hold before it
// copies it (O(1)) instead of copying, merging and handing on every member
// before fits refuses. The stores reserve each set whole, the members the read
// then drops (a transaction time past the pin, at or below After, the label's
// current members of a gatherScan, an entity on two shards twice) included: they
// are copied, so they are work. Its room is fits' (the current members of a
// gatherListed added, as the read leaves them out); its refusal is fits'. nil
// when nothing bounds the gather (gatherFree, no candidate limit).
func (k gatherKind) hold(t *storepkg.ReadTally, current int) *storepkg.GatherHold {
	if k == gatherFree || t == nil {
		return nil
	}
	room := t.CandidateRoom()
	if room == math.MaxUint64 {
		return nil
	}
	offset := 0
	if k == gatherListed {
		offset = current
	}
	limit := int64(min(room, math.MaxInt64/2)) + int64(offset) // #nosec G115 -- clamped
	return storepkg.NewGatherHold(limit, func(n int) error { return t.CandidatesAhead(n - offset) })
}

// over reports, counting nothing, whether fits would refuse: a gather that may
// still shrink its list (the envelope prune of the temporal ByLabel read) asks
// before it spends the refusal, which counts the list as work done.
func (k gatherKind) over(t *storepkg.ReadTally, current, n int) bool {
	if t == nil {
		return false
	}
	switch k {
	case gatherScan:
		return uint64(max(n, 0)) > t.CandidateRoom() // #nosec G115 -- a count
	case gatherListed:
		return uint64(max(n-current, 0)) > t.CandidateRoom() // #nosec G115 -- a count
	}
	return false
}

// noteListed tells the test hook (Core.candidatesListed) that a gather listed n
// IDs: the native count of the work, which no read meter keeps.
func (c *Core) noteListed(n int) {
	if c.candidatesListed != nil {
		c.candidatesListed(n)
	}
}

// noteProbed tells the test hook (Core.envelopeProbed) that an envelope prune
// handed n IDs to the store: one envelope probe each, the native count of the
// prune's work, which no read meter keeps (PRUNELIN).
func (c *Core) noteProbed(n int) {
	if c.envelopeProbed != nil {
		c.envelopeProbed(n)
	}
}

// membersFit refuses, before a store lists the current members of a label or a
// type for a gatherScan, a listing the candidate room cannot hold: the store's
// own count of them (count, O(1) on every store) against the room. It asks
// nothing when opts carries no candidate limit, and not a tiered store when the
// budget bounds ShardOpens, whose count may open a closed shard no counter
// would see: there the listing's own metered gather (nodeIDsByLabelUnder)
// charges the opens and the list is bounded by what the stores hold
// (documented as a limit of the budgeted scans, docs/api.md).
func (c *Core) membersFit(opts storepkg.QueryOpts, kind gatherKind, count func() (int, error)) error {
	if kind != gatherScan {
		return nil
	}
	t := opts.ReadTally()
	if t.CandidateRoom() == math.MaxUint64 {
		return nil
	}
	if m, _ := opts.ReadMeter(); m.BoundsShardOpens() {
		if _, opensShards := c.store.(*tiered.Store); opensShards {
			return nil
		}
	}
	n, err := count()
	if err != nil {
		return err
	}
	return t.CandidatesAhead(n)
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

// forEachLabelTxMemberUnder is members.ForEachLabelTxMember under t and the
// gather's hold where the store offers the metered form
// (storepkg.TxMemberGatherMeteredCapability: badger, memory, tiered), so the
// stores' copies are reserved and charged before they are made. A store without
// it (a wrapper) copies unbounded and fn counts the members after.
func forEachLabelTxMemberUnder(members storepkg.LabelTxMembershipCapability, tok uint16, t *storepkg.ReadTally, hold *storepkg.GatherHold, fn func(types.NodeID, types.Instant) bool) error {
	if m, ok := members.(storepkg.TxMemberGatherMeteredCapability); ok && t != nil {
		return m.ForEachLabelTxMemberMetered(tok, t, hold, fn)
	}
	return members.ForEachLabelTxMember(tok, fn)
}

// forEachRelTypeTxMemberUnder is forEachLabelTxMemberUnder for a type.
func forEachRelTypeTxMemberUnder(members storepkg.RelTypeTxMembershipCapability, tok uint16, t *storepkg.ReadTally, hold *storepkg.GatherHold, fn func(types.RelID, types.Instant) bool) error {
	if m, ok := members.(storepkg.TxMemberGatherMeteredCapability); ok && t != nil {
		return m.ForEachRelTypeTxMemberMetered(tok, t, hold, fn)
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
