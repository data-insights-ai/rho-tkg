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
//   - Candidates: every entry the gather takes into its hands is held ahead
//     on the read's one room (gatherScan, ReadTally.Hold): the label's (type's)
//     current members at the native snapshot before it copies them (the earlier
//     count is a preflight only). Each native ever-member snapshot charges every
//     copied entry, including repeats of current IDs. Fallbacks hold entries one by
//     one as the history fold yields them, over every label and shard of the
//     read. Hold charges at admission, including entries later dropped by a
//     pin, duplicate or envelope filter. Drop and Settle check for a shared
//     refusal, without a second charge when scanned. So a scan
//     whose candidates pass the room is refused before its list is built, with
//     the entries it held counted as work done and no callback made, and the
//     unlimited control's count is the smallest budget that answers. The
//     history fold of several labels on a store without the membership sidecar
//     builds its ID union in the store's own fold
//     (forEachNodeCandidateIDByDepth) before the gather holds an ID;
//   - BufferBytes: native ID snapshots, pending overlays and each gather copy,
//     charged by the helper before allocation; 8 per ID in a slice and 24 per
//     ID in a set. The consuming stream charges its own copies separately;
//   - ShardOpens (tiered): each closed shard the count (the label's or the
//     type's current members: storepkg.MemberCountMeteredCapability), the
//     gather (the current members and the ever-members:
//     storepkg.MemberGatherMeteredCapability) and the entity reads (the metered
//     read snapshot) are about to open;
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
	// stream reads (RelsEffectiveByIDs' retention gate, a label or type gather:
	// gathered), so admit counts nothing.
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

// gathered marks the run's candidates as counted by the gather that listed
// them (gatherKind.settle), so the stream's admit counts nothing again.
func (r *scanRun) gathered() {
	if r != nil {
		r.admitted = true
	}
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
// (R2 late counts, R2 meter): every entry a gather takes into its hands is one
// candidate of the read, charged before the work that produces it
// (ReadTally.Hold). Drop and Settle check for refusal without charging again.
// So for a
// read that runs to its end, the unlimited control's Work.Candidates is the
// smallest Candidates budget that answers. Within one read every Hold precedes
// the door's first read of an entry handed on.
type gatherKind uint8

const (
	// gatherFree: the caller holds and drops nothing. Native snapshots still
	// count their work; a paged door separately counts the entities it reads.
	gatherFree gatherKind = iota
	// gatherScan: the gather counts its entries before returning the list to
	// the effective scan: native snapshots count before copying, and a fallback
	// holds each extra member as it is produced.
	gatherScan
	// gatherListed: as gatherScan, and the store has counted the label's current
	// members where it listed them (the temporal ByLabel read): only the members
	// past them are held, against the room that listing left.
	gatherListed
)

// held returns o with the read's hold scope (storepkg.QueryOpts.Held): one
// scope a read, shared by its labels and shards. gatherFree holds nothing.
func (k gatherKind) held(o storepkg.QueryOpts) storepkg.QueryOpts {
	if k == gatherFree {
		return o
	}
	return o.Held()
}

// hold holds n entries the gather is about to take into its hands
// (ReadTally.Hold); gatherFree holds nothing.
func (k gatherKind) hold(t *storepkg.ReadTally, n int) error {
	if k == gatherFree || n <= 0 {
		return nil
	}
	return t.Hold(n)
}

// drop checks the read after rejecting entries already charged by Hold.
// gatherFree counts nothing (the page counts its own entities).
func (k gatherKind) drop(t *storepkg.ReadTally, n int) error {
	if k == gatherFree || n <= 0 {
		return nil
	}
	return t.Drop(n)
}

// settle checks for a shared refusal before the gather returns its list.
// Hold already charged every admitted entry; the consuming door does not
// count the list again. gatherFree leaves counting to its caller.
func (k gatherKind) settle(t *storepkg.ReadTally) error {
	if k == gatherFree {
		return nil
	}
	return t.Settle()
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

// membersFit holds, before a store lists the current members of a label or a
// type for a gatherScan under a read meter, the store's own count of them on
// opts' tally (ReadTally.Hold: the read's one room, net of what it holds
// already), so a listing the room cannot hold is refused before it is built,
// and returns how many it held. count runs under the same tally: a store that
// opens a closed shard to count (tiered: a cold shard without sealed counts,
// the archive) charges the open to it before the open
// (nodeCountByLabelUnder, relCountByTypeUnder), under every meter.
func (c *Core) membersFit(opts storepkg.QueryOpts, kind gatherKind, count func(*storepkg.ReadTally) (int, error)) (held int, err error) {
	t := opts.ReadTally()
	if kind != gatherScan || t == nil {
		return 0, nil
	}
	n, err := count(t)
	if err != nil {
		return 0, err
	}
	return n, kind.hold(t, n)
}

// nodeCountByLabelUnder is c.nodeCountByLabel, its shard opens counted in t
// where the store offers storepkg.MemberCountMeteredCapability (t nil: plain).
func (c *Core) nodeCountByLabelUnder(tok uint16, t *storepkg.ReadTally) (int, error) {
	if m, ok := c.store.(storepkg.MemberCountMeteredCapability); ok && t != nil {
		n, err := m.NodeCountByLabelMetered(tok, t)
		if err != nil {
			return 0, err
		}
		return n, validateStoreCount("NodeCountByLabel", n)
	}
	return c.nodeCountByLabel(tok)
}

// relCountByTypeUnder is c.relCountByType, its shard opens counted in t where
// the store offers storepkg.MemberCountMeteredCapability (t nil: plain).
func (c *Core) relCountByTypeUnder(tok uint16, t *storepkg.ReadTally) (int, error) {
	if m, ok := c.store.(storepkg.MemberCountMeteredCapability); ok && t != nil {
		n, err := m.RelCountByTypeMetered(tok, t)
		if err != nil {
			return 0, err
		}
		return n, validateStoreCount("RelCountByType", n)
	}
	return c.relCountByType(tok)
}

// holdListed holds, for a gatherScan, the current members the listing of n
// returned past the held ones membersFit held before it (all n when it held
// none), so every listed member is held once.
func holdListed(held int, kind gatherKind, opts storepkg.QueryOpts, n int) error {
	if kind != gatherScan || n <= held {
		return nil
	}
	return kind.hold(opts.ReadTally(), n-held)
}

// membersAhead preserves the cheap count preflight, including the shard opens
// needed to obtain it. It reserves nothing: the native snapshot admits its
// actual set while locked, so a writer between count and copy cannot escape.
func (c *Core) membersAhead(opts storepkg.QueryOpts, kind gatherKind, count func(*storepkg.ReadTally) (int, error)) error {
	t := opts.ReadTally()
	if kind != gatherScan || t == nil {
		return nil
	}
	n, err := count(t)
	if err != nil {
		return err
	}
	return t.CandidatesAhead(n)
}

func (c *Core) currentMembersMetered() storepkg.CurrentMemberIDsMeteredCapability {
	return nativeCapability[storepkg.CurrentMemberIDsMeteredCapability](c.store, "NodeIDsByLabelMetered", "RelIDsByTypeMetered")
}

func unsupportedMemberGather(opts storepkg.QueryOpts) error {
	return opts.ReadTally().RefuseUnsupported()
}

// nodeIDsByLabelUnder uses only an explicitly implemented native capability;
// a wrapper's inherited method cannot meter its overridden plain listing.
func (c *Core) nodeIDsByLabelUnder(members storepkg.MemberIDsCapability, tok uint16, opts storepkg.QueryOpts) ([]types.NodeID, error) {
	if t := opts.ReadTally(); t != nil {
		if m := c.currentMembersMetered(); m != nil {
			return m.NodeIDsByLabelMetered(tok, t)
		}
		return nil, unsupportedMemberGather(opts)
	}
	return members.NodeIDsByLabel(tok)
}

func (c *Core) relIDsByTypeUnder(members storepkg.MemberIDsCapability, tok uint16, opts storepkg.QueryOpts) ([]types.RelID, error) {
	if t := opts.ReadTally(); t != nil {
		if m := c.currentMembersMetered(); m != nil {
			return m.RelIDsByTypeMetered(tok, t)
		}
		return nil, unsupportedMemberGather(opts)
	}
	return members.RelIDsByType(tok)
}

// txMembersMetered accepts only a store's explicit native implementation.
// A wrapper's plain sidecar override cannot be bypassed by a promoted method.
func (c *Core) txMembersMetered() storepkg.TxMemberGatherMeteredCapability {
	return nativeCapability[storepkg.TxMemberGatherMeteredCapability](c.store, "ForEachLabelTxMemberMetered", "ForEachRelTypeTxMemberMetered")
}

func (c *Core) forEachLabelTxMemberUnder(members storepkg.LabelTxMembershipCapability, tok uint16, opts storepkg.QueryOpts, fn func(types.NodeID, types.Instant) bool) error {
	if t := opts.ReadTally(); t != nil {
		if m := c.txMembersMetered(); m != nil {
			return m.ForEachLabelTxMemberMetered(tok, t, fn)
		}
		return unsupportedMemberGather(opts)
	}
	return members.ForEachLabelTxMember(tok, fn)
}

func (c *Core) forEachRelTypeTxMemberUnder(members storepkg.RelTypeTxMembershipCapability, tok uint16, opts storepkg.QueryOpts, fn func(types.RelID, types.Instant) bool) error {
	if t := opts.ReadTally(); t != nil {
		if m := c.txMembersMetered(); m != nil {
			return m.ForEachRelTypeTxMemberMetered(tok, t, fn)
		}
		return unsupportedMemberGather(opts)
	}
	return members.ForEachRelTypeTxMember(tok, fn)
}

// foldOpensRefused refuses a candidate gather that walks the history IDs of
// every shard of a tiered store (no usable membership sidecar) under any
// meter: the walk cannot charge its cold opens or history probes. It refuses
// before that work even when the meter has no numeric limit.
func (c *Core) foldOpensRefused(opts storepkg.QueryOpts) error {
	if opts.ReadTally() == nil {
		return nil
	}
	if _, opensShards := c.store.(*tiered.Store); !opensShards {
		return nil
	}
	return opts.ReadTally().RefuseUnsupported()
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
