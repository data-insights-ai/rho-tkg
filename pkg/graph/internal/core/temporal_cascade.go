package core

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"

	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// =============================================================================
// Cascade timeline edit — append-only
//
// SetNodeVersionInterval / SetRelVersionInterval record, at transaction time
// `now`, the belief "[newVF, newVT) is `props` (a patch)" — `now` is the clock,
// or the caller instant of the *WithTx doors (interval_withtx.go,
// cascadeInstant: one instant for every appended row). Existing rows are
// NEVER rewritten (lesson 46): no stored ValidFrom/ValidTo/TxFrom/TxTo of a
// prior row changes. The cascade appends fresh rows stamped
// TxFrom = UpdatedAt = now:
//
//   - correction pieces covering [newVF, newVT), cut wherever the
//     pre-correction belief-winner changes; each piece is its base row's
//     content plus the patch (correctionChain.segments);
//   - a resumption row [newVT, next own boundary) re-asserting the value that
//     held at newVT, so the timeline after the target is unchanged (none when
//     newVT is open).
//
// How an existing row [vf, vt) relates to the target is described with these
// classification names, as vocabulary for what the appended rows do to the READ
// result — the stored row is untouched in every case:
//
//   - keep            no overlap; reads unchanged
//   - closeRight      vf < newVF < vt — reads of [newVF, vt) see the pieces
//   - openLeft        newVF <= vf < newVT < vt — reads from newVT see the
//                     resumption
//   - eclipse         newVF <= vf < vt <= newVT — the row is fully covered by
//                     newer pieces; it stays an ordinary row that answers at
//                     transaction pins before `now`
//   - split           vf < newVF < newVT < vt — pieces in the middle, the row
//                     itself on the left, the resumption on the right
//
// Resolution: the resolver filters the chain to TxFrom <= txAt and, where own
// intervals overlap, the newer belief (higher TxFrom, then version) wins
// (resolveNodeVersionAt). Pins before `now` therefore reconstruct the
// pre-correction belief exactly, pins at or after it the corrected one. Every
// row is an ordinary half-open span, including a one-tick [t, t+1) piece:
// there is no sentinel width, and the resolvers skip no shape.
//
// Integrity: TemporalMetadata is NOT part of the content hash (see
// integrity.computeNodeHashWithBuffer). Each appended row gets a fresh hash and
// its PrevHash is its BASE row's hash — the pre-correction belief-winner over
// that row's piece, whose labels/properties the row carries (patched); for a
// gap piece (no version valid there) the base is the "template", the most
// recent version (BACKLOG 10e). The base is selected by the READ-time resolver
// (its answer on the pristine pre-correction chain, correctionChain) — the
// cascade never derives a VT-axis predecessor from nodeVersionBounds/
// relVersionBounds' positional derivation at write time (BACKLOG 10b).
// PrevHash never affects resolution: verifyChainLinkage (integrity.go) only
// requires a non-genesis row's PrevHash to match SOME hash in the same
// entity's chain, and the base is always a member of that chain. Any change to
// how pieces or bases are chosen must re-run the full bitemporal oracle fuzz
// harness (bitemporaloracle_test.go / bitemporaloracle_commitwindow_test.go) —
// this file's bounds logic breaks in non-obvious multi-cascade ways (10b).
//
// "Current" slot: the newest belief among rows whose OWN interval is open
// (ValidTo == 0) takes the store's current slot; the prior current moves to
// history with its bytes unchanged. A whole-axis untimed write onto a closed
// entity moves the slot to its patch of the current row (cascadeNewCurrent).
// If no appended row takes the slot, the current row keeps it, below the
// appended rows' versions.
//
// Versions (backlog 18): appended rows take versions above every stored row;
// the row that takes the current slot gets the highest, so after the common
// correction (an open resumption) the current row is the newest row and a
// later write's version (version_alloc.go) is above every row here.
//
// A hard-deleted entity (no current row, a tombstone in history) is refused
// with ErrEntityDeleted (backlog 14): a correction appended after the
// tombstone would be a belief the as-of and valid-time doors read
// inconsistently.
// =============================================================================

// =============================================================================
// Node cascade
// =============================================================================

func (c *Core) cascadeNodeVersionInterval(ctx context.Context, id types.NodeID, newVF, newVT types.Instant, props map[string]any, at types.Instant) (*types.Node, error) {
	return c.cascadeNode(ctx, id, newVF, newVT, props, at, false)
}

// cascadeNode is the cascade kernel. void selects the excise (excise.go): the
// pieces over [newVF, newVT) are void rows and cover only the parts where a
// version is valid as believed before the call (none: ErrNoVersionValidAt).
func (c *Core) cascadeNode(ctx context.Context, id types.NodeID, newVF, newVT types.Instant, props map[string]any, at types.Instant, void bool) (*types.Node, error) {
	return c.cascadeNodeMode(ctx, id, newVF, newVT, props, at, cascadeModeOf(void), nil, nil)
}

// cascadeNodeMode is the node cascade kernel; mode selects a correction, an
// excise (cascadeVoid) or a whole-axis untimed write (cascadeExisting:
// pieces only where a version is valid, unchanged pieces skipped; prov, when
// set, is the update's provenance for every appended row). lop, set only with
// cascadeExisting, is a whole-axis label change applied to every segment in
// place of props (whole_axis_label.go).
func (c *Core) cascadeNodeMode(ctx context.Context, id types.NodeID, newVF, newVT types.Instant, props map[string]any, at types.Instant, mode cascadeMode, prov *updateProvenance, lop *nodeLabelOp) (*types.Node, error) {
	void, existing := mode == cascadeVoid, mode == cascadeExisting
	if err := storepkg.ValidateNodeID(id); err != nil {
		return nil, err
	}
	if err := checkCascadeInterval(c.axis(), newVF, newVT); err != nil {
		return nil, err
	}
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	c.entityLocks.LockEntity(id.SnowflakeID())
	defer c.entityLocks.UnlockEntity(id.SnowflakeID())

	if err := checkCtx(ctx); err != nil {
		return nil, err
	}
	if err := c.checkpointDirtyRegistriesBeforeMutation("cascade node"); err != nil {
		return nil, err
	}

	current, err := c.getCurrentNode(id)
	if err != nil && !errors.Is(err, storepkg.ErrNodeNotFound) {
		return nil, err
	}
	// The head answer before any write (backlog 68, effective_head.go). A
	// valid one stands in for the history read (backlog 68b,
	// effective_head_write.go).
	headBefore := c.nodeHeadBeforeWrite(id, current)
	// An append at the head is planned from the current row alone (backlog
	// 67, effective_head_write.go), else from a valid head entry.
	above, stamps := func(v uint32) (bool, error) { return c.nodeHasHistoryAbove(id, v) }, c.nodeHistoryStampsFunc(id)
	if headBefore != nil {
		above, stamps = headBefore.above, headBefore.stamps // exact for the chain, no store probe (row 85)
	}
	hc, fromCurrent := chainFromCurrent(c, nodeKernel, current, above, stamps, newVF, newVT, at, void)
	fromHead := fromCurrent
	if !fromCurrent {
		hc, fromHead = chainFromHead(c, headBefore, current, func(v uint32) (*types.Node, error) { return c.getNodeVersion(id, v) },
			newVF, newVT, at, void || existing)
	}
	var history []*types.Node
	if !fromHead {
		if history, err = c.getNodeHistory(id); err != nil {
			return nil, err
		}
		if current == nil && len(history) == 0 {
			return nil, storepkg.ErrNodeNotFound
		}
		if current == nil {
			// Hard-deleted: the chain ends in a tombstone. A correction appended
			// after it would be a belief no door reads consistently (backlog 14).
			return nil, nodeDeletedErr(id)
		}
	}

	maxVersion := hc.maxVersion
	var chainStamps []*types.TemporalMetadata // from the head: the order rule was checked
	if !fromHead {
		maxVersion = current.Version()
		for _, h := range history {
			maxVersion = max(maxVersion, h.Version())
		}
		chainStamps = temporalsOf(history, current)
	}
	nextVersion, err := nextEntityVersion(maxVersion)
	if err != nil {
		return nil, err
	}
	// The clock, or the caller instant at once the order rule holds for the
	// whole chain (interval_withtx.go). Every appended row is stamped with it.
	now, err := c.cascadeInstant(at, chainStamps)
	if err != nil {
		return nil, err
	}

	// The template — the current row, the most recent version — is the
	// content of a gap piece, where no version is valid to patch (see
	// correctionChain.segments).
	template := current

	// APPEND-ONLY (audited correction). The cascade records, AT `now`, a new
	// belief: "[newVF, newVT) is `props`". Transaction time is append-only and
	// monotonic, so we NEVER mutate an existing row's stored valid-interval or
	// transaction stamps (doing so would make a row claim the DB believed a
	// world-boundary at a past TxFrom it actually decided now — corrupting
	// NodeAtTx at earlier txAt). Instead we append two fresh-TxFrom rows and
	// leave every existing row untouched; the resolver tiles the TxFrom-filtered
	// chain by valid-from and, on overlap, the newer belief wins
	// (resolveNodeVersionAt). Existing rows therefore reconstruct the
	// pre-correction belief exactly, and the appended rows the post-correction
	// belief — no holes, no leaks.
	var preChain []*types.Node
	if !fromHead {
		preChain = preCorrectionChain(history, current) // a deleted entity was refused above
	}

	// Resumption: re-assert, from newVT onward, whatever value held AT newVT in
	// the pre-correction belief, so the part of the timeline after the
	// correction is unchanged. Open-ended (newVT == 0) corrections need none.
	//
	// BACKLOG 10b: the resumption row's ValidTo is stamped EXPLICITLY here,
	// via nodeResumptionEnd — the smallest own-interval boundary (any row's
	// own vStart or vEnd) in the PRE-correction chain that is strictly after
	// newVT. This is NOT "the positionally-next row after src" (that is
	// UNSAFE on an already-cascaded preChain: src can be chosen
	// by BELIEF, not position, so the row positionally after it in a
	// valid-from-sorted array can have an own vStart earlier than newVT
	// itself, producing an inverted [newVT, earlier) interval). Own-interval
	// bounds make every row's own [vStart, vEnd) a fixed, position-independent
	// interval, so the set of rows covering any t is piecewise-constant
	// between consecutive own-boundary points — meaning the belief-winner
	// (src) provably cannot change before the NEXT such boundary across ANY
	// row, not just src's positional neighbor. A resumption left open (or
	// bounded by the wrong point) would be structurally indistinguishable,
	// via its own stored ValidFrom/ValidTo/TxFrom, from a genuine override
	// starting at the same point — the two must resolve oppositely on
	// overlap, which is exactly why the bound must be exact.
	//
	// Every appended row's PrevHash is its base row's hash (prevHashes, index
	// aligned with appended); hashes are computed once the versions are final
	// (the version is hashed), see the allocation step below.
	appended := make([]*types.Node, 0, 4)
	prevHashes := make([]string, 0, 4)
	var prep *correctionChain[*types.Node] // the chain prepared once: every cut and the source (row 85)
	if !fromHead {
		prep = prepareCorrection(c, nodeKernel, preChain)
		if headBefore == nil {
			headBefore = prep.head(c.nodeHistoryEpoch(id), current) // the head answer this read paid for
		}
		if !c.axis().Open(newVT) {
			if src, ok := prep.winnerAt(newVT); ok {
				hc.src, hc.hasSrc, hc.srcEnd = src, true, nodeResumptionEnd(c, preChain, newVT)
			}
		}
	}
	// No resumption when src's own interval starts at newVT (RESUME, sigma):
	// the pieces end there, so every row covering an instant from newVT on
	// is unchanged and src still answers it; the copy would only be a
	// wasted row (one per rewritten step).
	if hc.hasSrc && !c.nodeStartsAt(hc.src, newVT) {
		resumption := hc.src.DeepCopy()
		ensureNodeTemporal(resumption)
		stampValidFrom(c.axis(), resumption.Temporal(), newVT, true)
		resumption.Temporal().ValidTo = hc.srcEnd // explicit; the axis's open end when src was the pre-correction open tail
		stampAppendedRow(resumption.Temporal(), now)
		appended = append(appended, resumption)
		prevHashes = append(prevHashes, nodeHashOf(hc.src))
	}

	// The inserted interval [newVF, newVT) carrying the corrected state. `props`
	// is a PATCH applied to the state that held — as believed BEFORE this
	// correction — at each instant of the interval, never to today's row: the
	// interval is split wherever the pre-correction belief-winner changes, and
	// each piece is the winner's content plus the patch (see
	// correctionChain.segments). Rows are fully built (patched, bounded, hashed)
	// before any write — preflight, then apply.
	segments := hc.segments
	if !fromHead {
		segments = prep.segments(template, newVF, newVT, void || existing)
	}
	if void {
		if err := exciseNoVersionErr(segments, newVF, newVT); err != nil {
			return nil, err
		}
	}
	var newVer *types.Node // the appended row covering newVF — the return value
	sup := wholeAxisSupersession{voidFree: hc.voidFree}
	if !fromHead {
		sup.voidFree = !slices.ContainsFunc(preChain, isVoidRow[*types.Node])
	}
	for _, seg := range segments {
		if (void || existing) && seg.gap {
			sup.note(true, false)
			continue // nothing valid there: no absence to record, no existence to add
		}
		if existing && !lop.segmentMutates(seg.base, func() bool { return nodePreparedUpdateMutates(seg.base, provOrZero(prov), updateTemporal{}, props) }) {
			sup.note(false, false)
			continue // the segment's version already holds the change
		}
		sup.note(false, true)
		row, err := c.buildNodeCorrectionRow(seg, template, now, props)
		if err != nil {
			return nil, err
		}
		if err := c.applyNodeLabelOp(row, lop); err != nil {
			return nil, err
		}
		if void {
			if err := markVoid(row); err != nil {
				return nil, err
			}
		}
		if newVer == nil {
			newVer = row
		}
		appended = append(appended, row)
		prevHashes = append(prevHashes, nodeHashOf(seg.base))
	}
	if existing && len(appended) == 0 {
		return nil, nil // no segment changes: the update is a no-op
	}
	// Provisional versions in build order, all above every stored version, so
	// the newest-belief comparison below ranks appended rows above existing
	// ones exactly as their final versions will.
	firstVersion := nextVersion
	for i, r := range appended {
		if i > 0 {
			if nextVersion, err = nextEntityVersion(nextVersion); err != nil {
				return nil, err
			}
		}
		r.SetVersion(nextVersion)
	}

	// The new "current" row: the newest own-open belief, or a whole-axis
	// write's patch of a closed current row (cascadeNewCurrent).
	newCurrent := cascadeNewCurrent(c, nodeKernel, nodeBeliefNewerThan, nodeHashOf, fromHead, hc.open, preChain, appended, prevHashes, current, existing)

	// Final versions (backlog 18): the row taking the current slot gets the
	// highest version of the cascade, so the newest row recorded at `now` is
	// the current row and a later write's version is above every row here.
	// The other appended rows keep their build order below it.
	curIsNew := moveToEnd(appended, prevHashes, newCurrent)
	for i, r := range appended {
		r.SetVersion(firstVersion + uint32(i)) // #nosec G115 -- bounded by the provisional allocation above
		if err := c.hashAppendedNodeRow(r, prevHashes[i]); err != nil {
			return nil, err
		}
		applyNodeProvenance(r.Integrity(), prov)
	}

	// Write: append the new rows, never touching existing ones. The store keeps
	// one "current" KV slot; place newCurrent there (demoting the prior current
	// to a history row — its bytes are unchanged, only its slot moves).
	labelSlot := false // the current slot changes by lop's one token (whole_axis_label.go)
	if curIsNew {
		// The store's current slot cannot change label tokens (ReplaceNode
		// rejects it). A correction row's labels come from its base row, and
		// the only row that can take the slot is the open tail, whose base is
		// the pre-correction current itself (or the template, for a gap) — so
		// this holds by construction. A whole-axis label change (lop) changes
		// them by exactly its token, through the store's label door below.
		// Checked here, BEFORE any write, so a violation surfaces as an error
		// instead of a half-applied cascade.
		var err error
		if labelSlot, err = lop.slotChanges(current, newCurrent); err != nil {
			return nil, fmt.Errorf("graph: cascade new current row: %w", err)
		}
		if !labelSlot {
			if err := storepkg.ValidateNodeReplacement(current, newCurrent); err != nil {
				return nil, fmt.Errorf("graph: cascade new current row: %w", err)
			}
		}
	}
	// Unique constraints (unique_cascade.go): the built rows are judged after
	// every kernel check and before the first write; the value stripes stay
	// held across every write below.
	// A failed write withdraws the UniqueForever claims no written row carries.
	uniqueHold, err := c.enforceUniqueForCascade(id, current, appended, newCurrent, curIsNew, newVT, props, lop.added())
	if err != nil {
		return nil, err
	}
	defer uniqueHold.release()
	written := make([]*types.Node, 0, len(appended)+1)
	for _, r := range appended {
		if r == newCurrent {
			continue // written via ReplaceNode below
		}
		if err := c.putNodeVersionScopedAware(ctx, id, r.Version(), r); err != nil {
			return nil, uniqueHold.writeFailed(written, fmt.Errorf("graph: cascade put appended version: %w", err))
		}
		written = append(written, r)
	}
	if curIsNew {
		demoted := current
		if existing && sup.replaces(current.Temporal(), now) {
			demoted = current.DeepCopy()
			ensureNodeTemporal(demoted)
			demoted.Temporal().TxTo = now
		}
		if err := c.putNodeVersionScopedAware(ctx, id, demoted.Version(), demoted); err != nil {
			return nil, uniqueHold.writeFailed(written, fmt.Errorf("graph: cascade demote current to history: %w", err))
		}
		written = append(written, demoted)
		if labelSlot {
			if err := lop.writeSlot(c, id, newCurrent); err != nil {
				return nil, uniqueHold.writeFailed(written, fmt.Errorf("graph: cascade replace current: %w", err))
			}
		} else if err := c.replaceNodeScopedAware(ctx, newCurrent); err != nil {
			return nil, uniqueHold.writeFailed(written, fmt.Errorf("graph: cascade replace current: %w", err))
		}
	}

	headCur := current
	if curIsNew {
		headCur = newCurrent
	}
	c.nodeHeadAfterWrite(id, headBefore, appended, headCur)
	switch {
	case fromCurrent:
		c.appendWrites.Add(1)
	case fromHead:
		c.headWrites.Add(1)
	}
	c.opNodeUpdates.Add(1)
	return newVer, nil
}

// nodeResumptionEnd returns the smallest own-interval boundary (any row's own
// vStart, or own vEnd when finite) in preChain that is STRICTLY AFTER newVT —
// the graph's valid axis's open end when no such boundary exists. BACKLOG 10b: own-interval bounds
// make every row's [vStart, vEnd) a fixed, position-independent interval, so
// the SET of rows covering any instant t is piecewise-constant between
// consecutive own-boundary points across the WHOLE chain — therefore the
// belief-winner at newVT (src) provably cannot change before the next such
// boundary, from ANY row, not merely the row positionally adjacent to src.
// Scanning every row (not just src's chain neighbor) is what makes this safe
// on an already-cascaded preChain, where src can be selected by belief
// rather than position.
func nodeResumptionEnd(c *Core, preChain []*types.Node, newVT types.Instant) types.Instant {
	a := c.axis()
	end := a.OpenEnd()
	consider := func(b types.Instant) {
		if b > newVT && (a.Open(end) || b < end) {
			end = b
		}
	}
	for _, row := range preChain {
		vStart, vEnd := c.nodeOwnBounds(row)
		consider(vStart)
		if !a.Open(vEnd) {
			consider(vEnd)
		}
	}
	return end
}

// nodeStartsAt reports whether n's own interval starts at t: a resumption
// from t would copy it.
func (c *Core) nodeStartsAt(n *types.Node, t types.Instant) bool {
	vStart, _ := c.nodeOwnBounds(n)
	return vStart == t
}

// relStartsAt mirrors nodeStartsAt for relationships.
func (c *Core) relStartsAt(r *types.Relationship, t types.Instant) bool {
	vStart, _ := c.relOwnBounds(r)
	return vStart == t
}

// relResumptionEnd mirrors nodeResumptionEnd for relationships.
func relResumptionEnd(c *Core, preChain []*types.Relationship, newVT types.Instant) types.Instant {
	a := c.axis()
	end := a.OpenEnd()
	consider := func(b types.Instant) {
		if b > newVT && (a.Open(end) || b < end) {
			end = b
		}
	}
	for _, row := range preChain {
		vStart, vEnd := c.relOwnBounds(row)
		consider(vStart)
		if !a.Open(vEnd) {
			consider(vEnd)
		}
	}
	return end
}

// =============================================================================
// Correction segments — the patch is applied to the THEN-VALID state
//
// SetNodeVersionInterval's `props` is a patch (nil deletes a key). A patch
// means "change these keys during [newVF, newVT); everything else stays as it
// was believed to be then". The inserted rows are therefore built, per piece
// of the interval, from the pre-correction belief-winner at that piece — not
// from the most recent version, which would copy every unpatched property
// from TODAY's state into the past.
//
// The winner is piecewise constant between consecutive own-interval
// boundaries of the pre-correction chain (the same argument as
// nodeResumptionEnd), so the interval is cut at every such boundary inside it,
// the winner is resolved at each piece's start, and adjacent pieces with the
// same winner are merged back into one row.
// =============================================================================

// correctionSegment is one maximal piece [from, to) of a cascade target
// interval (to open by the graph's valid axis) over which the patch base is
// one row.
type correctionSegment[T any] struct {
	from, to types.Instant
	base     T
	gap      bool // no version valid there: base is the template
}

// nodeCorrectionSegment and relCorrectionSegment are the node and
// relationship pieces.
type (
	nodeCorrectionSegment = correctionSegment[*types.Node]
	relCorrectionSegment  = correctionSegment[*types.Relationship]
)

// correctionCuts returns the sorted, de-duplicated boundaries strictly inside
// (newVF, newVT) — (newVF, +inf) when newVT is open by the valid axis a. An
// open bound (a row's open end) is no boundary.
func correctionCuts(a types.ValidAxis, bounds []types.Instant, newVF, newVT types.Instant) []types.Instant {
	cuts := make([]types.Instant, 0, len(bounds))
	for _, b := range bounds {
		if !a.Open(b) && b > newVF && (a.Open(newVT) || b < newVT) {
			cuts = append(cuts, b)
		}
	}
	slices.Sort(cuts)
	return slices.Compact(cuts)
}

// correctionChain is a pre-correction chain prepared once per correction
// (row 85): the point resolver's classification, order and every row's
// covering interval with its supersession cap (sweepIntervals over the
// pristine ascending-version chain, which it copies — lesson 73), so every
// cut point and the resumption source are answered from it instead of a
// resolver run each (a copy, sort, supersession caps and full scan per cut
// made a write cuts × V log V). The cascade's resolver caps no life end
// (resolveNodeVersionAt), and a void winner is no version (a gap), as the
// resolver answers.
type correctionChain[T interface {
	comparable
	storeutil.TemporalRow
}] struct {
	c       *Core
	k       *chainKernel[T]
	chain   []T // pristine, ascending version
	arr     []T // the resolver's order
	ivs     []effInterval
	cascade bool
}

// correctionScanMax is the most instants a correction answers by a scan of
// the intervals each; more are answered in one sweep (correctionChain.sweep).
const correctionScanMax = 8

// prepareCorrection prepares preChain (ascending version, not modified).
func prepareCorrection[T interface {
	comparable
	storeutil.TemporalRow
}](c *Core, k *chainKernel[T], preChain []T) *correctionChain[T] {
	p := &correctionChain[T]{c: c, k: k, chain: preChain}
	if !c.correctionPerCut {
		p.arr, p.ivs, p.cascade = sweepIntervals(c, k, preChain, nil)
	}
	return p
}

// beats reports whether interval x wins over y where both cover an instant.
func (p *correctionChain[T]) beats(x, y effInterval) bool {
	if p.cascade {
		return newerBelief(p.arr, x, y) > 0
	}
	return x.pos > y.pos // the monotonic arm: the highest position
}

// winner is the row interval i wins with (ok false: none, or a void row).
func (p *correctionChain[T]) winner(i int) (T, bool) {
	var zero T
	if i < 0 {
		return zero, false
	}
	r := p.arr[p.ivs[i].pos]
	if isVoidRow(r) {
		return zero, false // an excised piece wins: absent
	}
	return r, true
}

// winnerAt is the pre-correction belief-winner at t: the resolver's answer
// on the chain; ok false where no version is valid. One scan of the
// intervals.
func (p *correctionChain[T]) winnerAt(t types.Instant) (T, bool) {
	if p.c.correctionPerCut {
		// Tests (row 85 twin): the resolver on a fresh copy, per instant.
		r, err := p.k.resolve(p.c, slices.Clone(p.chain), t, nil)
		return r, err == nil
	}
	a := p.c.axis()
	best := -1
	for i, iv := range p.ivs {
		if storeutil.IntervalCovers(a, iv.s, iv.e, t) && (best < 0 || p.beats(iv, p.ivs[best])) {
			best = i
		}
	}
	return p.winner(best)
}

// sweep answers winnerAt at every instant of ts (ascending) in one pass:
// the intervals by start enter a max-heap by priority as the instants pass
// their start, and leave it once their end is passed. O((V + len(ts)) log V).
func (p *correctionChain[T]) sweep(ts []types.Instant) ([]T, []bool) {
	a := p.c.axis()
	ivs := slices.Clone(p.ivs)
	if p.cascade {
		rankBeliefs(p.arr, ivs)
	}
	slices.SortStableFunc(ivs, func(x, y effInterval) int { return cmp.Compare(x.s, y.s) })
	rows, oks := make([]T, len(ts)), make([]bool, len(ts))
	h := effHeap{}
	next := 0
	for j, t := range ts {
		for next < len(ivs) && ivs[next].s <= t {
			h.push(ivs[next])
			next++
		}
		for len(h) > 0 && !a.Open(h[0].e) && h[0].e <= t {
			h.pop()
		}
		if len(h) == 0 {
			continue // a gap
		}
		r := p.arr[h[0].pos]
		if !isVoidRow(r) {
			rows[j], oks[j] = r, true
		}
	}
	return rows, oks
}

// head is the head entry of the prepared chain under the history token
// epoch (effective_head.go), built from the intervals already classified:
// nil when the graph keeps no entries, the chain does not qualify
// (gatherHeadFacts: a delete, a shared version) or the cascade twin reads
// the history (headWriteOff). The pristine chain is in write order (no
// delete: ascending version), and with no delete the fold's life caps are
// empty, so its intervals are these (sweepIntervals with no caps).
func (p *correctionChain[T]) head(epoch uint64, current T) *headEntry {
	c := p.c
	if c.historyEpochs == nil || c.headCacheOff || c.headWriteOff || c.correctionPerCut || epoch == 0 || len(p.chain) == 0 {
		return nil
	}
	var facts headFacts
	gatherHeadFacts(&facts, c, p.k, p.chain)
	if !facts.ok {
		return nil
	}
	facts.cascade = p.cascade
	pieces := sweepPieces(c, p.arr, p.ivs, p.cascade, nil, false)
	return headFromFold(c, epoch, current, pieces, &facts)
}

// segments splits [newVF, newVT) at every boundary where the pre-correction
// belief-winner can change and pairs each piece with its base.
//
// Cut points are every row's OWN vStart/vEnd (the cascade-arm bounds, as in
// nodeResumptionEnd) plus its positional bounds (the monotonic arm's tiling,
// which on a legacy pre-migration chain can start a row at UpdatedAt rather
// than ValidFrom). Extra cuts are harmless — pieces with the same base are
// merged — while a missing cut would patch the wrong base.
//
// The base of a piece is the winner at its start (winnerAt; one sweep for
// more than correctionScanMax pieces). Where NO version
// is valid (a gap: before the entity's first valid-from, after a close, …)
// there is no then-valid state to patch, so the piece's base is the template
// (the most recent version). That is a choice, not a derivation — a gap piece
// asserts a state the entity was never believed to have, and the most recent
// state is the least surprising content for it. splitGaps keeps a gap piece
// apart from a neighbour with the template as its base.
func (p *correctionChain[T]) segments(template T, newVF, newVT types.Instant, splitGaps bool) []correctionSegment[T] {
	c, k := p.c, p.k
	bounds := make([]types.Instant, 0, 4*len(p.chain))
	for i, row := range p.chain {
		vStart, vEnd := k.ownBounds(c, row)
		pStart, pEnd := k.posBounds(c, p.chain, i) // cut points only
		bounds = append(bounds, vStart, vEnd, pStart, pEnd)
	}
	cuts := correctionCuts(c.axis(), bounds, newVF, newVT)
	return p.segmentsAt(template, newVF, newVT, cuts, splitGaps, len(cuts)+1 > correctionScanMax)
}

// segmentsAt is segments over the given cuts: the winner at each piece start
// by one sweep, or by a scan each.
func (p *correctionChain[T]) segmentsAt(template T, newVF, newVT types.Instant, cuts []types.Instant, splitGaps, sweep bool) []correctionSegment[T] {
	var rows []T
	var oks []bool
	if sweep && !p.c.correctionPerCut {
		rows, oks = p.sweep(append([]types.Instant{newVF}, cuts...))
	}
	segs := make([]correctionSegment[T], 0, len(cuts)+1)
	from := newVF
	for i := 0; i <= len(cuts); i++ {
		to := newVT
		if i < len(cuts) {
			to = cuts[i]
		}
		var base T
		var ok bool
		if rows != nil {
			base, ok = rows[i], oks[i]
		} else {
			base, ok = p.winnerAt(from)
		}
		gap := !ok
		if gap {
			base = template
		}
		if n := len(segs); n > 0 && segs[n-1].base == base && (!splitGaps || segs[n-1].gap == gap) {
			segs[n-1].to = to
		} else {
			segs = append(segs, correctionSegment[T]{from: from, to: to, base: base, gap: gap})
		}
		from = to
	}
	return segs
}

// exciseNoVersionErr is the excise's refusal when no version is valid in
// [newVF, newVT) as believed before the call: every piece of segments (split
// at gaps) is a gap. The cascade kernels and the caller-instant pre-flight
// (checkNodeCallerCascade / checkRelCallerCascade) decide it here, on the same
// pieces, so a batch or ingest unit is refused before any write.
func exciseNoVersionErr[T any](segments []correctionSegment[T], newVF, newVT types.Instant) error {
	for _, seg := range segments {
		if !seg.gap {
			return nil
		}
	}
	return fmt.Errorf("%w: excise [%d, %d): no version is valid there", storepkg.ErrNoVersionValidAt, newVF, newVT)
}

// correctionTemporal returns the temporal block for an appended correction
// row: the template's entity-level stamps (CreatedAt, …) with the piece's
// valid interval and the correction's transaction time. The base row supplies
// content (properties, labels, integrity metadata), never these stamps. A row
// recorded now retracts nothing: TxTo and DeletedAt are 0 (stampAppendedRow) —
// a copied TxTo (a superseded base or template) put TxTo below TxFrom and kept
// the row out of the as-of door while the TxAt doors read it (backlog 14).
func correctionTemporal(template *types.TemporalMetadata, from, to, now types.Instant) *types.TemporalMetadata {
	var tm types.TemporalMetadata
	if template != nil {
		tm = *template
	}
	tm.ValidFrom, tm.ValidTo = from, to // a piece's bounds are claims (to: open by the graph's valid axis)
	stampAppendedRow(&tm, now)
	return &tm
}

// stampAppendedRow stamps a row a write appends at transaction instant now:
// TxFrom = UpdatedAt = now, and no retraction (TxTo = DeletedAt = 0). Every
// door that appends a version (cascade pieces and resumptions, Update,
// CloseVersion, label changes, property CAS) starts its new row from a copy
// of an existing one; this clears the stamps that copy may carry.
func stampAppendedRow(tm *types.TemporalMetadata, now types.Instant) {
	tm.UpdatedAt = now
	tm.TxFrom = now
	tm.TxTo = 0
	tm.DeletedAt = 0
	tm.Void = false // a void piece is marked by the excise after stamping
}

// markVoid makes a built correction row a void piece: marked Void and
// without properties (VOIDPROPS: no read takes a value from a void row, so it
// keeps only what places it, the voided version's labels or a relationship's
// type and endpoints). It runs before the row is hashed.
func markVoid[T interface {
	Temporal() *types.TemporalMetadata
	SetProperties(types.PropertySlice) error
}](row T) error {
	row.Temporal().Void = true
	return row.SetProperties(nil)
}

// isVoidRow reports whether a chain row is a void piece of an excise
// (types.TemporalMetadata.Void): absent over its own valid interval.
func isVoidRow[T interface {
	Temporal() *types.TemporalMetadata
}](r T) bool {
	tm := r.Temporal()
	return tm != nil && tm.Void
}

// nodeHashOf is n's content hash ("" without integrity metadata) — the
// PrevHash of a row that corrects or re-asserts n.
func nodeHashOf(n *types.Node) string {
	if ig := n.Integrity(); ig != nil {
		return ig.Hash
	}
	return ""
}

// relHashOf mirrors nodeHashOf for relationships.
func relHashOf(r *types.Relationship) string {
	if ig := r.Integrity(); ig != nil {
		return ig.Hash
	}
	return ""
}

// cascadeNewCurrent picks the row that takes the store's current slot after a
// cascade (node and relationship alike). A row that is not appended (or the
// zero T) means the current row keeps the slot.
//
// The newest belief among rows whose OWN interval is open-ended takes it (the
// value at "now" — resolving the point query at +inf). BACKLOG 10b: this is
// deliberately NOT "the last positionally-open row in valid-from order" — a
// row's own-open status must never be decided by its neighbors, or an
// untouched older open row can wrongly keep the slot from a newer,
// wider-reaching open correction. From a head plan only appended rows can
// win (headNewCurrent); else every row of the post-correction chain competes.
//
// A whole-axis untimed write (existing) onto an entity with no own-open row
// (todo row 80: every segment closed, by a tkg_valid_to close or an excise)
// patches the current row's segments and is the newer belief about them: the
// newest appended row built from the current row (PrevHash = its hash)
// takes the slot, so Get, the write's return value and the current indexes
// follow the patch (tx_order.go: a closed entity's segments are patched).
// Its labels are the current row's plus the write's label change, so the
// slot checks below hold. No such row (the current row's segments were
// unchanged): the current row keeps the slot. An own-open row of the
// pre-correction chain other than the current row (an older belief the close
// or excise superseded) never takes the slot: only an appended row can.
func cascadeNewCurrent[T interface {
	comparable
	storeutil.TemporalRow
}](c *Core, k *chainKernel[T], newer func(a, b T) bool, hashOf func(T) string, fromHead bool, open openBelief,
	preChain, appended []T, prevHashes []string, current T, existing bool) T {
	ownOpen := func(r T) bool {
		_, end := k.ownBounds(c, r)
		return c.axis().Open(end) && !isVoidRow(r)
	}
	var cur, zero T
	if fromHead {
		cur = headNewCurrent(c, k, open, appended)
	} else {
		for _, chain := range [][]T{preChain, appended} {
			for _, r := range chain {
				if ownOpen(r) && (cur == zero || newer(r, cur)) {
					cur = r
				}
			}
		}
	}
	if !existing || slices.Contains(appended, cur) || current == zero || ownOpen(current) {
		return cur
	}
	currentHash := hashOf(current)
	for i := len(appended) - 1; i >= 0; i-- { // build order: the last is the newest belief
		if prevHashes[i] == currentHash && !isVoidRow(appended[i]) {
			return appended[i]
		}
	}
	return cur
}

// moveToEnd moves target (when present) to the end of rows, shifting the rows
// after it down by one, and moves the matching entry of hashes with it. It
// reports whether target was found.
func moveToEnd[T comparable](rows []T, hashes []string, target T) bool {
	for i, r := range rows {
		if r != target {
			continue
		}
		h := hashes[i]
		copy(rows[i:], rows[i+1:])
		copy(hashes[i:], hashes[i+1:])
		rows[len(rows)-1], hashes[len(hashes)-1] = target, h
		return true
	}
	return false
}

// hashAppendedNodeRow computes an appended row's content hash (its version is
// final) and sets its integrity metadata with PrevHash = prevHash, the hash of
// the row it corrects.
func (c *Core) hashAppendedNodeRow(row *types.Node, prevHash string) error {
	hash, err := c.nodeContentHash(row, c.nodeLabelsUnlocked(row))
	if err != nil {
		return fmt.Errorf("graph: cascade compute hash: %w", err)
	}
	row.SetIntegrity(nodeIntegrityWithHash(row.Integrity(), hash, prevHash))
	return nil
}

// hashAppendedRelRow mirrors hashAppendedNodeRow for relationships, refreshing
// the endpoint hashes as well.
func (c *Core) hashAppendedRelRow(row *types.Relationship, prevHash string) error {
	hash, err := c.relContentHash(row, c.relTypeUnlocked(row))
	if err != nil {
		return fmt.Errorf("graph: cascade compute rel hash: %w", err)
	}
	relIG := relIntegrityWithHash(row.Integrity(), hash, prevHash)
	if err := c.refreshRelationshipEndpointHashes(row, relIG); err != nil {
		return fmt.Errorf("graph: cascade refresh endpoint hashes: %w", err)
	}
	row.SetIntegrity(relIG)
	return nil
}

// buildNodeCorrectionRow materializes one correction piece: base content +
// patch, the piece's interval and TxFrom = now. Version and hash are set by
// the caller once the cascade's versions are final.
func (c *Core) buildNodeCorrectionRow(seg nodeCorrectionSegment, template *types.Node, now types.Instant, props map[string]any) (*types.Node, error) {
	row := seg.base.DeepCopy()
	for key, val := range props {
		if val == nil {
			if _, err := row.DeleteProperty(key); err != nil {
				return nil, fmt.Errorf("graph: cascade prop %q: %w", key, err)
			}
		} else if err := row.SetProperty(key, val); err != nil {
			return nil, fmt.Errorf("graph: cascade prop %q: %w", key, err)
		}
	}
	if row.PropertyCount() > c.validation.MaxPropertiesPerEntity {
		return nil, fmt.Errorf("%w: %d > %d", ErrTooManyProperties, row.PropertyCount(), c.validation.MaxPropertiesPerEntity)
	}
	row.SetTemporal(correctionTemporal(template.Temporal(), seg.from, seg.to, now))
	return row, nil
}

// buildRelCorrectionRow mirrors buildNodeCorrectionRow for relationships.
func (c *Core) buildRelCorrectionRow(seg relCorrectionSegment, template *types.Relationship, now types.Instant, props map[string]any) (*types.Relationship, error) {
	row := seg.base.DeepCopy()
	for key, val := range props {
		if val == nil {
			if _, err := row.DeleteProperty(key); err != nil {
				return nil, fmt.Errorf("graph: cascade prop %q: %w", key, err)
			}
		} else if err := row.SetProperty(key, val); err != nil {
			return nil, fmt.Errorf("graph: cascade prop %q: %w", key, err)
		}
	}
	if row.PropertyCount() > c.validation.MaxPropertiesPerEntity {
		return nil, fmt.Errorf("%w: %d > %d", ErrTooManyProperties, row.PropertyCount(), c.validation.MaxPropertiesPerEntity)
	}
	row.SetTemporal(correctionTemporal(template.Temporal(), seg.from, seg.to, now))
	return row, nil
}

func ensureNodeTemporal(n *types.Node) {
	if n.Temporal() == nil {
		n.SetTemporal(&types.TemporalMetadata{})
	}
}

func ensureRelTemporal(r *types.Relationship) {
	if r.Temporal() == nil {
		r.SetTemporal(&types.TemporalMetadata{})
	}
}

// =============================================================================
// Relationship cascade (mirror of node cascade, rule 2 parity)
// =============================================================================

func (c *Core) cascadeRelVersionInterval(ctx context.Context, id types.RelID, newVF, newVT types.Instant, props map[string]any, at types.Instant) (*types.Relationship, error) {
	return c.cascadeRel(ctx, id, newVF, newVT, props, at, false)
}

// cascadeRel mirrors cascadeNode for relationships.
func (c *Core) cascadeRel(ctx context.Context, id types.RelID, newVF, newVT types.Instant, props map[string]any, at types.Instant, void bool) (*types.Relationship, error) {
	return c.cascadeRelMode(ctx, id, newVF, newVT, props, at, cascadeModeOf(void), nil)
}

// cascadeRelMode is the rel cascade kernel; mode selects a correction, an
// excise (cascadeVoid) or a whole-axis untimed write (cascadeExisting:
// pieces only where a version is valid, unchanged pieces skipped; prov, when
// set, is the update's provenance for every appended row).
func (c *Core) cascadeRelMode(ctx context.Context, id types.RelID, newVF, newVT types.Instant, props map[string]any, at types.Instant, mode cascadeMode, prov *updateProvenance) (*types.Relationship, error) {
	void, existing := mode == cascadeVoid, mode == cascadeExisting
	if err := storepkg.ValidateRelID(id); err != nil {
		return nil, err
	}
	if err := checkCascadeInterval(c.axis(), newVF, newVT); err != nil {
		return nil, err
	}
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	c.entityLocks.LockEntity(id.SnowflakeID())
	defer c.entityLocks.UnlockEntity(id.SnowflakeID())

	if err := checkCtx(ctx); err != nil {
		return nil, err
	}
	if err := c.checkpointDirtyRegistriesBeforeMutation("cascade rel"); err != nil {
		return nil, err
	}

	current, err := c.getCurrentRelationship(id)
	if err != nil && !errors.Is(err, storepkg.ErrRelNotFound) {
		return nil, err
	}
	// The head answer before any write — see the node cascade.
	headBefore := c.relHeadBeforeWrite(id, current)
	// From the current row or the head entry — see the node cascade.
	above, stamps := func(v uint32) (bool, error) { return c.relHasHistoryAbove(id, v) }, c.relHistoryStampsFunc(id)
	if headBefore != nil {
		above, stamps = headBefore.above, headBefore.stamps // see the node cascade
	}
	hc, fromCurrent := chainFromCurrent(c, relKernel, current, above, stamps, newVF, newVT, at, void)
	fromHead := fromCurrent
	if !fromCurrent {
		hc, fromHead = chainFromHead(c, headBefore, current, func(v uint32) (*types.Relationship, error) { return c.getRelVersion(id, v) },
			newVF, newVT, at, void || existing)
	}
	var history []*types.Relationship
	if !fromHead {
		if history, err = c.getRelHistory(id); err != nil {
			return nil, err
		}
		if current == nil && len(history) == 0 {
			return nil, storepkg.ErrRelNotFound
		}
		if current == nil {
			// Hard-deleted — see the node cascade (backlog 14).
			return nil, relDeletedErr(id)
		}
	}

	maxVersion := hc.maxVersion
	var chainStamps []*types.TemporalMetadata // from the head: the order rule was checked
	if !fromHead {
		maxVersion = current.Version()
		for _, h := range history {
			maxVersion = max(maxVersion, h.Version())
		}
		chainStamps = temporalsOf(history, current)
	}
	nextVersion, err := nextEntityVersion(maxVersion)
	if err != nil {
		return nil, err
	}
	// The clock or the checked caller instant — see the node cascade.
	now, err := c.cascadeInstant(at, chainStamps)
	if err != nil {
		return nil, err
	}

	// Template: the current row — see the node cascade.
	template := current

	// APPEND-ONLY (audited correction) — mirror of cascadeNodeVersionInterval.
	// Never mutate an existing row's stored interval or transaction stamps;
	// append fresh-TxFrom rows and let the resolver tile by valid-from with the
	// newer belief winning on overlap. See the node cascade for the rationale.
	var preChain []*types.Relationship
	if !fromHead {
		preChain = preCorrectionChain(history, current) // a deleted entity was refused above
	}

	// BACKLOG 10b: explicit resumption ValidTo via relResumptionEnd — see the
	// node cascade above for the full rationale (own-interval boundary scan,
	// not "positionally-next row after src").
	appended := make([]*types.Relationship, 0, 4)
	prevHashes := make([]string, 0, 4)
	var prep *correctionChain[*types.Relationship] // see the node cascade (row 85)
	if !fromHead {
		prep = prepareCorrection(c, relKernel, preChain)
		if headBefore == nil {
			headBefore = prep.head(c.relHistoryEpoch(id), current)
		}
		if !c.axis().Open(newVT) {
			if src, ok := prep.winnerAt(newVT); ok {
				hc.src, hc.hasSrc, hc.srcEnd = src, true, relResumptionEnd(c, preChain, newVT)
			}
		}
	}
	if hc.hasSrc && !c.relStartsAt(hc.src, newVT) { // RESUME: as for nodes
		resumption := hc.src.DeepCopy()
		ensureRelTemporal(resumption)
		stampValidFrom(c.axis(), resumption.Temporal(), newVT, true)
		resumption.Temporal().ValidTo = hc.srcEnd
		stampAppendedRow(resumption.Temporal(), now)
		appended = append(appended, resumption)
		prevHashes = append(prevHashes, relHashOf(hc.src))
	}

	// Patch over the then-valid state, piece by piece — mirror of the node
	// cascade (see correctionChain.segments). Endpoints and type are immutable,
	// so every base row carries the same ones; only properties differ.
	segments := hc.segments
	if !fromHead {
		segments = prep.segments(template, newVF, newVT, void || existing)
	}
	if void {
		if err := exciseNoVersionErr(segments, newVF, newVT); err != nil {
			return nil, err
		}
	}
	var newVer *types.Relationship // the appended row covering newVF — the return value
	sup := wholeAxisSupersession{voidFree: hc.voidFree}
	if !fromHead {
		sup.voidFree = !slices.ContainsFunc(preChain, isVoidRow[*types.Relationship])
	}
	for _, seg := range segments {
		if (void || existing) && seg.gap {
			sup.note(true, false)
			continue // nothing valid there: no absence to record, no existence to add
		}
		if existing && !relPreparedUpdateMutates(seg.base, provOrZero(prov), updateTemporal{}, props) {
			sup.note(false, false)
			continue // the segment's version already holds the change
		}
		sup.note(false, true)
		row, err := c.buildRelCorrectionRow(seg, template, now, props)
		if err != nil {
			return nil, err
		}
		if void {
			if err := markVoid(row); err != nil {
				return nil, err
			}
		}
		if newVer == nil {
			newVer = row
		}
		appended = append(appended, row)
		prevHashes = append(prevHashes, relHashOf(seg.base))
	}
	if existing && len(appended) == 0 {
		return nil, nil // no segment changes: the update is a no-op
	}
	// Provisional versions — see the node cascade.
	firstVersion := nextVersion
	for i, r := range appended {
		if i > 0 {
			if nextVersion, err = nextEntityVersion(nextVersion); err != nil {
				return nil, err
			}
		}
		r.SetVersion(nextVersion)
	}

	// The new "current" row: see the node cascade (cascadeNewCurrent).
	newCurrent := cascadeNewCurrent(c, relKernel, relBeliefNewerThan, relHashOf, fromHead, hc.open, preChain, appended, prevHashes, current, existing)

	// Final versions: the row taking the current slot is the highest — see the
	// node cascade (backlog 18).
	curIsNew := moveToEnd(appended, prevHashes, newCurrent)
	for i, r := range appended {
		r.SetVersion(firstVersion + uint32(i)) // #nosec G115 -- bounded by the provisional allocation above
		if err := c.hashAppendedRelRow(r, prevHashes[i]); err != nil {
			return nil, err
		}
		applyRelProvenance(r.Integrity(), prov)
	}
	for _, r := range appended {
		if r == newCurrent {
			continue
		}
		if err := c.putRelVersionScopedAware(ctx, id, r.Version(), r); err != nil {
			return nil, fmt.Errorf("graph: cascade put appended rel version: %w", err)
		}
	}
	if curIsNew {
		demoted := current
		if existing && sup.replaces(current.Temporal(), now) {
			demoted = current.DeepCopy()
			ensureRelTemporal(demoted)
			demoted.Temporal().TxTo = now
		}
		if err := c.putRelVersionScopedAware(ctx, id, demoted.Version(), demoted); err != nil {
			return nil, fmt.Errorf("graph: cascade demote rel current: %w", err)
		}
		if err := c.replaceRelationshipScopedAware(ctx, newCurrent); err != nil {
			return nil, fmt.Errorf("graph: cascade replace rel current: %w", err)
		}
	}

	headCur := current
	if curIsNew {
		headCur = newCurrent
	}
	c.relHeadAfterWrite(id, headBefore, appended, headCur)
	switch {
	case fromCurrent:
		c.appendWrites.Add(1)
	case fromHead:
		c.headWrites.Add(1)
	}
	c.opRelUpdates.Add(1)
	return newVer, nil
}

// nodeDeletedErr is the refusal of a cascade on a hard-deleted node: it wraps
// ErrEntityDeleted and ErrNodeNotFound (backlog 14).
func nodeDeletedErr(id types.NodeID) error {
	return fmt.Errorf("%w: node %v: %w", ErrEntityDeleted, id, storepkg.ErrNodeNotFound)
}

// relDeletedErr mirrors nodeDeletedErr for relationships.
func relDeletedErr(id types.RelID) error {
	return fmt.Errorf("%w: relationship %v: %w", ErrEntityDeleted, id, storepkg.ErrRelNotFound)
}

// cascadeMissingNodeErr classifies a cascade target without a current row:
// ErrNodeNotFound when the ID never existed (no history), nodeDeletedErr when
// it was hard-deleted. A door that looks the current row up before the
// cascade kernel (the GraphTx snapshot) uses it, so every door refuses alike.
func (c *Core) cascadeMissingNodeErr(id types.NodeID) error {
	history, err := c.getNodeHistory(id)
	if err != nil {
		return err
	}
	if len(history) == 0 {
		return storepkg.ErrNodeNotFound
	}
	return nodeDeletedErr(id)
}

// cascadeMissingRelErr mirrors cascadeMissingNodeErr for relationships.
func (c *Core) cascadeMissingRelErr(id types.RelID) error {
	history, err := c.getRelHistory(id)
	if err != nil {
		return err
	}
	if len(history) == 0 {
		return storepkg.ErrRelNotFound
	}
	return relDeletedErr(id)
}

// cascadeMode selects what the cascade kernel writes.
type cascadeMode uint8

const (
	cascadeCorrection cascadeMode = iota // SetVersionInterval: [newVF, newVT) holds props
	cascadeVoid                          // excise: void pieces where a version is valid
	cascadeExisting                      // whole-axis untimed write: every valid segment patched (round 11)
)

func cascadeModeOf(void bool) cascadeMode {
	if void {
		return cascadeVoid
	}
	return cascadeCorrection
}

func provOrZero(prov *updateProvenance) updateProvenance {
	if prov == nil {
		return updateProvenance{}
	}
	return *prov
}

// applyNodeProvenance sets an update's caller provenance on an appended row's
// integrity, as the update door does (the fields are not hashed).
func applyNodeProvenance(ig *types.NodeIntegrity, prov *updateProvenance) {
	if ig == nil || prov == nil {
		return
	}
	if prov.hasAuthorID {
		ig.AuthorID = prov.authorID
	}
	if prov.hasSignature {
		ig.Signature = prov.signature
	}
	if prov.hasAuthorizedBy {
		ig.AuthorizedBy = prov.authorizedBy
	}
	if prov.hasAuthLevel {
		ig.AuthorizationLevel = prov.authLevel
	}
}

// applyRelProvenance mirrors applyNodeProvenance for relationships.
func applyRelProvenance(ig *types.RelIntegrity, prov *updateProvenance) {
	if ig == nil || prov == nil {
		return
	}
	if prov.hasAuthorID {
		ig.AuthorID = prov.authorID
	}
	if prov.hasSignature {
		ig.Signature = prov.signature
	}
	if prov.hasAuthorizedBy {
		ig.AuthorizedBy = prov.authorizedBy
	}
	if prov.hasAuthLevel {
		ig.AuthorizationLevel = prov.authLevel
	}
}

// checkCascadeInterval is the cascade kernels' interval check, on the
// graph's valid axis a (round 11): newVF must be a claimed start (the legacy
// axis's 0 is "unset") and a bounded end must follow it; an open end is the
// whole tail from newVF, an excise's included (OPENEXCISE).
func checkCascadeInterval(a types.ValidAxis, newVF, newVT types.Instant) error {
	if !a.Claimed(newVF) {
		return fmt.Errorf("%w: cascade requires a claimed validFrom (non-zero on the legacy valid axis)", ErrInvalidTimeRange)
	}
	if !a.Open(newVT) && newVF >= newVT {
		return fmt.Errorf("%w: validFrom %d >= validTo %d", ErrInvalidTimeRange, newVF, newVT)
	}
	return nil
}
