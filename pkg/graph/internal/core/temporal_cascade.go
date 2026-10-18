package core

import (
	"context"
	"errors"
	"fmt"
	"slices"

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
//     content plus the patch (nodeCorrectionSegments);
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
// (resolveNodeVersionAt on a pristine copy of the pre-correction chain) — the
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
// history with its bytes unchanged. If no appended row takes the slot, the
// current row keeps it, below the appended rows' versions.
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
	return c.cascadeNodeMode(ctx, id, newVF, newVT, props, at, cascadeModeOf(void), nil)
}

// cascadeNodeMode is the node cascade kernel; mode selects a correction, an
// excise (cascadeVoid) or a whole-axis untimed write (cascadeExisting:
// pieces only where a version is valid, unchanged pieces skipped; prov, when
// set, is the update's provenance for every appended row).
func (c *Core) cascadeNodeMode(ctx context.Context, id types.NodeID, newVF, newVT types.Instant, props map[string]any, at types.Instant, mode cascadeMode, prov *updateProvenance) (*types.Node, error) {
	void, existing := mode == cascadeVoid, mode == cascadeExisting
	if err := storepkg.ValidateNodeID(id); err != nil {
		return nil, err
	}
	if err := checkCascadeInterval(c.axis(), newVF, newVT, void); err != nil {
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
	hc, fromCurrent := chainFromCurrent(c, nodeKernel, current, func(v uint32) (bool, error) { return c.nodeHasHistoryAbove(id, v) },
		c.nodeHistoryStampsFunc(id), newVF, newVT, at, void)
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
	var stamps []*types.TemporalMetadata // from the head: the order rule was checked
	if !fromHead {
		maxVersion = current.Version()
		for _, h := range history {
			maxVersion = max(maxVersion, h.Version())
		}
		stamps = temporalsOf(history, current)
	}
	nextVersion, err := nextEntityVersion(maxVersion)
	if err != nil {
		return nil, err
	}
	// The clock, or the caller instant at once the order rule holds for the
	// whole chain (interval_withtx.go). Every appended row is stamped with it.
	now, err := c.cascadeInstant(at, stamps)
	if err != nil {
		return nil, err
	}

	// The template — the current row, the most recent version — is the
	// content of a gap piece, where no version is valid to patch (see
	// nodeCorrectionSegments).
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
	if !c.axis().Open(newVT) && !fromHead {
		if src, err := c.resolveNodeVersionAt(append([]*types.Node(nil), preChain...), newVT); err == nil && src != nil {
			hc.src, hc.hasSrc, hc.srcEnd = src, true, nodeResumptionEnd(c, preChain, newVT)
		} else if err != nil && !errors.Is(err, storepkg.ErrNoVersionValidAt) {
			return nil, fmt.Errorf("graph: cascade resolve resumption source: %w", err)
		}
	}
	if hc.hasSrc {
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
	// nodeCorrectionSegments). Rows are fully built (patched, bounded, hashed)
	// before any write — preflight, then apply.
	segments := hc.segments
	if !fromHead {
		if segments, err = c.nodeCorrectionSegments(preChain, template, newVF, newVT, void || existing); err != nil {
			return nil, err
		}
	}
	if void {
		if err := exciseNoVersionErr(segments, newVF, newVT); err != nil {
			return nil, err
		}
	}
	var newVer *types.Node // the appended row covering newVF — the return value
	sup := wholeAxisSupersession{voidFree: !fromHead && !slices.ContainsFunc(preChain, isVoidRow[*types.Node])}
	for _, seg := range segments {
		if (void || existing) && seg.gap {
			sup.note(true, false)
			continue // nothing valid there: no absence to record, no existence to add
		}
		if existing && !nodePreparedUpdateMutates(seg.base, provOrZero(prov), updateTemporal{}, props) {
			sup.note(false, false)
			continue // the segment's version already holds the change
		}
		sup.note(false, true)
		row, err := c.buildNodeCorrectionRow(seg, template, now, props)
		if err != nil {
			return nil, err
		}
		if void {
			row.Temporal().Void = true
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

	// The new "current" row is the NEWEST BELIEF among rows whose OWN interval
	// is open-ended (the value at "now" — resolving the point query at +inf).
	// BACKLOG 10b: this is deliberately NOT "the last positionally-open row in
	// valid-from order" — a row's own-open status must never be decided by its
	// neighbors, or an untouched older open row can wrongly keep the "current"
	// slot from a newer, wider-reaching open correction. See nodeOwnBounds.
	var newCurrent *types.Node
	if fromHead {
		newCurrent = headNewCurrent(c, nodeKernel, hc.open, appended)
	} else {
		postChain := make([]*types.Node, 0, len(preChain)+len(appended))
		postChain = append(postChain, preChain...)
		postChain = append(postChain, appended...)
		for i := range postChain {
			entry := postChain[i]
			if _, vEnd := c.nodeOwnBounds(entry); !c.axis().Open(vEnd) || isVoidRow(entry) {
				continue // only own-open rows can own the current slot
			}
			if newCurrent == nil || nodeBeliefNewerThan(entry, newCurrent) {
				newCurrent = entry
			}
		}
	}

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
	if curIsNew {
		// The store's current slot cannot change label tokens (ReplaceNode
		// rejects it). A correction row's labels come from its base row, and
		// the only row that can take the slot is the open tail, whose base is
		// the pre-correction current itself (or the template, for a gap) — so
		// this holds by construction. Checked here, BEFORE any write, so a
		// violation surfaces as an error instead of a half-applied cascade.
		if err := storepkg.ValidateNodeReplacement(current, newCurrent); err != nil {
			return nil, fmt.Errorf("graph: cascade new current row: %w", err)
		}
	}
	// Unique constraints (unique_cascade.go): the built rows are judged after
	// every kernel check and before the first write; the value stripes stay
	// held across every write below.
	// A failed write withdraws the UniqueForever claims no written row carries.
	uniqueHold, err := c.enforceUniqueForCascade(id, current, appended, newCurrent, curIsNew, newVT, props)
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
		if err := c.replaceNodeScopedAware(ctx, newCurrent); err != nil {
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

// nodeCorrectionSegments splits [newVF, newVT) at every boundary where the
// pre-correction belief-winner can change and pairs each piece with its base.
//
// Cut points are every row's OWN vStart/vEnd (the cascade-arm
// bounds, as in nodeResumptionEnd) plus its positional bounds (the monotonic
// arm's tiling, which on a legacy pre-migration chain can start a row at
// UpdatedAt rather than ValidFrom). Extra cuts are harmless — pieces with the
// same base are merged — while a missing cut would patch the wrong base.
//
// The base of a piece is resolveNodeVersionAt on a fresh copy of the pristine
// ascending-version preChain (the resolver sorts in place and classifies by
// input order — lesson 73). Where NO version is valid (a gap: before the
// entity's first valid-from, after a close, …) there is no then-valid state
// to patch, so the piece's base is the template (the most
// recent version). That is a choice, not a derivation — a gap piece asserts a
// state the entity was never believed to have, and the most recent state is
// the least surprising content for it.
func (c *Core) nodeCorrectionSegments(preChain []*types.Node, template *types.Node, newVF, newVT types.Instant, splitGaps bool) ([]nodeCorrectionSegment, error) {
	bounds := make([]types.Instant, 0, 4*len(preChain))
	for i, row := range preChain {
		vStart, vEnd := c.nodeOwnBounds(row)
		pStart, pEnd := c.nodeVersionBounds(preChain, i) // cut points only
		bounds = append(bounds, vStart, vEnd, pStart, pEnd)
	}
	cuts := correctionCuts(c.axis(), bounds, newVF, newVT)

	segs := make([]nodeCorrectionSegment, 0, len(cuts)+1)
	from := newVF
	for i := 0; i <= len(cuts); i++ {
		to := newVT
		if i < len(cuts) {
			to = cuts[i]
		}
		base, err := c.resolveNodeVersionAt(append([]*types.Node(nil), preChain...), from)
		gap := errors.Is(err, storepkg.ErrNoVersionValidAt)
		if gap {
			base, err = template, nil
		}
		if err != nil {
			return nil, fmt.Errorf("graph: cascade resolve correction base at %d: %w", from, err)
		}
		if n := len(segs); n > 0 && segs[n-1].base == base && (!splitGaps || segs[n-1].gap == gap) {
			segs[n-1].to = to
		} else {
			segs = append(segs, nodeCorrectionSegment{from: from, to: to, base: base, gap: gap})
		}
		from = to
	}
	return segs, nil
}

// relCorrectionSegments mirrors nodeCorrectionSegments for relationships.
func (c *Core) relCorrectionSegments(preChain []*types.Relationship, template *types.Relationship, newVF, newVT types.Instant, splitGaps bool) ([]relCorrectionSegment, error) {
	bounds := make([]types.Instant, 0, 4*len(preChain))
	for i, row := range preChain {
		vStart, vEnd := c.relOwnBounds(row)
		pStart, pEnd := c.relVersionBounds(preChain, i) // cut points only
		bounds = append(bounds, vStart, vEnd, pStart, pEnd)
	}
	cuts := correctionCuts(c.axis(), bounds, newVF, newVT)

	segs := make([]relCorrectionSegment, 0, len(cuts)+1)
	from := newVF
	for i := 0; i <= len(cuts); i++ {
		to := newVT
		if i < len(cuts) {
			to = cuts[i]
		}
		base, err := c.resolveRelVersionAt(append([]*types.Relationship(nil), preChain...), from)
		gap := errors.Is(err, storepkg.ErrNoVersionValidAt)
		if gap {
			base, err = template, nil
		}
		if err != nil {
			return nil, fmt.Errorf("graph: cascade resolve rel correction base at %d: %w", from, err)
		}
		if n := len(segs); n > 0 && segs[n-1].base == base && (!splitGaps || segs[n-1].gap == gap) {
			segs[n-1].to = to
		} else {
			segs = append(segs, relCorrectionSegment{from: from, to: to, base: base, gap: gap})
		}
		from = to
	}
	return segs, nil
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
	if err := checkCascadeInterval(c.axis(), newVF, newVT, void); err != nil {
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
	hc, fromCurrent := chainFromCurrent(c, relKernel, current, func(v uint32) (bool, error) { return c.relHasHistoryAbove(id, v) },
		c.relHistoryStampsFunc(id), newVF, newVT, at, void)
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
	var stamps []*types.TemporalMetadata // from the head: the order rule was checked
	if !fromHead {
		maxVersion = current.Version()
		for _, h := range history {
			maxVersion = max(maxVersion, h.Version())
		}
		stamps = temporalsOf(history, current)
	}
	nextVersion, err := nextEntityVersion(maxVersion)
	if err != nil {
		return nil, err
	}
	// The clock or the checked caller instant — see the node cascade.
	now, err := c.cascadeInstant(at, stamps)
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
	if !c.axis().Open(newVT) && !fromHead {
		if src, err := c.resolveRelVersionAt(append([]*types.Relationship(nil), preChain...), newVT); err == nil && src != nil {
			hc.src, hc.hasSrc, hc.srcEnd = src, true, relResumptionEnd(c, preChain, newVT)
		} else if err != nil && !errors.Is(err, storepkg.ErrNoVersionValidAt) {
			return nil, fmt.Errorf("graph: cascade resolve rel resumption source: %w", err)
		}
	}
	if hc.hasSrc {
		resumption := hc.src.DeepCopy()
		ensureRelTemporal(resumption)
		stampValidFrom(c.axis(), resumption.Temporal(), newVT, true)
		resumption.Temporal().ValidTo = hc.srcEnd
		stampAppendedRow(resumption.Temporal(), now)
		appended = append(appended, resumption)
		prevHashes = append(prevHashes, relHashOf(hc.src))
	}

	// Patch over the then-valid state, piece by piece — mirror of the node
	// cascade (see nodeCorrectionSegments). Endpoints and type are immutable,
	// so every base row carries the same ones; only properties differ.
	segments := hc.segments
	if !fromHead {
		if segments, err = c.relCorrectionSegments(preChain, template, newVF, newVT, void || existing); err != nil {
			return nil, err
		}
	}
	if void {
		if err := exciseNoVersionErr(segments, newVF, newVT); err != nil {
			return nil, err
		}
	}
	var newVer *types.Relationship // the appended row covering newVF — the return value
	sup := wholeAxisSupersession{voidFree: !fromHead && !slices.ContainsFunc(preChain, isVoidRow[*types.Relationship])}
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
			row.Temporal().Void = true
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

	// BACKLOG 10b: newest-belief-among-own-open — see the node cascade above.
	var newCurrent *types.Relationship
	if fromHead {
		newCurrent = headNewCurrent(c, relKernel, hc.open, appended)
	} else {
		postChain := make([]*types.Relationship, 0, len(preChain)+len(appended))
		postChain = append(postChain, preChain...)
		postChain = append(postChain, appended...)
		for i := range postChain {
			entry := postChain[i]
			if _, vEnd := c.relOwnBounds(entry); !c.axis().Open(vEnd) || isVoidRow(entry) {
				continue
			}
			if newCurrent == nil || relBeliefNewerThan(entry, newCurrent) {
				newCurrent = entry
			}
		}
	}

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
// axis's 0 is "unset"), an excise needs a bounded end, and a bounded end must
// follow the start.
func checkCascadeInterval(a types.ValidAxis, newVF, newVT types.Instant, void bool) error {
	if !a.Claimed(newVF) {
		return fmt.Errorf("%w: cascade requires a claimed validFrom (non-zero on the legacy valid axis)", ErrInvalidTimeRange)
	}
	if void && a.Open(newVT) {
		return fmt.Errorf("%w: excise requires a bounded end", ErrInvalidTimeRange)
	}
	if !a.Open(newVT) && newVF >= newVT {
		return fmt.Errorf("%w: validFrom %d >= validTo %d", ErrInvalidTimeRange, newVF, newVT)
	}
	return nil
}
