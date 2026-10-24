package core

import (
	"errors"
	"fmt"
	"math"
	"sort"

	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// =============================================================================
// Internal helpers
// =============================================================================

// resolveOpenEndInstant maps an open-ended `end == 0` upper bound to a
// concrete instant ("now + 1", now = c.readNow()) so a single per-query value
// is shared across every per-ID overlap predicate. Substituting at the entry
// point — rather than inside findNodeVersionMatchingDuring per
// invocation — eliminates time drift on long iterations, where each
// per-ID call would otherwise observe a different "now" and
// produce inclusion/exclusion that depends on iteration timing.
//
// Callers that hand `end` straight to findNodeVersionMatchingDuring /
// findRelVersionMatchingDuring MUST pass through this helper first.
func (c *Core) resolveOpenEndInstant(end types.Instant) types.Instant {
	if end == 0 {
		return c.readNow() + 1
	}
	return end
}

func (c *Core) normalizeDuringRange(start, end types.Instant) (types.Instant, error) {
	end = c.resolveOpenEndInstant(end)
	if start >= end {
		return 0, ErrInvalidTimeRange
	}
	return end, nil
}

// nodeValidFrom returns the effective valid-from of a node row: explicit
// ValidFrom, else its TxFrom (legacy: UpdatedAt, then the mint instant). It is
// the resolver's start AND its sort key, for a first and a later version alike
// (backlog 62). Delegates to storeutil.NodeValidFrom (the rule of
// storeutil.EntityValidFrom) — one definition shared with the store push-down,
// the temporal index envelopes and the column filters.
func (c *Core) nodeValidFrom(n *types.Node) types.Instant {
	return storeutil.NodeValidFrom(c.axis(), n)
}

// relValidFrom mirrors nodeValidFrom for relationships.
func (c *Core) relValidFrom(r *types.Relationship) types.Instant {
	return storeutil.RelValidFrom(c.axis(), r)
}

func validInstantAfter(at, start types.Instant) types.Instant {
	if at <= start {
		return start + 1
	}
	return at
}

func (c *Core) deleteInstantForNodeCascade(current *types.Node, outRels, inRels []*types.Relationship) types.Instant {
	at := validInstantAfter(c.now(), c.nodeValidFrom(current))
	for _, r := range outRels {
		at = validInstantAfter(at, c.relValidFrom(r))
	}
	for _, r := range inRels {
		at = validInstantAfter(at, c.relValidFrom(r))
	}
	tms := make([]*types.TemporalMetadata, 0, 1+len(outRels)+len(inRels))
	tms = append(tms, current.Temporal())
	for _, r := range outRels {
		tms = append(tms, r.Temporal())
	}
	for _, r := range inRels {
		tms = append(tms, r.Temporal())
	}
	return c.deleteInstantClearOfCloses(at, tms...)
}

func (c *Core) deleteInstantForRelationship(current *types.Relationship) types.Instant {
	return c.deleteInstantClearOfCloses(validInstantAfter(c.now(), c.relValidFrom(current)), current.Temporal())
}

// deleteInstantClearOfCloses moves a delete instant past any recorded close
// (ValidTo) it would coincide with. The pinned-read normalizers
// (normalizeTemporalVisibleAtTxTime, core and memory store) read
// "ValidTo == DeletedAt" as "the delete wrote this ValidTo" and reopen the row
// for a pin before the delete. stampDeleteTombstone keeps a close at or before
// the delete instant, so a close landing exactly ON it would be indistinguishable
// from a delete-stamped one; a later instant is not. Every row the delete
// tombstones shares one instant, so the loop runs until no row collides.
//
// The replacement instant comes from c.now(), so it is reserved on the
// transaction clock and no later write can be stamped with it. When `at` was
// already pushed past the clock by validInstantAfter (an explicit future
// valid-from), c.now() is below it and the candidate falls back to at+1: that
// path hands out unreserved instants today regardless of this helper (see the
// tasks/todo.md backlog 4 (ii)).
func (c *Core) deleteInstantClearOfCloses(at types.Instant, tms ...*types.TemporalMetadata) types.Instant {
	for moved := true; moved && at < math.MaxInt64; {
		moved = false
		for _, tm := range tms {
			if tm != nil && tm.ValidTo == at {
				at = max(at+1, c.now())
				moved = true
			}
		}
	}
	return at
}

// tombstoneSpec is how a delete door ends an entity (backlog 43): at is a
// caller transaction instant already gated by resolveCallerTxInstant (0 = the
// plain doors' clock instant), retract marks every tombstone the door writes
// as a retraction (types.TemporalMetadata.Retracted): belief in the life it
// ends ends at the tombstone's instant for every valid time. keepPast is the
// EndValidity doors' delete: the tombstone ends validity at its instant and
// keeps the valid past on every valid axis (the legacy axis's Delete), so it
// is never a retraction, also on an UntimedWholeAxis graph (deleteRetracts).
type tombstoneSpec struct {
	at       types.Instant
	retract  bool
	keepPast bool
}

// stampDeleteTombstone turns tm (the final version of an entity being hard
// deleted, already a private copy) into its delete tombstone at instant at.
// It is the ONE place the tombstone temporal stamps are decided; every delete
// door (standalone, tx, batch, ingest strong and concurrent mode, node cascade)
// funnels through deleteNodeLocked / deleteRelationshipInternal, which call it.
//
// ValidTo is stamped only when the row is still open (the open end of the
// graph's valid axis a) or scheduled to close after the delete (clamped to
// the delete). A close already recorded at or
// before the delete is kept: overwriting it rewrote valid-time history, and a
// reader pinned before the delete then reset the delete-stamped ValidTo to 0,
// reporting the closed entity as open-ended (a later write changing a
// historical answer). DeletedAt and TxTo are always the delete instant.
//
// clamped reports a close scheduled after the delete that the tombstone
// clamped: a valid end earlier pins read changed (backlog 25), which the
// caller reports to the pinned caches after its store write
// (noteClampedClose).
func stampDeleteTombstone(a types.ValidAxis, tm *types.TemporalMetadata, at types.Instant, retract bool) (clamped bool) {
	tm.DeletedAt = at
	tm.Retracted = retract
	open := a.Open(tm.ValidTo)
	clamped = !open && tm.ValidTo > at
	if open || tm.ValidTo > at {
		tm.ValidTo = at
	}
	if tm.TxFrom == 0 {
		tm.TxFrom = at
	}
	tm.TxTo = at
	return clamped
}

// supersession is what a plain superseding door (Update, the label and CAS
// doors, CloseVersion) stamps: tx, the transaction instant (TxFrom and
// UpdatedAt of the new row, TxTo of the superseded one), and from, the new
// row's valid start when it cannot start at tx (0: it starts at its TxFrom).
type supersession struct {
	tx, from types.Instant
	axis     types.ValidAxis // the graph's: whether the door claimed a start
}

// supersede is THE instant rule of a plain superseding write. tx is the
// clock, moved past the current row's own TxFrom only when that row was
// stamped ahead of the clock (a row stored before backlog 77, a re-import
// after a delete ahead of it), so the superseded row's TxTo never precedes its
// TxFrom. The new version must start after the current version's start (the
// one start rule, nodeValidFrom — backlog 62); when the clock is not past it
// (the current version starts in the future) that start is recorded as the
// row's ValidFrom, never as its TxFrom. Backlog 77: the start used to be
// stamped as TxFrom, a transaction instant ahead of the clock that no pin
// taken now reaches and that every later write on the entity is stamped below,
// so the unpinned point door and the head timeline answered different rows.
func (c *Core) supersede(start, recorded types.Instant) supersession {
	s := supersession{tx: validInstantAfter(c.now(), recorded), axis: c.axis()}
	if s.tx <= start {
		s.from = start + 1
	}
	return s
}

func (c *Core) nodeVersionUpdateInstant(current *types.Node) supersession {
	return c.supersede(c.nodeValidFrom(current), beliefTx(current.Temporal()))
}

func (c *Core) relVersionUpdateInstant(current *types.Relationship) supersession {
	return c.supersede(c.relValidFrom(current), beliefTx(current.Temporal()))
}

// stamp stamps the new row tm (its ValidFrom already set by the door: a
// caller's tkg_valid_from or a close's start, else the legacy axis's
// unclaimed 0) as appended at s.tx, starting at s.from when the door set no
// start. On the literal valid axis (UntimedWholeAxis) every door claims a
// start, so the stamp never derives one.
func (s supersession) stamp(tm *types.TemporalMetadata) {
	stampAppendedRow(tm, s.tx)
	if !s.axis.Claimed(tm.ValidFrom) {
		tm.ValidFrom = s.from
	}
}

// checkCloseAfterStart is the one valid-time close rule (backlog 65), shared by
// CloseVersion, an update that sets tkg_valid_to without tkg_valid_from, and
// UpdateInPlace: the version keeps its start and ends at end, so an end at or
// before that start is ErrInvalidTimeRange. what names the input in the error.
func checkCloseAfterStart(what string, end, start types.Instant, entity string) error {
	if end <= start {
		return fmt.Errorf("%w: %s %d must be after %s valid-from %d", ErrInvalidTimeRange, what, end, entity, start)
	}
	return nil
}

// checkNodeClose is the one close validation for a node version, shared by
// CloseVersion and every update door whose temporal input is a close (Update,
// UpdateWithTx, the GraphTx, batch and ingest twins): the version must be open
// (ErrAlreadyClosed), end must follow its start (checkCloseAfterStart) and the
// temporal constraints must hold for the closed row. It returns the start the
// closed row keeps (stampClose).
func (c *Core) checkNodeClose(id types.NodeID, current *types.Node, what string, end types.Instant) (types.Instant, error) {
	if tm := current.Temporal(); tm != nil && !c.axis().Open(tm.ValidTo) {
		return 0, ErrAlreadyClosed
	}
	start := c.nodeValidFrom(current)
	if err := checkCloseAfterStart(what, end, start, "node"); err != nil {
		return 0, err
	}
	if err := c.checkNodeCloseTemporalConstraints(id, current, end); err != nil {
		return 0, err
	}
	return start, nil
}

// checkRelClose is checkNodeClose for a relationship (endpoints locked by the
// caller).
func (c *Core) checkRelClose(current *types.Relationship, startID, endID types.NodeID, what string, end types.Instant) (types.Instant, error) {
	if tm := current.Temporal(); tm != nil && !c.axis().Open(tm.ValidTo) {
		return 0, ErrAlreadyClosed
	}
	start := c.relValidFrom(current)
	if err := checkCloseAfterStart(what, end, start, "relationship"); err != nil {
		return 0, err
	}
	if c.constraints.Len() > 0 {
		liveStart, liveEnd, err := c.liveEndpointNodes(startID, endID)
		if err != nil {
			return 0, err
		}
		if err := c.checkTemporalConstraints(relWithValidTo(current, end), liveStart, liveEnd); err != nil {
			return 0, err
		}
	}
	return start, nil
}

// stampClose writes a close onto the closing row: it keeps the version's start
// (stamped, since its own TxFrom is the close instant: a claim, the instant 0
// included) and ends at end.
func stampClose(tm *types.TemporalMetadata, start, end types.Instant) {
	tm.ValidFrom, tm.ValidTo = start, end
}

// closesVersion reports whether an update's temporal input is a close: a
// tkg_valid_to that is not the open end of the graph's valid axis a, without
// a claimed tkg_valid_from (parseValidClaim: on the legacy axis a plain 0 is
// no claim) — the version keeps its start (backlog 65).
func (tmp updateTemporal) closesVersion(a types.ValidAxis) bool {
	return tmp.hasValidTo && !a.Open(tmp.validTo) && !tmp.validFromSet
}

// isClosedTemporal reports whether tm's version has a valid end (not the
// open end of the valid axis a).
func isClosedTemporal(a types.ValidAxis, tm *types.TemporalMetadata) bool {
	return tm != nil && !a.Open(tm.ValidTo)
}

func (c *Core) rejectClosedNodeMutation(n *types.Node) error {
	if isClosedTemporal(c.axis(), n.Temporal()) {
		return ErrAlreadyClosed
	}
	return nil
}

func (c *Core) rejectClosedRelMutation(r *types.Relationship) error {
	if isClosedTemporal(c.axis(), r.Temporal()) {
		return ErrAlreadyClosed
	}
	return nil
}

// isNodeValidAt checks if a node is valid at the given instant.
// Valid when: effectiveValidFrom <= t AND (ValidTo open OR ValidTo > t).
// Delegates to the canonical predicate in storeutil — see nodeValidFrom.
func (c *Core) isNodeValidAt(n *types.Node, t types.Instant) bool {
	return storeutil.MatchesPointInTime(c.axis(), n.ID().SnowflakeID(), n.Temporal(), t)
}

// NodeMatchesValidTime reports whether n passes the valid-time filter in opts
// (ValidAt point, ValidStart/ValidEnd interval, or no filter), using the SAME
// canonical predicate (storeutil.MatchesTemporalFilterFrom) the store push-down
// and RelMatchesValidTime use, with the row's start taken as the chain resolver
// takes it (nodeValidFrom: explicit ValidFrom, else TxFrom — backlog 62).
// Exposed so a consumer that already holds a materialized node (e.g. a query
// engine post-filtering a traversal) decides validity as NodeAt does — never
// from the raw shadow value. This is the NODE mirror of RelMatchesValidTime
// (rule 17: two structural mirrors, same shape); a frozen node (returned by
// any plural/scan read, see the Defensive Copying design rule) is read-only
// here so it is accepted exactly like a mutable one. A nil node never matches.
func (t *TempOps) NodeMatchesValidTime(n *types.Node, opts storepkg.QueryOpts) bool {
	if n == nil {
		return false
	}
	return storeutil.MatchesTemporalFilterFrom(t.c.axis(), t.c.nodeValidFrom(n), n.Temporal(), opts)
}

// RelMatchesValidTime reports whether r passes the valid-time filter in opts
// (ValidAt point, ValidStart/ValidEnd interval, or no filter), using the SAME
// canonical predicate the store push-down uses. Exposed so a consumer that already holds a materialized relationship (e.g. a
// query engine post-filtering a traversal) decides validity with the start the
// chain resolver gives the row (relValidFrom, see NodeMatchesValidTime) —
// never the raw shadow value — keeping relationship valid-time semantics
// identical to the node path and RelAt. A nil relationship never matches.
func (t *TempOps) RelMatchesValidTime(r *types.Relationship, opts storepkg.QueryOpts) bool {
	if r == nil {
		return false
	}
	return storeutil.MatchesTemporalFilterFrom(t.c.axis(), t.c.relValidFrom(r), r.Temporal(), opts)
}

// resolveNodeVersionAt finds the version valid at time t from a pre-built chain.
func (c *Core) resolveNodeVersionAt(chain []*types.Node, t types.Instant) (*types.Node, error) {
	return c.resolveNodeVersionAtCapped(chain, t, nil)
}

// resolveNodeVersionAtCapped is resolveNodeVersionAt with every row's valid end
// capped at its life end (lifeEnds; nil caps nothing).
func (c *Core) resolveNodeVersionAtCapped(chain []*types.Node, t types.Instant, caps lifeEnds[*types.Node]) (*types.Node, error) {
	// Tiling histories (the overwhelming common case — Update rejects
	// backdated valid-from) take the positional arm: newest first, the first
	// covering version, O(1) for a current-state query, no belief scan. Every
	// other chain (a cascade correction, a close in place: SAMESTART) takes
	// the own-bounds arm and pays the sort + full scan.
	if !c.sortNodeChainForResolve(chain) {
		return c.resolveNodePositional(chain, t, caps)
	}
	return c.resolveNodeOwnBounds(chain, t, caps)
}

// resolveNodePositional is the resolver's fast arm for a tiling chain
// (sortNodeChainForResolve false): newest first, the first row whose
// positional interval covers t.
func (c *Core) resolveNodePositional(chain []*types.Node, t types.Instant, caps lifeEnds[*types.Node]) (*types.Node, error) {
	ax := c.axis()
	start := len(chain) - 1
	if c.bitemporalMigrated {
		// BACKLOG 10m: binary search for the backward scan's STARTING
		// index instead of always starting at the end — an old t would
		// otherwise force an O(n) walk past every newer version. Safe
		// because when bitemporalMigrated is true, nodeVersionBounds'
		// vStart is PROVABLY IDENTICAL to nodeValidFrom's — the very
		// function sortNodeChainForResolve just used to confirm the
		// chain is monotonic — so vStart is non-decreasing across the
		// array, exactly binary search's precondition. (Pre-migration,
		// the legacy inherited-ValidFrom heuristic can make them
		// diverge for an inherited entry, breaking that precondition —
		// why this is gated on bitemporalMigrated instead of applied
		// unconditionally.) idx is the first index whose vStart exceeds
		// t; every entry from idx onward provably cannot cover t, so
		// the UNCHANGED backward scan below can start one before it.
		idx := sort.Search(len(chain), func(i int) bool {
			vStart, _ := c.nodeVersionBounds(chain, i)
			return vStart > t
		})
		start = idx - 1
	}
	for i := start; i >= 0; i-- {
		entry := chain[i]
		vStart, vEnd := c.nodeVersionBounds(chain, i)
		vEnd = caps.end(ax, entry, vEnd)
		if storeutil.IntervalCovers(ax, vStart, vEnd, t) {
			if isVoidRow(entry) {
				return nil, storepkg.ErrNoVersionValidAt // an excised piece wins: absent
			}
			return entry, nil
		}
	}
	return nil, storepkg.ErrNoVersionValidAt
}

// resolveNodeOwnBounds is the resolver's own-bounds arm: every row's own
// interval capped by supersession, the newest belief covering t wins. It is
// the reference rule; the positional arm must agree with it on every chain
// sortNodeChainForResolve lets through (TestBitemporalOracleHarness).
func (c *Core) resolveNodeOwnBounds(chain []*types.Node, t types.Instant, caps lifeEnds[*types.Node]) (*types.Node, error) {
	ax := c.axis()
	// Cascade chain: effective intervals can overlap (an older, wider-valid row
	// under a newer, narrower correction), so the newer BELIEF wins — higher
	// TxFrom, then version. BACKLOG 10b: bounds here MUST be each row's own
	// asserted interval (nodeOwnBounds), never the positional next-neighbor
	// bound (nodeVersionBounds) — an untouched older row's ValidFrom must
	// never truncate a newer, wider-reaching correction. See nodeOwnBounds'
	// doc comment.
	//
	// A replacing write (Update, CloseVersion, …) ends every older belief
	// that started at or before its row's start, there (supersessionCaps).
	sc := getSupersessionScratch()
	defer putSupersessionScratch(sc)
	superseded := supersessionCaps(chain, c.nodeValidFrom, sc)
	starts := sc.startsOf(len(chain)) // nodeOwnBounds' start, computed once
	var best *types.Node
	for i := range chain {
		entry := chain[i]
		vStart, vEnd := starts[i], ax.OpenEnd() // nodeOwnBounds: own ValidTo, open by the axis
		if tm := entry.Temporal(); tm != nil {
			vEnd = tm.ValidTo
		}
		var empty bool
		if vEnd, empty = superseded.end(ax, i, vStart, vEnd); empty {
			continue // a newer belief replaced it from its start on
		}
		vEnd = caps.end(ax, entry, vEnd)
		if storeutil.IntervalCovers(ax, vStart, vEnd, t) {
			if best == nil || nodeBeliefNewerThan(entry, best) {
				best = entry
			}
		}
	}
	if best == nil || isVoidRow(best) {
		return nil, storepkg.ErrNoVersionValidAt
	}
	return best, nil
}

// beliefTx is a version's transaction-time belief instant (0 when unstamped).
func beliefTx(tm *types.TemporalMetadata) types.Instant {
	if tm == nil {
		return 0
	}
	return tm.TxFrom
}

// nodeBeliefNewerThan reports whether version a represents a strictly later
// BELIEF than b: higher TxFrom, or equal TxFrom and higher version. The
// tiebreaker on equal TxFrom (two rows recorded in the same millisecond) keeps
// selection deterministic across backends.
func nodeBeliefNewerThan(a, b *types.Node) bool {
	ta, tb := beliefTx(a.Temporal()), beliefTx(b.Temporal())
	if ta != tb {
		return ta > tb
	}
	return a.Version() > b.Version()
}

// sortNodeChainForResolve orders a (txAt-filtered) chain by effective valid-from
// ascending, version ascending — the order nodeVersionBounds' next-version
// tiling assumes. It returns true when the resolver must take its own-bounds
// arm: the chain had to be reordered (only an append-only cascade produces
// that — Update rejects backdated valid-from), or two rows sharing a start do
// not tile (tilesWithNext, SAMESTART). A tiling chain is detected by an O(n)
// scan and returns false without sorting.
func (c *Core) sortNodeChainForResolve(chain []*types.Node) bool {
	ax := c.axis() // once per chain, not per row
	tiles := true
	for i := 1; i < len(chain); i++ {
		pa, pb := ax.NodeStart(chain[i-1]), ax.NodeStart(chain[i])
		if tiles && !tilesWithNext(ax, chain[i-1].Temporal(), chain[i].Temporal(), pa, pb) {
			tiles = false
		}
		if pa > pb || (pa == pb && chain[i-1].Version() > chain[i].Version()) {
			sort.SliceStable(chain, func(a, b int) bool {
				va, vb := ax.NodeStart(chain[a]), ax.NodeStart(chain[b])
				if va != vb {
					return va < vb
				}
				return chain[a].Version() < chain[b].Version()
			})
			return true
		}
	}
	return !tiles
}

// versionVisibleAtTx reports whether a version participates in valid-time
// resolution as of transaction time txAt: the version must have been
// RECORDED by then (TxFrom <= txAt). txAt == 0 means "no TX filter" (return
// true unconditionally). A version
// without TemporalMetadata is treated as visible to all TX times.
//
// Deliberately NOT bounded by TxTo (lesson 43). TxTo marks when the version
// was SUPERSEDED as the current record — superseded is not retracted: the
// row remains the authority for its valid-time slot in every later belief
// state. Excluding rows with TxTo <= txAt (the previous predicate) made
// every superseded version unable to answer historical valid-time at any
// later txAt, which broke the flagship bitemporal scenario: after a tiled
// explicit-VT update, NodeAtTx(oldVT, now) returned nothing. The resolver's
// vEnd derivation over the TxFrom-filtered chain reconstructs the belief
// state as of txAt: versions recorded later than txAt are absent, so the
// then-latest version is open-ended exactly as it was believed to be then.
func versionVisibleAtTx(tm *types.TemporalMetadata, txAt types.Instant) bool {
	if txAt == 0 {
		return true
	}
	if tm == nil {
		return true
	}
	if tm.TxFrom != 0 && tm.TxFrom > txAt {
		return false
	}
	return true
}

// filterNodeChainByTxAt returns the subset of chain whose TX interval contains
// txAt. txAt == 0 returns chain unchanged (no allocation).
//
// Lesson 43 extended to DeletedAt (lesson 60): a hard Delete is a
// TRANSACTION-TIME tombstone. Delete stamps DeletedAt/TxTo (and ValidTo when the
// row was open or closed after the delete — stampDeleteTombstone) in place on
// the final version, so a row visible at an earlier txAt still carries
// post-txAt delete stamps — and the delete-stamped ValidTo then fails
// valid-time coverage in the resolver, silently rewriting history for every
// pinned scan (`ByLabel`/`ByType`/`All` with QueryOpts.TxAt) and for
// NodeAtTx/RelAtTx. A surviving row whose DeletedAt post-dates txAt is
// therefore DEEP-COPIED (chain rows may be shared frozen store rows — never
// mutate them) and normalized to the belief state as of txAt via the same
// normalizeTemporalVisibleAtTxTime the named as-of door (NodeAsOf/NodesAsOf)
// already applies, keeping the two doors consistent (testing rule 17).
func filterNodeChainByTxAt(a types.ValidAxis, chain []*types.Node, txAt types.Instant) []*types.Node {
	if txAt == 0 {
		return chain
	}
	out := make([]*types.Node, 0, len(chain))
	for _, entry := range chain {
		if row, ok := nodeRowAtTx(a, entry, txAt); ok {
			out = append(out, row)
		}
	}
	return out
}

// nodeRowAtTx is filterNodeChainByTxAt for one row (txAt > 0): false when the
// row was recorded after txAt; else the row, or a normalized deep copy when
// its delete stamps post-date txAt.
func nodeRowAtTx(a types.ValidAxis, entry *types.Node, txAt types.Instant) (*types.Node, bool) {
	tm := entry.Temporal()
	if !versionVisibleAtTx(tm, txAt) {
		return nil, false
	}
	if tm != nil && tm.DeletedAt > txAt {
		entry = entry.DeepCopy()
		storeutil.NormalizeTemporalVisibleAtTxTime(a, entry.Temporal(), txAt)
	}
	return entry, true
}

// filterRelChainByTxAt is the relationship counterpart of filterNodeChainByTxAt,
// including the post-txAt delete-tombstone normalization (see there).
func filterRelChainByTxAt(a types.ValidAxis, chain []*types.Relationship, txAt types.Instant) []*types.Relationship {
	if txAt == 0 {
		return chain
	}
	out := make([]*types.Relationship, 0, len(chain))
	for _, entry := range chain {
		if row, ok := relRowAtTx(a, entry, txAt); ok {
			out = append(out, row)
		}
	}
	return out
}

// relRowAtTx mirrors nodeRowAtTx.
func relRowAtTx(a types.ValidAxis, entry *types.Relationship, txAt types.Instant) (*types.Relationship, bool) {
	tm := entry.Temporal()
	if !versionVisibleAtTx(tm, txAt) {
		return nil, false
	}
	if tm != nil && tm.DeletedAt > txAt {
		entry = entry.DeepCopy()
		storeutil.NormalizeTemporalVisibleAtTxTime(a, entry.Temporal(), txAt)
	}
	return entry, true
}

// nodeOwnBounds returns [vStart, vEnd) using the row's OWN asserted end
// (ValidTo, 0 => +inf) rather than the positional next-version bound
// nodeVersionBounds derives. Used ONLY by resolveNodeVersionAt's cascade
// (non-monotonic) slow path and by the cascade kernel's newCurrent
// selection (temporal_cascade.go) — both contexts where overlaps between
// rows are resolved by belief recency (nodeBeliefNewerThan), not by
// position. A row's end must be its own claim there, never a neighbor's
// ValidFrom, or an untouched older row can wrongly truncate a newer,
// wider-reaching correction (BACKLOG 10b). vStart reuses nodeValidFrom
// — the same effective-start key the chain is already sorted by — so this
// helper never needs the chain, only the entry.
//
// Deliberately does NOT replace nodeVersionBounds: the monotonic fast path,
// the interval door, the Relate (Allen) door, and SelectAsOf all keep using
// positional bounds, which is correct for them (see the design rationale in
// docs/CHANGELOG.md's BACKLOG 10b entry) — do not widen this helper's use
// without re-running the full bitemporal oracle fuzz harness at full
// iteration count (10b's two prior fix attempts were reverted after
// touching nodeVersionBounds itself broke it in non-obvious multi-cascade
// ways; this helper's whole purpose is to avoid ever needing to).
func (c *Core) nodeOwnBounds(n *types.Node) (types.Instant, types.Instant) {
	vStart := c.nodeValidFrom(n)
	vEnd := c.axis().OpenEnd() // open (+inf) by the graph's valid axis
	if tm := n.Temporal(); tm != nil && !c.axis().Open(tm.ValidTo) {
		vEnd = tm.ValidTo
	}
	return vStart, vEnd
}

// nodeVersionBounds computes the effective [vStart, vEnd) for chain[i]: vEnd
// on the graph's valid axis (its open end when no later version and no own
// end bound it; ValidAxis.Open).
//
// vEnd is the NEXT version's effective valid-from (explicit ValidFrom, else
// its TxFrom: TX-time-as-VT-time for callers without explicit valid-time), so
// a chain tiles; an own ValidTo overrides it.
func (c *Core) nodeVersionBounds(chain []*types.Node, i int) (types.Instant, types.Instant) {
	entry := chain[i]
	a := c.axis()
	vStart, vEnd := types.Instant(0), a.OpenEnd()

	// Version start: the untimed start (TxFrom; legacy UpdatedAt / mint),
	// replaced by an explicit ValidFrom below unless it is a legacy inherited
	// one. Version end: the next version's start (backlog 62: one rule).
	vStart = storeutil.UntimedValidFrom(entry.ID().SnowflakeID(), entry.Temporal())
	if i+1 < len(chain) {
		vEnd = a.NodeStart(chain[i+1])
	}

	// Post-migration: every claimed ValidFrom is caller-supplied. Pre-
	// migration: keep the legacy inheritance heuristic for back-compat
	// (stores without MetaKVCapability never migrate).
	if tm := entry.Temporal(); tm != nil {
		if a.Claimed(tm.ValidFrom) && (c.bitemporalMigrated || !nodeInheritedValidFrom(chain, i, tm)) {
			vStart = tm.ValidFrom
		}
		if !a.Open(tm.ValidTo) {
			vEnd = tm.ValidTo
		}
	}

	return vStart, vEnd
}

// nodeInheritedValidFrom is the pre-migration heuristic (bitemporalMigrated
// false: a store without MetaKVCapability). Caveat (backlog 65): a close row
// stamps its version's start as ValidFrom, so a close of a version whose
// explicit ValidFrom equals its predecessor's reads as inherited here and
// starts at the close's TxFrom. Every in-tree store migrates.
func nodeInheritedValidFrom(chain []*types.Node, i int, tm *types.TemporalMetadata) bool {
	if i == 0 || tm.UpdatedAt == 0 {
		return false
	}
	prevTM := chain[i-1].Temporal()
	return prevTM != nil && prevTM.ValidFrom != 0 && prevTM.ValidFrom != types.UnboundedValidFrom && prevTM.ValidFrom == tm.ValidFrom
}

// resolveRelVersionAtCapped mirrors resolveNodeVersionAtCapped: chain is
// ordered by effective valid-from and, on an overlap, the newer belief
// (higher TxFrom, then version) wins.
func (c *Core) resolveRelVersionAtCapped(chain []*types.Relationship, t types.Instant, caps lifeEnds[*types.Relationship]) (*types.Relationship, error) {
	ax := c.axis() // once per chain, not per row
	if !c.sortRelChainForResolve(chain) {
		// BACKLOG 10m: mirrors resolveNodeVersionAt's binary-search-assisted
		// scan start — see there for the bitemporalMigrated-gating rationale.
		start := len(chain) - 1
		if c.bitemporalMigrated {
			idx := sort.Search(len(chain), func(i int) bool {
				vStart, _ := c.relVersionBounds(chain, i)
				return vStart > t
			})
			start = idx - 1
		}
		for i := start; i >= 0; i-- {
			entry := chain[i]
			vStart, vEnd := c.relVersionBounds(chain, i)
			vEnd = caps.end(ax, entry, vEnd)
			if storeutil.IntervalCovers(ax, vStart, vEnd, t) {
				if isVoidRow(entry) {
					return nil, storepkg.ErrNoVersionValidAt // an excised piece wins: absent
				}
				return entry, nil
			}
		}
		return nil, storepkg.ErrNoVersionValidAt
	}

	// BACKLOG 10b: own-interval bounds, not positional — see resolveNodeVersionAt
	// (also for the supersession cap).
	sc := getSupersessionScratch()
	defer putSupersessionScratch(sc)
	superseded := supersessionCaps(chain, c.relValidFrom, sc)
	starts := sc.startsOf(len(chain)) // relOwnBounds' start, computed once
	var best *types.Relationship
	for i := range chain {
		entry := chain[i]
		vStart, vEnd := starts[i], ax.OpenEnd() // relOwnBounds: own ValidTo, open by the axis
		if tm := entry.Temporal(); tm != nil {
			vEnd = tm.ValidTo
		}
		var empty bool
		if vEnd, empty = superseded.end(ax, i, vStart, vEnd); empty {
			continue // a newer belief replaced it from its start on
		}
		vEnd = caps.end(ax, entry, vEnd)
		if storeutil.IntervalCovers(ax, vStart, vEnd, t) {
			if best == nil || relBeliefNewerThan(entry, best) {
				best = entry
			}
		}
	}
	if best == nil || isVoidRow(best) {
		return nil, storepkg.ErrNoVersionValidAt
	}
	return best, nil
}

// relBeliefNewerThan mirrors nodeBeliefNewerThan for relationships.
func relBeliefNewerThan(a, b *types.Relationship) bool {
	ta, tb := beliefTx(a.Temporal()), beliefTx(b.Temporal())
	if ta != tb {
		return ta > tb
	}
	return a.Version() > b.Version()
}

// sortRelChainForResolve mirrors sortNodeChainForResolve for relationships:
// true when the resolver must take its own-bounds arm (reordered, or the chain
// does not tile).
func (c *Core) sortRelChainForResolve(chain []*types.Relationship) bool {
	ax := c.axis() // once per chain, not per row
	tiles := true
	for i := 1; i < len(chain); i++ {
		pa, pb := ax.RelStart(chain[i-1]), ax.RelStart(chain[i])
		if tiles && !tilesWithNext(ax, chain[i-1].Temporal(), chain[i].Temporal(), pa, pb) {
			tiles = false
		}
		if pa > pb || (pa == pb && chain[i-1].Version() > chain[i].Version()) {
			sort.SliceStable(chain, func(a, b int) bool {
				va, vb := ax.RelStart(chain[a]), ax.RelStart(chain[b])
				if va != vb {
					return va < vb
				}
				return chain[a].Version() < chain[b].Version()
			})
			return true
		}
	}
	return !tiles
}

// tilesWithNext reports whether the positional fast path answers like the
// own-bounds arm for a row (prev, starting at pa) and the next one in chain
// order (next, starting at pb >= pa). Rows with different starts tile: the
// fast path ends prev at pb, as every writer intends. Rows sharing a start
// tile when next ends no earlier than prev: next then covers prev wherever
// prev is valid and the newer row wins in both arms. A narrower next (a close
// to 93 of a row valid to 233) leaves prev exposed to the fast path; the
// own-bounds arm ends it at next's start (SAMESTART). The oracle harness
// (TestBitemporalOracleHarness, its close and regrow ops) holds the fast path
// to the reference rule on the chains the write doors produce.
func tilesWithNext(ax types.ValidAxis, prev, next *types.TemporalMetadata, pa, pb types.Instant) bool {
	if pa < pb {
		return true
	}
	nextOpen := next == nil || ax.Open(next.ValidTo)
	prevOpen := prev == nil || ax.Open(prev.ValidTo)
	switch {
	case nextOpen:
		return true
	case prevOpen:
		return false
	}
	return next.ValidTo >= prev.ValidTo
}

// relOwnBounds mirrors nodeOwnBounds for relationships. See there.
func (c *Core) relOwnBounds(r *types.Relationship) (types.Instant, types.Instant) {
	vStart := c.relValidFrom(r)
	vEnd := c.axis().OpenEnd() // open (+inf) by the graph's valid axis
	if tm := r.Temporal(); tm != nil && !c.axis().Open(tm.ValidTo) {
		vEnd = tm.ValidTo
	}
	return vStart, vEnd
}

// relVersionBounds computes the effective [vStart, vEnd) for chain[i].
// See nodeVersionBounds for the Phase 3 vEnd-from-next.ValidFrom rationale.
func (c *Core) relVersionBounds(chain []*types.Relationship, i int) (types.Instant, types.Instant) {
	entry := chain[i]
	a := c.axis()
	vStart, vEnd := types.Instant(0), a.OpenEnd()

	vStart = storeutil.UntimedValidFrom(entry.ID().SnowflakeID(), entry.Temporal())
	if i+1 < len(chain) {
		vEnd = a.RelStart(chain[i+1])
	}

	if tm := entry.Temporal(); tm != nil {
		if a.Claimed(tm.ValidFrom) && (c.bitemporalMigrated || !relInheritedValidFrom(chain, i, tm)) {
			vStart = tm.ValidFrom
		}
		if !a.Open(tm.ValidTo) {
			vEnd = tm.ValidTo
		}
	}

	return vStart, vEnd
}

func relInheritedValidFrom(chain []*types.Relationship, i int, tm *types.TemporalMetadata) bool {
	if i == 0 || tm.UpdatedAt == 0 {
		return false
	}
	prevTM := chain[i-1].Temporal()
	return prevTM != nil && prevTM.ValidFrom != 0 && prevTM.ValidFrom != types.UnboundedValidFrom && prevTM.ValidFrom == tm.ValidFrom
}

// --- Private helpers for history-aware queries ---

// forEachNodeCandidateID iterates the union of (currentIDs, ALL node-history
// IDs). Use for history-aware label/property queries where an entity's
// CURRENT state may diverge from its state at the queried instant: a node
// currently labelled Y but historically labelled X at t must still be
// visited so the per-entity predicate can match its at-t version.
// (Adjacency queries do NOT need the full history fold: a relationship's rows
// name a node its live adjacency does not only when it is deleted or was
// re-imported with other endpoints — the ex-adjacency. See
// forEachRelAdjacencyCandidateID.)
func (c *Core) forEachNodeCandidateID(currentIDs []types.NodeID, fn func(types.NodeID) error) error {
	return c.forEachNodeCandidateIDByDepth(currentIDs, storepkg.DepthAll, fn)
}

func (c *Core) forEachNodeCandidateIDByDepth(currentIDs []types.NodeID, depth storepkg.ShardDepth, fn func(types.NodeID) error) error {
	seen := make(map[types.NodeID]struct{}, len(currentIDs))
	for _, id := range currentIDs {
		seen[id] = struct{}{}
	}
	if err := c.forEachNodeHistoryIDByDepth(depth, func(id types.NodeID) bool {
		seen[id] = struct{}{}
		return true
	}); err != nil {
		return err
	}
	for id := range seen {
		if err := fn(id); err != nil {
			return err
		}
	}
	return nil
}

// forEachRelCandidateIDByDepth is the relationship counterpart of
// forEachNodeCandidateIDByDepth — the all-history fold (the fallback of
// forEachRelPropertyCandidateID and of the rel-type scans without the K1
// sidecar). For adjacency queries, prefer forEachRelAdjacencyCandidateID.
func (c *Core) forEachRelCandidateIDByDepth(currentIDs []types.RelID, depth storepkg.ShardDepth, fn func(types.RelID) error) error {
	seen := make(map[types.RelID]struct{}, len(currentIDs))
	for _, id := range currentIDs {
		seen[id] = struct{}{}
	}
	if err := c.forEachRelHistoryIDByDepth(depth, func(id types.RelID) bool {
		seen[id] = struct{}{}
		return true
	}); err != nil {
		return err
	}
	for id := range seen {
		if err := fn(id); err != nil {
			return err
		}
	}
	return nil
}

// adjDirs names the directions an adjacency gather reads a seed in.
type adjDirs uint8

const (
	adjOut  adjDirs = 1 << iota // the seed is the start node
	adjIn                       // the seed is the end node
	adjBoth = adjOut | adjIn
)

// adjDirsOf is adjOut for outgoing, adjIn otherwise.
func adjDirsOf(outgoing bool) adjDirs {
	if outgoing {
		return adjOut
	}
	return adjIn
}

// forEachRelAdjacencyCandidateID is the adjacency-query specialization of
// forEachRelCandidateIDByDepth (backlog 50): the candidates are currentIDs
// (the seeds' live adjacency) united with the seeds' ex-adjacency in dirs —
// every relationship that named a seed in a row the live adjacency no longer
// ties to it (a deleted relationship, or an earlier life of one re-imported
// with other endpoints). That is O(the seeds' ever-degree), not O(history) or
// O(deleted). Each door then resolves a candidate on its own and keeps it only
// when the resolved row names the seed. A seed need not exist any more.
func (c *Core) forEachRelAdjacencyCandidateID(seeds []types.NodeID, dirs adjDirs, currentIDs []types.RelID, fn func(types.RelID) error) error {
	seen := make(map[types.RelID]struct{}, len(currentIDs))
	for _, id := range currentIDs {
		seen[id] = struct{}{}
	}
	if err := c.forEachRelExAdjacentID(seeds, dirs, storepkg.DepthAll, func(id types.RelID) bool {
		seen[id] = struct{}{}
		return true
	}); err != nil {
		return err
	}
	for id := range seen {
		if err := fn(id); err != nil {
			return err
		}
	}
	return nil
}

// forEachRelExAdjacentID visits the seeds' ex-adjacency in dirs from the
// store's ExAdjacencyCapability (depth-scoped where the store scopes
// adjacency by depth, store.AdjacencyDepthCapability; else a superset under a
// depth filter). Without it — or before the one-time backfill of a store written
// without it completed (exAdjacencyReady) — it visits every relationship with
// history instead: sound, O(total relationship history). An ID may repeat.
func (c *Core) forEachRelExAdjacentID(seeds []types.NodeID, dirs adjDirs, depth storepkg.ShardDepth, fn func(types.RelID) bool) error {
	if c.exAdj == nil {
		return c.forEachRelHistoryIDByDepth(depth, fn)
	}
	stopped := false
	visit := func(id types.RelID) bool {
		if !fn(id) {
			stopped = true
		}
		return !stopped
	}
	byDepth, scoped := c.store.(storepkg.AdjacencyDepthCapability)
	scoped = scoped && depth != storepkg.DepthAll
	for _, seed := range seeds {
		for _, d := range []adjDirs{adjOut, adjIn} {
			if dirs&d == 0 {
				continue
			}
			var err error
			if scoped {
				err = byDepth.ForEachExAdjacentRelIDByDepth(seed, d == adjIn, depth, visit)
			} else {
				err = c.exAdj.ForEachExAdjacentRelID(seed, d == adjIn, visit)
			}
			if err != nil || stopped {
				return err
			}
		}
	}
	return nil
}

// forEachKnownNodeID collects the union of current + history node IDs
// via lazy ForEach iteration, then calls fn for each unique ID.
// Two-phase: collect (under store locks), then process (locks released).
// This avoids materializing all IDs from all shards into giant slices.
//
// Memory note: the seen-map materialises O(N) IDs where N = total node count
// (current + historical). This is intentional: Phase 1 must hold store locks
// for consistency, but fn (Phase 2) may re-enter the store, which would
// deadlock. The map is the unavoidable bridge between the two phases.
// For history-unaware queries over only current entities, prefer the
// streaming ForEach iterators in the Store interface directly.
func (c *Core) forEachKnownNodeID(fn func(types.NodeID) error) error {
	return c.forEachKnownNodeIDByDepth(storepkg.DepthAll, fn)
}

func (c *Core) forEachKnownNodeIDByDepth(depth storepkg.ShardDepth, fn func(types.NodeID) error) error {
	ids, err := c.collectKnownNodeIDsByDepth(depth)
	if err != nil {
		return err
	}
	for _, id := range ids {
		if err := fn(id); err != nil {
			return err
		}
	}
	return nil
}

func (c *Core) collectKnownNodeIDsByDepth(depth storepkg.ShardDepth) ([]types.NodeID, error) {
	seen := make(map[types.NodeID]struct{})

	// Phase 1: collect unique IDs (no store method calls in callbacks — lock reentrancy).
	if depth == storepkg.DepthAll {
		if err := c.forEachNodeID(func(id types.NodeID) bool {
			seen[id] = struct{}{}
			return true
		}); err != nil {
			return nil, err
		}
	} else {
		ids, err := c.allNodeIDs(storepkg.QueryOpts{Depth: depth})
		if err != nil {
			return nil, err
		}
		for _, id := range ids {
			seen[id] = struct{}{}
		}
	}
	if err := c.forEachNodeHistoryIDByDepth(depth, func(id types.NodeID) bool {
		seen[id] = struct{}{}
		return true
	}); err != nil {
		return nil, err
	}

	ids := make([]types.NodeID, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	return ids, nil
}

// forEachKnownRelID collects the union of current + history relationship IDs
// via lazy ForEach iteration, then calls fn for each unique ID.
// Two-phase: collect (under store locks), then process (locks released).
//
// Memory note: same O(N) materialisation trade-off as forEachKnownNodeID.
// See that function's doc comment for the rationale.
func (c *Core) forEachKnownRelID(fn func(types.RelID) error) error {
	return c.forEachKnownRelIDByDepth(storepkg.DepthAll, fn)
}

func (c *Core) forEachKnownRelIDByDepth(depth storepkg.ShardDepth, fn func(types.RelID) error) error {
	ids, err := c.collectKnownRelIDsByDepth(depth)
	if err != nil {
		return err
	}
	for _, id := range ids {
		if err := fn(id); err != nil {
			return err
		}
	}
	return nil
}

func (c *Core) collectKnownRelIDsByDepth(depth storepkg.ShardDepth) ([]types.RelID, error) {
	seen := make(map[types.RelID]struct{})

	// Phase 1: collect unique IDs.
	if depth == storepkg.DepthAll {
		if err := c.forEachRelID(func(id types.RelID) bool {
			seen[id] = struct{}{}
			return true
		}); err != nil {
			return nil, err
		}
	} else {
		ids, err := c.allRelIDs(storepkg.QueryOpts{Depth: depth})
		if err != nil {
			return nil, err
		}
		for _, id := range ids {
			seen[id] = struct{}{}
		}
	}
	if err := c.forEachRelHistoryIDByDepth(depth, func(id types.RelID) bool {
		seen[id] = struct{}{}
		return true
	}); err != nil {
		return nil, err
	}

	ids := make([]types.RelID, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	return ids, nil
}

func (c *Core) forEachNodeHistoryIDByDepth(depth storepkg.ShardDepth, fn func(types.NodeID) bool) error {
	wrapped := fn
	if !c.storeRowsTrust {
		var invalid error
		wrapped = func(id types.NodeID) bool {
			if err := storepkg.ValidateNodeID(id); err != nil {
				invalid = err
				return false
			}
			return fn(id)
		}
		err := c.forEachNodeHistoryIDByDepthTrusted(depth, wrapped)
		if invalid != nil {
			return invalid
		}
		return err
	}
	return c.forEachNodeHistoryIDByDepthTrusted(depth, wrapped)
}

func (c *Core) forEachNodeHistoryIDByDepthTrusted(depth storepkg.ShardDepth, fn func(types.NodeID) bool) error {
	if depth == storepkg.DepthAll {
		return c.store.ForEachNodeHistoryID(fn)
	}
	if c.depthHistory != nil {
		return c.depthHistory.ForEachNodeHistoryIDByDepth(depth, fn)
	}
	return c.store.ForEachNodeHistoryID(fn)
}

func (c *Core) forEachRelHistoryIDByDepth(depth storepkg.ShardDepth, fn func(types.RelID) bool) error {
	wrapped := fn
	if !c.storeRowsTrust {
		var invalid error
		wrapped = func(id types.RelID) bool {
			if err := storepkg.ValidateRelID(id); err != nil {
				invalid = err
				return false
			}
			return fn(id)
		}
		err := c.forEachRelHistoryIDByDepthTrusted(depth, wrapped)
		if invalid != nil {
			return invalid
		}
		return err
	}
	return c.forEachRelHistoryIDByDepthTrusted(depth, wrapped)
}

func (c *Core) forEachRelHistoryIDByDepthTrusted(depth storepkg.ShardDepth, fn func(types.RelID) bool) error {
	if depth == storepkg.DepthAll {
		return c.store.ForEachRelHistoryID(fn)
	}
	if c.depthHistory != nil {
		return c.depthHistory.ForEachRelHistoryIDByDepth(depth, fn)
	}
	return c.store.ForEachRelHistoryID(fn)
}

// hasTemporalFilter reports whether opts carries a temporal filter that
// requires history-aware resolution: a valid-time filter
// (QueryOpts.HasValidTimeFilter) or a transaction pin (HasTxAt, HasTxPin) —
// the QueryOpts presence rule, where an unflagged 0 is unset. An unflagged
// lone ValidStart or ValidEnd is no filter, so
// AllNodes(storepkg.QueryOpts{ValidStart: t}) takes the non-temporal path.
func hasTemporalFilter(opts storepkg.QueryOpts) bool {
	return opts.HasValidTimeFilter() || opts.HasTxAt() || opts.HasTxPin()
}

// pinInstant is the instant a present pin hands the code below, which reads
// 0 as "no pin": a pin at 0 becomes -1, an absent pin is 0. Both pins select
// the same rows: a recorded belief has a positive TxFrom (ErrInvalidTxFrom;
// as-of candidates need 0 < TxFrom) and the compaction and retention
// watermarks are positive, so nothing lies in [-1, 0].
func pinInstant(at types.Instant, present bool) types.Instant {
	switch {
	case !present:
		return 0
	case at == 0:
		return -1
	}
	return at
}

// txAtOf is opts' TxAt pin for the tx-time code (pinInstant).
func txAtOf(opts storepkg.QueryOpts) types.Instant { return pinInstant(opts.TxAt, opts.HasTxAt()) }

// txPinOf is opts' TxPin pin for the tx-time code (pinInstant).
func txPinOf(opts storepkg.QueryOpts) types.Instant { return pinInstant(opts.TxPin, opts.HasTxPin()) }

// effectiveTxPin returns the transaction-time pin a candidate must have been
// recorded by (TxPin, else TxAt). A member whose earliest label/type acquisition
// transaction time is strictly AFTER this pin could not have carried the
// label/type at the pin and is safely pruned from the K1 candidate set. Returns
// 0 for a pure valid-time filter (no transaction-time pin → no such pruning).
func effectiveTxPin(opts storepkg.QueryOpts) types.Instant {
	if opts.HasTxPin() {
		return txPinOf(opts)
	}
	return txAtOf(opts)
}

// membershipSidecarUsable reports whether a temporal read with this depth may
// take its candidates from a membership sidecar (label/type or property
// value): the sidecars answer for the whole store, so a depth-scoped read on
// a store with depth-aware history (tiered) keeps the depth fold.
func (c *Core) membershipSidecarUsable(depth storepkg.ShardDepth) bool {
	return depth == storepkg.DepthAll || c.depthHistory == nil
}

// memberIDsAt returns c.memberIDs where it answers a read at depth, else nil
// (the read takes the rows). Its lists, like the sidecars, cover the whole
// store, so on the same terms as membershipSidecarUsable.
func (c *Core) memberIDsAt(depth storepkg.ShardDepth) storepkg.MemberIDsCapability {
	if !c.membershipSidecarUsable(depth) {
		return nil
	}
	return c.memberIDs
}

// forEachRelPropertyCandidateID streams the candidate set of a temporal
// (relType, key = value) lookup into fn, once per candidate (backlog 8).
//
// With the store's property membership sidecar for a declared (relType, key)
// index the candidates are the value's ever-members, minus those whose
// earliest row carrying the value was recorded strictly after the effective
// transaction-time pin (TxPin, else TxAt; a pure valid-time filter prunes
// nothing), united with the current index matches (current). With a
// valid-time filter and the windowed capability (ledger row 62) they are only
// the members whose rows carrying the value may meet the filter's window,
// pruned the same way; the sidecar records every current and history row, so
// current is not read. Otherwise — no sidecar, no declared index
// (store.ErrIndexNotFound), or a depth-scoped read — the full-history fold.
// Every path feeds the same resolver and predicate, so they answer the same;
// the sidecar paths load only chains that can carry the value. current is
// called at most once, only by the paths that need it.
func (c *Core) forEachRelPropertyCandidateID(tok uint16, key string, valueKeys equalKeys, current func() ([]types.RelID, error), opts storepkg.QueryOpts, fn func(types.RelID) error) error {
	fold := func() error {
		currentIDs, err := current()
		if err != nil {
			return err
		}
		return c.forEachRelHistoryCandidateOfType(tok, currentIDs, opts, fn)
	}
	if c.relPropTxMembers == nil || !c.membershipSidecarUsable(opts.Depth) {
		return fold()
	}
	pinTx := effectiveTxPin(opts)
	start, end, windowed := validWindow(opts)
	windowed = windowed && c.relPropTxWindow != nil
	for {
		seen := make(map[types.RelID]struct{})
		declined := false
		var invalid error
		member := func(id types.RelID, firstTx types.Instant) bool {
			if !c.storeRowsTrust {
				if verr := storepkg.ValidateRelID(id); verr != nil {
					invalid = verr
					return false
				}
			}
			if pinTx != 0 && firstTx != 0 && firstTx > pinTx {
				return true // first carried the value after the pin: cannot match
			}
			seen[id] = struct{}{}
			return true
		}
		var window *memberWindow
		if windowed {
			window = &memberWindow{start, end, txAtOf(opts)}
		}
		err := c.forEachRelPropTxMemberOfKeys(tok, key, valueKeys, window, member)
		if invalid != nil {
			return invalid
		}
		if errors.Is(err, storepkg.ErrIndexNotFound) {
			declined = true
		} else if err != nil {
			return err
		}
		if declined {
			if windowed {
				windowed = false // the full enumeration decides (it may decline too)
				continue
			}
			return fold()
		}
		if !windowed {
			currentIDs, err := current()
			if err != nil {
				return err
			}
			for _, id := range currentIDs {
				seen[id] = struct{}{}
			}
		}
		for id := range seen {
			if err := fn(id); err != nil {
				return err
			}
		}
		return nil
	}
}

// memberWindow is the valid-time window and pin of a windowed membership
// enumeration (store.NodePropertyTxMemberWindowCapability); nil = the whole
// value's members.
type memberWindow struct {
	start, end, txAt types.Instant
}

// forEachRelPropTxMemberOfKeys enumerates the rel property membership sidecar
// of (tok, key) over every key of valueKeys (a value's equal keys): one call
// where the store has the several-key form (round 11; one fan-out on tiered
// and sharded), else one call per key, stopping at the first error or
// ErrIndexNotFound (fn may see an entity once per key; the keys form merges
// them, the lowest first tx, which prunes alike). window selects the windowed
// form.
func (c *Core) forEachRelPropTxMemberOfKeys(tok uint16, key string, valueKeys []string, window *memberWindow, fn func(types.RelID, types.Instant) bool) error {
	if c.relPropTxKeys != nil {
		if window != nil {
			return c.relPropTxKeys.ForEachRelPropertyTxMemberKeysDuring(tok, key, valueKeys, window.start, window.end, window.txAt, fn)
		}
		return c.relPropTxKeys.ForEachRelPropertyTxMemberKeys(tok, key, valueKeys, fn)
	}
	for _, vk := range valueKeys {
		var err error
		if window != nil {
			err = c.relPropTxWindow.ForEachRelPropertyTxMemberDuring(tok, key, vk, window.start, window.end, window.txAt, fn)
		} else {
			err = c.relPropTxMembers.ForEachRelPropertyTxMember(tok, key, vk, fn)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// forEachNodePropTxMemberOfKeys is forEachRelPropTxMemberOfKeys for a node
// (label, key) sidecar.
func (c *Core) forEachNodePropTxMemberOfKeys(tok uint16, key string, valueKeys []string, window *memberWindow, fn func(types.NodeID, types.Instant) bool) error {
	if c.nodePropTxKeys != nil {
		if window != nil {
			return c.nodePropTxKeys.ForEachNodePropertyTxMemberKeysDuring(tok, key, valueKeys, window.start, window.end, window.txAt, fn)
		}
		return c.nodePropTxKeys.ForEachNodePropertyTxMemberKeys(tok, key, valueKeys, fn)
	}
	for _, vk := range valueKeys {
		var err error
		if window != nil {
			err = c.nodePropTxWindow.ForEachNodePropertyTxMemberDuring(tok, key, vk, window.start, window.end, window.txAt, fn)
		} else {
			err = c.nodePropTxMembers.ForEachNodePropertyTxMember(tok, key, vk, fn)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// validWindow returns the valid-time window [start, end) a point or interval
// filter in opts restricts to (ValidAt t is [t, t+1)), and false when opts has
// no valid-time filter or the point has no representable successor.
func validWindow(opts storepkg.QueryOpts) (start, end types.Instant, ok bool) {
	switch {
	case opts.HasValidAt():
		if opts.ValidAt == math.MaxInt64 {
			return 0, 0, false
		}
		return opts.ValidAt, opts.ValidAt + 1, true
	case opts.HasValidInterval():
		return opts.ValidStart, opts.ValidEnd, true
	}
	return 0, 0, false
}

// forEachNodePropertyCandidateID is the node twin of
// forEachRelPropertyCandidateID for a conjunction of (key = value) pairs under
// one label (one pair for ByLabelAndProperty, 2..4 for ByLabelAndProperties).
// A node matches through one version carrying the label and every value, so
// it is an ever-member of each pair's (label, key) sidecar: the candidates are
// the INTERSECTION of the sidecar sets of the pairs whose key has a declared
// index (a pair without one, or with a non-indexable value, constrains
// nothing), pruned by the pin per pair, united with the current matches —
// or, under a valid-time filter with the windowed capability (ledger row 62),
// each pair's set narrowed to the members whose rows may meet the window and
// no current read. No usable pair at all: the history fold scoped to the
// label's ever-members (forEachNodeHistoryCandidateOfLabel).
func (c *Core) forEachNodePropertyCandidateID(tok uint16, valueKeys map[string]equalKeys, current func() ([]types.NodeID, error), opts storepkg.QueryOpts, fn func(types.NodeID) error) error {
	fold := func() error {
		currentIDs, err := current()
		if err != nil {
			return err
		}
		return c.forEachNodeHistoryCandidateOfLabel(tok, currentIDs, opts, fn)
	}
	if c.nodePropTxMembers == nil || !c.membershipSidecarUsable(opts.Depth) {
		return fold()
	}
	pinTx := effectiveTxPin(opts)
	start, end, windowed := validWindow(opts)
	windowed = windowed && c.nodePropTxWindow != nil
	keys := make([]string, 0, len(valueKeys))
	for k := range valueKeys {
		keys = append(keys, k)
	}
	sort.Strings(keys) // deterministic intersection order
	var cands map[types.NodeID]struct{}
	for _, key := range keys {
		vks := valueKeys[key]
		if len(vks) == 0 {
			continue
		}
		set := make(map[types.NodeID]struct{})
		var invalid error
		member := func(id types.NodeID, firstTx types.Instant) bool {
			if !c.storeRowsTrust {
				if verr := storepkg.ValidateNodeID(id); verr != nil {
					invalid = verr
					return false
				}
			}
			if pinTx != 0 && firstTx != 0 && firstTx > pinTx {
				return true
			}
			if cands != nil {
				if _, keep := cands[id]; !keep {
					return true
				}
			}
			set[id] = struct{}{}
			return true
		}
		var window *memberWindow
		if windowed {
			window = &memberWindow{start, end, txAtOf(opts)}
		}
		err := c.forEachNodePropTxMemberOfKeys(tok, key, vks, window, member)
		if invalid != nil {
			return invalid
		}
		if errors.Is(err, storepkg.ErrIndexNotFound) {
			continue // this pair constrains nothing
		}
		if err != nil {
			return err
		}
		cands = set
	}
	if cands == nil {
		return fold()
	}
	if !windowed {
		currentIDs, err := current()
		if err != nil {
			return err
		}
		for _, id := range currentIDs {
			cands[id] = struct{}{}
		}
	}
	for id := range cands {
		if err := fn(id); err != nil {
			return err
		}
	}
	return nil
}

// validateTemporalQueryOpts is the core-layer superset of
// storepkg.ValidateQueryOpts: it adds the cross-field TxPin conflict check.
// TxPin is a pure knowledge-time belief-state pin (identical semantics to
// NodesAsOf / RelsAsOf) with NO valid-time filtering, so combining it with any
// valid-time filter (ValidAt / ValidStart / ValidEnd) or with the combined
// bitemporal TxAt door is contradictory and rejected with
// ErrConflictingTemporalOpts rather than silently mis-resolved. Every generic
// query door that honours TxPin validates through here.
func validateTemporalQueryOpts(opts storepkg.QueryOpts) error {
	if err := storepkg.ValidateQueryOpts(opts); err != nil {
		return err
	}
	if opts.HasTxPin() && (opts.HasValidAt() || opts.HasValidBound() || opts.HasTxAt()) {
		return ErrConflictingTemporalOpts
	}
	return nil
}

// normalizeTxAtOnlyOpts resolves the open-ended "now" fallback ONCE for a
// TxAt-only query (opts.TxAt set, no TxPin/ValidAt/ValidStart/ValidEnd) before
// it is used across a multi-candidate scan. Without this, every call site that
// loops findNodeVersionForOpts/findRelVersionForOpts over many candidate IDs
// with the SAME opts value would have each candidate independently resolve
// opts.TxAt's implicit "now" via resolveOpenEndInstant(0) inside the callee —
// a fresh read of now PER CANDIDATE, exactly the iteration-timing hazard
// resolveOpenEndInstant's own doc comment warns against ("each per-ID call
// would otherwise observe a different "now" and produce inclusion/
// exclusion that depends on iteration timing"). A relationship or node whose
// ValidTo/DeletedAt boundary falls between two of those per-candidate reads
// would be included by one candidate's probe and excluded by another's within
// the SAME logical query — or diverge from an independent second scan over
// the same opts (e.g. a test's reference-model door) run moments later.
// Setting opts.ValidAt here makes the (already-correct, already-tested)
// ValidAt branch fire instead of the per-candidate TxAt-only branch, so every
// candidate in the scan is evaluated against the exact same valid-time pin.
// A no-op for any other QueryOpts shape (TxPin, explicit ValidAt already set,
// an interval query, or no temporal filter at all) — those either need no
// resolution or already resolve their bound once via resolveOpenEndInstant at
// their own entry point (findNodeVersionMatchingDuringTx / the interval arm).
func (c *Core) normalizeTxAtOnlyOpts(opts storepkg.QueryOpts) storepkg.QueryOpts {
	if opts.HasTxAt() && !opts.HasTxPin() && !opts.HasValidAt() && !opts.HasValidBound() {
		opts.ValidAt = c.readNow()
	}
	return opts
}

// findNodeVersionForOpts returns a node version that satisfies pred under the
// temporal filter in opts. ValidAt takes precedence over ValidStart/ValidEnd.
// For interval queries, all overlapping versions are scanned (most-recent
// first) and the first match is returned — so a node whose label/property held
// at any moment in the interval is found, even if a later version no longer
// matches. Returns storepkg.ErrNoVersionValidAt if no overlapping version satisfies
// pred. pred==nil means "any overlapping version".
func (c *Core) findNodeVersionForOpts(id types.NodeID, opts storepkg.QueryOpts, pred func(*types.Node) bool) (*types.Node, error) {
	if c.chainLoadTestHook != nil {
		c.chainLoadTestHook()
	}
	if opts.HasTxPin() {
		// Belief-state pin: pure knowledge-time resolution, NO valid-time
		// filter. Delegate to the SAME resolver the named as-of door uses
		// (nodeAsOfLocked feeds NodeAsOf / NodesAsOf) so the two doors agree by
		// construction — the version-ordered retraction rule (lesson 62) is
		// inherited, never re-implemented. Absent → the loop's skip sentinel.
		n, err := c.nodeAsOfLocked(id, txPinOf(opts))
		if err != nil {
			if errors.Is(err, ErrNoVersionAsOf) {
				return nil, storepkg.ErrNoVersionValidAt
			}
			return nil, err
		}
		if pred != nil && !pred(n) {
			return nil, storepkg.ErrNoVersionValidAt
		}
		return n, nil
	}
	if opts.HasValidAt() {
		n, err := c.nodeAtLockedTx(id, opts.ValidAt, txAtOf(opts))
		if err != nil {
			return nil, err
		}
		if pred != nil && !pred(n) {
			return nil, storepkg.ErrNoVersionValidAt
		}
		return n, nil
	}
	if opts.HasValidInterval() {
		return c.findNodeVersionMatchingDuringTx(id, opts.ValidStart, opts.ValidEnd, txAtOf(opts), pred)
	}
	if opts.HasTxAt() {
		// TX-only filter: return version visible at txAt as of "now" valid time
		// — i.e. whatever was current-or-most-recent at txAt.
		n, err := c.nodeAtLockedTx(id, c.readNow(), txAtOf(opts))
		if err != nil {
			return nil, err
		}
		if pred != nil && !pred(n) {
			return nil, storepkg.ErrNoVersionValidAt
		}
		return n, nil
	}
	return c.findNodeVersionMatchingDuring(id, opts.ValidStart, opts.ValidEnd, pred)
}

// findRelVersionForOpts is the relationship counterpart of findNodeVersionForOpts.
func (c *Core) findRelVersionForOpts(id types.RelID, opts storepkg.QueryOpts, pred func(*types.Relationship) bool) (*types.Relationship, error) {
	if c.chainLoadTestHook != nil {
		c.chainLoadTestHook()
	}
	if opts.HasTxPin() {
		// Belief-state pin — see findNodeVersionForOpts. Delegates to the same
		// relAsOfLocked the named RelAsOf / RelsAsOf door uses.
		r, err := c.relAsOfLocked(id, txPinOf(opts))
		if err != nil {
			if errors.Is(err, ErrNoVersionAsOf) {
				return nil, storepkg.ErrNoVersionValidAt
			}
			return nil, err
		}
		if pred != nil && !pred(r) {
			return nil, storepkg.ErrNoVersionValidAt
		}
		return r, nil
	}
	if opts.HasValidAt() {
		r, err := c.relAtLockedTx(id, opts.ValidAt, txAtOf(opts))
		if err != nil {
			return nil, err
		}
		if pred != nil && !pred(r) {
			return nil, storepkg.ErrNoVersionValidAt
		}
		return r, nil
	}
	if opts.HasValidInterval() {
		return c.findRelVersionMatchingDuringTx(id, opts.ValidStart, opts.ValidEnd, txAtOf(opts), pred)
	}
	if opts.HasTxAt() {
		r, err := c.relAtLockedTx(id, c.readNow(), txAtOf(opts))
		if err != nil {
			return nil, err
		}
		if pred != nil && !pred(r) {
			return nil, storepkg.ErrNoVersionValidAt
		}
		return r, nil
	}
	return c.findRelVersionMatchingDuring(id, opts.ValidStart, opts.ValidEnd, pred)
}

// findNodeVersionMatchingDuring iterates versions of node id whose validity
// overlaps [start, end), most-recent first, and returns the first version for
// which pred returns true. pred==nil returns the most-recent overlapping
// version (the original "any overlap" semantic). Returns storepkg.ErrNoVersionValidAt
// if no overlapping version satisfies pred, storepkg.ErrNodeNotFound if the node was
// never seen.
//
// This is the predicate-aware variant of "during" resolution. The "predicate
// on most-recent version" shortcut is wrong for combined queries: a label or
// property can hold during part of the interval and not on the most-recent
// version. Scanning all overlapping versions is the only correct semantic for
// "did this node match the predicate at any point during [start, end)?".
func (c *Core) findNodeVersionMatchingDuring(id types.NodeID, start, end types.Instant, pred func(*types.Node) bool) (*types.Node, error) {
	return c.findNodeVersionMatchingDuringTx(id, start, end, 0, pred)
}

// findNodeVersionMatchingDuringTx is the bitemporal variant: it filters the
// chain to versions visible at txAt before scanning for valid-time overlap.
// txAt == 0 → no TX filter (equivalent to findNodeVersionMatchingDuring).
// Callers must resolve end == 0 to a concrete bound via resolveOpenEndInstant
// before invoking.
func (c *Core) findNodeVersionMatchingDuringTx(id types.NodeID, start, end, txAt types.Instant, pred func(*types.Node) bool) (*types.Node, error) {
	current, err := c.getCurrentNode(id)
	if err != nil && !errors.Is(err, storepkg.ErrNodeNotFound) {
		return nil, err
	}
	c.afterCurrentRead(int64(id))

	history, err := c.getNodeHistory(id)
	if err != nil {
		return nil, err
	}

	if current == nil && len(history) == 0 {
		return nil, storepkg.ErrNodeNotFound
	}

	chain := chainOf(history, current)

	return c.resolveNodeChain(chain, chainProbe{kind: probeInterval, validStart: start, validEnd: end, tx: txAt}, pred)
}

// findRelVersionMatchingDuring is the relationship counterpart.
func (c *Core) findRelVersionMatchingDuring(id types.RelID, start, end types.Instant, pred func(*types.Relationship) bool) (*types.Relationship, error) {
	return c.findRelVersionMatchingDuringTx(id, start, end, 0, pred)
}

// findRelVersionMatchingDuringTx is the bitemporal variant. See findNodeVersionMatchingDuringTx.
func (c *Core) findRelVersionMatchingDuringTx(id types.RelID, start, end, txAt types.Instant, pred func(*types.Relationship) bool) (*types.Relationship, error) {
	current, err := c.getCurrentRelationship(id)
	if err != nil && !errors.Is(err, storepkg.ErrRelNotFound) {
		return nil, err
	}
	c.afterCurrentRead(int64(id))

	history, err := c.getRelHistory(id)
	if err != nil {
		return nil, err
	}

	if current == nil && len(history) == 0 {
		return nil, storepkg.ErrRelNotFound
	}

	chain := chainOf(history, current)

	return c.resolveRelChain(chain, chainProbe{kind: probeInterval, validStart: start, validEnd: end, tx: txAt}, pred)
}

// findNodeVersionRelating iterates versions of node id whose valid-interval has
// an Allen relation to the query interval [qStart, qEnd) that is a member of
// rels, most-recent-first, returning the first match (predicate-anywhere; see
// resolveNodeChainRelating). pred==nil applies no additional filter. qEnd == 0
// denotes an open query interval and is NOT pre-resolved — RelateOpen substitutes
// +∞ itself. Returns storepkg.ErrNoVersionValidAt if no version relates,
// storepkg.ErrNodeNotFound if the node was never seen.
func (c *Core) findNodeVersionRelating(id types.NodeID, qStart, qEnd types.Instant, rels types.AllenRelationSet, pred func(*types.Node) bool) (*types.Node, error) {
	current, err := c.getCurrentNode(id)
	if err != nil && !errors.Is(err, storepkg.ErrNodeNotFound) {
		return nil, err
	}
	c.afterCurrentRead(int64(id))

	history, err := c.getNodeHistory(id)
	if err != nil {
		return nil, err
	}

	if current == nil && len(history) == 0 {
		return nil, storepkg.ErrNodeNotFound
	}

	chain := chainOf(history, current)

	return c.resolveNodeChain(chain, chainProbe{kind: probeRelate, validStart: qStart, validEnd: qEnd, rels: rels}, pred)
}

// findRelVersionRelating is the relationship counterpart of findNodeVersionRelating.
func (c *Core) findRelVersionRelating(id types.RelID, qStart, qEnd types.Instant, rels types.AllenRelationSet, pred func(*types.Relationship) bool) (*types.Relationship, error) {
	current, err := c.getCurrentRelationship(id)
	if err != nil && !errors.Is(err, storepkg.ErrRelNotFound) {
		return nil, err
	}
	c.afterCurrentRead(int64(id))

	history, err := c.getRelHistory(id)
	if err != nil {
		return nil, err
	}

	if current == nil && len(history) == 0 {
		return nil, storepkg.ErrRelNotFound
	}

	chain := chainOf(history, current)

	return c.resolveRelChain(chain, chainProbe{kind: probeRelate, validStart: qStart, validEnd: qEnd, rels: rels}, pred)
}

// getNodeVersionDuring is a thin wrapper preserving the original "any overlap"
// semantic. Use findNodeVersionMatchingDuring with a predicate when the query
// has a label/property filter.
func (c *Core) getNodeVersionDuring(id types.NodeID, start, end types.Instant) (*types.Node, error) {
	return c.findNodeVersionMatchingDuring(id, start, end, nil)
}

// getRelVersionDuring is a thin wrapper preserving the original "any overlap"
// semantic for relationships.
func (c *Core) getRelVersionDuring(id types.RelID, start, end types.Instant) (*types.Relationship, error) {
	return c.findRelVersionMatchingDuring(id, start, end, nil)
}

// forEachNodeHistoryCandidateOfLabel is the candidate set of a temporal read
// by property value under label tok when no property membership sidecar
// constrains it: currentIDs (the current matches) united with the node
// history IDs that ever carried tok (the K1 label sidecar's point form),
// pruned by the effective transaction-time pin like nodeLabelCandidatesSorted.
// A node without history has only its current row, so it matches only if it
// is a current match; a node with history can match only through a version
// carrying tok, which makes it an ever-member. Before round 5 (request 8)
// every node with history was resolved, whatever its labels: a cold harness's
// churned nodes of another label cost a version chain each per seek. Without
// the lookup, or for a depth-scoped read on a store with depth-aware history,
// the full-history fold.
func (c *Core) forEachNodeHistoryCandidateOfLabel(tok uint16, currentIDs []types.NodeID, opts storepkg.QueryOpts, fn func(types.NodeID) error) error {
	if c.labelTxLookup == nil || !c.membershipSidecarUsable(opts.Depth) {
		return c.forEachNodeCandidateIDByDepth(currentIDs, opts.Depth, fn)
	}
	var history []types.NodeID
	if err := c.forEachNodeHistoryIDByDepth(opts.Depth, func(id types.NodeID) bool {
		history = append(history, id)
		return true
	}); err != nil {
		return err
	}
	seen := make(map[types.NodeID]struct{}, len(currentIDs))
	for _, id := range currentIDs {
		seen[id] = struct{}{}
	}
	if len(history) > 0 {
		pinTx := effectiveTxPin(opts)
		if err := c.labelTxLookup.LabelTxMembersOf(tok, history, func(id types.NodeID, firstTx types.Instant) bool {
			if pinTx != 0 && firstTx != 0 && firstTx > pinTx {
				return true // acquired the label strictly after the pin
			}
			seen[id] = struct{}{}
			return true
		}); err != nil {
			return err
		}
	}
	for id := range seen {
		if err := fn(id); err != nil {
			return err
		}
	}
	return nil
}

// forEachRelHistoryCandidateOfType is forEachNodeHistoryCandidateOfLabel for
// a temporal read by relationship property value under type tok (a type is
// immutable, so an ever-member of tok is a relationship of type tok).
func (c *Core) forEachRelHistoryCandidateOfType(tok uint16, currentIDs []types.RelID, opts storepkg.QueryOpts, fn func(types.RelID) error) error {
	if c.relTypeTxLookup == nil || !c.membershipSidecarUsable(opts.Depth) {
		return c.forEachRelCandidateIDByDepth(currentIDs, opts.Depth, fn)
	}
	var history []types.RelID
	if err := c.forEachRelHistoryIDByDepth(opts.Depth, func(id types.RelID) bool {
		history = append(history, id)
		return true
	}); err != nil {
		return err
	}
	seen := make(map[types.RelID]struct{}, len(currentIDs))
	for _, id := range currentIDs {
		seen[id] = struct{}{}
	}
	if len(history) > 0 {
		pinTx := effectiveTxPin(opts)
		if err := c.relTypeTxLookup.RelTypeTxMembersOf(tok, history, func(id types.RelID, firstTx types.Instant) bool {
			if pinTx != 0 && firstTx != 0 && firstTx > pinTx {
				return true
			}
			seen[id] = struct{}{}
			return true
		}); err != nil {
			return err
		}
	}
	for id := range seen {
		if err := fn(id); err != nil {
			return err
		}
	}
	return nil
}
