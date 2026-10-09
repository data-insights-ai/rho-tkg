package core

import (
	"cmp"
	"slices"

	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// =============================================================================
// Resolution funnel
//
// resolveNodeChain / resolveRelChain are THE single seam through which every
// core-layer temporal read selects a version from a pre-built version chain.
// Both the named doors (NodeAt / NodeAtTx / NodesDuring / NodesAsOf and their
// relationship mirrors) and the generic QueryOpts doors (ByLabel / ByType / All
// with a temporal filter, via findNodeVersionForOpts) route their per-candidate
// selection here. Concentrating the selection rules in one place is the whole
// point: a fix lands once and cannot drift between the two doors (testing
// rule 17).
//
// The chain handed in is (history ‖ current); the caller owns loading it and
// the "never existed" ErrNodeNotFound verdict.
//
// INPUT CONTRACT — ascending-version order, single use: the chain MUST arrive
// in ascending version order (history rows as stored ‖ current last), and the
// resolver may SORT the slice IN PLACE (sortNodeChainForResolve, cascade
// chains). Monotonicity detection is RELATIVE to the input order, so feeding a
// previously-resolved (sorted-by-valid-from) chain back in flips the
// monotonic-vs-cascade branch and silently changes bounds derivation
// (positional tiling vs own-bounds) — a caller that resolves twice (e.g. the
// skeleton-hydration fast path in nodeAtViaTemporalMeta) must hand EACH run
// its own copy of the pristine ascending-version chain.
//
// resolveNodeChain implements, and is the ONLY place that implements:
//
//   - TX visibility: a version is recorded-by-then iff TxFrom <= txAt; TxTo does
//     NOT bound visibility — superseded is not retracted (lesson 43). This lives
//     in filterNodeChainByTxAt, shared by the point and interval kinds.
//   - Tombstone normalization at a pre-delete pin: a surviving row whose delete
//     stamps post-date the pin is deep-copied and normalized to its then-belief
//     so a hard Delete does not silently rewrite valid-time history for pins
//     BEFORE the delete (lesson 60). Also in filterNodeChainByTxAt.
//   - Version-interval derivation [vStart, vEnd): the ValidTo override and the
//     next-version ValidFrom/UpdatedAt fallback (lessons 32/33/42), in
//     nodeVersionBounds.
//   - Newest-belief selection on overlap: highest (TxFrom, version) wins
//     (lessons 46/62), in resolveNodeVersionAt (point) and resolveNodeChainAsOf.
//   - Predicate-anywhere interval matching: a version that satisfied the
//     predicate during ANY part of [start, end) is found even when a later
//     overlapping version no longer matches (rule 16), in resolveNodeChainDuring.
//   - The as-of retraction rule: if the decisive newest belief was already
//     retracted/deleted by the pin, the entity is ABSENT — never fall through to
//     an older open row (lesson 62). Both the newest-belief-by-version selection
//     and the retraction rule live in the ONE shared storeutil.SelectAsOf, which
//     resolveNodeChainAsOf / resolveRelChainAsOf delegate to (the same rule the
//     memory backend consumes and the badger native reverse-scan is proven
//     equivalent to).
// =============================================================================

// probeKind selects which valid-/transaction-time selection rule a chainProbe
// asks resolveNodeChain / resolveRelChain to apply.
type probeKind uint8

const (
	// probePoint resolves the single version covering ValidAt, after filtering
	// the chain to versions recorded by TxAt (TxAt == 0 = no TX filter).
	probePoint probeKind = iota
	// probeInterval resolves any version overlapping [ValidStart, ValidEnd) that
	// satisfies the caller's predicate (predicate-anywhere), after the same TxAt
	// filter. ValidEnd must already be resolved to a concrete bound (open-ended
	// end mapped via resolveOpenEndInstant by the caller).
	probeInterval
	// probeAsOf is the pure knowledge-time belief-state pin: NO valid-time
	// filter. It selects the newest belief recorded by Tx (the TxPin) and applies
	// the retraction rule. The chain handed in must include the current row when
	// the current row is a candidate (see nodeAsOfLocked).
	probeAsOf
	// probeRelate resolves any version whose valid-interval [vStart, vEnd) has an
	// Allen relation to the query interval [validStart, validEnd) that is a member
	// of rels — predicate-anywhere, most-recent-first, like probeInterval. UNLIKE
	// probeInterval, validEnd is the RAW query end (0 = open, +∞): the classifier
	// types.RelateOpen substitutes +∞ itself, so pre-resolving the open end to a
	// concrete "now+" bound (as probeInterval requires) would corrupt Before/After
	// classification. No TX filter is applied (tx must be 0).
	probeRelate
)

// chainProbe is the query specification handed to resolveNodeChain /
// resolveRelChain. Exactly one valid-time selector is meaningful per kind
// (validAt for probePoint; validStart/validEnd for probeInterval; none for
// probeAsOf). tx is the transaction-time input: a TxAt filter for point/interval
// (0 = none) and the TxPin belief instant for probeAsOf.
type chainProbe struct {
	kind       probeKind
	validAt    types.Instant
	validStart types.Instant
	validEnd   types.Instant
	tx         types.Instant
	// rels is meaningful only for probeRelate: the set of Allen relations a
	// version's valid-interval must have with [validStart, validEnd) to match.
	rels types.AllenRelationSet
	// asOfCurrent is meaningful only for probeAsOf: the chain's last row is
	// the live current row (storeutil.SelectAsOfWithCurrent's current arm).
	asOfCurrent bool
}

// versionOrdered returns chain in ascending version order: chain itself when it
// already is, else a sorted copy (stable, so rows of equal version keep their
// order). The resolver's monotonic-vs-cascade classification and the
// positional tiling are relative to its input order (lesson 73), and
// "history ‖ current" is NOT ascending when a bounded SetVersionInterval left
// the current row in its slot below the rows it appended: the same chain then
// resolved one way while the row was current and another once a delete moved
// it to history (a closed entity read as valid again after a cascade).
func versionOrdered[T storeutil.TemporalRow](chain []T) []T {
	byVersion := func(a, b T) int { return cmp.Compare(a.Version(), b.Version()) }
	if slices.IsSortedFunc(chain, byVersion) {
		return chain
	}
	out := slices.Clone(chain)
	slices.SortStableFunc(out, byVersion)
	return out
}

// selectAsOfChain runs storeutil.SelectAsOfWithCurrent over a chain whose last
// row is the live current row when lastIsCurrent.
func selectAsOfChain[T storeutil.TemporalRow](chain []T, pin types.Instant, lastIsCurrent bool) (T, bool) {
	if lastIsCurrent && len(chain) > 0 {
		return storeutil.SelectAsOfWithCurrent(chain[:len(chain)-1], chain[len(chain)-1], true, pin)
	}
	return storeutil.SelectAsOf(chain, pin)
}

// lifeEnds maps each row of a TxAt-filtered chain recorded before a hard
// delete the chain holds to that delete's instant (handover 2a). A delete
// tombstones only the row holding the current slot; an older row of the same
// life that the cascade's own-bounds arm keeps open (the genesis under a
// bounded correction) would otherwise stay valid forever after the delete. A
// row's life is the span up to the first delete recorded at or after it, so a
// re-imported ID's later rows (recorded after the delete) are not capped. A
// nil map caps nothing.
type lifeEnds[T comparable] map[T]types.Instant

// chainLifeEnds builds the lifeEnds of a (TxAt-filtered, tombstone-normalized)
// chain: nil when no row carries a DeletedAt.
func chainLifeEnds[T interface {
	comparable
	storeutil.TemporalRow
}](chain []T) lifeEnds[T] {
	var deaths []types.Instant
	for _, r := range chain {
		if tm := r.Temporal(); tm != nil && tm.DeletedAt != 0 {
			deaths = append(deaths, tm.DeletedAt)
		}
	}
	if len(deaths) == 0 {
		return nil
	}
	slices.Sort(deaths)
	caps := make(lifeEnds[T], len(chain))
	for _, r := range chain {
		var recorded types.Instant
		if tm := r.Temporal(); tm != nil {
			recorded = tm.TxFrom
		}
		if i, _ := slices.BinarySearch(deaths, recorded); i < len(deaths) {
			caps[r] = deaths[i]
		}
	}
	return caps
}

// end caps a row's valid end at its life end (0 = open).
func (m lifeEnds[T]) end(row T, vEnd types.Instant) types.Instant {
	if lifeEnd, ok := m[row]; ok && (vEnd == 0 || vEnd > lifeEnd) {
		return lifeEnd
	}
	return vEnd
}

// cut is end for the interval doors, which test overlap rather than point
// coverage: gone reports a row whose capped interval is empty.
func (m lifeEnds[T]) cut(row T, vStart, vEnd types.Instant) (types.Instant, bool) {
	lifeEnd, ok := m[row]
	if !ok || (vEnd != 0 && vEnd <= lifeEnd) {
		return vEnd, false
	}
	return lifeEnd, lifeEnd <= vStart
}

// resolveNodeChain is the single node-side selection seam. pred is consulted
// ONLY for probeInterval (predicate-anywhere matching); point and as-of callers
// apply their own post-filter, mirroring the pre-refactor division of
// responsibility (findNodeVersionMatchingDuringTx took a predicate;
// nodeAtLockedTx / nodeAsOfLocked did not).
func (c *Core) resolveNodeChain(chain []*types.Node, probe chainProbe, pred func(*types.Node) bool) (*types.Node, error) {
	if probe.kind == probeAsOf {
		return c.resolveNodeChainAsOf(chain, probe.tx, probe.asOfCurrent)
	}
	chain = filterNodeChainByTxAt(versionOrdered(chain), probe.tx)
	if len(chain) == 0 {
		return nil, storepkg.ErrNoVersionValidAt
	}
	caps := chainLifeEnds(chain)
	if probe.kind == probeInterval {
		return c.resolveNodeChainDuring(chain, probe.validStart, probe.validEnd, pred, caps)
	}
	if probe.kind == probeRelate {
		return c.resolveNodeChainRelating(chain, probe.validStart, probe.validEnd, probe.rels, pred, caps)
	}
	return c.resolveNodeVersionAtCapped(chain, probe.validAt, caps)
}

// resolveNodeChainRelating scans a chain for a version whose valid-interval has
// an Allen relation to [qStart, qEnd) that is a member of rels, most-recent-first
// (predicate-anywhere — the same rationale as resolveNodeChainDuring). qEnd == 0
// denotes an open query interval; types.RelateOpen substitutes +∞ for both the
// query's and the version's open ends, so Before/After/Meets classify exactly.
func (c *Core) resolveNodeChainRelating(chain []*types.Node, qStart, qEnd types.Instant, rels types.AllenRelationSet, pred func(*types.Node) bool, caps lifeEnds[*types.Node]) (*types.Node, error) {
	if rels == 0 {
		return nil, storepkg.ErrNoVersionValidAt
	}
	c.sortNodeChainForResolve(chain)
	for i := len(chain) - 1; i >= 0; i-- {
		vStart, vEnd := c.nodeVersionBounds(chain, i)
		vEnd, gone := caps.cut(chain[i], vStart, vEnd)
		if gone {
			continue
		}
		rel, err := types.RelateOpen(vStart, vEnd, qStart, qEnd)
		if err != nil {
			continue
		}
		if rels.Contains(rel) && (pred == nil || pred(chain[i])) {
			return chain[i], nil
		}
	}
	return nil, storepkg.ErrNoVersionValidAt
}

// resolveNodeChainDuring scans a TxAt-filtered chain for a version whose
// validity overlaps [start, end) and (when pred != nil) satisfies pred,
// most-recent-first so the newest overlapping match is preferred. See the
// funnel comment for the predicate-anywhere rationale.
func (c *Core) resolveNodeChainDuring(chain []*types.Node, start, end types.Instant, pred func(*types.Node) bool, caps lifeEnds[*types.Node]) (*types.Node, error) {
	// Order by effective valid-from so next-version tiling is correct after an
	// append-only cascade (see sortNodeChainForResolve). Scan highest-valid-from
	// first to preserve the "most-recent overlapping match" semantic.
	c.sortNodeChainForResolve(chain)
	for i := len(chain) - 1; i >= 0; i-- {
		vStart, vEnd := c.nodeVersionBounds(chain, i)
		vEnd, gone := caps.cut(chain[i], vStart, vEnd)
		if gone {
			continue
		}
		// Overlap: vStart < end AND (vEnd == 0 OR vEnd > start).
		if vStart < end && (vEnd == 0 || vEnd > start) {
			if pred == nil || pred(chain[i]) {
				return chain[i], nil
			}
		}
	}
	return nil, storepkg.ErrNoVersionValidAt
}

// resolveNodeChainAsOf selects the newest belief recorded by txPin via the
// shared storeutil.SelectAsOf (newest version with TxFrom <= txPin, absent if
// that decisive belief was retracted/deleted; lesson 62) and normalizes the
// survivor to its then-visible state. Returns ErrNoVersionAsOf when SelectAsOf
// reports the entity absent at the pin.
func (c *Core) resolveNodeChainAsOf(chain []*types.Node, txPin types.Instant, lastIsCurrent bool) (*types.Node, error) {
	best, ok := selectAsOfChain(chain, txPin, lastIsCurrent)
	if !ok {
		return nil, ErrNoVersionAsOf
	}
	// Chain rows may be shared frozen store rows — never mutate them in place
	// (lesson 60, mirroring filterNodeChainByTxAt's discipline).
	return nodeVisibleAtTxTime(best.DeepCopy(), txPin), nil
}

// resolveRelChain is the relationship-side mirror of resolveNodeChain.
func (c *Core) resolveRelChain(chain []*types.Relationship, probe chainProbe, pred func(*types.Relationship) bool) (*types.Relationship, error) {
	if probe.kind == probeAsOf {
		return c.resolveRelChainAsOf(chain, probe.tx, probe.asOfCurrent)
	}
	chain = filterRelChainByTxAt(versionOrdered(chain), probe.tx)
	if len(chain) == 0 {
		return nil, storepkg.ErrNoVersionValidAt
	}
	caps := chainLifeEnds(chain)
	if probe.kind == probeInterval {
		return c.resolveRelChainDuring(chain, probe.validStart, probe.validEnd, pred, caps)
	}
	if probe.kind == probeRelate {
		return c.resolveRelChainRelating(chain, probe.validStart, probe.validEnd, probe.rels, pred, caps)
	}
	return c.resolveRelVersionAtCapped(chain, probe.validAt, caps)
}

// resolveRelChainRelating mirrors resolveNodeChainRelating for relationships.
func (c *Core) resolveRelChainRelating(chain []*types.Relationship, qStart, qEnd types.Instant, rels types.AllenRelationSet, pred func(*types.Relationship) bool, caps lifeEnds[*types.Relationship]) (*types.Relationship, error) {
	if rels == 0 {
		return nil, storepkg.ErrNoVersionValidAt
	}
	c.sortRelChainForResolve(chain)
	for i := len(chain) - 1; i >= 0; i-- {
		vStart, vEnd := c.relVersionBounds(chain, i)
		vEnd, gone := caps.cut(chain[i], vStart, vEnd)
		if gone {
			continue
		}
		rel, err := types.RelateOpen(vStart, vEnd, qStart, qEnd)
		if err != nil {
			continue
		}
		if rels.Contains(rel) && (pred == nil || pred(chain[i])) {
			return chain[i], nil
		}
	}
	return nil, storepkg.ErrNoVersionValidAt
}

// resolveRelChainDuring mirrors resolveNodeChainDuring for relationships.
func (c *Core) resolveRelChainDuring(chain []*types.Relationship, start, end types.Instant, pred func(*types.Relationship) bool, caps lifeEnds[*types.Relationship]) (*types.Relationship, error) {
	c.sortRelChainForResolve(chain)
	for i := len(chain) - 1; i >= 0; i-- {
		vStart, vEnd := c.relVersionBounds(chain, i)
		vEnd, gone := caps.cut(chain[i], vStart, vEnd)
		if gone {
			continue
		}
		if vStart < end && (vEnd == 0 || vEnd > start) {
			if pred == nil || pred(chain[i]) {
				return chain[i], nil
			}
		}
	}
	return nil, storepkg.ErrNoVersionValidAt
}

// resolveRelChainAsOf mirrors resolveNodeChainAsOf for relationships.
func (c *Core) resolveRelChainAsOf(chain []*types.Relationship, txPin types.Instant, lastIsCurrent bool) (*types.Relationship, error) {
	best, ok := selectAsOfChain(chain, txPin, lastIsCurrent)
	if !ok {
		return nil, ErrNoVersionAsOf
	}
	// Chain rows may be shared frozen store rows — never mutate them in place
	// (lesson 60, mirroring filterRelChainByTxAt's discipline).
	return relVisibleAtTxTime(best.DeepCopy(), txPin), nil
}
