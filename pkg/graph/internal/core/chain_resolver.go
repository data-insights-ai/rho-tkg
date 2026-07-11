package core

import (
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
// the "never existed" ErrNodeNotFound verdict. resolveNodeChain implements, and
// is the ONLY place that implements:
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
}

// resolveNodeChain is the single node-side selection seam. pred is consulted
// ONLY for probeInterval (predicate-anywhere matching); point and as-of callers
// apply their own post-filter, mirroring the pre-refactor division of
// responsibility (findNodeVersionMatchingDuringTx took a predicate;
// nodeAtLockedTx / nodeAsOfLocked did not).
func (c *Core) resolveNodeChain(chain []*types.Node, probe chainProbe, pred func(*types.Node) bool) (*types.Node, error) {
	if probe.kind == probeAsOf {
		return c.resolveNodeChainAsOf(chain, probe.tx)
	}
	chain = filterNodeChainByTxAt(chain, probe.tx)
	if len(chain) == 0 {
		return nil, storepkg.ErrNoVersionValidAt
	}
	if probe.kind == probeInterval {
		return c.resolveNodeChainDuring(chain, probe.validStart, probe.validEnd, pred)
	}
	return c.resolveNodeVersionAt(chain, probe.validAt)
}

// resolveNodeChainDuring scans a TxAt-filtered chain for a version whose
// validity overlaps [start, end) and (when pred != nil) satisfies pred,
// most-recent-first so the newest overlapping match is preferred. See the
// funnel comment for the predicate-anywhere rationale.
func (c *Core) resolveNodeChainDuring(chain []*types.Node, start, end types.Instant, pred func(*types.Node) bool) (*types.Node, error) {
	// Order by effective valid-from so next-version tiling is correct after an
	// append-only cascade (see sortNodeChainForResolve). Scan highest-valid-from
	// first to preserve the "most-recent overlapping match" semantic.
	c.sortNodeChainForResolve(chain)
	for i := len(chain) - 1; i >= 0; i-- {
		if eclipsedNodeBounds(chain[i]) {
			continue
		}
		vStart, vEnd := c.nodeVersionBounds(chain, i)
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
func (c *Core) resolveNodeChainAsOf(chain []*types.Node, txPin types.Instant) (*types.Node, error) {
	best, ok := storeutil.SelectAsOf(chain, txPin)
	if !ok {
		return nil, ErrNoVersionAsOf
	}
	return nodeVisibleAtTxTime(best, txPin), nil
}

// resolveRelChain is the relationship-side mirror of resolveNodeChain.
func (c *Core) resolveRelChain(chain []*types.Relationship, probe chainProbe, pred func(*types.Relationship) bool) (*types.Relationship, error) {
	if probe.kind == probeAsOf {
		return c.resolveRelChainAsOf(chain, probe.tx)
	}
	chain = filterRelChainByTxAt(chain, probe.tx)
	if len(chain) == 0 {
		return nil, storepkg.ErrNoVersionValidAt
	}
	if probe.kind == probeInterval {
		return c.resolveRelChainDuring(chain, probe.validStart, probe.validEnd, pred)
	}
	return c.resolveRelVersionAt(chain, probe.validAt)
}

// resolveRelChainDuring mirrors resolveNodeChainDuring for relationships.
func (c *Core) resolveRelChainDuring(chain []*types.Relationship, start, end types.Instant, pred func(*types.Relationship) bool) (*types.Relationship, error) {
	c.sortRelChainForResolve(chain)
	for i := len(chain) - 1; i >= 0; i-- {
		if eclipsedRelBounds(chain[i]) {
			continue
		}
		vStart, vEnd := c.relVersionBounds(chain, i)
		if vStart < end && (vEnd == 0 || vEnd > start) {
			if pred == nil || pred(chain[i]) {
				return chain[i], nil
			}
		}
	}
	return nil, storepkg.ErrNoVersionValidAt
}

// resolveRelChainAsOf mirrors resolveNodeChainAsOf for relationships.
func (c *Core) resolveRelChainAsOf(chain []*types.Relationship, txPin types.Instant) (*types.Relationship, error) {
	best, ok := storeutil.SelectAsOf(chain, txPin)
	if !ok {
		return nil, ErrNoVersionAsOf
	}
	return relVisibleAtTxTime(best, txPin), nil
}
