package core

import (
	"errors"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// =============================================================================
// Internal helpers
// =============================================================================

// resolveOpenEndInstant maps an open-ended `end == 0` upper bound to a
// concrete instant ("now + 1") so a single per-query value is shared
// across every per-ID overlap predicate. Substituting at the entry
// point — rather than inside findNodeVersionMatchingDuring per
// invocation — eliminates time drift on long iterations, where each
// per-ID call would otherwise observe a different `nowInstant()` and
// produce inclusion/exclusion that depends on iteration timing.
//
// Callers that hand `end` straight to findNodeVersionMatchingDuring /
// findRelVersionMatchingDuring MUST pass through this helper first.
func resolveOpenEndInstant(end types.Instant) types.Instant {
	if end == 0 {
		return nowInstant() + 1
	}
	return end
}

func normalizeDuringRange(start, end types.Instant) (types.Instant, error) {
	end = resolveOpenEndInstant(end)
	if start >= end {
		return 0, ErrInvalidTimeRange
	}
	return end, nil
}

// nodeValidFrom returns the effective valid-from time for a node.
// Uses explicit ValidFrom if set, falls back to snowflake ID timestamp.
func (c *Core) nodeValidFrom(n *types.Node) types.Instant {
	if tm := n.Temporal(); tm != nil && tm.ValidFrom != 0 {
		return tm.ValidFrom
	}
	return types.Instant(c.nodeIDGen.CreatedAt(n.ID().SnowflakeID()).UnixMilli())
}

// relValidFrom returns the effective valid-from time for a relationship.
// Uses explicit ValidFrom if set, falls back to snowflake ID timestamp.
func (c *Core) relValidFrom(r *types.Relationship) types.Instant {
	if tm := r.Temporal(); tm != nil && tm.ValidFrom != 0 {
		return tm.ValidFrom
	}
	return types.Instant(c.relIDGen.CreatedAt(r.ID().SnowflakeID()).UnixMilli())
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
	return at
}

func (c *Core) deleteInstantForRelationship(current *types.Relationship) types.Instant {
	return validInstantAfter(c.now(), c.relValidFrom(current))
}

func (c *Core) nodeCurrentVersionStart(n *types.Node) types.Instant {
	if n.Version() != 0 {
		if tm := n.Temporal(); tm != nil && tm.UpdatedAt != 0 {
			return tm.UpdatedAt
		}
	}
	return c.nodeValidFrom(n)
}

func (c *Core) relCurrentVersionStart(r *types.Relationship) types.Instant {
	if r.Version() != 0 {
		if tm := r.Temporal(); tm != nil && tm.UpdatedAt != 0 {
			return tm.UpdatedAt
		}
	}
	return c.relValidFrom(r)
}

func (c *Core) nodeVersionUpdateInstant(current *types.Node) types.Instant {
	return validInstantAfter(c.now(), c.nodeCurrentVersionStart(current))
}

func (c *Core) relVersionUpdateInstant(current *types.Relationship) types.Instant {
	return validInstantAfter(c.now(), c.relCurrentVersionStart(current))
}

func isClosedTemporal(tm *types.TemporalMetadata) bool {
	return tm != nil && tm.ValidTo != 0
}

func rejectClosedNodeMutation(n *types.Node) error {
	if isClosedTemporal(n.Temporal()) {
		return ErrAlreadyClosed
	}
	return nil
}

func rejectClosedRelMutation(r *types.Relationship) error {
	if isClosedTemporal(r.Temporal()) {
		return ErrAlreadyClosed
	}
	return nil
}

// isNodeValidAt checks if a node is valid at the given instant.
// Valid when: effectiveValidFrom <= t AND (ValidTo == 0 OR ValidTo > t).
func (c *Core) isNodeValidAt(n *types.Node, t types.Instant) bool {
	from := c.nodeValidFrom(n)
	if from > t {
		return false
	}
	if tm := n.Temporal(); tm != nil && tm.ValidTo != 0 {
		return tm.ValidTo > t
	}
	return true
}

// resolveNodeVersionAt finds the version valid at time t from a pre-built chain.
func (c *Core) resolveNodeVersionAt(chain []*types.Node, t types.Instant) (*types.Node, error) {
	for i := len(chain) - 1; i >= 0; i-- {
		entry := chain[i]
		if eclipsedNodeBounds(entry) {
			continue // cascade-eclipsed rows are invisible to VT queries
		}
		vStart, vEnd := c.nodeVersionBounds(chain, i)

		// Check: vStart <= t AND (vEnd == 0 OR vEnd > t).
		if vStart <= t && (vEnd == 0 || vEnd > t) {
			return entry, nil
		}
	}
	return nil, storepkg.ErrNoVersionValidAt
}

// versionVisibleAtTx reports whether a version's transaction interval
// contains txAt. txAt == 0 means "no TX filter" (return true unconditionally —
// preserves pre-bitemporal behaviour). A version without TemporalMetadata is
// treated as visible to all TX times.
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
	if tm.TxTo != 0 && tm.TxTo <= txAt {
		return false
	}
	return true
}

// filterNodeChainByTxAt returns the subset of chain whose TX interval contains
// txAt. txAt == 0 returns chain unchanged (no allocation).
func filterNodeChainByTxAt(chain []*types.Node, txAt types.Instant) []*types.Node {
	if txAt == 0 {
		return chain
	}
	out := make([]*types.Node, 0, len(chain))
	for _, entry := range chain {
		if versionVisibleAtTx(entry.Temporal(), txAt) {
			out = append(out, entry)
		}
	}
	return out
}

// filterRelChainByTxAt is the relationship counterpart of filterNodeChainByTxAt.
func filterRelChainByTxAt(chain []*types.Relationship, txAt types.Instant) []*types.Relationship {
	if txAt == 0 {
		return chain
	}
	out := make([]*types.Relationship, 0, len(chain))
	for _, entry := range chain {
		if versionVisibleAtTx(entry.Temporal(), txAt) {
			out = append(out, entry)
		}
	}
	return out
}

// nodeVersionBounds computes the effective [vStart, vEnd) for chain[i].
//
// Phase 3: vEnd uses the NEXT version's effective ValidFrom when explicit,
// not next.UpdatedAt. This auto-tiles timelines whenever the caller supplies
// tkg_valid_from on Update (Phase 2). When the next version has no explicit
// ValidFrom, fall back to UpdatedAt (preserves TX-time-as-VT-time semantics
// for callers who haven't adopted explicit valid-time).
func (c *Core) nodeVersionBounds(chain []*types.Node, i int) (types.Instant, types.Instant) {
	entry := chain[i]
	var vStart, vEnd types.Instant

	// Determine version start.
	if entry.Version() == 0 {
		vStart = c.nodeValidFrom(entry)
	} else {
		if tm := entry.Temporal(); tm != nil && tm.UpdatedAt != 0 {
			vStart = tm.UpdatedAt
		} else {
			vStart = c.nodeValidFrom(entry)
		}
	}

	// Determine version end. Use next's effective ValidFrom when set
	// (timeline tiles cleanly); otherwise fall back to next.UpdatedAt.
	// Skip eclipsed rows (cascade-marked zero-length intervals) — they are
	// invisible to VT queries and must not contribute to vEnd derivation.
	for j := i + 1; j < len(chain); j++ {
		next := chain[j]
		if eclipsedNodeBounds(next) {
			continue
		}
		if tm := next.Temporal(); tm != nil && tm.ValidFrom != 0 {
			vEnd = tm.ValidFrom
		} else if tm := next.Temporal(); tm != nil && tm.UpdatedAt != 0 {
			vEnd = tm.UpdatedAt
		} else {
			vEnd = c.nodeValidFrom(next)
		}
		break
	}
	// vEnd == 0 means open-ended (no future non-eclipsed version).

	// Post-migration: every non-zero ValidFrom is caller-supplied. Pre-
	// migration: keep the legacy inheritance heuristic for back-compat
	// (stores without MetaKVCapability never migrate).
	if tm := entry.Temporal(); tm != nil {
		if tm.ValidFrom != 0 && (c.bitemporalMigrated || !nodeInheritedValidFrom(chain, i, tm)) {
			vStart = tm.ValidFrom
		}
		if tm.ValidTo != 0 {
			vEnd = tm.ValidTo
		}
	}

	return vStart, vEnd
}

func nodeInheritedValidFrom(chain []*types.Node, i int, tm *types.TemporalMetadata) bool {
	if i == 0 || tm.UpdatedAt == 0 {
		return false
	}
	prevTM := chain[i-1].Temporal()
	return prevTM != nil && prevTM.ValidFrom != 0 && prevTM.ValidFrom == tm.ValidFrom
}

// resolveRelVersionAt finds the version valid at time t from a pre-built chain.
func (c *Core) resolveRelVersionAt(chain []*types.Relationship, t types.Instant) (*types.Relationship, error) {
	for i := len(chain) - 1; i >= 0; i-- {
		entry := chain[i]
		if eclipsedRelBounds(entry) {
			continue
		}
		vStart, vEnd := c.relVersionBounds(chain, i)

		if vStart <= t && (vEnd == 0 || vEnd > t) {
			return entry, nil
		}
	}
	return nil, storepkg.ErrNoVersionValidAt
}

// relVersionBounds computes the effective [vStart, vEnd) for chain[i].
// See nodeVersionBounds for the Phase 3 vEnd-from-next.ValidFrom rationale.
func (c *Core) relVersionBounds(chain []*types.Relationship, i int) (types.Instant, types.Instant) {
	entry := chain[i]
	var vStart, vEnd types.Instant

	if entry.Version() == 0 {
		vStart = c.relValidFrom(entry)
	} else {
		if tm := entry.Temporal(); tm != nil && tm.UpdatedAt != 0 {
			vStart = tm.UpdatedAt
		} else {
			vStart = c.relValidFrom(entry)
		}
	}

	for j := i + 1; j < len(chain); j++ {
		next := chain[j]
		if eclipsedRelBounds(next) {
			continue
		}
		if tm := next.Temporal(); tm != nil && tm.ValidFrom != 0 {
			vEnd = tm.ValidFrom
		} else if tm := next.Temporal(); tm != nil && tm.UpdatedAt != 0 {
			vEnd = tm.UpdatedAt
		} else {
			vEnd = c.relValidFrom(next)
		}
		break
	}

	if tm := entry.Temporal(); tm != nil {
		if tm.ValidFrom != 0 && (c.bitemporalMigrated || !relInheritedValidFrom(chain, i, tm)) {
			vStart = tm.ValidFrom
		}
		if tm.ValidTo != 0 {
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
	return prevTM != nil && prevTM.ValidFrom != 0 && prevTM.ValidFrom == tm.ValidFrom
}

// --- Private helpers for history-aware queries ---

// forEachNodeCandidateID iterates the union of (currentIDs, ALL node-history
// IDs). Use for history-aware label/property queries where an entity's
// CURRENT state may diverge from its state at the queried instant: a node
// currently labelled Y but historically labelled X at t must still be
// visited so the per-entity predicate can match its at-t version.
// (Adjacency queries do NOT need the full history fold — endpoints are
// immutable, so a rel that ever pointed at node N still points at N if
// alive. See forEachRelAdjacencyCandidateIDByDepth.)
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

// forEachRelCandidateID is the relationship counterpart of
// forEachNodeCandidateID — same all-history fold. For rel-type-flavored
// temporal queries (a rel's type changes via a new version), this is needed.
// For adjacency queries, prefer forEachRelAdjacencyCandidateIDByDepth.
func (c *Core) forEachRelCandidateID(currentIDs []types.RelID, fn func(types.RelID) error) error {
	return c.forEachRelCandidateIDByDepth(currentIDs, storepkg.DepthAll, fn)
}

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

// forEachRelAdjacencyCandidateID is the adjacency-query specialization of
// forEachRelCandidateID. Adjacency endpoints are immutable, so a rel that
// ever pointed at the queried node still points at it if alive — therefore
// the candidate set is (currentIDs ∪ DELETED rel IDs), not (currentIDs ∪ ALL
// rel history). When the underlying store implements
// DeletedIterationCapability the fold scales with deleted-rel count, not
// total rel history; otherwise it falls back to the wider history scan.
func (c *Core) forEachRelAdjacencyCandidateID(currentIDs []types.RelID, fn func(types.RelID) error) error {
	return c.forEachRelAdjacencyCandidateIDByDepth(currentIDs, storepkg.DepthAll, fn)
}

func (c *Core) forEachRelAdjacencyCandidateIDByDepth(currentIDs []types.RelID, depth storepkg.ShardDepth, fn func(types.RelID) error) error {
	seen := make(map[types.RelID]struct{}, len(currentIDs))
	for _, id := range currentIDs {
		seen[id] = struct{}{}
	}
	if err := c.forEachDeletedRelIDByDepth(depth, func(id types.RelID) bool {
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

// forEachDeletedRelIDByDepth prefers the store's DeletedIterationCapability
// (depth-aware variant first, then plain) so the fold visits only rel IDs
// whose history exists but whose current row is absent. Without either
// capability the graph layer falls back to the wider history scan —
// correct but O(total rel history). Only used by
// forEachRelAdjacencyCandidateIDByDepth today; the matching node helper is
// intentionally absent because no node temporal API has the "endpoints are
// immutable" simplification that lets adjacency drop full-history fold.
func (c *Core) forEachDeletedRelIDByDepth(depth storepkg.ShardDepth, fn func(types.RelID) bool) error {
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
		err := c.forEachDeletedRelIDByDepthTrusted(depth, wrapped)
		if invalid != nil {
			return invalid
		}
		return err
	}
	return c.forEachDeletedRelIDByDepthTrusted(depth, wrapped)
}

func (c *Core) forEachDeletedRelIDByDepthTrusted(depth storepkg.ShardDepth, fn func(types.RelID) bool) error {
	if depth != storepkg.DepthAll && c.deletedDepthIter != nil {
		return c.deletedDepthIter.ForEachDeletedRelIDByDepth(depth, fn)
	}
	if c.deletedIter != nil && depth == storepkg.DepthAll {
		return c.deletedIter.ForEachDeletedRelID(fn)
	}
	return c.forEachRelHistoryIDByDepthTrusted(depth, fn)
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
// requires history-aware resolution. Matches the Store-level storepkg.QueryOpts
// contract: ValidStart/ValidEnd form an interval filter ONLY when both are
// greater than zero; a single one-sided or non-positive bound is treated as "no
// filter" so the call falls through to the non-temporal fast path. Without this
// guard the interval predicate `vStart < end && (vEnd == 0 || vEnd > start)`
// collapses (e.g. with end <= 0) and rejects every entity, regressing
// AllNodes(storepkg.QueryOpts{ValidStart: t}) and similar non-active interval
// callers.
//
// Implemented as a free function rather than a method on storepkg.QueryOpts because
// storepkg.QueryOpts is now a type alias to internal/store.storepkg.QueryOpts after the
// v3.1.17 restructure, and Go forbids defining methods on a non-local
// aliased type.
func hasTemporalFilter(opts storepkg.QueryOpts) bool {
	if opts.ValidAt != 0 {
		return true
	}
	if opts.TxAt != 0 {
		return true
	}
	return opts.ValidStart > 0 && opts.ValidEnd > 0
}

// findNodeVersionForOpts returns a node version that satisfies pred under the
// temporal filter in opts. ValidAt takes precedence over ValidStart/ValidEnd.
// For interval queries, all overlapping versions are scanned (most-recent
// first) and the first match is returned — so a node whose label/property held
// at any moment in the interval is found, even if a later version no longer
// matches. Returns storepkg.ErrNoVersionValidAt if no overlapping version satisfies
// pred. pred==nil means "any overlapping version".
func (c *Core) findNodeVersionForOpts(id types.NodeID, opts storepkg.QueryOpts, pred func(*types.Node) bool) (*types.Node, error) {
	if opts.ValidAt != 0 {
		n, err := c.nodeAtLockedTx(id, opts.ValidAt, opts.TxAt)
		if err != nil {
			return nil, err
		}
		if pred != nil && !pred(n) {
			return nil, storepkg.ErrNoVersionValidAt
		}
		return n, nil
	}
	if opts.ValidStart > 0 && opts.ValidEnd > 0 {
		return c.findNodeVersionMatchingDuringTx(id, opts.ValidStart, opts.ValidEnd, opts.TxAt, pred)
	}
	if opts.TxAt != 0 {
		// TX-only filter: return version visible at txAt as of "now" valid time
		// — i.e. whatever was current-or-most-recent at txAt.
		n, err := c.nodeAtLockedTx(id, resolveOpenEndInstant(0)-1, opts.TxAt)
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
	if opts.ValidAt != 0 {
		r, err := c.relAtLockedTx(id, opts.ValidAt, opts.TxAt)
		if err != nil {
			return nil, err
		}
		if pred != nil && !pred(r) {
			return nil, storepkg.ErrNoVersionValidAt
		}
		return r, nil
	}
	if opts.ValidStart > 0 && opts.ValidEnd > 0 {
		return c.findRelVersionMatchingDuringTx(id, opts.ValidStart, opts.ValidEnd, opts.TxAt, pred)
	}
	if opts.TxAt != 0 {
		r, err := c.relAtLockedTx(id, resolveOpenEndInstant(0)-1, opts.TxAt)
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

	history, err := c.getNodeHistory(id)
	if err != nil {
		return nil, err
	}

	if current == nil && len(history) == 0 {
		return nil, storepkg.ErrNodeNotFound
	}

	chain := make([]*types.Node, 0, len(history)+1)
	chain = append(chain, history...)
	if current != nil {
		chain = append(chain, current)
	}

	chain = filterNodeChainByTxAt(chain, txAt)
	if len(chain) == 0 {
		return nil, storepkg.ErrNoVersionValidAt
	}

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

	history, err := c.getRelHistory(id)
	if err != nil {
		return nil, err
	}

	if current == nil && len(history) == 0 {
		return nil, storepkg.ErrRelNotFound
	}

	chain := make([]*types.Relationship, 0, len(history)+1)
	chain = append(chain, history...)
	if current != nil {
		chain = append(chain, current)
	}

	chain = filterRelChainByTxAt(chain, txAt)
	if len(chain) == 0 {
		return nil, storepkg.ErrNoVersionValidAt
	}

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
