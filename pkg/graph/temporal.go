package graph

import (
	"errors"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"

	snowflake "github.com/bds421/rho-snowflake-2026"

	temporalpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/temporal"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
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

// nodeValidFrom returns the effective valid-from time for a node.
// Uses explicit ValidFrom if set, falls back to snowflake ID timestamp.
func (g *Graph) nodeValidFrom(n *types.Node) types.Instant {
	if tm := n.Temporal(); tm != nil && tm.ValidFrom != 0 {
		return tm.ValidFrom
	}
	return types.Instant(g.nodeIDGen.CreatedAt(n.ID().SnowflakeID()).UnixMilli())
}

// relValidFrom returns the effective valid-from time for a relationship.
// Uses explicit ValidFrom if set, falls back to snowflake ID timestamp.
func (g *Graph) relValidFrom(r *types.Relationship) types.Instant {
	if tm := r.Temporal(); tm != nil && tm.ValidFrom != 0 {
		return tm.ValidFrom
	}
	return types.Instant(g.relIDGen.CreatedAt(r.ID().SnowflakeID()).UnixMilli())
}

// isNodeValidAt checks if a node is valid at the given instant.
// Valid when: effectiveValidFrom <= t AND (ValidTo == 0 OR ValidTo > t).
func (g *Graph) isNodeValidAt(n *types.Node, t types.Instant) bool {
	from := g.nodeValidFrom(n)
	if from > t {
		return false
	}
	if tm := n.Temporal(); tm != nil && tm.ValidTo != 0 {
		return tm.ValidTo > t
	}
	return true
}

// isRelValidAt checks if a relationship is valid at the given instant.
func (g *Graph) isRelValidAt(r *types.Relationship, t types.Instant) bool {
	from := g.relValidFrom(r)
	if from > t {
		return false
	}
	if tm := r.Temporal(); tm != nil && tm.ValidTo != 0 {
		return tm.ValidTo > t
	}
	return true
}

// resolveNodeVersionAt finds the version valid at time t from a pre-built chain.
func (g *Graph) resolveNodeVersionAt(chain []*types.Node, t types.Instant) (*types.Node, error) {
	for i := len(chain) - 1; i >= 0; i-- {
		entry := chain[i]
		vStart, vEnd := g.nodeVersionBounds(chain, i)

		// Check: vStart <= t AND (vEnd == 0 OR vEnd > t).
		if vStart <= t && (vEnd == 0 || vEnd > t) {
			return entry, nil
		}
	}
	return nil, storepkg.ErrNoVersionValidAt
}

// nodeVersionBounds computes the effective [vStart, vEnd) for chain[i].
func (g *Graph) nodeVersionBounds(chain []*types.Node, i int) (types.Instant, types.Instant) {
	entry := chain[i]
	var vStart, vEnd types.Instant

	// Determine version start.
	if entry.Version() == 0 {
		vStart = g.nodeValidFrom(entry)
	} else {
		if tm := entry.Temporal(); tm != nil && tm.UpdatedAt != 0 {
			vStart = tm.UpdatedAt
		} else {
			vStart = g.nodeValidFrom(entry)
		}
	}

	// Determine version end.
	if i < len(chain)-1 {
		next := chain[i+1]
		if tm := next.Temporal(); tm != nil && tm.UpdatedAt != 0 {
			vEnd = tm.UpdatedAt
		} else {
			vEnd = g.nodeValidFrom(next)
		}
	}
	// vEnd == 0 means open-ended (current version).

	// Explicit ValidFrom/ValidTo override derived values.
	if tm := entry.Temporal(); tm != nil {
		if tm.ValidFrom != 0 {
			vStart = tm.ValidFrom
		}
		if tm.ValidTo != 0 {
			vEnd = tm.ValidTo
		}
	}

	return vStart, vEnd
}

// resolveRelVersionAt finds the version valid at time t from a pre-built chain.
func (g *Graph) resolveRelVersionAt(chain []*types.Relationship, t types.Instant) (*types.Relationship, error) {
	for i := len(chain) - 1; i >= 0; i-- {
		entry := chain[i]
		vStart, vEnd := g.relVersionBounds(chain, i)

		if vStart <= t && (vEnd == 0 || vEnd > t) {
			return entry, nil
		}
	}
	return nil, storepkg.ErrNoVersionValidAt
}

// relVersionBounds computes the effective [vStart, vEnd) for chain[i].
func (g *Graph) relVersionBounds(chain []*types.Relationship, i int) (types.Instant, types.Instant) {
	entry := chain[i]
	var vStart, vEnd types.Instant

	if entry.Version() == 0 {
		vStart = g.relValidFrom(entry)
	} else {
		if tm := entry.Temporal(); tm != nil && tm.UpdatedAt != 0 {
			vStart = tm.UpdatedAt
		} else {
			vStart = g.relValidFrom(entry)
		}
	}

	if i < len(chain)-1 {
		next := chain[i+1]
		if tm := next.Temporal(); tm != nil && tm.UpdatedAt != 0 {
			vEnd = tm.UpdatedAt
		} else {
			vEnd = g.relValidFrom(next)
		}
	}

	if tm := entry.Temporal(); tm != nil {
		if tm.ValidFrom != 0 {
			vStart = tm.ValidFrom
		}
		if tm.ValidTo != 0 {
			vEnd = tm.ValidTo
		}
	}

	return vStart, vEnd
}

// --- Private helpers for history-aware queries ---

// forEachNodeCandidateID iterates the union of (currentIDs, history node IDs)
// without scanning the full current ID set via ForEachNodeID. Use when the
// caller already has a narrow indexed candidate list (label, property, or
// adjacency index) — folding only the deleted/historical node IDs on top
// preserves history-aware semantics without paying for a full table scan.
func (g *Graph) forEachNodeCandidateID(currentIDs []types.NodeID, fn func(types.NodeID) error) error {
	seen := make(map[types.NodeID]struct{}, len(currentIDs))
	for _, id := range currentIDs {
		seen[id] = struct{}{}
	}
	if err := g.store.ForEachNodeHistoryID(func(id types.NodeID) bool {
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
// forEachNodeCandidateID — same indexed-candidates + history-IDs union
// without scanning ForEachRelID.
func (g *Graph) forEachRelCandidateID(currentIDs []types.RelID, fn func(types.RelID) error) error {
	seen := make(map[types.RelID]struct{}, len(currentIDs))
	for _, id := range currentIDs {
		seen[id] = struct{}{}
	}
	if err := g.store.ForEachRelHistoryID(func(id types.RelID) bool {
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
func (g *Graph) forEachKnownNodeID(fn func(types.NodeID) error) error {
	seen := make(map[types.NodeID]struct{})

	// Phase 1: collect unique IDs (no store method calls in callbacks — lock reentrancy).
	if err := g.store.ForEachNodeID(func(id types.NodeID) bool {
		seen[id] = struct{}{}
		return true
	}); err != nil {
		return err
	}
	if err := g.store.ForEachNodeHistoryID(func(id types.NodeID) bool {
		seen[id] = struct{}{}
		return true
	}); err != nil {
		return err
	}

	// Phase 2: process (store locks released, safe to call GetNodeAt etc.).
	for id := range seen {
		if err := fn(id); err != nil {
			return err
		}
	}
	return nil
}

// forEachKnownRelID collects the union of current + history relationship IDs
// via lazy ForEach iteration, then calls fn for each unique ID.
// Two-phase: collect (under store locks), then process (locks released).
//
// Memory note: same O(N) materialisation trade-off as forEachKnownNodeID.
// See that function's doc comment for the rationale.
func (g *Graph) forEachKnownRelID(fn func(types.RelID) error) error {
	seen := make(map[types.RelID]struct{})

	// Phase 1: collect unique IDs.
	if err := g.store.ForEachRelID(func(id types.RelID) bool {
		seen[id] = struct{}{}
		return true
	}); err != nil {
		return err
	}
	if err := g.store.ForEachRelHistoryID(func(id types.RelID) bool {
		seen[id] = struct{}{}
		return true
	}); err != nil {
		return err
	}

	// Phase 2: process (store locks released).
	for id := range seen {
		if err := fn(id); err != nil {
			return err
		}
	}
	return nil
}

// hasTemporalFilter reports whether opts carries a temporal filter that
// requires history-aware resolution. Matches the Store-level storepkg.QueryOpts
// contract: ValidStart/ValidEnd form an interval filter ONLY when both are
// set; a single one-sided bound is treated as "no filter" so the call falls
// through to the non-temporal fast path. Without this guard the interval
// predicate `vStart < end && (vEnd == 0 || vEnd > start)` collapses
// (e.g. with end == 0) and rejects every entity, regressing
// AllNodes(storepkg.QueryOpts{ValidStart: t}) and similar one-sided callers.
//
// Implemented as a free function rather than a method on storepkg.QueryOpts because
// storepkg.QueryOpts is now a type alias to internal/store.storepkg.QueryOpts after the
// v3.1.17 restructure, and Go forbids defining methods on a non-local
// aliased type.
func hasTemporalFilter(opts storepkg.QueryOpts) bool {
	if opts.ValidAt != 0 {
		return true
	}
	return opts.ValidStart != 0 && opts.ValidEnd != 0
}

// findNodeVersionForOpts returns a node version that satisfies pred under the
// temporal filter in opts. ValidAt takes precedence over ValidStart/ValidEnd.
// For interval queries, all overlapping versions are scanned (most-recent
// first) and the first match is returned — so a node whose label/property held
// at any moment in the interval is found, even if a later version no longer
// matches. Returns storepkg.ErrNoVersionValidAt if no overlapping version satisfies
// pred. pred==nil means "any overlapping version".
func (g *Graph) findNodeVersionForOpts(id types.NodeID, opts storepkg.QueryOpts, pred func(*types.Node) bool) (*types.Node, error) {
	if opts.ValidAt != 0 {
		n, err := g.GetNodeAt(id, opts.ValidAt)
		if err != nil {
			return nil, err
		}
		if pred != nil && !pred(n) {
			return nil, storepkg.ErrNoVersionValidAt
		}
		return n, nil
	}
	return g.findNodeVersionMatchingDuring(id, opts.ValidStart, opts.ValidEnd, pred)
}

// findRelVersionForOpts is the relationship counterpart of findNodeVersionForOpts.
func (g *Graph) findRelVersionForOpts(id types.RelID, opts storepkg.QueryOpts, pred func(*types.Relationship) bool) (*types.Relationship, error) {
	if opts.ValidAt != 0 {
		r, err := g.GetRelAt(id, opts.ValidAt)
		if err != nil {
			return nil, err
		}
		if pred != nil && !pred(r) {
			return nil, storepkg.ErrNoVersionValidAt
		}
		return r, nil
	}
	return g.findRelVersionMatchingDuring(id, opts.ValidStart, opts.ValidEnd, pred)
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
func (g *Graph) findNodeVersionMatchingDuring(id types.NodeID, start, end types.Instant, pred func(*types.Node) bool) (*types.Node, error) {
	// Callers must resolve end == 0 to a concrete bound BEFORE invoking
	// this function; see resolveOpenEndInstant. Per-call substitution
	// here would cause time drift across a long iteration: each ID
	// would see a different `nowInstant()`, so an entity created
	// between iterations could be included or excluded
	// non-deterministically. The entry-point resolution gives every
	// candidate the same upper bound.
	current, err := g.store.GetNode(id)
	if err != nil && !errors.Is(err, storepkg.ErrNodeNotFound) {
		return nil, err
	}

	history, err := g.store.GetNodeHistory(id)
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

	for i := len(chain) - 1; i >= 0; i-- {
		vStart, vEnd := g.nodeVersionBounds(chain, i)
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
func (g *Graph) findRelVersionMatchingDuring(id types.RelID, start, end types.Instant, pred func(*types.Relationship) bool) (*types.Relationship, error) {
	// See findNodeVersionMatchingDuring: callers must pre-resolve end == 0.
	current, err := g.store.GetRelationship(id)
	if err != nil && !errors.Is(err, storepkg.ErrRelNotFound) {
		return nil, err
	}

	history, err := g.store.GetRelHistory(id)
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

	for i := len(chain) - 1; i >= 0; i-- {
		vStart, vEnd := g.relVersionBounds(chain, i)
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
func (g *Graph) getNodeVersionDuring(id types.NodeID, start, end types.Instant) (*types.Node, error) {
	return g.findNodeVersionMatchingDuring(id, start, end, nil)
}

// getRelVersionDuring is a thin wrapper preserving the original "any overlap"
// semantic for relationships.
func (g *Graph) getRelVersionDuring(id types.RelID, start, end types.Instant) (*types.Relationship, error) {
	return g.findRelVersionMatchingDuring(id, start, end, nil)
}

// =============================================================================
// Snapshot
// =============================================================================

// Snapshot returns a complete graph state at the given instant.
// Relationships are only included if both endpoints are valid at t.
// Acquires g.mu.RLock to prevent torn reads from concurrent Batch execution.
func (g *Graph) Snapshot(t types.Instant) (*temporalpkg.GraphSnapshot, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.snapshotAt(t)
}

// snapshotAt computes the graph snapshot at t. It does not acquire g.mu;
// callers that require strong snapshot consistency should hold g.mu.RLock.
func (g *Graph) snapshotAt(t types.Instant) (*temporalpkg.GraphSnapshot, error) {
	nodes, err := g.GetNodesValidAt(t)
	if err != nil {
		return nil, err
	}

	// Build set of valid node IDs for endpoint filtering.
	nodeSet := make(map[snowflake.ID]struct{}, len(nodes))
	for _, n := range nodes {
		nodeSet[n.ID().SnowflakeID()] = struct{}{}
	}

	allRels, err := g.GetRelationshipsValidAt(t)
	if err != nil {
		return nil, err
	}

	// Only include rels where both endpoints are in the valid node set.
	var rels []*types.Relationship
	for _, r := range allRels {
		_, startOK := nodeSet[r.StartNodeID().SnowflakeID()]
		_, endOK := nodeSet[r.EndNodeID().SnowflakeID()]
		if startOK && endOK {
			rels = append(rels, r)
		}
	}

	return &temporalpkg.GraphSnapshot{
		Timestamp:     t,
		Nodes:         nodes,
		Relationships: rels,
		NodeCount:     len(nodes),
		RelCount:      len(rels),
	}, nil
}

// =============================================================================
// Diff
// =============================================================================

// DiffSnapshots returns the set of entity changes between t1 and t2.
// Entities valid at T2 but not T1 → Created.
// Entities valid at both but with different integrity hash → Updated.
// Entities valid at T1 but not T2 → Deleted.
// Returns ErrInvalidTimeRange if t1 >= t2 or either is zero.
//
// Note: the two snapshots are read independently without holding g.mu. A
// concurrent backdated write that commits between the two reads may appear as
// a spurious Created/Deleted entry. This is an acceptable trade-off against
// blocking all writes for the full O(N) snapshot duration.
//
// TODO(v3.1.0): streaming DiffSnapshots to avoid O(N) RAM materialization.
func (g *Graph) DiffSnapshots(t1, t2 types.Instant) (*temporalpkg.SnapshotDiff, error) {
	if t1 == 0 || t2 == 0 || t1 >= t2 {
		return nil, ErrInvalidTimeRange
	}

	// No g.mu.RLock — see doc comment above.
	snap1, err := g.snapshotAt(t1)
	if err != nil {
		return nil, err
	}
	snap2, err := g.snapshotAt(t2)
	if err != nil {
		return nil, err
	}

	return buildDiff(t1, t2, snap1, snap2), nil
}

// buildDiff computes the SnapshotDiff between two snapshots.
func buildDiff(t1, t2 types.Instant, snap1, snap2 *temporalpkg.GraphSnapshot) *temporalpkg.SnapshotDiff {
	diff := &temporalpkg.SnapshotDiff{T1: t1, T2: t2}

	// --- Nodes ---
	nodes1 := make(map[snowflake.ID]*types.Node, len(snap1.Nodes))
	for _, n := range snap1.Nodes {
		nodes1[n.ID().SnowflakeID()] = n
	}
	nodes2 := make(map[snowflake.ID]*types.Node, len(snap2.Nodes))
	for _, n := range snap2.Nodes {
		nodes2[n.ID().SnowflakeID()] = n
	}

	for id, n2 := range nodes2 {
		if n1, ok := nodes1[id]; ok {
			// Present in both: compare hashes.
			h1 := nodeHash(n1)
			h2 := nodeHash(n2)
			if h1 != h2 {
				diff.NodesUpdated = append(diff.NodesUpdated, temporalpkg.NodeUpdate{Before: n1, After: n2})
			}
			// identical hash → unchanged; skip
		} else {
			// Only in snap2 → Created.
			diff.NodesCreated = append(diff.NodesCreated, n2)
		}
	}
	for id, n1 := range nodes1 {
		if _, ok := nodes2[id]; !ok {
			// Only in snap1 → Deleted.
			diff.NodesDeleted = append(diff.NodesDeleted, n1)
		}
	}

	// --- Relationships ---
	rels1 := make(map[snowflake.ID]*types.Relationship, len(snap1.Relationships))
	for _, r := range snap1.Relationships {
		rels1[r.ID().SnowflakeID()] = r
	}
	rels2 := make(map[snowflake.ID]*types.Relationship, len(snap2.Relationships))
	for _, r := range snap2.Relationships {
		rels2[r.ID().SnowflakeID()] = r
	}

	for id, r2 := range rels2 {
		if r1, ok := rels1[id]; ok {
			h1 := relHash(r1)
			h2 := relHash(r2)
			if h1 != h2 {
				diff.RelsUpdated = append(diff.RelsUpdated, temporalpkg.RelUpdate{Before: r1, After: r2})
			}
		} else {
			diff.RelsCreated = append(diff.RelsCreated, r2)
		}
	}
	for id, r1 := range rels1 {
		if _, ok := rels2[id]; !ok {
			diff.RelsDeleted = append(diff.RelsDeleted, r1)
		}
	}

	return diff
}

// nodeHash returns the integrity hash of a node, or empty string if unset.
func nodeHash(n *types.Node) string {
	if ig := n.Integrity(); ig != nil {
		return ig.Hash
	}
	return ""
}

// relHash returns the integrity hash of a relationship, or empty string if unset.
func relHash(r *types.Relationship) string {
	if ig := r.Integrity(); ig != nil {
		return ig.Hash
	}
	return ""
}
