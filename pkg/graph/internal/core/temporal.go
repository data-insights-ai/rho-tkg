package core

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

// isRelValidAt checks if a relationship is valid at the given instant.
func (c *Core) isRelValidAt(r *types.Relationship, t types.Instant) bool {
	from := c.relValidFrom(r)
	if from > t {
		return false
	}
	if tm := r.Temporal(); tm != nil && tm.ValidTo != 0 {
		return tm.ValidTo > t
	}
	return true
}

// resolveNodeVersionAt finds the version valid at time t from a pre-built chain.
func (c *Core) resolveNodeVersionAt(chain []*types.Node, t types.Instant) (*types.Node, error) {
	for i := len(chain) - 1; i >= 0; i-- {
		entry := chain[i]
		vStart, vEnd := c.nodeVersionBounds(chain, i)

		// Check: vStart <= t AND (vEnd == 0 OR vEnd > t).
		if vStart <= t && (vEnd == 0 || vEnd > t) {
			return entry, nil
		}
	}
	return nil, storepkg.ErrNoVersionValidAt
}

// nodeVersionBounds computes the effective [vStart, vEnd) for chain[i].
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

	// Determine version end.
	if i < len(chain)-1 {
		next := chain[i+1]
		if tm := next.Temporal(); tm != nil && tm.UpdatedAt != 0 {
			vEnd = tm.UpdatedAt
		} else {
			vEnd = c.nodeValidFrom(next)
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
func (c *Core) resolveRelVersionAt(chain []*types.Relationship, t types.Instant) (*types.Relationship, error) {
	for i := len(chain) - 1; i >= 0; i-- {
		entry := chain[i]
		vStart, vEnd := c.relVersionBounds(chain, i)

		if vStart <= t && (vEnd == 0 || vEnd > t) {
			return entry, nil
		}
	}
	return nil, storepkg.ErrNoVersionValidAt
}

// relVersionBounds computes the effective [vStart, vEnd) for chain[i].
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

	if i < len(chain)-1 {
		next := chain[i+1]
		if tm := next.Temporal(); tm != nil && tm.UpdatedAt != 0 {
			vEnd = tm.UpdatedAt
		} else {
			vEnd = c.relValidFrom(next)
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
func (c *Core) forEachNodeCandidateID(currentIDs []types.NodeID, fn func(types.NodeID) error) error {
	seen := make(map[types.NodeID]struct{}, len(currentIDs))
	for _, id := range currentIDs {
		seen[id] = struct{}{}
	}
	if err := c.store.ForEachNodeHistoryID(func(id types.NodeID) bool {
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
func (c *Core) forEachRelCandidateID(currentIDs []types.RelID, fn func(types.RelID) error) error {
	seen := make(map[types.RelID]struct{}, len(currentIDs))
	for _, id := range currentIDs {
		seen[id] = struct{}{}
	}
	if err := c.store.ForEachRelHistoryID(func(id types.RelID) bool {
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
func (c *Core) forEachKnownNodeID(fn func(types.NodeID) error) error {
	seen := make(map[types.NodeID]struct{})

	// Phase 1: collect unique IDs (no store method calls in callbacks — lock reentrancy).
	if err := c.store.ForEachNodeID(func(id types.NodeID) bool {
		seen[id] = struct{}{}
		return true
	}); err != nil {
		return err
	}
	if err := c.store.ForEachNodeHistoryID(func(id types.NodeID) bool {
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
func (c *Core) forEachKnownRelID(fn func(types.RelID) error) error {
	seen := make(map[types.RelID]struct{})

	// Phase 1: collect unique IDs.
	if err := c.store.ForEachRelID(func(id types.RelID) bool {
		seen[id] = struct{}{}
		return true
	}); err != nil {
		return err
	}
	if err := c.store.ForEachRelHistoryID(func(id types.RelID) bool {
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
func (c *Core) findNodeVersionForOpts(id types.NodeID, opts storepkg.QueryOpts, pred func(*types.Node) bool) (*types.Node, error) {
	if opts.ValidAt != 0 {
		n, err := c.GetNodeAt(id, opts.ValidAt)
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
		r, err := c.GetRelAt(id, opts.ValidAt)
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
	// Callers must resolve end == 0 to a concrete bound BEFORE invoking
	// this function; see resolveOpenEndInstant. Per-call substitution
	// here would cause time drift across a long iteration: each ID
	// would see a different `nowInstant()`, so an entity created
	// between iterations could be included or excluded
	// non-deterministically. The entry-point resolution gives every
	// candidate the same upper bound.
	current, err := c.store.GetNode(id)
	if err != nil && !errors.Is(err, storepkg.ErrNodeNotFound) {
		return nil, err
	}

	history, err := c.store.GetNodeHistory(id)
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
	// See findNodeVersionMatchingDuring: callers must pre-resolve end == 0.
	current, err := c.store.GetRelationship(id)
	if err != nil && !errors.Is(err, storepkg.ErrRelNotFound) {
		return nil, err
	}

	history, err := c.store.GetRelHistory(id)
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

// =============================================================================
// Snapshot
// =============================================================================

// Snapshot returns a complete graph state at the given instant.
// Relationships are only included if both endpoints are valid at t.
// Acquires c.mu.RLock to prevent torn reads from concurrent Batch execution.
func (c *Core) Snapshot(t types.Instant) (*temporalpkg.GraphSnapshot, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.snapshotAt(t)
}

// snapshotAt computes the graph snapshot at t. It does not acquire c.mu;
// callers that require strong snapshot consistency should hold c.mu.RLock.
func (c *Core) snapshotAt(t types.Instant) (*temporalpkg.GraphSnapshot, error) {
	nodes, err := c.GetNodesValidAt(t)
	if err != nil {
		return nil, err
	}

	// Build set of valid node IDs for endpoint filtering.
	nodeSet := make(map[snowflake.ID]struct{}, len(nodes))
	for _, n := range nodes {
		nodeSet[n.ID().SnowflakeID()] = struct{}{}
	}

	allRels, err := c.GetRelationshipsValidAt(t)
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
// Note: the two snapshots are read independently without holding c.mu. A
// concurrent backdated write that commits between the two reads may appear as
// a spurious Created/Deleted entry. This is an acceptable trade-off against
// blocking all writes for the full O(N) snapshot duration.
//
// Implementation: delegates to DiffSnapshotsCallback with handlers that
// accumulate into a SnapshotDiff. The callback path resolves each entity
// version on demand instead of materialising two full snapshots, so the
// peak working set is one entity at a time plus the dedup ID set.
func (c *Core) DiffSnapshots(t1, t2 types.Instant) (*temporalpkg.SnapshotDiff, error) {
	diff := &temporalpkg.SnapshotDiff{T1: t1, T2: t2}
	handlers := temporalpkg.DiffHandlers{
		OnNodeCreated: func(after *types.Node) error {
			diff.NodesCreated = append(diff.NodesCreated, after)
			return nil
		},
		OnNodeUpdated: func(before, after *types.Node) error {
			diff.NodesUpdated = append(diff.NodesUpdated, temporalpkg.NodeUpdate{Before: before, After: after})
			return nil
		},
		OnNodeDeleted: func(before *types.Node) error {
			diff.NodesDeleted = append(diff.NodesDeleted, before)
			return nil
		},
		OnRelCreated: func(after *types.Relationship) error {
			diff.RelsCreated = append(diff.RelsCreated, after)
			return nil
		},
		OnRelUpdated: func(before, after *types.Relationship) error {
			diff.RelsUpdated = append(diff.RelsUpdated, temporalpkg.RelUpdate{Before: before, After: after})
			return nil
		},
		OnRelDeleted: func(before *types.Relationship) error {
			diff.RelsDeleted = append(diff.RelsDeleted, before)
			return nil
		},
	}
	if err := c.DiffSnapshotsCallback(t1, t2, handlers); err != nil {
		return nil, err
	}
	return diff, nil
}

// DiffSnapshotsCallback streams entity changes between t1 (older) and t2
// (newer) via handler callbacks instead of materialising two full
// GraphSnapshot values. RAM usage is bounded by O(|distinct entity IDs| × 16B
// for the dedup set) plus the working memory for one entity version pair at
// a time, down from O(|entities valid at t1| + |entities valid at t2|).
//
// For every node ID known to the store (current ID set ∪ history ID set)
// the implementation calls GetNodeAt(id, t1) and GetNodeAt(id, t2). The
// result classifies each entity as Created (only at t2), Deleted (only at
// t1), Updated (present at both with a different integrity hash), or
// unchanged (skipped). Relationships follow the same pattern but are
// additionally subject to endpoint filtering: a relationship is treated as
// "present at t" only when both its start and end nodes are valid at t.
// This matches the snapshotAt rel-endpoint filter exactly and preserves
// behavioural parity with DiffSnapshots.
//
// nil handler fields are skipped cleanly. Returning a non-nil error from
// any handler halts iteration and returns that error. Order of delivery is
// implementation-defined; do not rely on it.
//
// Returns ErrInvalidTimeRange if t1 == 0, t2 == 0, or t1 >= t2.
func (c *Core) DiffSnapshotsCallback(t1, t2 types.Instant, h temporalpkg.DiffHandlers) error {
	if t1 == 0 || t2 == 0 || t1 >= t2 {
		return ErrInvalidTimeRange
	}

	// No c.mu.RLock: matches DiffSnapshots semantics. A concurrent backdated
	// write that commits between the per-entity GetNodeAt calls may appear
	// as a spurious Created/Deleted entry — same trade-off as before.

	// --- Nodes ---
	if err := c.forEachKnownNodeID(func(id types.NodeID) error {
		n1, err := c.lookupNodeAtForDiff(id, t1)
		if err != nil {
			return err
		}
		n2, err := c.lookupNodeAtForDiff(id, t2)
		if err != nil {
			return err
		}
		switch {
		case n1 == nil && n2 != nil:
			if h.OnNodeCreated != nil {
				return h.OnNodeCreated(n2)
			}
		case n1 != nil && n2 == nil:
			if h.OnNodeDeleted != nil {
				return h.OnNodeDeleted(n1)
			}
		case n1 != nil && n2 != nil:
			if nodeHash(n1) != nodeHash(n2) {
				if h.OnNodeUpdated != nil {
					return h.OnNodeUpdated(n1, n2)
				}
			}
		}
		// n1 == nil && n2 == nil: never visible in [t1, t2]; skip.
		return nil
	}); err != nil {
		return err
	}

	// --- Relationships ---
	// Endpoint validity is determined per-rel via GetNodeAt on the
	// resolved start/end at the queried time. We deliberately do NOT cache
	// node-validity decisions here: the dedup ID set already costs
	// O(|nodes|), and a second full-graph node-validity map would defeat
	// the bounded-RAM goal. The extra GetNodeAt calls are amortised into
	// the same chain reads that will also be needed if the rel itself
	// changes — Badger / TieredStore caches absorb most of the cost.
	return c.forEachKnownRelID(func(id types.RelID) error {
		r1, err := c.lookupRelAtForDiff(id, t1)
		if err != nil {
			return err
		}
		r2, err := c.lookupRelAtForDiff(id, t2)
		if err != nil {
			return err
		}
		switch {
		case r1 == nil && r2 != nil:
			if h.OnRelCreated != nil {
				return h.OnRelCreated(r2)
			}
		case r1 != nil && r2 == nil:
			if h.OnRelDeleted != nil {
				return h.OnRelDeleted(r1)
			}
		case r1 != nil && r2 != nil:
			if relHash(r1) != relHash(r2) {
				if h.OnRelUpdated != nil {
					return h.OnRelUpdated(r1, r2)
				}
			}
		}
		return nil
	})
}

// lookupNodeAtForDiff resolves the version of node id valid at t, returning
// (nil, nil) when no version covers t (entity not yet created or already
// deleted at that instant). All other errors are propagated. Used by
// DiffSnapshotsCallback so a missing-at-this-time signal is distinguishable
// from a real I/O error without sprinkling errors.Is checks at every call
// site.
func (c *Core) lookupNodeAtForDiff(id types.NodeID, t types.Instant) (*types.Node, error) {
	n, err := c.GetNodeAt(id, t)
	if err != nil {
		if errors.Is(err, storepkg.ErrNoVersionValidAt) || errors.Is(err, storepkg.ErrNodeNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return n, nil
}

// lookupRelAtForDiff resolves the version of relationship id valid at t,
// applying the same endpoint-validity filter that snapshotAt uses: a
// relationship is reported as "present at t" only when its start and end
// nodes are both valid at t. Returns (nil, nil) when the rel itself is
// absent at t or either endpoint is missing. All other errors are
// propagated.
func (c *Core) lookupRelAtForDiff(id types.RelID, t types.Instant) (*types.Relationship, error) {
	r, err := c.GetRelAt(id, t)
	if err != nil {
		if errors.Is(err, storepkg.ErrNoVersionValidAt) || errors.Is(err, storepkg.ErrRelNotFound) {
			return nil, nil
		}
		return nil, err
	}
	// Endpoint filter — mirror snapshotAt's "both endpoints valid" rule.
	startOK, err := c.nodeExistsAt(r.StartNodeID(), t)
	if err != nil {
		return nil, err
	}
	if !startOK {
		return nil, nil
	}
	endOK, err := c.nodeExistsAt(r.EndNodeID(), t)
	if err != nil {
		return nil, err
	}
	if !endOK {
		return nil, nil
	}
	return r, nil
}

// nodeExistsAt reports whether some version of the node is valid at t.
// ErrNoVersionValidAt and ErrNodeNotFound both map to false; every other
// error propagates so a transient backend failure does not silently
// reclassify entities as "deleted" in the diff stream.
func (c *Core) nodeExistsAt(id types.NodeID, t types.Instant) (bool, error) {
	if _, err := c.GetNodeAt(id, t); err != nil {
		if errors.Is(err, storepkg.ErrNoVersionValidAt) || errors.Is(err, storepkg.ErrNodeNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
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
