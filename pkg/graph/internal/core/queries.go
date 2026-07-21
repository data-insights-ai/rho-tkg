package core

import (
	"errors"
	"strings"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/grapherr"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"

	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func (c *Core) validateRelTypeQueryName(typeName string) error {
	if _, ok := c.cachedRelType(typeName); ok {
		return nil
	}
	return c.validateIndexName(typeName)
}

func (c *Core) lookupRelTypeQueryToken(typeName string) (uint16, bool) {
	if tok, ok := c.cachedRelType(typeName); ok {
		return tok, true
	}
	return c.relTypes.Lookup(typeName)
}

// --- Store passthrough queries ---
//
// Get is defined alongside Delete in node_delete.go / relationship_delete.go;
// the API 4.0 collapse merged the historical Get/Get pair into a
// single context-aware Get.
//
// Every public read method below has a paired (*Core).*Locked helper that
// runs the closure body without acquiring c.mu. The public method wraps the
// helper under c.mu.RLock; the tx-side mirror in tx_consistent_reads.go
// calls the helper directly under tx.mu (because BeginTx already holds
// c.mu.Lock and sync.RWMutex is not reentrant — lesson 31). Validation that
// does not touch shared state stays at the public-method layer so both the
// standalone and the tx call paths get the same input checks.

// ByLabel returns nodes with the given label (resolved from string),
// with optional pagination. Returns nil if the label is not registered.
//
// ByLabel opts carries a temporal filter (ValidAt or ValidStart/ValidEnd) the
// query is history-aware: every known node (current + history) is scanned and
// any version whose label set contained the requested label at the requested
// time matches. Without a temporal filter, the call falls through to the
// store-level label index for O(matches) lookup.
func (n *NodeOps) ByLabel(label string, opts storepkg.QueryOpts) ([]*types.Node, error) {
	c := n.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if err := c.validateIndexLabel(label); err != nil {
		return nil, err
	}
	if err := c.validateTemporalQueryOptsScan(opts); err != nil {
		return nil, err
	}
	var result []*types.Node
	err := c.readUnderRLock(func() error {
		var err error
		result, err = c.nodesByLabelLocked(label, opts)
		return err
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// nodeRangeCardinalityScanner is the OPTIONAL store capability behind
// NodeOps.RangeCardinality — an O(bitmap) range-count from the property index's
// bit-sliced index (R1), with no node scan.
type nodeRangeCardinalityScanner interface {
	NodeRangeCardinality(labelToken uint16, propKey string, min, max float64, inclMin, inclMax bool) (int64, bool, error)
}

// RangeCardinality returns the count of the label's nodes whose numeric propKey
// value lies in [min,max] (inclusivity per flags), summed directly from the
// property index's sorted per-value bucket sizes — O(distinct values in range),
// NO node scan. exact=false declines — the caller must scan-and-count — when the
// store lacks the capability, the index is absent, the index is poisoned (it
// holds an integer magnitude past 2^53, where float64 sort keys can collide), or
// a temporal filter is set (the index is valid-time agnostic). Fractional values
// and bounds are counted exactly. The bounds MUST capture the whole predicate;
// the caller enforces that (only the count-over-pure-range fast path may use this).
func (n *NodeOps) RangeCardinality(label, propKey string, min, max float64, inclMin, inclMax bool, opts storepkg.QueryOpts) (int64, bool, error) {
	c := n.c
	if err := c.checkOpen(); err != nil {
		return 0, false, err
	}
	if opts.ValidAt != 0 || opts.ValidStart != 0 || opts.ValidEnd != 0 || opts.TxAt != 0 || opts.TxPin != 0 {
		return 0, false, nil // temporal — the BSI cannot answer; caller scans
	}
	scanner, native := c.store.(nodeRangeCardinalityScanner)
	if !native {
		return 0, false, nil
	}
	var tok uint16
	var ok bool
	if err := c.readUnderRLock(func() error {
		tok, ok = c.lookupLabelLocked(label)
		return nil
	}); err != nil {
		return 0, false, err
	}
	if !ok {
		return 0, false, nil // unknown label — caller scans (finds zero)
	}
	return scanner.NodeRangeCardinality(tok, propKey, min, max, inclMin, inclMax)
}

// relRangeCardinalityScanner is the OPTIONAL store capability behind
// RelOps.RangeCardinality — the relationship mirror of nodeRangeCardinalityScanner
// (memory + badger; rel property indexes are RAM-only, so tiered/sharded decline).
type relRangeCardinalityScanner interface {
	RelRangeCardinality(relTypeToken uint16, propKey string, min, max float64, inclMin, inclMax bool) (int64, bool, error)
}

// RangeCardinality is the relationship mirror of NodeOps.RangeCardinality: the count
// of the type's relationships whose numeric propKey value lies in [min,max] summed
// directly from the rel property index's per-value bucket sizes — O(distinct values
// in range), NO rel scan. exact=false declines (the caller scans-and-counts) when the
// store lacks the capability (tiered/sharded — rel indexes are RAM-only), the index
// is absent or poisoned, or a temporal filter is set (the index is valid-time
// agnostic). The bounds MUST capture the whole predicate (caller-enforced). This is
// the rel ordering-soundness primitive for the ORDER BY r.prop LIMIT k push-down.
func (r *RelOps) RangeCardinality(typeName, propKey string, min, max float64, inclMin, inclMax bool, opts storepkg.QueryOpts) (int64, bool, error) {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return 0, false, err
	}
	if opts.ValidAt != 0 || opts.ValidStart != 0 || opts.ValidEnd != 0 || opts.TxAt != 0 || opts.TxPin != 0 {
		return 0, false, nil // temporal — the index cannot answer; caller scans
	}
	scanner, native := c.store.(relRangeCardinalityScanner)
	if !native {
		return 0, false, nil
	}
	var tok uint16
	var ok bool
	if err := c.readUnderRLock(func() error {
		tok, ok = c.lookupRelTypeQueryToken(typeName)
		return nil
	}); err != nil {
		return 0, false, err
	}
	if !ok {
		return 0, false, nil // unknown rel type — caller scans (finds zero)
	}
	return scanner.RelRangeCardinality(tok, propKey, min, max, inclMin, inclMax)
}

// nodeLabelScanner is the OPTIONAL store capability behind
// NodeOps.ForEachByLabel — streaming label scans whose peak memory stays
// O(1) in the label's cardinality. Implemented by the in-tree memory and
// badger stores; stores without it fall back to the materializing path.
type nodeLabelScanner interface {
	ForEachNodeByLabel(token uint16, opts storepkg.QueryOpts, fn func(*types.Node) bool) error
}

// ForEachByLabel streams nodes carrying the given label to fn in
// snowflake-ID order; fn returning false stops the scan early. The
// streaming sibling of ByLabel for scan consumers (count/filter/aggregate
// pipelines) that must not materialize the full result slice.
//
// Isolation is RELAXED relative to ByLabel: the scan capability snapshots
// the ID set, then fetches rows and calls fn without holding graph locks —
// fn may call back into the graph, and concurrent writers are neither
// blocked nor observed atomically (deleted rows are skipped, new rows are
// not seen). Rows are shared frozen pointers; fn must not mutate them.
//
// Temporal-filter queries and non-native stores route through the
// materializing ByLabel path (history-aware version walking needs the full
// machinery), so streaming currently applies to the plain current-state
// scan — exactly the shape whose materialization cost scales with label
// cardinality.
func (n *NodeOps) ForEachByLabel(label string, opts storepkg.QueryOpts, fn func(*types.Node) bool) error {
	c := n.c
	if err := c.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return grapherr.ErrNilCallback
	}
	if err := c.validateIndexLabel(label); err != nil {
		return err
	}
	if err := c.validateTemporalQueryOptsScan(opts); err != nil {
		return err
	}

	scanner, native := c.store.(nodeLabelScanner)
	if !native || !c.storeRowsTrust || hasTemporalFilter(opts) {
		nodes, err := n.ByLabel(label, opts)
		if err != nil {
			return err
		}
		for _, nd := range nodes {
			if !fn(nd) {
				return nil
			}
		}
		return nil
	}

	var tok uint16
	var ok bool
	if err := c.readUnderRLock(func() error {
		tok, ok = c.labels.Lookup(label)
		return nil
	}); err != nil {
		return err
	}
	if !ok {
		return nil
	}
	// Deliberately outside c.mu — see the isolation note above.
	return scanner.ForEachNodeByLabel(tok, opts, fn)
}

// nodeRangeScanner is the OPTIONAL store capability behind
// NodeOps.ForEachByLabelPropertyRange — streaming numeric range scans over
// a property index's ordered view (ordered range view). Implemented by the badger store.
type nodeRangeScanner interface {
	ForEachNodeByLabelPropertyRange(token uint16, propKey string, min, max float64, inclMin, inclMax bool, opts storepkg.QueryOpts, fn func(*types.Node) bool) error
}

// ForEachByLabelPropertyRange streams nodes carrying the label whose
// NUMERIC propKey value lies within [min, max] (per the inclusivity
// flags), in snowflake-ID order. The candidates come from the property
// index's ordered numeric view, which OVER-SELECTS by design (float64
// sort keys, ulp-widened bounds): fn must re-check its predicate with
// exact comparison semantics. Returns storepkg.ErrIndexNotFound when no
// property index with a usable ordered view exists for (label, propKey)
// or the store lacks the capability — callers fall back to a label scan.
// Same relaxed isolation and frozen-row contract as ForEachByLabel;
// temporal-filter opts route through the store's per-row temporal check.
func (n *NodeOps) ForEachByLabelPropertyRange(label, propKey string, min, max float64, inclMin, inclMax bool, opts storepkg.QueryOpts, fn func(*types.Node) bool) error {
	c := n.c
	if err := c.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return grapherr.ErrNilCallback
	}
	if err := c.validateIndexLabel(label); err != nil {
		return err
	}
	if err := c.validateTemporalQueryOptsScan(opts); err != nil {
		return err
	}
	scanner, native := c.store.(nodeRangeScanner)
	if !native || !c.storeRowsTrust {
		return storepkg.ErrIndexNotFound
	}
	var tok uint16
	var ok bool
	if err := c.readUnderRLock(func() error {
		tok, ok = c.labels.Lookup(label)
		return nil
	}); err != nil {
		return err
	}
	if !ok {
		return nil
	}
	// Deliberately outside c.mu — see ForEachByLabel's isolation note.
	return scanner.ForEachNodeByLabelPropertyRange(tok, propKey, min, max, inclMin, inclMax, opts, fn)
}

// nodeOrderedRangeScanner is the OPTIONAL store capability behind
// NodeOps.ForEachByLabelPropertyRangeOrdered — streaming NUMERIC range scans
// that emit in contractual VALUE ORDER (the ORDER BY prop [LIMIT k] / top-k
// access path). Implemented by the in-tree memory and badger stores.
type nodeOrderedRangeScanner interface {
	ForEachNodeByLabelPropertyRangeOrdered(token uint16, propKey string, min, max float64, inclMin, inclMax, desc bool, fn func(*types.Node) bool) error
}

// ForEachByLabelPropertyRangeOrdered streams nodes carrying the label whose
// NUMERIC propKey value lies within [min, max] to fn in CONTRACTUAL VALUE
// ORDER — ascending, or descending when desc — with ties (equal values)
// always broken by node ID ASCENDING in both directions. This is the ordered
// / top-k access path: a query layer compiling `ORDER BY n.prop [ASC|DESC]
// LIMIT k` streams here and returns false from fn once it has collected k
// rows, so the LIMIT is pushed into the index and the scan stops at
// O(k + log n) index work — never materializing the whole range.
//
// The candidates come from the property index's ordered numeric view, which
// OVER-SELECTS by design (float64 sort keys, ulp-widened bounds): fn MUST
// re-check its predicate with exact comparison semantics (the door never
// skips a boundary bucket — int64 magnitudes past 2^53 collapse onto
// neighbouring sort keys, so the exact inclusivity check per inclMin/inclMax
// is fn's responsibility).
//
// Non-temporal opts take the index-backed fast path (O(k + log n) top-k) and
// return storepkg.ErrIndexNotFound when no property index with a usable ordered
// view exists for (label, propKey) or the store lacks the capability. A TEMPORAL
// QueryOpts combination (ValidAt / ValidStart+ValidEnd / TxAt / TxPin) is instead
// served by a SOUND FULL FOLD (Stage B): every label member is resolved to its
// version at the pin, filtered to [min,max] on the value-AT-t, and sorted by that
// value — so ordering over a past belief/valid state is correct and complete
// (a value in range then but not now is included, and vice versa). The temporal
// path needs no property index (it reads resolved node values directly) and is
// O(N log N) in the label's temporal membership. Same relaxed isolation and
// frozen-row contract as ForEachByLabel.
func (n *NodeOps) ForEachByLabelPropertyRangeOrdered(label, propKey string, min, max float64, inclMin, inclMax, desc bool, opts storepkg.QueryOpts, fn func(*types.Node) bool) error {
	c := n.c
	if err := c.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return grapherr.ErrNilCallback
	}
	if err := c.validateIndexLabel(label); err != nil {
		return err
	}
	// Temporal ordered scan (Stage B): value-at-t is not indexed, so serve it as a
	// SOUND FULL FOLD — resolve every label member to its version at the pin, keep
	// those whose numeric value is in [min,max], sort by value. Needs no property
	// index (it reads resolved node values directly). Non-temporal opts take the
	// index-backed fast path below.
	if opts.ValidAt != 0 || opts.ValidStart != 0 || opts.ValidEnd != 0 || opts.TxAt != 0 || opts.TxPin != 0 {
		if err := c.validateTemporalQueryOptsScan(opts); err != nil {
			return err
		}
		return forEachNodeValueOrderedTemporal(c, label, opts, desc,
			func(nd *types.Node) (float64, bool) {
				v, ok := nd.GetProperty(propKey)
				if !ok {
					return 0, false
				}
				f, ok := coerceFloat64(v)
				if !ok || !numericInRange(f, min, max, inclMin, inclMax) {
					return 0, false
				}
				return f, true
			}, fn)
	}
	if err := storepkg.ValidateQueryOpts(opts); err != nil {
		return err
	}
	scanner, native := c.store.(nodeOrderedRangeScanner)
	if !native || !c.storeRowsTrust {
		return storepkg.ErrIndexNotFound
	}
	var tok uint16
	var ok bool
	if err := c.readUnderRLock(func() error {
		tok, ok = c.labels.Lookup(label)
		return nil
	}); err != nil {
		return err
	}
	if !ok {
		return nil
	}
	// Deliberately outside c.mu — see ForEachByLabel's isolation note.
	return scanner.ForEachNodeByLabelPropertyRangeOrdered(tok, propKey, min, max, inclMin, inclMax, desc, fn)
}

// nodePrefixScanner is the OPTIONAL store capability behind
// NodeOps.ForEachByLabelPropertyPrefix — streaming STRING prefix scans that emit
// in contractual lexicographic VALUE ORDER (the `WHERE n.k STARTS WITH $p
// [ORDER BY n.k] [LIMIT k]` access path). Implemented by the in-tree memory and
// badger stores.
type nodePrefixScanner interface {
	ForEachNodeByLabelPropertyPrefix(token uint16, propKey, prefix string, desc bool, fn func(*types.Node) bool) error
}

// ForEachByLabelPropertyPrefix streams nodes carrying the label whose STRING
// propKey value begins with prefix, to fn in CONTRACTUAL VALUE ORDER —
// lexicographic ascending, or descending when desc — with ties (equal values)
// always broken by node ID ASCENDING in both directions. It is the string prefix
// / `STARTS WITH` access path: a query layer compiling `WHERE n.k STARTS WITH $p
// ORDER BY n.k [ASC|DESC] LIMIT k` streams here and returns false from fn once it
// has k rows, so the LIMIT is pushed into the index (O(k + log n) index work — the
// whole prefix range is never materialized). An empty prefix matches every string
// value of the property.
//
// Candidates come from the property index's ordered STRING view, which — unlike
// the numeric ordered view — is EXACT (string sort keys never collide), so fn
// receives rows that already satisfy the prefix; fn still owns any further
// predicate.
//
// Non-temporal opts take the index-backed fast path and return
// storepkg.ErrIndexNotFound when no property index with a usable ordered view
// exists for (label, propKey) or the store lacks the capability. A TEMPORAL
// QueryOpts combination is served by a SOUND FULL FOLD (Stage B): every label
// member is resolved to its version at the pin, filtered to the prefix on the
// value-AT-t, and sorted lexicographically — needs no property index, O(N log N).
// Same relaxed isolation and frozen-row contract as ForEachByLabel.
func (n *NodeOps) ForEachByLabelPropertyPrefix(label, propKey, prefix string, desc bool, opts storepkg.QueryOpts, fn func(*types.Node) bool) error {
	c := n.c
	if err := c.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return grapherr.ErrNilCallback
	}
	if err := c.validateIndexLabel(label); err != nil {
		return err
	}
	// Temporal prefix scan (Stage B): value-at-t is not indexed, so serve it as a
	// SOUND FULL FOLD — resolve every label member to its version at the pin, keep
	// those whose string value begins with prefix, sort lexicographically. Needs no
	// property index. Non-temporal opts take the index-backed fast path below.
	if opts.ValidAt != 0 || opts.ValidStart != 0 || opts.ValidEnd != 0 || opts.TxAt != 0 || opts.TxPin != 0 {
		if err := c.validateTemporalQueryOptsScan(opts); err != nil {
			return err
		}
		return forEachNodeValueOrderedTemporal(c, label, opts, desc,
			func(nd *types.Node) (string, bool) {
				v, ok := nd.GetProperty(propKey)
				if !ok {
					return "", false
				}
				s, ok := v.(string)
				if !ok || !strings.HasPrefix(s, prefix) {
					return "", false
				}
				return s, true
			}, fn)
	}
	if err := storepkg.ValidateQueryOpts(opts); err != nil {
		return err
	}
	scanner, native := c.store.(nodePrefixScanner)
	if !native || !c.storeRowsTrust {
		return storepkg.ErrIndexNotFound
	}
	var tok uint16
	var ok bool
	if err := c.readUnderRLock(func() error {
		tok, ok = c.labels.Lookup(label)
		return nil
	}); err != nil {
		return err
	}
	if !ok {
		return nil
	}
	// Deliberately outside c.mu — see ForEachByLabel's isolation note.
	return scanner.ForEachNodeByLabelPropertyPrefix(tok, propKey, prefix, desc, fn)
}

// nodesByLabelLocked is the lock-free body of NodeOps.ByLabel. Callers must
// hold c.mu (R or W). Used by the public method (under RLock) and by
// (*GraphTx).NodesByLabel (under tx-inherited Lock).
func (c *Core) nodesByLabelLocked(label string, opts storepkg.QueryOpts) ([]*types.Node, error) {
	tok, ok := c.labels.Lookup(label)
	if !ok {
		return nil, nil
	}
	if !hasTemporalFilter(opts) {
		nodes, err := c.store.NodesByLabel(tok, opts)
		if err == nil && !c.storeRowsTrust {
			if err = c.validateNodesByLabelPage(tok, opts, nodes); err == nil {
				nodes = copyNodeRows(nodes)
			}
		}
		return nodes, err
	}
	current, err := c.store.NodesByLabel(tok, storepkg.QueryOpts{Depth: opts.Depth})
	if err != nil {
		return nil, err
	}
	currentIDs, err := c.nodeIDsFromLabelRows(tok, current)
	if err != nil {
		return nil, err
	}

	// Gather the candidate id set. K1: when the store owns a transaction-time
	// label-membership sidecar, scope candidates to the label's ever-members
	// (O(matches)) instead of folding ALL node history (O(everything that ever
	// carried ANY label)); otherwise fold by depth. The chain resolver
	// (findNodeVersionForOpts) stays the correctness authority — both the K1
	// sidecar and the B4 envelope prune below are sound supersets, so any
	// over-included candidate is rejected there. Tiered / wrapped stores decline
	// both capabilities and take the full fold.
	var candIDs []types.NodeID
	gather := func(id types.NodeID) error {
		candIDs = append(candIDs, id)
		return nil
	}
	if c.labelTxMembers != nil {
		if err := c.forEachLabelTxCandidate(tok, currentIDs, opts, gather); err != nil {
			return nil, err
		}
	} else if err := c.forEachNodeCandidateIDByDepth(currentIDs, opts.Depth, gather); err != nil {
		return nil, err
	}

	// when the store owns a per-label valid-time ENVELOPE index, drop every
	// candidate whose envelope provably cannot overlap the query's valid-time
	// filter — WITHOUT loading its version chain. PruneTemporalCandidates returns
	// ok=false (candidates unchanged) when opts carries no valid-time filter or no
	// envelope covers this label. The envelope is a sound superset of every
	// version's interval, so a kept id may still be rejected by the resolver, but a
	// pruned id can never have matched (positive-evidence-only pruning).
	if c.temporalCandidates != nil {
		if kept, ok := c.temporalCandidates.PruneTemporalCandidates(tok, candIDs, opts); ok {
			candIDs = kept
		}
	}

	var result []*types.Node
	pred := func(n *types.Node) bool { return n.HasLabelTokenRaw(tok) }
	for _, id := range candIDs {
		n, err := c.findNodeVersionForOpts(id, opts, pred)
		if err != nil {
			if errors.Is(err, storepkg.ErrNoVersionValidAt) || errors.Is(err, storepkg.ErrNodeNotFound) {
				continue
			}
			return nil, err
		}
		result = append(result, n)
	}
	storeutil.SortNodesByID(result)
	result = storeutil.PaginateNodes(result, opts.After, opts.Limit)
	return result, nil
}

// ByType returns relationships with the given type (resolved from string),
// with optional pagination. Returns nil if the type is not registered.
//
// ByType opts carries a temporal filter, every known relationship (current +
// history) is scanned. The type token is structurally immutable, so the only
// history-relevant information added by this scan is deleted/closed-out
// relationships that the current type index no longer references. Without a
// temporal filter, the call falls through to the store-level type index.
func (r *RelOps) ByType(typeName string, opts storepkg.QueryOpts) ([]*types.Relationship, error) {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if err := c.validateRelTypeQueryName(typeName); err != nil {
		return nil, err
	}
	if err := c.validateTemporalQueryOptsScan(opts); err != nil {
		return nil, err
	}
	var result []*types.Relationship
	err := c.readUnderRLock(func() error {
		var err error
		result, err = c.relsByTypeLocked(typeName, opts)
		return err
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// relsByTypeLocked is the lock-free body of RelOps.ByType.
func (c *Core) relsByTypeLocked(typeName string, opts storepkg.QueryOpts) ([]*types.Relationship, error) {
	tok, ok := c.lookupRelTypeQueryToken(typeName)
	if !ok {
		return nil, nil
	}
	if !hasTemporalFilter(opts) {
		rels, err := c.store.RelationshipsByType(tok, opts)
		if err == nil && !c.storeRowsTrust {
			if err = c.validateRelationshipsByTypePage(tok, opts, rels); err == nil {
				rels = copyRelationshipRows(rels)
			}
		}
		return rels, err
	}
	current, err := c.store.RelationshipsByType(tok, storepkg.QueryOpts{Depth: opts.Depth})
	if err != nil {
		return nil, err
	}
	currentIDs, err := c.relIDsFromTypeRows(tok, current)
	if err != nil {
		return nil, err
	}

	// scope the candidate set to the type's ever-members when the store owns
	// the transaction-time rel-type-membership sidecar (see nodesByLabelLocked).
	var candIDs []types.RelID
	gather := func(id types.RelID) error {
		candIDs = append(candIDs, id)
		return nil
	}
	if c.relTypeTxMembers != nil {
		if err := c.forEachRelTypeTxCandidate(tok, currentIDs, opts, gather); err != nil {
			return nil, err
		}
	} else if err := c.forEachRelCandidateIDByDepth(currentIDs, opts.Depth, gather); err != nil {
		return nil, err
	}

	// BACKLOG 21c: when the store owns a per-rel-type valid-time ENVELOPE
	// index, drop every candidate whose envelope provably cannot overlap the
	// query's valid-time filter — the rel-side mirror of the temporalCandidates
	// prune in nodesByLabelLocked. Sound superset: a kept id may still be
	// rejected by the resolver, a pruned id never could have matched.
	if c.relTypeTemporalCandidates != nil {
		if kept, ok := c.relTypeTemporalCandidates.PruneRelTypeTemporalCandidates(tok, candIDs, opts); ok {
			candIDs = kept
		}
	}

	var result []*types.Relationship
	pred := func(r *types.Relationship) bool { return r.HasTypeTokenRaw(tok) }
	for _, id := range candIDs {
		r, err := c.findRelVersionForOpts(id, opts, pred)
		if err != nil {
			if errors.Is(err, storepkg.ErrNoVersionValidAt) || errors.Is(err, storepkg.ErrRelNotFound) {
				continue
			}
			return nil, err
		}
		result = append(result, r)
	}
	storeutil.SortRelsByID(result)
	result = storeutil.PaginateRels(result, opts.After, opts.Limit)
	return result, nil
}

// relTypeScanner is the OPTIONAL store capability behind
// RelOps.ForEachByType — streaming type scans whose peak memory stays O(1)
// in the type's cardinality. Implemented by the in-tree memory and badger
// stores; stores without it fall back to the materializing path.
type relTypeScanner interface {
	ForEachRelByType(token uint16, opts storepkg.QueryOpts, fn func(*types.Relationship) bool) error
}

// ForEachByType streams relationships carrying the given type to fn in
// snowflake-ID order; fn returning false stops the scan early. The
// streaming sibling of ByType for scan consumers (count/filter/aggregate
// pipelines) that must not materialize the full result slice.
//
// Isolation is RELAXED relative to ByType: the scan capability snapshots
// the ID set, then fetches rows and calls fn without holding graph locks —
// fn may call back into the graph, and concurrent writers are neither
// blocked nor observed atomically (deleted rows are skipped, new rows are
// not seen). Rows are shared frozen pointers; fn must not mutate them.
//
// Temporal-filter queries and non-native stores route through the
// materializing ByType path (history-aware version walking needs the full
// machinery), so streaming currently applies to the plain current-state
// scan — exactly the shape whose materialization cost scales with type
// cardinality.
func (r *RelOps) ForEachByType(typeName string, opts storepkg.QueryOpts, fn func(*types.Relationship) bool) error {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return grapherr.ErrNilCallback
	}
	if err := c.validateRelTypeQueryName(typeName); err != nil {
		return err
	}
	if err := c.validateTemporalQueryOptsScan(opts); err != nil {
		return err
	}

	scanner, native := c.store.(relTypeScanner)
	if !native || !c.storeRowsTrust || hasTemporalFilter(opts) {
		rels, err := r.ByType(typeName, opts)
		if err != nil {
			return err
		}
		for _, rel := range rels {
			if !fn(rel) {
				return nil
			}
		}
		return nil
	}

	var tok uint16
	var ok bool
	if err := c.readUnderRLock(func() error {
		tok, ok = c.lookupRelTypeQueryToken(typeName)
		return nil
	}); err != nil {
		return err
	}
	if !ok {
		return nil
	}
	// Deliberately outside c.mu — see the isolation note above.
	return scanner.ForEachRelByType(tok, opts, fn)
}

// relAdjacencyScanner is the OPTIONAL store capability behind
// RelOps.ForEachOutgoing / ForEachIncoming — streaming adjacency scans for
// hub-degree consumers (BFS expansion around power-law hubs) that must not
// materialize a hub's full adjacency slice.
type relAdjacencyScanner interface {
	ForEachOutgoingRel(nid types.NodeID, typeToken uint16, fn func(*types.Relationship) bool) error
	ForEachIncomingRel(nid types.NodeID, typeToken uint16, fn func(*types.Relationship) bool) error
}

// ForEachOutgoing streams the node's outgoing relationships (optionally
// type-filtered; empty typeName means all types) to fn in snowflake-ID
// order; fn returning false stops the scan early. The streaming sibling of
// Outgoing — same relaxed isolation and frozen-row contract as
// ForEachByType. Unregistered typeName streams nothing (after verifying the
// node exists, matching Outgoing).
func (r *RelOps) ForEachOutgoing(nodeID types.NodeID, typeName string, fn func(*types.Relationship) bool) error {
	return r.forEachAdjacent(nodeID, typeName, false, fn)
}

// ForEachIncoming is ForEachOutgoing for the incoming direction.
func (r *RelOps) ForEachIncoming(nodeID types.NodeID, typeName string, fn func(*types.Relationship) bool) error {
	return r.forEachAdjacent(nodeID, typeName, true, fn)
}

func (r *RelOps) forEachAdjacent(nodeID types.NodeID, typeName string, incoming bool, fn func(*types.Relationship) bool) error {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return grapherr.ErrNilCallback
	}
	if typeName != "" {
		if err := c.validateRelTypeQueryName(typeName); err != nil {
			return err
		}
	}
	if err := storepkg.ValidateNodeID(nodeID); err != nil {
		return err
	}

	scanner, native := c.store.(relAdjacencyScanner)
	if !native || !c.storeRowsTrust {
		var rels []*types.Relationship
		var err error
		if incoming {
			rels, err = r.Incoming(nodeID, typeName)
		} else {
			rels, err = r.Outgoing(nodeID, typeName)
		}
		if err != nil {
			return err
		}
		for _, rel := range rels {
			if !fn(rel) {
				return nil
			}
		}
		return nil
	}

	var tok uint16
	if typeName != "" {
		var ok bool
		if err := c.readUnderRLock(func() error {
			tok, ok = c.lookupRelTypeQueryToken(typeName)
			return nil
		}); err != nil {
			return err
		}
		if !ok {
			// Unregistered type: mirror Outgoing/Incoming — the node-exists
			// check still applies, then nothing matches.
			return c.readUnderRLock(func() error {
				return c.validateRequestedNodesExist([]types.NodeID{nodeID})
			})
		}
	}
	// Deliberately outside c.mu — see ForEachByType's isolation note.
	if incoming {
		return scanner.ForEachIncomingRel(nodeID, tok, fn)
	}
	return scanner.ForEachOutgoingRel(nodeID, tok, fn)
}

// relEndpointScanner is the OPTIONAL store capability behind
// RelOps.ForEachAdjacentEndpoint — streaming adjacency that yields the OTHER
// endpoint (and the relationship's id) WITHOUT decoding the relationship row.
type relEndpointScanner interface {
	ForEachAdjacentEndpoint(nid types.NodeID, typeToken uint16, incoming bool, fn func(rel types.RelID, other types.NodeID) bool) error
}

// ForEachAdjacentEndpoint streams (relID, otherEndpoint) for the node's
// adjacency in the given direction WITHOUT decoding relationship rows — for
// traversals that need only the neighbour, not the relationship's properties
// (the dominant per-edge cost on the disk-backed store is the rel-row decode).
// Stores without the native capability fall back to decoding via
// Outgoing/Incoming and projecting the endpoint (correct, identical results —
// the in-memory store pays no decode anyway). Same relaxed isolation as
// ForEachOutgoing; fn returning false stops the scan.
func (r *RelOps) ForEachAdjacentEndpoint(nodeID types.NodeID, typeName string, incoming bool, fn func(rel types.RelID, other types.NodeID) bool) error {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return grapherr.ErrNilCallback
	}
	if typeName != "" {
		if err := c.validateRelTypeQueryName(typeName); err != nil {
			return err
		}
	}
	if err := storepkg.ValidateNodeID(nodeID); err != nil {
		return err
	}

	scanner, native := c.store.(relEndpointScanner)
	if !native {
		// Fallback: decode the rels and project the OTHER endpoint.
		var rels []*types.Relationship
		var err error
		if incoming {
			rels, err = r.Incoming(nodeID, typeName)
		} else {
			rels, err = r.Outgoing(nodeID, typeName)
		}
		if err != nil {
			return err
		}
		for _, rel := range rels {
			other := rel.EndNodeID()
			if incoming {
				other = rel.StartNodeID()
			}
			if !fn(rel.InternalID(), other) {
				return nil
			}
		}
		return nil
	}

	var tok uint16
	if typeName != "" {
		var ok bool
		if err := c.readUnderRLock(func() error {
			tok, ok = c.lookupRelTypeQueryToken(typeName)
			return nil
		}); err != nil {
			return err
		}
		if !ok {
			return c.readUnderRLock(func() error {
				return c.validateRequestedNodesExist([]types.NodeID{nodeID})
			})
		}
	}
	return scanner.ForEachAdjacentEndpoint(nodeID, tok, incoming, fn)
}

// relEndpointScannerAt is the OPTIONAL store capability behind
// RelOps.ForEachAdjacentEndpointAt — the temporal sibling of relEndpointScanner.
// It yields the OTHER endpoint of edges passing the opts temporal filter while
// rejecting expired edges from inline valid-time stamps WITHOUT decoding the
// relationship row.
type relEndpointScannerAt interface {
	ForEachAdjacentEndpointAt(nid types.NodeID, typeToken uint16, incoming bool, opts storepkg.QueryOpts, fn func(rel types.RelID, other types.NodeID) bool) error
}

// ForEachAdjacentEndpointAt streams (relID, otherEndpoint) for the node's
// adjacency in the given direction, yielding only edges whose valid interval
// passes the opts temporal filter (ValidAt / ValidStart+ValidEnd) — WITHOUT
// decoding relationship rows when the store carries inline valid-time stamps.
// Stores without the native capability fall back to decoding via
// Outgoing/Incoming and applying the canonical MatchesTemporalFilter (correct,
// identical results — the in-memory store pays no decode anyway). With no
// temporal filter set this is exactly ForEachAdjacentEndpoint. fn returning
// false stops the scan.
func (r *RelOps) ForEachAdjacentEndpointAt(nodeID types.NodeID, typeName string, incoming bool, opts storepkg.QueryOpts, fn func(rel types.RelID, other types.NodeID) bool) error {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return grapherr.ErrNilCallback
	}
	if typeName != "" {
		if err := c.validateRelTypeQueryName(typeName); err != nil {
			return err
		}
	}
	if err := storepkg.ValidateNodeID(nodeID); err != nil {
		return err
	}
	if err := c.validateTemporalQueryOptsScan(opts); err != nil {
		return err
	}

	scanner, native := c.store.(relEndpointScannerAt)
	if !native {
		// Fallback: decode the rels, apply the canonical temporal predicate, and
		// project the OTHER endpoint.
		var rels []*types.Relationship
		var err error
		if incoming {
			rels, err = r.Incoming(nodeID, typeName)
		} else {
			rels, err = r.Outgoing(nodeID, typeName)
		}
		if err != nil {
			return err
		}
		for _, rel := range rels {
			if !storeutil.MatchesTemporalFilter(rel.InternalID().SnowflakeID(), rel.Temporal(), opts) {
				continue
			}
			other := rel.EndNodeID()
			if incoming {
				other = rel.StartNodeID()
			}
			if !fn(rel.InternalID(), other) {
				return nil
			}
		}
		return nil
	}

	var tok uint16
	if typeName != "" {
		var ok bool
		if err := c.readUnderRLock(func() error {
			tok, ok = c.lookupRelTypeQueryToken(typeName)
			return nil
		}); err != nil {
			return err
		}
		if !ok {
			return c.readUnderRLock(func() error {
				return c.validateRequestedNodesExist([]types.NodeID{nodeID})
			})
		}
	}
	return scanner.ForEachAdjacentEndpointAt(nodeID, tok, incoming, opts, fn)
}

// relRelScannerAt is the OPTIONAL store capability behind
// RelOps.ForEachAdjacentRelAt — the decode-arm sibling of relEndpointScannerAt.
// It streams DECODED relationship rows for edges passing the opts temporal
// filter while SKIPPING the decode of inline-stamp-rejected edges.
type relRelScannerAt interface {
	ForEachAdjacentRelAt(nid types.NodeID, typeToken uint16, incoming bool, opts storepkg.QueryOpts, fn func(*types.Relationship) bool) error
}

// ForEachAdjacentRelAt streams the DECODED relationships for the node's adjacency
// in the given direction, yielding only edges whose valid interval passes the
// opts temporal filter — skipping the msgpack decode of expired edges when the
// store carries inline valid-time stamps. Stores without the native capability
// fall back to decoding via Outgoing/Incoming and applying the canonical
// MatchesTemporalFilter. With no temporal filter this is exactly
// ForEachOutgoing/ForEachIncoming. fn returning false stops the scan.
func (r *RelOps) ForEachAdjacentRelAt(nodeID types.NodeID, typeName string, incoming bool, opts storepkg.QueryOpts, fn func(*types.Relationship) bool) error {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return grapherr.ErrNilCallback
	}
	if typeName != "" {
		if err := c.validateRelTypeQueryName(typeName); err != nil {
			return err
		}
	}
	if err := storepkg.ValidateNodeID(nodeID); err != nil {
		return err
	}
	if err := c.validateTemporalQueryOptsScan(opts); err != nil {
		return err
	}

	scanner, native := c.store.(relRelScannerAt)
	if !native {
		// Fallback: decode via Outgoing/Incoming, apply the canonical filter.
		var rels []*types.Relationship
		var err error
		if incoming {
			rels, err = r.Incoming(nodeID, typeName)
		} else {
			rels, err = r.Outgoing(nodeID, typeName)
		}
		if err != nil {
			return err
		}
		for _, rel := range rels {
			if !storeutil.MatchesTemporalFilter(rel.InternalID().SnowflakeID(), rel.Temporal(), opts) {
				continue
			}
			if !fn(rel) {
				return nil
			}
		}
		return nil
	}

	var tok uint16
	if typeName != "" {
		var ok bool
		if err := c.readUnderRLock(func() error {
			tok, ok = c.lookupRelTypeQueryToken(typeName)
			return nil
		}); err != nil {
			return err
		}
		if !ok {
			return c.readUnderRLock(func() error {
				return c.validateRequestedNodesExist([]types.NodeID{nodeID})
			})
		}
	}
	return scanner.ForEachAdjacentRelAt(nodeID, tok, incoming, opts, fn)
}

// Outgoing returns all outgoing relationships from the given node.
// If typeName is empty, all types are returned. If typeName is non-empty, only
// relationships of that type are returned (nil if the type is not registered).
func (r *RelOps) Outgoing(nodeID types.NodeID, typeName string) ([]*types.Relationship, error) {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if typeName != "" {
		if err := c.validateRelTypeQueryName(typeName); err != nil {
			return nil, err
		}
	}
	if err := storepkg.ValidateNodeID(nodeID); err != nil {
		return nil, err
	}
	var result []*types.Relationship
	err := c.readUnderRLock(func() error {
		var err error
		result, err = c.outgoingRelsLocked(nodeID, typeName)
		return err
	})
	return result, err
}

// outgoingRelsLocked is the lock-free body of RelOps.Outgoing.
func (c *Core) outgoingRelsLocked(nodeID types.NodeID, typeName string) ([]*types.Relationship, error) {
	var tok uint16
	if typeName != "" {
		t, ok := c.lookupRelTypeQueryToken(typeName)
		if !ok {
			return nil, c.validateRequestedNodesExist([]types.NodeID{nodeID})
		}
		tok = t
	}
	if !c.storeRowsTrust {
		if err := c.validateRequestedNodesExist([]types.NodeID{nodeID}); err != nil {
			return nil, err
		}
	}
	rels, err := c.store.OutgoingRelationships(nodeID, tok)
	if err != nil {
		return nil, err
	}
	if !c.storeRowsTrust {
		if err := c.validateOutgoingRelationshipRows(nodeID, tok, rels); err != nil {
			return nil, err
		}
		rels = copyRelationshipRows(rels)
	}
	return rels, nil
}

// OutgoingForNodes returns outgoing relationships for multiple nodes
// in a single batched operation. Returns a map from nodeID to its outgoing rels
// (sorted by ID). Nodes with zero outgoing rels are absent from the map.
// If typeName is non-empty, only relationships of that type are returned.
// Unregistered typeName returns nil, nil. nil/empty nodeIDs returns nil, nil.
func (r *RelOps) OutgoingForNodes(nodeIDs []types.NodeID, typeName string) (map[types.NodeID][]*types.Relationship, error) {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if typeName != "" {
		if err := c.validateRelTypeQueryName(typeName); err != nil {
			return nil, err
		}
	}
	if len(nodeIDs) == 0 {
		return nil, nil
	}
	for _, id := range nodeIDs {
		if err := storepkg.ValidateNodeID(id); err != nil {
			return nil, err
		}
	}
	var result map[types.NodeID][]*types.Relationship
	err := c.readUnderRLock(func() error {
		var err error
		result, err = c.outgoingRelsForNodesLocked(nodeIDs, typeName)
		return err
	})
	return result, err
}

// outgoingRelsForNodesLocked is the lock-free body of RelOps.OutgoingForNodes.
func (c *Core) outgoingRelsForNodesLocked(nodeIDs []types.NodeID, typeName string) (map[types.NodeID][]*types.Relationship, error) {
	var tok uint16
	if typeName != "" {
		t, ok := c.lookupRelTypeQueryToken(typeName)
		if !ok {
			return nil, c.validateRequestedNodesExist(nodeIDs)
		}
		tok = t
	}
	if !c.storeRowsTrust {
		if err := c.validateRequestedNodesExist(nodeIDs); err != nil {
			return nil, err
		}
	}
	rels, err := c.store.OutgoingRelationshipsForNodes(nodeIDs, tok)
	if err != nil {
		return nil, err
	}
	if !c.storeRowsTrust {
		if err := c.validateOutgoingRelationshipMap(nodeIDs, tok, rels); err != nil {
			return nil, err
		}
		rels = copyRelationshipRowMap(rels)
	}
	return rels, nil
}

// IncomingForNodes returns incoming relationships for multiple nodes
// in a single batched operation. Returns a map from nodeID to its incoming rels
// (sorted by ID). Nodes with zero incoming rels are absent from the map.
// If typeName is non-empty, only relationships of that type are returned.
// Unregistered typeName returns nil, nil. nil/empty nodeIDs returns nil, nil.
func (r *RelOps) IncomingForNodes(nodeIDs []types.NodeID, typeName string) (map[types.NodeID][]*types.Relationship, error) {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if typeName != "" {
		if err := c.validateRelTypeQueryName(typeName); err != nil {
			return nil, err
		}
	}
	if len(nodeIDs) == 0 {
		return nil, nil
	}
	for _, id := range nodeIDs {
		if err := storepkg.ValidateNodeID(id); err != nil {
			return nil, err
		}
	}
	var result map[types.NodeID][]*types.Relationship
	err := c.readUnderRLock(func() error {
		var err error
		result, err = c.incomingRelsForNodesLocked(nodeIDs, typeName)
		return err
	})
	return result, err
}

// incomingRelsForNodesLocked is the lock-free body of RelOps.IncomingForNodes.
func (c *Core) incomingRelsForNodesLocked(nodeIDs []types.NodeID, typeName string) (map[types.NodeID][]*types.Relationship, error) {
	var tok uint16
	if typeName != "" {
		t, ok := c.lookupRelTypeQueryToken(typeName)
		if !ok {
			return nil, c.validateRequestedNodesExist(nodeIDs)
		}
		tok = t
	}
	if !c.storeRowsTrust {
		if err := c.validateRequestedNodesExist(nodeIDs); err != nil {
			return nil, err
		}
	}
	rels, err := c.store.IncomingRelationshipsForNodes(nodeIDs, tok)
	if err != nil {
		return nil, err
	}
	if !c.storeRowsTrust {
		if err := c.validateIncomingRelationshipMap(nodeIDs, tok, rels); err != nil {
			return nil, err
		}
		rels = copyRelationshipRowMap(rels)
	}
	return rels, nil
}

// OutgoingForNodesAtTx is the bitemporal (transaction-time-pinned) counterpart
// of OutgoingForNodes: instead of the live adjacency index alone, it resolves
// each candidate relationship's version through the SAME chain seam the generic
// TxAt door uses (findRelVersionForOpts, which funnels through
// filterRelChainByTxAt — the tombstone-normalization seam shared with
// NodeAtTx/RelAtTx and the named as-of door).
//
// SEMANTICS — this door agrees with the TxAt-pinned BITEMPORAL door
// (QueryOpts{TxAt: txAt}) filtered by endpoint, NOT with a belief-state pin.
// The TxAt arm applies a POINT valid-time probe at wall-now when no valid-time
// opts are set (see the QueryOpts.TxAt warning): an edge whose valid interval
// lies wholly in the past — a CloseVersion-ed edge, or a width-1 [t, t+1)
// point-event edge — is SILENTLY DROPPED here even though it was believed at
// txAt. For pure knowledge-time (belief-state) semantics that returns every
// edge believed at the pin regardless of valid time, use OutgoingForNodesAtPin
// / IncomingForNodesAtPin instead (agreeing with QueryOpts{TxPin} filtered by
// endpoint). The two doors are deliberately distinct — do NOT treat this one as
// a belief-state pin.
//
// Rel endpoints are immutable, so the candidate relationship-ID set is the
// live per-node adjacency (seeds the common case) UNIONED with every DELETED
// relationship ID via forEachRelAdjacencyCandidateID — mirroring the
// single-node OutgoingRelsAt/IncomingRelsAt fold (see CLAUDE.md "Adjacency-at-t
// fold uses deleted-only iteration"). A relationship deleted after txAt is
// therefore still visible (delete is a transaction-time tombstone, lesson 60);
// one created after txAt is invisible; one deleted before txAt is invisible;
// a backfilled relationship (AddWithTx) is visible from its backfilled TxFrom
// onward, not from wall-clock creation time.
//
// txAt == 0 delegates to OutgoingForNodes verbatim (no TX filter — identical
// to the generic ByType(opts) door's "TxAt==0 means no temporal filter"
// convention, and it keeps existing callers of OutgoingForNodes unaffected —
// this method is purely additive). An unregistered typeName still validates
// that every requested node currently exists (mirrors OutgoingForNodes) and
// returns (nil, nil) on success. Returned relationships are sorted by ID
// within each node's slice.
func (r *RelOps) OutgoingForNodesAtTx(nodeIDs []types.NodeID, typeName string, txAt types.Instant) (map[types.NodeID][]*types.Relationship, error) {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if typeName != "" {
		if err := c.validateRelTypeQueryName(typeName); err != nil {
			return nil, err
		}
	}
	if len(nodeIDs) == 0 {
		return nil, nil
	}
	for _, id := range nodeIDs {
		if err := storepkg.ValidateNodeID(id); err != nil {
			return nil, err
		}
	}
	var result map[types.NodeID][]*types.Relationship
	err := c.readUnderRLock(func() error {
		var err error
		result, err = c.directionalRelsForNodesAtTxLocked(nodeIDs, typeName, txAt, true)
		return err
	})
	return result, err
}

// IncomingForNodesAtTx is the bitemporal counterpart of IncomingForNodes. See
// OutgoingForNodesAtTx for the resolution semantics (identical, mirrored for
// the incoming direction).
func (r *RelOps) IncomingForNodesAtTx(nodeIDs []types.NodeID, typeName string, txAt types.Instant) (map[types.NodeID][]*types.Relationship, error) {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if typeName != "" {
		if err := c.validateRelTypeQueryName(typeName); err != nil {
			return nil, err
		}
	}
	if len(nodeIDs) == 0 {
		return nil, nil
	}
	for _, id := range nodeIDs {
		if err := storepkg.ValidateNodeID(id); err != nil {
			return nil, err
		}
	}
	var result map[types.NodeID][]*types.Relationship
	err := c.readUnderRLock(func() error {
		var err error
		result, err = c.directionalRelsForNodesAtTxLocked(nodeIDs, typeName, txAt, false)
		return err
	})
	return result, err
}

// directionalRelsForNodesAtTxLocked is the lock-free shared body of
// OutgoingForNodesAtTx / IncomingForNodesAtTx. outgoing=true selects the
// start-endpoint (outgoing) direction. See OutgoingForNodesAtTx for the
// resolution semantics.
func (c *Core) directionalRelsForNodesAtTxLocked(nodeIDs []types.NodeID, typeName string, txAt types.Instant, outgoing bool) (map[types.NodeID][]*types.Relationship, error) {
	if txAt == 0 {
		if outgoing {
			return c.outgoingRelsForNodesLocked(nodeIDs, typeName)
		}
		return c.incomingRelsForNodesLocked(nodeIDs, typeName)
	}

	var tok uint16
	hasType := typeName != ""
	if hasType {
		t, ok := c.lookupRelTypeQueryToken(typeName)
		if !ok {
			return nil, c.validateRequestedNodesExist(nodeIDs)
		}
		tok = t
	}
	if !c.storeRowsTrust {
		if err := c.validateRequestedNodesExist(nodeIDs); err != nil {
			return nil, err
		}
	}

	var (
		liveRows map[types.NodeID][]*types.Relationship
		err      error
	)
	if outgoing {
		liveRows, err = c.store.OutgoingRelationshipsForNodes(nodeIDs, tok)
	} else {
		liveRows, err = c.store.IncomingRelationshipsForNodes(nodeIDs, tok)
	}
	if err != nil {
		return nil, err
	}

	seedIDs, err := c.relIDsFromNodeMapRows(nodeIDs, tok, liveRows, outgoing)
	if err != nil {
		return nil, err
	}

	requested := make(map[types.NodeID]struct{}, len(nodeIDs))
	for _, id := range nodeIDs {
		requested[id] = struct{}{}
	}

	opts := storepkg.QueryOpts{TxAt: txAt}
	var pred func(*types.Relationship) bool
	if hasType {
		pred = func(r *types.Relationship) bool { return r.HasTypeTokenRaw(tok) }
	}

	result := make(map[types.NodeID][]*types.Relationship)
	if err := c.forEachRelAdjacencyCandidateID(seedIDs, func(id types.RelID) error {
		r, err := c.findRelVersionForOpts(id, opts, pred)
		if err != nil {
			if errors.Is(err, storepkg.ErrNoVersionValidAt) || errors.Is(err, storepkg.ErrRelNotFound) {
				return nil
			}
			return err
		}
		var endpoint types.NodeID
		if outgoing {
			endpoint = r.StartNodeID()
		} else {
			endpoint = r.EndNodeID()
		}
		if _, ok := requested[endpoint]; !ok {
			return nil
		}
		result[endpoint] = append(result[endpoint], r)
		return nil
	}); err != nil {
		return nil, err
	}

	if len(result) == 0 {
		return nil, nil
	}
	for id := range result {
		storeutil.SortRelsByID(result[id])
	}
	return result, nil
}

// OutgoingForNodesAtPin is the pure knowledge-time (belief-state) counterpart of
// OutgoingForNodes / OutgoingForNodesAtTx: it returns every outgoing
// relationship that was BELIEVED at the transaction-time pin, with NO valid-time
// filtering. It agrees with ByType(QueryOpts{TxPin: pin}) filtered by endpoint,
// BY CONSTRUCTION — every candidate is resolved through the SAME as-of
// resolution the generic TxPin door uses (findRelVersionForOpts's TxPin arm ->
// relAsOfLocked -> the chain resolver + storeutil.SelectAsOf), so the two doors
// cannot drift (rule 17: two doors, same shape).
//
// This is the door to use for AS-OF-SYSTEM-TIME semantics. Unlike
// OutgoingForNodesAtTx (which valid-filters at wall-now when no valid opts are
// set and therefore silently drops an edge whose valid interval lies wholly in
// the past — a CloseVersion-ed edge, or a width-1 [t, t+1) point-event edge),
// this door returns EVERY edge believed at the pin: past-valid facts, point
// events, and unset-valid_from (snowflake-fallback) edges alike. An edge
// hard-deleted after the pin is still visible (delete is a transaction-time
// tombstone); one created after the pin is invisible; one deleted before the pin
// is invisible; a backfilled edge (AddWithTx) is visible from its backfilled
// TxFrom onward.
//
// Candidate set — rel endpoints are immutable, so the candidates are the live
// per-node adjacency of the CURRENTLY-EXISTING seeds UNIONED with every DELETED
// relationship ID via forEachRelAdjacencyCandidateID (the same fold ByType uses,
// see CLAUDE.md "Adjacency-at-t fold uses deleted-only iteration").
//
// SEED TOLERANCE — unlike OutgoingForNodes/AtTx, this door does NOT hard-error
// ErrNodeNotFound on a seed that is absent from CURRENT state. A seed that was
// part of the belief state at the pin but was HARD-DELETED afterwards is
// accepted: its live adjacency entries were purged by the cascade, so it is
// excluded from the store's live-adjacency probe (which would otherwise
// ErrNodeNotFound), but its pre-delete edges — themselves cascade-deleted, hence
// present in the deleted-rel fold and still naming the seed as their (immutable)
// endpoint — are recovered and returned. A seed created after the pin
// contributes nothing (its live edges resolve to empty at the pin). A seed that
// NEVER existed at the pin (or never existed at all) contributes nothing and is
// SKIPPED SILENTLY — matching ByType{TxPin} filtered by endpoint, which simply
// has no entry for such a node (no rel believed at the pin names it). Seed IDs
// are still format-validated (ValidateNodeID), so a zero/invalid ID is rejected.
//
// pin == 0 delegates to OutgoingForNodes verbatim (belief-state at the zero
// instant is "no pin" — identical to the generic ByType(QueryOpts{TxPin:0})
// door's "no temporal filter" convention; the current-state door's
// ErrNodeNotFound seed validation applies in that delegated case). Returned
// relationships are sorted by ID within each node's slice.
func (r *RelOps) OutgoingForNodesAtPin(nodeIDs []types.NodeID, typeName string, pin types.Instant) (map[types.NodeID][]*types.Relationship, error) {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if typeName != "" {
		if err := c.validateRelTypeQueryName(typeName); err != nil {
			return nil, err
		}
	}
	if len(nodeIDs) == 0 {
		return nil, nil
	}
	for _, id := range nodeIDs {
		if err := storepkg.ValidateNodeID(id); err != nil {
			return nil, err
		}
	}
	var result map[types.NodeID][]*types.Relationship
	err := c.readUnderRLock(func() error {
		var err error
		result, err = c.directionalRelsForNodesAtPinLocked(nodeIDs, typeName, pin, true)
		return err
	})
	return result, err
}

// IncomingForNodesAtPin is the belief-state counterpart of IncomingForNodes. See
// OutgoingForNodesAtPin for the resolution semantics (identical, mirrored for
// the incoming direction).
func (r *RelOps) IncomingForNodesAtPin(nodeIDs []types.NodeID, typeName string, pin types.Instant) (map[types.NodeID][]*types.Relationship, error) {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if typeName != "" {
		if err := c.validateRelTypeQueryName(typeName); err != nil {
			return nil, err
		}
	}
	if len(nodeIDs) == 0 {
		return nil, nil
	}
	for _, id := range nodeIDs {
		if err := storepkg.ValidateNodeID(id); err != nil {
			return nil, err
		}
	}
	var result map[types.NodeID][]*types.Relationship
	err := c.readUnderRLock(func() error {
		var err error
		result, err = c.directionalRelsForNodesAtPinLocked(nodeIDs, typeName, pin, false)
		return err
	})
	return result, err
}

// directionalRelsForNodesAtPinLocked is the lock-free shared body of
// OutgoingForNodesAtPin / IncomingForNodesAtPin. outgoing=true selects the
// start-endpoint (outgoing) direction. See OutgoingForNodesAtPin for the
// resolution semantics and the seed-tolerance contract.
func (c *Core) directionalRelsForNodesAtPinLocked(nodeIDs []types.NodeID, typeName string, pin types.Instant, outgoing bool) (map[types.NodeID][]*types.Relationship, error) {
	if pin == 0 {
		if outgoing {
			return c.outgoingRelsForNodesLocked(nodeIDs, typeName)
		}
		return c.incomingRelsForNodesLocked(nodeIDs, typeName)
	}

	var tok uint16
	hasType := typeName != ""
	if hasType {
		t, ok := c.lookupRelTypeQueryToken(typeName)
		if !ok {
			// Unregistered type: no relationship of this type exists at any pin.
			// Tolerant (unlike the AtTx door, which validates node existence):
			// the belief-state door never rejects seeds, so return no edges.
			return nil, nil
		}
		tok = t
	}

	// Partition the requested seeds by CURRENT existence for the live-adjacency
	// probe. A seed hard-deleted after the pin is NOT current (its live
	// adjacency was purged by the cascade) — passing it to
	// OutgoingRelationshipsForNodes would hard-error ErrNodeNotFound. We tolerate
	// such seeds: their pre-delete edges are recovered purely through the
	// deleted-rel fold below (rel endpoints are immutable, so a cascade-deleted
	// edge still names the deleted seed as its endpoint). `requested` keeps ALL
	// deduped seeds so the endpoint filter still admits a deleted seed's edges.
	requested := make(map[types.NodeID]struct{}, len(nodeIDs))
	liveSeedIDs := make([]types.NodeID, 0, len(nodeIDs))
	for _, id := range nodeIDs {
		if _, dup := requested[id]; dup {
			continue
		}
		requested[id] = struct{}{}
		if _, err := c.getCurrentNode(id); err != nil {
			if errors.Is(err, storepkg.ErrNodeNotFound) {
				continue
			}
			return nil, err
		}
		liveSeedIDs = append(liveSeedIDs, id)
	}

	var (
		liveRows map[types.NodeID][]*types.Relationship
		err      error
	)
	if len(liveSeedIDs) > 0 {
		if outgoing {
			liveRows, err = c.store.OutgoingRelationshipsForNodes(liveSeedIDs, tok)
		} else {
			liveRows, err = c.store.IncomingRelationshipsForNodes(liveSeedIDs, tok)
		}
		if err != nil {
			return nil, err
		}
	}

	seedIDs, err := c.relIDsFromNodeMapRows(liveSeedIDs, tok, liveRows, outgoing)
	if err != nil {
		return nil, err
	}

	opts := storepkg.QueryOpts{TxPin: pin}
	var pred func(*types.Relationship) bool
	if hasType {
		pred = func(r *types.Relationship) bool { return r.HasTypeTokenRaw(tok) }
	}

	result := make(map[types.NodeID][]*types.Relationship)
	if err := c.forEachRelAdjacencyCandidateID(seedIDs, func(id types.RelID) error {
		r, err := c.findRelVersionForOpts(id, opts, pred)
		if err != nil {
			if errors.Is(err, storepkg.ErrNoVersionValidAt) || errors.Is(err, storepkg.ErrRelNotFound) {
				return nil
			}
			return err
		}
		var endpoint types.NodeID
		if outgoing {
			endpoint = r.StartNodeID()
		} else {
			endpoint = r.EndNodeID()
		}
		if _, ok := requested[endpoint]; !ok {
			return nil
		}
		result[endpoint] = append(result[endpoint], r)
		return nil
	}); err != nil {
		return nil, err
	}

	if len(result) == 0 {
		return nil, nil
	}
	for id := range result {
		storeutil.SortRelsByID(result[id])
	}
	return result, nil
}

// Incoming returns all incoming relationships to the given node.
// If typeName is empty, all types are returned. If typeName is non-empty, only
// relationships of that type are returned (nil if the type is not registered).
// OutgoingDegree returns the number of outgoing relationships from nodeID,
// optionally filtered by type. It uses the store's DegreeCapability fast-path
// (count from the adjacency index, no entity materialization) when available,
// and otherwise falls back to len(Outgoing(...)).
func (r *RelOps) OutgoingDegree(nodeID types.NodeID, typeName string) (int, error) {
	return r.degree(nodeID, typeName, true)
}

// IncomingDegree returns the number of incoming relationships to nodeID,
// optionally filtered by type. See OutgoingDegree for semantics.
func (r *RelOps) IncomingDegree(nodeID types.NodeID, typeName string) (int, error) {
	return r.degree(nodeID, typeName, false)
}

func (r *RelOps) degree(nodeID types.NodeID, typeName string, outgoing bool) (int, error) {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return 0, err
	}
	if typeName != "" {
		if err := c.validateRelTypeQueryName(typeName); err != nil {
			return 0, err
		}
	}
	if err := storepkg.ValidateNodeID(nodeID); err != nil {
		return 0, err
	}
	var result int
	err := c.readUnderRLock(func() error {
		var err error
		result, err = c.degreeLocked(nodeID, typeName, outgoing)
		return err
	})
	return result, err
}

// degreeLocked is the lock-free body of OutgoingDegree/IncomingDegree.
func (c *Core) degreeLocked(nodeID types.NodeID, typeName string, outgoing bool) (int, error) {
	var tok uint16
	if typeName != "" {
		t, ok := c.lookupRelTypeQueryToken(typeName)
		if !ok {
			// Unknown type → zero, but still surface a missing-node error to
			// match the adjacency methods' contract.
			return 0, c.validateRequestedNodesExist([]types.NodeID{nodeID})
		}
		tok = t
	}
	if !c.storeRowsTrust {
		if err := c.validateRequestedNodesExist([]types.NodeID{nodeID}); err != nil {
			return 0, err
		}
	}
	if dc, ok := c.store.(storepkg.DegreeCapability); ok {
		if outgoing {
			return dc.OutgoingDegree(nodeID, tok)
		}
		return dc.IncomingDegree(nodeID, tok)
	}
	// Fallback: materialize and count (validated rows).
	var rels []*types.Relationship
	var err error
	if outgoing {
		rels, err = c.store.OutgoingRelationships(nodeID, tok)
	} else {
		rels, err = c.store.IncomingRelationships(nodeID, tok)
	}
	if err != nil {
		return 0, err
	}
	return len(rels), nil
}

func (r *RelOps) Incoming(nodeID types.NodeID, typeName string) ([]*types.Relationship, error) {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if typeName != "" {
		if err := c.validateRelTypeQueryName(typeName); err != nil {
			return nil, err
		}
	}
	if err := storepkg.ValidateNodeID(nodeID); err != nil {
		return nil, err
	}
	var result []*types.Relationship
	err := c.readUnderRLock(func() error {
		var err error
		result, err = c.incomingRelsLocked(nodeID, typeName)
		return err
	})
	return result, err
}

// incomingRelsLocked is the lock-free body of RelOps.Incoming.
func (c *Core) incomingRelsLocked(nodeID types.NodeID, typeName string) ([]*types.Relationship, error) {
	var tok uint16
	if typeName != "" {
		t, ok := c.lookupRelTypeQueryToken(typeName)
		if !ok {
			return nil, c.validateRequestedNodesExist([]types.NodeID{nodeID})
		}
		tok = t
	}
	if !c.storeRowsTrust {
		if err := c.validateRequestedNodesExist([]types.NodeID{nodeID}); err != nil {
			return nil, err
		}
	}
	rels, err := c.store.IncomingRelationships(nodeID, tok)
	if err != nil {
		return nil, err
	}
	if !c.storeRowsTrust {
		if err := c.validateIncomingRelationshipRows(nodeID, tok, rels); err != nil {
			return nil, err
		}
		rels = copyRelationshipRows(rels)
	}
	return rels, nil
}

// Count returns the number of nodes in the store.
func (n *NodeOps) Count() (int, error) {
	c := n.c
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed.Load() {
		return 0, ErrGraphClosed
	}
	return c.nodeCount()
}

// Count returns the number of relationships in the store.
func (r *RelOps) Count() (int, error) {
	c := r.c
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed.Load() {
		return 0, ErrGraphClosed
	}
	return c.relCount()
}

// All returns all nodes in the store, with optional pagination.
//
// History-aware when opts carries a temporal filter (ValidAt or
// ValidStart/ValidEnd): merges current and history IDs, resolves each via
// the version chain, and surfaces deleted entities that were valid at the
// query time. Without a temporal filter the fast store-side pushdown path
// is preserved.
func (n *NodeOps) All(opts storepkg.QueryOpts) ([]*types.Node, error) {
	c := n.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if err := c.validateTemporalQueryOptsScan(opts); err != nil {
		return nil, err
	}
	var result []*types.Node
	err := c.readUnderRLock(func() error {
		var err error
		result, err = c.allNodesLocked(opts)
		return err
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// allNodesLocked is the lock-free body of NodeOps.All.
func (c *Core) allNodesLocked(opts storepkg.QueryOpts) ([]*types.Node, error) {
	if !hasTemporalFilter(opts) {
		nodes, err := c.store.AllNodes(opts)
		if err != nil {
			return nil, err
		}
		if !c.storeRowsTrust {
			if err := validateAllNodePage(opts, nodes); err != nil {
				return nil, err
			}
			nodes = copyNodeRows(nodes)
		}
		return nodes, nil
	}
	var result []*types.Node
	err := c.forEachKnownNodeIDByDepth(opts.Depth, func(id types.NodeID) error {
		n, err := c.findNodeVersionForOpts(id, opts, nil)
		if err != nil {
			if errors.Is(err, storepkg.ErrNoVersionValidAt) || errors.Is(err, storepkg.ErrNodeNotFound) {
				return nil
			}
			return err
		}
		result = append(result, n)
		return nil
	})
	if err != nil {
		return nil, err
	}
	storeutil.SortNodesByID(result)
	result = storeutil.PaginateNodes(result, opts.After, opts.Limit)
	return result, nil
}

// ForEach streams all nodes matching opts to fn. For current-state unpaginated
// scans it walks the store's node-ID iterator and fetches one row at a time, so
// peak memory is O(1) in graph cardinality. Temporal and paginated scans fall
// back to All to preserve the existing history-aware and ordering semantics.
//
// Isolation is relaxed, matching ForEachByLabel: the ID set is snapshotted by
// the store iterator, then each row is fetched and fn is called without holding
// graph locks. Concurrently deleted rows may be skipped; concurrently created
// rows are not guaranteed to be seen.
func (n *NodeOps) ForEach(opts storepkg.QueryOpts, fn func(*types.Node) bool) error {
	c := n.c
	if err := c.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return grapherr.ErrNilCallback
	}
	if err := c.validateTemporalQueryOptsScan(opts); err != nil {
		return err
	}

	if !c.storeRowsTrust || hasTemporalFilter(opts) || opts.After != 0 || opts.Limit != 0 {
		nodes, err := n.All(opts)
		if err != nil {
			return err
		}
		for _, nd := range nodes {
			if !fn(nd) {
				return nil
			}
		}
		return nil
	}

	var iterErr error
	err := c.forEachNodeID(func(id types.NodeID) bool {
		nd, getErr := c.store.GetNode(id)
		if getErr != nil {
			if errors.Is(getErr, storepkg.ErrNodeNotFound) {
				return true
			}
			iterErr = getErr
			return false
		}
		return fn(nd)
	})
	if err != nil {
		return err
	}
	return iterErr
}

// All returns all relationships in the store, with optional
// pagination.
//
// History-aware when opts carries a temporal filter (ValidAt or
// ValidStart/ValidEnd): merges current and history IDs, resolves each via
// the version chain, and surfaces deleted relationships that were valid at
// the query time. Without a temporal filter the fast store-side pushdown
// path is preserved.
func (r *RelOps) All(opts storepkg.QueryOpts) ([]*types.Relationship, error) {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if err := c.validateTemporalQueryOptsScan(opts); err != nil {
		return nil, err
	}
	var result []*types.Relationship
	err := c.readUnderRLock(func() error {
		var err error
		result, err = c.allRelsLocked(opts)
		return err
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// allRelsLocked is the lock-free body of RelOps.All.
func (c *Core) allRelsLocked(opts storepkg.QueryOpts) ([]*types.Relationship, error) {
	if !hasTemporalFilter(opts) {
		rels, err := c.store.AllRelationships(opts)
		if err != nil {
			return nil, err
		}
		if !c.storeRowsTrust {
			if err := validateAllRelationshipPage(opts, rels); err != nil {
				return nil, err
			}
			rels = copyRelationshipRows(rels)
		}
		return rels, nil
	}
	var result []*types.Relationship
	err := c.forEachKnownRelIDByDepth(opts.Depth, func(id types.RelID) error {
		r, err := c.findRelVersionForOpts(id, opts, nil)
		if err != nil {
			if errors.Is(err, storepkg.ErrNoVersionValidAt) || errors.Is(err, storepkg.ErrRelNotFound) {
				return nil
			}
			return err
		}
		result = append(result, r)
		return nil
	})
	if err != nil {
		return nil, err
	}
	storeutil.SortRelsByID(result)
	result = storeutil.PaginateRels(result, opts.After, opts.Limit)
	return result, nil
}

// ForEach streams all relationships matching opts to fn. For current-state
// unpaginated scans it walks the store's rel-ID iterator and fetches one row
// at a time, so peak memory is O(1) in graph cardinality. Temporal and
// paginated scans fall back to All to preserve the existing history-aware and
// ordering semantics. Mirrors NodeOps.ForEach exactly for Node/Rel test
// parity (Testing Rule 2) — added alongside nodes.API.Iter / rels.API.Iter.
//
// Isolation is relaxed, matching ForEachByType: the ID set is snapshotted by
// the store iterator, then each row is fetched and fn is called without
// holding graph locks. Concurrently deleted rows may be skipped; concurrently
// created rows are not guaranteed to be seen.
func (r *RelOps) ForEach(opts storepkg.QueryOpts, fn func(*types.Relationship) bool) error {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return grapherr.ErrNilCallback
	}
	if err := c.validateTemporalQueryOptsScan(opts); err != nil {
		return err
	}

	if !c.storeRowsTrust || hasTemporalFilter(opts) || opts.After != 0 || opts.Limit != 0 {
		rels, err := r.All(opts)
		if err != nil {
			return err
		}
		for _, rel := range rels {
			if !fn(rel) {
				return nil
			}
		}
		return nil
	}

	var iterErr error
	err := c.forEachRelID(func(id types.RelID) bool {
		rel, getErr := c.store.GetRelationship(id)
		if getErr != nil {
			if errors.Is(getErr, storepkg.ErrRelNotFound) {
				return true
			}
			iterErr = getErr
			return false
		}
		return fn(rel)
	})
	if err != nil {
		return err
	}
	return iterErr
}

// GetByIDs returns nodes for every requested ID sorted by ascending ID.
// Nil or empty ids returns nil, nil after the graph lifecycle check.
// Missing IDs return store.ErrNodeNotFound.
func (n *NodeOps) GetByIDs(ids []types.NodeID) ([]*types.Node, error) {
	c := n.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, nil
	}
	for _, id := range ids {
		if err := storepkg.ValidateNodeID(id); err != nil {
			return nil, err
		}
	}
	var result []*types.Node
	err := c.readUnderRLock(func() error {
		var err error
		result, err = c.nodesByIDsLocked(ids)
		return err
	})
	return result, err
}

// nodesByIDsLocked is the lock-free body of NodeOps.GetByIDs.
func (c *Core) nodesByIDsLocked(ids []types.NodeID) ([]*types.Node, error) {
	nodes, err := c.store.GetNodesByIDs(ids)
	if err != nil {
		return nil, err
	}
	if !c.storeRowsTrust {
		if err := validateNodesByIDRows(ids, nodes); err != nil {
			return nil, err
		}
		nodes = copyNodeRows(nodes)
	}
	c.opNodeReads.Add(int64(len(nodes)))
	return nodes, nil
}

// GetByIDs returns relationships for every requested ID sorted by ascending ID.
// Nil or empty ids returns nil, nil after the graph lifecycle check.
// Missing IDs return store.ErrRelNotFound.
func (r *RelOps) GetByIDs(ids []types.RelID) ([]*types.Relationship, error) {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, nil
	}
	for _, id := range ids {
		if err := storepkg.ValidateRelID(id); err != nil {
			return nil, err
		}
	}
	var result []*types.Relationship
	err := c.readUnderRLock(func() error {
		var err error
		result, err = c.relsByIDsLocked(ids)
		return err
	})
	return result, err
}

// relsByIDsLocked is the lock-free body of RelOps.GetByIDs.
func (c *Core) relsByIDsLocked(ids []types.RelID) ([]*types.Relationship, error) {
	rels, err := c.store.GetRelationshipsByIDs(ids)
	if err != nil {
		return nil, err
	}
	if !c.storeRowsTrust {
		if err := validateRelationshipsByIDRows(ids, rels); err != nil {
			return nil, err
		}
		rels = copyRelationshipRows(rels)
	}
	c.opRelReads.Add(int64(len(rels)))
	return rels, nil
}

// --- Per-label / per-type statistics ---

// CountByLabel returns the number of nodes with the given label. O(1).
// Returns 0 if the label has never been registered.
//
// Hot-path inlined: see NodeOps.Get for rationale (B4).
func (n *NodeOps) CountByLabel(label string) (int, error) {
	c := n.c
	if err := c.validateIndexLabel(label); err != nil {
		return 0, err
	}
	c.mu.RLock()
	if c.closed.Load() {
		c.mu.RUnlock()
		return 0, ErrGraphClosed
	}
	tok, ok := c.labels.Lookup(label)
	if !ok {
		c.mu.RUnlock()
		return 0, nil
	}
	count, err := c.nodeCountByLabel(tok)
	c.mu.RUnlock()
	return count, err
}

// CountByType returns the number of relationships with the given type. O(1).
// Returns 0 if the type has never been registered.
//
// Hot-path inlined: see NodeOps.Get for rationale (B4).
func (r *RelOps) CountByType(typeName string) (int, error) {
	c := r.c
	if err := c.validateRelTypeQueryName(typeName); err != nil {
		return 0, err
	}
	c.mu.RLock()
	if c.closed.Load() {
		c.mu.RUnlock()
		return 0, ErrGraphClosed
	}
	tok, ok := c.lookupRelTypeQueryToken(typeName)
	if !ok {
		c.mu.RUnlock()
		return 0, nil
	}
	count, err := c.relCountByType(tok)
	c.mu.RUnlock()
	return count, err
}

// countByLabelLocked is the lock-free body of NodeOps.CountByLabel.
func (c *Core) countByLabelLocked(label string) (int, error) {
	tok, ok := c.labels.Lookup(label)
	if !ok {
		return 0, nil
	}
	return c.nodeCountByLabel(tok)
}

// countByTypeLocked is the lock-free body of RelOps.CountByType.
func (c *Core) countByTypeLocked(typeName string) (int, error) {
	tok, ok := c.lookupRelTypeQueryToken(typeName)
	if !ok {
		return 0, nil
	}
	return c.relCountByType(tok)
}

// (AllLabelCounts and AllRelTypeCounts moved to StatOps in stats.go.)

// relRangeScanner is the OPTIONAL store capability behind
// RelOps.ForEachByTypePropertyRange — the relationship mirror of
// nodeRangeScanner. Implemented by the memory and badger stores.
type relRangeScanner interface {
	ForEachRelByTypePropertyRange(typeToken uint16, propKey string, min, max float64, inclMin, inclMax bool, opts storepkg.QueryOpts, fn func(*types.Relationship) bool) error
}

// ForEachByTypePropertyRange streams relationships carrying the type whose
// NUMERIC propKey value lies within [min, max] (per the inclusivity flags), in
// snowflake-ID order — the relationship mirror of
// NodeOps.ForEachByLabelPropertyRange. Candidates come from the rel property
// index's ordered numeric view, which OVER-SELECTS by design (float64 sort
// keys, ulp-widened bounds): fn must re-check its predicate with exact
// comparison semantics. Returns storepkg.ErrIndexNotFound when no rel property
// index with a usable ordered view exists for (type, propKey) or the store
// lacks the capability — callers fall back to a type scan. Same relaxed
// isolation and frozen-row contract as ForEachByType; temporal-filter opts route
// through the store's per-row temporal check.
func (r *RelOps) ForEachByTypePropertyRange(typeName, propKey string, min, max float64, inclMin, inclMax bool, opts storepkg.QueryOpts, fn func(*types.Relationship) bool) error {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return grapherr.ErrNilCallback
	}
	if err := c.validateRelTypeQueryName(typeName); err != nil {
		return err
	}
	if err := c.validateTemporalQueryOptsScan(opts); err != nil {
		return err
	}
	scanner, native := c.store.(relRangeScanner)
	if !native || !c.storeRowsTrust {
		return storepkg.ErrIndexNotFound
	}
	var tok uint16
	var ok bool
	if err := c.readUnderRLock(func() error {
		tok, ok = c.lookupRelTypeQueryToken(typeName)
		return nil
	}); err != nil {
		return err
	}
	if !ok {
		return nil
	}
	// Deliberately outside c.mu — see ForEachByLabel's isolation note.
	return scanner.ForEachRelByTypePropertyRange(tok, propKey, min, max, inclMin, inclMax, opts, fn)
}

// relOrderedRangeScanner is the OPTIONAL store capability behind
// RelOps.ForEachByTypePropertyRangeOrdered — the relationship mirror of
// nodeOrderedRangeScanner (streaming NUMERIC range scans that emit in
// contractual VALUE ORDER — the ORDER BY prop [LIMIT k] / top-k access path).
// Implemented by the in-tree memory and badger stores (rel indexes are RAM-only).
type relOrderedRangeScanner interface {
	ForEachRelByTypePropertyRangeOrdered(typeToken uint16, propKey string, min, max float64, inclMin, inclMax, desc bool, fn func(*types.Relationship) bool) error
}

// ForEachByTypePropertyRangeOrdered streams relationships carrying the type whose
// NUMERIC propKey value lies within [min, max] to fn in CONTRACTUAL VALUE ORDER —
// ascending, or descending when desc — with ties (equal values) always broken by
// rel ID ASCENDING in both directions. It is the relationship mirror of
// NodeOps.ForEachByLabelPropertyRangeOrdered (the ordered / top-k access path):
// a query layer compiling `ORDER BY r.prop [ASC|DESC] LIMIT k` streams here and
// returns false from fn once it has k rows, so the LIMIT is pushed into the index
// and the scan stops at O(k + log n) index work — never materializing the whole
// range.
//
// Candidates come from the rel property index's ordered numeric view, which
// OVER-SELECTS by design (float64 sort keys, ulp-widened bounds): fn MUST
// re-check its predicate with exact comparison semantics.
//
// Non-temporal opts take the index-backed fast path (O(k + log n) top-k) and
// return storepkg.ErrIndexNotFound when no rel property index with a usable
// ordered view exists for (type, propKey) or the store lacks the capability. A
// TEMPORAL QueryOpts combination (ValidAt / ValidStart+ValidEnd / TxAt / TxPin) is
// instead served by a SOUND FULL FOLD (Stage B): every rel of the type is
// resolved to its version at the pin, filtered to [min,max] on the value-AT-t,
// and sorted by that value — so ordering over a past belief/valid state is
// correct and complete. The temporal path needs no rel property index (it reads
// resolved rel values directly) and is O(N log N) in the type's temporal
// membership. Same relaxed isolation and frozen-row contract as ForEachByType.
func (r *RelOps) ForEachByTypePropertyRangeOrdered(typeName, propKey string, min, max float64, inclMin, inclMax, desc bool, opts storepkg.QueryOpts, fn func(*types.Relationship) bool) error {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return grapherr.ErrNilCallback
	}
	if err := c.validateRelTypeQueryName(typeName); err != nil {
		return err
	}
	// Temporal ordered scan (Stage B): value-at-t is not indexed, so serve it as a
	// SOUND FULL FOLD — resolve every rel of the type to its version at the pin,
	// keep those whose numeric value is in [min,max], sort by value. Needs no rel
	// property index. Non-temporal opts take the index-backed fast path below.
	if opts.ValidAt != 0 || opts.ValidStart != 0 || opts.ValidEnd != 0 || opts.TxAt != 0 || opts.TxPin != 0 {
		if err := c.validateTemporalQueryOptsScan(opts); err != nil {
			return err
		}
		return forEachRelValueOrderedTemporal(c, typeName, opts, desc,
			func(rl *types.Relationship) (float64, bool) {
				v, ok := rl.GetProperty(propKey)
				if !ok {
					return 0, false
				}
				f, ok := coerceFloat64(v)
				if !ok || !numericInRange(f, min, max, inclMin, inclMax) {
					return 0, false
				}
				return f, true
			}, fn)
	}
	if err := storepkg.ValidateQueryOpts(opts); err != nil {
		return err
	}
	scanner, native := c.store.(relOrderedRangeScanner)
	if !native || !c.storeRowsTrust {
		return storepkg.ErrIndexNotFound
	}
	var tok uint16
	var ok bool
	if err := c.readUnderRLock(func() error {
		tok, ok = c.lookupRelTypeQueryToken(typeName)
		return nil
	}); err != nil {
		return err
	}
	if !ok {
		return nil
	}
	// Deliberately outside c.mu — see ForEachByLabel's isolation note.
	return scanner.ForEachRelByTypePropertyRangeOrdered(tok, propKey, min, max, inclMin, inclMax, desc, fn)
}

// relPrefixScanner is the OPTIONAL store capability behind
// RelOps.ForEachByTypePropertyPrefix — the relationship mirror of
// nodePrefixScanner (streaming STRING prefix scans in contractual lex value
// order). Implemented by the memory and badger stores.
type relPrefixScanner interface {
	ForEachRelByTypePropertyPrefix(typeToken uint16, propKey, prefix string, desc bool, fn func(*types.Relationship) bool) error
}

// ForEachByTypePropertyPrefix streams relationships carrying the type whose
// STRING propKey value begins with prefix, to fn in CONTRACTUAL VALUE ORDER —
// lexicographic ascending, or descending when desc — with ties (equal values)
// broken by rel ID ASCENDING in both directions. It is the relationship mirror of
// NodeOps.ForEachByLabelPropertyPrefix (the `STARTS WITH` access path). fn
// returning false stops the scan (LIMIT pushdown); an empty prefix matches every
// string value.
//
// Non-temporal opts take the index-backed fast path and return
// storepkg.ErrIndexNotFound when no usable rel property index exists for (type,
// propKey) or the store lacks the capability. A TEMPORAL QueryOpts combination is
// served by a SOUND FULL FOLD (Stage B): every rel of the type is resolved to its
// version at the pin, filtered to the prefix on the value-AT-t, and sorted
// lexicographically — needs no rel property index, O(N log N). Same relaxed
// isolation and frozen-row contract as ForEachByType.
func (r *RelOps) ForEachByTypePropertyPrefix(typeName, propKey, prefix string, desc bool, opts storepkg.QueryOpts, fn func(*types.Relationship) bool) error {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return grapherr.ErrNilCallback
	}
	if err := c.validateRelTypeQueryName(typeName); err != nil {
		return err
	}
	// Temporal prefix scan (Stage B): value-at-t is not indexed, so serve it as a
	// SOUND FULL FOLD — resolve every rel of the type to its version at the pin,
	// keep those whose string value begins with prefix, sort lexicographically.
	// Needs no rel property index. Non-temporal opts take the index-backed fast
	// path below.
	if opts.ValidAt != 0 || opts.ValidStart != 0 || opts.ValidEnd != 0 || opts.TxAt != 0 || opts.TxPin != 0 {
		if err := c.validateTemporalQueryOptsScan(opts); err != nil {
			return err
		}
		return forEachRelValueOrderedTemporal(c, typeName, opts, desc,
			func(rl *types.Relationship) (string, bool) {
				v, ok := rl.GetProperty(propKey)
				if !ok {
					return "", false
				}
				s, ok := v.(string)
				if !ok || !strings.HasPrefix(s, prefix) {
					return "", false
				}
				return s, true
			}, fn)
	}
	if err := storepkg.ValidateQueryOpts(opts); err != nil {
		return err
	}
	scanner, native := c.store.(relPrefixScanner)
	if !native || !c.storeRowsTrust {
		return storepkg.ErrIndexNotFound
	}
	var tok uint16
	var ok bool
	if err := c.readUnderRLock(func() error {
		tok, ok = c.lookupRelTypeQueryToken(typeName)
		return nil
	}); err != nil {
		return err
	}
	if !ok {
		return nil
	}
	// Deliberately outside c.mu — see ForEachByLabel's isolation note.
	return scanner.ForEachRelByTypePropertyPrefix(tok, propKey, prefix, desc, fn)
}
