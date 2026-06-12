package core

import (
	"errors"

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
	if err := storepkg.ValidateQueryOpts(opts); err != nil {
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
// value lies in [min,max] (inclusivity per flags), computed from the bit-sliced
// index with NO node scan. exact=false declines — the caller must scan-and-count
// — when the store lacks the capability, the index is poisoned/absent, the
// bounds are not exact integers, or a temporal filter is set (the BSI is
// valid-time agnostic). The bounds MUST capture the whole predicate; the caller
// enforces that (only the count-over-pure-range fast path may use this).
func (n *NodeOps) RangeCardinality(label, propKey string, min, max float64, inclMin, inclMax bool, opts storepkg.QueryOpts) (int64, bool, error) {
	c := n.c
	if err := c.checkOpen(); err != nil {
		return 0, false, err
	}
	if opts.ValidAt != 0 || opts.ValidStart != 0 || opts.ValidEnd != 0 {
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
	if err := storepkg.ValidateQueryOpts(opts); err != nil {
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
	if err := storepkg.ValidateQueryOpts(opts); err != nil {
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

	var result []*types.Node
	pred := func(n *types.Node) bool { return n.HasLabelTokenRaw(tok) }
	if err := c.forEachNodeCandidateIDByDepth(currentIDs, opts.Depth, func(id types.NodeID) error {
		n, err := c.findNodeVersionForOpts(id, opts, pred)
		if err != nil {
			if errors.Is(err, storepkg.ErrNoVersionValidAt) || errors.Is(err, storepkg.ErrNodeNotFound) {
				return nil
			}
			return err
		}
		result = append(result, n)
		return nil
	}); err != nil {
		return nil, err
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
	if err := storepkg.ValidateQueryOpts(opts); err != nil {
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

	var result []*types.Relationship
	pred := func(r *types.Relationship) bool { return r.HasTypeTokenRaw(tok) }
	if err := c.forEachRelCandidateIDByDepth(currentIDs, opts.Depth, func(id types.RelID) error {
		r, err := c.findRelVersionForOpts(id, opts, pred)
		if err != nil {
			if errors.Is(err, storepkg.ErrNoVersionValidAt) || errors.Is(err, storepkg.ErrRelNotFound) {
				return nil
			}
			return err
		}
		result = append(result, r)
		return nil
	}); err != nil {
		return nil, err
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
	if err := storepkg.ValidateQueryOpts(opts); err != nil {
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
// relationship row (OPT15).
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
	if err := storepkg.ValidateQueryOpts(opts); err != nil {
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
// filter while SKIPPING the decode of inline-stamp-rejected edges (OPT15).
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
	if err := storepkg.ValidateQueryOpts(opts); err != nil {
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
	if err := storepkg.ValidateQueryOpts(opts); err != nil {
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
	if err := storepkg.ValidateQueryOpts(opts); err != nil {
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
