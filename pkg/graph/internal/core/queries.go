package core

import (
	"errors"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/store"

	storeutil "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/internal/storeutil"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/types"
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
		tok, ok := c.labels.Lookup(label)
		if !ok {
			return nil
		}
		if !hasTemporalFilter(opts) {
			nodes, err := c.store.NodesByLabel(tok, opts)
			if err == nil && !c.storeRowsTrust {
				if err = c.validateNodesByLabelPage(tok, opts, nodes); err == nil {
					nodes = copyNodeRows(nodes)
				}
			}
			result = nodes
			return err
		}
		// Indexed candidate set: current nodes that carry the label NOW, plus all
		// node IDs that ever appeared in history (covering the case where the
		// label was held in a previous version but not the current one). Avoids
		// a full ForEachNodeID scan.
		current, err := c.store.NodesByLabel(tok, storepkg.QueryOpts{Depth: opts.Depth})
		if err != nil {
			return err
		}
		currentIDs, err := c.nodeIDsFromLabelRows(tok, current)
		if err != nil {
			return err
		}

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
			return err
		}
		storeutil.SortNodesByID(result)
		result = storeutil.PaginateNodes(result, opts.After, opts.Limit)
		return nil
	})
	if err != nil {
		return nil, err
	}
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
		tok, ok := c.lookupRelTypeQueryToken(typeName)
		if !ok {
			return nil
		}
		if !hasTemporalFilter(opts) {
			rels, err := c.store.RelationshipsByType(tok, opts)
			if err == nil && !c.storeRowsTrust {
				if err = c.validateRelationshipsByTypePage(tok, opts, rels); err == nil {
					rels = copyRelationshipRows(rels)
				}
			}
			result = rels
			return err
		}
		// Indexed candidate set: current rels of this type, plus history IDs.
		// Type tokens are structurally immutable so the type predicate is only
		// needed for safety; history IDs cover the deleted-rel case.
		current, err := c.store.RelationshipsByType(tok, storepkg.QueryOpts{Depth: opts.Depth})
		if err != nil {
			return err
		}
		currentIDs, err := c.relIDsFromTypeRows(tok, current)
		if err != nil {
			return err
		}

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
			return err
		}
		storeutil.SortRelsByID(result)
		result = storeutil.PaginateRels(result, opts.After, opts.Limit)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
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
		var tok uint16
		if typeName != "" {
			t, ok := c.lookupRelTypeQueryToken(typeName)
			if !ok {
				return c.validateRequestedNodesExist([]types.NodeID{nodeID})
			}
			tok = t
		}
		if !c.storeRowsTrust {
			if err := c.validateRequestedNodesExist([]types.NodeID{nodeID}); err != nil {
				return err
			}
		}
		rels, err := c.store.OutgoingRelationships(nodeID, tok)
		if err != nil {
			return err
		}
		if !c.storeRowsTrust {
			if err := c.validateOutgoingRelationshipRows(nodeID, tok, rels); err != nil {
				return err
			}
			rels = copyRelationshipRows(rels)
		}
		result = rels
		return nil
	})
	return result, err
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
		var tok uint16
		if typeName != "" {
			t, ok := c.lookupRelTypeQueryToken(typeName)
			if !ok {
				return c.validateRequestedNodesExist(nodeIDs)
			}
			tok = t
		}
		if !c.storeRowsTrust {
			if err := c.validateRequestedNodesExist(nodeIDs); err != nil {
				return err
			}
		}
		rels, err := c.store.OutgoingRelationshipsForNodes(nodeIDs, tok)
		if err != nil {
			return err
		}
		if !c.storeRowsTrust {
			if err := c.validateOutgoingRelationshipMap(nodeIDs, tok, rels); err != nil {
				return err
			}
			rels = copyRelationshipRowMap(rels)
		}
		result = rels
		return nil
	})
	return result, err
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
		var tok uint16
		if typeName != "" {
			t, ok := c.lookupRelTypeQueryToken(typeName)
			if !ok {
				return c.validateRequestedNodesExist(nodeIDs)
			}
			tok = t
		}
		if !c.storeRowsTrust {
			if err := c.validateRequestedNodesExist(nodeIDs); err != nil {
				return err
			}
		}
		rels, err := c.store.IncomingRelationshipsForNodes(nodeIDs, tok)
		if err != nil {
			return err
		}
		if !c.storeRowsTrust {
			if err := c.validateIncomingRelationshipMap(nodeIDs, tok, rels); err != nil {
				return err
			}
			rels = copyRelationshipRowMap(rels)
		}
		result = rels
		return nil
	})
	return result, err
}

// Incoming returns all incoming relationships to the given node.
// If typeName is empty, all types are returned. If typeName is non-empty, only
// relationships of that type are returned (nil if the type is not registered).
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
		var tok uint16
		if typeName != "" {
			t, ok := c.lookupRelTypeQueryToken(typeName)
			if !ok {
				return c.validateRequestedNodesExist([]types.NodeID{nodeID})
			}
			tok = t
		}
		if !c.storeRowsTrust {
			if err := c.validateRequestedNodesExist([]types.NodeID{nodeID}); err != nil {
				return err
			}
		}
		rels, err := c.store.IncomingRelationships(nodeID, tok)
		if err != nil {
			return err
		}
		if !c.storeRowsTrust {
			if err := c.validateIncomingRelationshipRows(nodeID, tok, rels); err != nil {
				return err
			}
			rels = copyRelationshipRows(rels)
		}
		result = rels
		return nil
	})
	return result, err
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
		if !hasTemporalFilter(opts) {
			nodes, err := c.store.AllNodes(opts)
			if err != nil {
				return err
			}
			if !c.storeRowsTrust {
				if err := validateAllNodePage(opts, nodes); err != nil {
					return err
				}
				nodes = copyNodeRows(nodes)
			}
			result = nodes
			return nil
		}
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
			return err
		}
		storeutil.SortNodesByID(result)
		result = storeutil.PaginateNodes(result, opts.After, opts.Limit)
		return nil
	})
	if err != nil {
		return nil, err
	}
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
		if !hasTemporalFilter(opts) {
			rels, err := c.store.AllRelationships(opts)
			if err != nil {
				return err
			}
			if !c.storeRowsTrust {
				if err := validateAllRelationshipPage(opts, rels); err != nil {
					return err
				}
				rels = copyRelationshipRows(rels)
			}
			result = rels
			return nil
		}
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
			return err
		}
		storeutil.SortRelsByID(result)
		result = storeutil.PaginateRels(result, opts.After, opts.Limit)
		return nil
	})
	if err != nil {
		return nil, err
	}
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
		nodes, err := c.store.GetNodesByIDs(ids)
		if err != nil {
			return err
		}
		if !c.storeRowsTrust {
			if err := validateNodesByIDRows(ids, nodes); err != nil {
				return err
			}
			nodes = copyNodeRows(nodes)
		}
		result = nodes
		c.opNodeReads.Add(int64(len(nodes)))
		return nil
	})
	return result, err
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
		rels, err := c.store.GetRelationshipsByIDs(ids)
		if err != nil {
			return err
		}
		if !c.storeRowsTrust {
			if err := validateRelationshipsByIDRows(ids, rels); err != nil {
				return err
			}
			rels = copyRelationshipRows(rels)
		}
		result = rels
		c.opRelReads.Add(int64(len(rels)))
		return nil
	})
	return result, err
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

// (AllLabelCounts and AllRelTypeCounts moved to StatOps in stats.go.)
