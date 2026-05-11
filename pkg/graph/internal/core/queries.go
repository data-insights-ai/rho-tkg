package core

import (
	"context"
	"errors"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"

	storeutil "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/storeutil"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// --- Store passthrough queries ---

// Get retrieves a node by snowflake ID.
func (n *NodeOps) Get(id types.NodeID) (*types.Node, error) {
	c := n.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	return c.Nodes.GetWithContext(context.Background(), id)
}

// Get retrieves a relationship by snowflake ID.
func (r *RelOps) Get(id types.RelID) (*types.Relationship, error) {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	return c.Rels.GetWithContext(context.Background(), id)
}

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
		currentIDs := make([]types.NodeID, 0, len(current))
		for _, n := range current {
			currentIDs = append(currentIDs, n.ID())
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
	if err := c.validateIndexName(typeName); err != nil {
		return nil, err
	}
	if err := storepkg.ValidateQueryOpts(opts); err != nil {
		return nil, err
	}
	var result []*types.Relationship
	err := c.readUnderRLock(func() error {
		tok, ok := c.relTypes.Lookup(typeName)
		if !ok {
			return nil
		}
		if !hasTemporalFilter(opts) {
			rels, err := c.store.RelationshipsByType(tok, opts)
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
		currentIDs := make([]types.RelID, 0, len(current))
		for _, r := range current {
			currentIDs = append(currentIDs, r.ID())
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
		if err := c.validateIndexName(typeName); err != nil {
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
			t, ok := c.relTypes.Lookup(typeName)
			if !ok {
				return nil
			}
			tok = t
		}
		rels, err := c.store.OutgoingRelationships(nodeID, tok)
		result = rels
		return err
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
		if err := c.validateIndexName(typeName); err != nil {
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
			t, ok := c.relTypes.Lookup(typeName)
			if !ok {
				return nil
			}
			tok = t
		}
		rels, err := c.store.OutgoingRelationshipsForNodes(nodeIDs, tok)
		result = rels
		return err
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
		if err := c.validateIndexName(typeName); err != nil {
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
			t, ok := c.relTypes.Lookup(typeName)
			if !ok {
				return nil
			}
			tok = t
		}
		rels, err := c.store.IncomingRelationshipsForNodes(nodeIDs, tok)
		result = rels
		return err
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
		if err := c.validateIndexName(typeName); err != nil {
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
			t, ok := c.relTypes.Lookup(typeName)
			if !ok {
				return nil
			}
			tok = t
		}
		rels, err := c.store.IncomingRelationships(nodeID, tok)
		result = rels
		return err
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
	return c.store.NodeCount()
}

// Count returns the number of relationships in the store.
func (r *RelOps) Count() (int, error) {
	c := r.c
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed.Load() {
		return 0, ErrGraphClosed
	}
	return c.store.RelationshipCount()
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
			result = nodes
			return err
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
			result = rels
			return err
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
// Missing IDs return store.ErrNodeNotFound.
func (n *NodeOps) GetByIDs(ids []types.NodeID) ([]*types.Node, error) {
	c := n.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	for _, id := range ids {
		if err := storepkg.ValidateNodeID(id); err != nil {
			return nil, err
		}
	}
	var result []*types.Node
	err := c.readUnderRLock(func() error {
		nodes, err := c.store.GetNodesByIDs(ids)
		result = nodes
		return err
	})
	return result, err
}

// GetByIDs returns relationships for every requested ID sorted by ascending ID.
// Missing IDs return store.ErrRelNotFound.
func (r *RelOps) GetByIDs(ids []types.RelID) ([]*types.Relationship, error) {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	for _, id := range ids {
		if err := storepkg.ValidateRelID(id); err != nil {
			return nil, err
		}
	}
	var result []*types.Relationship
	err := c.readUnderRLock(func() error {
		rels, err := c.store.GetRelationshipsByIDs(ids)
		result = rels
		return err
	})
	return result, err
}

// --- Per-label / per-type statistics ---

// CountByLabel returns the number of nodes with the given label. O(1).
// Returns 0 if the label has never been registered.
func (n *NodeOps) CountByLabel(label string) (int, error) {
	c := n.c
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed.Load() {
		return 0, ErrGraphClosed
	}
	if err := c.validateIndexLabel(label); err != nil {
		return 0, err
	}
	tok, ok := c.labels.Lookup(label)
	if !ok {
		return 0, nil
	}
	return c.store.NodeCountByLabel(tok)
}

// CountByType returns the number of relationships with the given type. O(1).
// Returns 0 if the type has never been registered.
func (r *RelOps) CountByType(typeName string) (int, error) {
	c := r.c
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed.Load() {
		return 0, ErrGraphClosed
	}
	if err := c.validateIndexName(typeName); err != nil {
		return 0, err
	}
	tok, ok := c.cachedRelType(typeName)
	if !ok {
		tok, ok = c.relTypes.Lookup(typeName)
	}
	if !ok {
		return 0, nil
	}
	return c.store.RelCountByType(tok)
}

// (AllLabelCounts and AllRelTypeCounts moved to StatOps in stats.go.)
