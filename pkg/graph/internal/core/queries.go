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
	return c.Nodes.GetWithContext(context.Background(), id)
}

// Get retrieves a relationship by snowflake ID.
func (r *RelOps) Get(id types.RelID) (*types.Relationship, error) {
	c := r.c
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
	tok, ok := c.labels.Lookup(label)
	if !ok {
		return nil, nil
	}
	if !hasTemporalFilter(opts) {
		return c.store.NodesByLabel(tok, opts)
	}
	if opts.Depth != storepkg.DepthAll {
		return nil, ErrDepthTemporalUnsupported
	}
	// Indexed candidate set: current nodes that carry the label NOW, plus all
	// node IDs that ever appeared in history (covering the case where the
	// label was held in a previous version but not the current one). Avoids
	// a full ForEachNodeID scan.
	current, err := c.store.NodesByLabel(tok, storepkg.QueryOpts{})
	if err != nil {
		return nil, err
	}
	currentIDs := make([]types.NodeID, 0, len(current))
	for _, n := range current {
		currentIDs = append(currentIDs, n.ID())
	}

	pred := func(n *types.Node) bool { return n.HasLabelTokenRaw(tok) }
	var result []*types.Node
	if err := c.forEachNodeCandidateID(currentIDs, func(id types.NodeID) error {
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
	return storeutil.PaginateNodes(result, opts.After, opts.Limit), nil
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
	tok, ok := c.relTypes.Lookup(typeName)
	if !ok {
		return nil, nil
	}
	if !hasTemporalFilter(opts) {
		return c.store.RelationshipsByType(tok, opts)
	}
	if opts.Depth != storepkg.DepthAll {
		return nil, ErrDepthTemporalUnsupported
	}
	// Indexed candidate set: current rels of this type, plus history IDs.
	// Type tokens are structurally immutable so the type predicate is only
	// needed for safety; history IDs cover the deleted-rel case.
	current, err := c.store.RelationshipsByType(tok, storepkg.QueryOpts{})
	if err != nil {
		return nil, err
	}
	currentIDs := make([]types.RelID, 0, len(current))
	for _, r := range current {
		currentIDs = append(currentIDs, r.ID())
	}

	pred := func(r *types.Relationship) bool { return r.HasTypeTokenRaw(tok) }
	var result []*types.Relationship
	if err := c.forEachRelCandidateID(currentIDs, func(id types.RelID) error {
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
	return storeutil.PaginateRels(result, opts.After, opts.Limit), nil
}

// Outgoing returns all outgoing relationships from the given node.
// If typeName is empty, all types are returned. If typeName is non-empty, only
// relationships of that type are returned (nil if the type is not registered).
func (r *RelOps) Outgoing(nodeID types.NodeID, typeName string) ([]*types.Relationship, error) {
	c := r.c
	var tok uint16
	if typeName != "" {
		t, ok := c.relTypes.Lookup(typeName)
		if !ok {
			return nil, nil
		}
		tok = t
	}
	return c.store.OutgoingRelationships(nodeID, tok)
}

// OutgoingForNodes returns outgoing relationships for multiple nodes
// in a single batched operation. Returns a map from nodeID to its outgoing rels
// (sorted by ID). Nodes with zero outgoing rels are absent from the map.
// If typeName is non-empty, only relationships of that type are returned.
// Unregistered typeName returns nil, nil. nil/empty nodeIDs returns nil, nil.
func (r *RelOps) OutgoingForNodes(nodeIDs []types.NodeID, typeName string) (map[types.NodeID][]*types.Relationship, error) {
	c := r.c
	if len(nodeIDs) == 0 {
		return nil, nil
	}
	var tok uint16
	if typeName != "" {
		t, ok := c.relTypes.Lookup(typeName)
		if !ok {
			return nil, nil
		}
		tok = t
	}
	return c.store.OutgoingRelationshipsForNodes(nodeIDs, tok)
}

// IncomingForNodes returns incoming relationships for multiple nodes
// in a single batched operation. Returns a map from nodeID to its incoming rels
// (sorted by ID). Nodes with zero incoming rels are absent from the map.
// If typeName is non-empty, only relationships of that type are returned.
// Unregistered typeName returns nil, nil. nil/empty nodeIDs returns nil, nil.
func (r *RelOps) IncomingForNodes(nodeIDs []types.NodeID, typeName string) (map[types.NodeID][]*types.Relationship, error) {
	c := r.c
	if len(nodeIDs) == 0 {
		return nil, nil
	}
	var tok uint16
	if typeName != "" {
		t, ok := c.relTypes.Lookup(typeName)
		if !ok {
			return nil, nil
		}
		tok = t
	}
	return c.store.IncomingRelationshipsForNodes(nodeIDs, tok)
}

// Incoming returns all incoming relationships to the given node.
// If typeName is empty, all types are returned. If typeName is non-empty, only
// relationships of that type are returned (nil if the type is not registered).
func (r *RelOps) Incoming(nodeID types.NodeID, typeName string) ([]*types.Relationship, error) {
	c := r.c
	var tok uint16
	if typeName != "" {
		t, ok := c.relTypes.Lookup(typeName)
		if !ok {
			return nil, nil
		}
		tok = t
	}
	return c.store.IncomingRelationships(nodeID, tok)
}

// Count returns the number of nodes in the store.
func (n *NodeOps) Count() (int, error) {
	c := n.c
	return c.store.NodeCount()
}

// Count returns the number of relationships in the store.
func (r *RelOps) Count() (int, error) {
	c := r.c
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
	if !hasTemporalFilter(opts) {
		return c.store.AllNodes(opts)
	}
	if opts.Depth != storepkg.DepthAll {
		return nil, ErrDepthTemporalUnsupported
	}
	var result []*types.Node
	err := c.forEachKnownNodeID(func(id types.NodeID) error {
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
	return storeutil.PaginateNodes(result, opts.After, opts.Limit), nil
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
	if !hasTemporalFilter(opts) {
		return c.store.AllRelationships(opts)
	}
	if opts.Depth != storepkg.DepthAll {
		return nil, ErrDepthTemporalUnsupported
	}
	var result []*types.Relationship
	err := c.forEachKnownRelID(func(id types.RelID) error {
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
	return storeutil.PaginateRels(result, opts.After, opts.Limit), nil
}

// GetByIDs returns nodes matching the given IDs. Missing IDs are skipped.
func (n *NodeOps) GetByIDs(ids []types.NodeID) ([]*types.Node, error) {
	c := n.c
	return c.store.GetNodesByIDs(ids)
}

// GetByIDs returns relationships matching the given IDs. Missing IDs are skipped.
func (r *RelOps) GetByIDs(ids []types.RelID) ([]*types.Relationship, error) {
	c := r.c
	return c.store.GetRelationshipsByIDs(ids)
}

// --- Per-label / per-type statistics ---

// CountByLabel returns the number of nodes with the given label. O(1).
// Returns 0 if the label has never been registered.
func (n *NodeOps) CountByLabel(label string) (int, error) {
	c := n.c
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
	tok, ok := c.relTypes.Lookup(typeName)
	if !ok {
		return 0, nil
	}
	return c.store.RelCountByType(tok)
}

// (AllLabelCounts and AllRelTypeCounts moved to StatOps in stats.go.)
