package graph

import (
	"context"
	"errors"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// --- Store passthrough queries ---

// GetNode retrieves a node by snowflake ID.
func (g *Graph) GetNode(id types.NodeID) (*types.Node, error) {
	return g.GetNodeWithContext(context.Background(), id)
}

// GetRelationship retrieves a relationship by snowflake ID.
func (g *Graph) GetRelationship(id types.RelID) (*types.Relationship, error) {
	return g.GetRelationshipWithContext(context.Background(), id)
}

// NodesByLabel returns nodes with the given label (resolved from string),
// with optional pagination. Returns nil if the label is not registered.
//
// When opts carries a temporal filter (ValidAt or ValidStart/ValidEnd) the
// query is history-aware: every known node (current + history) is scanned and
// any version whose label set contained the requested label at the requested
// time matches. Without a temporal filter, the call falls through to the
// store-level label index for O(matches) lookup.
func (g *Graph) NodesByLabel(label string, opts QueryOpts) ([]*types.Node, error) {
	tok, ok := g.labels.Lookup(label)
	if !ok {
		return nil, nil
	}
	if !hasTemporalFilter(opts) {
		return g.store.NodesByLabel(tok, opts)
	}
	if opts.Depth != DepthAll {
		return nil, ErrDepthTemporalUnsupported
	}
	// Indexed candidate set: current nodes that carry the label NOW, plus all
	// node IDs that ever appeared in history (covering the case where the
	// label was held in a previous version but not the current one). Avoids
	// a full ForEachNodeID scan.
	current, err := g.store.NodesByLabel(tok, QueryOpts{})
	if err != nil {
		return nil, err
	}
	currentIDs := make([]types.NodeID, 0, len(current))
	for _, n := range current {
		currentIDs = append(currentIDs, n.ID())
	}

	pred := func(n *types.Node) bool { return n.HasLabelTokenRaw(tok) }
	var result []*types.Node
	if err := g.forEachNodeCandidateID(currentIDs, func(id types.NodeID) error {
		n, err := g.findNodeVersionForOpts(id, opts, pred)
		if err != nil {
			if errors.Is(err, ErrNoVersionValidAt) || errors.Is(err, ErrNodeNotFound) {
				return nil
			}
			return err
		}
		result = append(result, n)
		return nil
	}); err != nil {
		return nil, err
	}
	sortNodesByID(result)
	return paginateNodes(result, opts.After, opts.Limit), nil
}

// RelationshipsByType returns relationships with the given type (resolved from string),
// with optional pagination. Returns nil if the type is not registered.
//
// When opts carries a temporal filter, every known relationship (current +
// history) is scanned. The type token is structurally immutable, so the only
// history-relevant information added by this scan is deleted/closed-out
// relationships that the current type index no longer references. Without a
// temporal filter, the call falls through to the store-level type index.
func (g *Graph) RelationshipsByType(typeName string, opts QueryOpts) ([]*types.Relationship, error) {
	tok, ok := g.relTypes.Lookup(typeName)
	if !ok {
		return nil, nil
	}
	if !hasTemporalFilter(opts) {
		return g.store.RelationshipsByType(tok, opts)
	}
	if opts.Depth != DepthAll {
		return nil, ErrDepthTemporalUnsupported
	}
	// Indexed candidate set: current rels of this type, plus history IDs.
	// Type tokens are structurally immutable so the type predicate is only
	// needed for safety; history IDs cover the deleted-rel case.
	current, err := g.store.RelationshipsByType(tok, QueryOpts{})
	if err != nil {
		return nil, err
	}
	currentIDs := make([]types.RelID, 0, len(current))
	for _, r := range current {
		currentIDs = append(currentIDs, r.ID())
	}

	pred := func(r *types.Relationship) bool { return r.HasTypeTokenRaw(tok) }
	var result []*types.Relationship
	if err := g.forEachRelCandidateID(currentIDs, func(id types.RelID) error {
		r, err := g.findRelVersionForOpts(id, opts, pred)
		if err != nil {
			if errors.Is(err, ErrNoVersionValidAt) || errors.Is(err, ErrRelNotFound) {
				return nil
			}
			return err
		}
		result = append(result, r)
		return nil
	}); err != nil {
		return nil, err
	}
	sortRelsByID(result)
	return paginateRels(result, opts.After, opts.Limit), nil
}

// OutgoingRelationships returns all outgoing relationships from the given node.
// If typeName is empty, all types are returned. If typeName is non-empty, only
// relationships of that type are returned (nil if the type is not registered).
func (g *Graph) OutgoingRelationships(nodeID types.NodeID, typeName string) ([]*types.Relationship, error) {
	var tok uint16
	if typeName != "" {
		t, ok := g.relTypes.Lookup(typeName)
		if !ok {
			return nil, nil
		}
		tok = t
	}
	return g.store.OutgoingRelationships(nodeID, tok)
}

// OutgoingRelationshipsForNodes returns outgoing relationships for multiple nodes
// in a single batched operation. Returns a map from nodeID to its outgoing rels
// (sorted by ID). Nodes with zero outgoing rels are absent from the map.
// If typeName is non-empty, only relationships of that type are returned.
// Unregistered typeName returns nil, nil. nil/empty nodeIDs returns nil, nil.
func (g *Graph) OutgoingRelationshipsForNodes(nodeIDs []types.NodeID, typeName string) (map[types.NodeID][]*types.Relationship, error) {
	if len(nodeIDs) == 0 {
		return nil, nil
	}
	var tok uint16
	if typeName != "" {
		t, ok := g.relTypes.Lookup(typeName)
		if !ok {
			return nil, nil
		}
		tok = t
	}
	return g.store.OutgoingRelationshipsForNodes(nodeIDs, tok)
}

// IncomingRelationshipsForNodes returns incoming relationships for multiple nodes
// in a single batched operation. Returns a map from nodeID to its incoming rels
// (sorted by ID). Nodes with zero incoming rels are absent from the map.
// If typeName is non-empty, only relationships of that type are returned.
// Unregistered typeName returns nil, nil. nil/empty nodeIDs returns nil, nil.
func (g *Graph) IncomingRelationshipsForNodes(nodeIDs []types.NodeID, typeName string) (map[types.NodeID][]*types.Relationship, error) {
	if len(nodeIDs) == 0 {
		return nil, nil
	}
	var tok uint16
	if typeName != "" {
		t, ok := g.relTypes.Lookup(typeName)
		if !ok {
			return nil, nil
		}
		tok = t
	}
	return g.store.IncomingRelationshipsForNodes(nodeIDs, tok)
}

// IncomingRelationships returns all incoming relationships to the given node.
// If typeName is empty, all types are returned. If typeName is non-empty, only
// relationships of that type are returned (nil if the type is not registered).
func (g *Graph) IncomingRelationships(nodeID types.NodeID, typeName string) ([]*types.Relationship, error) {
	var tok uint16
	if typeName != "" {
		t, ok := g.relTypes.Lookup(typeName)
		if !ok {
			return nil, nil
		}
		tok = t
	}
	return g.store.IncomingRelationships(nodeID, tok)
}

// NodeCount returns the number of nodes in the store.
func (g *Graph) NodeCount() (int, error) {
	return g.store.NodeCount()
}

// RelationshipCount returns the number of relationships in the store.
func (g *Graph) RelationshipCount() (int, error) {
	return g.store.RelationshipCount()
}

// AllNodes returns all nodes in the store, with optional pagination.
//
// History-aware when opts carries a temporal filter (ValidAt or
// ValidStart/ValidEnd): merges current and history IDs, resolves each via
// the version chain, and surfaces deleted entities that were valid at the
// query time. Without a temporal filter the fast store-side pushdown path
// is preserved.
func (g *Graph) AllNodes(opts QueryOpts) ([]*types.Node, error) {
	if !hasTemporalFilter(opts) {
		return g.store.AllNodes(opts)
	}
	if opts.Depth != DepthAll {
		return nil, ErrDepthTemporalUnsupported
	}
	var result []*types.Node
	err := g.forEachKnownNodeID(func(id types.NodeID) error {
		n, err := g.findNodeVersionForOpts(id, opts, nil)
		if err != nil {
			if errors.Is(err, ErrNoVersionValidAt) || errors.Is(err, ErrNodeNotFound) {
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
	sortNodesByID(result)
	return paginateNodes(result, opts.After, opts.Limit), nil
}

// AllRelationships returns all relationships in the store, with optional
// pagination.
//
// History-aware when opts carries a temporal filter (ValidAt or
// ValidStart/ValidEnd): merges current and history IDs, resolves each via
// the version chain, and surfaces deleted relationships that were valid at
// the query time. Without a temporal filter the fast store-side pushdown
// path is preserved.
func (g *Graph) AllRelationships(opts QueryOpts) ([]*types.Relationship, error) {
	if !hasTemporalFilter(opts) {
		return g.store.AllRelationships(opts)
	}
	if opts.Depth != DepthAll {
		return nil, ErrDepthTemporalUnsupported
	}
	var result []*types.Relationship
	err := g.forEachKnownRelID(func(id types.RelID) error {
		r, err := g.findRelVersionForOpts(id, opts, nil)
		if err != nil {
			if errors.Is(err, ErrNoVersionValidAt) || errors.Is(err, ErrRelNotFound) {
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
	sortRelsByID(result)
	return paginateRels(result, opts.After, opts.Limit), nil
}

// GetNodesByIDs returns nodes matching the given IDs. Missing IDs are skipped.
func (g *Graph) GetNodesByIDs(ids []types.NodeID) ([]*types.Node, error) {
	return g.store.GetNodesByIDs(ids)
}

// GetRelationshipsByIDs returns relationships matching the given IDs. Missing IDs are skipped.
func (g *Graph) GetRelationshipsByIDs(ids []types.RelID) ([]*types.Relationship, error) {
	return g.store.GetRelationshipsByIDs(ids)
}

// --- Per-label / per-type statistics ---

// NodeCountByLabel returns the number of nodes with the given label. O(1).
// Returns 0 if the label has never been registered.
func (g *Graph) NodeCountByLabel(label string) (int, error) {
	tok, ok := g.labels.Lookup(label)
	if !ok {
		return 0, nil
	}
	return g.store.NodeCountByLabel(tok)
}

// RelCountByType returns the number of relationships with the given type. O(1).
// Returns 0 if the type has never been registered.
func (g *Graph) RelCountByType(typeName string) (int, error) {
	tok, ok := g.relTypes.Lookup(typeName)
	if !ok {
		return 0, nil
	}
	return g.store.RelCountByType(tok)
}

// AllLabelCounts returns a map of label name to node count for all registered labels.
// Labels with zero nodes are omitted.
func (g *Graph) AllLabelCounts() (map[string]int, error) {
	names := g.labels.ExportNames()
	result := make(map[string]int)

	// Skip index 0 (reserved empty string).
	for i := 1; i < len(names); i++ {
		count, err := g.store.NodeCountByLabel(uint16(i))
		if err != nil {
			return nil, err
		}
		if count > 0 {
			result[names[i]] = count
		}
	}
	return result, nil
}

// AllRelTypeCounts returns a map of relationship type name to relationship count
// for all registered types. Types with zero relationships are omitted.
func (g *Graph) AllRelTypeCounts() (map[string]int, error) {
	names := g.relTypes.ExportNames()
	result := make(map[string]int)

	// Skip index 0 (reserved empty string).
	for i := 1; i < len(names); i++ {
		count, err := g.store.RelCountByType(uint16(i))
		if err != nil {
			return nil, err
		}
		if count > 0 {
			result[names[i]] = count
		}
	}
	return result, nil
}
