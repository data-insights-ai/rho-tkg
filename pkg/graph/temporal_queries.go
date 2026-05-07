package graph

import (
	"errors"

	indexpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/index"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// --- Public temporal query methods ---

// GetNodesValidAt returns all nodes valid at the given instant.
// History-aware: includes deleted nodes that were valid at time t by consulting
// version history in addition to current entities.
func (g *Graph) GetNodesValidAt(t types.Instant) ([]*types.Node, error) {
	var result []*types.Node
	err := g.forEachKnownNodeID(func(id types.NodeID) error {
		n, err := g.GetNodeAt(id, t)
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
	return result, nil
}

// GetRelationshipsValidAt returns all relationships valid at the given instant.
// History-aware: includes deleted relationships that were valid at time t.
func (g *Graph) GetRelationshipsValidAt(t types.Instant) ([]*types.Relationship, error) {
	var result []*types.Relationship
	err := g.forEachKnownRelID(func(id types.RelID) error {
		r, err := g.GetRelAt(id, t)
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
	return result, nil
}

// GetNodesByLabelValidAt returns all nodes with the given label that are valid
// at the given instant. Returns nil if the label is not registered.
// History-aware: includes historical versions whose label set matched at t.
func (g *Graph) GetNodesByLabelValidAt(label string, t types.Instant) ([]*types.Node, error) {
	tok, ok := g.labels.Lookup(label)
	if !ok {
		return nil, nil
	}
	// Indexed candidate set + history IDs — avoids a full ForEachNodeID scan.
	currentByLabel, err := g.store.NodesByLabel(tok, QueryOpts{})
	if err != nil {
		return nil, err
	}
	currentIDs := make([]types.NodeID, 0, len(currentByLabel))
	for _, n := range currentByLabel {
		currentIDs = append(currentIDs, n.InternalID())
	}
	var result []*types.Node
	if err := g.forEachNodeCandidateID(currentIDs, func(id types.NodeID) error {
		n, err := g.GetNodeAt(id, t)
		if err != nil {
			if errors.Is(err, ErrNoVersionValidAt) || errors.Is(err, ErrNodeNotFound) {
				return nil
			}
			return err
		}
		if n.HasLabelTokenRaw(tok) {
			result = append(result, n)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	sortNodesByID(result)
	return result, nil
}

// GetNodesValidDuring returns all nodes whose validity overlaps [start, end).
// History-aware: includes deleted or updated nodes that had any version valid
// during the interval.
//
// end == 0 is interpreted as "open-ended to now" (mirrors ValidTo == 0).
// The substitution happens once at this entry point so every per-ID
// overlap predicate sees the same upper bound — avoids time drift across
// the iteration that would otherwise let a single call return different
// results depending on how long it ran.
func (g *Graph) GetNodesValidDuring(start, end types.Instant) ([]*types.Node, error) {
	end = resolveOpenEndInstant(end)
	var result []*types.Node
	err := g.forEachKnownNodeID(func(id types.NodeID) error {
		n, err := g.getNodeVersionDuring(id, start, end)
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
	return result, nil
}

// GetRelationshipsValidDuring returns all relationships whose validity overlaps [start, end).
// History-aware: includes deleted or updated relationships.
//
// end == 0 is interpreted as "open-ended to now" — see GetNodesValidDuring.
func (g *Graph) GetRelationshipsValidDuring(start, end types.Instant) ([]*types.Relationship, error) {
	end = resolveOpenEndInstant(end)
	var result []*types.Relationship
	err := g.forEachKnownRelID(func(id types.RelID) error {
		r, err := g.getRelVersionDuring(id, start, end)
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
	return result, nil
}

// GetNodeAt returns the version of a node that was valid at the given instant.
// Builds the full version chain (history + current) and finds the version
// whose validity period contains t.
//
// Version validity periods are derived from UpdatedAt timestamps:
//   - Genesis version: starts at nodeValidFrom (snowflake ID time or explicit ValidFrom)
//   - Subsequent versions: start at their UpdatedAt timestamp
//   - Each version's end time is the next version's start time (or open-ended for current)
//
// Explicit ValidFrom/ValidTo on the entity override derived values.
//
// Handles deleted entities: if the current node is gone but history exists,
// version resolution proceeds using only the history chain. The last entry
// in a deleted entity's history has ValidTo set (tombstone).
//
// Returns ErrNodeNotFound if the node never existed (no current, no history).
// Returns ErrNoVersionValidAt if no version covers the given time.
func (g *Graph) GetNodeAt(id types.NodeID, t types.Instant) (*types.Node, error) {
	current, err := g.store.GetNode(id)
	if err != nil && !errors.Is(err, ErrNodeNotFound) {
		return nil, err
	}
	// current may be nil for deleted entities.

	history, err := g.store.GetNodeHistory(id)
	if err != nil {
		return nil, err
	}

	if current == nil && len(history) == 0 {
		return nil, ErrNodeNotFound
	}

	// Build chain: history (ascending version order) + current (if exists).
	chain := make([]*types.Node, 0, len(history)+1)
	chain = append(chain, history...)
	if current != nil {
		chain = append(chain, current)
	}

	return g.resolveNodeVersionAt(chain, t)
}

// GetRelAt returns the version of a relationship that was valid at the given instant.
// Mirrors GetNodeAt for relationships. Handles deleted entities by consulting
// version history when the current relationship is gone.
//
// Returns ErrRelNotFound if the relationship never existed (no current, no history).
// Returns ErrNoVersionValidAt if no version covers the given time.
func (g *Graph) GetRelAt(id types.RelID, t types.Instant) (*types.Relationship, error) {
	current, err := g.store.GetRelationship(id)
	if err != nil && !errors.Is(err, ErrRelNotFound) {
		return nil, err
	}
	// current may be nil for deleted entities.

	history, err := g.store.GetRelHistory(id)
	if err != nil {
		return nil, err
	}

	if current == nil && len(history) == 0 {
		return nil, ErrRelNotFound
	}

	// Build chain: history (ascending version order) + current (if exists).
	chain := make([]*types.Relationship, 0, len(history)+1)
	chain = append(chain, history...)
	if current != nil {
		chain = append(chain, current)
	}

	return g.resolveRelVersionAt(chain, t)
}

// GetNeighborsValidAt returns all neighbor nodes reachable from nodeID via
// relationships that are valid at the given instant, where the neighbor nodes
// themselves are also valid at that instant.
func (g *Graph) GetNeighborsValidAt(nodeID types.NodeID, t types.Instant) ([]*types.Node, error) {
	// Indexed candidate set: current outgoing + incoming adjacency, merged
	// with all history rel IDs (covering deleted-rel neighbors). Avoids a
	// full ForEachRelID scan.
	out, err := g.OutgoingRelationships(nodeID, "")
	if err != nil {
		return nil, err
	}
	in, err := g.IncomingRelationships(nodeID, "")
	if err != nil {
		return nil, err
	}
	currentRelIDs := make([]types.RelID, 0, len(out)+len(in))
	for _, r := range out {
		currentRelIDs = append(currentRelIDs, r.InternalID())
	}
	for _, r := range in {
		currentRelIDs = append(currentRelIDs, r.InternalID())
	}

	neighborIDs := make(map[types.NodeID]struct{})
	if err := g.forEachRelCandidateID(currentRelIDs, func(id types.RelID) error {
		r, err := g.GetRelAt(id, t)
		if err != nil {
			if errors.Is(err, ErrNoVersionValidAt) || errors.Is(err, ErrRelNotFound) {
				return nil
			}
			return err
		}
		if r.StartNodeID() == nodeID {
			neighborIDs[r.EndNodeID()] = struct{}{}
		} else if r.EndNodeID() == nodeID {
			neighborIDs[r.StartNodeID()] = struct{}{}
		}
		return nil
	}); err != nil {
		return nil, err
	}

	if len(neighborIDs) == 0 {
		return nil, nil
	}

	var result []*types.Node
	for id := range neighborIDs {
		n, err := g.GetNodeAt(id, t)
		if err != nil {
			if errors.Is(err, ErrNoVersionValidAt) || errors.Is(err, ErrNodeNotFound) {
				continue
			}
			return nil, err
		}
		result = append(result, n)
	}
	sortNodesByID(result)
	return result, nil
}

// --- Combined label + property + temporal queries ---

// NodesByLabelPropertyAndTime returns nodes matching the label and property value
// that are valid at the given instant. History-aware.
// Returns nil if the label is not registered.
func (g *Graph) NodesByLabelPropertyAndTime(label, key string, value any, t types.Instant) ([]*types.Node, error) {
	tok, ok := g.labels.Lookup(label)
	if !ok {
		return nil, nil
	}
	targetKey := indexpkg.PropertyValueKey(value)
	if targetKey == "" {
		return nil, nil
	}
	// Seed candidates from the property index (falls back to a label scan
	// inside the store when no property index covers (label, key)).
	currentMatching, err := g.store.NodesByLabelAndProperty(tok, key, value, QueryOpts{})
	if err != nil {
		return nil, err
	}
	currentIDs := make([]types.NodeID, 0, len(currentMatching))
	for _, n := range currentMatching {
		currentIDs = append(currentIDs, n.InternalID())
	}
	var result []*types.Node
	if err := g.forEachNodeCandidateID(currentIDs, func(id types.NodeID) error {
		n, err := g.GetNodeAt(id, t)
		if err != nil {
			if errors.Is(err, ErrNoVersionValidAt) || errors.Is(err, ErrNodeNotFound) {
				return nil
			}
			return err
		}
		if !n.HasLabelTokenRaw(tok) {
			return nil
		}
		if v, found := n.GetProperty(key); found && indexpkg.PropertyValueKey(v) == targetKey {
			result = append(result, n)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	sortNodesByID(result)
	return result, nil
}

// NodesByLabelPropertyDuring returns nodes matching the label and property value
// whose validity overlaps [start, end). History-aware: returns the node if any
// overlapping version had the label and property, even when a later version
// no longer matches.
// Returns nil if the label is not registered.
func (g *Graph) NodesByLabelPropertyDuring(label, key string, value any, start, end types.Instant) ([]*types.Node, error) {
	tok, ok := g.labels.Lookup(label)
	if !ok {
		return nil, nil
	}
	targetKey := indexpkg.PropertyValueKey(value)
	if targetKey == "" {
		return nil, nil
	}
	// Resolve open-ended end once for the whole iteration — see
	// GetNodesValidDuring.
	end = resolveOpenEndInstant(end)
	// Seed candidates from the property index (falls back to a label scan
	// inside the store when no property index covers (label, key)).
	currentMatching, err := g.store.NodesByLabelAndProperty(tok, key, value, QueryOpts{})
	if err != nil {
		return nil, err
	}
	currentIDs := make([]types.NodeID, 0, len(currentMatching))
	for _, n := range currentMatching {
		currentIDs = append(currentIDs, n.InternalID())
	}
	pred := func(n *types.Node) bool {
		if !n.HasLabelTokenRaw(tok) {
			return false
		}
		v, found := n.GetProperty(key)
		return found && indexpkg.PropertyValueKey(v) == targetKey
	}
	var result []*types.Node
	if err := g.forEachNodeCandidateID(currentIDs, func(id types.NodeID) error {
		n, err := g.findNodeVersionMatchingDuring(id, start, end, pred)
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
	return result, nil
}
