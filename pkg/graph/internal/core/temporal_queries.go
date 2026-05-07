package core

import (
	"errors"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"

	indexpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/index"
	storeutil "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/storeutil"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// --- Public temporal query methods ---

// GetNodesValidAt returns all nodes valid at the given instant.
// History-aware: includes deleted nodes that were valid at time t by consulting
// version history in addition to current entities.
func (c *Core) GetNodesValidAt(t types.Instant) ([]*types.Node, error) {
	var result []*types.Node
	err := c.forEachKnownNodeID(func(id types.NodeID) error {
		n, err := c.GetNodeAt(id, t)
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
	return result, nil
}

// GetRelationshipsValidAt returns all relationships valid at the given instant.
// History-aware: includes deleted relationships that were valid at time t.
func (c *Core) GetRelationshipsValidAt(t types.Instant) ([]*types.Relationship, error) {
	var result []*types.Relationship
	err := c.forEachKnownRelID(func(id types.RelID) error {
		r, err := c.GetRelAt(id, t)
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
	return result, nil
}

// GetNodesByLabelValidAt returns all nodes with the given label that are valid
// at the given instant. Returns nil if the label is not registered.
// History-aware: includes historical versions whose label set matched at t.
func (c *Core) GetNodesByLabelValidAt(label string, t types.Instant) ([]*types.Node, error) {
	tok, ok := c.labels.Lookup(label)
	if !ok {
		return nil, nil
	}
	// Indexed candidate set + history IDs — avoids a full ForEachNodeID scan.
	currentByLabel, err := c.store.NodesByLabel(tok, storepkg.QueryOpts{})
	if err != nil {
		return nil, err
	}
	currentIDs := make([]types.NodeID, 0, len(currentByLabel))
	for _, n := range currentByLabel {
		currentIDs = append(currentIDs, n.InternalID())
	}
	var result []*types.Node
	if err := c.forEachNodeCandidateID(currentIDs, func(id types.NodeID) error {
		n, err := c.GetNodeAt(id, t)
		if err != nil {
			if errors.Is(err, storepkg.ErrNoVersionValidAt) || errors.Is(err, storepkg.ErrNodeNotFound) {
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
	storeutil.SortNodesByID(result)
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
func (c *Core) GetNodesValidDuring(start, end types.Instant) ([]*types.Node, error) {
	end = resolveOpenEndInstant(end)
	var result []*types.Node
	err := c.forEachKnownNodeID(func(id types.NodeID) error {
		n, err := c.getNodeVersionDuring(id, start, end)
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
	return result, nil
}

// GetRelationshipsValidDuring returns all relationships whose validity overlaps [start, end).
// History-aware: includes deleted or updated relationships.
//
// end == 0 is interpreted as "open-ended to now" — see GetNodesValidDuring.
func (c *Core) GetRelationshipsValidDuring(start, end types.Instant) ([]*types.Relationship, error) {
	end = resolveOpenEndInstant(end)
	var result []*types.Relationship
	err := c.forEachKnownRelID(func(id types.RelID) error {
		r, err := c.getRelVersionDuring(id, start, end)
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
// Returns storepkg.ErrNodeNotFound if the node never existed (no current, no history).
// Returns storepkg.ErrNoVersionValidAt if no version covers the given time.
func (c *Core) GetNodeAt(id types.NodeID, t types.Instant) (*types.Node, error) {
	current, err := c.store.GetNode(id)
	if err != nil && !errors.Is(err, storepkg.ErrNodeNotFound) {
		return nil, err
	}
	// current may be nil for deleted entities.

	history, err := c.store.GetNodeHistory(id)
	if err != nil {
		return nil, err
	}

	if current == nil && len(history) == 0 {
		return nil, storepkg.ErrNodeNotFound
	}

	// Build chain: history (ascending version order) + current (if exists).
	chain := make([]*types.Node, 0, len(history)+1)
	chain = append(chain, history...)
	if current != nil {
		chain = append(chain, current)
	}

	return c.resolveNodeVersionAt(chain, t)
}

// GetRelAt returns the version of a relationship that was valid at the given instant.
// Mirrors GetNodeAt for relationships. Handles deleted entities by consulting
// version history when the current relationship is gone.
//
// Returns storepkg.ErrRelNotFound if the relationship never existed (no current, no history).
// Returns storepkg.ErrNoVersionValidAt if no version covers the given time.
func (c *Core) GetRelAt(id types.RelID, t types.Instant) (*types.Relationship, error) {
	current, err := c.store.GetRelationship(id)
	if err != nil && !errors.Is(err, storepkg.ErrRelNotFound) {
		return nil, err
	}
	// current may be nil for deleted entities.

	history, err := c.store.GetRelHistory(id)
	if err != nil {
		return nil, err
	}

	if current == nil && len(history) == 0 {
		return nil, storepkg.ErrRelNotFound
	}

	// Build chain: history (ascending version order) + current (if exists).
	chain := make([]*types.Relationship, 0, len(history)+1)
	chain = append(chain, history...)
	if current != nil {
		chain = append(chain, current)
	}

	return c.resolveRelVersionAt(chain, t)
}

// GetNeighborsValidAt returns all neighbor nodes reachable from nodeID via
// relationships that are valid at the given instant, where the neighbor nodes
// themselves are also valid at that instant.
func (c *Core) GetNeighborsValidAt(nodeID types.NodeID, t types.Instant) ([]*types.Node, error) {
	// Indexed candidate set: current outgoing + incoming adjacency, merged
	// with all history rel IDs (covering deleted-rel neighbors). Avoids a
	// full ForEachRelID scan.
	out, err := c.OutgoingRelationships(nodeID, "")
	if err != nil {
		return nil, err
	}
	in, err := c.IncomingRelationships(nodeID, "")
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
	if err := c.forEachRelCandidateID(currentRelIDs, func(id types.RelID) error {
		r, err := c.GetRelAt(id, t)
		if err != nil {
			if errors.Is(err, storepkg.ErrNoVersionValidAt) || errors.Is(err, storepkg.ErrRelNotFound) {
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
		n, err := c.GetNodeAt(id, t)
		if err != nil {
			if errors.Is(err, storepkg.ErrNoVersionValidAt) || errors.Is(err, storepkg.ErrNodeNotFound) {
				continue
			}
			return nil, err
		}
		result = append(result, n)
	}
	storeutil.SortNodesByID(result)
	return result, nil
}

// --- Combined label + property + temporal queries ---

// NodesByLabelPropertyAndTime returns nodes matching the label and property value
// that are valid at the given instant. History-aware.
// Returns nil if the label is not registered.
func (c *Core) NodesByLabelPropertyAndTime(label, key string, value any, t types.Instant) ([]*types.Node, error) {
	tok, ok := c.labels.Lookup(label)
	if !ok {
		return nil, nil
	}
	targetKey := indexpkg.PropertyValueKey(value)
	if targetKey == "" {
		return nil, nil
	}
	// Seed candidates from the property index (falls back to a label scan
	// inside the store when no property index covers (label, key)).
	currentMatching, err := c.store.NodesByLabelAndProperty(tok, key, value, storepkg.QueryOpts{})
	if err != nil {
		return nil, err
	}
	currentIDs := make([]types.NodeID, 0, len(currentMatching))
	for _, n := range currentMatching {
		currentIDs = append(currentIDs, n.InternalID())
	}
	var result []*types.Node
	if err := c.forEachNodeCandidateID(currentIDs, func(id types.NodeID) error {
		n, err := c.GetNodeAt(id, t)
		if err != nil {
			if errors.Is(err, storepkg.ErrNoVersionValidAt) || errors.Is(err, storepkg.ErrNodeNotFound) {
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
	storeutil.SortNodesByID(result)
	return result, nil
}

// NodesByLabelPropertyDuring returns nodes matching the label and property value
// whose validity overlaps [start, end). History-aware: returns the node if any
// overlapping version had the label and property, even when a later version
// no longer matches.
// Returns nil if the label is not registered.
func (c *Core) NodesByLabelPropertyDuring(label, key string, value any, start, end types.Instant) ([]*types.Node, error) {
	tok, ok := c.labels.Lookup(label)
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
	currentMatching, err := c.store.NodesByLabelAndProperty(tok, key, value, storepkg.QueryOpts{})
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
	if err := c.forEachNodeCandidateID(currentIDs, func(id types.NodeID) error {
		n, err := c.findNodeVersionMatchingDuring(id, start, end, pred)
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
	return result, nil
}

// --- Relationship-side parity (mirrors of the Node methods above) ---

// GetRelationshipsByTypeValidAt returns all relationships of the given type
// that are valid at the given instant. Returns nil if the type is not
// registered.
//
// Thin wrapper over RelationshipsByType(typeName, storepkg.QueryOpts{ValidAt: t}) —
// the named convenience mirror of GetNodesByLabelValidAt. History-aware via
// the generic-opts path: includes deleted relationships whose type matched
// at t.
func (c *Core) GetRelationshipsByTypeValidAt(relType string, t types.Instant) ([]*types.Relationship, error) {
	return c.RelationshipsByType(relType, storepkg.QueryOpts{ValidAt: t})
}

// RelationshipsByTypePropertyAndTime returns relationships matching the type
// and property value that are valid at the given instant. History-aware.
// Returns nil if the type is not registered.
//
// Mirrors NodesByLabelPropertyAndTime. There is no store-level
// type+property index for relationships, so candidates are seeded from the
// type index only and the property predicate is evaluated on each historical
// version.
func (c *Core) RelationshipsByTypePropertyAndTime(relType, key string, value any, t types.Instant) ([]*types.Relationship, error) {
	tok, ok := c.relTypes.Lookup(relType)
	if !ok {
		return nil, nil
	}
	targetKey := indexpkg.PropertyValueKey(value)
	if targetKey == "" {
		return nil, nil
	}
	// Seed candidates from the type index. No (type, property) index exists
	// at the store level for relationships, so the property predicate is
	// evaluated per-version below.
	current, err := c.store.RelationshipsByType(tok, storepkg.QueryOpts{})
	if err != nil {
		return nil, err
	}
	currentIDs := make([]types.RelID, 0, len(current))
	for _, r := range current {
		currentIDs = append(currentIDs, r.InternalID())
	}
	var result []*types.Relationship
	if err := c.forEachRelCandidateID(currentIDs, func(id types.RelID) error {
		r, err := c.GetRelAt(id, t)
		if err != nil {
			if errors.Is(err, storepkg.ErrNoVersionValidAt) || errors.Is(err, storepkg.ErrRelNotFound) {
				return nil
			}
			return err
		}
		if !r.HasTypeTokenRaw(tok) {
			return nil
		}
		if v, found := r.GetProperty(key); found && indexpkg.PropertyValueKey(v) == targetKey {
			result = append(result, r)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	storeutil.SortRelsByID(result)
	return result, nil
}

// RelationshipsByTypePropertyDuring returns relationships matching the type
// and property value whose validity overlaps [start, end). History-aware:
// returns the relationship if any overlapping version had the type and
// property, even when a later version no longer matches.
// Returns nil if the type is not registered.
//
// Mirrors NodesByLabelPropertyDuring. Like the *AndTime variant, candidates
// are seeded from the type index and the property predicate is evaluated
// per-version via findRelVersionMatchingDuring — so a relationship whose
// property held during part of [start, end) but not on the most-recent
// overlapping version is still included.
func (c *Core) RelationshipsByTypePropertyDuring(relType, key string, value any, start, end types.Instant) ([]*types.Relationship, error) {
	tok, ok := c.relTypes.Lookup(relType)
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
	// Seed candidates from the type index. No (type, property) index exists
	// at the store level for relationships.
	current, err := c.store.RelationshipsByType(tok, storepkg.QueryOpts{})
	if err != nil {
		return nil, err
	}
	currentIDs := make([]types.RelID, 0, len(current))
	for _, r := range current {
		currentIDs = append(currentIDs, r.InternalID())
	}
	pred := func(r *types.Relationship) bool {
		if !r.HasTypeTokenRaw(tok) {
			return false
		}
		v, found := r.GetProperty(key)
		return found && indexpkg.PropertyValueKey(v) == targetKey
	}
	var result []*types.Relationship
	if err := c.forEachRelCandidateID(currentIDs, func(id types.RelID) error {
		r, err := c.findRelVersionMatchingDuring(id, start, end, pred)
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
	return result, nil
}
