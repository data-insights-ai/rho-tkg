package core

import (
	"errors"
	"fmt"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"

	indexpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/index"
	storeutil "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/storeutil"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// --- Public temporal query methods ---

// NodesAt returns all nodes valid at the given instant.
// History-aware: includes deleted nodes that were valid at time t by consulting
// version history in addition to current entities.
func (t *TempOps) NodesAt(at types.Instant) ([]*types.Node, error) {
	c := t.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	var result []*types.Node
	err := c.readUnderRLock(func() error {
		nodes, err := c.nodesAtLocked(at)
		result = nodes
		return err
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (c *Core) nodesAtLocked(at types.Instant) ([]*types.Node, error) {
	var result []*types.Node
	err := c.forEachKnownNodeID(func(id types.NodeID) error {
		n, err := c.nodeAtLocked(id, at)
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

// RelsAt returns all relationships valid at the given instant.
// History-aware: includes deleted relationships that were valid at time t.
func (t *TempOps) RelsAt(at types.Instant) ([]*types.Relationship, error) {
	c := t.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	var result []*types.Relationship
	err := c.readUnderRLock(func() error {
		rels, err := c.relationshipsAtLocked(at)
		result = rels
		return err
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (c *Core) relationshipsAtLocked(at types.Instant) ([]*types.Relationship, error) {
	var result []*types.Relationship
	err := c.forEachKnownRelID(func(id types.RelID) error {
		r, err := c.relAtLocked(id, at)
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

// NodesByLabelAt returns all nodes with the given label that are valid
// at the given instant. Returns nil if the label is not registered.
// History-aware: includes historical versions whose label set matched at t.
func (t *TempOps) NodesByLabelAt(label string, at types.Instant) ([]*types.Node, error) {
	c := t.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if err := c.validateIndexLabel(label); err != nil {
		return nil, err
	}
	var result []*types.Node
	err := c.readUnderRLock(func() error {
		tok, ok := c.labels.Lookup(label)
		if !ok {
			return nil
		}
		// Indexed candidate set + history IDs — avoids a full ForEachNodeID scan.
		currentByLabel, err := c.store.NodesByLabel(tok, storepkg.QueryOpts{})
		if err != nil {
			return err
		}
		currentIDs, err := c.nodeIDsFromLabelRows(tok, currentByLabel)
		if err != nil {
			return err
		}
		if err := c.forEachNodeCandidateID(currentIDs, func(id types.NodeID) error {
			n, err := c.nodeAtLocked(id, at)
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
			return err
		}
		storeutil.SortNodesByID(result)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// NodesDuring returns all nodes whose validity overlaps [start, end).
// History-aware: includes deleted or updated nodes that had any version valid
// during the interval.
//
// end == 0 is interpreted as "open-ended to now" (mirrors ValidTo == 0).
// The substitution happens once at this entry point so every per-ID
// overlap predicate sees the same upper bound — avoids time drift across
// the iteration that would otherwise let a single call return different
// results depending on how long it ran.
func (t *TempOps) NodesDuring(start, end types.Instant) ([]*types.Node, error) {
	c := t.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	resolvedEnd, err := normalizeDuringRange(start, end)
	if err != nil {
		return nil, err
	}
	end = resolvedEnd
	var result []*types.Node
	err = c.readUnderRLock(func() error {
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
			return err
		}
		storeutil.SortNodesByID(result)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// RelsDuring returns all relationships whose validity overlaps [start, end).
// History-aware: includes deleted or updated relationships.
//
// end == 0 is interpreted as "open-ended to now" — see GetNodesValidDuring.
func (t *TempOps) RelsDuring(start, end types.Instant) ([]*types.Relationship, error) {
	c := t.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	resolvedEnd, err := normalizeDuringRange(start, end)
	if err != nil {
		return nil, err
	}
	end = resolvedEnd
	var result []*types.Relationship
	err = c.readUnderRLock(func() error {
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
			return err
		}
		storeutil.SortRelsByID(result)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// NodeAt returns the version of a node that was valid at the given instant.
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
// NodeAt storepkg.ErrNodeNotFound if the node never existed (no current, no history).
// Returns storepkg.ErrNoVersionValidAt if no version covers the given time.
func (t *TempOps) NodeAt(id types.NodeID, at types.Instant) (*types.Node, error) {
	c := t.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if err := storepkg.ValidateNodeID(id); err != nil {
		return nil, err
	}
	var result *types.Node
	err := c.readUnderRLock(func() error {
		n, err := c.nodeAtLocked(id, at)
		result = n
		return err
	})
	return result, err
}

func (c *Core) nodeAtLocked(id types.NodeID, at types.Instant) (*types.Node, error) {
	if err := storepkg.ValidateNodeID(id); err != nil {
		return nil, err
	}
	current, err := c.getCurrentNode(id)
	if err != nil && !errors.Is(err, storepkg.ErrNodeNotFound) {
		return nil, err
	}
	// current may be nil for deleted entities.

	history, err := c.getNodeHistory(id)
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

	return c.resolveNodeVersionAt(chain, at)
}

// RelAt returns the version of a relationship that was valid at the given instant.
// Mirrors GetNodeAt for relationships. Handles deleted entities by consulting
// version history when the current relationship is gone.
//
// RelAt storepkg.ErrRelNotFound if the relationship never existed (no current, no history).
// Returns storepkg.ErrNoVersionValidAt if no version covers the given time.
func (t *TempOps) RelAt(id types.RelID, at types.Instant) (*types.Relationship, error) {
	c := t.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if err := storepkg.ValidateRelID(id); err != nil {
		return nil, err
	}
	var result *types.Relationship
	err := c.readUnderRLock(func() error {
		r, err := c.relAtLocked(id, at)
		result = r
		return err
	})
	return result, err
}

func (c *Core) relAtLocked(id types.RelID, at types.Instant) (*types.Relationship, error) {
	if err := storepkg.ValidateRelID(id); err != nil {
		return nil, err
	}
	current, err := c.getCurrentRelationship(id)
	if err != nil && !errors.Is(err, storepkg.ErrRelNotFound) {
		return nil, err
	}
	// current may be nil for deleted entities.

	history, err := c.getRelHistory(id)
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

	return c.resolveRelVersionAt(chain, at)
}

// NeighborsAt returns all neighbor nodes reachable from nodeID via
// relationships that are valid at the given instant, where the neighbor nodes
// themselves are also valid at that instant.
func (t *TempOps) NeighborsAt(nodeID types.NodeID, at types.Instant) ([]*types.Node, error) {
	c := t.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if err := storepkg.ValidateNodeID(nodeID); err != nil {
		return nil, err
	}
	var result []*types.Node
	err := c.readUnderRLock(func() error {
		if _, err := c.nodeAtLocked(nodeID, at); err != nil {
			return err
		}

		// Indexed candidate set: current outgoing + incoming adjacency, merged
		// with all history rel IDs (covering deleted-rel neighbors). Avoids a
		// full ForEachRelID scan.
		out, err := c.store.OutgoingRelationships(nodeID, 0)
		if err != nil {
			if !errors.Is(err, storepkg.ErrNodeNotFound) {
				return err
			}
			out = nil
		}
		in, err := c.store.IncomingRelationships(nodeID, 0)
		if err != nil {
			if !errors.Is(err, storepkg.ErrNodeNotFound) {
				return err
			}
			in = nil
		}
		outIDs, err := c.outgoingRelIDsFromRows(nodeID, 0, out)
		if err != nil {
			return err
		}
		inIDs, err := c.incomingRelIDsFromRows(nodeID, 0, in)
		if err != nil {
			return err
		}
		currentRelIDs := make([]types.RelID, 0, len(outIDs)+len(inIDs))
		currentRelIDs = append(currentRelIDs, outIDs...)
		currentRelIDs = append(currentRelIDs, inIDs...)

		neighborIDs := make(map[types.NodeID]struct{})
		if err := c.forEachRelCandidateID(currentRelIDs, func(id types.RelID) error {
			r, err := c.relAtLocked(id, at)
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
			return err
		}

		if len(neighborIDs) == 0 {
			return nil
		}

		for id := range neighborIDs {
			n, err := c.nodeAtLocked(id, at)
			if err != nil {
				if errors.Is(err, storepkg.ErrNoVersionValidAt) || errors.Is(err, storepkg.ErrNodeNotFound) {
					continue
				}
				return err
			}
			result = append(result, n)
		}
		storeutil.SortNodesByID(result)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// --- Combined label + property + temporal queries ---

// NodesByLabelPropertyAt returns nodes matching the label and property value
// that are valid at the given instant. History-aware.
// Returns nil if the label is not registered.
func (t *TempOps) NodesByLabelPropertyAt(label, key string, value any, at types.Instant) ([]*types.Node, error) {
	c := t.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if err := c.validateIndexLabel(label); err != nil {
		return nil, err
	}
	if err := c.validateIndexPropertyKey(key); err != nil {
		return nil, err
	}
	if err := types.ValidatePropertyValue(value); err != nil {
		return nil, fmt.Errorf("graph: nodes by label property at value: %w", err)
	}
	targetKey := indexpkg.PropertyValueKey(value)
	var result []*types.Node
	err := c.readUnderRLock(func() error {
		tok, ok := c.labels.Lookup(label)
		if !ok {
			return nil
		}
		if targetKey == "" {
			return nil
		}
		// Seed candidates from the property index when present, label scan
		// + property filter otherwise. Both produce a correct candidate
		// set; the index path is just faster.
		currentMatching, err := c.nodesByLabelAndProperty(tok, key, value, storepkg.QueryOpts{})
		if err != nil {
			return err
		}
		currentIDs, err := c.nodeIDsFromLabelRows(tok, currentMatching)
		if err != nil {
			return err
		}
		if err := c.forEachNodeCandidateID(currentIDs, func(id types.NodeID) error {
			n, err := c.nodeAtLocked(id, at)
			if err != nil {
				if errors.Is(err, storepkg.ErrNoVersionValidAt) || errors.Is(err, storepkg.ErrNodeNotFound) {
					return nil
				}
				return err
			}
			if !n.HasLabelTokenRaw(tok) {
				return nil
			}
			if valueKey, found := n.IndexablePropertyValueKey(key); found && valueKey == targetKey {
				result = append(result, n)
			}
			return nil
		}); err != nil {
			return err
		}
		storeutil.SortNodesByID(result)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// NodesByLabelPropertyDuring returns nodes matching the label and property value
// whose validity overlaps [start, end). History-aware: returns the node if any
// overlapping version had the label and property, even when a later version
// no longer matches.
// Returns nil if the label is not registered.
func (t *TempOps) NodesByLabelPropertyDuring(label, key string, value any, start, end types.Instant) ([]*types.Node, error) {
	c := t.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	resolvedEnd, err := normalizeDuringRange(start, end)
	if err != nil {
		return nil, err
	}
	end = resolvedEnd
	if err := c.validateIndexLabel(label); err != nil {
		return nil, err
	}
	if err := c.validateIndexPropertyKey(key); err != nil {
		return nil, err
	}
	if err := types.ValidatePropertyValue(value); err != nil {
		return nil, fmt.Errorf("graph: nodes by label property during value: %w", err)
	}
	targetKey := indexpkg.PropertyValueKey(value)
	var result []*types.Node
	err = c.readUnderRLock(func() error {
		tok, ok := c.labels.Lookup(label)
		if !ok {
			return nil
		}
		if targetKey == "" {
			return nil
		}
		// Seed candidates from the property index when present, label scan
		// + property filter otherwise.
		currentMatching, err := c.nodesByLabelAndProperty(tok, key, value, storepkg.QueryOpts{})
		if err != nil {
			return err
		}
		currentIDs, err := c.nodeIDsFromLabelRows(tok, currentMatching)
		if err != nil {
			return err
		}
		pred := func(n *types.Node) bool {
			if !n.HasLabelTokenRaw(tok) {
				return false
			}
			gotKey, found := n.IndexablePropertyValueKey(key)
			return found && gotKey == targetKey
		}
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
			return err
		}
		storeutil.SortNodesByID(result)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// --- Relationship-side parity (mirrors of the Node methods above) ---

// RelsByTypeAt returns all relationships of the given type
// that are valid at the given instant. Returns nil if the type is not
// registered.
//
// RelsByTypeAt wrapper over RelationshipsByType(typeName, storepkg.QueryOpts{ValidAt: t}) —
// the named convenience mirror of GetNodesByLabelValidAt. History-aware via
// the generic-opts path: includes deleted relationships whose type matched
// at t.
func (t *TempOps) RelsByTypeAt(relType string, at types.Instant) ([]*types.Relationship, error) {
	c := t.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	return c.Rels.ByType(relType, storepkg.QueryOpts{ValidAt: at})
}

// RelsByTypePropertyAt returns relationships matching the type
// and property value that are valid at the given instant. History-aware.
// Returns nil if the type is not registered.
//
// RelsByTypePropertyAt NodesByLabelPropertyAndTime. There is no store-level
// type+property index for relationships, so candidates are seeded from the
// type index only and the property predicate is evaluated on each historical
// version.
func (t *TempOps) RelsByTypePropertyAt(relType, key string, value any, at types.Instant) ([]*types.Relationship, error) {
	c := t.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if err := c.validateRelTypeQueryName(relType); err != nil {
		return nil, err
	}
	if err := c.validateIndexPropertyKey(key); err != nil {
		return nil, err
	}
	if err := types.ValidatePropertyValue(value); err != nil {
		return nil, fmt.Errorf("graph: relationships by type property at value: %w", err)
	}
	targetKey := indexpkg.PropertyValueKey(value)
	var result []*types.Relationship
	err := c.readUnderRLock(func() error {
		tok, ok := c.lookupRelTypeQueryToken(relType)
		if !ok {
			return nil
		}
		if targetKey == "" {
			return nil
		}
		// Seed candidates from the type index. No (type, property) index exists
		// at the store level for relationships, so the property predicate is
		// evaluated per-version below.
		current, err := c.store.RelationshipsByType(tok, storepkg.QueryOpts{})
		if err != nil {
			return err
		}
		currentIDs, err := c.relIDsFromTypeRows(tok, current)
		if err != nil {
			return err
		}
		if err := c.forEachRelCandidateID(currentIDs, func(id types.RelID) error {
			r, err := c.relAtLocked(id, at)
			if err != nil {
				if errors.Is(err, storepkg.ErrNoVersionValidAt) || errors.Is(err, storepkg.ErrRelNotFound) {
					return nil
				}
				return err
			}
			if !r.HasTypeTokenRaw(tok) {
				return nil
			}
			if valueKey, found := r.IndexablePropertyValueKey(key); found && valueKey == targetKey {
				result = append(result, r)
			}
			return nil
		}); err != nil {
			return err
		}
		storeutil.SortRelsByID(result)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// RelsByTypePropertyDuring returns relationships matching the type
// and property value whose validity overlaps [start, end). History-aware:
// returns the relationship if any overlapping version had the type and
// property, even when a later version no longer matches.
// Returns nil if the type is not registered.
//
// RelsByTypePropertyDuring NodesByLabelPropertyDuring. Like the *AndTime variant, candidates
// are seeded from the type index and the property predicate is evaluated
// per-version via findRelVersionMatchingDuring — so a relationship whose
// property held during part of [start, end) but not on the most-recent
// overlapping version is still included.
func (t *TempOps) RelsByTypePropertyDuring(relType, key string, value any, start, end types.Instant) ([]*types.Relationship, error) {
	c := t.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	resolvedEnd, err := normalizeDuringRange(start, end)
	if err != nil {
		return nil, err
	}
	end = resolvedEnd
	if err := c.validateRelTypeQueryName(relType); err != nil {
		return nil, err
	}
	if err := c.validateIndexPropertyKey(key); err != nil {
		return nil, err
	}
	if err := types.ValidatePropertyValue(value); err != nil {
		return nil, fmt.Errorf("graph: relationships by type property during value: %w", err)
	}
	targetKey := indexpkg.PropertyValueKey(value)
	var result []*types.Relationship
	err = c.readUnderRLock(func() error {
		tok, ok := c.lookupRelTypeQueryToken(relType)
		if !ok {
			return nil
		}
		if targetKey == "" {
			return nil
		}
		// Seed candidates from the type index. No (type, property) index exists
		// at the store level for relationships.
		current, err := c.store.RelationshipsByType(tok, storepkg.QueryOpts{})
		if err != nil {
			return err
		}
		currentIDs, err := c.relIDsFromTypeRows(tok, current)
		if err != nil {
			return err
		}
		pred := func(r *types.Relationship) bool {
			if !r.HasTypeTokenRaw(tok) {
				return false
			}
			gotKey, found := r.IndexablePropertyValueKey(key)
			return found && gotKey == targetKey
		}
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
			return err
		}
		storeutil.SortRelsByID(result)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}
