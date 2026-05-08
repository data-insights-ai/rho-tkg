package core

import (
	"errors"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// ErrNoVersionAsOf is returned when no entity version was recorded at the given
// transaction time.
var ErrNoVersionAsOf = errors.New("graph: no entity version recorded at the given transaction time")

// NodeAsOf returns the node version that was current at the given transaction time.
//
// Algorithm:
//  1. Try current: if TxFrom > 0 && TxFrom <= txTime && TxTo == 0 → return it.
//  2. Scan Nodes.History(id): find version where TxFrom > 0 && TxFrom <= txTime
//     && (TxTo == 0 || TxTo > txTime) → return latest matching.
//  3. None found → ErrNoVersionAsOf.
func (t *TempOps) NodeAsOf(id types.NodeID, txTime types.Instant) (*types.Node, error) {
	c := t.c
	// Try current node first.
	current, err := c.store.GetNode(id)
	if err != nil && !errors.Is(err, storepkg.ErrNodeNotFound) {
		return nil, err
	}
	if current != nil {
		if tm := current.Temporal(); tm != nil && tm.TxFrom > 0 && tm.TxFrom <= txTime && tm.TxTo == 0 {
			return current, nil
		}
	}

	// Scan history.
	hist, err := c.store.GetNodeHistory(id)
	if err != nil {
		return nil, err
	}

	var best *types.Node
	for _, v := range hist {
		tm := v.Temporal()
		if tm == nil || tm.TxFrom == 0 {
			continue
		}
		if tm.TxFrom <= txTime && (tm.TxTo == 0 || tm.TxTo > txTime) {
			// Take the latest qualifying version (highest TxFrom among candidates).
			if best == nil || tm.TxFrom > best.Temporal().TxFrom {
				best = v
			}
		}
	}
	if best != nil {
		return best, nil
	}
	return nil, ErrNoVersionAsOf
}

// RelAsOf returns the relationship version that was current at the given
// transaction time. Mirrors GetNodeAsOf for relationships.
func (t *TempOps) RelAsOf(id types.RelID, txTime types.Instant) (*types.Relationship, error) {
	c := t.c
	current, err := c.store.GetRelationship(id)
	if err != nil && !errors.Is(err, storepkg.ErrRelNotFound) {
		return nil, err
	}
	if current != nil {
		if tm := current.Temporal(); tm != nil && tm.TxFrom > 0 && tm.TxFrom <= txTime && tm.TxTo == 0 {
			return current, nil
		}
	}

	hist, err := c.store.GetRelHistory(id)
	if err != nil {
		return nil, err
	}

	var best *types.Relationship
	for _, v := range hist {
		tm := v.Temporal()
		if tm == nil || tm.TxFrom == 0 {
			continue
		}
		if tm.TxFrom <= txTime && (tm.TxTo == 0 || tm.TxTo > txTime) {
			if best == nil || tm.TxFrom > best.Temporal().TxFrom {
				best = v
			}
		}
	}
	if best != nil {
		return best, nil
	}
	return nil, ErrNoVersionAsOf
}

// NodesAsOf returns all nodes that existed at the given transaction time.
// Collects all known node IDs (current + history) using ForEach iterators,
// calls GetNodeAsOf per ID, skips ErrNoVersionAsOf.
// Returns nil, nil if no nodes existed at txTime.
func (t *TempOps) NodesAsOf(txTime types.Instant) ([]*types.Node, error) {
	c := t.c
	// Two-phase: collect IDs under store locks, then process after lock release.
	// Callback must NOT call store methods (B15 — lock reentrancy deadlock).
	seen := make(map[snowflake.ID]struct{})
	if err := c.store.ForEachNodeID(func(id types.NodeID) bool {
		seen[id.SnowflakeID()] = struct{}{}
		return true
	}); err != nil {
		return nil, err
	}
	if err := c.store.ForEachNodeHistoryID(func(id types.NodeID) bool {
		seen[id.SnowflakeID()] = struct{}{}
		return true
	}); err != nil {
		return nil, err
	}

	// Phase 2: process with store locks released — safe to call GetNodeAsOf.
	var result []*types.Node
	for id := range seen {
		n, err := c.Temporal.NodeAsOf(types.NodeID(id), txTime)
		if err != nil {
			if errors.Is(err, ErrNoVersionAsOf) {
				continue
			}
			return nil, err
		}
		result = append(result, n)
	}
	return result, nil
}

// RelsAsOf returns all relationships that existed at the given transaction time.
// Mirrors GetNodesAsOf for relationships.
func (t *TempOps) RelsAsOf(txTime types.Instant) ([]*types.Relationship, error) {
	c := t.c
	// Two-phase: collect IDs under store locks, then process after lock release.
	seen := make(map[snowflake.ID]struct{})
	if err := c.store.ForEachRelID(func(id types.RelID) bool {
		seen[id.SnowflakeID()] = struct{}{}
		return true
	}); err != nil {
		return nil, err
	}
	if err := c.store.ForEachRelHistoryID(func(id types.RelID) bool {
		seen[id.SnowflakeID()] = struct{}{}
		return true
	}); err != nil {
		return nil, err
	}

	// Phase 2: process with store locks released.
	var result []*types.Relationship
	for id := range seen {
		r, err := c.Temporal.RelAsOf(types.RelID(id), txTime)
		if err != nil {
			if errors.Is(err, ErrNoVersionAsOf) {
				continue
			}
			return nil, err
		}
		result = append(result, r)
	}
	return result, nil
}
