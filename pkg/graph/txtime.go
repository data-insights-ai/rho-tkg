package graph

import (
	"errors"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg-v3/pkg/types"
)

// ErrNoVersionAsOf is returned when no entity version was recorded at the given
// transaction time.
var ErrNoVersionAsOf = errors.New("graph: no entity version recorded at the given transaction time")

// GetNodeAsOf returns the node version that was current at the given transaction time.
//
// Algorithm:
//  1. Try current: if TxFrom > 0 && TxFrom <= txTime && TxTo == 0 → return it.
//  2. Scan GetNodeHistory(id): find version where TxFrom > 0 && TxFrom <= txTime
//     && (TxTo == 0 || TxTo > txTime) → return latest matching.
//  3. None found → ErrNoVersionAsOf.
func (g *Graph) GetNodeAsOf(id snowflake.ID, txTime types.Instant) (*types.Node, error) {
	// Try current node first.
	current, err := g.store.GetNode(id)
	if err != nil && !errors.Is(err, ErrNodeNotFound) {
		return nil, err
	}
	if current != nil {
		if tm := current.Temporal(); tm != nil && tm.TxFrom > 0 && tm.TxFrom <= txTime && tm.TxTo == 0 {
			return current, nil
		}
	}

	// Scan history.
	hist, err := g.store.GetNodeHistory(id)
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

// GetRelAsOf returns the relationship version that was current at the given
// transaction time. Mirrors GetNodeAsOf for relationships.
func (g *Graph) GetRelAsOf(id snowflake.ID, txTime types.Instant) (*types.Relationship, error) {
	current, err := g.store.GetRelationship(id)
	if err != nil && !errors.Is(err, ErrRelNotFound) {
		return nil, err
	}
	if current != nil {
		if tm := current.Temporal(); tm != nil && tm.TxFrom > 0 && tm.TxFrom <= txTime && tm.TxTo == 0 {
			return current, nil
		}
	}

	hist, err := g.store.GetRelHistory(id)
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

// GetNodesAsOf returns all nodes that existed at the given transaction time.
// Collects all known node IDs (current + history) using ForEach iterators,
// calls GetNodeAsOf per ID, skips ErrNoVersionAsOf.
// Returns nil, nil if no nodes existed at txTime.
func (g *Graph) GetNodesAsOf(txTime types.Instant) ([]*types.Node, error) {
	// Two-phase: collect IDs under store locks, then process after lock release.
	// Callback must NOT call store methods (B15 — lock reentrancy deadlock).
	seen := make(map[snowflake.ID]struct{})
	if err := g.store.ForEachNodeID(func(id snowflake.ID) bool {
		seen[id] = struct{}{}
		return true
	}); err != nil {
		return nil, err
	}
	if err := g.store.ForEachNodeHistoryID(func(id snowflake.ID) bool {
		seen[id] = struct{}{}
		return true
	}); err != nil {
		return nil, err
	}

	// Phase 2: process with store locks released — safe to call GetNodeAsOf.
	var result []*types.Node
	for id := range seen {
		n, err := g.GetNodeAsOf(id, txTime)
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

// GetRelsAsOf returns all relationships that existed at the given transaction time.
// Mirrors GetNodesAsOf for relationships.
func (g *Graph) GetRelsAsOf(txTime types.Instant) ([]*types.Relationship, error) {
	// Two-phase: collect IDs under store locks, then process after lock release.
	seen := make(map[snowflake.ID]struct{})
	if err := g.store.ForEachRelID(func(id snowflake.ID) bool {
		seen[id] = struct{}{}
		return true
	}); err != nil {
		return nil, err
	}
	if err := g.store.ForEachRelHistoryID(func(id snowflake.ID) bool {
		seen[id] = struct{}{}
		return true
	}); err != nil {
		return nil, err
	}

	// Phase 2: process with store locks released.
	var result []*types.Relationship
	for id := range seen {
		r, err := g.GetRelAsOf(id, txTime)
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
