package core

import (
	"errors"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/integrity"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// VerifyNodeHashChain verifies the full hash chain for a node.
// Returns (true, nil) if the chain is valid. Returns (false, nil) if a hash
// mismatch or broken PrevHash link is detected. Returns (false, err) on I/O
// failure or if the node never existed (no current entity AND no history).
//
// Handles deleted entities: if the current node is gone (storepkg.ErrNodeNotFound) but
// history exists, verifies the history chain alone. Labels are extracted from
// the last history entry's internal tokens.
func (c *Core) VerifyNodeHashChain(id types.NodeID) (bool, error) {
	current, err := c.store.GetNode(id)
	if err != nil && !errors.Is(err, storepkg.ErrNodeNotFound) {
		return false, err
	}
	// current may be nil for deleted entities.

	history, err := c.store.GetNodeHistory(id)
	if err != nil {
		return false, err
	}

	if current == nil && len(history) == 0 {
		return false, storepkg.ErrNodeNotFound
	}

	// Build chain: history (ascending version order) + current (if exists).
	chain := make([]*types.Node, 0, len(history)+1)
	chain = append(chain, history...)
	if current != nil {
		chain = append(chain, current)
	}

	for i, entry := range chain {
		ig := entry.Integrity()
		if ig == nil {
			return false, nil
		}

		if entry.Version() == 0 {
			// Genesis: PrevHash must be empty.
			if ig.PrevHash != "" {
				return false, nil
			}
		} else if i > 0 {
			// Non-genesis with predecessor in chain: verify PrevHash link.
			prevIG := chain[i-1].Integrity()
			if prevIG == nil {
				return false, nil
			}
			if ig.PrevHash != prevIG.Hash {
				return false, nil
			}
		}
		// else: i == 0 && version != 0 → truncated history, skip link check.
		// Hash recomputation below still verifies content integrity.

		// Recompute hash and compare with stored.
		labels := c.NodeLabels(entry)
		computed := integrity.ComputeNodeHash(entry, labels)
		if ig.Hash != computed {
			return false, nil
		}
	}

	return true, nil
}

// VerifyRelHashChain verifies the full hash chain for a relationship.
// Returns (true, nil) if the chain is valid. Returns (false, nil) if a hash
// mismatch or broken PrevHash link is detected. Returns (false, err) on I/O
// failure or if the relationship never existed (no current AND no history).
//
// Handles deleted entities: if the current relationship is gone (storepkg.ErrRelNotFound)
// but history exists, verifies the history chain alone.
func (c *Core) VerifyRelHashChain(id types.RelID) (bool, error) {
	current, err := c.store.GetRelationship(id)
	if err != nil && !errors.Is(err, storepkg.ErrRelNotFound) {
		return false, err
	}
	// current may be nil for deleted entities.

	history, err := c.store.GetRelHistory(id)
	if err != nil {
		return false, err
	}

	if current == nil && len(history) == 0 {
		return false, storepkg.ErrRelNotFound
	}

	// Build chain: history (ascending version order) + current (if exists).
	chain := make([]*types.Relationship, 0, len(history)+1)
	chain = append(chain, history...)
	if current != nil {
		chain = append(chain, current)
	}

	// Extract type name from the best available source.
	typeSource := current
	if typeSource == nil {
		typeSource = chain[len(chain)-1]
	}
	typeName := c.RelationshipType(typeSource)

	for i, entry := range chain {
		ig := entry.Integrity()
		if ig == nil {
			return false, nil
		}

		if entry.Version() == 0 {
			// Genesis: PrevHash must be empty.
			if ig.PrevHash != "" {
				return false, nil
			}
		} else if i > 0 {
			// Non-genesis with predecessor in chain: verify PrevHash link.
			prevIG := chain[i-1].Integrity()
			if prevIG == nil {
				return false, nil
			}
			if ig.PrevHash != prevIG.Hash {
				return false, nil
			}
		}
		// else: i == 0 && version != 0 → truncated history, skip link check.

		// Recompute hash and compare with stored.
		computed := integrity.ComputeRelHash(entry, typeName)
		if ig.Hash != computed {
			return false, nil
		}
	}

	return true, nil
}
