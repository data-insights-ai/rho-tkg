package graph

import (
	"sync"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg-v3/pkg/types"
)

// GraphTx is a create-only transaction that tracks created entities for rollback.
// It holds the graph write lock for the entire duration of the transaction,
// blocking concurrent Batch/Snapshot operations. Not suitable for long-running
// transactions, but acceptable for create-only transactions (fast).
//
// All methods check the done flag and return ErrTxDone after Commit/Rollback.
type GraphTx struct {
	g            *Graph
	createdNodes []snowflake.ID
	createdRels  []snowflake.ID
	mu           sync.Mutex // protects done flag
	done         bool
}

// BeginTx starts a new create-only transaction.
// Acquires the graph write lock, blocking concurrent Batch/Snapshot operations.
// The lock is released on Commit() or Rollback().
func (g *Graph) BeginTx() *GraphTx {
	g.mu.Lock()
	return &GraphTx{g: g}
}

// AddNode creates a new node within the transaction.
// The node ID is tracked for rollback. Delegates to Graph.AddNode.
func (tx *GraphTx) AddNode(labels []string, props map[string]any) (*types.Node, error) {
	tx.mu.Lock()
	if tx.done {
		tx.mu.Unlock()
		return nil, ErrTxDone
	}
	tx.mu.Unlock()

	n, err := tx.g.AddNode(labels, props)
	if err != nil {
		return nil, err
	}

	tx.mu.Lock()
	tx.createdNodes = append(tx.createdNodes, n.InternalID().SnowflakeID())
	tx.mu.Unlock()

	return n, nil
}

// AddRelationship creates a new relationship within the transaction.
// The relationship ID is tracked for rollback. Delegates to Graph.AddRelationship.
func (tx *GraphTx) AddRelationship(typeName string, startNode, endNode *types.Node, props map[string]any) (*types.Relationship, error) {
	tx.mu.Lock()
	if tx.done {
		tx.mu.Unlock()
		return nil, ErrTxDone
	}
	tx.mu.Unlock()

	r, err := tx.g.AddRelationship(typeName, startNode, endNode, props)
	if err != nil {
		return nil, err
	}

	tx.mu.Lock()
	tx.createdRels = append(tx.createdRels, r.InternalID().SnowflakeID())
	tx.mu.Unlock()

	return r, nil
}

// Commit finalizes the transaction, making all created entities permanent.
// Releases the graph write lock. After Commit, all tx methods return ErrTxDone.
func (tx *GraphTx) Commit() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.done {
		return ErrTxDone
	}
	tx.done = true
	tx.g.mu.Unlock()
	return nil
}

// Rollback undoes all entity creations in reverse order, then releases the
// graph write lock. Uses store.Delete* directly to avoid writing tombstone
// versions — rolled-back entities should vanish completely.
//
// Best-effort: continues on error, returns the first error encountered.
// After Rollback, all tx methods return ErrTxDone.
func (tx *GraphTx) Rollback() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.done {
		return ErrTxDone
	}
	tx.done = true

	var firstErr error

	// Delete relationships in reverse creation order.
	for i := len(tx.createdRels) - 1; i >= 0; i-- {
		if err := tx.g.store.DeleteRelationship(tx.createdRels[i]); err != nil {
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	// Delete nodes in reverse creation order (cascade to handle any remaining refs).
	for i := len(tx.createdNodes) - 1; i >= 0; i-- {
		if err := tx.g.store.DeleteNodeCascade(tx.createdNodes[i]); err != nil {
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	tx.g.mu.Unlock()
	return firstErr
}

// CreatedNodeIDs returns the snowflake IDs of all nodes created in this transaction.
// Useful for inspecting transaction state in tests.
func (tx *GraphTx) CreatedNodeIDs() []snowflake.ID {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	cp := make([]snowflake.ID, len(tx.createdNodes))
	copy(cp, tx.createdNodes)
	return cp
}

// CreatedRelIDs returns the snowflake IDs of all relationships created in this transaction.
func (tx *GraphTx) CreatedRelIDs() []snowflake.ID {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	cp := make([]snowflake.ID, len(tx.createdRels))
	copy(cp, tx.createdRels)
	return cp
}
