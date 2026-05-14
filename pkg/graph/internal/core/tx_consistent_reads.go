package core

import (
	"io"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/store/tiered"
	temporalpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/temporal"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/types"
)

// Tx-scoped consistent-read APIs (R5-F2).
//
// The standalone IO/Temporal/Admin snapshot-style entry points acquire
// c.mu.Lock themselves. Code that is already inside g.Tx.Run /
// g.Tx.RunContext must call these tx-scoped methods instead: the tx
// already holds c.mu.Lock, so the underlying lock-free implementations
// run without re-entering c.mu.
//
// Calling the standalone (*IOOps).Export, (*TempOps).Snapshot, or
// (*AdminOps).VerifyShard from inside g.Tx.Run would deadlock because
// sync.RWMutex is not reentrant — those methods try to Lock while
// the tx holds Lock. The tx-scoped methods below avoid that trap.

// Export writes a portable graph snapshot to w under the transaction's
// write lock. Strict consistency: no concurrent mutations can
// interleave with the export's reads while the transaction is live.
// See (*IOOps).Export for stream format details.
func (tx *GraphTx) Export(w io.Writer) error {
	if err := tx.lockActive(); err != nil {
		return err
	}
	defer tx.mu.Unlock()
	return tx.g.exportLocked(w)
}

// Snapshot returns the graph state at the given instant under the
// transaction's write lock. Strict consistency: no concurrent
// mutations can interleave with the snapshot's reads while the
// transaction is live.
func (tx *GraphTx) Snapshot(at types.Instant) (*temporalpkg.GraphSnapshot, error) {
	if err := tx.lockActive(); err != nil {
		return nil, err
	}
	defer tx.mu.Unlock()
	return tx.g.snapshotAt(at)
}

// VerifyShard runs hash chain verification on a tiered-store shard
// under the transaction's write lock. Strict consistency: no
// concurrent mutations can flip an entity's hash chain mid-scan.
// Returns ErrNotTieredStore if the underlying store does not support
// shard-level verification.
func (tx *GraphTx) VerifyShard(shardName string) (*tiered.VerifyResult, error) {
	if err := tx.lockActive(); err != nil {
		return nil, err
	}
	defer tx.mu.Unlock()
	return tx.g.verifyShardLocked(shardName)
}

// Tx-side resolution and shadow-property mirrors.
//
// Calling g.Nodes.Labels, g.Nodes.HasLabel, g.Nodes.PrimaryLabel,
// g.Rels.Type, g.Rels.HasType, g.Resolve.NodeProperty, or
// g.Resolve.RelProperty from inside g.Tx.Run / g.Tx.RunContext deadlocks
// the caller goroutine because each acquires c.mu.RLock while the tx
// already holds c.mu.Lock (sync.RWMutex is not reentrant — lesson 9).
// The mirrors below call the lock-free *Unlocked helpers directly and
// guard only the tx-state mutex.
//
// After Commit/Rollback every method returns the zero value of its
// return type — matching the silently-fail-closed contract of the
// non-tx accessors, which return zero values when c.closed is set.

// Labels resolves all label tokens on the node to strings within the
// transaction. Returns nil after Commit/Rollback.
func (tx *GraphTx) Labels(n *types.Node) []string {
	if tx == nil || tx.g == nil {
		return nil
	}
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.done {
		return nil
	}
	return tx.g.nodeLabelsUnlocked(n)
}

// PrimaryLabel resolves the node's primary label token within the
// transaction. Returns "" after Commit/Rollback.
func (tx *GraphTx) PrimaryLabel(n *types.Node) string {
	if tx == nil || tx.g == nil || n == nil {
		return ""
	}
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.done {
		return ""
	}
	return tx.g.nodePrimaryLabelUnlocked(n)
}

// HasLabel reports whether the node carries the given label name within
// the transaction. Returns false after Commit/Rollback or for an invalid
// or unregistered label.
func (tx *GraphTx) HasLabel(n *types.Node, label string) bool {
	if tx == nil || tx.g == nil || n == nil {
		return false
	}
	if err := tx.g.validateName(label); err != nil {
		return false
	}
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.done {
		return false
	}
	return tx.g.nodeHasLabelUnlocked(n, label)
}

// RelType resolves the relationship's type token to a string within the
// transaction. Returns "" after Commit/Rollback.
func (tx *GraphTx) RelType(r *types.Relationship) string {
	if tx == nil || tx.g == nil || r == nil {
		return ""
	}
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.done {
		return ""
	}
	return tx.g.relTypeUnlocked(r)
}

// HasType reports whether the relationship has the given type name
// within the transaction. Returns false after Commit/Rollback or for an
// invalid or unregistered type name.
func (tx *GraphTx) HasType(r *types.Relationship, typ string) bool {
	if tx == nil || tx.g == nil || r == nil {
		return false
	}
	if err := tx.g.validateName(typ); err != nil {
		return false
	}
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.done {
		return false
	}
	return tx.g.relHasTypeUnlocked(r, typ)
}

// NodeProperty resolves a property key (including tkg_* shadow keys) on a
// node within the transaction. Returns (nil, false) after Commit/Rollback.
func (tx *GraphTx) NodeProperty(n *types.Node, key string) (any, bool) {
	if tx == nil || tx.g == nil {
		return nil, false
	}
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.done {
		return nil, false
	}
	return tx.g.nodePropertyUnlocked(n, key)
}

// RelProperty resolves a property key (including tkg_* shadow keys) on a
// relationship within the transaction. Returns (nil, false) after
// Commit/Rollback.
func (tx *GraphTx) RelProperty(r *types.Relationship, key string) (any, bool) {
	if tx == nil || tx.g == nil {
		return nil, false
	}
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.done {
		return nil, false
	}
	return tx.g.relPropertyUnlocked(r, key)
}
