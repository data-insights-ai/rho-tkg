package core

import (
	"io"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/tiered"
	temporalpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/temporal"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// Tx-scoped consistent-read APIs (R5-F2).
//
// The standalone IO/Temporal/Admin entry points hold c.mu.RLock for
// their duration, which excludes tx/batch but NOT individual
// standalone mutations (which also take RLock). For a strict view of
// the graph — no concurrent writers, no torn reads — call these
// methods from inside g.Tx.Run / g.Tx.RunContext: the tx already
// holds c.mu.Lock, so the underlying lock-free implementations run
// without re-entering c.mu.
//
// Calling the standalone (*IOOps).Export, (*TempOps).Snapshot, or
// (*AdminOps).VerifyShard from inside g.Tx.Run would deadlock because
// sync.RWMutex is not reentrant — those methods try to RLock while
// the tx holds Lock. The tx-scoped methods below avoid that trap.

// Export writes a portable graph snapshot to w under the transaction's
// write lock. Strict consistency: no concurrent mutations can
// interleave with the export's reads while the transaction is live.
// See (*IOOps).Export for stream format details.
func (tx *GraphTx) Export(w io.Writer) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.done {
		return storepkg.ErrTxDone
	}
	return tx.g.exportLocked(w)
}

// Snapshot returns the graph state at the given instant under the
// transaction's write lock. Strict consistency: no concurrent
// mutations can interleave with the snapshot's reads while the
// transaction is live.
func (tx *GraphTx) Snapshot(at types.Instant) (*temporalpkg.GraphSnapshot, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.done {
		return nil, storepkg.ErrTxDone
	}
	return tx.g.snapshotAt(at)
}

// VerifyShard runs hash chain verification on a tiered-store shard
// under the transaction's write lock. Strict consistency: no
// concurrent mutations can flip an entity's hash chain mid-scan.
// Returns ErrNotTieredStore if the underlying store does not support
// shard-level verification.
func (tx *GraphTx) VerifyShard(shardName string) (*tiered.VerifyResult, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.done {
		return nil, storepkg.ErrTxDone
	}
	return tx.g.verifyShardLocked(shardName)
}
