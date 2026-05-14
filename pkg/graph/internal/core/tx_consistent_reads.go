package core

import (
	"io"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/tiered"
	temporalpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/temporal"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
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
