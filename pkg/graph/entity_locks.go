package graph

import (
	"sync"

	snowflake "github.com/bds421/rho-snowflake-2026"
)

const entityLockShards = 256

// entityLockManager provides fine-grained entity-level locking using sharded mutexes.
// 256 shards (2KB total) balance lock contention against memory overhead.
// Used by the Graph layer to serialize overlapping AddRelationship + DeleteNode operations.
type entityLockManager struct {
	shards [entityLockShards]sync.Mutex
}

// newEntityLockManager creates a new entity lock manager.
func newEntityLockManager() *entityLockManager {
	return &entityLockManager{}
}

// shardIndex returns the shard index for the given entity ID.
func shardIndex(id snowflake.ID) uint8 {
	return uint8(uint64(id) & (entityLockShards - 1)) // #nosec G115 — masking to 8 bits
}

// LockEntity acquires the lock for a single entity.
func (lm *entityLockManager) LockEntity(id snowflake.ID) {
	lm.shards[shardIndex(id)].Lock()
}

// UnlockEntity releases the lock for a single entity.
func (lm *entityLockManager) UnlockEntity(id snowflake.ID) {
	lm.shards[shardIndex(id)].Unlock()
}

// LockTwo acquires locks for two entities in ascending shard order (deadlock-free).
// If both IDs hash to the same shard, only one lock is acquired.
func (lm *entityLockManager) LockTwo(a, b snowflake.ID) {
	sa, sb := shardIndex(a), shardIndex(b)
	if sa == sb {
		lm.shards[sa].Lock()
		return
	}
	if sa > sb {
		sa, sb = sb, sa
	}
	lm.shards[sa].Lock()
	lm.shards[sb].Lock()
}

// UnlockTwo releases locks for two entities.
// If both IDs hash to the same shard, only one lock is released.
func (lm *entityLockManager) UnlockTwo(a, b snowflake.ID) {
	sa, sb := shardIndex(a), shardIndex(b)
	if sa == sb {
		lm.shards[sa].Unlock()
		return
	}
	// Unlock order doesn't matter for correctness, but unlock in reverse
	// acquisition order by convention.
	if sa > sb {
		sa, sb = sb, sa
	}
	lm.shards[sb].Unlock()
	lm.shards[sa].Unlock()
}
