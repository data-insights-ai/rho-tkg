package graph

import (
	"sort"
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
// Uses the low 8 bits of the snowflake timestamp field. Entities created
// >256 time ticks apart land in different shards.
func shardIndex(id snowflake.ID) uint8 {
	return uint8(snowflakeLayout.Decompose(id).Time) & (entityLockShards - 1)
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

// LockMany acquires locks for multiple entities in ascending shard order (deadlock-free).
// Deduplicates shard indices so each shard is locked at most once.
func (lm *entityLockManager) LockMany(ids []snowflake.ID) {
	shards := lm.uniqueSortedShards(ids)
	for _, s := range shards {
		lm.shards[s].Lock()
	}
}

// UnlockMany releases locks for multiple entities in reverse shard order.
func (lm *entityLockManager) UnlockMany(ids []snowflake.ID) {
	shards := lm.uniqueSortedShards(ids)
	for i := len(shards) - 1; i >= 0; i-- {
		lm.shards[shards[i]].Unlock()
	}
}

// uniqueSortedShards returns the deduplicated, ascending-sorted shard indices for ids.
func (lm *entityLockManager) uniqueSortedShards(ids []snowflake.ID) []uint8 {
	seen := make(map[uint8]struct{}, len(ids))
	for _, id := range ids {
		seen[shardIndex(id)] = struct{}{}
	}
	shards := make([]uint8, 0, len(seen))
	for s := range seen {
		shards = append(shards, s)
	}
	sort.Slice(shards, func(i, j int) bool { return shards[i] < shards[j] })
	return shards
}
