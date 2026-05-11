package locks

import (
	"sort"
	"sync"

	snowflake "github.com/bds421/rho-snowflake-2026"
	snowflakepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/snowflake"
)

// ShardCount is the number of mutex shards held by Manager.
// Exported so tests in this package can assert range invariants.
const ShardCount = 256

// Manager provides fine-grained entity-level locking using sharded mutexes.
// 256 shards (2KB total) balance lock contention against memory overhead.
// Used by the Graph layer to serialize overlapping AddRelationship + DeleteNode operations.
//
// Type-agnostic by design: the same 256-shard pool serves both node and rel IDs
// by hashing the snowflake bits. LockEntity / LockTwo / LockMany all take raw
// snowflake.ID rather than a typed wrapper because the lock identity is a hash
// of the ID's timestamp bits, not "this is a node" vs "this is a rel". A typed
// wrapper would imply distinct lock domains; they're not. Documented Tier D.
type Manager struct {
	shards [ShardCount]sync.Mutex
}

// NewManager creates a new entity lock manager.
func NewManager() *Manager {
	return &Manager{}
}

// ShardIndex returns the shard index for the given entity ID.
// Uses the low 8 bits of the snowflake timestamp field. Entities created
// >256 time ticks apart land in different shards.
func ShardIndex(id snowflake.ID) uint8 {
	return uint8(snowflakepkg.Layout.Decompose(id).Time) & (ShardCount - 1) // #nosec G115 — masked to 8 bits
}

// LockEntity acquires the lock for a single entity.
func (lm *Manager) LockEntity(id snowflake.ID) {
	lm.shards[ShardIndex(id)].Lock()
}

// UnlockEntity releases the lock for a single entity.
func (lm *Manager) UnlockEntity(id snowflake.ID) {
	lm.shards[ShardIndex(id)].Unlock()
}

// LockTwo acquires locks for two entities in ascending shard order (deadlock-free).
// If both IDs hash to the same shard, only one lock is acquired.
func (lm *Manager) LockTwo(a, b snowflake.ID) {
	sa, sb := ShardIndex(a), ShardIndex(b)
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
func (lm *Manager) UnlockTwo(a, b snowflake.ID) {
	sa, sb := ShardIndex(a), ShardIndex(b)
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

// LockThree acquires locks for three entities in ascending shard order
// (deadlock-free). Duplicate shard indices are locked only once.
func (lm *Manager) LockThree(a, b, c snowflake.ID) {
	shards, n := uniqueSortedThreeShards(a, b, c)
	for i := 0; i < n; i++ {
		lm.shards[shards[i]].Lock()
	}
}

// UnlockThree releases locks for three entities in reverse shard order.
func (lm *Manager) UnlockThree(a, b, c snowflake.ID) {
	shards, n := uniqueSortedThreeShards(a, b, c)
	for i := n - 1; i >= 0; i-- {
		lm.shards[shards[i]].Unlock()
	}
}

func uniqueSortedThreeShards(a, b, c snowflake.ID) ([3]uint8, int) {
	shards := [3]uint8{ShardIndex(a)}
	n := 1
	insertUniqueShard := func(s uint8) {
		for i := 0; i < n; i++ {
			if shards[i] == s {
				return
			}
		}
		j := n
		for j > 0 && shards[j-1] > s {
			shards[j] = shards[j-1]
			j--
		}
		shards[j] = s
		n++
	}
	insertUniqueShard(ShardIndex(b))
	insertUniqueShard(ShardIndex(c))
	return shards, n
}

// LockMany acquires locks for multiple entities in ascending shard order (deadlock-free).
// Deduplicates shard indices so each shard is locked at most once.
func (lm *Manager) LockMany(ids []snowflake.ID) {
	shards := lm.uniqueSortedShards(ids)
	for _, s := range shards {
		lm.shards[s].Lock()
	}
}

// UnlockMany releases locks for multiple entities in reverse shard order.
func (lm *Manager) UnlockMany(ids []snowflake.ID) {
	shards := lm.uniqueSortedShards(ids)
	for i := len(shards) - 1; i >= 0; i-- {
		lm.shards[shards[i]].Unlock()
	}
}

// uniqueSortedShards returns the deduplicated, ascending-sorted shard indices for ids.
func (lm *Manager) uniqueSortedShards(ids []snowflake.ID) []uint8 {
	seen := make(map[uint8]struct{}, len(ids))
	for _, id := range ids {
		seen[ShardIndex(id)] = struct{}{}
	}
	shards := make([]uint8, 0, len(seen))
	for s := range seen {
		shards = append(shards, s)
	}
	sort.Slice(shards, func(i, j int) bool { return shards[i] < shards[j] })
	return shards
}
