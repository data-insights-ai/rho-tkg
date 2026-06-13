package index

import (
	"cmp"
	"slices"

	snowflake "github.com/bds421/rho-snowflake-2026"
)

// EntityCache is the method set the badger store depends on for its node/rel
// entity caches. Both *Cache[V] (single-mutex) and *ShardedCache[V] (N-way
// sharded) satisfy it, so the store field can hold either with no other change.
//
// Defining the interface here (not in the store package) keeps the index
// package the single source of truth for the cache contract: adding a method to
// Cache without adding it here is a compile error at the store, not a silent
// drift.
type EntityCache[V any] interface {
	Get(key snowflake.ID) (V, CacheStatus)
	GetNoPromote(key snowflake.ID) (V, CacheStatus)
	SetNoEvict()
	Peek(key snowflake.ID) (V, CacheStatus)
	Put(key snowflake.ID, value V)
	MarkDeleted(key snowflake.ID)
	LoadClean(key snowflake.ID, value V)
	CollectDirty() []Entry[V]
	MarkFlushed(flushed map[snowflake.ID]uint64)
	Cap() int
	Len() int
	CleanCount() int
	Bytes() int64
	Budget() int64
	Hits() int64
	Misses() int64
	ResetForTest()
	EvictForTest(key snowflake.ID) bool
}

// Compile-time proof both implementations satisfy the contract.
var (
	_ EntityCache[int] = (*Cache[int])(nil)
	_ EntityCache[int] = (*ShardedCache[int])(nil)
)

// maxShards caps the shard count. 256 is far beyond any realistic worker count
// and keeps per-shard capacity meaningful even for modest total capacities.
const maxShards = 256

// ShardedCache is an N-way sharded wrapper over Cache[V]. Each shard owns an
// independent mutex + LRU + dirtySet, so concurrent Get/Put/MarkDeleted/LoadClean
// on keys that hash to different shards proceed in parallel. This removes the
// single-mutex serialization that made concurrent label scans (one Get per node)
// collapse to single-core throughput.
//
// External semantics match Cache[V] EXACTLY for every routed and aggregating
// method, with ONE documented behavioral difference: the LRU recency order is
// shard-local. Per-shard capacity is globalCapacity/N, so eviction picks the LRU
// victim within the key's shard, not across a single global recency list. With a
// balanced hash this loses negligible hit rate; it is the standard RocksDB
// ShardedLRUCache tradeoff. Aggregate accounting (Cap/Budget/Len/Bytes/Hits/...)
// returns the sum across shards, so the configured totals round-trip.
type ShardedCache[V any] struct {
	shards []*Cache[V]
	mask   uint64 // len(shards)-1; len(shards) is always a power of two
}

// shardCount picks a power-of-two shard count >= hint, clamped to [1, maxShards].
func shardCount(hint int) int {
	if hint < 1 {
		hint = 1
	}
	n := 1
	for n < hint && n < maxShards {
		n <<= 1
	}
	return n
}

// ShardHint derives a shard count from a parallelism hint (typically
// GOMAXPROCS). It floors at 16 so even on small machines concurrent scans get
// enough shards to spread across the benchmark's 24 workers, and rounds up to a
// power of two (clamped to maxShards).
func ShardHint(parallelism int) int {
	if parallelism < 16 {
		parallelism = 16
	}
	return shardCount(parallelism)
}

// NewShardedCache creates a sharded LRU with the given TOTAL capacity, split
// evenly across nShards (rounded up to a power of two). Each shard gets
// capacity/N (floored at 1). nShards <= 0 picks a single shard.
func NewShardedCache[V any](capacity, nShards int) *ShardedCache[V] {
	return NewShardedCacheWithBudget[V](capacity, 0, nil, nShards)
}

// NewShardedCacheWithBudget is the budgeted form. The TOTAL byte budget is split
// evenly across shards (budget/N). budget <= 0 or a nil sizer disables byte
// accounting (capacity-only), matching NewCacheWithBudget.
//
// Tiny-capacity clamp: never create more shards than the total capacity. With
// capacity=4 and a 16-shard hint, each shard would floor to capacity 1 for a
// TOTAL of 16 — quadrupling the configured size and changing eviction. Clamping
// nShards to capacity keeps the aggregate capacity ~= the requested value. For
// production caches (capacity in the 100k–millions) this never triggers.
func NewShardedCacheWithBudget[V any](
	capacity int, budget int64, sizer func(V) int64, nShards int,
) *ShardedCache[V] {
	n := shardCount(nShards)
	if capacity >= 1 && n > capacity {
		n = capacity
	}

	// Distribute capacity (and budget) so the AGGREGATE equals the requested
	// total EXACTLY: the first `capRem` shards get one extra slot. A plain
	// capacity/n floor silently loses the remainder (e.g. 250/16 -> 240), which
	// both undersizes the cache and trips exact-capacity assertions (a tiered
	// per-shard tuning test caught this). When capacity >= n (always, given the
	// tiny-capacity clamp above for capacity >= 1) the per-shard floor never
	// fires and the sum is capBase*n + capRem == capacity.
	capBase := capacity / n
	capRem := capacity % n
	hasBudget := budget > 0 && sizer != nil
	var budBase, budRem int64
	if hasBudget {
		budBase = budget / int64(n)
		budRem = budget % int64(n)
	}

	shards := make([]*Cache[V], n)
	for i := range shards {
		c := capBase
		if i < capRem {
			c++
		}
		if c < 1 {
			c = 1 // NewCache also floors at 1 (covers capacity == 0)
		}
		var b int64
		if hasBudget {
			b = budBase
			if int64(i) < budRem {
				b++
			}
			if b < 1 {
				b = 1
			}
		}
		shards[i] = NewCacheWithBudget(c, b, sizer)
	}
	return &ShardedCache[V]{shards: shards, mask: uint64(n - 1)}
}

// indexFor mixes the snowflake ID and routes to a shard index by the low bits of
// the mix. THIS IS THE SINGLE SOURCE OF TRUTH for routing — every routed and
// per-key method funnels through it. Splitting the hash across two copies is a
// divergence hazard: if Put and MarkFlushed disagree on a key's shard,
// MarkFlushed clears dirty on the wrong entry and leaks the real one's write
// forever (the exact lost-write → counter-mismatch → fatal-on-restart failure).
//
// Snowflake IDs are time-ordered (high bits = timestamp), so key%N or raw low
// bits would cluster temporally adjacent inserts into the same shard. The
// splitmix64 finalizer avalanches every input bit into every output bit, so the
// post-mix low bits are uniform even for a monotonic ID sequence.
func (s *ShardedCache[V]) indexFor(key snowflake.ID) int {
	x := uint64(key.Int64())
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return int(x & s.mask) // post-avalanche low bits are uniform
}

func (s *ShardedCache[V]) shardFor(key snowflake.ID) *Cache[V] {
	return s.shards[s.indexFor(key)]
}

// --- Routed single-shard methods: each takes only its own shard's mutex. ---

func (s *ShardedCache[V]) Get(key snowflake.ID) (V, CacheStatus) {
	return s.shardFor(key).Get(key)
}

// SetNoEvict puts every shard into resident mode (clean entries never evicted).
// See Cache.SetNoEvict.
func (s *ShardedCache[V]) SetNoEvict() {
	for _, sh := range s.shards {
		sh.SetNoEvict()
	}
}

// GetNoPromote routes a non-promoting read to the key's shard. See
// Cache.GetNoPromote — used by every prefetch*Scan path (label, type, all,
// temporal, numeric-range) so concurrent scans take only the shard's read lock
// and never serialize on MoveToFront.
func (s *ShardedCache[V]) GetNoPromote(key snowflake.ID) (V, CacheStatus) {
	return s.shardFor(key).GetNoPromote(key)
}

func (s *ShardedCache[V]) Peek(key snowflake.ID) (V, CacheStatus) {
	return s.shardFor(key).Peek(key)
}

func (s *ShardedCache[V]) Put(key snowflake.ID, value V) {
	s.shardFor(key).Put(key, value)
}

func (s *ShardedCache[V]) MarkDeleted(key snowflake.ID) {
	s.shardFor(key).MarkDeleted(key)
}

func (s *ShardedCache[V]) LoadClean(key snowflake.ID, value V) {
	s.shardFor(key).LoadClean(key, value)
}

func (s *ShardedCache[V]) EvictForTest(key snowflake.ID) bool {
	return s.shardFor(key).EvictForTest(key)
}

// --- Aggregating methods. ---

// CollectDirty walks every shard's dirtySet under that shard's lock,
// concatenates the per-shard snapshots, then applies ONE global sort so the
// merged batch stays key-ordered. The flush layer relies on deterministic,
// sorted batches; per-shard slices are each sorted but their concatenation is
// not, so the post-concat sort is MANDATORY (dropping it silently loses the
// ordering the flusher's "deterministic batches" comment depends on).
//
// Locks are taken and released one shard at a time — never two at once — so this
// can run concurrently with single-shard Get/Put on other shards and can never
// deadlock.
func (s *ShardedCache[V]) CollectDirty() []Entry[V] {
	var out []Entry[V]
	for _, sh := range s.shards {
		out = append(out, sh.CollectDirty()...) // per-shard sorted; merged below
	}
	slices.SortFunc(out, func(a, b Entry[V]) int { return cmp.Compare(a.Key, b.Key) })
	return out
}

// MarkFlushed groups the flushed versions by owning shard, then calls each
// shard's MarkFlushed once with its slice of the map. A key routes (via the same
// indexFor used by Put/MarkDeleted) to exactly the shard that holds it, so the
// version-match check runs against the correct entry. Per-shard nextVer counters
// are sufficient: a version is only ever compared against the SAME key's stored
// entry within the SAME shard, so versions never need to be globally unique.
func (s *ShardedCache[V]) MarkFlushed(flushed map[snowflake.ID]uint64) {
	if len(flushed) == 0 {
		return
	}
	// Bucket keys by shard index to avoid taking N locks when only a few shards
	// were touched — and to call each shard's MarkFlushed exactly once.
	buckets := make(map[int]map[snowflake.ID]uint64)
	for id, ver := range flushed {
		idx := s.indexFor(id)
		b := buckets[idx]
		if b == nil {
			b = make(map[snowflake.ID]uint64)
			buckets[idx] = b
		}
		b[id] = ver
	}
	for idx, b := range buckets {
		s.shards[idx].MarkFlushed(b)
	}
}

// --- Sum/aggregate scalars. ---
//
// These read each shard under its own lock sequentially, so they are NOT atomic
// snapshots across shards: a concurrent writer can change a later shard after an
// earlier one was read. This matches Cache[V], which already returns an
// approximate count the instant its lock releases; no caller treats these as
// transactionally consistent.

func (s *ShardedCache[V]) Len() int {
	n := 0
	for _, sh := range s.shards {
		n += sh.Len()
	}
	return n
}

func (s *ShardedCache[V]) CleanCount() int {
	n := 0
	for _, sh := range s.shards {
		n += sh.CleanCount()
	}
	return n
}

// Cap returns the aggregate capacity (sum of shard caps), so the Clear()
// round-trip (newCache(old.Cap(), old.Budget())) rebuilds an identically-sized
// cache. The constructor distributes the remainder across shards, so this
// equals the requested total exactly for any capacity >= the shard count; it
// rounds up to the shard count only in the degenerate capacity==0 case (the
// per-shard floor of 1).
func (s *ShardedCache[V]) Cap() int {
	n := 0
	for _, sh := range s.shards {
		n += sh.Cap()
	}
	return n
}

func (s *ShardedCache[V]) Bytes() int64 {
	var b int64
	for _, sh := range s.shards {
		b += sh.Bytes()
	}
	return b
}

// Budget returns the aggregate budget (sum of per-shard budgets ~= the
// configured total). Returning one shard's budget would shrink the budget by a
// factor of N on every Clear round-trip.
func (s *ShardedCache[V]) Budget() int64 {
	var b int64
	for _, sh := range s.shards {
		b += sh.Budget()
	}
	return b
}

func (s *ShardedCache[V]) Hits() int64 {
	var h int64
	for _, sh := range s.shards {
		h += sh.Hits()
	}
	return h
}

func (s *ShardedCache[V]) Misses() int64 {
	var m int64
	for _, sh := range s.shards {
		m += sh.Misses()
	}
	return m
}

func (s *ShardedCache[V]) ResetForTest() {
	for _, sh := range s.shards {
		sh.ResetForTest()
	}
}
