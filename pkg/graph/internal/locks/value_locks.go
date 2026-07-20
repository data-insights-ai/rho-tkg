package locks

import (
	"sort"
	"sync"
)

// FNV-1a 64-bit constants (matches hash/fnv's fnv.New64a()). Inlined below
// instead of using fnv.New64a() directly — BACKLOG 15g: hash.Hash64 is an
// interface, so New64a()'s concrete *sum64a escapes to the heap on every
// ValueStripe call (once per constrained property per node write, a hot
// path). Accumulating over a local uint64 keeps this allocation-free.
const (
	fnvOffset64 uint64 = 14695981039346656037
	fnvPrime64  uint64 = 1099511628211
)

// ValueShardCount is the number of mutex stripes held by ValueManager.
// Exported so tests in this package can assert range invariants. Mirrors the
// entity-lock Manager's 256 shards.
const ValueShardCount = 256

// ValueManager provides fine-grained locking keyed by a constrained
// (labelToken, keyToken, canonical value bytes) triple, striped across 256
// mutexes. It guards unique-property-constraint enforcement: a create or update
// that introduces or changes a constrained value holds the value stripe across
// BOTH the index lookup AND the store write, so two writers racing to claim the
// same value serialize and exactly one wins (the loser sees the other's freshly
// written index entry and is rejected).
//
// LOCK ORDER (global, documented once here and repeated in CLAUDE.md):
//
//	entity locks  ->  value locks  ->  idxMu
//
// A caller that needs both an entity lock and a value lock MUST take the entity
// lock first. A caller that holds value locks must never take an entity lock
// afterwards. An update that CHANGES a constrained value takes BOTH the old and
// the new value stripe, always in ascending stripe order (LockValues sorts and
// deduplicates), so two updates crossing the same pair of stripes cannot form a
// lock cycle. Because a fresh generated-ID create holds no other operation's
// entity lock, taking a value stripe before the (unshared) fresh entity's store
// write introduces no cycle either.
type ValueManager struct {
	shards [ValueShardCount]sync.Mutex
}

// NewValueManager creates a new value-lock manager.
func NewValueManager() *ValueManager { return &ValueManager{} }

// ValueStripe returns the stripe index for a constrained value triple.
// Deterministic across process restarts (FNV-1a over label token, key token,
// and the canonical value bytes) — byte-for-byte equivalent to hashing the
// same 4-byte header + value through fnv.New64a(), see the golden-vector
// test pinning this.
func ValueStripe(labelToken, keyToken uint16, value []byte) uint8 {
	h := fnvOffset64
	h = (h ^ uint64(byte(labelToken))) * fnvPrime64
	h = (h ^ uint64(byte(labelToken>>8))) * fnvPrime64
	h = (h ^ uint64(byte(keyToken))) * fnvPrime64
	h = (h ^ uint64(byte(keyToken>>8))) * fnvPrime64
	for _, b := range value {
		h = (h ^ uint64(b)) * fnvPrime64
	}
	return uint8(h & (ValueShardCount - 1)) // #nosec G115 — masked to 8 bits
}

// LockValue acquires the stripe for one constrained value triple and returns
// the stripe index the caller passes to UnlockStripe.
func (vm *ValueManager) LockValue(labelToken, keyToken uint16, value []byte) uint8 {
	s := ValueStripe(labelToken, keyToken, value)
	vm.shards[s].Lock()
	return s
}

// UnlockStripe releases a single stripe previously acquired via LockValue.
func (vm *ValueManager) UnlockStripe(s uint8) { vm.shards[s].Unlock() }

// LockStripes acquires the given stripe indices in ascending order,
// deduplicating so each stripe is locked at most once. Returns the sorted,
// deduplicated stripe list to hand back to UnlockStripes. Deadlock-free by the
// same ascending-order argument as Manager.LockMany.
func (vm *ValueManager) LockStripes(stripes []uint8) []uint8 {
	ordered := uniqueSortedStripes(stripes)
	for _, s := range ordered {
		vm.shards[s].Lock()
	}
	return ordered
}

// LockStripesExcept acquires the given stripe indices in ascending order,
// deduplicating, but SKIPS any stripe already present in `held` (which the
// caller holds — a stripe mutex is not reentrant, so re-locking it would
// deadlock). Returns the sorted, deduplicated, non-held stripe list to hand back
// to UnlockStripes. When `held` is empty this is exactly LockStripes.
func (vm *ValueManager) LockStripesExcept(stripes, held []uint8) []uint8 {
	if len(held) == 0 {
		return vm.LockStripes(stripes)
	}
	skip := make(map[uint8]struct{}, len(held))
	for _, s := range held {
		skip[s] = struct{}{}
	}
	ordered := uniqueSortedStripes(stripes)
	kept := ordered[:0]
	for _, s := range ordered {
		if _, ok := skip[s]; ok {
			continue
		}
		vm.shards[s].Lock()
		kept = append(kept, s)
	}
	return kept
}

// UnlockStripes releases stripes (as returned by LockStripes) in reverse order.
func (vm *ValueManager) UnlockStripes(ordered []uint8) {
	for i := len(ordered) - 1; i >= 0; i-- {
		vm.shards[ordered[i]].Unlock()
	}
}

func uniqueSortedStripes(stripes []uint8) []uint8 {
	if len(stripes) == 0 {
		return nil
	}
	seen := make(map[uint8]struct{}, len(stripes))
	out := make([]uint8, 0, len(stripes))
	for _, s := range stripes {
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
