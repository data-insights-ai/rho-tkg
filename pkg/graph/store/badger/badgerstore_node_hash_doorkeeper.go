package badger

import "github.com/data-insights-ai/rho-tkg/v4/pkg/types"

// nodeHashDoorBits is the doorkeeper's size (HASHFILL): 1 << 20 bits, 128 KiB
// once a store's first hash miss allocates it.
const nodeHashDoorBits = 1 << 20

// nodeHashDoor admits a node's hash into the cache on its second lookup, so an
// ID looked up once (an event: an endpoint once, TestNodeHashCapHitCounts)
// fills nothing. Two bits per ID; an ID whose bits are both set was seen
// before (or collides: a fill, never a wrong hash, as the cache only skips a
// recompute). After nodeHashLimit() IDs it clears, so it never saturates:
// with k = 2 a fresh ID passes it about 1.4 % of the time when full. Guarded
// by idxMu, as the cache.
type nodeHashDoor struct {
	bits []uint64
	sets int
}

// admit reports whether nid was seen since the last clear, marking it seen.
func (d *nodeHashDoor) admit(nid types.NodeID, limit int) bool {
	if d.bits == nil {
		d.bits = make([]uint64, nodeHashDoorBits/64)
	}
	h := uint64(nid) * 0x9e3779b97f4a7c15 // #nosec G115 -- a hash of the ID's bits
	h ^= h >> 31
	a, b := h%nodeHashDoorBits, (h>>32)%nodeHashDoorBits
	if d.bits[a/64]&(1<<(a%64)) != 0 && d.bits[b/64]&(1<<(b%64)) != 0 {
		return true
	}
	if d.sets++; d.sets > limit {
		clear(d.bits)
		d.sets = 1
	}
	d.bits[a/64] |= 1 << (a % 64)
	d.bits[b/64] |= 1 << (b % 64)
	return false
}

// bytes is the doorkeeper's RAM (MemoryAccount NodeHashes).
func (d *nodeHashDoor) bytes() int64 { return int64(len(d.bits)) * 8 }

// reset drops what the doorkeeper saw (Clear).
func (d *nodeHashDoor) reset() { clear(d.bits); d.sets = 0 }
