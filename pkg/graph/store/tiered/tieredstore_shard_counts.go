package tiered

import "sync/atomic"

// shardCounts is what a shard contained when it was last closed.
//
// Counting is the one question a caller can ask about a shard that does not
// need the shard's data — only a handful of numbers the open store already
// keeps as atomics. Without this, every count fold OPENED every closed cold
// shard to read them, and did so per label: measured on a real 26-shard store
// with 19 cold, AllLabelCounts took 22.4s against 0.000s when the same shards
// were open, because each of the eight labels reopened all nineteen.
//
// Immutable by construction: a closed shard cannot change, because every write
// path opens it first. The snapshot is taken at the moment of closing, so it
// describes the shard's final state, and it is dropped when the shard is opened
// again so a reopened shard is never answered from a stale copy.
type shardCounts struct {
	nodes     int
	rels      int
	byLabel   map[uint16]int
	byRelType map[uint16]int
}

// cachedCounts returns the counts recorded when this shard was last closed, or
// nil if none were recorded (it has never been closed, or is currently open).
func (es *EventShard) cachedCounts() *shardCounts {
	return es.counts.Load()
}

// snapshotCountsLocked records what the shard contains, for answering count
// queries after it is closed. Caller must hold es.shardMu and es.store must be
// non-nil; call it immediately before closing, so nothing can change between
// the snapshot and the close.
func (es *EventShard) snapshotCountsLocked() {
	if es.store == nil {
		return
	}
	nodes, rels, byLabel, byRelType, err := es.store.CountSnapshot()
	if err != nil {
		// The store is already unusable; leaving the previous snapshot in
		// place is better than clearing it, and a wrong count is not worth a
		// failed close.
		return
	}
	es.counts.Store(&shardCounts{
		nodes:     nodes,
		rels:      rels,
		byLabel:   byLabel,
		byRelType: byRelType,
	})
}

// dropCachedCounts forgets the snapshot. Called when a shard is opened, since
// from then on the store itself is the answer and may diverge from the copy.
func (es *EventShard) dropCachedCounts() {
	es.counts.Store(nil)
}

// countFromShard answers one count for one shard without opening it when it is
// closed and a snapshot exists.
//
// pick reads the number out of a snapshot; live reads it from an open store.
// Returns ok=false only when the shard is closed AND has no snapshot, which
// leaves the caller to open it as before.
func (es *EventShard) countFromCache(pick func(*shardCounts) int) (int, bool) {
	if c := es.cachedCounts(); c != nil {
		return pick(c), true
	}
	return 0, false
}

// countsCacheField is declared on EventShard; see tieredstore.go.
var _ = atomic.Pointer[shardCounts]{}
