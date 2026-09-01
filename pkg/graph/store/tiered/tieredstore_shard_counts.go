package tiered

import (
	"fmt"
	"log/slog"
	"sort"
	"sync/atomic"
)

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
//
// It also SEALS the counts in the catalog, which is what makes them survive the
// process. A shard's counts are fixed the moment it closes — nothing can write
// to it without opening it first — so recomputing them on the next start is
// work with a known answer. Returns true if the catalog was changed and needs
// saving; the caller saves, because saving under this lock would hold it across
// disk I/O.
func (es *EventShard) snapshotCountsLocked(ts *Store) bool {
	if es.store == nil {
		return false
	}
	nodes, rels, byLabel, byRelType, err := es.store.CountSnapshot()
	if err != nil {
		// The store is already unusable; leaving the previous snapshot in
		// place is better than clearing it, and a wrong count is not worth a
		// failed close.
		return false
	}
	es.counts.Store(&shardCounts{
		nodes:     nodes,
		rels:      rels,
		byLabel:   byLabel,
		byRelType: byRelType,
	})
	return ts.catalog.SealShardCounts(es.name, nodes, rels, byLabel, byRelType)
}

// adoptSealedCounts loads a shard's counts from the catalog, so a shard that has
// not been opened in this process can still be counted.
//
// This is the point of sealing: without it the in-memory snapshot starts empty
// at every boot, and the first fold reopens every cold shard to rebuild numbers
// that were settled when the shard closed.
func (es *EventShard) adoptSealedCounts(entry ShardEntry) {
	if !entry.CountsSealed {
		return
	}
	es.counts.Store(&shardCounts{
		nodes:     entry.ApproxNodes,
		rels:      entry.ApproxRels,
		byLabel:   tokenCountsFromJSON(entry.NodeCountsByLabel),
		byRelType: tokenCountsFromJSON(entry.RelCountsByType),
	})
}

// dropCachedCounts forgets the snapshot, in memory and in the catalog. Called
// when a shard is opened, since from then on the store itself is the answer and
// the copy may diverge from it.
//
// Unsealing matters as much as sealing: a shard that is open can be written to,
// so a sealed count left behind would outlive its truth and be adopted as exact
// by the next start. Returns true if the catalog changed.
func (es *EventShard) dropCachedCounts(ts *Store) bool {
	es.counts.Store(nil)
	if ts == nil {
		return false
	}
	return ts.catalog.UnsealShardCounts(es.name)
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

// enforceColdShardCap closes the least recently used open cold shards until no
// more than maxOpenColdShards remain open.
//
// This is what lets a cold shard STAY open after a read. The alternative — the
// behaviour this replaces — was to close it the moment its read finished, which
// bounded handles but made every read of the same old data reopen the same
// shards: 16.9s to open an old case, then 7.5s again on the next read.
//
// Shards with a reader in flight are never closed, and their counts are sealed
// before closing so a shard evicted here can still be counted without reopening.
func (ts *Store) enforceColdShardCap() {
	cap := ts.maxOpenColdShards
	if cap <= 0 {
		return
	}

	ts.mu.RLock()
	open := make([]*EventShard, 0, len(ts.eventShards))
	for _, es := range ts.eventShards {
		if es.currentTier() != TierCold {
			continue
		}
		es.shardMu.Lock()
		hasStore := es.store != nil
		es.shardMu.Unlock()
		if hasStore {
			open = append(open, es)
		}
	}
	ts.mu.RUnlock()

	if len(open) <= cap {
		return
	}

	// Oldest access first: those are the ones least likely to be wanted next.
	sort.Slice(open, func(i, j int) bool {
		return open[i].lastAccess.Load() < open[j].lastAccess.Load()
	})

	sealed := false
	for _, es := range open[:len(open)-cap] {
		es.shardMu.Lock()
		// A shard being read right now is not a candidate however old its last
		// access looks: closing it under a reader is the use-after-close this
		// whole lifecycle is built to avoid.
		if es.store != nil && es.activeReqs.Load() == 0 {
			sealed = es.snapshotCountsLocked(ts) || sealed
			if err := es.store.Close(); err != nil {
				ts.recordBackgroundError(fmt.Errorf("graph: cap-close cold shard %s: %w", es.name, err))
				slog.Error("tiered cold shard cap-close failed", "shard", es.name, "err", err)
			}
			es.store = nil
			es.readTransientOpen = false
			ts.openColdShards.Add(-1)
		}
		es.shardMu.Unlock()
	}
	if sealed {
		ts.saveCatalogBestEffort("seal shard counts on cap-close")
	}
}
