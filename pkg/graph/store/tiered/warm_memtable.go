package tiered

import (
	"fmt"

	badgerv4 "github.com/dgraph-io/badger/v4"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
)

// WARMMEM (R3, tasks/evidence/R3/measure.txt). A writable badger open
// allocates a memtable arena of about 1.3 x MemTableSize on the Go heap, and a
// shard that is not hot rarely fills it: a late write into a warm or cold
// shard, a backfill into a window shard made on demand and a restart that
// reopens the warm shards each paid 83.2 MiB at 64 MiB. Config.WarmMemTableSize
// sizes the memtable of a writable open in a non-hot role; every role-blind
// open (hot, reference, archive, owner index) keeps MemTableSize.

// validateWarmMemTableSize refuses a WarmMemTableSize outside MemTableSize's
// bounds, or above the hot memtable. The bound to the hot size keeps every
// read-only open at least as large as any writable one: a read-only open
// replays a leftover WAL into an arena of its own MemTableSize and refuses a
// WAL that does not fit (badger.ErrOversizedWAL), so a larger warm size could
// leave a WAL the read-only recovery probe cannot open.
func validateWarmMemTableSize(cfg Config) error {
	warm := cfg.WarmMemTableSize
	if warm == 0 {
		return nil
	}
	if err := badger.ValidateMemTableSize(warm); err != nil {
		return fmt.Errorf("graph: WarmMemTableSize %d out of range [8MB, 1GB]", warm)
	}
	hot := hotMemTableSize(cfg.MemTableSize)
	if warm > hot {
		return fmt.Errorf("graph: WarmMemTableSize %d above MemTableSize %d", warm, hot)
	}
	return nil
}

// hotMemTableSize is the effective MemTableSize of a role-blind open: the
// configured one, or badger's stock 64 MiB when that is 0. It is the largest
// memtable any shard opens with, and so the size a leftover WAL is replayed
// at (badgerCfg sets badger.Config.WALReplayMemTableSize to it; P0
// WALREPLAY).
func hotMemTableSize(memTableSize int64) int64 {
	if memTableSize == 0 {
		return badgerv4.DefaultOptions("").MemTableSize
	}
	return memTableSize
}

// coldMemTableSize is the memtable of a writable open of a shard that is cold
// at the open when WarmMemTableSize is 0 (R3-3 LATEWRITE, L1): an arena of
// 10.4 MiB instead of 83.2 MiB at 64 MiB, for a late write into a closed day
// or a cold window shard made on demand. badger's floor at its 1 MiB
// ValueThreshold (badgerstore_options.go minMemTableSize).
const coldMemTableSize = 8 << 20

// coldGrowBytes is what a small cold handle commits before the handle sweep
// closes it so the shard's next writable open takes MemTableSize (R3-3, the
// grow; tasks/evidence/R3/design-latewrite-fix.txt section 1): four 8 MiB
// memtables, which badger keeps in L0 unmerged (it compacts L0 at 5 tables,
// NumLevelZeroTables). A backfill into a cold day pays the small memtable (a
// flush per 8 MiB, a transaction limit of 1.2 MiB) for its first 32 MiB only;
// ai-soc's daily late write stays below it and keeps the small open.
const coldGrowBytes = 4 * coldMemTableSize

// coldGrowLimit is coldGrowBytes, or the lower limit a test set on this store
// (SetColdGrowBytesForTest), so a grow test runs in -short.
func (ts *Store) coldGrowLimit() int64 {
	if n := ts.coldGrowBytesForTest.Load(); n > 0 {
		return n
	}
	return coldGrowBytes
}

// coldGrowOwedLocked reports whether es's handle has outgrown the small cold
// memtable: WarmMemTableSize is 0 (an explicit one is the consumer's word and
// never grows), es is cold on a writable handle opened below the hot size, and
// that handle has committed coldGrowBytes. The handle sweep then closes it
// (sweepClose) and the next writable open (openColdLocked) takes MemTableSize.
// Caller holds es.shardMu.
func (ts *Store) coldGrowOwedLocked(es *EventShard) bool {
	if ts.warmMemTableSize != 0 || es.currentTier() != TierCold || es.store == nil || es.storeRO {
		return false
	}
	return es.store.CommittedBytes() >= ts.coldGrowLimit() && es.store.MemTableSize() < hotMemTableSize(ts.memTableSize)
}

// pendColdGrowLocked hands es to the handle sweep when its handle owes the
// grow; a writable checkout of a cold shard calls it (two atomic loads).
// Caller holds es.shardMu.
func (ts *Store) pendColdGrowLocked(es *EventShard) {
	if ts.coldGrowOwedLocked(es) {
		ts.sweepPend(es)
		ts.kickHandleSweep()
	}
}

// nonHotCfg sizes the memtable of a writable open in a non-hot role:
// WarmMemTableSize when set; else, for a disk shard cold at this open that
// does not grow, at most coldMemTableSize; else MemTableSize. In memory no
// sweep can grow the handle, so it keeps MemTableSize. A rotation restores
// a catalog-warm shard's warm-size handle before promotion, so a small cold
// handle never becomes the hot shard's. A small handle stays until IdleTimeout or the
// store's close (coldOpened), unless it commits coldGrowBytes: then the sweep
// closes it and the open after (grow, EventShard.growNext) takes MemTableSize.
// A read-only open allocates no memtable and keeps the hot size (see
// validateWarmMemTableSize), so every WAL a smaller handle leaves fits the
// read-only recovery probe.
func (ts *Store) nonHotCfg(cfg BadgerStoreConfig, small bool) BadgerStoreConfig {
	switch {
	case cfg.ReadOnly:
	case ts.warmMemTableSize > 0:
		cfg.MemTableSize = ts.warmMemTableSize
	case small && !ts.inMemory:
		cfg.MemTableSize = min(hotMemTableSize(cfg.MemTableSize), coldMemTableSize)
	}
	return cfg
}

// openNonHotBadgerStore is openBadgerStore for a shard made in a non-hot role
// (a window shard made on demand, window_shard.go); cold: the shard is cold
// at this open.
func (ts *Store) openNonHotBadgerStore(name string, cold bool) (*BadgerStore, error) {
	if err := ts.requireVolume(name); err != nil {
		return nil, err
	}
	return ts.newShardStore(ts.nonHotCfg(ts.badgerCfg(name, false), cold))
}
