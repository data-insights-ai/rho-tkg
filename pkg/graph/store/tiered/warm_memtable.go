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
// reopens the warm shards each paid 83 MB at 64 MB. Config.WarmMemTableSize
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
	hot := cfg.MemTableSize
	if hot == 0 {
		hot = badgerv4.DefaultOptions("").MemTableSize
	}
	if warm > hot {
		return fmt.Errorf("graph: WarmMemTableSize %d above MemTableSize %d", warm, hot)
	}
	return nil
}

// nonHotCfg sizes the memtable of a writable open in a non-hot role. A
// read-only open allocates no memtable and keeps the hot size (see
// validateWarmMemTableSize).
func (ts *Store) nonHotCfg(cfg BadgerStoreConfig) BadgerStoreConfig {
	if !cfg.ReadOnly && ts.warmMemTableSize > 0 {
		cfg.MemTableSize = ts.warmMemTableSize
	}
	return cfg
}

// openNonHotBadgerStore is openBadgerStore for a shard made in a non-hot role
// (a window shard made on demand, window_shard.go).
func (ts *Store) openNonHotBadgerStore(name string) (*BadgerStore, error) {
	if err := ts.requireVolume(name); err != nil {
		return nil, err
	}
	return ts.newShardStore(ts.nonHotCfg(ts.badgerCfg(name, false)))
}
