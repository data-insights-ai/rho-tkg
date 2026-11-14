package badger

import (
	"errors"

	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// A metered lazy open may restore persistent index state as part of its
// charged shard open. WAL replay and rebuilds from entity rows have no native
// read tally, so they must wait for a normal writable open to finish repair.
func guardReadOpenWAL(cfg Config) error {
	if cfg.OpenReadTally == nil {
		return nil
	}
	if err := cfg.OpenReadTally.Exceeded(); err != nil {
		return err
	}
	if cfg.InMemory {
		return cfg.OpenReadTally.RefuseUnsupported()
	}
	n, err := maxWALFileSize(cfg.Dir)
	if err != nil {
		return err
	}
	if n != 0 {
		return cfg.OpenReadTally.RefuseUnsupported()
	}
	return nil
}

// Check only for a marker or the first staging key. In particular, do not
// enumerate abandoned staging just to find that this open cannot clean it.
func guardReadOpenRecovery(db *badgerv4.DB, tally *storecontract.ReadTally) error {
	if tally == nil {
		return nil
	}
	return db.View(func(txn *badgerv4.Txn) error {
		_, err := txn.Get(storeutil.FlushStageMarkerKey)
		if err == nil {
			return tally.RefuseUnsupported()
		}
		if !errors.Is(err, badgerv4.ErrKeyNotFound) {
			return err
		}
		it := txn.NewIterator(badgerv4.IteratorOptions{Prefix: []byte{storeutil.KeyFlushStage}, PrefetchValues: false})
		defer it.Close()
		it.Rewind()
		if it.Valid() {
			return tally.RefuseUnsupported()
		}
		return nil
	})
}
