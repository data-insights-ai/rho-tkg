package tiered

import (
	"errors"
	"fmt"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// DurableFlushSupported implements store.DurableFlushCapability: false for an
// InMemory tiered store (its shards have no write-ahead log).
func (ts *Store) DurableFlushSupported() bool {
	return ts != nil && !ts.inMemory
}

// DurableFlush implements store.DurableFlushCapability (Config.DurableCommit):
// it runs BadgerStore.DurableFlush on the reference shard, the archive when it
// is open, and every event shard whose store is open — the shards a commit
// group can have written to. Unlike Flush it never lazy-opens a closed cold
// shard: a closed shard has nothing pending (its close flushed it), and opening
// historical shards on every commit would cost one Badger open each. A shard
// with nothing written since its last sync is not fsynced again. Every shard is
// attempted; the errors are joined, and a failed shard keeps its pending
// operations for the next flush.
func (ts *Store) DurableFlush() error {
	if err := ts.checkOpen(); err != nil {
		return err
	}
	if ts.inMemory {
		return fmt.Errorf("graph: tiered: durable flush of an in-memory store: %w", storecontract.ErrCapabilityNotSupported)
	}
	var errs []error
	ref, refCheckin, err := ts.checkoutRefShard()
	if err != nil {
		return err
	}
	if err := ref.DurableFlush(); err != nil {
		errs = append(errs, fmt.Errorf("graph: tiered: durable flush reference shard: %w", err))
	}
	refCheckin()
	if ts.refArchive.Load() != nil { // open archive only; never open one for a flush
		archive, archiveCheckin, err := ts.checkoutArchive()
		if err != nil {
			errs = append(errs, err)
		} else if archive != nil {
			if err := archive.DurableFlush(); err != nil {
				errs = append(errs, fmt.Errorf("graph: tiered: durable flush archive shard: %w", err))
			}
		}
		archiveCheckin()
	}
	ts.mu.RLock()
	shards := ts.eventShardSnapshot(DepthAll)
	ts.mu.RUnlock()
	for _, es := range shards {
		store, release, open, err := es.checkoutOpenStoreForRead(ts)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if !open {
			continue
		}
		if err := store.DurableFlush(); err != nil {
			errs = append(errs, fmt.Errorf("graph: tiered: durable flush shard %s: %w", es.name, err))
		}
		release()
	}
	return errors.Join(errs...)
}
