package badger

import (
	"fmt"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// DurableFlushSupported implements store.DurableFlushCapability: a disk store
// can make writes durable, an InMemory store has no write-ahead log.
func (bs *Store) DurableFlushSupported() bool {
	return bs != nil && !bs.inMemory
}

// DurableFlush implements store.DurableFlushCapability (Config.DurableCommit):
// it commits the whole pending write buffer through the normal flush (one
// WriteBatch, counters and change-log records included; Badger splits a batch
// above its transaction size limit) and then fsyncs the write-ahead log when
// anything was written without an fsync since the last sync — by this call or
// by an earlier background or backpressure flush. Under SyncWrites Badger
// already fsynced every batch, so no second sync is issued.
//
// flushMu is held across the flush AND the sync: Close's final flush takes it
// too, so the DB cannot be closed under the sync. On a flush error the
// operations are requeued by flushIndexLocked (nothing is dropped); on a sync
// error the store stays marked unsynced so the next DurableFlush retries it.
// An InMemory store declines with ErrCapabilityNotSupported (Badger's in-memory
// mode has no WAL; db.Sync would dereference a nil file).
func (bs *Store) DurableFlush() error {
	if err := bs.checkOpen(); err != nil {
		return err
	}
	if bs.testHookDurableAfterCheckOpen != nil {
		bs.testHookDurableAfterCheckOpen()
	}
	if bs.inMemory {
		return fmt.Errorf("graph: durable flush of an in-memory store: %w", storecontract.ErrCapabilityNotSupported)
	}
	bs.flushMu.Lock()
	defer bs.flushMu.Unlock()
	bs.idxMu.RLock()
	if err := bs.flushIndexLocked(false); err != nil {
		return err
	}
	if !bs.unsynced.Swap(false) {
		return nil
	}
	// No dbClosed re-check: checkOpen passed and Close marks dbClosed only
	// after its own final flush, which waits for the flushMu held here.
	if injected := bs.failNextSync.Swap(nil); injected != nil { // test seam
		bs.unsynced.Store(true)
		return fmt.Errorf("graph: durable flush: sync write-ahead log: %w", *injected)
	}
	if err := bs.db.Sync(); err != nil {
		bs.unsynced.Store(true)
		return fmt.Errorf("graph: durable flush: sync write-ahead log: %w", err)
	}
	bs.durableSyncs.Add(1)
	return nil
}
