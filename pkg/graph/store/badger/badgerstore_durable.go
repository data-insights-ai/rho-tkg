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
// flushMu is held across the flush AND the sync, and the closed state is
// checked under it: Close's final flush takes flushMu too, so the DB cannot be
// closed under the sync. On a flush error the operations are requeued by
// flushIndexLocked (nothing is dropped); on a sync error the store stays
// marked unsynced so the next DurableFlush retries it.
//
// Power-loss limit: db.Sync fsyncs the ACTIVE memtable's WAL and the current
// value-log file only. When a flush fills the memtable, Badger switches to a
// new one without syncing the retired WAL, and a finished value-log file is
// synced only under SyncWrites — so rows of a group that crossed such a switch
// can still be unsynced after DurableFlush returns. A process crash cannot lose
// them (they are in the OS page cache); a power loss can. SyncWrites closes it.
//
// An InMemory store declines with ErrCapabilityNotSupported: it has nothing on
// disk to make durable (Badger's own Sync is a no-op there), and the flag
// promises durability.
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
	// Re-check under flushMu: a Close that started after checkOpen above may
	// already have run its final flush and closed the DB (db.Sync would then
	// dereference the dropped memtable). Close sets closing before its final
	// flush, and that flush needs flushMu, so once this check passes under the
	// lock the DB stays open until the lock is released.
	if bs.closing.Load() || bs.dbClosed.Load() {
		return ErrStoreClosed
	}
	bs.idxMu.RLock()
	if err := bs.flushIndexLocked(false); err != nil {
		return err
	}
	if !bs.unsynced.Swap(false) {
		return nil
	}
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
