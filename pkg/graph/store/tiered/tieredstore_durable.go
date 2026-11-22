package tiered

import (
	"errors"
	"fmt"
	"sync"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// DurableFlushSupported implements store.DurableFlushCapability: false for an
// InMemory tiered store (its shards have no write-ahead log).
func (ts *Store) DurableFlushSupported() bool {
	return ts != nil && !ts.inMemory
}

// DurableFlush implements store.DurableFlushCapability (Config.DurableCommit):
// it runs BadgerStore.DurableFlush on every event shard whose store is open,
// the archive when it is open, and the reference shard last — the shards a
// commit group can have written to. A durable reference row implies that every
// row buffered before it on another shard is durable
// (flushRowShardsBeforeReference holds this on every reference flush). Unlike
// Flush it never lazy-opens a closed cold shard: a closed shard has nothing
// pending (its close flushed it), and opening historical shards on every
// commit would cost one Badger open each. A shard with nothing written since
// its last sync is not fsynced again. Every shard is attempted; the errors are
// joined, and a failed shard keeps its pending operations for the next flush.
func (ts *Store) DurableFlush() error {
	if err := ts.checkOpen(); err != nil {
		return err
	}
	if ts.inMemory {
		return fmt.Errorf("graph: tiered: durable flush of an in-memory store: %w", storecontract.ErrCapabilityNotSupported)
	}
	var errs []error
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
	ref, refCheckin, err := ts.checkoutRefShard()
	if err != nil {
		return errors.Join(append(errs, err)...)
	}
	if err := ref.DurableFlush(); err != nil {
		errs = append(errs, fmt.Errorf("graph: tiered: durable flush reference shard: %w", err))
	}
	refCheckin()
	return errors.Join(errs...)
}

// rowWriterSet holds the open writable non-reference shards (event shards and
// the archive): newShardStore adds each, its Close removes it after its final
// flush (badger.Config.OnClose, rowWriterClosed). lost is sticky: a shard
// whose final flush failed dropped rows the reference shard may name, so every
// later reference flush is refused until RecoverBackgroundError. A leaf lock:
// no shard or store lock is taken under it.
type rowWriterSet struct {
	mu   sync.Mutex
	open map[*BadgerStore]chan struct{} // closed by remove
	lost error
}

func (w *rowWriterSet) add(s *BadgerStore) {
	w.mu.Lock()
	if w.open == nil {
		w.open = make(map[*BadgerStore]chan struct{})
	}
	w.open[s] = make(chan struct{})
	w.mu.Unlock()
}

// lostErr is the sticky loss, nil when none.
func (w *rowWriterSet) lostErr() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.lost
}

// rowWriterClosed is every row writer's badger.Config.OnClose: it drops s from
// rowWriters and, when its final flush failed, records the loss.
func (ts *Store) rowWriterClosed(s *BadgerStore, finalFlushErr error) {
	var lost error
	if finalFlushErr != nil {
		lost = fmt.Errorf("graph: tiered: a shard closed with unflushed rows, reference flushes refused: %w", finalFlushErr)
	}
	w := &ts.rowWriters
	w.mu.Lock()
	if done, ok := w.open[s]; ok {
		close(done)
		delete(w.open, s)
	}
	if lost != nil {
		w.lost = errors.Join(w.lost, lost)
	}
	w.mu.Unlock()
	if lost != nil {
		ts.recordBackgroundError(lost)
	}
}

// flushRowShardsBeforeReference is the reference shard's
// badger.Config.BeforeFlushRows. It syncs the owner index, then flushes every
// open writable non-reference shard, durably when the reference rows will be
// synced (durable). So every reference flush — a SyncWrites mutation, a hold
// bound, DurableFlush, Flush, a durable meta write, Close — writes its rows
// only after the rows buffered before them on other shards: a reference row
// (an ai-soc cut record) is never durable while rows it names are still in
// RAM. A shard that is closing is waited for until its Close's final flush has
// run; a failed one leaves rowWriters.lost set. An error fails the reference
// flush, which keeps its rows pending.
//
// Lock order: the reference shard's flushMu, then each row shard's flushMu;
// no row shard's flush, Close or hook takes the reference shard's flushMu (its
// writes to the reference shard — commit floor, change-log watermark,
// property-key registry — are direct meta writes).
func (ts *Store) flushRowShardsBeforeReference(durable bool) error {
	if err := ts.syncOwnerIndexBeforeRows(durable); err != nil {
		return err
	}
	ts.rowWriters.mu.Lock()
	stores := make([]*BadgerStore, 0, len(ts.rowWriters.open))
	dones := make([]chan struct{}, 0, len(ts.rowWriters.open))
	for s, done := range ts.rowWriters.open {
		stores = append(stores, s)
		dones = append(dones, done)
	}
	ts.rowWriters.mu.Unlock()
	var errs []error
	for i, s := range stores {
		var err error
		if durable && !ts.inMemory {
			err = s.DurableFlush()
		} else {
			err = s.Flush()
		}
		if err != nil && s.Closing() {
			<-dones[i]
			continue
		}
		if err != nil {
			errs = append(errs, err)
		}
	}
	if err := ts.rowWriters.lostErr(); err != nil {
		errs = append(errs, err)
	}
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("graph: tiered: flush shards before the reference shard: %w", err)
	}
	return nil
}
