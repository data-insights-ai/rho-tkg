package badger

// FlushesEveryMutation implements store.MutationFlushHoldCapability: true when
// every mutation flushes (and Badger syncs) synchronously, i.e. SyncWrites.
func (bs *Store) FlushesEveryMutation() bool {
	return bs != nil && bs.syncWrites
}

// HoldMutationFlushes implements store.MutationFlushHoldCapability
// (Config.DurableCommit under SyncWrites, backlog 52): until
// ReleaseMutationFlushes, a mutation's flushIfNeeded leaves its writes pending
// unless the pending buffer has reached the hold bound (Config.MaxPendingWrites,
// DefaultMaxPendingWrites when 0, unbounded when negative). The caller holds
// the graph's exclusive write lock and makes the group durable with
// DurableFlush; every WriteBatch that commits it is synced by Badger
// (SyncWrites), a memtable or value-log switch included.
func (bs *Store) HoldMutationFlushes() {
	bs.flushHold.Store(true)
}

// ReleaseMutationFlushes implements store.MutationFlushHoldCapability: it ends
// the hold without flushing; the held writes stay pending for the group's
// DurableFlush or the next flush.
func (bs *Store) ReleaseMutationFlushes() {
	bs.flushHold.Store(false)
}

// holdFull reports whether a held buffer reached the hold bound.
func (bs *Store) holdFull() bool {
	return bs.holdMaxPending > 0 && bs.pendingLen() >= bs.holdMaxPending
}
