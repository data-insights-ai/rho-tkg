package tiered

import storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"

var _ storecontract.MutationFlushHoldCapability = (*Store)(nil)

// FlushesEveryMutation implements store.MutationFlushHoldCapability: true when
// every writable shard flushes and syncs each mutation (Config.SyncWrites).
func (ts *Store) FlushesEveryMutation() bool {
	return ts != nil && ts.syncWrites
}

// HoldMutationFlushes implements store.MutationFlushHoldCapability
// (graph Config.DurableCommit under SyncWrites): it holds the per-mutation
// flush on every shard — reference, archive, every event shard, and any shard
// opened or created before ReleaseMutationFlushes — through the one flag
// badgerCfg shares with them (badger.Config.SharedFlushHold). Each shard still
// flushes at its own hold bound; the group's DurableFlush syncs the rest.
func (ts *Store) HoldMutationFlushes() {
	ts.flushHold.Store(true)
}

// ReleaseMutationFlushes implements store.MutationFlushHoldCapability: it ends
// the hold on every shard without flushing; the held writes stay pending for
// the group's DurableFlush or the next flush.
func (ts *Store) ReleaseMutationFlushes() {
	ts.flushHold.Store(false)
}
