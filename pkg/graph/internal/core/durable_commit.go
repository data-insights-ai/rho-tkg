package core

import "fmt"

// flushDurableCommit makes a just-committed group durable under
// Config.DurableCommit: one store DurableFlush (pending buffer + WAL fsync on
// every open shard that took writes). No-op when the flag is off. Called by
// GraphTx.Commit and Batch.Execute after the group is applied and the graph
// locks are released, so readers and the next writer are not blocked behind the
// fsync; the flush still covers the group, whose operations were buffered
// before the locks were released. Never called on a rollback path.
//
// An error wraps ErrCommitNotDurable: the group is committed in memory (visible,
// change-log records minted, not rolled back) and its operations stay pending
// for the next flush.
func (c *Core) flushDurableCommit() error {
	if c.durableFlush == nil {
		return nil
	}
	if err := c.durableFlush.DurableFlush(); err != nil {
		return fmt.Errorf("%w: %w", ErrCommitNotDurable, err)
	}
	return nil
}

// holdGroupFlushes starts the store's flush hold for a durable group's
// mutations (c.flushHold: Config.DurableCommit on a store that flushes every
// mutation, i.e. Badger SyncWrites; backlog 52). The caller holds c.mu.Lock
// until releaseGroupFlushes, so no other mutation runs inside the hold. No-op
// without a hold.
func (c *Core) holdGroupFlushes() {
	if c.flushHold != nil {
		c.flushHold.HoldMutationFlushes()
	}
}

// releaseGroupFlushes ends the hold without flushing; the held writes stay
// pending for the group's durable flush. No-op without a hold.
func (c *Core) releaseGroupFlushes() {
	if c.flushHold != nil {
		c.flushHold.ReleaseMutationFlushes()
	}
}

// flushHeldGroup ends the hold and makes the held writes durable (a rollback's
// restores, which SyncWrites made durable before the hold). No-op without a
// hold.
func (c *Core) flushHeldGroup() error {
	if c.flushHold == nil {
		return nil
	}
	c.flushHold.ReleaseMutationFlushes()
	return c.durableFlush.DurableFlush()
}
