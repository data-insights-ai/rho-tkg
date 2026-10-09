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
