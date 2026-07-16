package core

import (
	eventspkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/events"
)

// checkOpen returns ErrGraphClosed if the graph has been closed. Every
// public sub-API entry point — read, write, admin, IO, stats, hash,
// constraints, index management, tx/batch begin — calls this before
// touching the store, registries, or indexes. Routing through one
// primitive guarantees the same sentinel behavior across all
// surfaces.
func (c *Core) checkOpen() error {
	if c.closed.Load() {
		return ErrGraphClosed
	}
	return nil
}

// checkWritable is the gate for every USER mutation door (node/rel create,
// update, delete, label/property mutation, version-interval edits, index/schema
// changes, tx begin, batch execute, admin reset). It is checkOpen plus the
// read-only-replica guard: a replica accepts changes ONLY through the replica
// apply path (ApplyChange, which calls store doors directly and does NOT route
// through this gate), so direct user writes are rejected with ErrReadOnlyReplica
// to keep the replica byte-identical to its primary. Read paths use checkOpen,
// never this; the bootstrap importer (IOOps.Import) also uses checkOpen so a
// fresh replica can be seeded.
func (c *Core) checkWritable() error {
	if c.closed.Load() {
		return ErrGraphClosed
	}
	if c.readOnlyReplica {
		return ErrReadOnlyReplica
	}
	return nil
}

// runUnderRLock acquires c.mu for read, runs fn under a defer-backed
// RUnlock, and returns the event publisher captured under the lock plus
// any closed-state error. Mutation entry points use this so dispatchEvent
// can run AFTER the lock is released (event handlers may re-enter the
// graph and would deadlock under the lock window).
func (c *Core) runUnderRLock(fn func()) (eventspkg.Publisher, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed.Load() {
		return c.events, ErrGraphClosed
	}
	fn()
	return c.events, nil
}

// runUnderRLockShard is runUnderRLock keyed to a reader stripe (ADR-0007
// lever #1). The concurrent-ingest apply path passes its session lane so N
// lanes fan out across N stripes instead of contending on stripe 0. Semantics
// are otherwise identical to runUnderRLock: a writer (Lock) still excludes this
// reader, the closed check is the same, and events are dispatched by the caller
// AFTER the returned publisher is captured under the lock.
func (c *Core) runUnderRLockShard(hint uint, fn func()) (eventspkg.Publisher, error) {
	tok := c.mu.RLockShard(hint)
	defer c.mu.RUnlockShard(tok)
	if c.closed.Load() {
		return c.events, ErrGraphClosed
	}
	fn()
	return c.events, nil
}

// readUnderRLock runs an operation that does not require exclusive graph access
// under c.mu.RLock and returns ErrGraphClosed if Close has set the lifecycle
// flag before the lock is acquired. Public read/query paths and small
// registry-mutating helpers use this so Close drains them before closing the
// underlying store or persisting registries.
func (c *Core) readUnderRLock(fn func() error) error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed.Load() {
		return ErrGraphClosed
	}
	return fn()
}
