package core

import (
	eventspkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/events"
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

// runUnderLock is the write-lock counterpart of runUnderRLock. Used by
// admin operations that mutate live shard topology, registries, or
// the close-gated provider map. Returns ErrGraphClosed if the graph
// has been closed before fn runs.
func (c *Core) runUnderLock(fn func()) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed.Load() {
		return ErrGraphClosed
	}
	fn()
	return nil
}

// readUnderRLock is the read-only counterpart for query/inspection
// paths that need a consistent view but no event dispatch. Returns
// ErrGraphClosed if the graph has been closed before fn runs.
func (c *Core) readUnderRLock(fn func()) error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed.Load() {
		return ErrGraphClosed
	}
	fn()
	return nil
}
