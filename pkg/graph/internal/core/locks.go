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

// readUnderRLock runs an operation that does not require exclusive graph access
// under c.mu.RLock and returns ErrGraphClosed if Close has set the lifecycle
// flag before the lock is acquired. Public read/query paths and small
// registry-mutating helpers use this so Close drains them before closing the
// underlying store or persisting registries.
func (c *Core) readUnderRLock(fn func() error) error {
	var err error
	_, closeErr := c.runUnderRLock(func() {
		err = fn()
	})
	if closeErr != nil {
		return closeErr
	}
	return err
}
