package core

import (
	eventspkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/events"
)

// runUnderRLock acquires c.mu for read, runs fn under a defer-backed
// RUnlock, and returns the event publisher captured under the lock.
//
// Public mutation entry points capture c.events under the lock so that
// dispatchEvent can run AFTER the lock is released (event handlers may
// call back into the graph and would deadlock if invoked inside the
// lock window). Wrapping the lock + capture inside this helper makes
// every entry point panic-safe: a panic from a custom Store or any
// other downstream call unwinds through fn, the defer-backed RUnlock
// fires, and the panic continues to the caller's recover boundary.
//
// The previous explicit `c.mu.RUnlock()` after the inline call was not
// panic-safe — the unlock never ran on the panic path, leaking the
// read lock for the rest of the process and deadlocking every later
// writer (including Close, which takes a write lock to tear down
// index providers).
func (c *Core) runUnderRLock(fn func()) eventspkg.Publisher {
	c.mu.RLock()
	defer c.mu.RUnlock()
	fn()
	return c.events
}
