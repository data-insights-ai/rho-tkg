package core

import (
	eventspkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/events"
)

// runUnderRLock acquires c.mu for read, runs fn under a defer-backed
// RUnlock, and returns the event publisher captured under the lock plus
// any closed-state error.
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
//
// Closed-state gate (R4-F3): if Close has set c.closed, the caller
// receives ErrGraphClosed and fn never runs. This is checked BOTH after
// acquiring the RLock (so we observe the state Close set immediately
// before its Lock acquisition drained readers) AND would be checked
// before fn runs. The Close path sets c.closed before taking c.mu.Lock,
// so any RLock acquired after Close.Lock is released sees closed=true.
func (c *Core) runUnderRLock(fn func()) (eventspkg.Publisher, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed.Load() {
		return c.events, ErrGraphClosed
	}
	fn()
	return c.events, nil
}
