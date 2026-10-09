// Package raftlog is a provisional durable log adapter, not a graph store.
// All APIs are internal; consensus and VFS dependencies do not enter public APIs.
package raftlog

import "errors"

var (
	// ErrInvalid marks rejected API input.
	ErrInvalid = errors.New("raftlog: invalid input")
	// ErrCorrupt marks invalid verified durable bytes or ledger.
	ErrCorrupt = errors.New("raftlog: corrupt durable state")
	// ErrLimit marks a finite adapter resource budget exceeded.
	ErrLimit = errors.New("raftlog: resource limit")
	// ErrClosed marks a released store.
	ErrClosed = errors.New("raftlog: closed")
	// ErrPoisoned requires recovery after an uncertain recoverable storage error.
	ErrPoisoned = errors.New("raftlog: uncertain storage failure; reopen required")
)
