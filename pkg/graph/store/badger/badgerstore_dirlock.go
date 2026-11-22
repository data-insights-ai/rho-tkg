package badger

import (
	"strings"
	"time"

	badgerv4 "github.com/dgraph-io/badger/v4"
)

// Badger locks a directory with flock on a descriptor of it and releases the
// lock by closing that descriptor, without LOCK_UN (dir_unix.go). A process
// forked from this one while the handle is open (os/exec: any command the
// embedding program or a test runs) holds a copy of that descriptor until its
// exec closes it, and with it the lock: a reopen of the directory right after
// the Close is refused "Cannot acquire directory lock" though no other handle
// is open. Under load the copy lives some milliseconds (TAGFLAKES: the tiered
// tests that fork crash children; 145 of 300 close-reopens refused on Linux
// with eight goroutines running /bin/true). OpenDB waits that out.

// dirLockWait bounds how long OpenDB retries a refused directory lock. A copy
// held by a forked child lasts until its exec; a lock held by another open
// handle or process is refused again after the wait.
const dirLockWait = time.Second

// dirLockPause sleeps between the attempts; a test replaces it to release the
// lock it holds instead.
var dirLockPause = time.Sleep

// OpenDB is badgerv4.Open, retried while the directory lock is refused, up to
// dirLockWait. Every badger open of rho goes through it.
func OpenDB(opts badgerv4.Options) (*badgerv4.DB, error) {
	deadline := time.Now().Add(dirLockWait)
	pause := time.Millisecond
	for {
		db, err := badgerv4.Open(opts)
		if err == nil || !dirLockRefused(err) || time.Now().After(deadline) {
			return db, err
		}
		dirLockPause(pause)
		pause = min(2*pause, 50*time.Millisecond)
	}
}

// dirLockRefused reports badger's refusal of the directory lock. Badger
// formats the flock error into its text (y.Wrapf without %w), so the text is
// all there is to match.
func dirLockRefused(err error) bool {
	return strings.Contains(err.Error(), "Cannot acquire directory lock")
}
