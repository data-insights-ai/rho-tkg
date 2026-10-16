//go:build unix

package graph

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// lockFileName is the engine's own lock file inside Config.Dir. It is not part
// of the store: a directory holding only this file is a new graph.
const lockFileName = "rho-v5.lock"

// dirLock is an exclusive flock on Dir/lockFileName. flock locks the open file
// description, so a second Open conflicts in the same process and in another.
type dirLock struct{ f *os.File }

// lockDir creates dir if needed and takes the exclusive lock without blocking;
// a held lock is ErrLocked.
func lockDir(dir string) (*dirLock, error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("%w: cannot create the directory", ErrInvalidConfig)
	}
	f, err := os.OpenFile(filepath.Join(dir, lockFileName), os.O_CREATE|os.O_RDWR, 0o600) // #nosec G304 -- Config.Dir is the caller's store.
	if err != nil {
		return nil, fmt.Errorf("%w: cannot open the lock file", ErrInvalidConfig)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil { // #nosec G115 -- a file descriptor fits int.
		_ = f.Close()
		if err == syscall.EWOULDBLOCK {
			return nil, ErrLocked
		}
		return nil, fmt.Errorf("%w: cannot lock the directory", ErrInvalidConfig)
	}
	return &dirLock{f}, nil
}

// unlock releases the lock; closing the descriptor drops the flock.
func (l *dirLock) unlock() {
	if l != nil && l.f != nil {
		_ = l.f.Close()
		l.f = nil
	}
}
