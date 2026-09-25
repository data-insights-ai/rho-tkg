//go:build unix

package segdir

import (
	"errors"
	"fmt"
	"math"
	"os"
	"syscall"
)

// lockHandle holds the directory's LOCK file open with an exclusive flock.
// The kernel drops the lock when the process dies, so a crash never leaves
// the directory locked.
type lockHandle struct{ f *os.File }

func lockDir(path string) (lockHandle, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600) // #nosec G304 -- the configured segment directory's lock file
	if err != nil {
		return lockHandle{}, fmt.Errorf("segdir: open %s: %w", path, err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil { // #nosec G115 -- a file descriptor
		_ = f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return lockHandle{}, fmt.Errorf("%w: %s", ErrLocked, path)
		}
		return lockHandle{}, fmt.Errorf("segdir: lock %s: %w", path, err)
	}
	return lockHandle{f: f}, nil
}

func (l lockHandle) release() {
	if l.f != nil {
		_ = l.f.Close() // closing the descriptor drops the flock
	}
}

// mapFile maps size bytes of f read-only and shared: clean file-backed
// pages the kernel can drop and re-read (the page cache is the warm tier,
// ADR-0011 §3.2).
func mapFile(f *os.File, size int64) ([]byte, func() error, error) {
	if size <= 0 || size > math.MaxInt {
		return nil, nil, fmt.Errorf("size %d", size)
	}
	data, err := syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED) // #nosec G115 -- a descriptor; size checked above
	if err != nil {
		return nil, nil, err
	}
	return data, func() error { return syscall.Munmap(data) }, nil
}
