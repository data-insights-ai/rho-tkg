//go:build !unix

package segdir

import (
	"fmt"
	"io"
	"os"
)

// lockHandle is a no-op off unix: the directory is not locked against a
// second process (documented in AGENTS.md; the supported platforms are unix).
type lockHandle struct{}

func lockDir(string) (lockHandle, error) { return lockHandle{}, nil }

func (lockHandle) release() {}

// mapFile reads the file into memory off unix (no mmap): correct, but the
// segment bytes are then heap, not page cache.
func mapFile(f *os.File, size int64) ([]byte, func() error, error) {
	if size <= 0 {
		return nil, nil, fmt.Errorf("size %d", size)
	}
	data := make([]byte, size)
	if _, err := io.ReadFull(f, data); err != nil {
		return nil, nil, err
	}
	return data, func() error { return nil }, nil
}
