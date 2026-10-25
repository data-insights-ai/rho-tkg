//go:build !windows && !plan9 && !js && !wasip1 && !aix

package tiered

import (
	"os"

	"golang.org/x/sys/unix"
)

// lockShardDir takes the exclusive, non-blocking lock badger takes on a
// shard directory it opens (flock on the directory, dir_unix.go:47-52), so
// it fails while a store has the shard open and keeps one from opening it
// until the returned release.
func lockShardDir(dir string) (func(), error) {
	f, err := os.Open(dir) // #nosec G304 -- a shard directory the catalog names
	if err != nil {
		return nil, err
	}
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = f.Close()
		return nil, err
	}
	return func() { _ = f.Close() }, nil
}
