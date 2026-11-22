//go:build unix

package badger

import (
	"os"
	"testing"
	"time"

	badgerv4 "github.com/dgraph-io/badger/v4"
	"golang.org/x/sys/unix"
)

// TestOpenDBWaitsOutAForkedLockCopy: a descriptor of the directory with the
// lock on it stands for the copy a forked child holds of badger's descriptor
// until its exec (TAGFLAKES: "Cannot acquire directory lock" on the reopen of
// a closed cold shard in the tag CI). badgerv4.Open is refused while it lives;
// OpenDB retries, and opens once the copy is gone.
func TestOpenDBWaitsOutAForkedLockCopy(t *testing.T) {
	dir := t.TempDir()
	opts := badgerv4.DefaultOptions(dir).WithLogger(nil)
	db, err := OpenDB(opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	child, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = child.Close() })
	if err := unix.Flock(int(child.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil { // #nosec G115 -- a file descriptor
		t.Fatal(err)
	}
	if db, err := badgerv4.Open(opts); err == nil || !dirLockRefused(err) {
		if db != nil {
			_ = db.Close()
		}
		t.Fatalf("badgerv4.Open with the lock held: %v; want the directory lock refused", err)
	}
	pauses := 0
	dirLockPause = func(time.Duration) {
		pauses++
		_ = child.Close() // the child's exec closes its copy
	}
	t.Cleanup(func() { dirLockPause = time.Sleep })
	db, err = OpenDB(opts)
	if err != nil {
		t.Fatalf("OpenDB after the copy closed: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if pauses != 1 {
		t.Fatalf("OpenDB paused %d times; want 1", pauses)
	}
}
