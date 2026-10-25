package badger

import (
	"sync/atomic"
	"testing"
	"time"
)

// TestSnapshotsEndAfterCloseWithoutBlocking pins DISCARDCLOSE: 101 scan
// snapshots, or 101 relationship-type build snapshots, that hold a read
// transaction when the store closes all end afterwards. badger's Discard
// after db.Close sends to a 100-slot channel nobody drains any more
// (tasks/evidence/USAGE/discard_test.go.txt): the 101st blocked forever.
func TestSnapshotsEndAfterCloseWithoutBlocking(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		name string
		open func(bs *Store) func()
	}{
		{"scan", func(bs *Store) func() {
			s := newScanSnapshot(bs, bs.nodeCache.FlushEpoch)
			s.anyTxn()
			return s.close
		}},
		{"rel-type build", func(bs *Store) func() {
			s := &relBuildSnap{bs: bs, db: bs.db, epoch: bs.relCache.FlushEpoch}
			s.current()
			return s.close
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			bs, err := New(Config{InMemory: true})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			ends := make([]func(), 101)
			for i := range ends {
				ends[i] = c.open(bs)
			}
			if err := bs.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			var ended atomic.Int64
			done := make(chan struct{})
			go func() {
				for _, end := range ends {
					end()
					ended.Add(1)
				}
				close(done)
			}()
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Fatalf("%d of %d snapshots ended within 10 s after Close", ended.Load(), len(ends))
			}
		})
	}
}
