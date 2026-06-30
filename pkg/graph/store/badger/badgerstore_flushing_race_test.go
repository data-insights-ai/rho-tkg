package badger

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// SYSTEM PROPERTY (real concurrency): the `flushing` overlay must be published
// and consumed under wbMu. flush() swaps `pending`->`flushing`, releases idxMu,
// commits the WriteBatch (on a real on-disk DB this is a genuine, non-trivial
// window), then clears `flushing` under wbMu. Every overlay reader takes wbMu
// (via rangePending / lookupPending) while iterating those maps. If flush()'s
// swap/clear were moved outside wbMu, or a reader dropped its wbMu lock, this
// test must (a) trip the race detector on the concurrent map access, and/or
// (b) observe a just-written history version spuriously reported ErrVersionNotFound
// during the commit window. Run with: go test -race -run ... -count=5.
//
// Single-goroutine white-box tests (badgerstore_flushing_test.go) cannot catch a
// lock removal — only a true data race under -race does.
func TestFlushingOverlay_ConcurrentFlushVsReaders_Race(t *testing.T) {
	// Real Badger commit window: on-disk dir, FlushInterval=1h so only our manual
	// Flush() goroutine drives commits, NOT SyncWrites so flush()'s swap/commit/
	// clear runs with idxMu released (the window the overlay protects).
	bs, err := New(Config{
		Dir:                  t.TempDir(),
		FlushInterval:        time.Hour,
		LabelIndexOnDisk:     true,
		AdjacencyIndexOnDisk: true,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = bs.Close() })

	const seedCount = 100
	const labelTok = uint16(10)
	const relTok = uint16(5)

	// Pre-seed nodes (each with a couple of history versions), rels, labels, and
	// adjacency rows. Commit them so they are durable before the race begins.
	for i := int64(1); i <= seedCount; i++ {
		putTestNode(t, bs, i, labelTok, nil)
		for v := uint32(0); v < 2; v++ {
			n := types.NewNode(types.NodeID(snowflake.ID(i)), labelTok, nil)
			n.SetVersion(v)
			if err := bs.PutNodeVersion(types.NodeID(snowflake.ID(i)), v, n); err != nil {
				t.Fatalf("PutNodeVersion(%d,%d): %v", i, v, err)
			}
		}
	}
	for i := int64(1); i < seedCount; i++ { // chain rels i -> i+1
		putTestRel(t, bs, 1000+i, relTok, i, i+1)
	}
	if err := bs.Flush(); err != nil {
		t.Fatalf("seed Flush: %v", err)
	}

	const iters = 400
	var (
		wg       sync.WaitGroup
		stop     atomic.Bool
		firstErr atomic.Value // error
	)
	recordErr := func(e error) {
		if e != nil {
			firstErr.CompareAndSwap(nil, e)
			stop.Store(true)
		}
	}

	// Writer/reader: write a brand-new history version, then immediately read it
	// back. During the commit window the version lives only in `flushing`; a
	// dropped wbMu lock or an unpublished swap makes this read spuriously fail
	// with ErrVersionNotFound. Each iteration uses a fresh, unique version so a
	// concurrent flush truncation cannot legitimately remove it.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iters && !stop.Load(); i++ {
			ver := uint32(100 + i)
			nid := types.NodeID(snowflake.ID(1))
			n := types.NewNode(nid, labelTok, nil)
			n.SetVersion(ver)
			if err := bs.PutNodeVersion(nid, ver, n); err != nil {
				recordErr(fmt.Errorf("PutNodeVersion(1,%d): %w", ver, err))
				return
			}
			got, err := bs.GetNodeVersion(nid, ver)
			if errors.Is(err, ErrVersionNotFound) {
				recordErr(fmt.Errorf("GetNodeVersion(1,%d) spuriously ErrVersionNotFound in the commit window (flushing overlay not seen)", ver))
				return
			}
			if err != nil {
				recordErr(fmt.Errorf("GetNodeVersion(1,%d): %w", ver, err))
				return
			}
			if got == nil || got.Version() != ver {
				recordErr(fmt.Errorf("GetNodeVersion(1,%d) returned wrong version %v", ver, got))
				return
			}
		}
	}()

	// Flush loop — drives the swap/commit/clear cycle concurrently with readers.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iters && !stop.Load(); i++ {
			if err := bs.Flush(); err != nil {
				recordErr(fmt.Errorf("Flush: %w", err))
				return
			}
		}
	}()

	// Concurrent overlay readers exercising every wbMu-guarded door.
	readers := []func() error{
		func() error {
			_, err := bs.GetNodeHistory(types.NodeID(snowflake.ID(1)))
			return err
		},
		func() error {
			_, err := bs.AllNodeHistoryIDs()
			return err
		},
		func() error {
			bs.idxMu.RLock()
			_, err := bs.labelNodeIDsSnapshotLocked(labelTok)
			bs.idxMu.RUnlock()
			return err
		},
		func() error {
			bs.idxMu.RLock()
			_, err := bs.adjacentRelIDsSnapshotLocked(types.NodeID(snowflake.ID(2)), 0, false)
			bs.idxMu.RUnlock()
			return err
		},
		func() error {
			bs.idxMu.RLock()
			_, err := bs.adjacentRelIDsSnapshotLocked(types.NodeID(snowflake.ID(2)), 0, true)
			bs.idxMu.RUnlock()
			return err
		},
	}
	for _, fn := range readers {
		wg.Add(1)
		go func(read func() error) {
			defer wg.Done()
			for i := 0; i < iters && !stop.Load(); i++ {
				if err := read(); err != nil {
					recordErr(err)
					return
				}
			}
		}(fn)
	}

	wg.Wait()
	if v := firstErr.Load(); v != nil {
		t.Fatalf("concurrent flush vs readers: %v", v.(error))
	}
}
