package badger

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Catches: a DurableFlush that relies on "checkOpen passed, so the DB is
// still open" (a timing argument). Close runs between checkOpen and the flush
// lock: its final flush writes the pending row (leaving unsynced set) and
// db.Close drops the memtable, so a later db.Sync dereferences it (SIGSEGV in
// memTable.SyncWAL). Reachable from GraphTx.Commit racing Graph.Close.
func TestDurableFlush_CloseAfterCheckOpenReturnsStoreClosed(t *testing.T) {
	bs, err := New(Config{Dir: t.TempDir(), FlushInterval: time.Hour})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	putTestNode(t, bs, 1001, 1, nil)
	bs.testHookDurableAfterCheckOpen = func() {
		if err := bs.Close(); err != nil {
			t.Errorf("Close inside the hook: %v", err)
		}
	}
	if err := bs.DurableFlush(); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("DurableFlush after a Close that ran past its checkOpen: got %v, want ErrStoreClosed", err)
	}
}

// Catches: unsynchronized access between DurableFlush, the background flush
// loop, a second DurableFlush and Close (run under -race), and any error from
// DurableFlush other than ErrStoreClosed once Close started.
func TestDurableFlush_ConcurrentWithBackgroundFlushDurableFlushAndClose(t *testing.T) {
	bs, err := New(Config{Dir: t.TempDir(), FlushInterval: time.Millisecond})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	var closed atomic.Bool
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { // writer feeding the background flush and the durable flushes
		defer wg.Done()
		for id := int64(1); ; id++ {
			if err := bs.PutNode(newDurableTestNode(id)); err != nil {
				if !closed.Load() {
					t.Errorf("PutNode before Close: %v", err)
				}
				return
			}
		}
	}()
	for w := 0; w < 2; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				err := bs.DurableFlush()
				if err == nil {
					continue
				}
				if !errors.Is(err, ErrStoreClosed) || !closed.Load() {
					t.Errorf("DurableFlush: %v (closed=%v), want nil before Close and ErrStoreClosed after", err, closed.Load())
				}
				return
			}
		}()
	}
	time.Sleep(50 * time.Millisecond)
	closed.Store(true)
	if err := bs.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
	wg.Wait()
	if bs.DurableSyncCountForTest() == 0 {
		t.Fatal("no DurableFlush fsynced while rows were written")
	}
}

func newDurableTestNode(id int64) *types.Node {
	return types.NewNode(types.NodeID(snowflake.ID(id)), 1, nil)
}
