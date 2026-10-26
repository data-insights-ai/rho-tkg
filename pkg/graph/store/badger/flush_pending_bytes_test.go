package badger

import (
	"errors"
	"strings"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// ASYNCSTAGE (WRITERATE, tasks/evidence/WRITERATE/results.txt). An async
// store's backpressure flush fired only at MaxPendingWrites (100 k ops, ~60 MB
// of 500 B facts), far above badger's transaction limit, so every such flush
// was staged: each entry written three times. The pending buffer is now bounded
// by bytes and entries below the limit too, so a stream of writes commits in
// plain transactions.
func TestAsyncBackpressureFlushFitsOneTransaction(t *testing.T) {
	// 8 MB memtables: a transaction holds about 1.26 MB and 13,107 entries.
	cfg := func(dir string) Config {
		return Config{Dir: dir, ChangeLog: true, MemTableSize: 8 << 20, FlushInterval: time.Hour}
	}
	id := func(i int) types.NodeID { return types.NodeID(snowflake.ID(1_000_000 + i)) }
	// writeAll runs write for 0..n-1 and a final Flush; it fails on a staged
	// flush, or on more than maxCommits commits (one flush per write).
	writeAll := func(t *testing.T, bs *Store, n, maxCommits int, write func(i int) error) {
		t.Helper()
		staged := 0
		bs.stagedFlushFault = func(s stagedFlushStep) error {
			if s == stagedStepBegin {
				staged++
			}
			return nil
		}
		defer func() { bs.stagedFlushFault = nil }()
		v0 := bs.db.MaxVersion()
		for i := range n {
			if err := write(i); err != nil {
				t.Fatal(err)
			}
		}
		if err := bs.Flush(); err != nil {
			t.Fatal(err)
		}
		if staged != 0 {
			t.Fatalf("%d staged flushes, want 0: the pending buffer outgrew a transaction", staged)
		}
		if commits := bs.db.MaxVersion() - v0; commits > uint64(maxCommits) {
			t.Fatalf("%d commits for %d writes, want <= %d: the buffer flushes far below its bound", commits, n, maxCommits)
		}
	}
	reopenCount := func(t *testing.T, bs *Store, dir string) int {
		t.Helper()
		if err := bs.Close(); err != nil {
			t.Fatal(err)
		}
		bs, err := New(cfg(dir))
		if err != nil {
			t.Fatal(err)
		}
		defer bs.Close()
		got, err := bs.NodeCount()
		if err != nil {
			t.Fatal(err)
		}
		return got
	}

	t.Run("counted", func(t *testing.T) {
		// The counter covers every entry the next flush writes (rows, index
		// entries, change-log records), and again after a failed flush's
		// requeue: the bound sees the whole transaction.
		bs, err := New(Config{Dir: t.TempDir(), ChangeLog: true, FlushInterval: time.Hour, MaxPendingWrites: -1})
		if err != nil {
			t.Fatal(err)
		}
		defer bs.Close()
		pad := strings.Repeat("x", 500)
		for i := range 100 {
			node := types.NewNode(id(i), 7, nil)
			_ = node.SetProperty("pad", pad)
			if err := bs.PutNode(node); err != nil {
				t.Fatal(err)
			}
		}
		size := func(key, value int) int64 { return int64(key+value) + 12 } // docs/badger.md section 2, inline
		check := func(when string) {
			bs.wbMu.Lock()
			defer bs.wbMu.Unlock()
			var want int64
			for k, op := range bs.pending {
				want += size(len(k), len(op.value))
			}
			for _, r := range bs.pendingLog {
				want += size(len(storepkg.ChangeLogKey(r.lsn)), len(r.value))
			}
			if bs.pendingBytes < want {
				t.Fatalf("%s: pendingBytes = %d, below the %d bytes pending", when, bs.pendingBytes, want)
			}
		}
		check("buffered")
		bs.FailNextFlushForTest(errors.New("injected"))
		if err := bs.Flush(); err == nil {
			t.Fatal("injected flush failure not reported")
		}
		check("requeued")
	})

	t.Run("bytes", func(t *testing.T) {
		// 4,000 facts of 500 B: about 2 MB, so one flush of them all is staged.
		dir := t.TempDir()
		bs, err := New(cfg(dir))
		if err != nil {
			t.Fatal(err)
		}
		pad := strings.Repeat("x", 500)
		const n = 4_000
		writeAll(t, bs, n, 20, func(i int) error {
			node := types.NewNode(id(i), 7, nil)
			_ = node.SetProperty("pad", pad)
			return bs.PutNode(node)
		})
		if got := reopenCount(t, bs, dir); got != n {
			t.Fatalf("NodeCount after reopen = %d, want %d", got, n)
		}
	})

	t.Run("entries", func(t *testing.T) {
		// Deleting 6,000 nodes queues about 18,000 entries of ~26 B: over the
		// entry limit while under half the byte limit.
		dir := t.TempDir()
		bs, err := New(cfg(dir))
		if err != nil {
			t.Fatal(err)
		}
		const n = 6_000
		for i := range n {
			node := types.NewNode(id(i), 7, nil)
			_ = node.SetProperty("k", int64(i))
			if err := bs.PutNode(node); err != nil {
				t.Fatal(err)
			}
		}
		writeAll(t, bs, n, 20, func(i int) error { return bs.DeleteNode(id(i)) })
		if got := reopenCount(t, bs, dir); got != 0 {
			t.Fatalf("NodeCount after reopen = %d, want 0", got)
		}
	})
}
