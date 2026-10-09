package tiered

import (
	"errors"
	"testing"
	"time"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Catches: a DurableFlush built on Flush / forEachOpenShard, which lazy-opens
// every closed cold shard on each commit (a durable commit would then cost one
// Badger open per historical shard). A closed cold shard has no pending writes,
// so it must be skipped. MaxOpenColdShards keeps a lazily opened shard open, so
// such an open stays observable after the call.
func TestDurableFlush_DoesNotOpenClosedColdShard(t *testing.T) {
	ts, err := New(Config{
		DataDir:           t.TempDir(),
		RefLabels:         []string{"Case"},
		ShardWindow:       7 * 24 * time.Hour,
		FlushInterval:     1<<63 - 1,
		MaxOpenColdShards: 8,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = ts.Close() })
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	signalTok, _ := reg.GetOrCreate("Signal")
	n := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), signalTok, nil)
	if err := ts.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}

	coldName := ts.HotShardForTest().Name()
	forceRotation(t, ts)
	demoteToCold(ts, coldName)
	cold := ts.EventShardsForTest()[coldName]
	cold.LockShardMuForTest()
	if cold.Store() != nil {
		if err := cold.Store().Close(); err != nil {
			cold.UnlockShardMuForTest()
			t.Fatalf("close cold store: %v", err)
		}
		cold.SetStoreForTest(nil)
	}
	cold.UnlockShardMuForTest()

	if err := ts.DurableFlush(); err != nil {
		t.Fatalf("DurableFlush: %v", err)
	}
	assertColdShardClosedForTest(t, cold, "DurableFlush")
}

func newDurableTieredStore(t *testing.T) (*Store, uint16, uint16) {
	t.Helper()
	ts, err := New(Config{
		DataDir:       t.TempDir(),
		RefLabels:     []string{"Case"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = ts.Close() })
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	caseTok, _ := reg.GetOrCreate("Case")
	signalTok, _ := reg.GetOrCreate("Signal")
	return ts, caseTok, signalTok
}

// Catches: a DurableFlush that skips an open archive shard (a group that
// touched the archive would be left in its pending buffer).
func TestDurableFlush_ReachesOpenArchive(t *testing.T) {
	ts, caseTok, _ := newDurableTieredStore(t)
	if err := ts.EnsureRefArchiveForTest(); err != nil {
		t.Fatalf("EnsureRefArchiveForTest: %v", err)
	}
	archive := ts.refArchive.Load()
	n := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), caseTok, nil)
	if err := archive.PutNode(n); err != nil {
		t.Fatalf("archive PutNode: %v", err)
	}
	if archive.PendingWriteCount() == 0 {
		t.Fatal("setup: archive write not pending")
	}
	if err := ts.DurableFlush(); err != nil {
		t.Fatalf("DurableFlush: %v", err)
	}
	if n := archive.PendingWriteCount(); n != 0 {
		t.Fatalf("archive: %d writes pending after DurableFlush, want 0", n)
	}
	if archive.DurableSyncCountForTest() != 1 {
		t.Fatalf("archive fsyncs = %d, want 1", archive.DurableSyncCountForTest())
	}
}

// Catches: a DurableFlush that stops at the first failing shard (the other
// shards' groups stay unflushed) or that swallows a shard's error.
func TestDurableFlush_FailedShardReportedOthersStillFlushed(t *testing.T) {
	t.Run("reference-shard-fails", func(t *testing.T) {
		ts, _, signalTok := newDurableTieredStore(t)
		n := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), signalTok, nil)
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("PutNode: %v", err)
		}
		hot := ts.HotShardForTest().Store()
		if hot.PendingWriteCount() == 0 {
			t.Fatal("setup: event write not pending")
		}
		if err := ts.RefShardForTest().Close(); err != nil {
			t.Fatalf("close reference shard: %v", err)
		}
		err := ts.DurableFlush()
		if !errors.Is(err, ErrStoreClosed) {
			t.Fatalf("DurableFlush with a closed reference shard: got %v, want ErrStoreClosed", err)
		}
		if n := hot.PendingWriteCount(); n != 0 {
			t.Fatalf("hot shard: %d writes pending — the flush stopped at the failing reference shard", n)
		}
	})
	t.Run("event-shard-fails", func(t *testing.T) {
		ts, caseTok, _ := newDurableTieredStore(t)
		n := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), caseTok, nil)
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("PutNode: %v", err)
		}
		ref := ts.RefShardForTest()
		if ref.PendingWriteCount() == 0 {
			t.Fatal("setup: reference write not pending")
		}
		if err := ts.HotShardForTest().Store().Close(); err != nil {
			t.Fatalf("close hot shard: %v", err)
		}
		err := ts.DurableFlush()
		if !errors.Is(err, ErrStoreClosed) {
			t.Fatalf("DurableFlush with a closed event shard: got %v, want ErrStoreClosed", err)
		}
		if n := ref.PendingWriteCount(); n != 0 {
			t.Fatalf("reference shard: %d writes pending after DurableFlush, want 0", n)
		}
	})
}
