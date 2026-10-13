package tiered

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Backlog 44 on tiered: an event shard's commit floor lives on the reference
// shard (tieredstore_commit_floor.go).

func cfTieredStore(t *testing.T, dir string) (*Store, uint16) {
	t.Helper()
	ts, err := New(Config{DataDir: dir, RefLabels: []string{"Case"}, ShardWindow: 7 * 24 * time.Hour, FlushInterval: 1<<63 - 1})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	tok, _ := reg.GetOrCreate("Signal")
	return ts, tok
}

func cfPutSignal(t *testing.T, ts *Store, tok uint16, txFrom types.Instant) {
	t.Helper()
	n := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), tok, nil)
	n.SetTemporal(&types.TemporalMetadata{ValidFrom: txFrom, TxFrom: txFrom})
	if err := ts.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
}

// Power loss under DurableCommit: the reference shard's floor entry must be
// fsynced before the event rows it covers are, by the durable flush that makes
// them durable — whether that flush writes the rows itself or a background
// flush wrote them before. A background flush pays no fsync.
func TestShardCommitFloorSyncedByDurableFlush(t *testing.T) {
	ts, tok := cfTieredStore(t, t.TempDir())
	t.Cleanup(func() { _ = ts.Close() })
	ref := ts.RefShardForTest()
	syncs := func() int64 { return ref.MetaSyncCountForTest() }
	steps := []struct {
		name      string
		run       func() error
		wantSyncs func(n int64) bool
		want      string
	}{
		{"durable flush writes the rows", func() error {
			cfPutSignal(t, ts, tok, 500)
			return ts.DurableFlush()
		}, func(n int64) bool { return n >= 1 }, ">= 1"},
		{"background flush", func() error {
			cfPutSignal(t, ts, tok, 600)
			return ts.Flush()
		}, func(n int64) bool { return n == 0 }, "0"},
		{"durable flush after the background flush wrote the rows", ts.DurableFlush,
			func(n int64) bool { return n >= 1 }, ">= 1"},
		{"durable flush with nothing new", ts.DurableFlush, func(n int64) bool { return n == 0 }, "0"},
	}
	for _, st := range steps {
		before := syncs()
		if err := st.run(); err != nil {
			t.Fatalf("%s: %v", st.name, err)
		}
		if n := syncs() - before; !st.wantSyncs(n) {
			t.Fatalf("%s: reference-shard floor fsyncs = %d, want %s", st.name, n, st.want)
		}
	}
}

// Clear wipes the reference shard's floor entry; rows written after it must
// write it again, also when their stamps are below the floor already handed
// over. The crash is a copy of the open store's directory.
func TestShardCommitFloorRewrittenAfterClear(t *testing.T) {
	dir := t.TempDir()
	ts, tok := cfTieredStore(t, dir)
	t.Cleanup(func() { _ = ts.Close() })
	cfPutSignal(t, ts, tok, 900)
	if err := ts.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := ts.Clear(); err != nil {
		t.Fatalf("Clear: %v", err)
	}
	cfPutSignal(t, ts, tok, 50)
	if err := ts.Flush(); err != nil {
		t.Fatal(err)
	}
	crash := filepath.Join(t.TempDir(), "crash")
	if err := os.CopyFS(crash, os.DirFS(dir)); err != nil {
		t.Fatalf("copy: %v", err)
	}
	re, _ := cfTieredStore(t, crash)
	defer func() { _ = re.Close() }()
	if got, err := re.StoredCommitFloor(); err != nil || got < 50 {
		t.Fatalf("StoredCommitFloor after Clear, write, crash = (%d, %v), want >= 50", got, err)
	}
}
