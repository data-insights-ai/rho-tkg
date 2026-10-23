package tiered

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// shardDirFixture writes one event node, rotates its window out and returns
// the store, the data directory, the node and the old shard's name.
func shardDirFixture(t *testing.T) (*Store, string, *types.Node, string) {
	t.Helper()
	dir := t.TempDir()
	ts, err := New(Config{DataDir: dir, RefLabels: []string{"Case"}, ShardWindow: 7 * 24 * time.Hour, FlushInterval: 1<<63 - 1})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = ts.Close() })
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	tok, _ := reg.GetOrCreate("Signal")
	n := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), tok, nil)
	if err := ts.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	old := ts.HotShardForTest().Name()
	forceRotation(t, ts)
	return ts, dir, n, old
}

// TestLazyOpenRefusesMissingShardDir pins SHARD step 1: a cataloged cold shard
// whose directory is gone (a lost or unmounted volume) fails its lazy open
// with fs.ErrNotExist and the directory is not recreated. Mutation: open
// without the directory check → badger creates an empty shard there and the
// read answers ErrNodeNotFound, a wrong answer.
func TestLazyOpenRefusesMissingShardDir(t *testing.T) {
	t.Parallel()
	ts, dir, n, old := shardDirFixture(t)
	demoteToCold(ts, old)
	cold := ts.EventShardsForTest()[old]
	cold.LockShardMuForTest()
	if err := cold.Store().Close(); err != nil {
		cold.UnlockShardMuForTest()
		t.Fatalf("close cold store: %v", err)
	}
	cold.SetStoreForTest(nil)
	cold.UnlockShardMuForTest()
	shardPath := filepath.Join(dir, cold.Path())
	if err := os.RemoveAll(shardPath); err != nil {
		t.Fatal(err)
	}
	_, err := ts.GetNode(n.ID())
	if !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("GetNode on a shard whose directory is gone = %v, want fs.ErrNotExist", err)
	}
	if _, statErr := os.Stat(shardPath); !errors.Is(statErr, fs.ErrNotExist) {
		t.Fatalf("the open recreated the missing shard directory %s (stat err %v)", shardPath, statErr)
	}
}

// TestNewRefusesMissingWarmShardDir is the twin at open: a warm shard in the
// catalog whose directory is gone fails New with fs.ErrNotExist, and the
// directory is not recreated. Mutation: as above, New would open an empty
// warm shard.
func TestNewRefusesMissingWarmShardDir(t *testing.T) {
	t.Parallel()
	ts, dir, _, old := shardDirFixture(t)
	shardPath := filepath.Join(dir, ts.EventShardsForTest()[old].Path())
	if err := ts.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := os.RemoveAll(shardPath); err != nil {
		t.Fatal(err)
	}
	reopened, err := New(Config{DataDir: dir, RefLabels: []string{"Case"}, ShardWindow: 7 * 24 * time.Hour, FlushInterval: 1<<63 - 1})
	if err == nil {
		_ = reopened.Close()
		t.Fatal("New opened a store whose warm shard directory is gone")
	}
	if !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("New = %v, want fs.ErrNotExist", err)
	}
	if _, statErr := os.Stat(shardPath); !errors.Is(statErr, fs.ErrNotExist) {
		t.Fatalf("New recreated the missing shard directory %s (stat err %v)", shardPath, statErr)
	}
}

// TestNewRefusesMissingHotOrReferenceShardDir pins the wrong answer step 1
// removes: the reference shard and an existing hot shard open through the
// plain (creating) badger open, so a store whose directory for either is gone
// reopened as an empty shard and answered "not found" for every row in it.
// Now New fails with fs.ErrNotExist and recreates nothing. Mutation: drop the
// directory check → New succeeds and GetNode answers ErrNodeNotFound.
func TestNewRefusesMissingHotOrReferenceShardDir(t *testing.T) {
	t.Parallel()
	for _, which := range []string{"hot", "reference"} {
		t.Run(which, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			cfg := Config{DataDir: dir, RefLabels: []string{"Case"}, ShardWindow: 7 * 24 * time.Hour, FlushInterval: 1<<63 - 1}
			ts, err := New(cfg)
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			reg := registrypkg.NewLabelRegistry()
			ts.SetLabelRegistry(reg)
			label := "Signal"
			if which == "reference" {
				label = "Case"
			}
			tok, _ := reg.GetOrCreate(label)
			n := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), tok, nil)
			if err := ts.PutNode(n); err != nil {
				t.Fatalf("PutNode: %v", err)
			}
			shardPath := filepath.Join(dir, "reference")
			if which == "hot" {
				shardPath = filepath.Join(dir, ts.HotShardForTest().Path())
			}
			if err := ts.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			if err := os.RemoveAll(shardPath); err != nil {
				t.Fatal(err)
			}
			reopened, err := New(cfg)
			if err == nil {
				_, getErr := reopened.GetNode(n.ID())
				_ = reopened.Close()
				t.Fatalf("New opened a store whose %s shard directory is gone; GetNode = %v", which, getErr)
			}
			if !errors.Is(err, fs.ErrNotExist) {
				t.Fatalf("New = %v, want fs.ErrNotExist", err)
			}
			if _, statErr := os.Stat(shardPath); !errors.Is(statErr, fs.ErrNotExist) {
				t.Fatalf("New recreated the missing %s shard directory (stat err %v)", which, statErr)
			}
		})
	}
}
