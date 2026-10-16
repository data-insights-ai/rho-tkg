package tiered

import (
	"io/fs"
	"path/filepath"
	"testing"
	"time"

	badgerpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// A store open builds each shard's relationship temporal index exactly once:
// the read-only WAL-recovery probe a warm shard opens first must not build it
// too (the probe is closed right away; its build is thrown away).
func TestTieredRelTemporalIndex_WarmOpenBuildsOnce(t *testing.T) {
	if testing.Short() && !raceEnabled {
		t.Skip("long tier: release gate")
	}
	dir := t.TempDir()
	cfg := Config{
		DataDir:       dir,
		RefLabels:     []string{"Case", "User"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	}
	ts, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	const typ uint16 = 4
	if err := relTemporalDDL(t, ts).CreateRelTemporalIndex(typ); err != nil {
		t.Fatalf("create: %v", err)
	}
	forceRotation(t, ts) // one warm shard, one hot shard
	if err := ts.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	reopen := func() int64 {
		before := badgerpkg.RelTemporalIndexBuildsTotalForTest()
		ts, err := New(cfg)
		if err != nil {
			t.Fatalf("reopen: %v", err)
		}
		built := badgerpkg.RelTemporalIndexBuildsTotalForTest() - before
		if err := ts.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
		return built
	}
	// A clean close: every shard restores the index from its open image.
	if got := reopen(); got != 0 {
		t.Errorf("clean reopen built %d relationship temporal indexes, want 0 (restored, backlog 54)", got)
	}
	// Without trusted images (a foreign write after Close), each shard rebuilds once.
	invalidateOpenImages(t, dir)
	if got := reopen(); got != 3 {
		t.Errorf("reopen built %d relationship temporal indexes, want 3 (reference, warm, hot)", got)
	}
}

// invalidateOpenImages commits one foreign key into every badger directory
// under dir, so no shard trusts its clean-close open image.
func invalidateOpenImages(t *testing.T, dir string) {
	t.Helper()
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || d.Name() != "MANIFEST" {
			return err
		}
		db, err := badgerv4.Open(badgerv4.DefaultOptions(filepath.Dir(path)).WithLogger(nil))
		if err != nil {
			return err
		}
		if err := db.Update(func(txn *badgerv4.Txn) error { return txn.Set([]byte("\xfeforeign"), []byte("x")) }); err != nil {
			_ = db.Close()
			return err
		}
		return db.Close()
	})
	if err != nil {
		t.Fatal(err)
	}
}
