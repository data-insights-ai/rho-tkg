// Tests in this file pin the round-2 review's R2-F2 finding: hot-shard
// rotation must persist the catalog BEFORE mutating live in-memory
// topology, so a catalog-Save failure does not leave the process running
// with a switched-over hotShard but a durable catalog describing the old
// topology (split-brain on restart).

package tiered

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestRotateHotShard_CatalogSaveFailure_RollsBackInMemory(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("chmod-driven write-failure injection is unreliable on Windows")
	}
	t.Parallel()

	dir := t.TempDir()
	ts, err := New(Config{
		DataDir:     dir,
		ShardWindow: time.Hour,
		RefLabels:   []string{"Person"},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = ts.Close() })

	hotBefore := ts.hotShard
	nameBefore := hotBefore.name
	tierBefore := hotBefore.tier
	timeEndBefore := hotBefore.timeEnd
	shardCountBefore := len(ts.eventShards)
	catalogBefore := ts.catalog.snapshotShards()

	// Make the catalog directory read-only so atomicWriteFile's
	// CreateTemp call fails. The catalog file lives at
	// <DataDir>/meta/shard_catalog.json.
	metaDir := filepath.Join(dir, "meta")
	if err := os.Chmod(metaDir, 0o500); err != nil {
		t.Fatalf("chmod meta read-only: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(metaDir, 0o700) })

	if err := ts.ForceRotate(); err == nil {
		t.Fatal("ForceRotate: nil error, want failure (catalog dir is read-only)")
	}

	// Live in-memory topology must be unchanged.
	if ts.hotShard != hotBefore {
		t.Errorf("hotShard pointer switched after failed rotate")
	}
	if ts.hotShard.name != nameBefore {
		t.Errorf("hotShard name = %q, want %q", ts.hotShard.name, nameBefore)
	}
	if ts.hotShard.tier != tierBefore {
		t.Errorf("hotShard tier = %q, want %q", ts.hotShard.tier, tierBefore)
	}
	if !ts.hotShard.timeEnd.Equal(timeEndBefore) {
		t.Errorf("hotShard timeEnd changed after failed rotate (was %v, now %v)", timeEndBefore, ts.hotShard.timeEnd)
	}
	if len(ts.eventShards) != shardCountBefore {
		t.Errorf("eventShards count = %d, want %d (no new shard should be added on rotation failure)", len(ts.eventShards), shardCountBefore)
	}

	// Catalog must be rolled back to its pre-rotation state.
	catalogAfter := ts.catalog.snapshotShards()
	if len(catalogAfter) != len(catalogBefore) {
		t.Errorf("catalog shard count = %d, want %d (rollback failed)", len(catalogAfter), len(catalogBefore))
	}
	for i := range catalogBefore {
		if i >= len(catalogAfter) {
			break
		}
		if catalogBefore[i].Name != catalogAfter[i].Name {
			t.Errorf("catalog[%d].Name = %q, want %q", i, catalogAfter[i].Name, catalogBefore[i].Name)
		}
		if catalogBefore[i].Tier != catalogAfter[i].Tier {
			t.Errorf("catalog[%d].Tier = %q, want %q", i, catalogAfter[i].Tier, catalogBefore[i].Tier)
		}
		if !catalogBefore[i].TimeEnd.Equal(catalogAfter[i].TimeEnd) {
			t.Errorf("catalog[%d].TimeEnd changed", i)
		}
	}
}

func TestRotateHotShard_Success_PersistsBeforeLiveSwitch(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	ts, err := New(Config{
		DataDir:     dir,
		ShardWindow: time.Hour,
		RefLabels:   []string{"Person"},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = ts.Close() })

	hotNameBefore := ts.hotShard.name

	if err := ts.ForceRotate(); err != nil {
		t.Fatalf("ForceRotate: %v", err)
	}

	// Successful rotation should switch the live hotShard and persist.
	if ts.hotShard.name == hotNameBefore {
		t.Errorf("hotShard name unchanged after successful rotate; got %q", ts.hotShard.name)
	}
	// The catalog file should now describe the new hot shard.
	hot, ok := ts.catalog.HotEventShard()
	if !ok {
		t.Fatal("catalog has no hot event shard after successful rotate")
	}
	if hot.Name != ts.hotShard.name {
		t.Errorf("catalog hot=%q, live hot=%q", hot.Name, ts.hotShard.name)
	}
}
