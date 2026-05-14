// Tests in this file pin the round-2 review's R2-F2 finding: hot-shard
// rotation must persist the catalog BEFORE mutating live in-memory
// topology, so a catalog-Save failure does not leave the process running
// with a switched-over hotShard but a durable catalog describing the old
// topology (split-brain on restart).

package tiered

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
	"time"

	registrypkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/internal/registry"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/types"
)

type alwaysOKVerifier struct{}

func (alwaysOKVerifier) VerifyNodeChain(types.NodeID) (bool, error) { return true, nil }
func (alwaysOKVerifier) VerifyRelChain(types.RelID) (bool, error)   { return true, nil }

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
	tierBefore := hotBefore.currentTier()
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
	if ts.hotShard.currentTier() != tierBefore {
		t.Errorf("hotShard tier = %q, want %q", ts.hotShard.currentTier(), tierBefore)
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

func TestRotateHotShard_TrackedIndexSetupFailure_RollsBackNewShard(t *testing.T) {
	ts := newTestTieredStore(t)

	hotBefore := ts.hotShard
	shardCountBefore := len(ts.eventShards)
	catalogBefore := ts.catalog.snapshotShards()

	ts.tempIdxMu.Lock()
	ts.tempIdxLabels = []uint16{0}
	ts.tempIdxMu.Unlock()

	if err := ts.ForceRotate(); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("ForceRotate error = %v, want ErrInvalidStoreMutation", err)
	}
	if ts.hotShard != hotBefore {
		t.Fatalf("hotShard changed after tracked index setup failure")
	}
	if len(ts.eventShards) != shardCountBefore {
		t.Fatalf("eventShards count = %d, want %d", len(ts.eventShards), shardCountBefore)
	}
	if catalogAfter := ts.catalog.snapshotShards(); !reflect.DeepEqual(catalogAfter, catalogBefore) {
		t.Fatalf("catalog changed after tracked index setup failure: got %#v want %#v", catalogAfter, catalogBefore)
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

func TestOpenRefArchive_CatalogSaveFailure_RollsBack(t *testing.T) {
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

	metaDir := filepath.Join(dir, "meta")
	if err := os.Chmod(metaDir, 0o500); err != nil {
		t.Fatalf("chmod meta read-only: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(metaDir, 0o700) })

	if err := ts.ensureRefArchive(); err == nil {
		t.Fatal("ensureRefArchive: nil error, want catalog-save failure")
	}
	if ts.refArchive.Load() != nil {
		t.Fatal("refArchive was published after failed catalog save")
	}
	if _, ok := ts.catalog.GetShard("archive"); ok {
		t.Fatal("catalog retained archive entry after failed catalog save")
	}
	if _, err := os.Stat(filepath.Join(dir, "archive")); !os.IsNotExist(err) {
		t.Fatalf("archive directory after rollback stat err = %v, want not exist", err)
	}
}

func TestVerifyShard_CatalogSaveFailure_RollsBackCache(t *testing.T) {
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

	oldHot := ts.hotShard.name
	if err := ts.ForceRotate(); err != nil {
		t.Fatalf("ForceRotate: %v", err)
	}
	before, ok := ts.catalog.GetShard(oldHot)
	if !ok {
		t.Fatalf("old hot shard %q missing from catalog", oldHot)
	}
	if before.Verified {
		t.Fatal("old hot shard unexpectedly verified before VerifyShard")
	}

	metaDir := filepath.Join(dir, "meta")
	if err := os.Chmod(metaDir, 0o500); err != nil {
		t.Fatalf("chmod meta read-only: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(metaDir, 0o700) })

	result, err := ts.VerifyShard(alwaysOKVerifier{}, oldHot)
	if err == nil {
		t.Fatal("VerifyShard: nil error, want catalog-save failure")
	}
	if result == nil {
		t.Fatal("VerifyShard returned nil result with cache-save error")
	}
	after, ok := ts.catalog.GetShard(oldHot)
	if !ok {
		t.Fatalf("old hot shard %q missing from catalog after failed cache save", oldHot)
	}
	if after.Verified {
		t.Fatal("catalog retained verified=true after failed cache save")
	}
}

func TestRebuildCatalog_CatalogSaveFailure_RollsBackStats(t *testing.T) {
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

	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	personTok, err := reg.GetOrCreate("Person")
	if err != nil {
		t.Fatalf("GetOrCreate Person: %v", err)
	}
	nodeID := tieredNodeGen(t).Generate()
	if err := ts.PutNode(types.NewNode(types.NodeID(nodeID), personTok, nil)); err != nil {
		t.Fatalf("PutNode: %v", err)
	}

	before, ok := ts.catalog.GetShard("reference")
	if !ok {
		t.Fatal("reference shard missing before RebuildCatalog")
	}
	if before.ApproxNodes != 0 {
		t.Fatalf("reference ApproxNodes before RebuildCatalog = %d, want 0 setup", before.ApproxNodes)
	}

	metaDir := filepath.Join(dir, "meta")
	if err := os.Chmod(metaDir, 0o500); err != nil {
		t.Fatalf("chmod meta read-only: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(metaDir, 0o700) })

	if err := ts.RebuildCatalog(); err == nil {
		t.Fatal("RebuildCatalog: nil error, want catalog-save failure")
	}
	after, ok := ts.catalog.GetShard("reference")
	if !ok {
		t.Fatal("reference shard missing after failed RebuildCatalog")
	}
	if after.ApproxNodes != before.ApproxNodes || after.ApproxRels != before.ApproxRels {
		t.Fatalf("reference stats after failed RebuildCatalog = (%d,%d), want (%d,%d)",
			after.ApproxNodes, after.ApproxRels, before.ApproxNodes, before.ApproxRels)
	}
}
