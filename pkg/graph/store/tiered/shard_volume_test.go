package tiered

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// volumeCfg is shardDirFixture's config with the given volumes.
func volumeCfg(dataDir, warmDir, coldDir string) Config {
	return Config{DataDir: dataDir, WarmDir: warmDir, ColdDir: coldDir, RefLabels: []string{"Case"},
		ShardWindow: 7 * 24 * time.Hour, FlushInterval: 1<<63 - 1}
}

// editCatalog rewrites the closed store's catalog file entry by entry, as
// raw JSON, so it can write what Save would refuse; an entry the edit marks
// dropEntry is removed, add entries are appended.
func editCatalog(t *testing.T, dataDir string, edit func(e map[string]any), add ...map[string]any) {
	t.Helper()
	p := filepath.Join(dataDir, "meta", "shard_catalog.json")
	data, err := os.ReadFile(p) // #nosec G304 -- test temp dir
	if err != nil {
		t.Fatal(err)
	}
	var c struct {
		Shards []map[string]any `json:"shards"`
	}
	if err := json.Unmarshal(data, &c); err != nil {
		t.Fatal(err)
	}
	kept := c.Shards[:0]
	for _, e := range c.Shards {
		if edit(e); e[dropEntry] == nil {
			kept = append(kept, e)
		}
	}
	c.Shards = append(kept, add...)
	if data, err = json.Marshal(c); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

const dropEntry = "drop entry"

// moveToWarm writes one event node, rotates its shard out, closes the store,
// moves that shard's directory to the warm volume and records the move in
// the catalog with the format sentinel (what the mover will do). It returns the node and shard name.
func moveToWarm(t *testing.T, dataDir, warmDir string) (*types.Node, string) {
	t.Helper()
	ts, err := New(volumeCfg(dataDir, warmDir, ""))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	tok, _ := reg.GetOrCreate("Signal")
	n := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), tok, nil)
	if err := ts.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	old := ts.HotShardForTest().Name()
	forceRotation(t, ts)
	rel := ts.EventShardsForTest()[old].Path()
	if err := ts.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(warmDir, "events"), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(dataDir, rel), filepath.Join(warmDir, rel)); err != nil {
		t.Fatal(err)
	}
	editCatalog(t, dataDir, func(e map[string]any) {
		if e["name"] == old {
			e["volume"] = VolumeWarm
		}
	}, map[string]any{"name": catalogFormatSentinel, "kind": string(catalogFormatKind)})
	return n, old
}

// TestShardOpensFromItsVolume pins SHARD step 3: an event shard the catalog
// places on the warm volume opens from WarmDir, its rows read back, the
// listing names that volume and directory, and nothing is recreated under
// DataDir. Catches a shard path that ignores the catalog's volume (New fails:
// the shard's directory under DataDir is gone).
func TestShardOpensFromItsVolume(t *testing.T) {
	t.Parallel()
	dataDir, warmDir := t.TempDir(), t.TempDir()
	n, old := moveToWarm(t, dataDir, warmDir)

	ts, err := New(volumeCfg(dataDir, warmDir, ""))
	if err != nil {
		t.Fatalf("New with the shard on the warm volume: %v", err)
	}
	defer func() { _ = ts.Close() }()
	if _, err := ts.GetNode(n.ID()); err != nil {
		t.Fatalf("GetNode of a row on the warm volume: %v", err)
	}
	rel := ts.EventShardsForTest()[old].Path()
	infos, err := ts.ListShards()
	if err != nil {
		t.Fatalf("ListShards: %v", err)
	}
	for _, si := range infos {
		want, wantPath := VolumeHot, filepath.Join(dataDir, "reference")
		switch si.Name {
		case old:
			want, wantPath = VolumeWarm, filepath.Join(warmDir, rel)
		case "reference":
		default:
			continue
		}
		if si.Volume != want || si.Path != wantPath || si.DiskBytes <= 0 {
			t.Errorf("%s: volume %q path %q bytes %d, want %q %q and its bytes", si.Name, si.Volume, si.Path, si.DiskBytes, want, wantPath)
		}
	}
	if _, err := os.Stat(filepath.Join(dataDir, rel)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("the shard's directory was recreated under DataDir (stat err %v)", err)
	}
}

// TestReferenceOnWarmVolume: a new store with ReferenceVolume warm makes its
// reference shard under WarmDir (none under DataDir), its catalog carries the
// format sentinel, and a reopen reads the reference row back from there.
// Catches a reference shard made under DataDir and a catalog without the
// sentinel.
func TestReferenceOnWarmVolume(t *testing.T) {
	t.Parallel()
	dataDir, warmDir := t.TempDir(), t.TempDir()
	cfg := volumeCfg(dataDir, warmDir, "")
	cfg.ReferenceVolume = VolumeWarm
	ts, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	tok, _ := reg.GetOrCreate("Case")
	n := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), tok, nil)
	if err := ts.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	if err := ts.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := os.Stat(filepath.Join(warmDir, "reference")); err != nil {
		t.Fatalf("no reference shard under WarmDir: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dataDir, "reference")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("a reference directory under DataDir (stat err %v)", err)
	}
	data, err := os.ReadFile(filepath.Join(dataDir, "meta", "shard_catalog.json")) // #nosec G304 -- test temp dir
	if err != nil || !strings.Contains(string(data), catalogFormatSentinel) {
		t.Fatalf("catalog without the format sentinel (err %v)", err)
	}
	ts, err = New(cfg)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = ts.Close() }()
	ts.SetLabelRegistry(reg)
	if _, err := ts.GetNode(n.ID()); err != nil {
		t.Fatalf("GetNode of the reference row after reopen: %v", err)
	}
}

// TestUnmappedVolumeFailsNew: New refuses a catalog that places a shard on a
// volume the config does not map, naming it. Catches the shard opened (or
// made) under another directory.
func TestUnmappedVolumeFailsNew(t *testing.T) {
	t.Parallel()
	dataDir, warmDir := t.TempDir(), t.TempDir()
	moveToWarm(t, dataDir, warmDir)
	if _, err := New(volumeCfg(dataDir, "", "")); err == nil || !strings.Contains(err.Error(), "volume warm, which the config does not map") {
		t.Fatalf("New without WarmDir = %v, want a refusal naming the warm volume", err)
	}
}

// TestCatalogVolumeShapesRefused: loading a catalog refuses an unknown
// volume, the hot shard off the hot volume, the reference shard on the cold
// volume, a format sentinel of a newer release, a shard off the hot volume
// without the sentinel. Catches any of them loading.
func TestCatalogVolumeShapesRefused(t *testing.T) {
	t.Parallel()
	dataDir, warmDir := t.TempDir(), t.TempDir()
	_, old := moveToWarm(t, dataDir, warmDir)
	catalog := filepath.Join(dataDir, "meta", "shard_catalog.json")
	orig, err := os.ReadFile(catalog) // #nosec G304 -- test temp dir
	if err != nil {
		t.Fatal(err)
	}
	if err := NewShardCatalog(catalog).Load(); err != nil {
		t.Fatalf("the moved store's catalog does not load: %v", err)
	}
	isHot := func(e map[string]any) bool { return e["tier"] == string(TierHot) && e["kind"] == string(ShardEvent) }
	for name, c := range map[string]struct {
		edit func(e map[string]any)
		want string
	}{
		"unknown volume":   {func(e map[string]any) { setIf(e, e["name"] == old, "volume", "blue") }, "unknown volume"},
		"hot shard warm":   {func(e map[string]any) { setIf(e, isHot(e), "volume", VolumeWarm) }, "off the hot volume"},
		"reference cold":   {func(e map[string]any) { setIf(e, e["name"] == "reference", "volume", VolumeCold) }, "on the cold volume"},
		"newer sentinel":   {func(e map[string]any) { setIf(e, e["name"] == catalogFormatSentinel, "kind", "catalog/3") }, "newer"},
		"sentinel missing": {func(e map[string]any) { setIf(e, e["name"] == catalogFormatSentinel, dropEntry, true) }, "no " + catalogFormatSentinel},
	} {
		if err := os.WriteFile(catalog, orig, 0o600); err != nil {
			t.Fatal(err)
		}
		editCatalog(t, dataDir, c.edit)
		if err := NewShardCatalog(catalog).Load(); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: Load = %v, want an error containing %q", name, err, c.want)
		}
	}
}

// relTo is dir relative to the working directory.
func relTo(t *testing.T, dir string) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	rel, err := filepath.Rel(wd, dir)
	if err != nil {
		t.Fatal(err)
	}
	return rel
}

func setIf(e map[string]any, cond bool, k string, v any) {
	if cond {
		e[k] = v
	}
}

// TestVolumeConfigRefused: New refuses volume settings it cannot honour,
// and a missing volume root (an unmounted device) is neither written to nor
// made.
func TestVolumeConfigRefused(t *testing.T) {
	t.Parallel()
	d := t.TempDir()
	for name, c := range map[string]struct {
		cfg  Config
		want string
	}{
		"warm is DataDir":           {Config{DataDir: d, WarmDir: d}, "Config.WarmDir is Config.DataDir"},
		"cold is WarmDir":           {Config{DataDir: d, WarmDir: filepath.Join(d, "w"), ColdDir: filepath.Join(d, "w") + "/"}, "Config.ColdDir is Config.WarmDir"},
		"warm is DataDir, relative": {Config{DataDir: d, WarmDir: relTo(t, d)}, "Config.WarmDir is Config.DataDir"},
		"reference cold":            {Config{DataDir: d, ColdDir: filepath.Join(d, "c"), ReferenceVolume: VolumeCold}, "cannot be cold"},
		"reference warm, no dir":    {Config{DataDir: d, ReferenceVolume: VolumeWarm}, "Config.ReferenceVolume is warm, which the config does not map"},
		"archive cold, no dir":      {Config{DataDir: d, ArchiveVolume: VolumeCold}, "Config.ArchiveVolume is cold, which the config does not map"},
		"unknown volume":            {Config{DataDir: d, ArchiveVolume: "blue"}, "is not a volume"},
		"in memory with volume":     {Config{InMemory: true, WarmDir: d}, "need a disk-backed store"},
	} {
		if ts, err := New(c.cfg); err == nil || !strings.Contains(err.Error(), c.want) {
			if ts != nil {
				_ = ts.Close()
			}
			t.Errorf("%s: New = %v, want an error containing %q", name, err, c.want)
		}
	}
	unmounted := filepath.Join(t.TempDir(), "ssd")
	if ts, err := New(Config{DataDir: t.TempDir(), WarmDir: unmounted, ReferenceVolume: VolumeWarm}); !errors.Is(err, fs.ErrNotExist) {
		if ts != nil {
			_ = ts.Close()
		}
		t.Fatalf("New with the reference volume missing = %v, want fs.ErrNotExist", err)
	}
	if _, err := os.Stat(unmounted); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("New made the missing volume root %s (stat err %v)", unmounted, err)
	}
}

// oldValidateCatalog is v4.77.2's catalog validator (shard_catalog.go
// validateLoadedLocked, frozen): a release before the volume minor runs it
// on every load.
func oldValidateCatalog(shards []ShardEntry) error {
	seen := map[string]bool{}
	for _, e := range shards {
		if e.Name == "" {
			return fmt.Errorf("empty shard name")
		}
		if seen[e.Name] {
			return fmt.Errorf("duplicate shard name %q", e.Name)
		}
		seen[e.Name] = true
		if !isSafeCatalogShardPath(e.Path) {
			return fmt.Errorf("invalid path for shard %q: %q", e.Name, e.Path)
		}
		switch e.Kind {
		case ShardReference, ShardArchive, ShardEvent:
		default:
			return fmt.Errorf("shard %q has invalid kind %q", e.Name, e.Kind)
		}
	}
	return nil
}

// TestSentinelRejectedByOldValidator: the catalog of a store with a shard off
// the hot volume fails the validator of the releases before the volume minor
// (they refuse the store at open and write nothing), and a store that never
// left the hot volume writes no sentinel and passes it. Catches a sentinel an
// old release would accept and a sentinel written for an all-hot store.
func TestSentinelRejectedByOldValidator(t *testing.T) {
	t.Parallel()
	load := func(dir string) []ShardEntry {
		data, err := os.ReadFile(filepath.Join(dir, "meta", "shard_catalog.json")) // #nosec G304 -- test temp dir
		if err != nil {
			t.Fatal(err)
		}
		var c struct {
			Shards []ShardEntry `json:"shards"`
		}
		if err := json.Unmarshal(data, &c); err != nil {
			t.Fatal(err)
		}
		return c.Shards
	}
	hot := t.TempDir()
	ts, err := New(volumeCfg(hot, t.TempDir(), ""))
	if err != nil {
		t.Fatal(err)
	}
	if err := ts.Close(); err != nil {
		t.Fatal(err)
	}
	if err := oldValidateCatalog(load(hot)); err != nil {
		t.Fatalf("an all-hot catalog fails the old validator: %v", err)
	}

	dataDir, warmDir := t.TempDir(), t.TempDir()
	moveToWarm(t, dataDir, warmDir)
	ts, err = New(volumeCfg(dataDir, warmDir, "")) // a save through the new code writes the sentinel
	if err != nil {
		t.Fatal(err)
	}
	if err := ts.CatalogForTest().Save(); err != nil {
		t.Fatal(err)
	}
	if err := ts.Close(); err != nil {
		t.Fatal(err)
	}
	if err := oldValidateCatalog(load(dataDir)); err == nil {
		t.Fatal("a catalog with a shard on the warm volume passes the old validator")
	}
}

// TestArchiveOnColdVolume: with ArchiveVolume cold the archive is made under
// ColdDir and cataloged there, so a reopen opens it from there; a store whose
// archive is not made yet, reopened with the cold volume's root missing,
// refuses to make it (and the root) instead of writing to the device
// underneath. Catches an archive entry without its volume and a lazy open
// that makes a missing volume root.
func TestArchiveOnColdVolume(t *testing.T) {
	t.Parallel()
	dataDir, coldDir := t.TempDir(), t.TempDir()
	cfg := volumeCfg(dataDir, "", coldDir)
	cfg.ArchiveVolume = VolumeCold
	for i := 0; i < 2; i++ { // make it, then reopen it
		ts, err := New(cfg)
		if err != nil {
			t.Fatalf("New %d: %v", i, err)
		}
		if err := ts.EnsureRefArchiveForTest(); err != nil {
			t.Fatalf("open archive %d: %v", i, err)
		}
		infos, err := ts.ListShards()
		if err != nil {
			t.Fatal(err)
		}
		for _, si := range infos {
			if si.Name == "archive" && (si.Volume != VolumeCold || si.Path != filepath.Join(coldDir, "archive")) {
				t.Fatalf("archive %d: volume %q path %q, want cold under ColdDir", i, si.Volume, si.Path)
			}
		}
		if err := ts.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := os.Stat(filepath.Join(dataDir, "archive")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("an archive directory under DataDir (stat err %v)", err)
	}

	hot := t.TempDir()
	ts, err := New(volumeCfg(hot, "", ""))
	if err != nil {
		t.Fatal(err)
	}
	if err := ts.Close(); err != nil {
		t.Fatal(err)
	}
	unmounted := filepath.Join(t.TempDir(), "blockdev")
	cfg = volumeCfg(hot, "", unmounted)
	cfg.ArchiveVolume = VolumeCold
	if ts, err = New(cfg); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ts.Close() }()
	if err := ts.EnsureRefArchiveForTest(); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("making the archive on a missing volume = %v, want fs.ErrNotExist", err)
	}
	if _, err := os.Stat(unmounted); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("the archive open made the missing volume root %s (stat err %v)", unmounted, err)
	}
}
