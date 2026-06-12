package tiered

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// tuningCfg returns a disk-backed Config with every per-shard footprint knob set
// small and a per-shard cache byte budget, plus manual flushing for determinism.
func tuningCfg(dir string) Config {
	return Config{
		DataDir:          dir,
		RefLabels:        []string{"Case", "User"},
		ShardWindow:      7 * 24 * time.Hour,
		FlushInterval:    1<<63 - 1, // manual flush
		ValueLogFileSize: 1 << 20,
		MemTableSize:     8 << 20,
		BlockCacheSize:   1 << 20,
		NumCompactors:    2,
		CacheBudgetBytes: 4 << 20,
	}
}

// TestTieredFootprintTuningRoundTrips writes reference + hot + (post-rotation)
// warm-shard data under the full footprint tuning, closes, and reopens with the
// SAME tuning. The warm shard reopens through openBadgerStoreWithRecovery, so
// this proves the knobs reach every shard kind AND that the pre-probe migration
// added to the recovery path is a no-op on a cleanly closed dir.
func TestTieredFootprintTuningRoundTrips(t *testing.T) {
	dir := t.TempDir()
	cfg := tuningCfg(dir)

	ts, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	// The cache byte budget must have reached the reference shard.
	if got := ts.RefShardForTest().NodeCacheForTest().Budget(); got != cfg.CacheBudgetBytes {
		t.Fatalf("ref shard node-cache budget = %d, want %d", got, cfg.CacheBudgetBytes)
	}

	gen := tieredNodeGen(t)
	refNode := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	if err := ts.PutNode(refNode); err != nil {
		t.Fatalf("put ref node: %v", err)
	}
	var evt []types.NodeID
	for i := 0; i < 50; i++ {
		n := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("put event node %d: %v", i, err)
		}
		evt = append(evt, n.ID())
	}

	ts.MuForTest().RLock()
	_ = ts.RefShardForTest().Flush()
	_ = ts.HotShardForTest().Store().Flush()
	ts.MuForTest().RUnlock()

	// Rotate hot -> warm so the reopen exercises openBadgerStoreWithRecovery.
	time.Sleep(2 * time.Millisecond)
	if err := ts.RotateHotShard(); err != nil {
		t.Fatalf("rotate: %v", err)
	}
	for i := 0; i < 10; i++ {
		n := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("put post-rotation event node %d: %v", i, err)
		}
		evt = append(evt, n.ID())
	}
	if err := ts.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Reopen with the same tuning; read every entity back (GetNode routes by ID).
	ts2, err := New(cfg)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer ts2.Close()

	// The warm shard reopened through openBadgerStoreWithRecovery — assert the
	// tuning reached THAT path (the same path cold lazy-reopens use), not just
	// the eagerly-opened reference/hot shards. A recovery open that forgot the
	// cache budget or footprint would be caught here.
	var warmName string
	ts2.MuForTest().RLock()
	for _, es := range ts2.EventShardsForTest() {
		if es.Tier() == TierWarm && es.Store() != nil {
			warmName = es.Name()
			if got := es.Store().NodeCacheForTest().Budget(); got != cfg.CacheBudgetBytes {
				t.Errorf("recovery-reopened warm shard cache budget = %d, want %d", got, cfg.CacheBudgetBytes)
			}
		}
	}
	ts2.MuForTest().RUnlock()
	if warmName == "" {
		t.Fatal("no open warm shard after reopen to verify recovery-path tuning")
	}
	if got := maxMemFileSizeTiered(t, filepath.Join(dir, "events", warmName)); got != 2*cfg.MemTableSize {
		t.Errorf("recovery-reopened warm shard WAL apparent size = %d, want 2x MemTableSize (%d)", got, 2*cfg.MemTableSize)
	}

	if _, err := ts2.GetNode(refNode.ID()); err != nil {
		t.Fatalf("reference node lost across tuned reopen: %v", err)
	}
	for _, id := range evt {
		if _, err := ts2.GetNode(id); err != nil {
			t.Fatalf("event node %d lost across tuned reopen: %v", id.SnowflakeID(), err)
		}
	}
}

// TestTieredTuningValidationSurfacesAtNew confirms an out-of-range footprint
// knob is rejected at New() (it surfaces when the reference shard opens), with a
// message naming the offending field.
func TestTieredTuningValidationSurfacesAtNew(t *testing.T) {
	cases := []struct {
		name     string
		inMemory bool
		mutate   func(*Config)
		wantErr  string
	}{
		{"memtable below floor", false, func(c *Config) { c.MemTableSize = 1 << 20 }, "MemTableSize"},
		{"vlog above max", false, func(c *Config) { c.ValueLogFileSize = maxValueLogFileSizeForTest + 1 }, "ValueLogFileSize"},
		{"block cache negative", false, func(c *Config) { c.BlockCacheSize = -1 }, "BlockCacheSize"},
		{"compactors below minimum", false, func(c *Config) { c.NumCompactors = 1 }, "NumCompactors"},
		// Validation must NOT be skipped for an in-memory reference shard.
		{"in-memory memtable below floor", true, func(c *Config) { c.MemTableSize = 1 << 20 }, "MemTableSize"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Config{RefLabels: []string{"Case"}}
			if tc.inMemory {
				cfg.InMemory = true
			} else {
				cfg.DataDir = t.TempDir()
			}
			tc.mutate(&cfg)
			ts, err := New(cfg)
			if err == nil {
				_ = ts.Close()
				t.Fatalf("expected New to reject %s, got nil", tc.name)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error %q does not name %q", err, tc.wantErr)
			}
		})
	}
}

// maxValueLogFileSizeForTest mirrors the badger package's accepted vlog ceiling
// (2GB - 1) so the validation test can build an above-max value without
// importing an internal constant.
const maxValueLogFileSizeForTest = 2<<30 - 1

// TestTieredCacheTuningReachesEveryShard asserts both the entry capacity and the
// byte budget pass through to the reference shard, the hot shard, and (after a
// rotation) the warm shard — the per-shard heap-bound knobs behind the
// many-open-shards memory cost.
func TestTieredCacheTuningReachesEveryShard(t *testing.T) {
	const wantCap = 250
	const wantBudget = int64(7 << 20)
	ts, err := New(Config{
		InMemory:         true,
		RefLabels:        []string{"Case", "User"},
		ShardWindow:      7 * 24 * time.Hour,
		FlushInterval:    1<<63 - 1,
		CacheCapacity:    wantCap,
		CacheBudgetBytes: wantBudget,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer ts.Close()

	check := func(label string, bs *BadgerStore) {
		if bs == nil {
			t.Fatalf("%s shard store is nil", label)
		}
		if got := bs.NodeCacheForTest().Cap(); got != wantCap {
			t.Errorf("%s node-cache capacity = %d, want %d", label, got, wantCap)
		}
		if got := bs.NodeCacheForTest().Budget(); got != wantBudget {
			t.Errorf("%s node-cache budget = %d, want %d", label, got, wantBudget)
		}
		if got := bs.RelCacheForTest().Budget(); got != wantBudget {
			t.Errorf("%s rel-cache budget = %d, want %d", label, got, wantBudget)
		}
	}

	check("reference", ts.RefShardForTest())
	ts.MuForTest().RLock()
	check("hot", ts.HotShardForTest().Store())
	ts.MuForTest().RUnlock()

	// Rotate so a warm shard exists, then confirm it inherited the tuning too.
	time.Sleep(2 * time.Millisecond)
	if err := ts.RotateHotShard(); err != nil {
		t.Fatalf("rotate: %v", err)
	}
	var warmChecked bool
	ts.MuForTest().RLock()
	for _, es := range ts.EventShardsForTest() {
		if es.Tier() == TierWarm && es.Store() != nil {
			check("warm", es.Store())
			warmChecked = true
		}
	}
	ts.MuForTest().RUnlock()
	if !warmChecked {
		t.Fatal("no open warm shard found to verify tuning passthrough")
	}
}

// TestTieredFootprintKnobsReachShards is the adversarial complement to the cache
// passthrough test: it proves the per-instance footprint knobs are actually
// APPLIED to each shard's Badger, not merely stored on the config. Badger creates
// each shard's WAL at exactly 2x MemTableSize and its vlog at 2x ValueLogFileSize;
// asserting those apparent sizes on BOTH the reference shard and the event shard
// would catch a passthrough that quietly dropped the knobs (leaving the stock
// 128MB WAL / ~2GB vlog per shard — the exact multiply-by-shard-count cost this
// change exists to remove).
func TestTieredFootprintKnobsReachShards(t *testing.T) {
	if testing.Short() {
		t.Skip("opens shard files at tuned apparent sizes incl a multi-MB vlog")
	}
	dir := t.TempDir()
	const memtable = 8 << 20
	const vlog = 4 << 20
	ts, err := New(Config{
		DataDir:          dir,
		RefLabels:        []string{"Case", "User"},
		ShardWindow:      7 * 24 * time.Hour,
		FlushInterval:    1<<63 - 1,
		MemTableSize:     memtable,
		ValueLogFileSize: vlog,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")
	gen := tieredNodeGen(t)

	// Reference node -> reference shard.
	if err := ts.PutNode(types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)); err != nil {
		t.Fatalf("put ref node: %v", err)
	}
	// Event node carrying a >1MB value -> hot event shard (forces a vlog entry).
	evt := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	if err := evt.SetProperties(mustTieredProps(t, map[string]any{"blob": strings.Repeat("Z", 2<<20)})); err != nil {
		t.Fatalf("set props: %v", err)
	}
	if err := ts.PutNode(evt); err != nil {
		t.Fatalf("put event node: %v", err)
	}

	ts.MuForTest().RLock()
	hotName := ts.HotShardForTest().Name()
	_ = ts.RefShardForTest().Flush()
	_ = ts.HotShardForTest().Store().Flush()
	ts.MuForTest().RUnlock()

	refDir := filepath.Join(dir, "reference")
	hotDir := filepath.Join(dir, "events", hotName)

	for _, sh := range []struct{ label, path string }{{"reference", refDir}, {"hot event", hotDir}} {
		if got := maxMemFileSizeTiered(t, sh.path); got != 2*memtable {
			t.Errorf("%s shard WAL apparent size = %d, want 2x MemTableSize (%d) — footprint tuning did not reach this shard",
				sh.label, got, 2*memtable)
		}
	}
	if got := maxFileSizeWithExtTiered(t, hotDir, ".vlog"); got != 2*vlog {
		t.Errorf("hot event shard vlog apparent size = %d, want 2x ValueLogFileSize (%d)", got, 2*vlog)
	}
	if err := ts.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// TestTieredFootprintKnobsReachBadgerOptions verifies all four footprint knobs
// reach EVERY shard's live Badger options — the application-level complement to
// the file-size test, which only witnesses vlog + memtable. A badgerCfg that
// dropped BlockCacheSize or NumCompactors would pass the file-size test but fail
// here, on the reference, hot, and (post-rotation) warm shards alike.
func TestTieredFootprintKnobsReachBadgerOptions(t *testing.T) {
	const (
		vlog       = 64 << 20
		memtable   = 16 << 20
		cache      = 32 << 20
		compactors = 3
	)
	ts, err := New(Config{
		DataDir:          t.TempDir(),
		RefLabels:        []string{"Case", "User"},
		ShardWindow:      7 * 24 * time.Hour,
		FlushInterval:    1<<63 - 1,
		ValueLogFileSize: vlog,
		MemTableSize:     memtable,
		BlockCacheSize:   cache,
		NumCompactors:    compactors,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer ts.Close()

	check := func(label string, bs *BadgerStore) {
		if bs == nil {
			t.Fatalf("%s shard store is nil", label)
		}
		opts := bs.DBForTest().Opts()
		if opts.ValueLogFileSize != vlog {
			t.Errorf("%s shard ValueLogFileSize = %d, want %d", label, opts.ValueLogFileSize, vlog)
		}
		if opts.MemTableSize != memtable {
			t.Errorf("%s shard MemTableSize = %d, want %d", label, opts.MemTableSize, memtable)
		}
		if opts.BlockCacheSize != cache {
			t.Errorf("%s shard BlockCacheSize = %d, want %d", label, opts.BlockCacheSize, cache)
		}
		if opts.NumCompactors != compactors {
			t.Errorf("%s shard NumCompactors = %d, want %d", label, opts.NumCompactors, compactors)
		}
	}

	check("reference", ts.RefShardForTest())
	ts.MuForTest().RLock()
	check("hot", ts.HotShardForTest().Store())
	ts.MuForTest().RUnlock()

	time.Sleep(2 * time.Millisecond)
	if err := ts.RotateHotShard(); err != nil {
		t.Fatalf("rotate: %v", err)
	}
	var warmChecked bool
	ts.MuForTest().RLock()
	for _, es := range ts.EventShardsForTest() {
		if es.Tier() == TierWarm && es.Store() != nil {
			check("warm", es.Store())
			warmChecked = true
		}
	}
	ts.MuForTest().RUnlock()
	if !warmChecked {
		t.Fatal("no open warm shard found to verify footprint passthrough")
	}
}

// TestTieredArchiveShardInheritsTuning closes the coverage gap on the one shard
// kind nothing else exercised: the reference archive. It opens through
// openBadgerStore -> badgerCfg, so it must inherit the footprint knobs and the
// cache byte budget like every other shard.
func TestTieredArchiveShardInheritsTuning(t *testing.T) {
	const (
		vlog       = 64 << 20
		memtable   = 16 << 20
		cache      = 32 << 20
		compactors = 3
		budget     = int64(4 << 20)
	)
	ts, err := New(Config{
		DataDir:          t.TempDir(),
		RefLabels:        []string{"Case", "User"},
		ShardWindow:      7 * 24 * time.Hour,
		FlushInterval:    1<<63 - 1,
		ValueLogFileSize: vlog,
		MemTableSize:     memtable,
		BlockCacheSize:   cache,
		NumCompactors:    compactors,
		CacheBudgetBytes: budget,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer ts.Close()

	if err := ts.EnsureRefArchiveForTest(); err != nil {
		t.Fatalf("EnsureRefArchiveForTest: %v", err)
	}
	archive := ts.RefArchiveForTest().Load()
	if archive == nil {
		t.Fatal("archive shard nil after EnsureRefArchiveForTest")
	}
	opts := archive.DBForTest().Opts()
	if opts.ValueLogFileSize != vlog {
		t.Errorf("archive ValueLogFileSize = %d, want %d", opts.ValueLogFileSize, vlog)
	}
	if opts.MemTableSize != memtable {
		t.Errorf("archive MemTableSize = %d, want %d", opts.MemTableSize, memtable)
	}
	if opts.BlockCacheSize != cache {
		t.Errorf("archive BlockCacheSize = %d, want %d", opts.BlockCacheSize, cache)
	}
	if opts.NumCompactors != compactors {
		t.Errorf("archive NumCompactors = %d, want %d", opts.NumCompactors, compactors)
	}
	if got := archive.NodeCacheForTest().Budget(); got != budget {
		t.Errorf("archive node-cache budget = %d, want %d", got, budget)
	}
}

// TestTieredColdShardRetroLinkWrite is the §4d safety check for enabling
// ColdAfter/IdleTimeout: a write that must touch a cold shard. An old event
// node is rotated to a cold tier, then a current reference node is retro-linked
// to it — the relationship write must reach the cold shard's incoming adjacency
// and be queryable afterward.
func TestTieredColdShardRetroLinkWrite(t *testing.T) {
	dir := t.TempDir()
	ts, err := New(Config{
		DataDir:       dir,
		RefLabels:     []string{"Case", "User"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer ts.Close()
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	nodeGen := tieredNodeGen(t)
	relGen := tieredRelGen(t)

	// Old event node E on the hot shard.
	e := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	if err := ts.PutNode(e); err != nil {
		t.Fatalf("put event node: %v", err)
	}
	ts.MuForTest().RLock()
	hotName := ts.HotShardForTest().Name()
	_ = ts.HotShardForTest().Store().Flush()
	ts.MuForTest().RUnlock()

	// Rotate hot -> warm, demote to cold: E now lives on a cold shard.
	time.Sleep(2 * time.Millisecond)
	if err := ts.RotateHotShard(); err != nil {
		t.Fatalf("rotate: %v", err)
	}
	demoteToCold(ts, hotName)

	// Current reference node R.
	r := types.NewNode(types.NodeID(nodeGen.Generate()), caseTok, nil)
	if err := ts.PutNode(r); err != nil {
		t.Fatalf("put ref node: %v", err)
	}

	// Retro-link R -> E: a cross-shard write reaching the cold shard.
	rel := types.NewRelationship(types.RelID(relGen.Generate()), 1, r.ID(), e.ID())
	if err := ts.PutRelationship(rel); err != nil {
		t.Fatalf("retro-link write to cold shard: %v", err)
	}

	if _, err := ts.GetRelationship(rel.ID()); err != nil {
		t.Fatalf("GetRelationship after cold cross-shard write: %v", err)
	}
	in, err := ts.IncomingRelationships(e.ID(), 1)
	if err != nil {
		t.Fatalf("IncomingRelationships of cold-shard node: %v", err)
	}
	found := false
	for _, x := range in {
		if x.ID() == rel.ID() {
			found = true
		}
	}
	if !found {
		t.Fatal("retro-link relationship missing from cold-shard node incoming adjacency")
	}
}

// TestTieredOversizedWALMigratedOnWarmReopen is the faithful tiered reproduction
// of the production failure: a DataDir snapshotted from a RUNNING server holds a
// warm-shard WAL written under a larger memtable. Reopening at a smaller
// MemTableSize must NOT crash — openBadgerStoreWithRecovery migrates the
// oversized WAL before its read-only probe, then serves every record.
func TestTieredOversizedWALMigratedOnWarmReopen(t *testing.T) {
	if testing.Short() {
		t.Skip("writes ~30MB through a 64MB memtable and copies the live DataDir")
	}
	srcDir := t.TempDir()
	ts1, err := New(Config{
		DataDir:       srcDir,
		RefLabels:     []string{"Case", "User"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
		MemTableSize:  64 << 20, // large: the payload stays in the live WAL
	})
	if err != nil {
		t.Fatalf("New (source): %v", err)
	}
	reg := registrypkg.NewLabelRegistry()
	ts1.SetLabelRegistry(reg)
	_, _ = reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	pad := strings.Repeat("y", 30*1024)
	const n = 1000
	ids := make([]types.NodeID, 0, n)
	for i := 0; i < n; i++ {
		node := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
		if err := node.SetProperties(mustTieredProps(t, map[string]any{"i": i, "pad": pad})); err != nil {
			t.Fatalf("set props: %v", err)
		}
		if err := ts1.PutNode(node); err != nil {
			t.Fatalf("put event node %d: %v", i, err)
		}
		ids = append(ids, node.ID())
	}

	// Drain into the hot shard's WAL, then rotate so the WAL-laden shard becomes
	// the WARM shard (warm shards reopen via openBadgerStoreWithRecovery).
	ts1.MuForTest().RLock()
	hotName := ts1.HotShardForTest().Name()
	if err := ts1.HotShardForTest().Store().Flush(); err != nil {
		ts1.MuForTest().RUnlock()
		t.Fatalf("flush hot shard: %v", err)
	}
	ts1.MuForTest().RUnlock()
	time.Sleep(2 * time.Millisecond)
	if err := ts1.RotateHotShard(); err != nil {
		t.Fatalf("rotate: %v", err)
	}

	// Snapshot the LIVE DataDir (ts1 still open) — a crash-consistent copy.
	dstDir := t.TempDir()
	copyTree(t, srcDir, dstDir)
	if err := ts1.Close(); err != nil {
		t.Fatalf("close source: %v", err)
	}

	// Precondition: the copied warm shard must carry an oversized WAL.
	warmDir := filepath.Join(dstDir, "events", hotName)
	if got := maxMemFileSizeTiered(t, warmDir); got <= 2*(16<<20) {
		t.Fatalf("fixture invalid: warm shard %s has no oversized WAL (max .mem = %d)", warmDir, got)
	}

	// Reopen at the SMALLER memtable. Without the pre-probe migration the warm
	// shard's read-only recovery probe would terminate the process.
	ts2, err := New(Config{
		DataDir:       dstDir,
		RefLabels:     []string{"Case", "User"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
		MemTableSize:  16 << 20,
	})
	if err != nil {
		t.Fatalf("reopen at smaller memtable: %v", err)
	}
	defer ts2.Close()
	for i, id := range ids {
		if _, err := ts2.GetNode(id); err != nil {
			t.Fatalf("event node %d lost across tiered WAL migration: %v", i, err)
		}
	}
	if got := maxMemFileSizeTiered(t, warmDir); got > 2*(16<<20) {
		t.Fatalf("oversized warm-shard WAL (%d bytes) survived the migration", got)
	}
}

// --- helpers ----------------------------------------------------------------

func mustTieredProps(t *testing.T, m map[string]any) types.PropertySlice {
	t.Helper()
	ps, err := types.NewPropertySlice(m)
	if err != nil {
		t.Fatalf("NewPropertySlice: %v", err)
	}
	return ps
}

// copyTree recursively copies the directory tree at src to dst (the tiered
// DataDir has subdirectories: meta, reference, events/<name>, archive).
func copyTree(t *testing.T, src, dst string) {
	t.Helper()
	err := filepath.WalkDir(src, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if d.IsDir() {
			return os.MkdirAll(target, 0o750)
		}
		in, err := os.Open(path)
		if err != nil {
			return err
		}
		defer in.Close()
		out, err := os.Create(target)
		if err != nil {
			return err
		}
		defer out.Close()
		_, err = io.Copy(out, in)
		return err
	})
	if err != nil {
		t.Fatalf("copyTree: %v", err)
	}
}

func maxMemFileSizeTiered(t *testing.T, dir string) int64 {
	t.Helper()
	return maxFileSizeWithExtTiered(t, dir, ".mem")
}

func maxFileSizeWithExtTiered(t *testing.T, dir, ext string) int64 {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var max int64
	for _, e := range entries {
		if filepath.Ext(e.Name()) != ext {
			continue
		}
		info, err := e.Info()
		if err != nil {
			t.Fatal(err)
		}
		if info.Size() > max {
			max = info.Size()
		}
	}
	return max
}
