package graph_test

import (
	"context"
	"strings"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
)

// --- Option field assertions -------------------------------------------
//
// Per "each profile sets exactly the documented Config fields
// (construct Config via the options and assert fields — export nothing new
// for this; apply options to a Config directly)". These tests never open a
// real graph — they apply the Option function to a bare Config literal and
// inspect the resulting fields.

func TestWithProfileSmall_SetsDocumentedFields(t *testing.T) {
	t.Parallel()

	cfg := graphpkg.Config{}
	graphpkg.WithProfileSmall()(&cfg)

	const (
		wantVLog   = int64(64 * 1 << 20)
		wantMem    = int64(8 * 1 << 20)
		wantCache  = int64(32 * 1 << 20)
		wantCompac = 2
	)
	if cfg.ValueLogFileSize != wantVLog {
		t.Errorf("ValueLogFileSize = %d, want %d", cfg.ValueLogFileSize, wantVLog)
	}
	if cfg.MemTableSize != wantMem {
		t.Errorf("MemTableSize = %d, want %d", cfg.MemTableSize, wantMem)
	}
	if cfg.BlockCacheSize != wantCache {
		t.Errorf("BlockCacheSize = %d, want %d", cfg.BlockCacheSize, wantCache)
	}
	if cfg.NumCompactors != wantCompac {
		t.Errorf("NumCompactors = %d, want %d", cfg.NumCompactors, wantCompac)
	}

	// [4.8.0] validated bounds: vlog [1MB,2GB), memtable [8MB,1GB],
	// cache >= 0, compactors 0 or >= 2. Assert the profile respects them
	// directly (not just via a live New — see the bounds test below too).
	assertWithinBadgerFootprintBounds(t, cfg)
}

func TestWithProfileServer_ZeroesAllFootprintFields(t *testing.T) {
	t.Parallel()

	// Start from a config where every footprint field is already set to a
	// nonzero, non-default value — WithProfileServer must reset ALL FOUR to
	// zero (Badger stock), not merely leave zero fields untouched.
	cfg := graphpkg.Config{
		ValueLogFileSize: 999,
		MemTableSize:     999,
		BlockCacheSize:   999,
		NumCompactors:    999,
	}
	graphpkg.WithProfileServer()(&cfg)

	if cfg.ValueLogFileSize != 0 {
		t.Errorf("ValueLogFileSize = %d, want 0 (stock)", cfg.ValueLogFileSize)
	}
	if cfg.MemTableSize != 0 {
		t.Errorf("MemTableSize = %d, want 0 (stock)", cfg.MemTableSize)
	}
	if cfg.BlockCacheSize != 0 {
		t.Errorf("BlockCacheSize = %d, want 0 (stock)", cfg.BlockCacheSize)
	}
	if cfg.NumCompactors != 0 {
		t.Errorf("NumCompactors = %d, want 0 (stock)", cfg.NumCompactors)
	}
}

func TestWithProfileBulkLoad_SetsDocumentedFields(t *testing.T) {
	t.Parallel()

	cfg := graphpkg.Config{}
	graphpkg.WithProfileBulkLoad()(&cfg)

	const (
		wantVLog   = int64(512 * 1 << 20)
		wantMem    = int64(256 * 1 << 20)
		wantCompac = 4
	)
	if cfg.ValueLogFileSize != wantVLog {
		t.Errorf("ValueLogFileSize = %d, want %d", cfg.ValueLogFileSize, wantVLog)
	}
	if cfg.MemTableSize != wantMem {
		t.Errorf("MemTableSize = %d, want %d", cfg.MemTableSize, wantMem)
	}
	if cfg.NumCompactors != wantCompac {
		t.Errorf("NumCompactors = %d, want %d", cfg.NumCompactors, wantCompac)
	}

	assertWithinBadgerFootprintBounds(t, cfg)
}

func TestWithSnowflakeNodeID_SetsField(t *testing.T) {
	t.Parallel()

	cfg := graphpkg.Config{}
	graphpkg.WithSnowflakeNodeID(7)(&cfg)

	if cfg.SnowflakeNodeID != 7 {
		t.Errorf("SnowflakeNodeID = %d, want 7", cfg.SnowflakeNodeID)
	}
}

func TestWithValidation_SetsField(t *testing.T) {
	t.Parallel()

	v := graphpkg.ValidationLimits{
		MaxLabelsPerNode:       5,
		MaxPropertiesPerEntity: 10,
		MaxPropertyKeyLength:   20,
		MaxPropertyValueSize:   30,
		MaxNameLength:          40,
		AllowSelfLoops:         true,
	}
	cfg := graphpkg.Config{}
	graphpkg.WithValidation(v)(&cfg)

	if cfg.Validation != v {
		t.Errorf("Validation = %+v, want %+v", cfg.Validation, v)
	}
}

// assertWithinBadgerFootprintBounds is a static, non-opening check of the
// [4.8.0] validated bounds so a profile's field values are asserted twice:
// once here (pure, no I/O) and once by actually constructing a graph in
// TestProfiles_OpenSucceedsWithinBadgerBounds below.
func assertWithinBadgerFootprintBounds(t *testing.T, cfg graphpkg.Config) {
	t.Helper()
	const (
		vlogMin = int64(1) << 20 // 1MB
		vlogMax = int64(2) << 30 // 2GB (exclusive)
		memMin  = int64(8) << 20 // 8MB
		memMax  = int64(1) << 30 // 1GB (inclusive)
	)
	if cfg.ValueLogFileSize != 0 && (cfg.ValueLogFileSize < vlogMin || cfg.ValueLogFileSize >= vlogMax) {
		t.Errorf("ValueLogFileSize %d outside validated [1MB,2GB)", cfg.ValueLogFileSize)
	}
	if cfg.MemTableSize != 0 && (cfg.MemTableSize < memMin || cfg.MemTableSize > memMax) {
		t.Errorf("MemTableSize %d outside validated [8MB,1GB]", cfg.MemTableSize)
	}
	if cfg.BlockCacheSize < 0 {
		t.Errorf("BlockCacheSize %d must be >= 0", cfg.BlockCacheSize)
	}
	if cfg.NumCompactors != 0 && cfg.NumCompactors < 2 {
		t.Errorf("NumCompactors %d must be 0 or >= 2", cfg.NumCompactors)
	}
}

// --- Live bounds guard: each profile must not trip badger.New's [4.8.0]
// validation when actually opened. -------------------------------------

func TestProfiles_OpenSucceedsWithinBadgerBounds(t *testing.T) {
	t.Parallel()

	profiles := map[string]graphpkg.Option{
		"small":    graphpkg.WithProfileSmall(),
		"server":   graphpkg.WithProfileServer(),
		"bulkLoad": graphpkg.WithProfileBulkLoad(),
	}

	for name, opt := range profiles {
		t.Run(name+"/tempDir", func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			g, err := graphpkg.Open(dir, opt)
			if err != nil {
				t.Fatalf("Open(%s, %s profile): %v", dir, name, err)
			}
			t.Cleanup(func() { _ = g.Close() })
		})

		t.Run(name+"/inMemory", func(t *testing.T) {
			t.Parallel()

			// Apply the profile option to a BadgerInMemory config directly
			// (Open's dir argument only ever sets BadgerDir, never
			// BadgerInMemory) so the badger.New validation path — which
			// runs identically for BadgerInMemory and BadgerDir — is
			// exercised for every profile.
			cfg := graphpkg.Config{BadgerInMemory: true}
			opt(&cfg)
			g, err := graphpkg.New(cfg)
			if err != nil {
				t.Fatalf("New(BadgerInMemory, %s profile): %v", name, err)
			}
			t.Cleanup(func() { _ = g.Close() })
		})
	}
}

// --- Open("") / OpenInMemory equivalence --------------------------------

func TestOpenEmptyDir_EquivalentToOpenInMemory(t *testing.T) {
	t.Parallel()

	// Same SnowflakeNodeID applied through the same Option to both
	// constructors. If Open("") and OpenInMemory() take different code
	// paths to decide the backend, this would still pass (both are
	// memory-backed either way) — the point of this test is the pair of
	// assertions below (add/get roundtrip + minted-ID node-field parity)
	// which fail if either constructor silently picked a different
	// generator instance identity or backend behavior.
	g1, err := graphpkg.Open("", graphpkg.WithSnowflakeNodeID(3))
	if err != nil {
		t.Fatalf("Open(\"\"): %v", err)
	}
	defer func() { _ = g1.Close() }()

	g2, err := graphpkg.OpenInMemory(graphpkg.WithSnowflakeNodeID(3))
	if err != nil {
		t.Fatalf("OpenInMemory(): %v", err)
	}
	defer func() { _ = g2.Close() }()

	ctx := context.Background()
	n1, err := g1.Nodes().Add(ctx, []string{"Thing"}, map[string]any{"k": "v"})
	if err != nil {
		t.Fatalf("g1 Nodes.Add: %v", err)
	}
	n2, err := g2.Nodes().Add(ctx, []string{"Thing"}, map[string]any{"k": "v"})
	if err != nil {
		t.Fatalf("g2 Nodes.Add: %v", err)
	}

	// Both took the memory-store path (no BadgerDir/BadgerInMemory ever
	// got set) AND used the same SnowflakeNodeID, so both minted node IDs
	// decompose to the same node-field value (SnowflakeNodeID*2 for nodes).
	c1 := graphpkg.DecomposeNodeID(n1.ID())
	c2 := graphpkg.DecomposeNodeID(n2.ID())
	if c1.NodeID != c2.NodeID {
		t.Fatalf("NodeID component differs: Open(\"\")=%d OpenInMemory=%d", c1.NodeID, c2.NodeID)
	}
	const wantNodeField = int64(3 * 2)
	if c1.NodeID != wantNodeField {
		t.Fatalf("NodeID component = %d, want %d (SnowflakeNodeID*2)", c1.NodeID, wantNodeField)
	}

	// Independent stores: writing to g1 must not be visible from g2.
	if got, err := g2.Nodes().Get(ctx, n1.ID()); err == nil {
		t.Fatalf("g2 unexpectedly sees g1's node: %v", got)
	}
}

func TestOpenInMemory_Basic(t *testing.T) {
	t.Parallel()

	g, err := graphpkg.OpenInMemory()
	if err != nil {
		t.Fatalf("OpenInMemory: %v", err)
	}
	defer func() { _ = g.Close() }()

	ctx := context.Background()
	n, err := g.Nodes().Add(ctx, []string{"Person"}, map[string]any{"name": "Ada"})
	if err != nil {
		t.Fatalf("Nodes.Add: %v", err)
	}
	if cnt, err := g.Nodes().Count(); err != nil || cnt != 1 {
		t.Fatalf("Nodes.Count = (%d, %v), want (1, nil)", cnt, err)
	}
	got, err := g.Nodes().Get(ctx, n.ID())
	if err != nil || got == nil || got.ID() != n.ID() {
		t.Fatalf("Nodes.Get roundtrip failed: got=%v err=%v", got, err)
	}
}

// --- Open(dir) wires BadgerDir through to New ---------------------------

func TestOpen_WithDirIsBadgerBacked(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	g, err := graphpkg.Open(dir)
	if err != nil {
		t.Fatalf("Open(%s): %v", dir, err)
	}
	ctx := context.Background()
	n, err := g.Nodes().Add(ctx, []string{"Durable"}, map[string]any{"x": int64(1)})
	if err != nil {
		t.Fatalf("Nodes.Add: %v", err)
	}
	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen the same directory: the node must still be there, proving
	// Open(dir) actually set Config.BadgerDir (a MemoryStore would have
	// lost it).
	g2, err := graphpkg.Open(dir)
	if err != nil {
		t.Fatalf("Open(%s) reopen: %v", dir, err)
	}
	defer func() { _ = g2.Close() }()

	got, err := g2.Nodes().Get(ctx, n.ID())
	if err != nil || got == nil || got.ID() != n.ID() {
		t.Fatalf("reopened graph lost node: got=%v err=%v", got, err)
	}
}

// --- Whitespace-dir error parity with New -------------------------------

func TestOpen_WhitespaceDirParityWithNew(t *testing.T) {
	t.Parallel()

	whitespace := "   \t\n "

	_, newErr := graphpkg.New(graphpkg.Config{BadgerDir: whitespace})
	if newErr == nil {
		t.Fatal("New(BadgerDir: whitespace) should return an error")
	}

	_, openErr := graphpkg.Open(whitespace)
	if openErr == nil {
		t.Fatal("Open(whitespace) should return an error")
	}

	// No sentinel exists for this error (it is a plain fmt.Errorf in
	// core.New) — assert message parity instead of errors.Is.
	if newErr.Error() != openErr.Error() {
		t.Fatalf("New and Open disagree on the whitespace-dir error:\n  New:  %v\n  Open: %v", newErr, openErr)
	}
	if !strings.Contains(openErr.Error(), "whitespace-only") {
		t.Fatalf("Open error should mention whitespace-only, got: %v", openErr)
	}
}

// --- Options apply in order, later wins ---------------------------------

func TestOpen_OptionsApplyInOrder(t *testing.T) {
	t.Parallel()

	cfg := graphpkg.Config{}
	graphpkg.WithSnowflakeNodeID(1)(&cfg)
	graphpkg.WithSnowflakeNodeID(9)(&cfg)
	if cfg.SnowflakeNodeID != 9 {
		t.Fatalf("last option should win: SnowflakeNodeID = %d, want 9", cfg.SnowflakeNodeID)
	}

	g, err := graphpkg.OpenInMemory(graphpkg.WithSnowflakeNodeID(1), graphpkg.WithSnowflakeNodeID(9))
	if err != nil {
		t.Fatalf("OpenInMemory: %v", err)
	}
	defer func() { _ = g.Close() }()

	n, err := g.Nodes().Add(context.Background(), []string{"X"}, nil)
	if err != nil {
		t.Fatalf("Nodes.Add: %v", err)
	}
	got := graphpkg.DecomposeNodeID(n.ID())
	const wantNodeField = int64(9 * 2)
	if got.NodeID != wantNodeField {
		t.Fatalf("NodeID component = %d, want %d (last option, SnowflakeNodeID=9)", got.NodeID, wantNodeField)
	}
}
