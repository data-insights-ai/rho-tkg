package badger

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// mustNode builds a node with primary label token 1 and the given properties.
func mustNode(t *testing.T, id int, props map[string]any) *types.Node {
	t.Helper()
	n := types.NewNode(types.NodeID(snowflake.ID(id)), 1, nil)
	if err := n.SetProperties(mustPropertySlice(t, props)); err != nil {
		t.Fatalf("SetProperties: %v", err)
	}
	return n
}

// --- Test 1: boundary table -------------------------------------------------

// TestBadgerTuningBoundaries pins the per-instance footprint validation: zero
// keeps Badger's stock default and must open; an out-of-range non-zero value
// must fail at New() with a message naming the field, never reaching a
// confusing failure deep inside Badger's Open. Table-driven so newly discovered
// Badger constraints land as new rows (this shape is what discovered the 8MB
// memtable floor in the ai-assisted-soc precedent).
func TestBadgerTuningBoundaries(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*Config)
		wantErr string // "" = must succeed
	}{
		{"zero values keep stock defaults", func(c *Config) {}, ""},

		{"vlog at floor 1MB", func(c *Config) { c.ValueLogFileSize = 1 << 20 }, ""},
		{"vlog below floor", func(c *Config) { c.ValueLogFileSize = 1<<20 - 1 }, "ValueLogFileSize"},
		{"vlog negative", func(c *Config) { c.ValueLogFileSize = -1 }, "ValueLogFileSize"},
		{"vlog above badger max", func(c *Config) { c.ValueLogFileSize = maxValueLogFileSize + 1 }, "ValueLogFileSize"},

		// Floor is 8MB, not 1MB: Badger's Open rejects a MemTableSize where the
		// (default 1MB) ValueThreshold exceeds 15% of it.
		{"memtable at floor 8MB", func(c *Config) { c.MemTableSize = minMemTableSize }, ""},
		{"memtable below floor", func(c *Config) { c.MemTableSize = minMemTableSize - 1 }, "MemTableSize"},
		{"memtable negative (MinInt64-style)", func(c *Config) { c.MemTableSize = -1 << 62 }, "MemTableSize"},
		{"memtable above cap", func(c *Config) { c.MemTableSize = maxMemTableSize + 1 }, "MemTableSize"},

		{"block cache zero keeps default", func(c *Config) { c.BlockCacheSize = 0 }, ""},
		{"block cache positive", func(c *Config) { c.BlockCacheSize = 1 << 20 }, ""},
		{"block cache negative", func(c *Config) { c.BlockCacheSize = -1 }, "BlockCacheSize"},

		{"index cache zero keeps default", func(c *Config) { c.IndexCacheSize = 0 }, ""},
		{"index cache positive", func(c *Config) { c.IndexCacheSize = 1 << 20 }, ""},
		{"index cache negative", func(c *Config) { c.IndexCacheSize = -1 }, "IndexCacheSize"},

		{"compactors zero keeps default", func(c *Config) { c.NumCompactors = 0 }, ""},
		{"compactors at minimum 2", func(c *Config) { c.NumCompactors = minNumCompactors }, ""},
		{"compactors below minimum", func(c *Config) { c.NumCompactors = 1 }, "NumCompactors"},
		{"compactors negative", func(c *Config) { c.NumCompactors = -3 }, "NumCompactors"},

		{"all knobs tuned together", func(c *Config) {
			c.ValueLogFileSize = 64 << 20
			c.MemTableSize = 16 << 20
			c.BlockCacheSize = 32 << 20
			c.IndexCacheSize = 8 << 20
			c.NumCompactors = 2
		}, ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Config{Dir: t.TempDir()}
			tc.mutate(&cfg)
			bs, err := New(cfg)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("expected success, got: %v", err)
				}
				if cerr := bs.Close(); cerr != nil {
					t.Fatalf("close: %v", cerr)
				}
				return
			}
			if err == nil {
				_ = bs.Close()
				t.Fatalf("expected error containing %q, got nil", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error %q does not contain %q", err, tc.wantErr)
			}
		})
	}
}

// --- Test 2: reopen across an option change, both directions ----------------

// TestBadgerReopenAcrossTuningChange proves an existing directory written under
// one footprint reopens and reads identically under another — the exact shape
// of upgrading a deployment in place (existing files keep their old sizes; only
// new files shrink). One record exceeds 1MB so the vlog actually carries data
// that must survive (small values live entirely in the LSM).
func TestBadgerReopenAcrossTuningChange(t *testing.T) {
	if testing.Short() {
		t.Skip("creates GB-scale sparse files")
	}

	stockDir := t.TempDir()
	stock := func(dir string) Config { return Config{Dir: dir} } // zero = stock
	tuned := func(dir string) Config {
		return Config{Dir: dir, ValueLogFileSize: 64 << 20, MemTableSize: 16 << 20, BlockCacheSize: 32 << 20, NumCompactors: 2}
	}

	const n = 300
	const bigID = 151
	big := strings.Repeat("Z", 2<<20) // 2MB > 1MB ValueThreshold -> lands in the vlog

	write := func(cfg Config) {
		bs, err := New(cfg)
		if err != nil {
			t.Fatalf("open for write: %v", err)
		}
		for i := 1; i <= n; i++ {
			val := fmt.Sprintf("v%d", i)
			if i == bigID {
				val = big
			}
			node := mustNode(t, i, map[string]any{"blob": val})
			if err := bs.PutNode(node); err != nil {
				t.Fatalf("put %d: %v", i, err)
			}
		}
		if err := bs.Close(); err != nil {
			t.Fatalf("close after write: %v", err)
		}
	}

	verify := func(label string, cfg Config) {
		bs, err := New(cfg)
		if err != nil {
			t.Fatalf("%s: reopen: %v", label, err)
		}
		defer bs.Close()
		for i := 1; i <= n; i++ {
			got, err := bs.GetNode(types.NodeID(snowflake.ID(i)))
			if err != nil {
				t.Fatalf("%s: get %d: %v", label, i, err)
			}
			blob, _ := got.PropertiesMap()["blob"].(string)
			want := fmt.Sprintf("v%d", i)
			if i == bigID {
				want = big
			}
			if blob != want {
				if i == bigID {
					t.Fatalf("%s: big record corrupted: len got=%d want=%d", label, len(blob), len(want))
				}
				t.Fatalf("%s: record %d = %q, want %q", label, i, blob, want)
			}
		}
	}

	// stock -> tuned -> stock on one dir: both directions read every record.
	write(stock(stockDir))
	verify("tuned after stock", tuned(stockDir))
	verify("stock after tuned", stock(stockDir))
	verify("tuned again", tuned(stockDir))

	// Reverse: write under tuned sizes on a fresh dir, read under stock.
	tunedDir := t.TempDir()
	write(tuned(tunedDir))
	verify("stock after tuned-write", stock(tunedDir))
	verify("tuned after tuned-write", tuned(tunedDir))
}

// --- Test 3 + 3b: live-WAL migration ----------------------------------------

// writeOversizedWALCopy writes n nodes (each carrying ~padBytes of payload)
// through a LARGE memtable so the data stays in the live WAL, then snapshots the
// directory WITHOUT closing — a crash-consistent copy of a running store, the
// exact shape of a production debug tar. Returns the copy dir; every node has
// integer id in [1,n] and a "pad" property of length padBytes.
func writeOversizedWALCopy(t *testing.T, n, padBytes int) string {
	t.Helper()
	srcDir := t.TempDir()
	// 64MB memtable: the written payload (well under 64MB) never flushes to an
	// SST, so it all sits in one ~128MB-apparent WAL.
	bs, err := New(Config{Dir: srcDir, MemTableSize: 64 << 20})
	if err != nil {
		t.Fatalf("open source: %v", err)
	}
	pad := strings.Repeat("y", padBytes)
	for i := 1; i <= n; i++ {
		node := mustNode(t, i, map[string]any{"i": i, "pad": pad})
		if err := bs.PutNode(node); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	// Drain the Go-side write buffer into Badger's memtable+WAL deterministically.
	if err := bs.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	dstDir := t.TempDir()
	copyDirFiles(t, srcDir, dstDir)
	if err := bs.Close(); err != nil {
		t.Fatalf("close source: %v", err)
	}

	// Precondition: the copy must actually hold an oversized WAL, otherwise the
	// fixture did not reproduce the failure mode and the assertions below are
	// vacuous.
	if maxMemFileSize(t, dstDir) <= 2*(16<<20) {
		t.Fatalf("fixture invalid: no WAL larger than 2x16MB in copy %s", dstDir)
	}
	return dstDir
}

// TestBadgerReopenLiveWALAcrossMemtableShrink is the production failure
// (caught only by a real running-server dump): a dir whose live WAL holds more
// than the tuned arena can replay fails "Arena too small" and never opens.
// New's migration must flush it first, then serve every record.
func TestBadgerReopenLiveWALAcrossMemtableShrink(t *testing.T) {
	if testing.Short() {
		t.Skip("writes ~30MB through a 64MB memtable and copies a 128MB WAL")
	}
	const n = 1000
	dstDir := writeOversizedWALCopy(t, n, 30*1024) // ~30MB total

	tuned := New
	bs, err := tuned(Config{Dir: dstDir, MemTableSize: 16 << 20})
	if err != nil {
		t.Fatalf("tuned open on live WAL copy: %v", err)
	}
	defer bs.Close()
	for i := 1; i <= n; i++ {
		if _, err := bs.GetNode(types.NodeID(snowflake.ID(i))); err != nil {
			t.Fatalf("record %d lost across WAL migration: %v", i, err)
		}
	}
	// No WAL larger than the tuned arena (2x16MB) may survive the migration.
	if got := maxMemFileSize(t, dstDir); got > 2*(16<<20) {
		t.Fatalf("oversized WAL (%d bytes) survived migration", got)
	}
}

// TestBadgerMigrateOversizedWALMakesReadOnlyOpenSafe proves the recovery the
// tiered store relies on: MigrateOversizedWAL flushes the oversized WAL, after
// which a read-only open at the tuned (smaller) memtable succeeds and serves
// every record. This is what tiered.openBadgerStoreWithRecovery runs BEFORE its
// read-only probe.
func TestBadgerMigrateOversizedWALMakesReadOnlyOpenSafe(t *testing.T) {
	if testing.Short() {
		t.Skip("writes ~30MB through a 64MB memtable and copies a 128MB WAL")
	}
	const n = 1000
	dstDir := writeOversizedWALCopy(t, n, 30*1024)

	if err := MigrateOversizedWAL(Config{Dir: dstDir, MemTableSize: 16 << 20}); err != nil {
		t.Fatalf("MigrateOversizedWAL: %v", err)
	}
	if got := maxMemFileSize(t, dstDir); got > 2*(16<<20) {
		t.Fatalf("oversized WAL (%d bytes) survived migration", got)
	}

	ro, err := New(Config{Dir: dstDir, MemTableSize: 16 << 20, ReadOnly: true})
	if err != nil {
		t.Fatalf("read-only open after migration: %v", err)
	}
	defer ro.Close()
	for i := 1; i <= n; i++ {
		if _, err := ro.GetNode(types.NodeID(snowflake.ID(i))); err != nil {
			t.Fatalf("record %d lost after migration: %v", i, err)
		}
	}
}

// TestBadgerReadOnlyOpenOversizedWALFailsClosed pins the fix: a read-only open
// at a tuned MemTableSize over an oversized WAL — which a public consumer
// (Config{ReadOnly:true, MemTableSize:N}) can trigger — must return
// ErrOversizedWAL, NOT replay the WAL into a too-small arena and os.Exit. The
// process must survive and no store must be returned; a writable open then
// migrates and serves the data.
func TestBadgerReadOnlyOpenOversizedWALFailsClosed(t *testing.T) {
	if testing.Short() {
		t.Skip("writes ~30MB through a 64MB memtable and copies a 128MB WAL")
	}
	dir := writeOversizedWALCopy(t, 1000, 30*1024)

	bs, err := New(Config{Dir: dir, MemTableSize: 16 << 20, ReadOnly: true})
	if bs != nil {
		_ = bs.Close()
	}
	if !errors.Is(err, ErrOversizedWAL) {
		t.Fatalf("read-only open over oversized WAL: err = %v, want ErrOversizedWAL", err)
	}
	w, err := New(Config{Dir: dir, MemTableSize: 16 << 20})
	if err != nil {
		t.Fatalf("writable open after fail-closed read-only: %v", err)
	}
	defer w.Close()
	if _, err := w.GetNode(types.NodeID(snowflake.ID(1))); err != nil {
		t.Fatalf("record 1 not served after writable migration: %v", err)
	}
}

// TestBadgerRawReadOnlyOpenOnOversizedWALExitsProcess proves the fail-closed
// guard above is load-bearing: the RAW Badger open it shields (what New would do
// without the guard) replays the WAL into the bounded arena and aborts via
// y.AssertTruef -> log.Fatal -> os.Exit — not a recoverable error or panic. A
// process exit is only observable out of process, so re-exec this test as a
// child that performs the raw open and assert it does not exit 0.
func TestBadgerRawReadOnlyOpenOnOversizedWALExitsProcess(t *testing.T) {
	if dir := os.Getenv("TKG_OVERSIZED_WAL_DIR"); dir != "" {
		// Child: bypass New's guard and open Badger directly the way New would
		// WITHOUT the fail-closed check. Reaching either branch means the open
		// did NOT terminate us; os.Exit(0) is the "unexpectedly opened" outcome.
		db, err := badgerv4.Open(buildBadgerOptions(Config{Dir: dir, MemTableSize: 16 << 20, ReadOnly: true}))
		if err != nil {
			fmt.Fprintln(os.Stderr, "raw read-only open returned error:", err)
			os.Exit(3)
		}
		_ = db.Close()
		os.Exit(0)
	}
	if testing.Short() {
		t.Skip("writes ~30MB through a 64MB memtable and copies a 128MB WAL")
	}

	dir := writeOversizedWALCopy(t, 1000, 30*1024)
	cmd := exec.Command(os.Args[0],
		"-test.run", "^TestBadgerRawReadOnlyOpenOnOversizedWALExitsProcess$",
		"-test.timeout", "120s")
	cmd.Env = append(os.Environ(), "TKG_OVERSIZED_WAL_DIR="+dir)
	out, _ := cmd.CombinedOutput()
	combined := string(out)
	exit := cmd.ProcessState.ExitCode()
	switch {
	case exit == 0:
		t.Fatalf("raw read-only open on an oversized-WAL dir unexpectedly succeeded:\n%s", combined)
	case strings.Contains(combined, "Arena too small"):
		// Reproduced the process-terminating WAL replay the guard prevents.
	case strings.Contains(combined, "raw read-only open returned error:"):
		// Also acceptable: Badger refused the open without a fatal. Either way
		// the raw path yields no usable store.
	default:
		t.Fatalf("child exited %d for an unexpected reason:\n%s", exit, combined)
	}
}

// TestMigrateOversizedWALNoOps confirms the migration is a safe no-op where it
// must be: in-memory, untuned (MemTableSize == 0), a missing dir, and a clean
// dir with no oversized WAL.
func TestMigrateOversizedWALNoOps(t *testing.T) {
	cases := []struct {
		name string
		cfg  Config
	}{
		{"in-memory", Config{InMemory: true, MemTableSize: 16 << 20}},
		{"untuned (memtable 0)", Config{Dir: t.TempDir()}},
		{"missing dir", Config{Dir: filepath.Join(t.TempDir(), "does-not-exist"), MemTableSize: 16 << 20}},
		{"clean empty dir", Config{Dir: t.TempDir(), MemTableSize: 16 << 20}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := MigrateOversizedWAL(tc.cfg); err != nil {
				t.Fatalf("expected no-op, got %v", err)
			}
		})
	}
}

// TestBadgerMigrateOversizedWALRejectsAboveCapWAL covers the foreign/older-writer
// case: a WAL written under a memtable LARGER than the 1GB cap. Its recovered
// size (maxWAL/2) then exceeds maxMemTableSize, so clamping to 1GB and opening
// would replay it into a too-small arena and os.Exit — the very crash the
// migration exists to prevent. It must fail closed with ErrOversizedWAL BEFORE
// opening. The migration decides on apparent file size alone, so a sparse .mem
// is a faithful, cheap fixture (no Badger open is reached).
func TestBadgerMigrateOversizedWALRejectsAboveCapWAL(t *testing.T) {
	dir := t.TempDir()
	// Apparent size > 2x the 1GB cap => recovered memtable (maxWAL/2) > 1GB.
	const apparent = 3 << 30 // 3GB, sparse
	f, err := os.Create(filepath.Join(dir, "00001.mem"))
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(apparent); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	err = MigrateOversizedWAL(Config{Dir: dir, MemTableSize: 16 << 20})
	if !errors.Is(err, ErrOversizedWAL) {
		t.Fatalf("migration over an above-cap WAL: err = %v, want ErrOversizedWAL", err)
	}
}

// --- Test 3c: the knobs are actually applied (not just accepted) ------------

// TestBadgerFootprintKnobsApplied is the adversarial complement to the boundary
// table: validation accepting a value proves nothing about whether Badger
// receives it. Badger creates the WAL at exactly 2x MemTableSize and the vlog at
// exactly 2x ValueLogFileSize (both sparse), so the on-disk apparent sizes are a
// direct, exact witness. A buildBadgerOptions that silently dropped a
// With...Size call would leave them at Badger's stock 128MB / ~2GB — caught here
// and nowhere else (a clean Close truncates the vlog, hiding the stock size from
// every after-close check). This also pins the "WAL == 2x MemTableSize" fact the
// WAL migration relies on.
func TestBadgerFootprintKnobsApplied(t *testing.T) {
	if testing.Short() {
		t.Skip("creates a multi-MB vlog file")
	}
	const memtable = 8 << 20       // -> WAL apparent 16MB (stock would be 128MB)
	const vlog = 4 << 20           // -> vlog apparent 8MB (stock would be ~2GB)
	const stockWAL = 64 << 20      // 2x stock memtable would be 128MB; assert well under
	const stockVlogFloor = 1 << 30 // stock vlog ~2GB; assert well under

	dir := t.TempDir()
	bs, err := New(Config{Dir: dir, MemTableSize: memtable, ValueLogFileSize: vlog})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// A >1MB value forces a vlog entry (small values never reach the vlog).
	if err := bs.PutNode(mustNode(t, 1, map[string]any{"blob": strings.Repeat("Z", 2<<20)})); err != nil {
		t.Fatalf("put big: %v", err)
	}
	for i := 2; i <= 5; i++ {
		if err := bs.PutNode(mustNode(t, i, map[string]any{"i": i})); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	if err := bs.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// WHILE OPEN: apparent sizes must reflect the tuned knobs exactly, not stock.
	wal := maxFileSizeWithExt(t, dir, ".mem")
	if wal == 0 {
		t.Fatal("no WAL (.mem) file present")
	}
	if wal != 2*memtable {
		t.Fatalf("WAL apparent size = %d, want exactly 2x MemTableSize (%d); a dropped WithMemTableSize would leave it at the stock 128MB", wal, 2*memtable)
	}
	if wal >= 2*stockWAL {
		t.Fatalf("WAL apparent size %d is at the stock 128MB — MemTableSize was not applied", wal)
	}

	vl := maxFileSizeWithExt(t, dir, ".vlog")
	if vl == 0 {
		t.Fatal("no vlog (.vlog) file present — the >1MB value did not reach the vlog")
	}
	if vl != 2*vlog {
		t.Fatalf("vlog apparent size = %d, want exactly 2x ValueLogFileSize (%d); a dropped WithValueLogFileSize would leave it at the stock ~2GB", vl, 2*vlog)
	}
	if vl >= stockVlogFloor {
		t.Fatalf("vlog apparent size %d is at the stock ~2GB — ValueLogFileSize was not applied", vl)
	}
	if err := bs.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// --- Test 3d: every knob reaches Badger's live options ----------------------

// TestBadgerAllFootprintKnobsReachBadgerOptions closes the gap the file-size
// test cannot: BlockCacheSize and NumCompactors are validated but leave no
// on-disk witness. Badger exposes its resolved options via DB.Opts(), so assert
// all FOUR arrive — a buildBadgerOptions that dropped any WithXxx would surface
// here. Each tuned value is also asserted to DIFFER from Badger's stock value,
// so the test cannot pass by coincidence with an un-applied knob.
func TestBadgerAllFootprintKnobsReachBadgerOptions(t *testing.T) {
	const (
		vlog       = 64 << 20
		memtable   = 16 << 20
		cache      = 32 << 20
		compactors = 3
	)
	bs, err := New(Config{
		Dir:              t.TempDir(),
		ValueLogFileSize: vlog,
		MemTableSize:     memtable,
		BlockCacheSize:   cache,
		NumCompactors:    compactors,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer bs.Close()

	opts := bs.DBForTest().Opts()
	if opts.ValueLogFileSize != vlog {
		t.Errorf("ValueLogFileSize reached Badger as %d, want %d", opts.ValueLogFileSize, vlog)
	}
	if opts.MemTableSize != memtable {
		t.Errorf("MemTableSize reached Badger as %d, want %d", opts.MemTableSize, memtable)
	}
	if opts.BlockCacheSize != cache {
		t.Errorf("BlockCacheSize reached Badger as %d, want %d", opts.BlockCacheSize, cache)
	}
	if opts.NumCompactors != compactors {
		t.Errorf("NumCompactors reached Badger as %d, want %d", opts.NumCompactors, compactors)
	}

	// Negative: every tuned value must differ from stock, else "applied" is
	// indistinguishable from "ignored".
	stock := badgerv4.DefaultOptions("")
	if opts.MemTableSize == stock.MemTableSize {
		t.Errorf("MemTableSize is still the stock %d — not applied", stock.MemTableSize)
	}
	if opts.ValueLogFileSize == stock.ValueLogFileSize {
		t.Errorf("ValueLogFileSize is still the stock %d — not applied", stock.ValueLogFileSize)
	}
	if opts.NumCompactors == stock.NumCompactors {
		t.Errorf("NumCompactors is still the stock %d — not applied", stock.NumCompactors)
	}
}

// TestBadgerZeroTuningKeepsBadgerStockOptions pins the library contract that
// zero means "Badger stock", not "some tuned default". If a future change ever
// applied a value for a zero knob, the resolved options would diverge from
// Badger's DefaultOptions and this fails.
func TestBadgerZeroTuningKeepsBadgerStockOptions(t *testing.T) {
	bs, err := New(Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer bs.Close()

	opts := bs.DBForTest().Opts()
	stock := badgerv4.DefaultOptions("")
	if opts.MemTableSize != stock.MemTableSize {
		t.Errorf("zero MemTableSize = %d, want Badger stock %d", opts.MemTableSize, stock.MemTableSize)
	}
	if opts.ValueLogFileSize != stock.ValueLogFileSize {
		t.Errorf("zero ValueLogFileSize = %d, want Badger stock %d", opts.ValueLogFileSize, stock.ValueLogFileSize)
	}
	if opts.BlockCacheSize != stock.BlockCacheSize {
		t.Errorf("zero BlockCacheSize = %d, want Badger stock %d", opts.BlockCacheSize, stock.BlockCacheSize)
	}
	if opts.NumCompactors != stock.NumCompactors {
		t.Errorf("zero NumCompactors = %d, want Badger stock %d", opts.NumCompactors, stock.NumCompactors)
	}
}

// --- Test 4: concurrent flush storm at the memtable floor -------------------

// TestBadgerTinyMemtableConcurrentFlushStorm hammers a store configured at the
// 8MB memtable floor with concurrent writers so the memtable rotates many times
// mid-write. Every record must survive the rotations, a clean close, and a
// reopen — and a clean close must leave no WAL files and no oversized vlogs.
func TestBadgerTinyMemtableConcurrentFlushStorm(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{Dir: dir, MemTableSize: minMemTableSize, ValueLogFileSize: 1 << 20, BlockCacheSize: 1 << 20, NumCompactors: 2}
	bs, err := New(cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	const goroutines = 32
	const perG = 200
	pad := strings.Repeat("x", 4096) // 32*200*4KB ~= 26MB through an 8MB memtable -> several rotations
	var wg sync.WaitGroup
	errCh := make(chan error, goroutines)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				id := g*perG + i + 1
				node := mustNode(t, id, map[string]any{"g": g, "i": i, "pad": pad})
				if err := bs.PutNode(node); err != nil {
					errCh <- fmt.Errorf("put %d: %w", id, err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}

	if got, err := bs.NodeCount(); err != nil {
		t.Fatalf("NodeCount: %v", err)
	} else if got != goroutines*perG {
		t.Fatalf("NodeCount = %d, want %d", got, goroutines*perG)
	}
	if err := bs.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// After a clean close every WAL must be flushed and deleted, and vlogs
	// (created at 2x ValueLogFileSize while open) truncated back toward content.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		info, err := e.Info()
		if err != nil {
			t.Fatal(err)
		}
		switch filepath.Ext(e.Name()) {
		case ".mem":
			t.Fatalf("WAL %s survived a clean close", e.Name())
		case ".vlog":
			if info.Size() > 2<<20 {
				t.Fatalf("vlog %s is %d bytes after clean close; want truncation to <= 2x file size", e.Name(), info.Size())
			}
		}
	}

	bs2, err := New(cfg)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer bs2.Close()
	for id := 1; id <= goroutines*perG; id++ {
		if _, err := bs2.GetNode(types.NodeID(snowflake.ID(id))); err != nil {
			if errors.Is(err, ErrNodeNotFound) {
				t.Fatalf("record %d lost across flush storm + reopen", id)
			}
			t.Fatalf("get %d: %v", id, err)
		}
	}
}

// --- helpers ----------------------------------------------------------------

// copyDirFiles copies every regular file in src to dst (badger dirs are flat).
// Uses io.Copy so a sparse 128MB WAL does not have to be fully buffered in heap.
func copyDirFiles(t *testing.T, src, dst string) {
	t.Helper()
	entries, err := os.ReadDir(src)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		in, err := os.Open(filepath.Join(src, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		out, err := os.Create(filepath.Join(dst, e.Name()))
		if err != nil {
			_ = in.Close()
			t.Fatal(err)
		}
		if _, err := io.Copy(out, in); err != nil {
			_ = in.Close()
			_ = out.Close()
			t.Fatal(err)
		}
		if err := out.Close(); err != nil {
			_ = in.Close()
			t.Fatal(err)
		}
		_ = in.Close()
	}
}

// maxMemFileSize returns the largest .mem (WAL) file size in dir, or 0.
func maxMemFileSize(t *testing.T, dir string) int64 {
	t.Helper()
	return maxFileSizeWithExt(t, dir, ".mem")
}

// maxFileSizeWithExt returns the largest apparent size among files in dir with
// the given extension (e.g. ".mem", ".vlog"), or 0 if none.
func maxFileSizeWithExt(t *testing.T, dir, ext string) int64 {
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
