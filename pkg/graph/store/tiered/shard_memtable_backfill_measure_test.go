//go:build measure

package tiered

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestWarmMemTableBackfillWrite is the price of a small WarmMemTableSize: a
// bulk write into a shard made on demand (a backfill of a past window) runs
// through the smaller memtable, which flushes to L0 more often. 60 MB of
// 100 KB rows, then Close, per size.
func TestWarmMemTableBackfillWrite(t *testing.T) {
	if testing.Short() || raceEnabled {
		t.Skip("measurement: not in -short, no race detector")
	}
	pad := strings.Repeat("y", 100<<10)
	var b strings.Builder
	fmt.Fprintf(&b, "\nbackfill of 600 x 100 KB rows into an on-demand window shard (hot MemTableSize 64 MB)\n")
	fmt.Fprintf(&b, "%-18s | %9s %9s %9s | %9s %9s %8s\n", "WarmMemTableSize", "write ms", "close ms", "MB/s", "alloc MB", "peakAl MB", "tables")
	for _, warm := range []int64{0, 32 * mib, 16 * mib, 8 * mib} {
		dir := t.TempDir()
		clock := &tierClock{}
		clock.set(julDay(25, 8))
		cfg := tierClockCfg(dir, clock)
		cfg.MemTableSize = 64 * mib
		cfg.WarmMemTableSize = warm
		cfg.CacheCapacity = 100
		ts, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		reg := registrypkg.NewLabelRegistry()
		ts.SetLabelRegistry(reg)
		tok, _ := reg.GetOrCreate("Signal")
		runtime.GC()
		var before runtime.MemStats
		runtime.ReadMemStats(&before)
		smp := startSample()
		t0 := time.Now()
		for i := 0; i < 600; i++ {
			id, ok, err := ts.MintNodeIDAt(tok, ms(julDay(10, 1+i%7)), true)
			if err != nil || !ok {
				t.Fatalf("MintNodeIDAt: %v %v", ok, err)
			}
			n := types.NewNode(id, tok, nil)
			_ = n.SetProperty("pad", pad)
			_ = n.SetProperty("i", int64(i))
			if err := ts.PutNode(n); err != nil {
				t.Fatal(err)
			}
		}
		writeDur := time.Since(t0)
		t1 := time.Now()
		if err := ts.Close(); err != nil {
			t.Fatal(err)
		}
		closeDur := time.Since(t1)
		pa, _, _, _ := smp.finish()
		var after runtime.MemStats
		runtime.ReadMemStats(&after)
		tables := 0
		_ = filepath.WalkDir(filepath.Join(dir, "events", "2026-07-10"), func(p string, d os.DirEntry, err error) error {
			if err == nil && strings.HasSuffix(p, ".sst") {
				tables++
			}
			return nil
		})
		fmt.Fprintf(&b, "%-18s | %9.0f %9.0f %9.1f | %9.1f %9.1f %8d\n", fmt.Sprintf("%d MB", warm/mib),
			float64(writeDur.Milliseconds()), float64(closeDur.Milliseconds()), 60/(writeDur+closeDur).Seconds(),
			float64(after.TotalAlloc-before.TotalAlloc)/mib, float64(pa)/mib, tables)
	}
	fmt.Print(b.String())
	t.Log(b.String())
}
