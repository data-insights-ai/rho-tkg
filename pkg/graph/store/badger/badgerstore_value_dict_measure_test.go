//go:build measure

// Measurement level (René 4 Nov, `scripts/measure.sh`): live heap after a GC moves
// with other tests and host load, so this bound runs by hand, not in a gate
// (MEMGATE). The entry counts across a reopen gate in
// TestValueDictEntryStatsAcrossReopen.

package badger

import (
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"testing"
)

const vdictHeapChildEnv = "RHO_TEST_VDICT_HEAP_CHILD"

// TestValueDictEntryHeap (VDICTDISK): a dictionary entry added while rows are
// written keeps one copy of its value and its map and slice slots, about 118 B
// for a 51 B value: neither the caller's string (a row's decode buffer) nor an
// interned handle (unique, ~214 B more) stays live; the stats' estimate is the
// measured heap within a fifth. It runs in its own process: the heap it
// measures is the process's, and memory an earlier test leaves to be freed
// moved the figure by a fifth (tasks/evidence/VDICTDISK/red-heap-flake.txt).
func TestValueDictEntryHeap(t *testing.T) {
	if os.Getenv(vdictHeapChildEnv) == "" {
		cmd := exec.Command(os.Args[0], "-test.run=^TestValueDictEntryHeap$", "-test.v") // #nosec G204 -- this test binary
		cmd.Env = append(os.Environ(), vdictHeapChildEnv+"=1")
		out, err := cmd.CombinedOutput()
		if t.Log(string(out)); err != nil {
			t.Fatalf("own process: %v", err)
		}
		return
	}
	const n = 100_000
	dir := t.TempDir()
	bs := vdOpen(t, dir, vdDicts(n))
	defer bs.Close()
	if err := bs.vdict.ensureLoaded(bs.view); err != nil {
		t.Fatal(err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	d, _ := bs.vdict.plan.Index("folder")
	live := func() uint64 {
		var m runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&m)
		return m.HeapAlloc
	}
	before := live()
	for i := range n {
		row := fmt.Sprintf(`C:\Users\user%07d\AppData\Local\Temp\file-%07d.tmp%250s`, i, i, "the rest of the row")
		if _, ok := bs.vdict.token(bs, d, row[:51]); !ok { // the value shares the row's ~300 B buffer
			t.Fatal("no token")
		}
	}
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	per := float64(live()-before) / n
	t.Logf("%.0f B live per entry of a 51 B value", per)
	if per > 180 {
		t.Fatalf("%.0f B live per entry of a 51 B value, want at most 180", per)
	}
	s := bs.ValueDictionaryStats()[d]
	if est := float64(s.Bytes) / n; s.Entries != n || est < 0.8*per || est > 1.2*per {
		t.Fatalf("stats %+v: %.0f B an entry estimated, %.0f B measured; want %d entries, the estimate within a fifth", s, est, per, n)
	}
}
