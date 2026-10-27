package badger

import (
	"errors"
	"fmt"
	"testing"
	"time"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// PENDSEEK (SCALE, ai-soc's kit A/B on v4.87.0 with PropertyIndexOnDisk: push
// 60 to 120 s). An equality lookup on the on-disk property index walked the
// whole pending-write overlay (pending and the flush in flight) to find the
// buffered entries of its values, so a commit group of a few thousand writes
// with a lookup per record was quadratic. The lookup now finds the buffered
// entries of each value by its prefix.
func TestPendSeekLookupCostIgnoresUnrelatedPending(t *testing.T) {
	const label = uint16(1)
	// perLookup fills a store's pending buffer with n entries of distinct
	// values and returns the time of one lookup of a buffered value.
	perLookup := func(t *testing.T, n int) time.Duration {
		bs, err := New(Config{InMemory: true, PropertyIndexOnDisk: true, PropertyKeyRegistry: registrypkg.NewPropertyKeyRegistry(),
			FlushInterval: time.Hour, MaxPendingWrites: -1})
		if err != nil {
			t.Fatal(err)
		}
		defer bs.Close()
		if err := bs.CreatePropertyIndex(label, "k"); err != nil {
			t.Fatal(err)
		}
		batch := make([]*types.Node, 0, 1000)
		for i := range n {
			nd := types.NewNode(types.NodeID(existMapBase+int64(i)), label, nil)
			_ = nd.SetProperty("k", int64(i))
			batch = append(batch, nd)
			if len(batch) == cap(batch) || i == n-1 {
				if err := bs.PutNodesBatch(batch); err != nil {
					t.Fatal(err)
				}
				batch = batch[:0]
			}
		}
		const lookups = 300
		best := time.Duration(1 << 62)
		for range 3 {
			start := time.Now()
			for i := range lookups {
				if got := nodeIDsVia(t, bs, label, "k", int64(i*(n/lookups))); len(got) != 1 {
					t.Fatalf("lookup %d found %v", i, got)
				}
			}
			best = min(best, time.Since(start)/lookups)
		}
		return best
	}
	const small, large = 1_000, 64_000
	ts, tl := perLookup(t, small), perLookup(t, large)
	t.Logf("one lookup: %v with %d pending entries, %v with %d", ts, small, tl, large)
	if tl > 8*ts {
		t.Fatalf("a lookup takes %v with %d pending entries against %v with %d: it walks the pending buffer", tl, large, ts, small)
	}
}

// TestPendSeekOverlayStates: the buffered entries a lookup finds by prefix are
// those of the pending buffer and of the flush in flight, a newer op of the
// same key winning; a failed flush's requeued entries stay found, and Clear
// drops them.
func TestPendSeekOverlayStates(t *testing.T) {
	const label = uint16(1)
	var bs *Store
	var inFlight []string // lookups made while a flush holds its snapshot
	probing := false
	probe := func() string {
		return fmt.Sprint(nodeIDsVia(t, bs, label, "k", int64(5)), nodeIDsVia(t, bs, label, "k", int64(6)))
	}
	var err error
	bs, err = New(Config{InMemory: true, PropertyIndexOnDisk: true, PropertyKeyRegistry: registrypkg.NewPropertyKeyRegistry(),
		FlushInterval: time.Hour, MaxPendingWrites: -1,
		BeforeFlushRows: func(bool) error {
			if probing {
				checkPendSeekIndex(t, bs, "in flight")
				inFlight = append(inFlight, probe())
			}
			return nil
		}})
	if err != nil {
		t.Fatal(err)
	}
	defer bs.Close()
	if err := bs.CreatePropertyIndex(label, "k"); err != nil {
		t.Fatal(err)
	}
	seen := map[int64]bool{}
	put := func(id, k int64) { // a create, or a replace of a node seen before
		t.Helper()
		nd := types.NewNode(types.NodeID(id), label, nil)
		_ = nd.SetProperty("k", k)
		write := bs.PutNode
		if seen[id] {
			write = bs.ReplaceNode
		}
		if err := write(nd); err != nil {
			t.Fatal(err)
		}
		seen[id] = true
	}
	check := func(when, want string) {
		t.Helper()
		checkPendSeekIndex(t, bs, when)
		if got := probe(); got != want {
			t.Fatalf("%s: values 5 and 6 find %s, want %s", when, got, want)
		}
	}

	put(1, 5)
	put(2, 5)
	put(1, 6) // node 1 moves from 5 to 6 inside the buffer
	put(1, 5)
	put(1, 6) // and back and forth: each key listed once
	check("pending", "[2] [1]")
	bs.FailNextFlushForTest(errors.New("injected"))
	if err := bs.Flush(); err == nil {
		t.Fatal("the injected flush failure was not reported")
	}
	check("requeued after a failed flush", "[2] [1]")
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	check("flushed", "[2] [1]")
	put(2, 6) // a persisted entry moved by a buffered op
	put(3, 5)
	probing = true
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	probing = false
	if want := "[3] [1 2]"; len(inFlight) != 1 || inFlight[0] != want {
		t.Fatalf("during the flush values 5 and 6 found %v, want [%s]", inFlight, want)
	}
	check("flushed again", "[3] [1 2]")
	put(4, 5)
	seen = map[int64]bool{}
	if err := bs.Clear(); err != nil {
		t.Fatal(err)
	}
	check("cleared", "[] []")
}

// checkPendSeekIndex fails unless pendingByPrefix and flushingByPrefix list exactly
// the history and property-index keys of pending and flushing, each once,
// under its prefix.
func checkPendSeekIndex(t *testing.T, bs *Store, when string) {
	t.Helper()
	bs.wbMu.Lock()
	defer bs.wbMu.Unlock()
	for _, side := range []struct {
		name  string
		buf   map[string]writeOp
		index map[string][]string
	}{{"pending", bs.pending, bs.pendingByPrefix}, {"flushing", bs.flushing, bs.flushingByPrefix}} {
		listed := map[string]int{}
		for p, keys := range side.index {
			for _, k := range keys {
				listed[k]++
				if _, ok := side.buf[k]; !ok || k[:len(k)-8] != p {
					t.Fatalf("%s: %s index lists %x under %x, not a buffered key of that prefix", when, side.name, k, p)
				}
			}
		}
		for k := range side.buf {
			if listedByPrefix(k) && listed[k] != 1 {
				t.Fatalf("%s: %s key %x listed %d times, want once", when, side.name, k, listed[k])
			}
		}
	}
}
