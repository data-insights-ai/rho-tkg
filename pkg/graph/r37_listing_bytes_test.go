package graph_test

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"runtime"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	tieredpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

const r37Facts = 128

// r37Open opens ai-soc's tiered configuration with Derived on the reference shard.
func r37Open(t *testing.T, dir string) *graphpkg.Graph {
	t.Helper()
	ts, err := tieredpkg.New(tieredpkg.Config{DataDir: dir, RefLabels: []string{"Derived", "Device"}, ShardWindow: 24 * time.Hour,
		ColdAfter: 72 * time.Hour, HistoryDeltaEncoding: true, SyncWrites: true, RelsByMint: true,
		AdjacencyIndexOnDisk: true, LabelIndexOnDisk: true})
	if err != nil {
		t.Fatal(err)
	}
	g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 12, Store: ts, DurableCommit: true,
		UntimedValidStart: graphpkg.UntimedUnbounded})
	if err != nil {
		t.Fatal(err)
	}
	return g
}

// r37Props is fact i: 2 key terms, a key and 11 strings of 40 B (447 B of values).
func r37Props(i int) map[string]any {
	p := map[string]any{"namespace": "estate", "relation": "r", "key": fmt.Sprintf("k%04d", i)}
	for c := 0; c < 11; c++ {
		h := sha1.Sum([]byte(fmt.Sprintf("%d/%d", i, c)))
		p[fmt.Sprintf("c%02d", c)] = hex.EncodeToString(h[:])
	}
	return p
}

// r37List lists every fact once through sigma's page door and returns the
// bytes allocated per fact (process-wide TotalAlloc, as ai-soc measures).
func r37List(t *testing.T, g *graphpkg.Graph) float64 {
	t.Helper()
	ctx := context.Background()
	pin, err := g.Temporal().PeekTx()
	if err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	var m0, m1 runtime.MemStats
	runtime.ReadMemStats(&m0)
	n := 0
	if _, err := g.Temporal().ForEachNodeEffectiveByLabelAndProperties(ctx, "Derived", map[string]any{"namespace": "estate", "relation": "r"}, pin,
		temporal.EffectivePageOpts{Limit: 1024}, func(types.NodeID, []temporal.NodeSegment) bool { n++; return true }); err != nil {
		t.Fatal(err)
	}
	runtime.ReadMemStats(&m1)
	if n != r37Facts {
		t.Fatalf("listed %d facts, want %d", n, r37Facts)
	}
	return float64(m1.TotalAlloc-m0.TotalAlloc) / r37Facts
}

// r37Fallback reads every fact as sigma's ListDerivedRows does for a fact
// with two or more segments: Nodes().Get, then NodeEffectiveTimelines at the
// pin (no read snapshot). It returns the bytes allocated per fact. Each read
// opens its own badger transaction, and badger copies every value it reads
// twice (into the item, then in Item.Value): 18.5x before the version shared
// the current row's values, 15.8x after; the page door is the cheap path.
func r37Fallback(t *testing.T, g *graphpkg.Graph, ids []types.NodeID) float64 {
	t.Helper()
	ctx := context.Background()
	pin, err := g.Temporal().PeekTx()
	if err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	var m0, m1 runtime.MemStats
	runtime.ReadMemStats(&m0)
	for _, id := range ids {
		if _, err := g.Nodes().Get(ctx, id); err != nil {
			t.Fatal(err)
		}
		segs, err := g.Temporal().NodeEffectiveTimelines(id, []types.Instant{pin})
		if err != nil || len(segs) != 1 || len(segs[0]) != 2 {
			t.Fatalf("NodeEffectiveTimelines = %v, %v; want 2 segments", segs, err)
		}
	}
	runtime.ReadMemStats(&m1)
	return float64(m1.TotalAlloc-m0.TotalAlloc) / float64(len(ids))
}

// TestR37ListingBytesPerFact pins R37 (ai-soc, 26 Oct: the cold listing of
// changed facts allocated 24.7x a fact's values against their limit of 4x) on
// ai-soc's shape: 128 Derived facts of 447 B on the reference shard of their
// tiered config, listed through ForEachNodeEffectiveByLabelAndProperties, with
// one validity version or two (the second rewrites the same values). Before
// the fix: warm 0.9x and 8.5x, cold 5.3x and 14.9x; after: 0.9x and 4.1x, 5.3x
// and 10.5x (the history skeleton is one sorted slice; an unchanged version
// shares the current row's values and property slice). The bounds leave room
// for the process-wide counter. Serial: it reads runtime.MemStats.
func TestR37ListingBytesPerFact(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and reopens two disk stores")
	}
	const valueBytes = 447.0
	ctx := context.Background()
	bounds := map[int][2]float64{1: {1.5, 6.5}, 2: {5.0, 12.0}} // versions: warm, cold bound (x a fact's values)
	for _, versions := range []int{1, 2} {
		dir := t.TempDir()
		g := r37Open(t, dir)
		if err := g.Index().CreateComposite("Derived", []string{"namespace", "relation"}); err != nil {
			t.Fatal(err)
		}
		ids := make([]types.NodeID, r37Facts)
		for i := range ids {
			p := r37Props(i)
			p["tkg_valid_from"], p["tkg_valid_to"] = types.Instant(1_000_000), types.Instant(1_000_005)
			n, err := g.Nodes().Add(ctx, []string{"Derived"}, p)
			if err != nil {
				t.Fatal(err)
			}
			ids[i] = n.ID()
		}
		if versions == 2 {
			for i, id := range ids {
				p := r37Props(i)
				delete(p, "namespace")
				delete(p, "relation")
				delete(p, "key")
				if _, err := g.Temporal().SetNodeVersionInterval(ctx, id, 1_000_010, 1_000_015, p); err != nil {
					t.Fatal(err)
				}
			}
		}
		r37List(t, g) // the first listing fills the node cache
		warm := r37List(t, g) / valueBytes
		if versions == 2 {
			r37Fallback(t, g, ids)
			fb := r37Fallback(t, g, ids) / valueBytes
			t.Logf("2 versions, sigma's fallback (Get + NodeEffectiveTimelines): %.1fx a fact's values", fb)
			if fb > 17.0 {
				t.Fatalf("sigma's fallback: %.1fx a fact's values, bound 17.0x", fb)
			}
		}
		g.Close()
		g = r37Open(t, dir)
		time.Sleep(200 * time.Millisecond) // the reopen's background work settles
		cold := r37List(t, g) / valueBytes
		g.Close()
		t.Logf("%d version(s): warm %.1fx, cold %.1fx a fact's values", versions, warm, cold)
		if warm > bounds[versions][0] || cold > bounds[versions][1] {
			t.Fatalf("%d version(s): warm %.1fx (bound %.1fx), cold %.1fx (bound %.1fx) a fact's values",
				versions, warm, bounds[versions][0], cold, bounds[versions][1])
		}
	}
}
