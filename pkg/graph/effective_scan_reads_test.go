package graph_test

import (
	"context"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Backlog 33: an effective scan (ForEachRelEffectiveByType /
// ForEachNodeEffectiveByLabel) on badger reads each entity's current row once.
// The faulty gather it names decodes every current row of the type or label
// to collect the candidate IDs and then reads each row again for its timeline:
// with the rows evicted from a one-entry cache that is two cache misses per
// entity; the ID-only gather leaves one (the timeline's own read).
func TestEffectiveScanReadsEachRowOnce(t *testing.T) {
	ctx := context.Background()
	st, err := badger.New(badger.Config{Dir: t.TempDir(), FlushInterval: time.Hour, CacheCapacity: 1})
	if err != nil {
		t.Fatal(err)
	}
	g, err := graphpkg.New(graphpkg.Config{Store: st})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	const n = 200
	var prev types.NodeID
	for i := 0; i < n; i++ {
		node, err := g.Nodes().Add(ctx, []string{"Host"}, map[string]any{"i": int64(i)})
		if err != nil {
			t.Fatal(err)
		}
		if i > 0 {
			if _, err := g.Rels().AddByID(ctx, "FLOW", prev, node.ID(), nil); err != nil {
				t.Fatal(err)
			}
		}
		prev = node.ID()
	}
	pin, err := g.Temporal().NowTx()
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Flush(); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		misses func() int64
		scan   func() (int, error)
		rows   int
	}{
		{"rels", st.RelCacheMisses, func() (int, error) {
			k := 0
			err := g.Temporal().ForEachRelEffectiveByType("FLOW", pin, func(temporal.RelSegment) bool { k++; return true })
			return k, err
		}, n - 1},
		{"nodes", st.NodeCacheMisses, func() (int, error) {
			k := 0
			err := g.Temporal().ForEachNodeEffectiveByLabel("Host", pin, func(temporal.NodeSegment) bool { k++; return true })
			return k, err
		}, n},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := tc.misses()
			k, err := tc.scan()
			if err != nil || k != tc.rows {
				t.Fatalf("scan = %d segments, %v; want %d", k, err, tc.rows)
			}
			if got := tc.misses() - before; got > int64(tc.rows)+2 {
				t.Fatalf("scan of %d entities missed the entity cache %d times: the gather reads every row before the timeline reads it again (want <= %d)", tc.rows, got, tc.rows+2)
			}
		})
	}
}
