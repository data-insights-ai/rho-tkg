package core

import (
	"math"
	"testing"
	"time"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
)

// ROWPROBE's budget (lesson 89) on sigma's paged-row probe (sigma 26 Oct,
// BenchmarkPagedRowsProbeCut): DerivedPage rows carry an int64 hash column
// with a property index; a probe asks ByLabelAndPropertyValues for a list of
// hashes. One call with 1,000 values costs per value at most 1.2x a call with
// 16 values, on every backend, and with the index on disk at most 2x the RAM
// index's cost per value. Before the fix the matched rows were checked against
// every value (NodeHasAnyIndexKey: nodes x values; 1,000 values in one call
// cost 1.75 M allocations against 249 k for 1,000 calls), and the on-disk
// index looked each of a value's ten typed keys up with its own read txn,
// iterator and pending-overlay pass (146 allocs per value on badger, 290 on
// tiered, against 17 on the RAM index). Mutation: the per-node match over the
// value list breaks the first bound, a disk lookup per value key the second.

func rowprobeBackends(tb testing.TB) []r10Backend {
	return []r10Backend{
		{"memory", func() Config { return Config{Store: memory.New()} }},
		{"badger-ram", func() Config { return Config{BadgerInMemory: true} }},
		{"badger-disk", func() Config { return Config{BadgerInMemory: true, PropertyIndexOnDisk: true} }},
		{"tiered-disk", func() Config {
			ts, err := tiered.New(tiered.Config{InMemory: true, RefLabels: []string{"DerivedPage"},
				ShardWindow: 7 * 24 * time.Hour, FlushInterval: 1<<63 - 1, PropertyIndexOnDisk: true})
			if err != nil {
				tb.Fatalf("tiered.New: %v", err)
			}
			tb.Cleanup(func() { _ = ts.Close() })
			return Config{Store: ts}
		}},
	}
}

func TestRowProbeValuesCostLinearInValues(t *testing.T) {
	if testing.Short() && !isRaceEnabled() {
		t.Skip("long tier: release gate")
	}
	const rows, small, large = 4000, 16, 1000
	cost := map[string]float64{}
	for _, be := range rowprobeBackends(t) {
		t.Run(be.name, func(t *testing.T) {
			g, err := New(be.cfg())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = g.Close() })
			if err := g.Index.CreateProperty("DerivedPage", "ov_h"); err != nil {
				t.Fatal(err)
			}
			hash := func(i int) int64 { return int64(i)*7919 + 13 }
			for i := range rows {
				if _, err := g.Nodes.Add(t.Context(), []string{"DerivedPage"}, map[string]any{
					"ov_namespace": "n", "ov_relation": "r", "ov_h": hash(i), "c1": int64(i)}); err != nil {
					t.Fatal(err)
				}
			}
			if f, ok := g.store.(interface{ Flush() error }); ok {
				if err := f.Flush(); err != nil {
					t.Fatal(err)
				}
			}
			perValue := func(n int) float64 {
				values := make([]any, n)
				for i := range values {
					values[i] = hash(i * (rows / n))
				}
				best := math.Inf(1)
				for range 3 {
					var got []int
					m := mallocs(func() {
						nodes, err := g.Nodes.ByLabelAndPropertyValues("DerivedPage", "ov_h", values, storepkg.QueryOpts{})
						if err != nil {
							t.Fatal(err)
						}
						got = append(got, len(nodes))
					})
					if got[0] != n {
						t.Fatalf("%d values: %d nodes, want %d", n, got[0], n)
					}
					best = min(best, float64(m)/float64(n))
				}
				return best
			}
			a, b := perValue(small), perValue(large)
			t.Logf("allocs per value: %.1f with %d values, %.1f with %d (x%.2f)", a, small, b, large, b/a)
			if b > 1.2*a {
				t.Errorf("one call with %d values costs %.1f allocs per value, over 1.2x the %.1f with %d", large, b, a, small)
			}
			cost[be.name] = b
		})
	}
	for _, disk := range []string{"badger-disk", "tiered-disk"} {
		if ram, ok := cost["badger-ram"]; ok && cost[disk] > 2*ram {
			t.Errorf("%s: %.1f allocs per value, over 2x the RAM index's %.1f", disk, cost[disk], ram)
		}
	}
}
