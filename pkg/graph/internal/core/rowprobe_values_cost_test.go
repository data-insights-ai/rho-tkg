package core

import (
	"math"
	"testing"
	"time"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
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

// TestRowProbeDoorCostLinearInValues is ROWPROBE's budget on the value-list
// page door: sigma's join level (label DerivedPage, ov_namespace and
// ov_relation fixed, ov_h IN the probes, one pin; half of a hash's rows are
// another relation's) in one call costs per value at most 1.2x a call with 16
// values, and at most 0.9x the unbatched composition it replaces (per probe:
// ByLabelAndProperty on ov_h, then NodeEffectiveTimelines at the pin), on
// every backend and with the index on disk. Measured 26 Oct at 1,000 values:
// 0.79x on memory and badger (both pay a value's ~10 equal-form keys; the
// door's set of them for the row re-check is its extra), 0.57x on badger-disk,
// 0.45x on tiered-disk. Mutation: the forms' keys built twice (the door's
// match and the lookup's) break the second bound; a set rebuilt per row, the
// first.
func TestRowProbeDoorCostLinearInValues(t *testing.T) {
	if testing.Short() && !isRaceEnabled() {
		t.Skip("long tier: release gate")
	}
	const rows, small, large = 4000, 16, 1000
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
				rel := "r"
				if i%2 == 1 {
					rel = "other" // the hash is relation-agnostic: half the hits are another relation's
				}
				if _, err := g.Nodes.Add(t.Context(), []string{"DerivedPage"}, map[string]any{
					"ov_namespace": "n", "ov_relation": rel, "ov_h": hash(i / 2), "c1": int64(i)}); err != nil {
					t.Fatal(err)
				}
			}
			if f, ok := g.store.(interface{ Flush() error }); ok {
				if err := f.Flush(); err != nil {
					t.Fatal(err)
				}
			}
			pin := g.readNow()
			fixed := map[string]any{"ov_namespace": "n", "ov_relation": "r"}
			probes := func(n int) []any {
				values := make([]any, n)
				for i := range values {
					values[i] = hash(i * (rows / 2 / n))
				}
				return values
			}
			door := func(values []any) int {
				got := 0
				if _, err := g.Temporal.ForEachNodeEffectiveByLabelAndPropertyValues(t.Context(), "DerivedPage", fixed, "ov_h", values, pin, temporal.EffectivePageOpts{}, func(_ types.NodeID, segs []temporal.NodeSegment) bool {
					got += len(segs)
					return true
				}); err != nil {
					t.Fatal(err)
				}
				return got
			}
			unbatched := func(values []any) int {
				got := 0
				for _, v := range values {
					nodes, err := g.Nodes.ByLabelAndProperty("DerivedPage", "ov_h", v, storepkg.QueryOpts{})
					if err != nil {
						t.Fatal(err)
					}
					for _, n := range nodes {
						if rel, _ := n.GetProperty("ov_relation"); rel != "r" {
							continue
						}
						segs, err := g.Temporal.NodeEffectiveTimelines(n.ID(), []types.Instant{pin})
						if err != nil {
							t.Fatal(err)
						}
						got += len(segs[0])
					}
				}
				return got
			}
			perValue := func(run func([]any) int, n int) float64 {
				values := probes(n)
				best := math.Inf(1)
				for range 3 {
					var got int
					m := mallocs(func() { got = run(values) })
					if got != n {
						t.Fatalf("%d values: %d segments, want %d", n, got, n)
					}
					best = min(best, float64(m)/float64(n))
				}
				return best
			}
			a, b, u := perValue(door, small), perValue(door, large), perValue(unbatched, large)
			start := time.Now()
			door(probes(large))
			dd := time.Since(start)
			start = time.Now()
			unbatched(probes(large))
			du := time.Since(start)
			t.Logf("door allocs per value: %.1f with %d values, %.1f with %d (x%.2f); unbatched %.1f (x%.2f); %d values: door %v, unbatched %v",
				a, small, b, large, b/a, u, b/u, large, dd, du)
			if b > 1.2*a {
				t.Errorf("one call with %d values costs %.1f allocs per value, over 1.2x the %.1f with %d", large, b, a, small)
			}
			if b > 0.9*u {
				t.Errorf("the door costs %.1f allocs per value, over 0.9x the unbatched %.1f", b, u)
			}
		})
	}
}
