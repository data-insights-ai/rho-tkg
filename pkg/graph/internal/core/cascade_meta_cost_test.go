package core

import (
	"context"
	"fmt"
	"runtime"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// cascadeMissCost is the mean allocations and bytes of one correction on a
// head miss of rel (rel true) or node id, planned on skeletons or (off) on
// the whole history.
func cascadeMissCost(t *testing.T, g *Core, rel bool, nid types.NodeID, rid types.RelID, off bool) (allocs, bytes uint64) {
	t.Helper()
	ctx := context.Background()
	const runs = 4
	g.cascadeMetaOff = off
	defer func() { g.cascadeMetaOff = false }()
	var a, b runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&a)
	for i := range runs {
		p := map[string]any{"ov_c1": int64(10_000 + i)}
		var err error
		if rel {
			_, err = g.Temporal.SetRelVersionInterval(ctx, rid, 1500, 2500, p)
		} else {
			_, err = g.Temporal.SetNodeVersionInterval(ctx, nid, 1500, 2500, p)
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	runtime.ReadMemStats(&b)
	return (b.Mallocs - a.Mallocs) / runs, (b.TotalAlloc - a.TotalAlloc) / runs
}

// cascadeCostShape is a row shape of the cost test: r10's seven small
// properties, or those plus a 400-byte one (sigma's row size).
func cascadeCostProps(shape string, i int) map[string]any {
	p := r10Props(i)
	if shape == "blob400" {
		p["blob"] = fmt.Sprintf("%0400d", i)
	}
	return p
}

// TestCascadeMetaCost: one correction on a head miss (sigma's shape: a
// validity [1000, 2000) rewritten at every version, then a correction
// [1500, 2500)) allocates, planned on skeletons, a bounded share of what the
// whole-history plan allocates, at 64 and 1,024 versions, small and
// 400-byte rows, on memory, badger and tiered; numbers in
// tasks/evidence/RELHIST/cascademeta-cost.txt. Not parallel: it reads the
// process's allocation counters. Mutation: the skeleton path off, or
// hydrating every skeleton, fails the bound.
func TestCascadeMetaCost(t *testing.T) {
	for _, be := range r10AllBackends(t) {
		if be.name == "sharded" {
			continue // no skeleton door: the whole-history plan, unchanged
		}
		for _, shape := range []string{"r10", "blob400"} {
			for _, versions := range []int{64, 1024} {
				t.Run(fmt.Sprintf("%s/%s/versions=%d", be.name, shape, versions), func(t *testing.T) {
					checkCascadeMetaCost(t, be, shape, versions)
				})
			}
		}
	}
}

func checkCascadeMetaCost(t *testing.T, be r10Backend, shape string, versions int) {
	g, err := New(be.cfg())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close() })
	ctx := context.Background()
	p := cascadeCostProps(shape, 0)
	p["tkg_valid_from"], p["tkg_valid_to"] = int64(1000), int64(2000)
	start, err := g.Nodes.Add(ctx, []string{"Derived"}, p)
	if err != nil {
		t.Fatal(err)
	}
	nid := start.ID()
	end, err := g.Nodes.Add(ctx, []string{"Derived"}, map[string]any{"tkg_valid_from": int64(10)})
	if err != nil {
		t.Fatal(err)
	}
	r, err := g.Rels.Add(ctx, "R", start, end, p)
	if err != nil {
		t.Fatal(err)
	}
	// Built at the head: each version planned from the entry.
	for i := 1; i < versions; i++ {
		if _, err := g.Temporal.SetNodeVersionInterval(ctx, nid, 1000, 2000, cascadeCostProps(shape, i)); err != nil {
			t.Fatal(err)
		}
		if _, err := g.Temporal.SetRelVersionInterval(ctx, r.ID(), 1000, 2000, cascadeCostProps(shape, i)); err != nil {
			t.Fatal(err)
		}
	}
	g.headWriteOff = true // every correction below is a head miss
	for _, rel := range []bool{false, true} {
		beforeA, beforeB := cascadeMissCost(t, g, rel, nid, r.ID(), true)
		metaBefore := g.metaWrites.Load()
		afterA, afterB := cascadeMissCost(t, g, rel, nid, r.ID(), false)
		if g.metaWrites.Load() == metaBefore {
			t.Fatal("no correction was planned on skeletons")
		}
		t.Logf("CASCADEMETA cost %s %s versions=%d rel=%v: whole history %d allocs %d B; skeletons %d allocs %d B (%.1fx fewer bytes, %.1fx fewer allocs)",
			be.name, shape, versions, rel, beforeA, beforeB, afterA, afterB,
			float64(beforeB)/float64(max(afterB, 1)), float64(beforeA)/float64(max(afterA, 1)))
		// The bound (in tenths; measured ratios in the evidence file): the
		// skeleton read itself still costs O(versions) (the store decodes
		// each row's temporal block), so the gain grows with the versions
		// and the row size.
		bytesX10, allocsX10 := uint64(12), uint64(13)
		switch {
		case versions >= 1024 && be.name == "memory":
			bytesX10, allocsX10 = 14, 17
		case versions >= 1024:
			bytesX10, allocsX10 = 18, 40
		}
		if afterB*bytesX10 > beforeB*10 || afterA*allocsX10 > beforeA*10 {
			t.Errorf("%s %s versions=%d rel=%v: skeletons %d B %d allocs, whole history %d B %d allocs: want %.1fx fewer bytes, %.1fx fewer allocs",
				be.name, shape, versions, rel, afterB, afterA, beforeB, beforeA, float64(bytesX10)/10, float64(allocsX10)/10)
		}
	}
}
