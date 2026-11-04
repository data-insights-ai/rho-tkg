package core

import (
	"context"
	"fmt"
	"math"
	"runtime"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// cascadeMissCost runs four corrections on a head miss of rel (rel true) or
// node id, planned on skeletons or (off) on the whole history. Per
// correction it counts the full rows read from the store (Core.historyRead)
// and the skeleton reads (Core.skeletonRead); allocations and bytes are the
// mean, for the log only (process-wide counters).
type cascadeCost struct {
	minRows, maxRows uint64 // full rows read by one correction
	skeletons        int    // skeleton reads, all corrections
	allocs, bytes    uint64 // mean per correction, log only
}

const cascadeCostRuns = 4

func cascadeMissCost(t *testing.T, g *Core, rel bool, nid types.NodeID, rid types.RelID, off bool) cascadeCost {
	t.Helper()
	ctx := context.Background()
	g.cascadeMetaOff = off
	var read uint64
	out := cascadeCost{minRows: math.MaxUint64}
	g.historyRead = func(n int) { read += uint64(n) }
	g.skeletonRead = func(int) { out.skeletons++ }
	defer func() { g.cascadeMetaOff, g.historyRead, g.skeletonRead = false, nil, nil }()
	var a, b runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&a)
	for i := range cascadeCostRuns {
		read = 0
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
		out.minRows, out.maxRows = min(out.minRows, read), max(out.maxRows, read)
	}
	runtime.ReadMemStats(&b)
	out.allocs, out.bytes = (b.Mallocs-a.Mallocs)/cascadeCostRuns, (b.TotalAlloc-a.TotalAlloc)/cascadeCostRuns
	return out
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

// cascadeMetaRowsMax bounds the full rows the skeleton plan reads per
// correction: the segment base it hydrates and the resumption source (2 in
// every case, memory, badger and tiered, 64 and 1,024 versions, 4 Nov).
const cascadeMetaRowsMax = 2

// TestCascadeMetaCost: one correction on a head miss (sigma's shape: a
// validity [1000, 2000) rewritten at every version, then a correction
// [1500, 2500)), planned on skeletons, reads at most cascadeMetaRowsMax full
// rows from the store where the whole-history plan reads every version, at
// 64 and 1,024 versions, small and 400-byte rows, on memory, badger and
// tiered. The gate counts rows (Core.historyRead), which no load moves;
// allocations and bytes are logged for the evidence file
// (tasks/evidence/RELHIST/cascademeta-cost.txt), not gated: they count the
// whole process. Mutation: the skeleton path off, or hydrating every
// skeleton, fails the bound.
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
		whole := cascadeMissCost(t, g, rel, nid, r.ID(), true)
		metaBefore := g.metaWrites.Load()
		skel := cascadeMissCost(t, g, rel, nid, r.ID(), false)
		if g.metaWrites.Load() == metaBefore {
			t.Fatal("no correction was planned on skeletons")
		}
		// Information only: allocations and bytes count the whole process,
		// so badger's background flushes and compactions move them with the
		// machine's load (ci-docker read 3.9x where a quiet run reads 5.8x).
		t.Logf("CASCADEMETA cost %s %s versions=%d rel=%v: full rows per correction %d-%d -> %d-%d, skeleton reads %d -> %d; allocs %d -> %d (%.1fx); bytes %d -> %d (%.1fx)",
			be.name, shape, versions, rel, whole.minRows, whole.maxRows, skel.minRows, skel.maxRows, whole.skeletons, skel.skeletons,
			whole.allocs, skel.allocs, float64(whole.allocs)/float64(max(skel.allocs, 1)),
			whole.bytes, skel.bytes, float64(whole.bytes)/float64(max(skel.bytes, 1)))
		// The gate counts per correction (deterministic, whatever the load):
		// the whole-history plan reads every older version (the first
		// correction versions-1, later ones more, as each appends rows) and
		// no skeletons; the skeleton plan reads one skeleton list and only
		// the full rows it hydrates.
		if whole.minRows < uint64(versions-1) || whole.skeletons != 0 {
			t.Errorf("%s %s versions=%d rel=%v: the whole-history plan read %d full rows (least per correction), %d skeleton lists; want >= %d and 0 (the baseline is not the whole history)",
				be.name, shape, versions, rel, whole.minRows, whole.skeletons, versions-1)
		}
		if skel.maxRows > cascadeMetaRowsMax || skel.skeletons != cascadeCostRuns {
			t.Errorf("%s %s versions=%d rel=%v: the skeleton plan read up to %d full rows per correction and %d skeleton lists in %d corrections; want <= %d and one each",
				be.name, shape, versions, rel, skel.maxRows, skel.skeletons, cascadeCostRuns, cascadeMetaRowsMax)
		}
	}
}
