package bench

// ADR-0011 step S0: the row store's baseline for the column-segment gates.
//
// It re-runs the 2026-09-24 data-model analysis inside this repository: a
// synthday-shaped workload (internal/synthhop — the ai-soc mix of HOP, ORIGIN,
// VATTR and CATTR, or HOP alone) is written through g.Rels().AddWithTx /
// Add exactly as ai-soc's materialization writes it, and the harness records:
//
//   - resident bytes per relationship: live heap after the writes minus live
//     heap after Close (both after two GCs), divided by the relationship count
//     — the analysis's `real` method (graph = store + nodes + rels);
//   - heap objects per relationship over the same window;
//   - on-disk bytes per relationship for the badger modes (`du -sb` of the
//     directory after Close, i.e. after badger's block compression);
//   - HOP scan throughput: g.Rels().ByType and g.Rels().ForEachByType rows/s;
//   - wall time of the writes.
//
// Store modes: `memory` (graph.Config defaults), `badger` (ai-soc's
// engine.OpenAt: BadgerDir + SyncWrites off, everything else default) and
// `lean` (planner stats off, label/adjacency/property indexes on disk, 256 MB
// entity-cache budget).
//
// It is a measurement, not a unit test: it runs only when
// RHO_TKG_SEGMENT_BASELINE is set, e.g.
//
//	RHO_TKG_SEGMENT_BASELINE=790k,3.15M,12.6M \
//	RHO_TKG_SEGMENT_BASELINE_MODES=memory,badger,lean \
//	RHO_TKG_SEGMENT_BASELINE_WORKLOADS=mix,hop \
//	go test -run TestSegmentBaseline -v -count=1 -timeout 0 ./bench
//
// RHO_TKG_SEGMENT_BASELINE_DIR places the badger directories (default: a test
// temp dir); RHO_TKG_SEGMENT_BASELINE_REPEAT=N repeats every configuration
// (badger's resident figure moves with memtable and compaction timing). The
// measured numbers and their comparison with the analysis are recorded in
// CHANGELOG (Unreleased) and ADR-0011 §6.

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/internal/synthhop"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

type baselineResult struct {
	size, mode, workload string
	rels, hop            int
	graphBytes           uint64
	objects              uint64
	diskBytes            int64
	write                time.Duration
	byTypeRate, feRate   float64
}

func (r baselineResult) String() string {
	disk := "-"
	if r.diskBytes > 0 {
		disk = fmt.Sprintf("%.0f", float64(r.diskBytes)/float64(r.rels))
	}
	return fmt.Sprintf("size=%-6s mode=%-6s workload=%-4s rels=%d hop=%d | resident %.0f B/rel (%.1f MB) objects %.2f/rel | disk %s B/rel | write %.1fs | HOP ByType %.2fM rows/s ForEachByType %.2fM rows/s",
		r.size, r.mode, r.workload, r.rels, r.hop,
		float64(r.graphBytes)/float64(r.rels), float64(r.graphBytes)/1e6,
		float64(r.objects)/float64(r.rels), disk, r.write.Seconds(),
		r.byTypeRate/1e6, r.feRate/1e6)
}

func liveHeap() (bytes, objects uint64) {
	runtime.GC()
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.HeapAlloc, ms.HeapObjects
}

func baselineConfig(mode, dir string) (graph.Config, error) {
	cfg := graph.Config{
		Validation:      graph.ValidationLimits{AllowSelfLoops: true},
		AllowTxBackfill: true,
	}
	switch mode {
	case "memory":
	case "badger":
		cfg.BadgerDir = dir
		cfg.SyncWrites = false
	case "lean":
		cfg.BadgerDir = dir
		cfg.SyncWrites = false
		cfg.DisablePlannerStats = true
		cfg.LabelIndexOnDisk = true
		cfg.AdjacencyIndexOnDisk = true
		cfg.PropertyIndexOnDisk = true
		cfg.CacheBudgetBytes = 256 << 20
	default:
		return cfg, fmt.Errorf("unknown mode %q", mode)
	}
	return cfg, nil
}

func dirBytes(dir string) (int64, error) {
	var total int64
	err := filepath.WalkDir(dir, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		total += info.Size()
		return nil
	})
	return total, err
}

// runBaseline writes one workload into a fresh graph and measures it.
func runBaseline(tb testing.TB, sz synthhop.Size, mode string, wl synthhop.Workload, dir string) baselineResult {
	tb.Helper()
	ctx := context.Background()
	res := baselineResult{size: sz.Name, mode: mode, workload: map[synthhop.Workload]string{synthhop.WorkloadMix: "mix", synthhop.WorkloadHOP: "hop"}[wl]}
	cfg, err := baselineConfig(mode, dir)
	if err != nil {
		tb.Fatal(err)
	}

	g, err := graph.New(cfg)
	if err != nil {
		tb.Fatalf("graph.New: %v", err)
	}
	nodes := make([]*types.Node, 0, sz.Nodes())
	start := time.Now()
	err = synthhop.Generate(synthhop.Config{Size: sz, Schema: synthhop.SchemaLegacy, Workload: wl},
		func(n synthhop.Node) error {
			node, err := g.Nodes().Add(ctx, []string{"Asset"}, n.Props)
			if err != nil {
				return err
			}
			nodes = append(nodes, node)
			return nil
		},
		func(e synthhop.Edge) error {
			var err error
			if e.TxFrom > 0 {
				_, err = g.Rels().AddWithTx(ctx, e.Type, nodes[e.Start], nodes[e.End], e.Props, types.Instant(e.TxFrom))
			} else {
				_, err = g.Rels().Add(ctx, e.Type, nodes[e.Start], nodes[e.End], e.Props)
			}
			if err == nil {
				res.rels++
				if e.Type == "HOP" {
					res.hop++
				}
			}
			return err
		})
	if err != nil {
		tb.Fatalf("generate: %v", err)
	}
	res.write = time.Since(start)
	// The caller's node copies are not the graph's (ai-soc drops them with
	// its Materialized maps before the analysis measured).
	nodes = nil
	runtime.KeepAlive(nodes)

	// Scan throughput over HOP (before the heap reading, so any cache the scan
	// fills is part of the resident figure, as in the analysis which also
	// scanned every type before measuring).
	t0 := time.Now()
	rs, err := g.Rels().ByType("HOP", graph.QueryOpts{})
	if err != nil {
		tb.Fatalf("ByType: %v", err)
	}
	res.byTypeRate = float64(len(rs)) / time.Since(t0).Seconds()
	if len(rs) != res.hop {
		tb.Fatalf("ByType returned %d HOP, wrote %d", len(rs), res.hop)
	}
	rs = nil
	runtime.KeepAlive(rs)
	n := 0
	t0 = time.Now()
	if err := g.Rels().ForEachByType("HOP", graph.QueryOpts{}, func(*types.Relationship) bool { n++; return true }); err != nil {
		tb.Fatalf("ForEachByType: %v", err)
	}
	res.feRate = float64(n) / time.Since(t0).Seconds()
	// The analysis read every type and every node back before its heap
	// reading (which fills the badger entity caches the same way); do the same.
	for _, typ := range []string{"ORIGIN", "VATTR", "CATTR"} {
		if _, err := g.Rels().ByType(typ, graph.QueryOpts{}); err != nil {
			tb.Fatalf("ByType(%s): %v", typ, err)
		}
	}
	if _, err := g.Nodes().ByLabel("Asset", graph.QueryOpts{}); err != nil {
		tb.Fatalf("ByLabel: %v", err)
	}

	h1, o1 := liveHeap()
	if err := g.Close(); err != nil {
		tb.Fatalf("Close: %v", err)
	}
	g = nil
	runtime.KeepAlive(g)
	h2, o2 := liveHeap()
	res.graphBytes = h1 - h2
	res.objects = o1 - o2
	if dir != "" && mode != "memory" {
		if res.diskBytes, err = dirBytes(dir); err != nil {
			tb.Fatalf("du: %v", err)
		}
	}
	return res
}

func envList(key, def string) []string {
	v := os.Getenv(key)
	if v == "" {
		v = def
	}
	var out []string
	for _, s := range strings.Split(v, ",") {
		if s = strings.TrimSpace(s); s != "" {
			out = append(out, s)
		}
	}
	return out
}

// TestSegmentBaseline is the S0 measurement (see the file comment).
func TestSegmentBaseline(t *testing.T) {
	sizes := os.Getenv("RHO_TKG_SEGMENT_BASELINE")
	if sizes == "" {
		t.Skip("measurement: set RHO_TKG_SEGMENT_BASELINE=790k,3.15M,12.6M to run")
	}
	root := os.Getenv("RHO_TKG_SEGMENT_BASELINE_DIR")
	if root == "" {
		root = t.TempDir()
	}
	repeat := 1
	if v := os.Getenv("RHO_TKG_SEGMENT_BASELINE_REPEAT"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			t.Fatalf("RHO_TKG_SEGMENT_BASELINE_REPEAT=%q: want a positive integer", v)
		}
		repeat = n
	}
	for _, name := range envList("RHO_TKG_SEGMENT_BASELINE", "") {
		sz, err := synthhop.SizeByName(name)
		if err != nil {
			t.Fatal(err)
		}
		for _, wlName := range envList("RHO_TKG_SEGMENT_BASELINE_WORKLOADS", "mix") {
			wl := synthhop.WorkloadMix
			if wlName == "hop" {
				wl = synthhop.WorkloadHOP
			}
			for _, mode := range envList("RHO_TKG_SEGMENT_BASELINE_MODES", "memory,badger,lean") {
				for rep := 0; rep < repeat; rep++ {
					dir := ""
					if mode != "memory" {
						dir = filepath.Join(root, fmt.Sprintf("%s-%s-%s-%d", mode, sz.Name, wlName, time.Now().UnixNano()))
					}
					res := runBaseline(t, sz, mode, wl, dir)
					t.Log(res.String())
					fmt.Println("S0", res.String())
				}
			}
		}
	}
}

// TestSegmentBaselineSmoke keeps the harness compiling and working in the
// ordinary suite: a tiny workload on every mode, with sanity bounds only.
func TestSegmentBaselineSmoke(t *testing.T) {
	sz := synthhop.Size{Name: "smoke", HOP: 400, Pairs: 100, VATTR: 120, CATTR: 5, Hosts: 30, Actors: 6}
	for _, mode := range []string{"memory", "badger", "lean"} {
		dir := ""
		if mode != "memory" {
			dir = filepath.Join(t.TempDir(), mode)
		}
		res := runBaseline(t, sz, mode, synthhop.WorkloadMix, dir)
		if res.rels != sz.Rels() || res.hop != sz.HOP {
			t.Fatalf("%s: wrote %d rels / %d HOP, want %d / %d", mode, res.rels, res.hop, sz.Rels(), sz.HOP)
		}
		if res.graphBytes == 0 || res.byTypeRate <= 0 || res.feRate <= 0 {
			t.Fatalf("%s: empty measurement %s", mode, res)
		}
		if mode != "memory" && res.diskBytes <= 0 {
			t.Fatalf("%s: no on-disk bytes measured", mode)
		}
	}
}
