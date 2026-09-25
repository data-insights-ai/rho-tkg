package memory_test

// P7 measurement: what one row of the memory row store costs (resident bytes
// per relationship and per node, retained, not allocated) and how fast its
// doors are, at the three synthday sizes. Undeclared types only: this is the
// part of the memory store that ADR-0011's segments do not take over (nodes,
// undeclared relationship types, a declared type's unsealed memtable).
//
// Method (S0/S2): the synthday workload (internal/synthhop) is written through
// g.Nodes().Add and g.Rels().AddWithTx / Add into a graph over a memory.Store.
// Resident graph bytes = live heap (two GCs) after the writes minus live heap
// after Close with the graph dropped. Per relationship:
//
//	B/rel  = (graph bytes of the run - graph bytes of the nodes-only run) / rels
//	B/node = (graph bytes of the nodes-only run - an empty graph) / nodes
//
// Throughput: write (relationships only), g.Rels().ByType / ForEachByType over
// HOP, g.Rels().Get over 20,000 random HOP IDs.
//
// RHO_TKG_P7_HEAPPROFILE=<dir> writes an in-use heap profile (after two GCs,
// runtime.MemProfileRate = 4096) per run, for `go tool pprof -sample_index=
// inuse_space`.
//
// A measurement, not a unit test: it runs only when RHO_TKG_P7 is set, e.g.
//
//	RHO_TKG_P7=790k,3.15M,12.6M RHO_TKG_P7_RUNS=hop-p6,hop-legacy,mix-p6 \
//	go test -run TestRowStoreP7Scale -v -count=1 -timeout 0 ./pkg/graph/store/memory
//
// The numbers are recorded in CHANGELOG (Unreleased).

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/internal/synthhop"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

type p7Run struct {
	name     string
	schema   synthhop.Schema
	workload synthhop.Workload
	nodes    bool // write nodes only
	empty    bool // write nothing
}

var p7Runs = map[string]p7Run{
	"hop-p6":     {name: "hop-p6", schema: synthhop.SchemaP6, workload: synthhop.WorkloadHOP},
	"hop-legacy": {name: "hop-legacy", schema: synthhop.SchemaLegacy, workload: synthhop.WorkloadHOP},
	"mix-p6":     {name: "mix-p6", schema: synthhop.SchemaP6, workload: synthhop.WorkloadMix},
	"mix-legacy": {name: "mix-legacy", schema: synthhop.SchemaLegacy, workload: synthhop.WorkloadMix},
}

type p7Result struct {
	run                   string
	size                  string
	nodes, rels, hop      int
	graphBytes            int64
	objects               int64
	write                 time.Duration
	byType, forEach, gets float64
}

func p7Heap() (uint64, uint64) {
	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc, m.HeapObjects
}

func runP7(tb testing.TB, sz synthhop.Size, run p7Run, profile string) p7Result {
	tb.Helper()
	ctx := context.Background()
	res := p7Result{run: run.name, size: sz.Name}
	g, err := graph.New(graph.Config{Store: memory.New(), Validation: graph.ValidationLimits{AllowSelfLoops: true}, AllowTxBackfill: true})
	if err != nil {
		tb.Fatal(err)
	}
	var nodes []*types.Node
	hopIDs := make([]types.RelID, 0, sz.HOP)
	var start time.Time
	if !run.empty {
		err = synthhop.Generate(synthhop.Config{Size: sz, Schema: run.schema, Workload: run.workload},
			func(n synthhop.Node) error {
				node, err := g.Nodes().Add(ctx, []string{"Asset"}, n.Props)
				nodes = append(nodes, node)
				return err
			},
			func(e synthhop.Edge) error {
				if run.nodes {
					return nil
				}
				if start.IsZero() {
					start = time.Now()
				}
				var r *types.Relationship
				var err error
				if e.TxFrom > 0 {
					r, err = g.Rels().AddWithTx(ctx, e.Type, nodes[e.Start], nodes[e.End], e.Props, types.Instant(e.TxFrom))
				} else {
					r, err = g.Rels().Add(ctx, e.Type, nodes[e.Start], nodes[e.End], e.Props)
				}
				if err != nil {
					return err
				}
				res.rels++
				if e.Type == "HOP" {
					hopIDs = append(hopIDs, r.ID())
				}
				return nil
			})
		if err != nil {
			tb.Fatal(err)
		}
	}
	if !start.IsZero() {
		res.write = time.Since(start)
	}
	res.nodes = len(nodes)
	res.hop = len(hopIDs)
	nodes = nil // the harness's node copies are not the graph's (S0)
	runtime.KeepAlive(nodes)

	h1, o1 := p7Heap()
	if profile != "" && !run.empty {
		f, err := os.Create(filepath.Join(profile, fmt.Sprintf("p7-%s-%s.heap", run.name, sz.Name)))
		if err != nil {
			tb.Fatal(err)
		}
		if err := pprof.Lookup("heap").WriteTo(f, 0); err != nil {
			tb.Fatal(err)
		}
		if err := f.Close(); err != nil {
			tb.Fatal(err)
		}
	}
	if res.hop > 0 {
		t0 := time.Now()
		rs, err := g.Rels().ByType("HOP", graph.QueryOpts{})
		if err != nil || len(rs) != res.hop {
			tb.Fatalf("ByType: %d rows, %v", len(rs), err)
		}
		res.byType = float64(len(rs)) / time.Since(t0).Seconds()
		rs = nil
		runtime.KeepAlive(rs)
		n := 0
		t0 = time.Now()
		if err := g.Rels().ForEachByType("HOP", graph.QueryOpts{}, func(*types.Relationship) bool { n++; return true }); err != nil || n != res.hop {
			tb.Fatalf("ForEachByType: %d rows, %v", n, err)
		}
		res.forEach = float64(n) / time.Since(t0).Seconds()
		rng := rand.New(rand.NewPCG(1, 2)) // #nosec G404 -- measurement sample
		const probes = 20_000
		t0 = time.Now()
		for i := 0; i < probes; i++ {
			if _, err := g.Rels().Get(ctx, hopIDs[rng.IntN(len(hopIDs))]); err != nil {
				tb.Fatal(err)
			}
		}
		res.gets = probes / time.Since(t0).Seconds()
	}
	if err := g.Close(); err != nil {
		tb.Fatal(err)
	}
	g = nil
	runtime.KeepAlive(g)
	h2, o2 := p7Heap()
	runtime.KeepAlive(hopIDs)
	res.graphBytes = int64(h1) - int64(h2)
	res.objects = int64(o1) - int64(o2)
	return res
}

// TestRowStoreP7Scale is the P7 measurement (see the file comment).
func TestRowStoreP7Scale(t *testing.T) {
	if os.Getenv("RHO_TKG_P7") == "" {
		t.Skip("measurement: set RHO_TKG_P7=790k,3.15M,12.6M to run")
	}
	profile := os.Getenv("RHO_TKG_P7_HEAPPROFILE")
	if profile != "" {
		runtime.MemProfileRate = 4096
	}
	for _, name := range s2List("RHO_TKG_P7", "") {
		sz, err := synthhop.SizeByName(name)
		if err != nil {
			t.Fatal(err)
		}
		empty := runP7(t, sz, p7Run{name: "empty", empty: true}, "")
		nodesHOP := runP7(t, sz, p7Run{name: "nodes-hop", nodes: true, workload: synthhop.WorkloadHOP, schema: synthhop.SchemaP6}, profile)
		nodesMix := runP7(t, sz, p7Run{name: "nodes-mix", nodes: true, workload: synthhop.WorkloadMix, schema: synthhop.SchemaP6}, "")
		line := fmt.Sprintf("P7 size=%-6s nodes | %d nodes %.1f B/node %.2f objects/node",
			sz.Name, nodesHOP.nodes, float64(nodesHOP.graphBytes-empty.graphBytes)/float64(nodesHOP.nodes),
			float64(nodesHOP.objects-empty.objects)/float64(nodesHOP.nodes))
		t.Log(line)
		fmt.Println(line)
		for _, rn := range s2List("RHO_TKG_P7_RUNS", "hop-p6,hop-legacy,mix-p6") {
			run, ok := p7Runs[rn]
			if !ok {
				t.Fatalf("unknown run %q", rn)
			}
			base := nodesHOP
			if run.workload == synthhop.WorkloadMix {
				base = nodesMix
			}
			r := runP7(t, sz, run, profile)
			line := fmt.Sprintf("P7 size=%-6s run=%-10s rels=%d hop=%d | %.1f B/rel %.2f objects/rel | write %.2fs (%.0f K rels/s) | ByType %.2fM rows/s ForEachByType %.2fM rows/s Get %.3fM/s",
				sz.Name, r.run, r.rels, r.hop,
				float64(r.graphBytes-base.graphBytes)/float64(r.rels), float64(r.objects-base.objects)/float64(r.rels),
				r.write.Seconds(), float64(r.rels)/r.write.Seconds()/1e3, r.byType/1e6, r.forEach/1e6, r.gets/1e6)
			t.Log(line)
			fmt.Println(line)
		}
	}
}

// TestRowStoreP7ScaleSmoke keeps the harness compiling and working in the
// ordinary suite on a tiny workload, with sanity bounds only.
func TestRowStoreP7ScaleSmoke(t *testing.T) {
	sz := synthhop.Size{Name: "smoke", HOP: 2000, Pairs: 600, VATTR: 700, CATTR: 5, Hosts: 100, Actors: 20}
	for _, rn := range []string{"hop-p6", "mix-legacy"} {
		r := runP7(t, sz, p7Runs[rn], "")
		if r.rels == 0 || r.hop != sz.HOP || r.graphBytes <= 0 || r.byType <= 0 || r.forEach <= 0 || r.gets <= 0 {
			t.Fatalf("%s: empty measurement %+v", rn, r)
		}
	}
}
