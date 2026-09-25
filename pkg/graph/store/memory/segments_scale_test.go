package memory_test

// ADR-0011 step S2 scale gate: resident bytes per HOP with HOP declared as a
// bulk type, at the three synthday sizes, next to the undeclared row store,
// plus the read throughput of the row doors and of the internal column path.
//
// Method (the S0 method, per relationship type): the synthday HOP workload
// (internal/synthhop, HOP only) is written through g.Rels().AddWithTx into a
// graph whose store is a memory.Store. The graph's resident bytes are the
// live heap (two GCs) at a measuring point minus the live heap after Close
// with the graph dropped — S0's "graph = store + nodes + rels". They are read
// after the writes (the unsealed tail up to the memtable budget still in the
// row store) and, for the declared store, after a final explicit seal
// (memtable empty). A third run writes the same nodes and no relationship.
// Then, per HOP:
//
//	beyond the memtable = (graph after final seal - graph with nodes only) / HOP
//	with the tail       = (graph after the writes - graph with nodes only) / HOP
//
// Throughput: g.Rels().ByType and ForEachByType over HOP (the row doors),
// g.Rels().Get over 20,000 random HOP IDs, g.ScanRelColumns (ID order; row
// path on the row store, segment columns on the declared type) and
// g.ScanRelSegments (segment order, dictionary codes, no Relationship built).
//
// A measurement, not a unit test: it runs only when RHO_TKG_SEGMENT_S2 is
// set, e.g.
//
//	RHO_TKG_SEGMENT_S2=790k,3.15M,12.6M RHO_TKG_SEGMENT_S2_SCHEMAS=p6,legacy \
//	RHO_TKG_SEGMENT_S2_MODES=rows,segments,nvme [RHO_TKG_SEGMENT_S2_BUDGET_MB=64] \
//	[RHO_TKG_SEGMENT_S3_DIR=/nvme/scratch] \
//	go test -run TestSegmentS2Scale -v -count=1 -timeout 0 ./pkg/graph/store/memory
//
// Mode nvme (ADR-0011 S3) is mode segments with Config.SegmentDir set to a
// fresh directory under RHO_TKG_SEGMENT_S3_DIR (default os.TempDir()):
// segments are files, read memory-mapped. Its "resident" numbers are the Go
// heap; the process RSS is reported next to them, split into anonymous
// memory (the heap and runtime, after debug.FreeOSMemory) and file-backed
// pages (the mapped segment pages the reads touched, plus the test binary's
// text) — file-backed pages are clean and reclaimable, so they are the page
// cache, not a bound on memory.
//
// The numbers are recorded in CHANGELOG (Unreleased) and ADR-0011 §6 (S2, S3).

import (
	"bufio"
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/internal/synthhop"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func s2Heap() uint64 {
	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}

// s2Spec declares HOP for the given synthhop schema: every string property as
// a string column, t_lo / t_hi as int64 columns; the legacy support list is
// not declared (it stays in the fallback column, as ADR-0011 §4.9 expects).
func s2Spec(schema synthhop.Schema) graph.RelSegmentSpec {
	cols := []graph.SegmentColumn{
		{Name: "actor", Kind: graph.SegmentString}, {Name: "asset_class", Kind: graph.SegmentString},
		{Name: "family", Kind: graph.SegmentString}, {Name: "orch", Kind: graph.SegmentString},
	}
	if schema == synthhop.SchemaLegacy {
		cols = append(cols, graph.SegmentColumn{Name: "scenario", Kind: graph.SegmentString},
			graph.SegmentColumn{Name: "t_lo", Kind: graph.SegmentInt64}, graph.SegmentColumn{Name: "t_hi", Kind: graph.SegmentInt64})
	}
	return graph.RelSegmentSpec{Type: "HOP", Columns: cols}
}

type s2Result struct {
	graphWithTail, graphSealed      int64 // resident graph bytes
	sections                        map[string]int
	size, schema, mode              string
	hop                             int
	withTail, sealed                float64 // B/HOP
	segBytes, tailBytes             float64 // B/HOP from the store's stats
	segments                        int
	seals                           uint64
	write, finalSeal                time.Duration
	byType, forEach, point, colRate float64 // rows/s
	byTypeAgain, scanColsRate       float64
	reopen                          time.Duration // mode nvme: New over the directory (map + verify)
	// RSS (mode nvme and segments): anonymous and file-backed resident
	// bytes after the final seal and after the reads, and file-backed at
	// the start of the run (the binary's text).
	rssAnonSealed, rssFileSealed, rssAnonRead, rssFileRead, rssFileStart int64
}

// rssNow returns the process's resident anonymous and file-backed bytes
// (/proc/self/status RssAnon, RssFile; 0, 0 where unavailable), after
// returning freed heap to the OS so RssAnon is the live footprint.
func rssNow() (anon, file int64) {
	debug.FreeOSMemory()
	f, err := os.Open("/proc/self/status")
	if err != nil {
		return 0, 0
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		k, v, ok := strings.Cut(sc.Text(), ":")
		if !ok {
			continue
		}
		fields := strings.Fields(v)
		if len(fields) != 2 || fields[1] != "kB" {
			continue
		}
		n, err := strconv.ParseInt(fields[0], 10, 64)
		if err != nil {
			continue
		}
		switch k {
		case "RssAnon":
			anon = n << 10
		case "RssFile":
			file = n << 10
		}
	}
	return anon, file
}

func (r s2Result) String() string {
	var sec strings.Builder
	if len(r.sections) > 0 {
		names := make([]string, 0, len(r.sections))
		for k := range r.sections {
			names = append(names, k)
		}
		sort.Slice(names, func(i, j int) bool { return r.sections[names[i]] > r.sections[names[j]] })
		sec.WriteString("\n    sections B/HOP:")
		for _, k := range names {
			fmt.Fprintf(&sec, " %s=%.2f", k, float64(r.sections[k])/float64(r.hop))
		}
	}
	rss := ""
	if r.rssAnonSealed > 0 {
		mb := func(b int64) float64 { return float64(b) / (1 << 20) }
		rss = fmt.Sprintf("\n    RSS MiB: after final seal anon %.0f file %.0f; after reads anon %.0f file %.0f (file at start %.0f: +%.1f B/HOP mapped pages)",
			mb(r.rssAnonSealed), mb(r.rssFileSealed), mb(r.rssAnonRead), mb(r.rssFileRead), mb(r.rssFileStart),
			float64(r.rssFileRead-r.rssFileStart)/float64(r.hop))
	}
	if r.reopen > 0 {
		rss += fmt.Sprintf("\n    reopen (map + verify every integrity group) %.2fs = %.2fM rows/s", r.reopen.Seconds(), float64(r.hop)/r.reopen.Seconds()/1e6)
	}
	return fmt.Sprintf("S2 size=%-6s schema=%-6s mode=%-8s hop=%d | resident %.1f B/HOP beyond memtable, %.1f B/HOP with tail | segments %d (%d seals) %.2f B/HOP encoded, tail %.1f B/HOP | write %.1fs final seal %.2fs | ByType %.2fM rows/s (again %.2fM) ForEachByType %.2fM rows/s Get %.3fM/s ScanRelColumns %.2fM rows/s ScanRelSegments %.1fM rows/s",
		r.size, r.schema, r.mode, r.hop, r.sealed, r.withTail, r.segments, r.seals, r.segBytes, r.tailBytes,
		r.write.Seconds(), r.finalSeal.Seconds(), r.byType/1e6, r.byTypeAgain/1e6, r.forEach/1e6, r.point/1e6, r.scanColsRate/1e6, r.colRate/1e6) + rss + sec.String()
}

func runS2(tb testing.TB, sz synthhop.Size, schema synthhop.Schema, mode string) s2Result {
	tb.Helper()
	ctx := context.Background()
	res := s2Result{size: sz.Name, mode: mode, schema: map[synthhop.Schema]string{synthhop.SchemaP6: "p6", synthhop.SchemaLegacy: "legacy"}[schema]}
	st := memory.New()
	cfg := graph.Config{Store: st, Validation: graph.ValidationLimits{AllowSelfLoops: true}, AllowTxBackfill: true}
	_, res.rssFileStart = rssNow()
	sealing := mode == "segments" || mode == "nvme"
	if mode == "nvme" {
		root := os.Getenv("RHO_TKG_SEGMENT_S3_DIR")
		if root == "" {
			root = os.TempDir()
		}
		dir, err := os.MkdirTemp(root, "rho-tkg-s3-")
		if err != nil {
			tb.Fatal(err)
		}
		defer os.RemoveAll(dir)
		cfg.SegmentDir = dir
	}
	if sealing {
		cfg.RelSegments = []graph.RelSegmentSpec{s2Spec(schema)}
		if v := os.Getenv("RHO_TKG_SEGMENT_S2_BUDGET_MB"); v != "" {
			mb, err := strconv.Atoi(v)
			if err != nil || mb <= 0 {
				tb.Fatalf("RHO_TKG_SEGMENT_S2_BUDGET_MB=%q", v)
			}
			cfg.SegmentMemoryBudget = int64(mb) << 20
		}
	}
	g, err := graph.New(cfg)
	if err != nil {
		tb.Fatal(err)
	}
	var nodes []*types.Node
	ids := make([]types.RelID, 0, sz.HOP)
	var start time.Time
	err = synthhop.Generate(synthhop.Config{Size: sz, Schema: schema, Workload: synthhop.WorkloadHOP},
		func(n synthhop.Node) error {
			node, err := g.Nodes().Add(ctx, []string{"Asset"}, n.Props)
			nodes = append(nodes, node)
			return err
		},
		func(e synthhop.Edge) error {
			if mode == "nodes" {
				return nil
			}
			if start.IsZero() {
				start = time.Now()
			}
			r, err := g.Rels().AddWithTx(ctx, e.Type, nodes[e.Start], nodes[e.End], e.Props, types.Instant(e.TxFrom))
			if err == nil {
				ids = append(ids, r.ID())
			}
			return err
		})
	if err != nil {
		tb.Fatal(err)
	}
	if mode != "nodes" {
		res.write = time.Since(start)
	}
	res.hop = len(ids)
	nodes = nil // the harness's node copies are not the graph's (S0)
	runtime.KeepAlive(nodes)
	if sealing {
		s2WaitSealer(tb, g) // the tail reading excludes a background seal's buffers
	}
	hW := s2Heap()
	hS := hW
	if sealing {
		pre, err := g.Admin().RelSegmentStats("HOP")
		if err != nil {
			tb.Fatal(err)
		}
		res.tailBytes = float64(pre.UnsealedBytes) / float64(res.hop)
		t0 := time.Now()
		if err := g.Admin().SealRelSegments("HOP"); err != nil {
			tb.Fatal(err)
		}
		res.finalSeal = time.Since(t0)
		stats, err := g.Admin().RelSegmentStats("HOP")
		if err != nil {
			tb.Fatal(err)
		}
		if stats.LiveSealedRows != int64(res.hop) || stats.UnsealedRows != 0 {
			tb.Fatalf("after the final seal every HOP row is sealed: %+v", stats)
		}
		hS = s2Heap()
		res.rssAnonSealed, res.rssFileSealed = rssNow()
		res.segments, res.seals = stats.Segments, stats.Seals
		res.segBytes = float64(stats.SegmentBytes) / float64(res.hop)
	}
	if mode != "nodes" {
		t0 := time.Now()
		rs, err := g.Rels().ByType("HOP", graph.QueryOpts{})
		if err != nil || len(rs) != res.hop {
			tb.Fatalf("ByType: %d rows, %v", len(rs), err)
		}
		res.byType = float64(len(rs)) / time.Since(t0).Seconds()
		rs = nil
		runtime.KeepAlive(rs)
		t0 = time.Now()
		rs, _ = g.Rels().ByType("HOP", graph.QueryOpts{})
		res.byTypeAgain = float64(len(rs)) / time.Since(t0).Seconds()
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
			if _, err := g.Rels().Get(ctx, ids[rng.IntN(len(ids))]); err != nil {
				tb.Fatal(err)
			}
		}
		res.point = probes / time.Since(t0).Seconds()
	}
	if mode != "nodes" {
		// ScanRelColumns: the ID-ordered column door (row path on the row
		// store, segment columns on a declared type).
		props := []string{"actor", "asset_class", "orch", "family"}
		t0 := time.Now()
		n := 0
		ok, err := g.ScanRelColumns("HOP", props, graph.QueryOpts{}, func(b *graph.RelColumnBatch) bool { n += len(b.IDs); return true })
		if err != nil || !ok || n != res.hop {
			tb.Fatalf("ScanRelColumns: %d rows, %t, %v", n, ok, err)
		}
		res.scanColsRate = float64(n) / time.Since(t0).Seconds()
	}
	if sealing {
		// ScanRelSegments: the segment-order column door S5 adds, touching
		// every value a consumer reads.
		props := []string{"actor", "asset_class", "orch", "family"}
		t0 := time.Now()
		rows, sum := 0, 0
		ok, err := g.ScanRelSegments("HOP", props, func(b *graph.RelSegmentBatch) bool {
			for k := 0; k < b.Len(); k++ {
				sum += int(b.StartIDs[k]^b.EndIDs[k]) + int(b.ValidFrom[k]^b.ValidTo[k])
				for c := range b.Cols {
					if b.Cols[c].Present[k] && b.Cols[c].Codes != nil {
						sum += int(b.Cols[c].Codes[k])
					}
				}
			}
			rows += b.Len()
			return true
		})
		if err != nil || !ok || rows != res.hop {
			tb.Fatalf("ScanRelSegments: %d rows, %t, %v", rows, ok, err)
		}
		runtime.KeepAlive(sum)
		res.colRate = float64(rows) / time.Since(t0).Seconds()
		res.sections = memory.SealedSectionBytesForTest(st, 1)
		res.rssAnonRead, res.rssFileRead = rssNow()
	}
	if err := g.Close(); err != nil {
		tb.Fatal(err)
	}
	if mode == "nvme" {
		// Reopen: a new graph over the directory maps, verifies (every
		// integrity group) and serves every sealed row.
		rcfg := cfg
		rcfg.Store = memory.New()
		t0 := time.Now()
		g2, err := graph.New(rcfg)
		if err != nil {
			tb.Fatalf("reopen: %v", err)
		}
		res.reopen = time.Since(t0)
		stats, err := g2.Admin().RelSegmentStats("HOP")
		if err != nil || stats.LiveSealedRows != int64(res.hop) {
			tb.Fatalf("reopen serves %+v, %v; want %d rows", stats, err, res.hop)
		}
		if err := g2.Close(); err != nil {
			tb.Fatal(err)
		}
	}
	g, st = nil, nil
	runtime.KeepAlive(g)
	runtime.KeepAlive(st)
	h2 := s2Heap()
	runtime.KeepAlive(ids)
	res.graphWithTail = int64(hW) - int64(h2)
	res.graphSealed = int64(hS) - int64(h2)
	return res
}

// s2WaitSealer waits until the store's background sealer is idle.
func s2WaitSealer(tb testing.TB, g *graph.Graph) {
	tb.Helper()
	deadline := time.Now().Add(5 * time.Minute)
	for {
		st, err := g.Admin().RelSegmentStats("HOP")
		if err != nil {
			tb.Fatal(err)
		}
		if !st.Sealing {
			return
		}
		if time.Now().After(deadline) {
			tb.Fatal("background sealer did not go idle")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func s2List(key, def string) []string {
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

// TestSegmentS2Scale is the S2 measurement (see the file comment).
func TestSegmentS2Scale(t *testing.T) {
	if os.Getenv("RHO_TKG_SEGMENT_S2") == "" {
		t.Skip("measurement: set RHO_TKG_SEGMENT_S2=790k,3.15M,12.6M to run")
	}
	for _, name := range s2List("RHO_TKG_SEGMENT_S2", "") {
		sz, err := synthhop.SizeByName(name)
		if err != nil {
			t.Fatal(err)
		}
		for _, sc := range s2List("RHO_TKG_SEGMENT_S2_SCHEMAS", "p6") {
			schema := synthhop.SchemaP6
			if sc == "legacy" {
				schema = synthhop.SchemaLegacy
			}
			base := runS2(t, sz, schema, "nodes")
			for _, mode := range s2List("RHO_TKG_SEGMENT_S2_MODES", "rows,segments") {
				res := s2PerHOP(runS2(t, sz, schema, mode), base)
				t.Log(res.String())
				fmt.Println(res.String())
			}
		}
	}
}

// s2PerHOP turns graph bytes into bytes per HOP over the nodes-only run.
func s2PerHOP(r, nodesOnly s2Result) s2Result {
	r.withTail = float64(r.graphWithTail-nodesOnly.graphWithTail) / float64(r.hop)
	r.sealed = float64(r.graphSealed-nodesOnly.graphWithTail) / float64(r.hop)
	return r
}

// TestSegmentS2ScaleSmoke keeps the harness compiling and working in the
// ordinary suite on a tiny workload, with sanity bounds only.
func TestSegmentS2ScaleSmoke(t *testing.T) {
	sz := synthhop.Size{Name: "smoke", HOP: 3000, Pairs: 900, VATTR: 10, CATTR: 5, Hosts: 120, Actors: 30}
	base := runS2(t, sz, synthhop.SchemaP6, "nodes")
	if base.graphWithTail <= 0 {
		t.Fatalf("nodes-only run measured no graph bytes: %+v", base)
	}
	for _, mode := range []string{"rows", "segments", "nvme"} {
		res := s2PerHOP(runS2(t, sz, synthhop.SchemaP6, mode), base)
		if res.hop != sz.HOP || res.byType <= 0 || res.forEach <= 0 || res.point <= 0 || res.graphSealed <= 0 {
			t.Fatalf("%s: empty measurement %s", mode, res)
		}
		if mode != "rows" && (res.segments == 0 || res.colRate <= 0 || res.segBytes <= 0) {
			t.Fatalf("segments: nothing sealed %s", res)
		}
	}
}
