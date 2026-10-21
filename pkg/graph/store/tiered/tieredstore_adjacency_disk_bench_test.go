package tiered

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"testing"
	"time"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Row 95 measurements for ai-soc (tasks/todo.md "R3 sizing"). Both skip under
// -short. Run with -run '^$' -bench <name> -benchtime=1x.
//
// BenchmarkR3OpenShardFootprint: bytes per rel and process RSS after opening
// a tiered store whose event shard holds N rels (d=1: two private endpoints
// per rel, the ai-soc per-instance shape), AdjacencyIndexOnDisk on vs off.
//   RHO_R3_RELS  N (default 200000; ai-soc target 22000000)
//   RHO_R3_ADJ   on | off | both (default both; RSS is cleanest with one mode per process)
//   RHO_R3_DIR   seed directory to keep and reuse across runs (default a temp dir)
//
// BenchmarkR3HotAdjacencyReadPending: one OutgoingRelationships read of a
// flushed 10-rel node on the hot shard while its pending buffer holds P ops.
//   RHO_R3_PENDING  comma list of P (default 0,1000,10000,99000; the buffer
//                   flushes at 100000)

func r3EnvInt(b *testing.B, name string, def int) int {
	b.Helper()
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		b.Fatalf("%s=%q: want a non-negative integer", name, v)
	}
	return n
}

func r3Modes(b *testing.B) []bool {
	b.Helper()
	switch v := os.Getenv("RHO_R3_ADJ"); v {
	case "", "both":
		return []bool{false, true}
	case "on":
		return []bool{true}
	case "off":
		return []bool{false}
	default:
		b.Fatalf("RHO_R3_ADJ=%q: want on, off or both", v)
		return nil
	}
}

// r3RSS returns the process resident set size in bytes (Linux).
func r3RSS(b *testing.B) int64 {
	b.Helper()
	f, err := os.Open("/proc/self/status")
	if err != nil {
		b.Skipf("no /proc/self/status: %v", err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if rest, ok := strings.CutPrefix(sc.Text(), "VmRSS:"); ok {
			kb, err := strconv.ParseInt(strings.TrimSuffix(strings.TrimSpace(rest), " kB"), 10, 64)
			if err != nil {
				b.Fatalf("parse VmRSS %q: %v", rest, err)
			}
			return kb << 10
		}
	}
	b.Fatal("VmRSS not found")
	return 0
}

func r3Config(dir string, adjOnDisk bool) Config {
	return Config{
		DataDir:              dir,
		RefLabels:            []string{"Case"},
		ShardWindow:          30 * 24 * time.Hour,
		FlushInterval:        1<<63 - 1,
		AdjacencyIndexOnDisk: adjOnDisk,
	}
}

// r3Seed writes 2n event nodes and n rels into dir unless a marker of a
// previous seed with the same n is there.
func r3Seed(b *testing.B, dir string, n int) {
	b.Helper()
	marker := filepath.Join(dir, fmt.Sprintf("r3-seeded-%d", n))
	if _, err := os.Stat(marker); err == nil {
		return
	}
	start := time.Now()
	ts, err := New(r3Config(dir, true))
	if err != nil {
		b.Fatalf("seed New: %v", err)
	}
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	_, _ = reg.GetOrCreate("Case")
	signal, _ := reg.GetOrCreate("Signal")
	ngen, rgen := newTestGen(b, 0), newTestGen(b, 1)
	const chunk = 10_000
	nodes := make([]*types.Node, 0, 2*chunk)
	rels := make([]*types.Relationship, 0, chunk)
	for done := 0; done < n; {
		nodes, rels = nodes[:0], rels[:0]
		for i := 0; i < chunk && done < n; i, done = i+1, done+1 {
			s := types.NewNode(types.NodeID(ngen.Generate()), signal, nil)
			e := types.NewNode(types.NodeID(ngen.Generate()), signal, nil)
			nodes = append(nodes, s, e)
			rels = append(rels, types.NewRelationship(types.RelID(rgen.Generate()), 1, s.ID(), e.ID()))
		}
		if err := ts.PutNodesBatch(nodes); err != nil {
			b.Fatalf("seed nodes: %v", err)
		}
		if err := ts.PutRelationshipsBatch(rels); err != nil {
			b.Fatalf("seed rels: %v", err)
		}
	}
	if err := ts.Close(); err != nil {
		b.Fatalf("seed close: %v", err)
	}
	if err := os.WriteFile(marker, nil, 0o600); err != nil {
		b.Fatalf("marker: %v", err)
	}
	b.Logf("seeded %d rels / %d nodes in %s", n, 2*n, time.Since(start).Round(time.Second))
}

func BenchmarkR3OpenShardFootprint(b *testing.B) {
	if testing.Short() {
		b.Skip("long tier: ai-soc R3 measurement")
	}
	n := r3EnvInt(b, "RHO_R3_RELS", 200_000)
	dir := os.Getenv("RHO_R3_DIR")
	if dir == "" {
		dir = b.TempDir()
	} else if err := os.MkdirAll(dir, 0o700); err != nil {
		b.Fatal(err)
	}
	r3Seed(b, dir, n)
	for _, on := range r3Modes(b) {
		b.Run(fmt.Sprintf("adj=%v/rels=%d", on, n), func(b *testing.B) {
			for b.Loop() {
				runtime.GC()
				debug.FreeOSMemory()
				var m0, m1 runtime.MemStats
				runtime.ReadMemStats(&m0)
				rss0 := r3RSS(b)
				start := time.Now()
				ts, err := New(r3Config(dir, on))
				if err != nil {
					b.Fatalf("open: %v", err)
				}
				openDur := time.Since(start)
				runtime.GC()
				runtime.ReadMemStats(&m1)
				rss1 := r3RSS(b)
				var rels, mapBytes float64
				ts.mu.RLock()
				for _, es := range ts.eventShards {
					if s := es.Store(); s != nil {
						c, _ := s.RelationshipCount()
						rels += float64(c)
						if n <= 2_000_000 { // the measurement clones the maps
							mapBytes += s.RelMapBytesForTest()
						}
					}
				}
				ts.mu.RUnlock()
				if int(rels) != n {
					b.Fatalf("open event shards hold %.0f rels, want %d", rels, n)
				}
				heap := float64(int64(m1.HeapAlloc) - int64(m0.HeapAlloc))
				b.ReportMetric(heap/rels, "heapB/rel")
				b.ReportMetric(float64(rss1-rss0)/rels, "rssB/rel")
				b.ReportMetric(float64(rss1)/(1<<20), "rssMiB")
				b.ReportMetric(openDur.Seconds(), "open-s")
				if mapBytes > 0 {
					b.ReportMetric(mapBytes/rels, "relmapB/rel")
				}
				if err := ts.Close(); err != nil {
					b.Fatalf("close: %v", err)
				}
			}
		})
	}
}

func BenchmarkR3HotAdjacencyReadPending(b *testing.B) {
	if testing.Short() {
		b.Skip("long tier: ai-soc R3 measurement")
	}
	var pendings []int
	spec := os.Getenv("RHO_R3_PENDING")
	if spec == "" {
		spec = "0,1000,10000,99000"
	}
	for _, f := range strings.Split(spec, ",") {
		p, err := strconv.Atoi(strings.TrimSpace(f))
		if err != nil || p < 0 || p >= 100_000 {
			b.Fatalf("RHO_R3_PENDING entry %q: want 0 <= P < 100000 (the buffer flushes at 100000)", f)
		}
		pendings = append(pendings, p)
	}
	for _, on := range []bool{false, true} {
		for _, p := range pendings {
			b.Run(fmt.Sprintf("adj=%v/pending=%d", on, p), func(b *testing.B) {
				ts, err := New(Config{
					InMemory:             true,
					RefLabels:            []string{"Case"},
					ShardWindow:          30 * 24 * time.Hour,
					FlushInterval:        1<<63 - 1,
					AdjacencyIndexOnDisk: on,
				})
				if err != nil {
					b.Fatal(err)
				}
				defer ts.Close()
				reg := registrypkg.NewLabelRegistry()
				ts.SetLabelRegistry(reg)
				_, _ = reg.GetOrCreate("Case")
				signal, _ := reg.GetOrCreate("Signal")
				ngen, rgen := newTestGen(b, 0), newTestGen(b, 1)
				hub := types.NodeID(ngen.Generate())
				if err := ts.PutNode(types.NewNode(hub, signal, nil)); err != nil {
					b.Fatal(err)
				}
				for i := 0; i < 10; i++ {
					end := types.NodeID(ngen.Generate())
					if err := ts.PutNode(types.NewNode(end, signal, nil)); err != nil {
						b.Fatal(err)
					}
					if err := ts.PutRelationship(types.NewRelationship(types.RelID(rgen.Generate()), 1, hub, end)); err != nil {
						b.Fatal(err)
					}
				}
				if err := ts.Flush(); err != nil {
					b.Fatal(err)
				}
				ts.mu.RLock()
				hot := ts.hotShard.Store()
				ts.mu.RUnlock()
				// Fill the pending buffer with unrelated rels (between other nodes).
				var a, z types.NodeID
				for hot.PendingWriteCount() < p {
					if a == 0 {
						a, z = types.NodeID(ngen.Generate()), types.NodeID(ngen.Generate())
						if err := ts.PutNodesBatch([]*types.Node{types.NewNode(a, signal, nil), types.NewNode(z, signal, nil)}); err != nil {
							b.Fatal(err)
						}
					}
					if err := ts.PutRelationship(types.NewRelationship(types.RelID(rgen.Generate()), 1, a, z)); err != nil {
						b.Fatal(err)
					}
				}
				got := hot.PendingWriteCount()
				if got < p || (p == 0 && got != 0) {
					b.Fatalf("pending %d, want >= %d (and 0 for 0)", got, p)
				}
				b.ResetTimer()
				for b.Loop() {
					rels, err := ts.OutgoingRelationships(hub, 0)
					if err != nil || len(rels) != 10 {
						b.Fatalf("read: %d rels err %v, want 10", len(rels), err)
					}
				}
				b.StopTimer()
				if hot.PendingWriteCount() != got {
					b.Fatalf("pending buffer changed during the reads: %d -> %d", got, hot.PendingWriteCount())
				}
			})
		}
	}
}
