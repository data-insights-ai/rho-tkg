package memory_test

// ADR-0011 S3 write gate: the 12.6 M synthday write through g.Rels().AddWithTx
// with HOP declared, S2 (in-RAM segments) and S3 (SegmentDir) alternating,
// with a seal log per run: every seal's snapshot and sealed rows, the rows
// written meanwhile, and where its time went (encode, store, install; for
// S3 the store phase split at the directory's protocol steps).
//
// A measurement, not a unit test:
//
//	RHO_TKG_SEGMENT_S3_GATE=5 [RHO_TKG_SEGMENT_S3_GATE_SIZE=12.6M] [RHO_TKG_SEGMENT_S3_DIR=/nvme/x] \
//	taskset -c 8-15 go test -run TestSegmentS3WriteGate -v -count=1 -timeout 0 ./pkg/graph/store/memory

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/internal/synthhop"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/segdir"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

type s3GateRun struct {
	mode        string
	load1       string
	p7          int
	write       time.Duration // the writes (background seals run concurrently)
	finalSeal   time.Duration
	seals       []memory.SealLogEntry
	steps       [][]time.Time // S3: per seal, the four protocol step times
	stepNames   []string
	hop         int
	segBytes    int64
	writerSeals int
}

// s3P7Procs counts processes whose working directory is the P7 worktree
// (the other agent on this host).
func s3P7Procs() int {
	ents, _ := os.ReadDir("/proc")
	n := 0
	for _, e := range ents {
		if _, err := strconv.Atoi(e.Name()); err != nil {
			continue
		}
		if cwd, err := os.Readlink(filepath.Join("/proc", e.Name(), "cwd")); err == nil && strings.Contains(cwd, "p7-compact") {
			n++
		}
	}
	return n
}

func s3Load1() string {
	b, _ := os.ReadFile("/proc/loadavg")
	f := strings.Fields(string(b))
	if len(f) == 0 {
		return "?"
	}
	return f[0]
}

func runS3Gate(tb testing.TB, sz synthhop.Size, mode string) s3GateRun {
	tb.Helper()
	ctx := context.Background()
	res := s3GateRun{mode: mode, load1: s3Load1(), p7: s3P7Procs()}
	st := memory.New()
	cfg := graph.Config{Store: st, Validation: graph.ValidationLimits{AllowSelfLoops: true}, AllowTxBackfill: true,
		RelSegments: []graph.RelSegmentSpec{s2Spec(synthhop.SchemaP6)}}
	if mode == "S3" {
		root := os.Getenv("RHO_TKG_SEGMENT_S3_DIR")
		if root == "" {
			root = os.TempDir()
		}
		dir, err := os.MkdirTemp(root, "rho-tkg-s3gate-")
		if err != nil {
			tb.Fatal(err)
		}
		defer os.RemoveAll(dir)
		cfg.SegmentDir = dir
	}
	var mu sync.Mutex
	memory.SetSealLogForTest(st, func(r memory.SealLogEntry) {
		mu.Lock()
		res.seals = append(res.seals, r)
		mu.Unlock()
	})
	g, err := graph.New(cfg)
	if err != nil {
		tb.Fatal(err)
	}
	var cur []time.Time
	if mode == "S3" {
		res.stepNames = []string{segdir.StepSegmentWritten, segdir.StepSegmentSynced, segdir.StepManifestWritten, segdir.StepManifestDurable}
		memory.SetSegDirHookForTest(st, func(step string) error {
			mu.Lock()
			cur = append(cur, time.Now())
			if step == segdir.StepManifestDurable {
				res.steps = append(res.steps, cur)
				cur = nil
			}
			mu.Unlock()
			return nil
		})
	}
	var nodes []*types.Node
	var start time.Time
	err = synthhop.Generate(synthhop.Config{Size: sz, Schema: synthhop.SchemaP6, Workload: synthhop.WorkloadHOP},
		func(n synthhop.Node) error {
			node, err := g.Nodes().Add(ctx, []string{"Asset"}, n.Props)
			nodes = append(nodes, node)
			return err
		},
		func(e synthhop.Edge) error {
			if start.IsZero() {
				start = time.Now()
			}
			_, err := g.Rels().AddWithTx(ctx, e.Type, nodes[e.Start], nodes[e.End], e.Props, types.Instant(e.TxFrom))
			if err == nil {
				res.hop++
			}
			return err
		})
	if err != nil {
		tb.Fatal(err)
	}
	res.write = time.Since(start)
	s2WaitSealer(tb, g)
	t0 := time.Now()
	if err := g.Admin().SealRelSegments("HOP"); err != nil {
		tb.Fatal(err)
	}
	res.finalSeal = time.Since(t0)
	stats, err := g.Admin().RelSegmentStats("HOP")
	if err != nil || stats.LiveSealedRows != int64(res.hop) {
		tb.Fatalf("%+v %v", stats, err)
	}
	res.segBytes = stats.SegmentBytes
	if err := g.Close(); err != nil {
		tb.Fatal(err)
	}
	return res
}

func (r s3GateRun) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s load1=%s p7procs=%d write=%.2fs finalSeal=%.2fs seals=%d hop=%d bytes=%.2fB/HOP\n",
		r.mode, r.load1, r.p7, r.write.Seconds(), r.finalSeal.Seconds(), len(r.seals), r.hop, float64(r.segBytes)/float64(r.hop))
	for i, s := range r.seals {
		fmt.Fprintf(&b, "  seal %d explicit=%t snapshot=%d rows=%d writtenMeanwhile=%d bytes=%d encode=%.0fms store=%.0fms install=%.0fms",
			i+1, s.Explicit, s.Snapshot, s.Rows, s.UnsealedAfter, s.Bytes, ms(s.Encode), ms(s.Store), ms(s.Install))
		if i < len(r.steps) && len(r.steps[i]) == 4 {
			st := r.steps[i]
			storeStart := s.Start.Add(s.Encode)
			fmt.Fprintf(&b, " | probe+create+write=%.1fms fsync+rename+dirsync=%.1fms map+open+manifestWrite=%.1fms manifestFsync+rename+dirsync=%.1fms",
				ms(st[0].Sub(storeStart)), ms(st[1].Sub(st[0])), ms(st[2].Sub(st[1])), ms(st[3].Sub(st[2])))
		}
		b.WriteString("\n")
	}
	return b.String()
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

func TestSegmentS3WriteGate(t *testing.T) {
	v := os.Getenv("RHO_TKG_SEGMENT_S3_GATE")
	if v == "" {
		t.Skip("measurement: set RHO_TKG_SEGMENT_S3_GATE=<runs per mode>")
	}
	runs, err := strconv.Atoi(v)
	if err != nil || runs < 1 {
		t.Fatalf("RHO_TKG_SEGMENT_S3_GATE=%q", v)
	}
	name := os.Getenv("RHO_TKG_SEGMENT_S3_GATE_SIZE")
	if name == "" {
		name = "12.6M"
	}
	sz, err := synthhop.SizeByName(name)
	if err != nil {
		t.Fatal(err)
	}
	writes := map[string][]float64{}
	for i := 0; i < runs; i++ {
		for _, mode := range []string{"S2", "S3"} {
			r := runS3Gate(t, sz, mode)
			fmt.Printf("run %d %s", i+1, r)
			writes[mode] = append(writes[mode], r.write.Seconds())
			writes[fmt.Sprintf("%s/%dseals", mode, len(r.seals))] = append(writes[fmt.Sprintf("%s/%dseals", mode, len(r.seals))], r.write.Seconds())
		}
	}
	keys := make([]string, 0, len(writes))
	for k := range writes {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		w := append([]float64(nil), writes[k]...)
		sort.Float64s(w)
		fmt.Printf("SUMMARY %-12s n=%d median=%.2fs min=%.2fs max=%.2fs all=%v\n", k, len(w), w[len(w)/2], w[0], w[len(w)-1], w)
	}
}
