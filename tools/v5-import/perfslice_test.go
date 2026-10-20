package importmap

import (
	"encoding/json"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	fixtures "github.com/data-insights-ai/rho-tkg/tools/v5-import-fixtures"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/graph"
)

func TestPerfSlice(t *testing.T) {
	exp, work := os.Getenv("PS_EXPORT"), os.Getenv("PS_WORK")
	if exp == "" {
		t.Skip()
	}
	n := 20000
	if s := os.Getenv("PS_N"); s != "" {
		var x int
		_, _ = fmt.Sscan(s, &x)
		n = x
	}
	raw, err := os.ReadFile(exp)
	if err != nil {
		t.Fatal(err)
	}
	src, err := fixtures.ReadExport(fixtures.Config{UntimedValidStart: "tx_from", IntegrityHashes: true}, raw)
	if err != nil {
		t.Fatal(err)
	}
	res, err := Map(src)
	if err != nil {
		t.Fatal(err)
	}
	sub := &Result{Ops: res.Ops[:n]}
	dir := filepath.Join(work, "v5graph")
	os.RemoveAll(dir)
	opt := Options{DropKindConflicts: true}
	fmt.Sscan(os.Getenv("PS_MAXOPS"), &opt.MaxOps)
	cfg, err := sub.Config(baseConfig(t, dir), opt)
	if err != nil {
		t.Fatal(err)
	}
	g, err := graph.Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	ng, ops := 0, map[string]int{}
	for _, o := range sub.Ops {
		gs, _ := o.Graphs(cfg)
		ng += len(gs)
		ops[o.Op]++
	}
	t.Logf("graph ops %d, by importer op %v", ng, ops)
	batches := 0
	last := 0
	opt.AfterBatch = func(n int) error { batches++; last = n; return nil }
	t0 := time.Now()
	rep, err := sub.Import(context.Background(), g, cfg, opt)
	if err != nil {
		p, _ := sub.plan(opt)
		for i := last; i < last+3 && i < len(p.ops); i++ {
			o := p.ops[i]
			b, _ := json.Marshal(o)
			t.Logf("op %d: %s %s %d props=%d json=%d bytes", i, o.Op, o.Entity, o.ID, len(o.Props), len(b))
		}
		t.Fatalf("after %d ops in %s: %v", last, time.Since(t0), err)
	}
	el := time.Since(t0)
	t.Logf("PERF %d ops in %s = %.0f ops/s, %d batches, report commits=%d refused=%d", n, el, float64(n)/el.Seconds(), batches, rep.Commits, rep.Refused)
}

func TestPerfOneBatch(t *testing.T) {
	exp := os.Getenv("PS_EXPORT")
	if exp == "" {
		t.Skip()
	}
	raw, _ := os.ReadFile(exp)
	src, err := fixtures.ReadExport(fixtures.Config{UntimedValidStart: "tx_from", IntegrityHashes: true}, raw)
	if err != nil {
		t.Fatal(err)
	}
	res, _ := Map(src)
	sub := &Result{Ops: res.Ops[:4000]}
	for _, n := range []int{1, 8, 16, 32, 48, 64, 128, 256} {
		dir := t.TempDir()
		cfg, _ := sub.Config(baseConfig(t, dir), Options{DropKindConflicts: true})
		g, err := graph.Open(context.Background(), cfg)
		if err != nil {
			t.Fatal(err)
		}
		p, _ := sub.plan(Options{DropKindConflicts: true})
		w := &writer{ctx: context.Background(), g: g, cfg: cfg, p: p, skipped: map[ref]bool{}}
		// warm: 512 ops in 32s
		for w.applied < 512 {
			if err := w.batch(w.applied, 32); err != nil {
				t.Fatal(err)
			}
			w.applied += 32
		}
		t0 := time.Now()
		err = w.batch(w.applied, n)
		el := time.Since(t0)
		recs := 0
		t.Logf("batch n=%d: %s (%.2f ms/op) err=%v recs=%d", n, el, float64(el.Microseconds())/1000/float64(n), err, recs)
		g.Close()
	}
}
