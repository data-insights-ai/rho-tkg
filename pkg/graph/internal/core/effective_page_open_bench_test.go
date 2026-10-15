package core

import (
	"context"
	"fmt"
	"testing"
	"time"

	badgerv4 "github.com/dgraph-io/badger/v4"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
)

// BenchmarkEffectivePageFirstAfterOpen (backlog 74, ai-soc: "after every
// open, the first page waits about 3 minutes"): a disk store with 29 indexed
// relationship types and B74_BENCH_RELS relationships (default 200,000; every
// fourth updated once, so a quarter carry a history row). Each iteration
// opens the store and asks one page of 500; open-ms and first-page-ms are the
// two halves of "open to first answer". clean reopens after a clean close
// (the open image is trusted); dirty deletes the persisted open state first,
// as after a crash or an older release (the open rebuilds from the rows).
func BenchmarkEffectivePageFirstAfterOpen(b *testing.B) {
	const nTypes, limit = 29, 500
	n := benchEnvInt(b, "B74_BENCH_RELS", 200_000, 1_000, 10_000_000)
	ctx := context.Background()
	dir := b.TempDir()
	g, err := New(Config{BadgerDir: dir})
	if err != nil {
		b.Fatal(err)
	}
	s, err := g.Nodes.Add(ctx, []string{"Host"}, nil)
	if err != nil {
		b.Fatal(err)
	}
	e, err := g.Nodes.Add(ctx, []string{"Host"}, nil)
	if err != nil {
		b.Fatal(err)
	}
	relTypes := make([]string, nTypes)
	for i := range relTypes {
		relTypes[i] = fmt.Sprintf("T%02d", i)
		if err := g.Index.CreateRelProperty(relTypes[i], epProp); err != nil {
			b.Fatal(err)
		}
	}
	for i := 0; i < n; i++ {
		r, err := g.Rels.AddByID(ctx, relTypes[i%nTypes], s.ID(), e.ID(), map[string]any{epProp: fmt.Sprintf("e%d", i%10), "seq": int64(i)})
		if err != nil {
			b.Fatal(err)
		}
		if i%4 == 0 {
			if _, err := g.Rels.Update(ctx, r.ID(), map[string]any{"seq": int64(-i)}); err != nil {
				b.Fatal(err)
			}
		}
	}
	if err := g.Close(); err != nil {
		b.Fatal(err)
	}
	for _, mode := range []string{"clean", "dirty"} {
		b.Run(mode, func(b *testing.B) {
			var openD, pageD time.Duration
			for i := 0; i < b.N; i++ {
				if mode == "dirty" {
					db, err := badgerv4.Open(badgerv4.DefaultOptions(dir).WithLogger(nil))
					if err != nil {
						b.Fatal(err)
					}
					if err := db.Update(badger.DeleteOpenStateForTest); err != nil {
						b.Fatal(err)
					}
					if err := db.Close(); err != nil {
						b.Fatal(err)
					}
				}
				t0 := time.Now()
				g, err := New(Config{BadgerDir: dir})
				if err != nil {
					b.Fatal(err)
				}
				t1 := time.Now()
				pin, _ := g.Temporal.NowTx()
				segs, _, err := g.Temporal.RelsEffectivePage(ctx, temporal.Pin{Tx: pin}, temporal.Filter{Prop: epProp, Eq: "e3"}, 0, limit)
				if err != nil {
					b.Fatal(err)
				}
				t2 := time.Now()
				if len(segs) < limit {
					b.Fatalf("first page holds %d segments, want >= %d", len(segs), limit)
				}
				openD += t1.Sub(t0)
				pageD += t2.Sub(t1)
				if err := g.Close(); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(openD.Milliseconds())/float64(b.N), "open-ms")
			b.ReportMetric(float64(pageD.Milliseconds())/float64(b.N), "first-page-ms")
		})
	}
}
