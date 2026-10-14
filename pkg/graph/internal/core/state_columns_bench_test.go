package core

import (
	"context"
	"fmt"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	badgerpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BenchmarkStateColumns (backlog 28): the state count and column scan of a
// label / type of n entities at {ValidAt: now, TxAt: pin}, every tenth entity
// updated after the pin (sigma's measured shape), against the unpinned column
// scan of the same rows. Each iteration reads at a new valid instant, as
// sigma's "now" does; count_state_new_pin reads at a new pin each time (the
// first build).
func BenchmarkStateColumns(b *testing.B) {
	const n = 100_000
	for _, backend := range []string{"memory", "badger"} {
		for _, rel := range []bool{false, true} {
			g, pin := stBenchGraph(b, backend, rel, n)
			kind := map[bool]string{false: "node", true: "rel"}[rel]
			v := g.readNow()
			scan := func(opts storepkg.QueryOpts) {
				var err error
				if rel {
					_, err = g.ScanRelColumns(ccType, []string{"x"}, opts, func(*storepkg.RelColumnBatch) bool { return true })
				} else {
					_, err = g.ScanNodeColumns(ccLabel, []string{"x"}, opts, func(*storepkg.ColumnBatch) bool { return true })
				}
				if err != nil {
					b.Fatal(err)
				}
			}
			count := func(opts storepkg.QueryOpts) {
				var err error
				if rel {
					_, err = g.Rels.CountByTypeAt(ccType, opts)
				} else {
					_, err = g.Nodes.CountByLabelAt(ccLabel, opts)
				}
				if err != nil {
					b.Fatal(err)
				}
			}
			b.Run(fmt.Sprintf("%s/%s/count_state", backend, kind), func(b *testing.B) {
				count(storepkg.QueryOpts{ValidAt: v, TxAt: pin}) // the first build, untimed
				i := types.Instant(0)
				for b.Loop() {
					i++
					count(storepkg.QueryOpts{ValidAt: v + i, TxAt: pin})
				}
			})
			b.Run(fmt.Sprintf("%s/%s/scan_state", backend, kind), func(b *testing.B) {
				scan(storepkg.QueryOpts{ValidAt: v, TxAt: pin}) // the first build, untimed
				i := types.Instant(0)
				for b.Loop() {
					i++
					scan(storepkg.QueryOpts{ValidAt: v + i, TxAt: pin})
				}
			})
			b.Run(fmt.Sprintf("%s/%s/count_state_new_pin", backend, kind), func(b *testing.B) {
				for b.Loop() { // a pin never read before: the cut's first build (or the fold)
					p, err := g.Temporal.NowTx()
					if err != nil {
						b.Fatal(err)
					}
					count(storepkg.QueryOpts{ValidAt: v, TxAt: p})
				}
			})
			b.Run(fmt.Sprintf("%s/%s/scan_unpinned", backend, kind), func(b *testing.B) {
				for b.Loop() {
					scan(storepkg.QueryOpts{})
				}
			})
			_ = g.Close()
		}
	}
}

// stBenchGraph builds n tracked entities in batches on backend, pins, then
// updates every tenth one.
func stBenchGraph(b *testing.B, backend string, rel bool, n int) (*Core, types.Instant) {
	b.Helper()
	cfg := Config{}
	if backend == "badger" {
		st, err := badgerpkg.New(badgerpkg.Config{InMemory: true, FlushInterval: 1<<63 - 1})
		if err != nil {
			b.Fatal(err)
		}
		cfg.Store = st
	}
	g, err := New(cfg)
	if err != nil {
		b.Fatal(err)
	}
	useTestClock(b, g)
	ctx := context.Background()
	start, err := g.Nodes.Add(ctx, []string{"Ref"}, nil)
	if err != nil {
		b.Fatal(err)
	}
	end, err := g.Nodes.Add(ctx, []string{"Ev"}, nil)
	if err != nil {
		b.Fatal(err)
	}
	var ids []int64
	for at := 0; at < n; at += 10_000 {
		bb, err := NewBatchBuilder(g)
		if err != nil {
			b.Fatal(err)
		}
		for i := at; i < min(at+10_000, n); i++ {
			props := map[string]any{"tkg_valid_from": types.Instant(1000), "x": int64(i)}
			if rel {
				r, err := bb.AddRelationship(ccType, start, end, props)
				if err != nil {
					b.Fatal(err)
				}
				ids = append(ids, int64(r.ID()))
			} else {
				nd, err := bb.AddNode([]string{ccLabel}, props)
				if err != nil {
					b.Fatal(err)
				}
				ids = append(ids, int64(nd.ID()))
			}
		}
		if res, err := bb.Execute(); err != nil || res.Failed != 0 {
			b.Fatalf("Execute: %+v %v", res, err)
		}
	}
	pin, err := g.Temporal.NowTx()
	if err != nil {
		b.Fatal(err)
	}
	bb, err := NewBatchBuilder(g)
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < len(ids); i += 10 {
		if rel {
			err = bb.UpdateRelationship(types.RelID(ids[i]), map[string]any{"x": int64(-i)})
		} else {
			err = bb.UpdateNode(types.NodeID(ids[i]), map[string]any{"x": int64(-i)})
		}
		if err != nil {
			b.Fatal(err)
		}
	}
	if res, err := bb.Execute(); err != nil || res.Failed != 0 {
		b.Fatalf("Execute updates: %+v %v", res, err)
	}
	if f, ok := g.store.(interface{ Flush() error }); ok {
		if err := f.Flush(); err != nil {
			b.Fatal(err)
		}
	}
	return g, pin
}
