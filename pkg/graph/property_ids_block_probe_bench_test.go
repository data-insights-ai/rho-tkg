package graph_test

import (
	"fmt"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
)

// BenchmarkPropertyIDsBlockProbe (RHOPOST, sigma's READBACK block probe):
// IDsByLabelAndPropertyValues over an on-disk property index of string
// values, one row per value, flushed; each call asks for a block of adjacent
// values (values=100) or one value (values=1), at a different place each
// call. ns/value is the time per looked-up value.
func BenchmarkPropertyIDsBlockProbe(b *testing.B) {
	for _, backend := range []string{"badger", "tiered"} {
		for _, rows := range []int{5_000, 20_000, 80_000} {
			g := blockProbeGraph(b, backend, rows)
			for _, values := range []int{1, 100} {
				b.Run(fmt.Sprintf("%s/rows=%dk/values=%d", backend, rows/1000, values), func(b *testing.B) {
					b.ReportAllocs()
					ask := make([]any, values)
					calls := 0
					for b.Loop() {
						first := (calls * 7919) % (rows - values)
						calls++
						for i := range ask {
							ask[i] = fmt.Sprintf("v%08d", first+i)
						}
						ids, err := g.Nodes().IDsByLabelAndPropertyValues("Blk", "k", ask, storepkg.QueryOpts{})
						if err != nil || len(ids) != values {
							b.Fatalf("%d ids, %v", len(ids), err)
						}
					}
					b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*values), "ns/value")
				})
			}
			if err := g.Close(); err != nil {
				b.Fatal(err)
			}
		}
	}
}

// blockProbeGraph opens a graph on backend with rows nodes of label Blk whose
// on-disk indexed "k" is v<i>, flushed.
func blockProbeGraph(b *testing.B, backend string, rows int) *graphpkg.Graph {
	b.Helper()
	var st storepkg.MandatoryStore
	var flush func() error
	switch backend {
	case "badger":
		bs, err := badger.New(badger.Config{Dir: b.TempDir(), PropertyIndexOnDisk: true})
		if err != nil {
			b.Fatal(err)
		}
		st, flush = bs, bs.Flush
	default:
		ts, err := tiered.New(tiered.Config{DataDir: b.TempDir(), RefLabels: []string{"Blk"}, ShardWindow: 7 * 24 * time.Hour, PropertyIndexOnDisk: true})
		if err != nil {
			b.Fatal(err)
		}
		st, flush = ts, ts.Flush
	}
	g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 1, Store: st})
	if err != nil {
		b.Fatal(err)
	}
	if err := g.Index().CreateProperty("Blk", "k"); err != nil {
		b.Fatal(err)
	}
	for lo := 0; lo < rows; lo += 4096 {
		props := make([]map[string]any, min(4096, rows-lo))
		for i := range props {
			props[i] = map[string]any{"k": fmt.Sprintf("v%08d", lo+i)}
		}
		if err := g.Tx().Run(func(tx *graphpkg.GraphTx) error { _, err := tx.AddNodes([]string{"Blk"}, props); return err }); err != nil {
			b.Fatal(err)
		}
	}
	if err := flush(); err != nil {
		b.Fatal(err)
	}
	return g
}
