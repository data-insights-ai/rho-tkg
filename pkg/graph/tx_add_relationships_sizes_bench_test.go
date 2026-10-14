package graph_test

import (
	"fmt"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BenchmarkTxCreateRelationshipsSizes is sigma-tkgd's bulk create statement
// UNWIND range(1, $n) AS i CREATE (:N {i: i})-[:R {w: i}]->(:N {i: -i}) on
// a fresh graph per backend and size, one transaction per statement, the
// close (its flush) timed; "5x50000" runs five statements before the close
// (sustained ingest). Arms:
//
//   - binding: per binding AddNode, AddNode, AddRelationship (the endpoints
//     were written just before their relationship);
//   - rels-one: the starts, then the ends with AddNodes, then one
//     AddRelationshipByID per relationship (sigma-tkgd's write order: one
//     kind after the other, calls of at most 8,192);
//   - rels-batch: the same with AddRelationships calls of at most 8,192.
//
// rels-one against rels-batch isolates the batch door; binding is the
// shape the bulk variant replaces. Reports ns per binding and the time
// inside the relationship create calls per relationship ("rel-ns/rel").
func BenchmarkTxCreateRelationshipsSizes(b *testing.B) {
	const call = 8192
	for _, backend := range []struct {
		name string
		cfg  graphpkg.Config
	}{
		{"memory", graphpkg.Config{SnowflakeNodeID: 0}},
		{"badger", graphpkg.Config{SnowflakeNodeID: 0, BadgerInMemory: true}},
	} {
		for _, size := range []struct{ n, rounds int }{{1000, 1}, {10000, 1}, {50000, 1}, {50000, 5}} {
			n := size.n
			for _, mode := range []string{"binding", "rels-one", "rels-batch"} {
				b.Run(fmt.Sprintf("%s/%dx%d/%s", backend.name, size.rounds, n, mode), func(b *testing.B) {
					b.ReportAllocs()
					var inRels time.Duration
					for b.Loop() {
						g, err := graphpkg.New(backend.cfg)
						if err != nil {
							b.Fatal(err)
						}
						for range size.rounds {
							if err := g.Tx().Run(func(tx *graphpkg.GraphTx) error {
								spent, err := createStatement(tx, mode, n, call)
								inRels += spent
								return err
							}); err != nil {
								b.Fatal(err)
							}
						}
						if err := g.Close(); err != nil {
							b.Fatal(err)
						}
					}
					b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*size.rounds*n), "ns/binding")
					b.ReportMetric(float64(inRels.Nanoseconds())/float64(b.N*size.rounds*n), "rel-ns/rel")
				})
			}
		}
	}
}

func createStatement(tx *graphpkg.GraphTx, mode string, n, call int) (inRels time.Duration, err error) {
	labels := []string{"N"}
	if mode == "binding" {
		for i := range n {
			start, err := tx.AddNode(labels, map[string]any{"i": int64(i)})
			if err != nil {
				return inRels, err
			}
			end, err := tx.AddNode(labels, map[string]any{"i": int64(-i)})
			if err != nil {
				return inRels, err
			}
			at := time.Now()
			_, err = tx.AddRelationship("R", start, end, map[string]any{"w": int64(i)})
			inRels += time.Since(at)
			if err != nil {
				return inRels, err
			}
		}
		return inRels, nil
	}
	nodes := func(sign int64) ([]*types.Node, error) {
		out := make([]*types.Node, 0, n)
		for lo := 0; lo < n; lo += call {
			hi := min(lo+call, n)
			props := make([]map[string]any, 0, hi-lo)
			for i := lo; i < hi; i++ {
				props = append(props, map[string]any{"i": sign * int64(i)})
			}
			made, err := tx.AddNodes(labels, props)
			if err != nil {
				return nil, err
			}
			out = append(out, made...)
		}
		return out, nil
	}
	starts, err := nodes(1)
	if err != nil {
		return 0, err
	}
	ends, err := nodes(-1)
	if err != nil {
		return 0, err
	}
	for lo := 0; lo < n; lo += call {
		hi := min(lo+call, n)
		if mode == "rels-one" {
			for i := lo; i < hi; i++ {
				props := map[string]any{"w": int64(i)}
				at := time.Now()
				_, err := tx.AddRelationshipByID("R", starts[i].ID(), ends[i].ID(), props)
				inRels += time.Since(at)
				if err != nil {
					return inRels, err
				}
			}
			continue
		}
		specs := make([]graphpkg.RelCreate, 0, hi-lo)
		for i := lo; i < hi; i++ {
			specs = append(specs, graphpkg.RelCreate{StartID: starts[i].ID(), EndID: ends[i].ID(), Props: map[string]any{"w": int64(i)}})
		}
		at := time.Now()
		_, err := tx.AddRelationships("R", specs)
		inRels += time.Since(at)
		if err != nil {
			return inRels, err
		}
	}
	return inRels, nil
}
