package graph_test

import (
	"context"
	"errors"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/ingest"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// The caller-instant interval corrections through the public surface —
// g.Temporal(), GraphTx, BatchBuilder and ingest.Session (strong and
// concurrent) — on every backend, node and relationship (errors.Is at the
// facade layer, rule 4).
//
// Catches: a public door that forwards to its plain twin (t = TxFrom then
// succeeds, and the rows carry the clock), a facade or batch/ingest wrapper
// that swallows or replaces the sentinel (graph.ErrTxOrder,
// graph.ErrInvalidTxFrom, graph.ErrEntityDeleted, graph.ErrTxBackfillDisabled
// must match at this layer). One errors.Is row per sentinel per door (node or
// relationship; the stamping and parity are covered in internal/core).
func TestIntervalWithTx_PublicDoors(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	type door struct {
		name string
		node func(g *graphpkg.Graph, id types.NodeID, vf, vt types.Instant, p map[string]any, at types.Instant) error
		rel  func(g *graphpkg.Graph, id types.RelID, vf, vt types.Instant, p map[string]any, at types.Instant) error
	}
	session := func(g *graphpkg.Graph, concurrent bool, q func(s *ingest.Session) error) error {
		s, err := g.Ingest().NewSession(ingest.IngestOptions{Sync: true, Concurrent: concurrent})
		if err != nil {
			return err
		}
		qErr := q(s)
		_, subErr := s.Submit()
		if cErr := s.Close(); cErr != nil {
			return cErr
		}
		if qErr != nil {
			return qErr
		}
		return subErr
	}
	doors := []door{
		{"Temporal",
			func(g *graphpkg.Graph, id types.NodeID, vf, vt types.Instant, p map[string]any, at types.Instant) error {
				_, err := g.Temporal().SetNodeVersionIntervalWithTx(ctx, id, vf, vt, p, at)
				return err
			},
			func(g *graphpkg.Graph, id types.RelID, vf, vt types.Instant, p map[string]any, at types.Instant) error {
				_, err := g.Temporal().SetRelVersionIntervalWithTx(ctx, id, vf, vt, p, at)
				return err
			}},
		{"GraphTx",
			func(g *graphpkg.Graph, id types.NodeID, vf, vt types.Instant, p map[string]any, at types.Instant) error {
				return g.Tx().Run(func(tx *graphpkg.GraphTx) error {
					_, err := tx.SetNodeVersionIntervalWithTx(id, vf, vt, p, at)
					return err
				})
			},
			func(g *graphpkg.Graph, id types.RelID, vf, vt types.Instant, p map[string]any, at types.Instant) error {
				return g.Tx().Run(func(tx *graphpkg.GraphTx) error {
					_, err := tx.SetRelVersionIntervalWithTx(id, vf, vt, p, at)
					return err
				})
			}},
		{"BatchBuilder",
			func(g *graphpkg.Graph, id types.NodeID, vf, vt types.Instant, p map[string]any, at types.Instant) error {
				_, err := g.Batch().Run(func(bb *graphpkg.BatchBuilder) error { return bb.SetNodeVersionIntervalWithTx(id, vf, vt, p, at) })
				return err
			},
			func(g *graphpkg.Graph, id types.RelID, vf, vt types.Instant, p map[string]any, at types.Instant) error {
				_, err := g.Batch().Run(func(bb *graphpkg.BatchBuilder) error { return bb.SetRelVersionIntervalWithTx(id, vf, vt, p, at) })
				return err
			}},
	}
	for _, concurrent := range []bool{false, true} {
		name := "Session"
		if concurrent {
			name = "Session(concurrent)"
		}
		doors = append(doors, door{name,
			func(g *graphpkg.Graph, id types.NodeID, vf, vt types.Instant, p map[string]any, at types.Instant) error {
				return session(g, concurrent, func(s *ingest.Session) error { return s.SetNodeVersionIntervalWithTx(id, vf, vt, p, at) })
			},
			func(g *graphpkg.Graph, id types.RelID, vf, vt types.Instant, p map[string]any, at types.Instant) error {
				return session(g, concurrent, func(s *ingest.Session) error { return s.SetRelVersionIntervalWithTx(id, vf, vt, p, at) })
			}})
	}
	for _, b := range allStoreBackendsWith(func(c *graphpkg.Config) { c.AllowTxBackfill = true }) {
		for _, d := range doors {
			t.Run(b.name+"/"+d.name, func(t *testing.T) {
				g := b.open(t)
				base := types.Instant(time.Now().Add(-2 * time.Hour).UnixMilli())
				props := map[string]any{"tkg_valid_from": base - 3_600_000}
				n, err := g.Nodes().AddWithTx(ctx, []string{"Ref"}, props, base)
				if err != nil {
					t.Fatalf("Nodes.AddWithTx: %v", err)
				}
				e, err := g.Nodes().AddWithTx(ctx, []string{"Ev"}, props, base)
				if err != nil {
					t.Fatalf("Nodes.AddWithTx: %v", err)
				}
				r, err := g.Rels().AddWithTx(ctx, "LINK", n, e, props, base)
				if err != nil {
					t.Fatalf("Rels.AddWithTx: %v", err)
				}
				vf := base - 1_000_000
				if err := d.rel(g, r.ID(), vf, 0, nil, base); !errors.Is(err, graphpkg.ErrTxOrder) || !errors.Is(err, graphpkg.ErrInvalidTxFrom) {
					t.Fatalf("t = TxFrom err = %v; want graph.ErrTxOrder wrapping graph.ErrInvalidTxFrom", err)
				}
				if err := d.rel(g, r.ID(), vf, 0, nil, -1); !errors.Is(err, graphpkg.ErrInvalidTxFrom) {
					t.Fatalf("t = -1 err = %v; want graph.ErrInvalidTxFrom", err)
				}
				if err := g.Nodes().Delete(ctx, n.ID()); err != nil {
					t.Fatalf("Nodes.Delete: %v", err)
				}
				at, _ := g.Temporal().NowTx()
				if err := d.node(g, n.ID(), vf, 0, nil, at); !errors.Is(err, graphpkg.ErrEntityDeleted) || !errors.Is(err, graphpkg.ErrNodeNotFound) {
					t.Fatalf("deleted node err = %v; want graph.ErrEntityDeleted and graph.ErrNodeNotFound", err)
				}
			})
		}
	}
	for _, b := range allStoreBackendsWith(nil) {
		for _, d := range doors {
			t.Run(b.name+"/"+d.name+"/gate_off", func(t *testing.T) {
				g := b.open(t)
				n, err := g.Nodes().Add(ctx, []string{"Ref"}, map[string]any{"tkg_valid_from": types.Instant(1_000)})
				if err != nil {
					t.Fatalf("Add: %v", err)
				}
				at, _ := g.Temporal().NowTx()
				if err := d.node(g, n.ID(), 2_000, 0, nil, at); !errors.Is(err, graphpkg.ErrTxBackfillDisabled) {
					t.Fatalf("gate off err = %v; want graph.ErrTxBackfillDisabled", err)
				}
			})
		}
	}
}
