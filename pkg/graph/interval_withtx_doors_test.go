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
// must match at this layer), and a refusal that still writes. Counterpart:
// TxFrom+1 is stamped on every appended row.
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
	type rowStamps map[uint32]types.TemporalMetadata
	nodeRows := func(t *testing.T, g *graphpkg.Graph, id types.NodeID) rowStamps {
		t.Helper()
		out := rowStamps{}
		if n, err := g.Nodes().Get(ctx, id); err == nil {
			out[n.Version()] = *n.Temporal()
		}
		hist, err := g.Nodes().History(id)
		if err != nil {
			t.Fatalf("History: %v", err)
		}
		for _, h := range hist {
			out[h.Version()] = *h.Temporal()
		}
		return out
	}
	relRows := func(t *testing.T, g *graphpkg.Graph, id types.RelID) rowStamps {
		t.Helper()
		out := rowStamps{}
		if r, err := g.Rels().Get(ctx, id); err == nil {
			out[r.Version()] = *r.Temporal()
		}
		hist, err := g.Rels().History(id)
		if err != nil {
			t.Fatalf("History: %v", err)
		}
		for _, h := range hist {
			out[h.Version()] = *h.Temporal()
		}
		return out
	}
	for _, b := range allStoreBackendsWith(func(c *graphpkg.Config) { c.AllowTxBackfill = true }) {
		for _, d := range doors {
			t.Run(b.name+"/"+d.name, func(t *testing.T) {
				g := b.open(t)
				base := types.Instant(time.Now().Add(-2 * time.Hour).UnixMilli())
				vf := base - 3_600_000
				props := map[string]any{"tkg_valid_from": vf, "w": int64(1)}
				n, err := g.Nodes().AddWithTx(ctx, []string{"Ref"}, props, base)
				if err != nil {
					t.Fatalf("Nodes.AddWithTx: %v", err)
				}
				s, err := g.Nodes().AddWithTx(ctx, []string{"Ref"}, props, base)
				if err != nil {
					t.Fatalf("Nodes.AddWithTx: %v", err)
				}
				e, err := g.Nodes().AddWithTx(ctx, []string{"Ev"}, props, base)
				if err != nil {
					t.Fatalf("Nodes.AddWithTx: %v", err)
				}
				r, err := g.Rels().AddWithTx(ctx, "LINK", s, e, props, base)
				if err != nil {
					t.Fatalf("Rels.AddWithTx: %v", err)
				}
				p := map[string]any{"w": int64(2)}

				// Order refusal at t = TxFrom: both sentinels, nothing written.
				nb, rb := nodeRows(t, g, n.ID()), relRows(t, g, r.ID())
				for _, err := range []error{
					d.node(g, n.ID(), vf+1000, 0, p, base),
					d.rel(g, r.ID(), vf+1000, 0, p, base),
				} {
					if !errors.Is(err, graphpkg.ErrTxOrder) || !errors.Is(err, graphpkg.ErrInvalidTxFrom) {
						t.Fatalf("t = TxFrom err = %v; want graph.ErrTxOrder wrapping graph.ErrInvalidTxFrom", err)
					}
				}
				if na, ra := nodeRows(t, g, n.ID()), relRows(t, g, r.ID()); len(na) != len(nb) || len(ra) != len(rb) {
					t.Fatalf("refused but rows changed: node %d -> %d, rel %d -> %d", len(nb), len(na), len(rb), len(ra))
				}
				// Counterpart: TxFrom+1 lands on every appended row.
				if err := d.node(g, n.ID(), vf+1000, 0, p, base+1); err != nil {
					t.Fatalf("node t = TxFrom+1: %v", err)
				}
				if err := d.rel(g, r.ID(), vf+1000, 0, p, base+1); err != nil {
					t.Fatalf("rel t = TxFrom+1: %v", err)
				}
				for v, tm := range nodeRows(t, g, n.ID()) {
					if _, old := nb[v]; !old && (tm.TxFrom != base+1 || tm.UpdatedAt != base+1) {
						t.Fatalf("node appended v%d %+v; want TxFrom = UpdatedAt = %d", v, tm, base+1)
					}
				}
				for v, tm := range relRows(t, g, r.ID()) {
					if _, old := rb[v]; !old && (tm.TxFrom != base+1 || tm.UpdatedAt != base+1) {
						t.Fatalf("rel appended v%d %+v; want TxFrom = UpdatedAt = %d", v, tm, base+1)
					}
				}
				// A hard-deleted entity: graph.ErrEntityDeleted and the not-found sentinel.
				if err := g.Rels().Delete(ctx, r.ID()); err != nil {
					t.Fatalf("Rels.Delete: %v", err)
				}
				if err := g.Nodes().Delete(ctx, n.ID()); err != nil {
					t.Fatalf("Nodes.Delete: %v", err)
				}
				at, _ := g.Temporal().NowTx()
				if err := d.node(g, n.ID(), vf+1000, 0, p, at); !errors.Is(err, graphpkg.ErrEntityDeleted) || !errors.Is(err, graphpkg.ErrNodeNotFound) {
					t.Fatalf("deleted node err = %v; want graph.ErrEntityDeleted and graph.ErrNodeNotFound", err)
				}
				if err := d.rel(g, r.ID(), vf+1000, 0, p, at); !errors.Is(err, graphpkg.ErrEntityDeleted) || !errors.Is(err, graphpkg.ErrRelNotFound) {
					t.Fatalf("deleted rel err = %v; want graph.ErrEntityDeleted and graph.ErrRelNotFound", err)
				}
			})
		}
	}
	// The gate, at the public layer, through every door.
	for _, b := range allStoreBackendsWith(nil) {
		for _, d := range doors {
			t.Run(b.name+"/"+d.name+"/gate_off", func(t *testing.T) {
				g := b.open(t)
				n, err := g.Nodes().Add(ctx, []string{"Ref"}, map[string]any{"tkg_valid_from": types.Instant(1_000)})
				if err != nil {
					t.Fatalf("Add: %v", err)
				}
				e, err := g.Nodes().Add(ctx, []string{"Ev"}, nil)
				if err != nil {
					t.Fatalf("Add: %v", err)
				}
				r, err := g.Rels().Add(ctx, "LINK", n, e, map[string]any{"tkg_valid_from": types.Instant(1_000)})
				if err != nil {
					t.Fatalf("Rels.Add: %v", err)
				}
				at, _ := g.Temporal().NowTx()
				if err := d.node(g, n.ID(), 2_000, 0, nil, at); !errors.Is(err, graphpkg.ErrTxBackfillDisabled) {
					t.Fatalf("node gate off err = %v; want graph.ErrTxBackfillDisabled", err)
				}
				if err := d.rel(g, r.ID(), 2_000, 0, nil, at); !errors.Is(err, graphpkg.ErrTxBackfillDisabled) {
					t.Fatalf("rel gate off err = %v; want graph.ErrTxBackfillDisabled", err)
				}
				if err := d.node(g, n.ID(), 2_000, 0, nil, 0); !errors.Is(err, graphpkg.ErrInvalidTxFrom) {
					t.Fatalf("node t = 0 err = %v; want graph.ErrInvalidTxFrom", err)
				}
				if err := d.rel(g, r.ID(), 2_000, 0, nil, -1); !errors.Is(err, graphpkg.ErrInvalidTxFrom) {
					t.Fatalf("rel t = -1 err = %v; want graph.ErrInvalidTxFrom", err)
				}
			})
		}
	}
}
