package graph_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/ingest"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// The GraphTx, BatchBuilder and ingest.Session caller-instant node doors
// through the pkg/graph facade on every backend — the node twin of
// TestRelsWithTx_TxBatchIngestFacade (rule 2), and the ordering rule (t after
// the current TxFrom and after every recorded TxTo) on each door with
// errors.Is at the public layer (rule 4).
//
// Catches: a public door that forwards to its plain twin (t equal to TxFrom
// then succeeds and the node is stamped with the clock), a rule that reads
// only the current row (a bounded cascade that leaves the current row in its
// slot appends a history row recorded after the current TxFrom, so t between
// them must refuse), an
// order refusal that does not match graph.ErrTxOrder / graph.ErrInvalidTxFrom
// through the batch or ingest wrapping, and a refusal that still writes.
// Counterpart: TxFrom+1 is stamped exactly (and, for the delete, on the
// cascaded relationship).
func TestNodesWithTx_TxBatchIngestFacade(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backends := allStoreBackendsWith(func(c *graphpkg.Config) { c.AllowTxBackfill = true })
	for _, b := range backends {
		t.Run(b.name, func(t *testing.T) {
			g := b.open(t)
			batch := func(q func(bb *graphpkg.BatchBuilder) error) error {
				_, err := g.Batch().Run(q)
				return err
			}
			session := func(concurrent bool, q func(s *ingest.Session) error) error {
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
			upd := map[string]any{"w": int64(2)}
			type door struct {
				name string
				run  func(id types.NodeID, at types.Instant) error
			}
			doors := []door{
				{"GraphTx.DeleteNodeWithTx", func(id types.NodeID, at types.Instant) error {
					return g.Tx().Run(func(tx *graphpkg.GraphTx) error { return tx.DeleteNodeWithTx(id, at) })
				}},
				{"GraphTx.UpdateNodeWithTx", func(id types.NodeID, at types.Instant) error {
					return g.Tx().Run(func(tx *graphpkg.GraphTx) error {
						_, err := tx.UpdateNodeWithTx(id, upd, at)
						return err
					})
				}},
				{"BatchBuilder.DeleteNodeWithTx", func(id types.NodeID, at types.Instant) error {
					return batch(func(bb *graphpkg.BatchBuilder) error { return bb.DeleteNodeWithTx(id, at) })
				}},
				{"BatchBuilder.UpdateNodeWithTx", func(id types.NodeID, at types.Instant) error {
					return batch(func(bb *graphpkg.BatchBuilder) error { return bb.UpdateNodeWithTx(id, upd, at) })
				}},
			}
			for _, concurrent := range []bool{false, true} {
				mode := fmt.Sprintf("Session(concurrent=%v)", concurrent)
				doors = append(doors,
					door{mode + ".DeleteNodeWithTx", func(id types.NodeID, at types.Instant) error {
						return session(concurrent, func(s *ingest.Session) error { return s.DeleteNodeWithTx(id, at) })
					}},
					door{mode + ".UpdateNodeWithTx", func(id types.NodeID, at types.Instant) error {
						return session(concurrent, func(s *ingest.Session) error { return s.UpdateNodeWithTx(id, upd, at) })
					}},
				)
			}
			base := types.Instant(time.Now().Add(-2 * time.Hour).UnixMilli())
			vf := map[string]any{"tkg_valid_from": base - 1000}
			refused := func(name string, id types.NodeID, at types.Instant, run func(types.NodeID, types.Instant) error) {
				t.Helper()
				before, err := g.Nodes().Get(ctx, id)
				if err != nil {
					t.Fatalf("Get: %v", err)
				}
				hist, _ := g.Nodes().History(id)
				err = run(id, at)
				if !errors.Is(err, graphpkg.ErrTxOrder) || !errors.Is(err, graphpkg.ErrInvalidTxFrom) {
					t.Fatalf("%s(t=%d) err = %v; want ErrTxOrder wrapping ErrInvalidTxFrom", name, at, err)
				}
				cur, err := g.Nodes().Get(ctx, id)
				hist2, _ := g.Nodes().History(id)
				if err != nil || cur.Version() != before.Version() || cur.Temporal().TxFrom != before.Temporal().TxFrom || len(hist2) != len(hist) {
					t.Fatalf("%s(t=%d) refused but the node changed: %v, %v, history %d -> %d", name, at, cur, err, len(hist), len(hist2))
				}
			}
			for _, d := range doors {
				// t at and below the current TxFrom.
				n, err := g.Nodes().AddWithTx(ctx, []string{"Ref"}, map[string]any{"tkg_valid_from": base - 1000, "w": int64(1)}, base)
				if err != nil {
					t.Fatalf("AddWithTx: %v", err)
				}
				ev, err := g.Nodes().AddWithTx(ctx, []string{"Ev"}, vf, base)
				if err != nil {
					t.Fatalf("AddWithTx: %v", err)
				}
				r, err := g.Rels().AddWithTx(ctx, "LINK", n, ev, vf, base)
				if err != nil {
					t.Fatalf("Rels.AddWithTx: %v", err)
				}
				for _, at := range []types.Instant{base, base - 1} {
					refused(d.name, n.ID(), at, d.run)
				}
				if _, err := g.Rels().Get(ctx, r.ID()); err != nil {
					t.Fatalf("%s refused but the cascaded rel is gone: %v", d.name, err)
				}
				if err := d.run(n.ID(), base+1); err != nil {
					t.Fatalf("%s(TxFrom+1): %v", d.name, err)
				}
				hist, err := g.Nodes().History(n.ID())
				if err != nil || len(hist) != 1 || hist[0].Temporal().TxTo != base+1 {
					t.Fatalf("%s(TxFrom+1): history %v, %v; want one row with TxTo = %d", d.name, hist, err, base+1)
				}
				if rh, err := g.Rels().History(r.ID()); d.name[len(d.name)-len("DeleteNodeWithTx"):] == "DeleteNodeWithTx" &&
					(err != nil || len(rh) != 1 || rh[0].Temporal().TxTo != base+1) {
					t.Fatalf("%s: cascaded rel history %v, %v; want one tombstone with TxTo = %d", d.name, rh, err, base+1)
				}

				// t below a history TxFrom that lies above the current TxFrom: a
				// bounded cascade before the first valid-from appends its row
				// above the current row, which keeps its slot. (The fixture was
				// a delete plus a backfilled re-import inside the first life,
				// refused since backlog 38; the core tests keep that stored
				// shape, TestTxBackfillNode_OrderEqualReversed.)
				m, err := g.Nodes().AddWithTx(ctx, []string{"Ref"}, map[string]any{"tkg_valid_from": base - 1000, "w": int64(1)}, base)
				if err != nil {
					t.Fatalf("AddWithTx: %v", err)
				}
				if _, err := g.Temporal().SetNodeVersionInterval(ctx, m.ID(), base-3000, base-2000, map[string]any{"w": int64(2)}); err != nil {
					t.Fatalf("SetNodeVersionInterval: %v", err)
				}
				var hiTxFrom types.Instant
				mh, _ := g.Nodes().History(m.ID())
				for _, h := range mh {
					hiTxFrom = max(hiTxFrom, h.Temporal().TxFrom)
				}
				if cur, err := g.Nodes().Get(ctx, m.ID()); err != nil || cur.Temporal().TxFrom != base || hiTxFrom <= base+20 {
					t.Fatalf("fixture: current %v (%v), history TxFrom %d not above %d", cur, err, hiTxFrom, base+20)
				}
				for _, at := range []types.Instant{base + 20, hiTxFrom} {
					refused(d.name+" below history TxFrom", m.ID(), at, d.run)
				}
			}
		})
	}
}
