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

// The GraphTx, BatchBuilder and ingest.Session caller-instant relationship
// doors through the pkg/graph facade on every backend.
//
// Catches: a public door that forwards to its plain twin (t equal to TxFrom
// then succeeds and the row is stamped with the clock), an order refusal that
// does not match graph.ErrTxOrder / graph.ErrInvalidTxFrom at the public
// layer (a batch or ingest wrapper swallowing the sentinel), and a refusal
// that still writes. Counterpart: TxFrom+1 is stamped exactly.
func TestRelsWithTx_TxBatchIngestFacade(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backends := allStoreBackendsWith(func(c *graphpkg.Config) { c.AllowTxBackfill = true })
	for _, b := range backends {
		t.Run(b.name, func(t *testing.T) {
			g := b.open(t)
			start, err := g.Nodes().Add(ctx, []string{"Ref"}, nil)
			if err != nil {
				t.Fatalf("Add: %v", err)
			}
			end, err := g.Nodes().Add(ctx, []string{"Ev"}, nil)
			if err != nil {
				t.Fatalf("Add: %v", err)
			}
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
			doors := []struct {
				name string
				run  func(id types.RelID, at types.Instant) error
			}{
				{"GraphTx.DeleteRelationshipWithTx", func(id types.RelID, at types.Instant) error {
					return g.Tx().Run(func(tx *graphpkg.GraphTx) error { return tx.DeleteRelationshipWithTx(id, at) })
				}},
				{"GraphTx.UpdateRelationshipWithTx", func(id types.RelID, at types.Instant) error {
					return g.Tx().Run(func(tx *graphpkg.GraphTx) error {
						_, err := tx.UpdateRelationshipWithTx(id, upd, at)
						return err
					})
				}},
				{"BatchBuilder.DeleteRelationshipWithTx", func(id types.RelID, at types.Instant) error {
					return batch(func(bb *graphpkg.BatchBuilder) error { return bb.DeleteRelationshipWithTx(id, at) })
				}},
				{"BatchBuilder.UpdateRelationshipWithTx", func(id types.RelID, at types.Instant) error {
					return batch(func(bb *graphpkg.BatchBuilder) error { return bb.UpdateRelationshipWithTx(id, upd, at) })
				}},
			}
			for _, concurrent := range []bool{false, true} {
				doors = append(doors,
					struct {
						name string
						run  func(id types.RelID, at types.Instant) error
					}{"Session.DeleteRelationshipWithTx", func(id types.RelID, at types.Instant) error {
						return session(concurrent, func(s *ingest.Session) error { return s.DeleteRelationshipWithTx(id, at) })
					}},
					struct {
						name string
						run  func(id types.RelID, at types.Instant) error
					}{"Session.UpdateRelationshipWithTx", func(id types.RelID, at types.Instant) error {
						return session(concurrent, func(s *ingest.Session) error { return s.UpdateRelationshipWithTx(id, upd, at) })
					}},
				)
			}
			base := types.Instant(time.Now().Add(-2 * time.Hour).UnixMilli())
			for _, door := range doors {
				r, err := g.Rels().AddWithTx(ctx, "LINK", start, end, map[string]any{"tkg_valid_from": base - 1000, "w": int64(1)}, base)
				if err != nil {
					t.Fatalf("AddWithTx: %v", err)
				}
				err = door.run(r.ID(), base)
				if !errors.Is(err, graphpkg.ErrTxOrder) || !errors.Is(err, graphpkg.ErrInvalidTxFrom) {
					t.Fatalf("%s(t = TxFrom) err = %v; want ErrTxOrder wrapping ErrInvalidTxFrom", door.name, err)
				}
				cur, err := g.Rels().Get(ctx, r.ID())
				if err != nil || cur.Version() != r.Version() || cur.Temporal().TxFrom != base {
					t.Fatalf("%s refused but the row changed: %v, %v", door.name, cur, err)
				}
				if err := door.run(r.ID(), base+1); err != nil {
					t.Fatalf("%s(TxFrom+1): %v", door.name, err)
				}
				hist, err := g.Rels().History(r.ID())
				if err != nil || len(hist) != 1 || hist[0].Temporal().TxTo != base+1 {
					t.Fatalf("%s(TxFrom+1): history %v, %v; want one row with TxTo = %d", door.name, hist, err, base+1)
				}
			}
		})
	}
}
