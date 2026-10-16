package graph_test

import (
	"context"
	"fmt"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/ingest"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestTxUpdateValidToClosesLikeCloseVersion (sigma-tkgd R9, backlog 65): an
// update that sets only tkg_valid_to = t closes the version exactly as
// Nodes() / Rels().CloseVersion(id, t) does, through every update door: a
// GraphTx that writes the fact and then closes it with UpdateNode /
// UpdateRelationship, GraphTx UpdateNodeWithTx / UpdateRelationshipWithTx,
// Nodes() / Rels().UpdateWithTx, a Batch and an ingest session. After the
// write every answer equals the CloseVersion twin's: the version kept its
// start and ends at t, the chain has the same shape, and the as-of read just
// before the close still answers the open version. Four backends. Answers are
// compared relative to each entity's start (its TxFrom).
func TestTxUpdateValidToClosesLikeCloseVersion(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	const span = 1_000
	end := func(tx types.Instant) map[string]any { return map[string]any{"tkg_valid_to": tx + span} }
	for _, be := range allStoreBackendsWith(func(c *graphpkg.Config) { c.AllowTxBackfill = true; txFromCfg(c) }) {
		t.Run(be.name, func(t *testing.T) {
			g := be.open(t)
			add := func() (*types.Node, *types.Relationship) {
				t.Helper()
				a, err := g.Nodes().Add(ctx, []string{"N"}, map[string]any{"k": "a"})
				if err != nil {
					t.Fatal(err)
				}
				c, err := g.Nodes().Add(ctx, []string{"N"}, map[string]any{"k": "c"})
				if err != nil {
					t.Fatal(err)
				}
				r, err := g.Rels().Add(ctx, "R", a, c, nil)
				if err != nil {
					t.Fatal(err)
				}
				return a, r
			}
			now := func() types.Instant {
				t.Helper()
				at, err := g.Temporal().NowTx()
				if err != nil {
					t.Fatal(err)
				}
				return at
			}
			type pair struct {
				n types.NodeID
				r types.RelID
			}
			closed := map[string]pair{}

			// The twin: CloseVersion.
			a, r := add()
			if err := g.Nodes().CloseVersion(ctx, a.ID(), a.Temporal().TxFrom+span); err != nil {
				t.Fatal(err)
			}
			if err := g.Rels().CloseVersion(ctx, r.ID(), r.Temporal().TxFrom+span); err != nil {
				t.Fatal(err)
			}
			closed["CloseVersion"] = pair{a.ID(), r.ID()}

			// A GraphTx that writes the fact and closes it.
			if err := g.Tx().Run(func(tx *graphpkg.GraphTx) error {
				a, err := tx.AddNode([]string{"N"}, map[string]any{"k": "a"})
				if err != nil {
					return err
				}
				c, err := tx.AddNode([]string{"N"}, map[string]any{"k": "c"})
				if err != nil {
					return err
				}
				r, err := tx.AddRelationship("R", a, c, nil)
				if err != nil {
					return err
				}
				closed["tx UpdateNode"] = pair{a.ID(), r.ID()}
				if _, err := tx.UpdateNode(a.ID(), end(a.Temporal().TxFrom)); err != nil {
					return err
				}
				_, err = tx.UpdateRelationship(r.ID(), end(r.Temporal().TxFrom))
				return err
			}); err != nil {
				t.Fatalf("tx: %v", err)
			}

			a, r = add()
			at := now()
			if err := g.Tx().Run(func(tx *graphpkg.GraphTx) error {
				if _, err := tx.UpdateNodeWithTx(a.ID(), end(a.Temporal().TxFrom), at); err != nil {
					return err
				}
				_, err := tx.UpdateRelationshipWithTx(r.ID(), end(r.Temporal().TxFrom), at)
				return err
			}); err != nil {
				t.Fatalf("tx WithTx: %v", err)
			}
			closed["tx UpdateWithTx"] = pair{a.ID(), r.ID()}

			a, r = add()
			at = now()
			if _, err := g.Nodes().UpdateWithTx(ctx, a.ID(), end(a.Temporal().TxFrom), at); err != nil {
				t.Fatal(err)
			}
			if _, err := g.Rels().UpdateWithTx(ctx, r.ID(), end(r.Temporal().TxFrom), at); err != nil {
				t.Fatal(err)
			}
			closed["UpdateWithTx"] = pair{a.ID(), r.ID()}

			a, r = add()
			bb, err := g.Batch().New()
			if err != nil {
				t.Fatal(err)
			}
			_ = bb.UpdateNode(a.ID(), end(a.Temporal().TxFrom))
			_ = bb.UpdateRelationship(r.ID(), end(r.Temporal().TxFrom))
			if res, err := bb.Execute(); err != nil || len(res.Errors) != 0 {
				t.Fatalf("batch close: %v %+v", err, res)
			}
			closed["Batch"] = pair{a.ID(), r.ID()}

			a, r = add()
			s, err := g.Ingest().NewSession(ingest.IngestOptions{Sync: true})
			if err != nil {
				t.Fatal(err)
			}
			_ = s.UpdateNode(a.ID(), end(a.Temporal().TxFrom))
			_ = s.UpdateRelationship(r.ID(), end(r.Temporal().TxFrom))
			if err := s.Close(); err != nil {
				t.Fatalf("ingest close: %v", err)
			}
			closed["ingest"] = pair{a.ID(), r.ID()}

			nodeAnswers := func(id types.NodeID) string {
				t.Helper()
				hist, err := g.Nodes().History(id)
				if err != nil || len(hist) == 0 {
					t.Fatalf("node history: %d rows, %v", len(hist), err)
				}
				start := hist[0].Temporal().TxFrom
				cur, err := g.Nodes().Get(ctx, id)
				if err != nil {
					t.Fatal(err)
				}
				out := fmt.Sprintf("v%d rows=%d", cur.Version(), len(hist)+1)
				for _, off := range []types.Instant{-1, 0, span - 1, span} {
					if n, err := g.Temporal().NodeAt(id, start+off); err != nil {
						out += fmt.Sprintf(" @%d:-", off)
					} else {
						out += fmt.Sprintf(" @%d:v%d", off, n.Version())
					}
				}
				// As of just before the close: the open version.
				if n, err := g.Temporal().NodeAsOf(id, cur.Temporal().TxFrom-1); err != nil || n == nil {
					out += fmt.Sprintf(" asof:%v", err)
				} else {
					_, to, _ := n.ValidRange()
					out += fmt.Sprintf(" asof:v%d open=%v", n.Version(), to == 0)
				}
				segs, err := g.Temporal().NodeEffectiveTimeline(id, now())
				if err != nil {
					t.Fatal(err)
				}
				for _, sg := range segs {
					out += fmt.Sprintf(" [%d,%d)v%d", sg.ValidFrom-start, sg.ValidTo-start, sg.Node.Version())
				}
				return out
			}
			relAnswers := func(id types.RelID) string {
				t.Helper()
				hist, err := g.Rels().History(id)
				if err != nil || len(hist) == 0 {
					t.Fatalf("rel history: %d rows, %v", len(hist), err)
				}
				start := hist[0].Temporal().TxFrom
				cur, err := g.Rels().Get(ctx, id)
				if err != nil {
					t.Fatal(err)
				}
				out := fmt.Sprintf("v%d rows=%d", cur.Version(), len(hist)+1)
				for _, off := range []types.Instant{-1, 0, span - 1, span} {
					if rr, err := g.Temporal().RelAt(id, start+off); err != nil {
						out += fmt.Sprintf(" @%d:-", off)
					} else {
						out += fmt.Sprintf(" @%d:v%d", off, rr.Version())
					}
				}
				if rr, err := g.Temporal().RelAsOf(id, cur.Temporal().TxFrom-1); err != nil || rr == nil {
					out += fmt.Sprintf(" asof:%v", err)
				} else {
					_, to, _ := rr.ValidRange()
					out += fmt.Sprintf(" asof:v%d open=%v", rr.Version(), to == 0)
				}
				segs, err := g.Temporal().RelEffectiveTimeline(id, now())
				if err != nil {
					t.Fatal(err)
				}
				for _, sg := range segs {
					out += fmt.Sprintf(" [%d,%d)v%d", sg.ValidFrom-start, sg.ValidTo-start, sg.Rel.Version())
				}
				return out
			}

			want := closed["CloseVersion"]
			wantNode, wantRel := nodeAnswers(want.n), relAnswers(want.r)
			if exp := fmt.Sprintf("v1 rows=2 @-1:- @0:v1 @%d:v1 @%d:- asof:v0 open=true [0,%d)v1", span-1, span, span); wantNode != exp {
				t.Errorf("CloseVersion node answers %q, want %q", wantNode, exp)
			}
			for name, p := range closed {
				if got := nodeAnswers(p.n); got != wantNode {
					t.Errorf("%s node close %q, CloseVersion %q", name, got, wantNode)
				}
				if got := relAnswers(p.r); got != wantRel {
					t.Errorf("%s rel close %q, CloseVersion %q", name, got, wantRel)
				}
			}
		})
	}
}
