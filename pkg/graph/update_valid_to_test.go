package graph_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/ingest"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestUpdateValidToClosesTheVersion (backlog 65): an update that sets
// tkg_valid_to without tkg_valid_from closes the current version as
// CloseVersion does — the version keeps its start and ends at tkg_valid_to —
// and an end at or before that start is ErrInvalidTimeRange and writes
// nothing, through Update, UpdateInPlace, the GraphTx, batch and ingest twins,
// for nodes and relationships, on every backend. CloseVersion itself keeps the
// start of an untimed version (it used to start the closing row at the close
// instant, so a close in the past did not end the entity before it was written).
func TestUpdateValidToClosesTheVersion(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	early := map[string]any{"tkg_valid_to": types.Instant(5_000)}
	for _, be := range allStoreBackendsWith(func(c *graphpkg.Config) { c.AllowTxBackfill = true; txFromCfg(c) }) {
		t.Run(be.name, func(t *testing.T) {
			g := be.open(t)
			a, err := g.Nodes().Add(ctx, []string{"D"}, map[string]any{"tkg_valid_from": types.Instant(10_000), "s": "v"})
			if err != nil {
				t.Fatal(err)
			}
			b, err := g.Nodes().Add(ctx, []string{"D"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			r, err := g.Rels().Add(ctx, "R", a, b, map[string]any{"tkg_valid_from": types.Instant(10_000)})
			if err != nil {
				t.Fatal(err)
			}
			wantErr := func(name string, err error) {
				t.Helper()
				if !errors.Is(err, graphpkg.ErrInvalidTimeRange) {
					t.Errorf("%s: err = %v, want ErrInvalidTimeRange", name, err)
				}
			}
			// An end before the version's start (10000) is refused everywhere.
			_, err = g.Nodes().Update(ctx, a.ID(), early)
			wantErr("node Update", err)
			_, err = g.Rels().Update(ctx, r.ID(), early)
			wantErr("rel Update", err)
			_, err = g.Nodes().UpdateInPlace(ctx, a.ID(), early)
			wantErr("node UpdateInPlace", err)
			_, err = g.Rels().UpdateInPlace(ctx, r.ID(), early)
			wantErr("rel UpdateInPlace", err)
			_, err = g.Nodes().Update(ctx, a.ID(), map[string]any{"tkg_valid_to": types.Instant(10_000)})
			wantErr("node Update, end == start", err)
			wantErr("tx node", g.Tx().Run(func(tx *graphpkg.GraphTx) error {
				_, err := tx.UpdateNode(a.ID(), early)
				return err
			}))
			wantErr("tx rel", g.Tx().Run(func(tx *graphpkg.GraphTx) error {
				_, err := tx.UpdateRelationship(r.ID(), early)
				return err
			}))
			bb, err := g.Batch().New()
			if err != nil {
				t.Fatal(err)
			}
			_ = bb.UpdateNode(a.ID(), early)
			_ = bb.UpdateRelationship(r.ID(), early)
			res, _ := bb.Execute()
			if res == nil || len(res.Errors) != 2 {
				t.Errorf("batch: result %+v, want two refused updates", res)
			} else {
				wantErr("batch node", res.Errors[0].Err)
				wantErr("batch rel", res.Errors[1].Err)
			}
			s, err := g.Ingest().NewSession(ingest.IngestOptions{Sync: true})
			if err != nil {
				t.Fatal(err)
			}
			_ = s.UpdateNode(a.ID(), early)
			wantErr("ingest node", s.Close())

			// The caller-instant (WithTx) units are all-or-nothing: one valid update
			// next to one refused close writes nothing — the pre-flight runs the
			// same close check (backlog 65).
			at, err := g.Temporal().NowTx()
			if err != nil {
				t.Fatal(err)
			}
			bw, err := g.Batch().New()
			if err != nil {
				t.Fatal(err)
			}
			_ = bw.UpdateNodeWithTx(b.ID(), map[string]any{"s": "x"}, at)
			_ = bw.UpdateNodeWithTx(a.ID(), early, at)
			_, err = bw.Execute()
			wantErr("WithTx batch, node close", err)
			bw, err = g.Batch().New()
			if err != nil {
				t.Fatal(err)
			}
			_ = bw.UpdateNodeWithTx(b.ID(), map[string]any{"s": "x"}, at)
			_ = bw.UpdateRelationshipWithTx(r.ID(), early, at)
			_, err = bw.Execute()
			wantErr("WithTx batch, rel close", err)
			for _, rel := range []bool{false, true} {
				sw, err := g.Ingest().NewSession(ingest.IngestOptions{Sync: true})
				if err != nil {
					t.Fatal(err)
				}
				_ = sw.UpdateNodeWithTx(b.ID(), map[string]any{"s": "x"}, at)
				if rel {
					_ = sw.UpdateRelationshipWithTx(r.ID(), early, at)
				} else {
					_ = sw.UpdateNodeWithTx(a.ID(), early, at)
				}
				wantErr(fmt.Sprintf("WithTx ingest, close (rel=%v)", rel), sw.Close())
			}
			if h, _ := g.Nodes().History(b.ID()); len(h) != 0 {
				t.Errorf("the valid half of a refused WithTx unit was written: node history rows = %d, want 0", len(h))
			}
			if h, _ := g.Nodes().History(a.ID()); len(h) != 0 {
				t.Errorf("node history rows = %d after refused updates, want 0", len(h))
			}
			if h, _ := g.Rels().History(r.ID()); len(h) != 0 {
				t.Errorf("rel history rows = %d after refused updates, want 0", len(h))
			}

			// A past end after the start closes the version where it started.
			if _, err := g.Nodes().Update(ctx, a.ID(), map[string]any{"tkg_valid_to": types.Instant(20_000)}); err != nil {
				t.Fatalf("node close at 20000: %v", err)
			}
			if _, err := g.Rels().Update(ctx, r.ID(), map[string]any{"tkg_valid_to": types.Instant(20_000)}); err != nil {
				t.Fatalf("rel close at 20000: %v", err)
			}
			for _, c := range []struct {
				at   types.Instant
				want bool
			}{{9_999, false}, {10_000, true}, {19_999, true}, {20_000, false}} {
				_, err := g.Temporal().NodeAt(a.ID(), c.at)
				if got := err == nil; got != c.want {
					t.Errorf("closed node NodeAt(%d) valid = %v (%v), want %v", c.at, got, err, c.want)
				}
				_, err = g.Temporal().RelAt(r.ID(), c.at)
				if got := err == nil; got != c.want {
					t.Errorf("closed rel RelAt(%d) valid = %v (%v), want %v", c.at, got, err, c.want)
				}
			}

			// An untimed version (start = its TxFrom) closed in the past, through
			// Update and through CloseVersion, after the commit clock moved on.
			c1, err := g.Nodes().Add(ctx, []string{"D"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			c2, err := g.Nodes().Add(ctx, []string{"D"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			now, err := g.Temporal().NowTx()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := g.Temporal().AdvanceClock(now + 60_000); err != nil {
				t.Fatal(err)
			}
			for name, close := range map[string]func(n *types.Node, end types.Instant) error{
				"Update": func(n *types.Node, end types.Instant) error {
					_, err := g.Nodes().Update(ctx, n.ID(), map[string]any{"tkg_valid_to": end})
					return err
				},
				"CloseVersion": func(n *types.Node, end types.Instant) error {
					return g.Nodes().CloseVersion(ctx, n.ID(), end)
				},
			} {
				n := c1
				if name == "CloseVersion" {
					n = c2
				}
				start := n.Temporal().TxFrom
				end := start + 1_000
				if err := close(n, end); err != nil {
					t.Fatalf("%s untimed close: %v", name, err)
				}
				for _, c := range []struct {
					at   types.Instant
					want bool
				}{{start - 1, false}, {start, true}, {end - 1, true}, {end, false}, {now + 59_000, false}} {
					_, err := g.Temporal().NodeAt(n.ID(), c.at)
					if got := err == nil; got != c.want {
						t.Errorf("%s untimed close: NodeAt(start%+d) valid = %v (%v), want %v", name, c.at-start, got, err, c.want)
					}
				}
			}
		})
	}
}
