package core

import (
	"context"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// A retracted life was never true — so none of its rows may shape the answer
// of another life either (backlog 43). The case that tells "the retracted
// rows are gone" from "the retracted rows are capped": a life L1 ended by a
// Delete at D1, then a re-import L2 valid from BELOW L1's start, updated at a
// valid start S inside L1's span. While L2 is believed, its update S ends the
// older beliefs from S on (read-time supersession, chain_supersession.go), L1
// included. Once L2 is retracted, L2's update never happened: L1 answers its
// whole span again — exactly what a pin after D1 and before the re-import
// answers. A resolver that only caps L2's rows keeps L2's supersession of L1
// and leaves a hole at valid times in [S, D1).
func TestRetraction_RetractedLifeShapesNoOtherLife(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for _, be := range txbBackends() {
		for _, kind := range []string{"node", "rel"} {
			t.Run(be.name+"/"+kind, func(t *testing.T) {
				g := be.open(t, true)
				var (
					point    func(v, pin types.Instant) (string, error)
					timeline func(pin types.Instant) (string, error)
					del      func() error
					reimport func(vf types.Instant) error
					update   func(vf types.Instant) error
					retract  func() error
				)
				switch kind {
				case "node":
					n, err := g.Nodes.Add(ctx, []string{"Ev"}, map[string]any{"k": 1, "tkg_valid_from": types.Instant(1000)})
					if err != nil {
						t.Fatalf("Add: %v", err)
					}
					id := n.ID()
					point = func(v, pin types.Instant) (string, error) {
						r, err := g.Temporal.NodeAtTx(id, v, pin)
						if err != nil {
							return "", err
						}
						return nodeRowString(r), nil
					}
					timeline = func(pin types.Instant) (string, error) {
						segs, err := g.Temporal.NodeEffectiveTimeline(id, pin)
						return nodeSegmentsString(segs), err
					}
					del = func() error { return g.Nodes.Delete(ctx, id) }
					reimport = func(vf types.Instant) error {
						_, err := g.Nodes.Import(ctx, id, []string{"Ev"}, map[string]any{"k": 2, "tkg_valid_from": vf})
						return err
					}
					update = func(vf types.Instant) error {
						_, err := g.Nodes.Update(ctx, id, map[string]any{"k": 3, "tkg_valid_from": vf})
						return err
					}
					retract = func() error { return g.Nodes.Retract(ctx, id) }
				case "rel":
					s, e := txbEndpoints(t, g)
					sn, err := g.Nodes.Get(ctx, s)
					if err != nil {
						t.Fatal(err)
					}
					en, err := g.Nodes.Get(ctx, e)
					if err != nil {
						t.Fatal(err)
					}
					r, err := g.Rels.AddByID(ctx, "LINK", s, e, map[string]any{"k": 1, "tkg_valid_from": types.Instant(1000)})
					if err != nil {
						t.Fatalf("AddByID: %v", err)
					}
					id := r.ID()
					point = func(v, pin types.Instant) (string, error) {
						r, err := g.Temporal.RelAtTx(id, v, pin)
						if err != nil {
							return "", err
						}
						return relRowString(r), nil
					}
					timeline = func(pin types.Instant) (string, error) {
						segs, err := g.Temporal.RelEffectiveTimeline(id, pin)
						return relSegmentsString(segs), err
					}
					del = func() error { return g.Rels.Delete(ctx, id) }
					reimport = func(vf types.Instant) error {
						_, err := g.Rels.Import(ctx, id, "LINK", sn, en, map[string]any{"k": 2, "tkg_valid_from": vf})
						return err
					}
					update = func(vf types.Instant) error {
						_, err := g.Rels.Update(ctx, id, map[string]any{"k": 3, "tkg_valid_from": vf})
						return err
					}
					retract = func() error { return g.Rels.Retract(ctx, id) }
				}

				if err := del(); err != nil {
					t.Fatalf("Delete (ends L1): %v", err)
				}
				pinL1, err := g.Temporal.NowTx()
				if err != nil {
					t.Fatal(err)
				}
				if err := reimport(500); err != nil {
					t.Fatalf("re-import (L2 valid from 500, below L1's 1000): %v", err)
				}
				if err := update(2000); err != nil {
					t.Fatalf("update L2 at valid 2000 (inside L1's span): %v", err)
				}
				pinL2, err := g.Temporal.NowTx()
				if err != nil {
					t.Fatal(err)
				}
				if got, err := point(3000, pinL2); err != nil || got == "" {
					t.Fatalf("fixture: L2 must answer valid 3000 while believed: %q, %v", got, err)
				}
				if err := retract(); err != nil {
					t.Fatalf("Retract (L2): %v", err)
				}
				pinAfter, err := g.Temporal.NowTx()
				if err != nil {
					t.Fatal(err)
				}
				for _, v := range []types.Instant{700, 1000, 1500, 2000, 3000, 10_000} {
					want, wantErr := point(v, pinL1)
					got, gotErr := point(v, pinAfter)
					if got != want || (wantErr == nil) != (gotErr == nil) {
						t.Fatalf("valid %d after the retraction of L2: got %q (%v); want L1's answer before L2 existed: %q (%v)", v, got, gotErr, want, wantErr)
					}
				}
				want, err := timeline(pinL1)
				if err != nil {
					t.Fatalf("timeline at pinL1: %v", err)
				}
				got, err := timeline(pinAfter)
				if err != nil || got != want {
					t.Fatalf("timeline after the retraction of L2:\n got  %s (%v)\n want %s", got, err, want)
				}
				if l2, err := timeline(pinL2); err != nil || l2 == want {
					t.Fatalf("fixture: L2's timeline must differ from L1's: %q (%v)", l2, err)
				}
			})
		}
	}
}
