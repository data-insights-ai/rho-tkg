package graph_test

import (
	"context"
	"errors"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Round 11, the final review of UntimedAtemporal: every door that writes
// without a valid time follows the mode. (1) The GraphTx update doors decide
// the no-op over every segment, not the current row. (2) Compare-and-set is a
// one-version untimed door: whole-axis on a pure entity, refused
// (ErrUntimedModeMismatch) on one with explicit segments; it never starts a
// version at its TxFrom. (3) The caller-instant pre-flight of a batch decides
// what the kernel decides (per segment). (4) The label doors answer the mode's
// refusal before the closed check. Every backend.

// r11Segmented creates a node with k=1 on [100,200) and k=2 on [200,open).
func r11Segmented(t *testing.T, g *graphpkg.Graph) types.NodeID {
	t.Helper()
	ctx := context.Background()
	n, err := g.Nodes().Add(ctx, []string{"A"}, map[string]any{"k": int64(1), types.ShadowValidFrom: types.Instant(100)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.Temporal().SetNodeVersionInterval(ctx, n.ID(), 200, 0, map[string]any{"k": int64(2)}); err != nil {
		t.Fatal(err)
	}
	return n.ID()
}

func TestRound11AtemporalReviewDoors(t *testing.T) {
	ctx := context.Background()
	for _, b := range allStoreBackendsWith(func(c *graphpkg.Config) {
		c.UntimedValidStart = graphpkg.UntimedAtemporal
		c.AllowTxBackfill = true
	}) {
		t.Run(b.name, func(t *testing.T) {
			g := b.open(t)

			// (1) GraphTx: k=2 already holds on the current segment only.
			id := r11Segmented(t, g)
			if err := g.Tx().Run(func(tx *graphpkg.GraphTx) error {
				_, err := tx.UpdateNode(id, map[string]any{"k": int64(2)})
				return err
			}); err != nil {
				t.Fatal(err)
			}
			r11Want(t, "GraphTx k=2: AT 150", r11KAt(t, g, id, "k", 150, 0), "2")

			// (2) Compare-and-set: a pure node, whole axis; a segmented one refused.
			pure, err := g.Nodes().Add(ctx, []string{"A"}, map[string]any{"k": int64(1)})
			if err != nil {
				t.Fatal(err)
			}
			if ok, err := g.Nodes().CompareAndSetProperty(ctx, pure.ID(), "k", int64(1), int64(2)); err != nil || !ok {
				t.Fatalf("CAS on a pure atemporal node: %v, %v", ok, err)
			}
			r11Want(t, "CAS: AT -1e6", r11KAt(t, g, pure.ID(), "k", -1_000_000, 0), "2")
			cur, err := g.Nodes().Get(ctx, pure.ID())
			if err != nil {
				t.Fatal(err)
			}
			if cur.EffectiveValidFrom() != types.UnboundedValidFrom {
				t.Errorf("CAS version starts at %d, want -inf", cur.EffectiveValidFrom())
			}
			if err := g.Nodes().AddLabel(ctx, pure.ID(), "B"); err != nil {
				t.Errorf("label change after a CAS of a pure node: %v", err)
			}
			seg := r11Segmented(t, g)
			if _, err := g.Nodes().CompareAndSetProperty(ctx, seg, "k", int64(2), int64(3)); !errors.Is(err, graphpkg.ErrUntimedModeMismatch) {
				t.Errorf("CAS on a segmented node: %v, want ErrUntimedModeMismatch", err)
			}
			hub, err := g.Nodes().Add(ctx, []string{"H"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			r, err := g.Rels().Add(ctx, "E", hub, pure, map[string]any{"k": int64(1)})
			if err != nil {
				t.Fatal(err)
			}
			if ok, err := g.Rels().CompareAndSetProperty(ctx, r.ID(), "k", int64(1), int64(2)); err != nil || !ok {
				t.Fatalf("CAS on a pure atemporal relationship: %v, %v", ok, err)
			}
			r11Want(t, "rel CAS: AT -1e6", r11RelKAt(t, g, r.ID(), "k", -1_000_000, 0), "2")

			// (3) Batch at a caller instant: k=2 changes [100,200) only.
			id3 := r11Segmented(t, g)
			now := r11NowAfter(t, g)
			bb, err := g.Batch().New()
			if err != nil {
				t.Fatal(err)
			}
			if err := bb.UpdateNodeWithTx(id3, map[string]any{"k": int64(2)}, now); err != nil {
				t.Fatal(err)
			}
			if _, err := bb.Execute(); err != nil {
				t.Fatalf("batch caller-instant update changing an earlier segment: %v", err)
			}
			r11Want(t, "batch k=2: AT 150", r11KAt(t, g, id3, "k", 150, 0), "2")
			// A unit whose update changes no segment is refused before any write.
			other, err := g.Nodes().Add(ctx, []string{"A"}, map[string]any{"k": int64(1)})
			if err != nil {
				t.Fatal(err)
			}
			now = r11NowAfter(t, g)
			bb, err = g.Batch().New()
			if err != nil {
				t.Fatal(err)
			}
			if err := bb.UpdateNodeWithTx(other.ID(), map[string]any{"k": int64(5)}, now); err != nil {
				t.Fatal(err)
			}
			if err := bb.UpdateNodeWithTx(id3, map[string]any{"k": int64(2)}, now); err != nil {
				t.Fatal(err)
			}
			if _, err := bb.Execute(); !errors.Is(err, graphpkg.ErrTxOrder) {
				t.Errorf("batch with a no-op caller-instant update: %v, want ErrTxOrder", err)
			}
			r11Want(t, "the refused unit's other op", r11KAt(t, g, other.ID(), "k", 0, 0), "1")

			// (4) A closed atemporal node: the label door answers the mode.
			closed, err := g.Nodes().Add(ctx, []string{"A"}, map[string]any{"k": int64(1), types.ShadowValidFrom: types.Instant(100)})
			if err != nil {
				t.Fatal(err)
			}
			if err := g.Nodes().CloseVersion(ctx, closed.ID(), 300); err != nil {
				t.Fatal(err)
			}
			if err := g.Nodes().AddLabel(ctx, closed.ID(), "B"); !errors.Is(err, graphpkg.ErrUntimedModeMismatch) {
				t.Errorf("label change of a closed atemporal node: %v, want ErrUntimedModeMismatch", err)
			}
			if _, err := g.Nodes().Update(ctx, closed.ID(), map[string]any{"j": int64(5)}); err != nil {
				t.Fatalf("untimed update of a closed atemporal node: %v", err)
			}
			r11Want(t, "closed: j AT 150", r11KAt(t, g, closed.ID(), "j", 150, 0), "5")
			r11Want(t, "closed: j AT 350", r11KAt(t, g, closed.ID(), "j", 350, 0), "none")
		})
	}
}
