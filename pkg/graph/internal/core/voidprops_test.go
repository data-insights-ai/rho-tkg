package core

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// VOIDPROPS (goal 1, every stored value once; sigma 27 Oct, TestStateCopiesSplit:
// 168-336 excise rows in 60 cuts, each repeating the voided version's 100-400 B
// properties). An excise's void piece was a copy of the version it voids, its
// properties included, though no read takes a value from a void row: it says
// only that the entity is absent over its interval. A void piece now keeps the
// voided version's labels (a relationship's type and endpoints) and stores no
// properties.

// TestVoidPropsExciseRowStoresNoValues: after a bounded and an open excise of a
// node and a bounded excise of a relationship, each with a 400-byte property,
// the void rows History reads carry no properties and the voided version's
// labels, type and endpoints, a stored void row is smaller than the property,
// the hash chains verify, and every read answers as it did when void rows
// copied the properties: inside the excised spans nothing, outside and at a
// pin before the excise the full version, the effective timeline the two
// pieces around the hole, and a later correction over the hole patches the
// current row.
func TestVoidPropsExciseRowStoresNoValues(t *testing.T) {
	t.Parallel()
	blob := strings.Repeat("x", 400)
	for name, open := range pageDoorStores {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			g, err := New(Config{Store: open(t)})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = g.Close() })
			ctx := context.Background()
			props := func() map[string]any {
				return map[string]any{"k": int64(1), "blob": blob, "tkg_valid_from": types.Instant(1000)}
			}
			n, err := g.Nodes.Add(ctx, []string{"D", "E"}, props())
			if err != nil {
				t.Fatal(err)
			}
			m, err := g.Nodes.Add(ctx, []string{"D"}, props())
			if err != nil {
				t.Fatal(err)
			}
			r, err := g.Rels.Add(ctx, "R", n, m, props())
			if err != nil {
				t.Fatal(err)
			}
			before, err := g.Temporal.NowTx()
			if err != nil {
				t.Fatal(err)
			}
			if err := g.Temporal.ExciseNodeValidity(ctx, n.ID(), 1500, 1600); err != nil {
				t.Fatal(err)
			}
			if err := g.Temporal.ExciseNodeValidity(ctx, n.ID(), 3000, 0); err != nil {
				t.Fatal(err)
			}
			if err := g.Temporal.ExciseRelValidity(ctx, r.ID(), 1500, 1600); err != nil {
				t.Fatal(err)
			}

			full := func(what string, row interface {
				GetProperty(string) (any, bool)
			}, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("%s: %v", what, err)
				}
				if v, _ := row.GetProperty("blob"); v != blob {
					t.Fatalf("%s: blob %d bytes, want the full version", what, len(v.(string)))
				}
			}
			absent := func(what string, err error) {
				t.Helper()
				if !errors.Is(err, storepkg.ErrNoVersionValidAt) {
					t.Fatalf("%s: %v, want ErrNoVersionValidAt", what, err)
				}
			}

			hist, err := g.Nodes.History(n.ID())
			if err != nil {
				t.Fatal(err)
			}
			voids := 0
			for _, row := range hist {
				if !row.Temporal().Void {
					full("node history row", row, nil)
					continue
				}
				voids++
				if row.PropertyCount() != 0 || !slices.Equal(row.AllLabelTokens(), n.AllLabelTokens()) {
					t.Fatalf("node void row v%d: %d properties, labels %v, want none and %v", row.Version(), row.PropertyCount(), row.AllLabelTokens(), n.AllLabelTokens())
				}
			}
			if voids != 2 {
				t.Fatalf("node history holds %d void rows, want 2", voids)
			}
			rhist, err := g.Rels.History(r.ID())
			if err != nil {
				t.Fatal(err)
			}
			voids = 0
			for _, row := range rhist {
				if !row.Temporal().Void {
					full("rel history row", row, nil)
					continue
				}
				voids++
				if row.PropertyCount() != 0 || row.TypeToken() != r.TypeToken() || row.StartNodeID() != n.ID() || row.EndNodeID() != m.ID() {
					t.Fatalf("rel void row v%d: %d properties, type %v %d->%d", row.Version(), row.PropertyCount(), row.TypeToken(), row.StartNodeID(), row.EndNodeID())
				}
			}
			if voids != 1 {
				t.Fatalf("rel history holds %d void rows, want 1", voids)
			}

			if rows, err := g.Admin.NodeStoredSizes(n.ID()); err == nil {
				for _, row := range rows {
					if row.Void {
						t.Logf("stored void row v%d: %d bytes", row.Version, row.Bytes)
					}
					if row.Void && row.Bytes >= len(blob) {
						t.Fatalf("stored void row v%d: %d bytes, the voided property alone is %d", row.Version, row.Bytes, len(blob))
					}
				}
			} else if !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
				t.Fatal(err)
			}

			for _, id := range []types.NodeID{n.ID()} {
				if ok, err := g.Hash.VerifyNodeChain(id); err != nil || !ok {
					t.Fatalf("node chain: %v %v", ok, err)
				}
			}
			if ok, err := g.Hash.VerifyRelChain(r.ID()); err != nil || !ok {
				t.Fatalf("rel chain: %v %v", ok, err)
			}

			for _, at := range []types.Instant{1500, 1599, 3000, 4000} {
				_, err := g.Temporal.NodeAt(n.ID(), at)
				absent("NodeAt in an excised span", err)
			}
			for _, at := range []types.Instant{1000, 1499, 1600, 2999} {
				row, err := g.Temporal.NodeAt(n.ID(), at)
				full("NodeAt outside the excised spans", row, err)
			}
			row, err := g.Temporal.NodeAtTx(n.ID(), 1550, before)
			full("NodeAtTx before the excise", row, err)
			_, err = g.Temporal.RelAt(r.ID(), 1550)
			absent("RelAt in the excised span", err)
			rel, err := g.Temporal.RelAt(r.ID(), 1700)
			full("RelAt after the excised span", rel, err)

			now, err := g.Temporal.NowTx()
			if err != nil {
				t.Fatal(err)
			}
			segs, err := g.Temporal.NodeEffectiveTimeline(n.ID(), now)
			if err != nil {
				t.Fatal(err)
			}
			if len(segs) != 2 || segs[0].ValidFrom != 1000 || segs[0].ValidTo != 1500 || segs[1].ValidFrom != 1600 || segs[1].ValidTo != 3000 {
				t.Fatalf("effective timeline %+v, want [1000,1500) [1600,3000)", segs)
			}
			for _, s := range segs {
				full("effective timeline piece", s.Node, nil)
			}

			if _, err := g.Temporal.SetNodeVersionInterval(ctx, n.ID(), 1500, 1600, map[string]any{"k": int64(2)}); err != nil {
				t.Fatal(err)
			}
			row, err = g.Temporal.NodeAt(n.ID(), 1550)
			full("NodeAt in the refilled hole", row, err)
			if k, _ := row.GetProperty("k"); k != int64(2) {
				t.Fatalf("refilled hole: k = %v, want 2", k)
			}
		})
	}
}
