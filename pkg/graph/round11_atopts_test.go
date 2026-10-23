package graph_test

import (
	"context"
	"errors"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Round 11 item 3 (sigma-tkgd, 2026-10-17): presence on the per-entity named
// doors. NodeAtTx / RelAtTx read a txAt of 0 as "no pin", so a pin at the
// instant 0 had to be handed over as -1 (sigma's graphhost.PresentPin). The
// QueryOpts forms (NodeAtOpts, RelAtOpts, NodesAtOptsByIDs, RelsAtOptsByIDs)
// take ValidAt / TxAt with explicit presence, as the generic doors do: a pin
// at 0 is the instant 0 (nothing was recorded by then), an absent pin is
// none, and each answers what the generic door (ByLabel / ByType with the
// same opts) answers for that entity.

// TestRound11AtOptsDoors: on every backend, for a node and a relationship
// updated at a later pin (two-phase), every opts form equals the generic
// door's answer for the entity and the named door's where both can say it:
// TxAt absent = NodeAtTx(id, v, 0); a present TxAt p > 0 = NodeAtTx(id, v, p);
// a present TxAt 0 = nothing recorded (ErrNoVersionValidAt, nil in the
// batch), also = NodeAtTx(id, v, -1); ValidAt absent = now.
func TestRound11AtOptsDoors(t *testing.T) {
	t.Parallel()
	for _, b := range allStoreBackends() {
		t.Run(b.name, func(t *testing.T) {
			g := b.open(t)
			ctx := context.Background()
			hub, err := g.Nodes().Add(ctx, []string{"Hub"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			n, err := g.Nodes().Add(ctx, []string{"A"}, map[string]any{"k": int64(1), types.ShadowValidFrom: types.Instant(-100)})
			if err != nil {
				t.Fatal(err)
			}
			r, err := g.Rels().Add(ctx, "E", hub, n, map[string]any{"k": int64(1), types.ShadowValidFrom: types.Instant(-100)})
			if err != nil {
				t.Fatal(err)
			}
			time.Sleep(2 * time.Millisecond)
			p1, err := g.Temporal().NowTx()
			if err != nil {
				t.Fatal(err)
			}
			time.Sleep(2 * time.Millisecond)
			if _, err := g.Nodes().Update(ctx, n.ID(), map[string]any{"k": int64(2)}); err != nil {
				t.Fatal(err)
			}
			if _, err := g.Rels().Update(ctx, r.ID(), map[string]any{"k": int64(2)}); err != nil {
				t.Fatal(err)
			}
			kOf := func(v interface{ GetProperty(string) (any, bool) }, err error) (int64, error) {
				if err != nil {
					return 0, err
				}
				k, _ := v.GetProperty("k")
				return k.(int64), nil
			}
			for _, c := range []struct {
				name  string
				opts  storepkg.QueryOpts
				want  int64 // 0: no version
				named func() (int64, error)
			}{
				{"no pin, valid now", storepkg.QueryOpts{}, 2, func() (int64, error) {
					return kOf(g.Temporal().NodeAtTx(n.ID(), types.Instant(time.Now().UnixMilli()), 0))
				}},
				{"no pin, valid at -50", storepkg.QueryOpts{ValidAt: -50, ValidAtSet: true}, 1, func() (int64, error) {
					return kOf(g.Temporal().NodeAtTx(n.ID(), -50, 0))
				}},
				{"no pin, valid at the instant 0", storepkg.QueryOpts{ValidAt: 0, ValidAtSet: true}, 1, func() (int64, error) {
					return kOf(g.Temporal().NodeAtTx(n.ID(), 0, 0))
				}},
				{"pin p1, valid now", storepkg.QueryOpts{TxAt: p1, TxAtSet: true}, 1, func() (int64, error) {
					return kOf(g.Temporal().NodeAtTx(n.ID(), types.Instant(time.Now().UnixMilli()), p1))
				}},
				{"pin 0 (present)", storepkg.QueryOpts{TxAt: 0, TxAtSet: true}, 0, func() (int64, error) {
					return kOf(g.Temporal().NodeAtTx(n.ID(), types.Instant(time.Now().UnixMilli()), -1))
				}},
				{"pin 0 (present), valid at 0", storepkg.QueryOpts{ValidAt: 0, ValidAtSet: true, TxAt: 0, TxAtSet: true}, 0, func() (int64, error) {
					return kOf(g.Temporal().NodeAtTx(n.ID(), 0, -1))
				}},
				{"pin -5", storepkg.QueryOpts{TxAt: -5, TxAtSet: true}, 0, func() (int64, error) {
					return kOf(g.Temporal().NodeAtTx(n.ID(), types.Instant(time.Now().UnixMilli()), -5))
				}},
			} {
				got, err := kOf(g.Temporal().NodeAtOpts(n.ID(), c.opts))
				if c.want == 0 {
					if !errors.Is(err, graphpkg.ErrNoVersionValidAt) {
						t.Errorf("%s: NodeAtOpts = %d, %v; want ErrNoVersionValidAt", c.name, got, err)
					}
				} else if err != nil || got != c.want {
					t.Errorf("%s: NodeAtOpts = %d, %v; want %d", c.name, got, err, c.want)
				}
				if named, nerr := c.named(); (nerr == nil) != (err == nil) || named != got {
					t.Errorf("%s: NodeAtOpts %d/%v, named door %d/%v", c.name, got, err, named, nerr)
				}
				rgot, rerr := kOf(g.Temporal().RelAtOpts(r.ID(), c.opts))
				if (rerr == nil) != (err == nil) || rgot != got {
					t.Errorf("%s: RelAtOpts %d/%v, NodeAtOpts %d/%v", c.name, rgot, rerr, got, err)
				}
				// The generic door with the same opts lists the same version.
				if c.opts.HasValidAt() || c.opts.HasTxAt() {
					ns, gerr := g.Nodes().ByLabel("A", c.opts)
					if gerr != nil {
						t.Fatal(gerr)
					}
					var gk int64
					for _, x := range ns {
						if x.ID() == n.ID() {
							gk, _ = kOf(x, nil)
						}
					}
					if gk != got {
						t.Errorf("%s: ByLabel lists k=%d, NodeAtOpts k=%d", c.name, gk, got)
					}
				}
				batch, berr := g.Temporal().NodesAtOptsByIDs([]types.NodeID{n.ID(), hub.ID()}, c.opts)
				if berr != nil {
					t.Fatalf("%s: NodesAtOptsByIDs: %v", c.name, berr)
				}
				var bk int64
				if len(batch) != 2 {
					t.Fatalf("%s: NodesAtOptsByIDs returned %d rows, want 2 (nil for none)", c.name, len(batch))
				}
				if batch[0] != nil {
					bk, _ = kOf(batch[0], nil)
				}
				if bk != got {
					t.Errorf("%s: NodesAtOptsByIDs k=%d, NodeAtOpts k=%d", c.name, bk, got)
				}
				rbatch, rberr := g.Temporal().RelsAtOptsByIDs([]types.RelID{r.ID()}, c.opts)
				if rberr != nil || len(rbatch) != 1 {
					t.Fatalf("%s: RelsAtOptsByIDs: %d rows, %v", c.name, len(rbatch), rberr)
				}
				var rbk int64
				if rbatch[0] != nil {
					rbk, _ = kOf(rbatch[0], nil)
				}
				if rbk != got {
					t.Errorf("%s: RelsAtOptsByIDs k=%d, NodeAtOpts k=%d", c.name, rbk, got)
				}
			}
			// Only the point form: a pin of the record door, a window or a page is
			// refused with ErrConflictingTemporalOpts.
			for _, bad := range []storepkg.QueryOpts{
				{TxPin: p1, TxPinSet: true},
				{ValidStart: 0, ValidEnd: 10, ValidWindowSet: true},
				{Limit: 1},
			} {
				if _, err := g.Temporal().NodeAtOpts(n.ID(), bad); !errors.Is(err, graphpkg.ErrConflictingTemporalOpts) {
					t.Errorf("NodeAtOpts(%+v) = %v, want ErrConflictingTemporalOpts", bad, err)
				}
				if _, err := g.Temporal().NodesAtOptsByIDs([]types.NodeID{n.ID()}, bad); !errors.Is(err, graphpkg.ErrConflictingTemporalOpts) {
					t.Errorf("NodesAtOptsByIDs(%+v) = %v, want ErrConflictingTemporalOpts", bad, err)
				}
			}
			future := storepkg.QueryOpts{TxAt: types.Instant(time.Now().Add(time.Hour).UnixMilli()), TxAtSet: true}
			if _, err := g.Temporal().NodesAtOptsByIDs([]types.NodeID{n.ID()}, future); !errors.Is(err, graphpkg.ErrTxPinTooNew) {
				t.Errorf("NodesAtOptsByIDs at a pin above the clock = %v, want ErrTxPinTooNew", err)
			}
			if _, err := g.Temporal().RelsAtOptsByIDs([]types.RelID{r.ID()}, future); !errors.Is(err, graphpkg.ErrTxPinTooNew) {
				t.Errorf("RelsAtOptsByIDs at a pin above the clock = %v, want ErrTxPinTooNew", err)
			}
			if _, err := g.Temporal().NodeAtOpts(0, storepkg.QueryOpts{}); err == nil {
				t.Error("NodeAtOpts(0) = nil error")
			}
		})
	}
}
