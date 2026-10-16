package graph_test

import (
	"context"
	"errors"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestBatchExciseNoVersionRefusesUnit: a batch unit holding a caller-instant
// excise over a range where no version is valid (ErrNoVersionValidAt) must be
// refused WHOLE with nothing written — CHANGELOG v4.54.0 "the batch *WithTx
// twin refuses the whole unit. Refusals, nothing written: ... no version valid
// in [a, b) (ErrNoVersionValidAt)". The plain (no caller instant) excise is the
// documented exception (excise.go BatchBuilder.ExciseNodeValidity godoc: the
// kernel's refusals fail that op alone) and is pinned as such.
//
// Break-the-code cases: the failing excise (node, and the relationship twin
// ExciseRelValidityWithTx) queued first, in the middle and last beside an
// AddNode and an UpdateNode of another node; memory, badger, tiered, sharded.
func TestBatchExciseNoVersionRefusesUnit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	type variant struct {
		rel, withTx bool
	}
	for _, b := range allStoreBackendsWith(func(c *graphpkg.Config) { c.AllowTxBackfill = true }) {
		for _, v := range []variant{{false, true}, {false, false}, {true, true}, {true, false}} {
			for _, pos := range []string{"first", "middle", "last"} {
				rel, withTx := v.rel, v.withTx
				name := b.name + "/node"
				if rel {
					name = b.name + "/rel"
				}
				if withTx {
					name += "/withTx/" + pos
				} else {
					name += "/plain/" + pos
				}
				t.Run(name, func(t *testing.T) {
					g := b.open(t)
					add := func() types.NodeID {
						n, err := g.Nodes().Add(ctx, []string{"D"}, map[string]any{"tkg_valid_from": k(10), "s": "v"})
						if err != nil {
							t.Fatal(err)
						}
						return n.ID()
					}
					target, other := add(), add()
					// The relationship twin's target also starts at k(10).
					var targetRel types.RelID
					if rel {
						from, err := g.Nodes().Get(ctx, target)
						if err != nil {
							t.Fatal(err)
						}
						to, err := g.Nodes().Get(ctx, other)
						if err != nil {
							t.Fatal(err)
						}
						r, err := g.Rels().Add(ctx, "R", from, to, map[string]any{"tkg_valid_from": k(10), "s": "v"})
						if err != nil {
							t.Fatal(err)
						}
						targetRel = r.ID()
					}
					targetHistory := func() int {
						if !rel {
							return historyLen(t, g, target)
						}
						h, err := g.Rels().History(targetRel)
						if err != nil {
							t.Fatalf("Rels().History(%d): %v", targetRel, err)
						}
						return len(h)
					}
					at := nowTx(t, g)
					bb, err := g.Batch().New()
					if err != nil {
						t.Fatal(err)
					}
					excise := func() {
						// Nothing is valid in [k(1), k(5)): the node starts at k(10).
						var err error
						switch {
						case !rel && withTx:
							err = bb.ExciseNodeValidityWithTx(target, k(1), k(5), at)
						case !rel:
							err = bb.ExciseNodeValidity(target, k(1), k(5))
						case withTx:
							err = bb.ExciseRelValidityWithTx(targetRel, k(1), k(5), at)
						default:
							err = bb.ExciseRelValidity(targetRel, k(1), k(5))
						}
						if err != nil {
							t.Fatalf("queue excise: %v", err)
						}
					}
					ghost := func() {
						if _, err := bb.AddNode([]string{"Ghost"}, map[string]any{"s": "g"}); err != nil {
							t.Fatal(err)
						}
					}
					update := func() {
						if err := bb.UpdateNode(other, map[string]any{"s": "changed"}); err != nil {
							t.Fatal(err)
						}
					}
					switch pos {
					case "first":
						excise()
						ghost()
						update()
					case "middle":
						ghost()
						excise()
						update()
					case "last":
						ghost()
						update()
						excise()
					}
					res, err := bb.Execute()
					// A whole-unit refusal wraps the sentinel in the error; a
					// per-op failure reports it in the result (ErrBatchFailed).
					refused := errors.Is(err, graphpkg.ErrNoVersionValidAt)
					if !refused && res != nil {
						for _, e := range res.Errors {
							refused = refused || errors.Is(e.Err, graphpkg.ErrNoVersionValidAt)
						}
					}
					if err == nil || !refused {
						t.Fatalf("Execute: %v (%+v); want ErrNoVersionValidAt", err, res)
					}
					ghosts, err := g.Nodes().ByLabel("Ghost", storepkg.QueryOpts{})
					if err != nil {
						t.Fatal(err)
					}
					o, err := g.Nodes().Get(ctx, other)
					if err != nil {
						t.Fatal(err)
					}
					s, _ := o.GetProperty("s")
					if withTx {
						if len(ghosts) != 0 || s != "v" || targetHistory() != 0 || historyLen(t, g, other) != 0 {
							t.Fatalf("refused unit wrote: ghosts=%d other.s=%v target history=%d other history=%d; want nothing written",
								len(ghosts), s, targetHistory(), historyLen(t, g, other))
						}
						return
					}
					// Plain excise: documented per-op failure, the other ops kept.
					if len(ghosts) != 1 || s != "changed" || targetHistory() != 0 {
						t.Fatalf("plain excise: ghosts=%d other.s=%v target history=%d; want 1, changed, 0", len(ghosts), s, targetHistory())
					}
				})
			}
		}
	}
}
