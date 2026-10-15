package graph_test

// Backlog 73, final review before v4.56.0: a conversion cut short must not
// let an export or an untimed create through (item 1), and the endpoint
// constraint holds against the endpoint's whole current life (item 4).

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	temporalpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// cutCtx is a context that reports cancellation from its (after+1)-th Done
// call on: a conversion cancelled midway, after some rows were stamped.
type cutCtx struct {
	context.Context
	after int32
	calls atomic.Int32
	done  chan struct{}
}

func newCutCtx(after int32) *cutCtx {
	d := make(chan struct{})
	close(d)
	return &cutCtx{Context: context.Background(), after: after, done: d}
}

func (c *cutCtx) Done() <-chan struct{} {
	if c.calls.Add(1) > c.after {
		return c.done
	}
	return nil
}

func (c *cutCtx) Err() error {
	if c.calls.Load() > c.after {
		return context.Canceled
	}
	return nil
}

// TestUntimedConversionCutShortFailsClosed (final review item 1): a cancelled
// conversion leaves the store marked converting with part of its untimed
// first versions at -inf. Export and ExportSince then wrote a TxFrom header
// over the mixed rows (an empty importer adopted TxFrom with -inf rows), and
// untimed creates went on under TxFrom. Both must fail with
// ErrUntimedModeMismatch until the conversion finishes; then the export
// carries UntimedUnbounded (an empty TxFrom importer adopts it).
func TestUntimedConversionCutShortFailsClosed(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	g, err := graphpkg.New(graphpkg.Config{BadgerDir: t.TempDir(), ChangeLog: true})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	var ids []types.NodeID
	for i := 0; i < 8; i++ {
		n, err := g.Nodes().Add(ctx, []string{"U"}, map[string]any{"i": int64(i)})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, n.ID())
	}
	a, err := g.Nodes().Get(ctx, ids[0])
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.Nodes().Get(ctx, ids[1])
	if err != nil {
		t.Fatal(err)
	}
	cursor, err := g.IO().Watermark()
	if err != nil {
		t.Fatal(err)
	}

	if err := g.Admin().SetUntimedValidStart(newCutCtx(3), graphpkg.UntimedUnbounded); !errors.Is(err, context.Canceled) {
		t.Fatalf("cut-short conversion: err = %v, want context.Canceled", err)
	}
	var buf bytes.Buffer
	if err := g.IO().Export(&buf); !errors.Is(err, graphpkg.ErrUntimedModeMismatch) {
		t.Errorf("Export after a cut-short conversion: err = %v, want ErrUntimedModeMismatch", err)
	}
	if err := g.Tx().Run(func(tx *graphpkg.GraphTx) error { return tx.Export(&bytes.Buffer{}) }); !errors.Is(err, graphpkg.ErrUntimedModeMismatch) {
		t.Errorf("tx Export after a cut-short conversion: err = %v, want ErrUntimedModeMismatch", err)
	}
	if err := g.IO().ExportSince(&bytes.Buffer{}, cursor); !errors.Is(err, graphpkg.ErrUntimedModeMismatch) {
		t.Errorf("ExportSince after a cut-short conversion: err = %v, want ErrUntimedModeMismatch", err)
	}
	if _, err := g.Nodes().Add(ctx, []string{"U"}, nil); !errors.Is(err, graphpkg.ErrUntimedModeMismatch) {
		t.Errorf("untimed node create after a cut-short conversion: err = %v, want ErrUntimedModeMismatch", err)
	}
	if _, err := g.Rels().Add(ctx, "R", a, b, nil); !errors.Is(err, graphpkg.ErrUntimedModeMismatch) {
		t.Errorf("untimed rel create after a cut-short conversion: err = %v, want ErrUntimedModeMismatch", err)
	}
	bb, err := g.Batch().New()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bb.AddNode([]string{"U"}, nil); err == nil {
		if _, err := bb.Execute(); !errors.Is(err, graphpkg.ErrUntimedModeMismatch) {
			t.Errorf("batch untimed create after a cut-short conversion: err = %v, want ErrUntimedModeMismatch", err)
		}
	} else if !errors.Is(err, graphpkg.ErrUntimedModeMismatch) {
		t.Errorf("batch AddNode after a cut-short conversion: err = %v, want ErrUntimedModeMismatch", err)
	}
	// A create with a valid-from is not an untimed first version.
	if _, err := g.Nodes().Add(ctx, []string{"T"}, map[string]any{"tkg_valid_from": types.Instant(5_000)}); err != nil {
		t.Errorf("timed create after a cut-short conversion: %v", err)
	}

	if err := g.Admin().SetUntimedValidStart(ctx, graphpkg.UntimedUnbounded); err != nil {
		t.Fatalf("finish the conversion: %v", err)
	}
	buf.Reset()
	if err := g.IO().Export(&buf); err != nil {
		t.Fatalf("Export after the finished conversion: %v", err)
	}
	if err := g.IO().ExportSince(&bytes.Buffer{}, cursor); err != nil {
		t.Errorf("ExportSince after the finished conversion: %v", err)
	}
	if n, err := g.Nodes().Add(ctx, []string{"U"}, nil); err != nil || n.Temporal().ValidFrom != types.UnboundedValidFrom {
		t.Errorf("untimed create after the finished conversion: %v, err %v", n, err)
	}
	imp, err := graphpkg.New(graphpkg.Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer imp.Close()
	if err := imp.IO().Import(bytes.NewReader(buf.Bytes()), tkgio.ImportOptions{}); err != nil {
		t.Fatalf("import the finished export: %v", err)
	}
	if n, err := imp.Nodes().Add(ctx, []string{"U"}, nil); err != nil || n.Temporal().ValidFrom != types.UnboundedValidFrom {
		t.Errorf("the importer did not adopt UntimedUnbounded (export header uu=false): %v, err %v", n, err)
	}
	for _, id := range ids {
		if _, err := imp.Temporal().NodeAt(id, uvsFar); err != nil {
			t.Errorf("imported node %d at far: %v", id, err)
		}
	}
}

// TestEndpointConstraintCoversCurrentLife (final review item 4): the
// constraint compared a relationship's start with the endpoint's CURRENT row
// start, so after an update at tx1 a relationship starting at tx1-1 was
// refused although NodeAt(A, tx1-1) answers A. The bound is the start of the
// endpoint's current life (its first version since the last delete). Both
// modes. Break cases: a relationship before a timed first version, and one
// before a re-imported life (after the earlier life's start), are refused.
func TestEndpointConstraintCoversCurrentLife(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for _, mode := range []graphpkg.UntimedValidStart{graphpkg.UntimedFromTxFrom, graphpkg.UntimedUnbounded} {
		for _, b := range allStoreBackendsWith(func(c *graphpkg.Config) { c.UntimedValidStart = mode }) {
			t.Run(uvsModeName(mode)+"/"+b.name, func(t *testing.T) {
				t.Parallel()
				g := b.open(t)
				if err := g.Constraints().Add(temporalpkg.TemporalConstraint{Kind: temporalpkg.ConstraintRelWithinEndpoints}); err != nil {
					t.Fatal(err)
				}
				a, err := g.Nodes().Add(ctx, []string{"N"}, nil)
				if err != nil {
					t.Fatal(err)
				}
				bn, err := g.Nodes().Add(ctx, []string{"N"}, nil)
				if err != nil {
					t.Fatal(err)
				}
				a1, err := g.Nodes().Update(ctx, a.ID(), map[string]any{"v": int64(1)})
				if err != nil {
					t.Fatal(err)
				}
				tx1 := a1.Temporal().TxFrom
				if _, err := g.Temporal().NodeAt(a.ID(), tx1-1); err != nil {
					t.Fatalf("NodeAt(A, tx1-1): %v", err)
				}
				if _, err := g.Rels().Add(ctx, "R", a1, bn, map[string]any{"tkg_valid_from": tx1 - 1}); err != nil {
					t.Errorf("rel at tx1-1 from an endpoint updated at tx1: %v", err)
				}
				if _, err := g.Rels().Add(ctx, "R", bn, a1, map[string]any{"tkg_valid_from": tx1 - 1}); err != nil {
					t.Errorf("rel at tx1-1 to an endpoint updated at tx1: %v", err)
				}
				if mode == graphpkg.UntimedFromTxFrom {
					if _, err := g.Rels().Add(ctx, "R", a1, bn, map[string]any{"tkg_valid_from": a.Temporal().TxFrom - 1}); !errors.Is(err, temporalpkg.ErrRelBeforeStartNode) {
						t.Errorf("rel before the endpoint's life: err = %v, want ErrRelBeforeStartNode", err)
					}
				}

				// A timed first version bounds the life, also after an update
				// (early is an endpoint valid long before it).
				const t0 = types.Instant(1_000_000)
				early, err := g.Nodes().Add(ctx, []string{"N"}, map[string]any{"tkg_valid_from": uvsFar})
				if err != nil {
					t.Fatal(err)
				}
				timed, err := g.Nodes().Add(ctx, []string{"N"}, map[string]any{"tkg_valid_from": t0})
				if err != nil {
					t.Fatal(err)
				}
				timed, err = g.Nodes().Update(ctx, timed.ID(), map[string]any{"v": int64(1)})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := g.Rels().Add(ctx, "R", timed, early, map[string]any{"tkg_valid_from": t0}); err != nil {
					t.Errorf("rel at the timed first version's start: %v", err)
				}
				if _, err := g.Rels().Add(ctx, "R", timed, early, map[string]any{"tkg_valid_from": t0 - 1}); !errors.Is(err, temporalpkg.ErrRelBeforeStartNode) {
					t.Errorf("rel before a timed first version: err = %v, want ErrRelBeforeStartNode", err)
				}
				if _, err := g.Rels().Add(ctx, "R", early, timed, map[string]any{"tkg_valid_from": t0 - 1}); !errors.Is(err, temporalpkg.ErrRelBeforeEndNode) {
					t.Errorf("rel before a timed first version (end): err = %v, want ErrRelBeforeEndNode", err)
				}

				// A re-imported ID's life starts at its re-import: the earlier
				// life does not cover a relationship.
				if err := g.Nodes().Delete(ctx, a.ID()); err != nil {
					t.Fatal(err)
				}
				re, err := g.Nodes().Import(ctx, a.ID(), []string{"N"}, nil)
				if err != nil {
					t.Fatal(err)
				}
				re, err = g.Nodes().Update(ctx, re.ID(), map[string]any{"v": int64(2)})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := g.Rels().Add(ctx, "R", re, bn, map[string]any{"tkg_valid_from": tx1}); !errors.Is(err, temporalpkg.ErrRelBeforeStartNode) {
					t.Errorf("rel in the earlier life of a re-imported endpoint: err = %v, want ErrRelBeforeStartNode", err)
				}
			})
		}
	}
}
