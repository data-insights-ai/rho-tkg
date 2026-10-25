package core

import (
	"context"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestShadowIDResolves pins TKGID (sigma, 24 Oct): the shadow key tkg_id
// resolves to the entity's own ID on a node and a relationship, through the
// graph's resolver and inside a transaction, and stays reserved: a write of it
// is refused.
func TestShadowIDResolves(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)
	ctx := context.Background()
	a, err := g.Nodes.Add(ctx, []string{"Person"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.Nodes.Add(ctx, []string{"Person"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	r, err := g.Rels.Add(ctx, "KNOWS", a, b, nil)
	if err != nil {
		t.Fatal(err)
	}
	if v, ok := g.Resolve.NodeProperty(a, types.ShadowID); !ok || v != a.ID() {
		t.Errorf("NodeProperty(tkg_id) = (%v, %v), want (%v, true)", v, ok, a.ID())
	}
	if v, ok := g.Resolve.RelProperty(r, types.ShadowID); !ok || v != r.ID() {
		t.Errorf("RelProperty(tkg_id) = (%v, %v), want (%v, true)", v, ok, r.ID())
	}
	tx, err := g.BeginTx()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if v, ok := tx.NodeProperty(b, types.ShadowID); !ok || v != b.ID() {
		t.Errorf("tx.NodeProperty(tkg_id) = (%v, %v), want (%v, true)", v, ok, b.ID())
	}
	if v, ok := tx.RelProperty(r, types.ShadowID); !ok || v != r.ID() {
		t.Errorf("tx.RelProperty(tkg_id) = (%v, %v), want (%v, true)", v, ok, r.ID())
	}
	_ = tx.Rollback()
	if _, err := g.Nodes.Add(ctx, []string{"Person"}, map[string]any{types.ShadowID: int64(7)}); err == nil {
		t.Error("a node written with tkg_id was accepted")
	}
}
