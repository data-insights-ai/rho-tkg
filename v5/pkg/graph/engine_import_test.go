package graph

import (
	"context"
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/types"
)

func openSeeded(t testing.TB, seed uint64) (*Graph, Config) {
	t.Helper()
	c := engineConfig(t, t.TempDir())
	c.Allocation = Allocation{Seed: seed}
	g, err := Open(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close() })
	return g, c
}

func rowsView(t testing.TB, g *Graph, view View, nodes []types.NodeID, rels []types.RelID, valid temporal.Scope) []Entity {
	t.Helper()
	s, err := g.OpenSnapshot(context.Background(), Fresh())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	p, err := s.Lookup(context.Background(), LookupRequest{
		Selector:   Selector{View: view, Valid: valid, Known: knownAll(t)},
		Projection: Projection{Properties: []string{"n", "w"}}, Nodes: nodes, Rels: rels, Paging: PageLimits{Rows: 100, MaxBytes: 1 << 20}})
	if err != nil {
		t.Fatal(err)
	}
	return p.Rows
}

func sameScope(t testing.TB, got, want temporal.Scope) bool {
	t.Helper()
	d1, _ := got.Difference(want, temporal.Limits{})
	d2, _ := want.Difference(got, temporal.Limits{})
	return d1.Kind() == temporal.ScopeEmpty && d2.Kind() == temporal.ScopeEmpty
}

func noCommit(t testing.TB, g *Graph, name string, want error, fn func(Tx) error) {
	t.Helper()
	ctx := context.Background()
	before, _ := g.OpenSnapshot(ctx, Fresh())
	_, err := g.Update(ctx, fn)
	after, _ := g.OpenSnapshot(ctx, Fresh())
	if !errors.Is(err, want) {
		t.Errorf("%s: %v, want %v", name, err, want)
	}
	if string(before.Cut().Bytes()) != string(after.Cut().Bytes()) {
		t.Errorf("%s: a refused Update committed", name)
	}
	_ = before.Close()
	_ = after.Close()
}

func TestExplicitIDRefusesAboveSeedCollisionAndZero(t *testing.T) {
	g, cfg := openSeeded(t, 100)
	all := scopeAll(t)
	var got types.NodeID
	update(t, g, func(tx Tx) (err error) {
		got, err = tx.CreateNode(CreateNode{ID: 50, Props: map[string]Value{"n": Int(1)}, Scope: all})
		if err != nil {
			return
		}
		if _, err = tx.CreateNode(CreateNode{ID: 100, Scope: all}); err != nil { // boundary
			return
		}
		_, err = tx.CreateRel(CreateRel{ID: 70, Type: "R", From: 50, To: 100, Scope: all})
		return
	})
	if got != 50 {
		t.Fatalf("explicit id: %d", got)
	}
	if rows := rowsView(t, g, ViewDeclared, []types.NodeID{50}, nil, all); len(rows) != 1 {
		t.Fatalf("imported node not readable: %+v", rows)
	}
	noCommit(t, g, "above seed", ErrInvalidID, func(tx Tx) error {
		_, err := tx.CreateNode(CreateNode{ID: 101, Scope: all})
		return err
	})
	noCommit(t, g, "negative", ErrInvalidID, func(tx Tx) error {
		_, err := tx.CreateNode(CreateNode{ID: -3, Scope: all})
		return err
	})
	noCommit(t, g, "rel above seed", ErrInvalidID, func(tx Tx) error {
		_, err := tx.CreateRel(CreateRel{ID: 101, Type: "R", From: 50, To: 100, Scope: all})
		return err
	})
	noCommit(t, g, "committed collision", ErrAlreadyExists, func(tx Tx) error {
		_, err := tx.CreateNode(CreateNode{ID: 50, Scope: all})
		return err
	})
	noCommit(t, g, "node id taken by a rel", ErrAlreadyExists, func(tx Tx) error {
		_, err := tx.CreateNode(CreateNode{ID: 70, Scope: all})
		return err
	})
	noCommit(t, g, "rel id taken by a node", ErrAlreadyExists, func(tx Tx) error {
		_, err := tx.CreateRel(CreateRel{ID: 50, Type: "R", From: 50, To: 100, Scope: all})
		return err
	})
	noCommit(t, g, "duplicate in one Update", ErrAlreadyExists, func(tx Tx) error {
		if _, err := tx.CreateNode(CreateNode{ID: 60, Scope: all}); err != nil {
			return err
		}
		_, err := tx.CreateNode(CreateNode{ID: 60, Scope: all})
		return err
	})
	// Zero allocates: the first minted ID is Seed+1, untouched by explicit IDs.
	var minted types.NodeID
	update(t, g, func(tx Tx) (err error) {
		minted, err = tx.CreateNode(CreateNode{Scope: all})
		return
	})
	if minted != 101 {
		t.Fatalf("minted %d, want 101", minted)
	}
	// Scan without an index still finds imported entities in the reserved range.
	s, _ := g.OpenSnapshot(context.Background(), Fresh())
	pg, err := s.Scan(context.Background(), ScanRequest{Selector: goodSel(t), Paging: PageLimits{Rows: 100, MaxBytes: 1 << 20}})
	_ = s.Close()
	if err != nil || len(pg.Rows) != 4 {
		t.Fatalf("scan: %d rows, %v (want 4: nodes 50, 100, 101, rel 70)", len(pg.Rows), err)
	}
	// The collision survives a reopen.
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	g2, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer g2.Close()
	noCommit(t, g2, "collision after reopen", ErrAlreadyExists, func(tx Tx) error {
		_, err := tx.CreateNode(CreateNode{ID: 50, Scope: all})
		return err
	})
	// No seed: no explicit range at all.
	g0 := openMem(t)
	noCommit(t, g0, "seed 0", ErrInvalidID, func(tx Tx) error {
		_, err := tx.CreateNode(CreateNode{ID: 1, Scope: all})
		return err
	})
}

func TestReopenSecondLifeOverlapRefused(t *testing.T) {
	g, cfg := openSeeded(t, 0)
	all := scopeAll(t)
	var a, b types.NodeID
	update(t, g, func(tx Tx) (err error) {
		if a, err = tx.CreateNode(CreateNode{Props: map[string]Value{"n": Int(1)}, Scope: spanMS(t, 0, 1000)}); err != nil {
			return
		}
		b, err = tx.CreateNode(CreateNode{Scope: all})
		return
	})
	ra, _ := NodeRef(a)
	update(t, g, func(tx Tx) error { return tx.End(End{Target: ra, Scope: spanMS(t, 0, 100)}) })
	noCommit(t, g, "overlap with life 1", ErrInvalidScope, func(tx Tx) error {
		return tx.Reopen(Reopen{Target: ra, Scope: spanMS(t, 50, 150)})
	})
	ghost, _ := NodeRef(424242)
	noCommit(t, g, "unknown target", ErrNotFound, func(tx Tx) error {
		return tx.Reopen(Reopen{Target: ghost, Scope: spanMS(t, 200, 300)})
	})
	update(t, g, func(tx Tx) error {
		if err := tx.Reopen(Reopen{Target: ra, Scope: spanMS(t, 200, 300)}); err != nil {
			return err
		}
		// Ops after the Reopen in the same Update resolve to life 2.
		return tx.Set(Set{Target: ra, Key: "n", Value: Int(2), Scope: spanMS(t, 200, 300)})
	})
	noCommit(t, g, "second reopen overlapping life 2", ErrInvalidScope, func(tx Tx) error {
		return tx.Reopen(Reopen{Target: ra, Scope: spanMS(t, 250, 400)})
	})
	// A scope spanning the gap between two lives is refused.
	noCommit(t, g, "set spanning lives", ErrInvalidScope, func(tx Tx) error {
		return tx.Set(Set{Target: ra, Key: "n", Value: Int(3), Scope: spanMS(t, 90, 250)})
	})
	// A rel over both lives: declared whole, effective with the gap.
	var r types.RelID
	update(t, g, func(tx Tx) (err error) {
		r, err = tx.CreateRel(CreateRel{Type: "R", From: a, To: b, Props: map[string]Value{"w": Int(5)}, Scope: spanMS(t, 0, 300)})
		return
	})
	check := func(g *Graph) {
		t.Helper()
		na := rowsView(t, g, ViewDeclared, []types.NodeID{a}, nil, all)
		if len(na) != 2 || !sameScope(t, na[0].Valid, spanMS(t, 0, 100)) || !sameScope(t, na[1].Valid, spanMS(t, 200, 300)) ||
			na[0].Props["n"] != Int(1) || na[1].Props["n"] != Int(2) {
			t.Fatalf("node over two lives: %+v", na)
		}
		d := rowsView(t, g, ViewDeclared, nil, []types.RelID{r}, all)
		if len(d) != 1 || !sameScope(t, d[0].Valid, spanMS(t, 0, 300)) {
			t.Fatalf("declared rel: %+v", d)
		}
		e := rowsView(t, g, ViewEffective, nil, []types.RelID{r}, all)
		if len(e) != 2 || !sameScope(t, e[0].Valid, spanMS(t, 0, 100)) || !sameScope(t, e[1].Valid, spanMS(t, 200, 300)) {
			t.Fatalf("effective rel must show the gap: %+v", e)
		}
	}
	check(g)
	// After a reopen of the store; ops still find life 2; End is per life.
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	g2, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer g2.Close()
	check(g2)
	update(t, g2, func(tx Tx) error { return tx.End(End{Target: ra, Scope: spanMS(t, 0, 280)}) })
	update(t, g2, func(tx Tx) error {
		return tx.Correct(Correct{Target: ra, Set: map[string]Value{"n": Int(9)}, Scope: spanMS(t, 250, 270)})
	})
	noCommit(t, g2, "correct after life 2's End", ErrInvalidScope, func(tx Tx) error {
		return tx.Correct(Correct{Target: ra, Set: map[string]Value{"n": Int(9)}, Scope: spanMS(t, 285, 290)})
	})
}

func TestRelOutsideEndpointsDeclaredShownEffectiveMasked(t *testing.T) {
	g := openMem(t)
	var a, b types.NodeID
	var r types.RelID
	update(t, g, func(tx Tx) (err error) {
		if a, err = tx.CreateNode(CreateNode{Scope: spanMS(t, 0, 100)}); err != nil {
			return
		}
		if b, err = tx.CreateNode(CreateNode{Scope: scopeAll(t)}); err != nil {
			return
		}
		r, err = tx.CreateRel(CreateRel{Type: "R", From: a, To: b, Props: map[string]Value{"w": Int(1)}, Scope: spanMS(t, 0, 200)})
		return
	})
	all := scopeAll(t)
	d := rowsView(t, g, ViewDeclared, nil, []types.RelID{r}, all)
	if len(d) != 1 || !sameScope(t, d[0].Valid, spanMS(t, 0, 200)) {
		t.Fatalf("declared: %+v", d)
	}
	e := rowsView(t, g, ViewEffective, nil, []types.RelID{r}, all)
	if len(e) != 1 || !sameScope(t, e[0].Valid, spanMS(t, 0, 100)) {
		t.Fatalf("effective: %+v", e)
	}
	s, _ := g.OpenSnapshot(context.Background(), Fresh())
	defer s.Close()
	pg, err := s.Scan(context.Background(), ScanRequest{Selector: Selector{View: ViewEffective, Valid: spanMS(t, 150, 160), Known: knownAll(t), RelType: "R"}, Paging: PageLimits{Rows: 10, MaxBytes: 1 << 20}})
	if err != nil || len(pg.Rows) != 0 {
		t.Fatalf("effective scan outside the endpoint: %+v %v", pg.Rows, err)
	}
	noCommit(t, g, "missing endpoint", ErrNotFound, func(tx Tx) error {
		_, err := tx.CreateRel(CreateRel{Type: "R", From: a, To: 99999, Scope: all})
		return err
	})
}

func TestMetaRowRollsBackWithBatch(t *testing.T) {
	ctx := context.Background()
	c := engineConfig(t, t.TempDir())
	g, err := Open(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	all := scopeAll(t)
	meta := func(g *Graph, spec SnapshotSpec, key string) (string, bool) {
		t.Helper()
		s, err := g.OpenSnapshot(ctx, spec)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		v, ok, err := s.Meta(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
		return string(v), ok
	}
	if _, ok := meta(g, Fresh(), "k"); ok {
		t.Fatal("meta before any write")
	}
	update(t, g, func(tx Tx) error {
		if err := tx.SetMeta("k", []byte("v1")); err != nil {
			return err
		}
		_, err := tx.CreateNode(CreateNode{Scope: all})
		return err
	})
	if v, ok := meta(g, Fresh(), "k"); !ok || v != "v1" {
		t.Fatalf("v1: %q %v", v, ok)
	}
	s1, _ := g.OpenSnapshot(ctx, Fresh())
	cut1 := s1.Cut()
	_ = s1.Close()
	boom := errors.New("boom")
	if _, err := g.Update(ctx, func(tx Tx) error {
		_ = tx.SetMeta("k", []byte("v2"))
		return boom
	}); !errors.Is(err, boom) {
		t.Fatal(err)
	}
	ghost, _ := NodeRef(424242)
	noCommit(t, g, "stage failure after SetMeta", ErrNotFound, func(tx Tx) error {
		if err := tx.SetMeta("k", []byte("v2")); err != nil {
			return err
		}
		return tx.Set(Set{Target: ghost, Key: "n", Value: Int(1), Scope: all})
	})
	if v, _ := meta(g, Fresh(), "k"); v != "v1" {
		t.Fatalf("rolled-back meta visible: %q", v)
	}
	noCommit(t, g, "oversize value", ErrInvalidRequest, func(tx Tx) error {
		return tx.SetMeta("k", make([]byte, MaxMetaValueBytes+1))
	})
	noCommit(t, g, "empty key", ErrInvalidRequest, func(tx Tx) error { return tx.SetMeta("", []byte("x")) })
	// A meta-only Update commits; the last SetMeta of a key wins; empty is a value.
	update(t, g, func(tx Tx) error {
		_ = tx.SetMeta("k", []byte("v2"))
		_ = tx.SetMeta("k", []byte("v3"))
		return tx.SetMeta("e", nil)
	})
	if v, ok := meta(g, Fresh(), "k"); !ok || v != "v3" {
		t.Fatalf("v3: %q %v", v, ok)
	}
	if _, ok := meta(g, Fresh(), "e"); !ok {
		t.Fatal("empty value lost")
	}
	if v, _ := meta(g, At(cut1), "k"); v != "v1" {
		t.Fatalf("old cut sees %q, want v1", v)
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	g, err = Open(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	if v, ok := meta(g, Fresh(), "k"); !ok || v != "v3" {
		t.Fatalf("after reopen: %q %v", v, ok)
	}
}
