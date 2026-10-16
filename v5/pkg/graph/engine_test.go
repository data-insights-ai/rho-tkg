package graph

import (
	"context"
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/types"
)

func engineConfig(t testing.TB, dir string) Config {
	c := goodConfig(t)
	c.Dir = dir
	c.NodeProperties = []PropertySchema{{Key: "n", Kind: KindInt}, {Key: "name", Kind: KindString}}
	c.RelProperties = []PropertySchema{{Key: "w", Kind: KindInt}}
	return c
}

func knownAll(t testing.TB) temporal.Scope {
	s, err := temporal.All(binding2(t).Axis())
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func binding2(t testing.TB) types.DefaultAxisBinding { return bindingOn(t, types.GraphID{3}, 20) }

func lookupNode(t testing.TB, s *Snapshot, id types.NodeID, rows, maxBytes int) (Page, error) {
	return s.Lookup(context.Background(), LookupRequest{
		Selector:   Selector{View: ViewDeclared, Valid: scopeAll(t), Known: knownAll(t)},
		Projection: Projection{Properties: []string{"n", "name"}},
		Nodes:      []types.NodeID{id}, Paging: PageLimits{Rows: rows, MaxBytes: maxBytes},
	})
}

// TestCommitDurableAcrossReopen: commit, close, reopen the directory, read.
func TestCommitDurableAcrossReopen(t *testing.T) {
	ctx := context.Background()
	cfg := engineConfig(t, t.TempDir())
	g, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	var id types.NodeID
	rc, err := g.Update(ctx, func(tx Tx) error {
		var err error
		id, err = tx.CreateNode(CreateNode{Labels: []string{"P"}, Props: map[string]Value{"n": Int(7), "name": String("a")}, Scope: scopeAll(t)})
		return err
	})
	if err != nil || rc.IsZero() || id <= 0 {
		t.Fatalf("update: %v %v %v", id, rc, err)
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	g, err = Open(ctx, cfg)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer g.Close()
	s, err := g.OpenSnapshot(ctx, After(rc))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	p, err := lookupNode(t, s, id, 10, 1<<20)
	if err != nil || len(p.Rows) != 1 {
		t.Fatalf("lookup: %v %v", p, err)
	}
	r := p.Rows[0]
	if n, _ := r.Props["n"].AsInt(); n != 7 || len(r.Labels) != 1 || r.Labels[0] != "P" || r.Ref.Node() != id {
		t.Fatalf("row after reopen = %+v", r)
	}
	if s.Cut().IsZero() {
		t.Fatal("zero cut")
	}
}

// TestLookupPageLimits: ID ceiling, byte limit and foreign cursors.
func TestLookupPageLimits(t *testing.T) {
	ctx := context.Background()
	g, err := Open(ctx, engineConfig(t, t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	var ids []types.NodeID
	if _, err := g.Update(ctx, func(tx Tx) error {
		for i := 0; i < 3; i++ {
			id, err := tx.CreateNode(CreateNode{Props: map[string]Value{"n": Int(int64(i))}, Scope: scopeAll(t)})
			if err != nil {
				return err
			}
			ids = append(ids, id)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	s, err := g.OpenSnapshot(ctx, Fresh())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	sel := Selector{View: ViewDeclared, Valid: scopeAll(t), Known: knownAll(t)}
	many := make([]types.NodeID, MaxLookupIDs+1)
	for i := range many {
		many[i] = types.NodeID(i + 1)
	}
	if _, err := s.Lookup(ctx, LookupRequest{Selector: sel, Nodes: many, Paging: PageLimits{Rows: 10, MaxBytes: 1 << 20}}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("4097 IDs: %v", err)
	}
	if _, err := s.Lookup(ctx, LookupRequest{Selector: sel, Nodes: ids, Paging: PageLimits{Rows: 10}}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("MaxBytes 0: %v", err)
	}
	// Rows=1 pages the three nodes; every row exactly once, then Done.
	p, err := s.Lookup(ctx, LookupRequest{Selector: sel, Nodes: ids, Paging: PageLimits{Rows: 1, MaxBytes: 1 << 20}})
	if err != nil || len(p.Rows) != 1 || p.Next.IsZero() {
		t.Fatalf("first page: %v %v", p, err)
	}
	seen := map[types.NodeID]bool{p.Rows[0].Ref.Node(): true}
	first := p.Next
	for !p.Next.IsZero() {
		if p, err = s.Resume(ctx, p.Next, PageLimits{Rows: 1, MaxBytes: 1 << 20}); err != nil {
			t.Fatal(err)
		}
		for _, r := range p.Rows {
			if seen[r.Ref.Node()] {
				t.Fatalf("row %d twice", r.Ref.Node())
			}
			seen[r.Ref.Node()] = true
		}
	}
	if len(seen) != 3 {
		t.Fatalf("saw %v", seen)
	}
	// A cursor from another snapshot (other cut) is foreign.
	if _, err := g.Update(ctx, func(tx Tx) error {
		_, err := tx.CreateNode(CreateNode{Scope: scopeAll(t)})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	s2, err := g.OpenSnapshot(ctx, Fresh())
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if _, err := s2.Resume(ctx, first, PageLimits{Rows: 1, MaxBytes: 1 << 20}); !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("foreign cursor: %v", err)
	}
	// Unknown ID is absent from the page, not an error.
	if p, err := lookupNode(t, s, 999999, 5, 1<<20); err != nil || len(p.Rows) != 0 {
		t.Fatalf("unknown id: %v %v", p, err)
	}
}

// TestZeroGraphAndSnapshotAreClosed: the zero values are ErrClosed, Page has no Done.
func TestZeroGraphAndSnapshotAreClosed(t *testing.T) {
	var g Graph
	if _, err := g.Update(context.Background(), func(Tx) error { return nil }); !errors.Is(err, ErrClosed) {
		t.Fatalf("zero Graph Update: %v", err)
	}
	if _, err := g.OpenSnapshot(context.Background(), Fresh()); !errors.Is(err, ErrClosed) {
		t.Fatalf("zero Graph OpenSnapshot: %v", err)
	}
	if err := g.Close(); !errors.Is(err, ErrClosed) {
		t.Fatalf("zero Graph Close: %v", err)
	}
	var s Snapshot
	if _, err := s.Lookup(context.Background(), LookupRequest{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("zero Snapshot Lookup: %v", err)
	}
}
