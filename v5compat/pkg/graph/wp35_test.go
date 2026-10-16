package graph

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	v5 "github.com/data-insights-ai/rho-tkg/v5/pkg/graph"
	v5t "github.com/data-insights-ai/rho-tkg/v5/pkg/types"
	"github.com/data-insights-ai/rho-tkg/v5compat/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v5compat/pkg/types"
)

func openT(t *testing.T, dir string) *Graph {
	t.Helper()
	g, err := New(Config{BadgerDir: dir, AllowTxBackfill: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close() })
	return g
}

// rawProps are the stored v5 property names of a node.
func rawProps(t *testing.T, g *Graph, id types.NodeID) map[string]bool {
	t.Helper()
	ctx := context.Background()
	s, err := g.g.OpenSnapshot(ctx, v5.Fresh())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	p, err := s.Lookup(ctx, v5.LookupRequest{Selector: g.selector(g.ax.known(), v5.ViewDeclared, fullSpan(), nil, nil, ""),
		Projection: v5.Projection{AllProperties: true}, Paging: paging, Nodes: []v5t.NodeID{v5t.NodeID(id)}})
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]bool{}
	for _, e := range p.Rows {
		for k := range e.Props {
			out[k] = true
		}
	}
	return out
}

// lastRevision is the newest v5 commit that touched the node.
func lastRevision(t *testing.T, g *Graph, id types.NodeID) uint64 {
	t.Helper()
	ctx := context.Background()
	s, err := g.g.OpenSnapshot(ctx, v5.Fresh())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ref, _ := g.ref(int64(id), false)
	p, err := s.Timeline(ctx, v5.TimelineRequest{Ref: ref, Selector: g.selector(g.ax.known(), v5.ViewDeclared, fullSpan(), nil, nil, ""),
		Projection: v5.Projection{Revisions: true}, Paging: paging})
	es, err := drain(ctx, s, p, err)
	if err != nil || len(es) == 0 {
		t.Fatalf("timeline of %d: %d rows, %v", id, len(es), err)
	}
	return es[len(es)-1].Revision
}

func TestOneCommitPerTx(t *testing.T) {
	ctx := context.Background()
	g := openT(t, "")
	a, _ := g.Nodes().AddWithTx(ctx, []string{"A"}, map[string]any{"n": 1}, 10)
	b, _ := g.Nodes().AddWithTx(ctx, []string{"A"}, map[string]any{"n": 1}, 20)
	if err := g.Tx().Run(func(tx *GraphTx) error {
		if _, err := tx.UpdateNodeWithTx(a.ID(), map[string]any{"n": 2}, 100); err != nil {
			return err
		}
		_, err := tx.UpdateNodeWithTx(b.ID(), map[string]any{"n": 2}, 200)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if ra, rb := lastRevision(t, g, a.ID()), lastRevision(t, g, b.ID()); ra != rb {
		t.Fatalf("two stamps in one Tx took two commits: revisions %d and %d", ra, rb)
	}
}

func TestBackfillPinReads(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	g := openT(t, dir)
	n, err := g.Nodes().AddWithTx(ctx, []string{"A"}, map[string]any{"v": 1}, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.Nodes().UpdateWithTx(ctx, n.ID(), map[string]any{"v": 2}, 2000); err != nil {
		t.Fatal(err)
	}
	if tf, _, _ := n.TxStamps(); tf != 1000 {
		t.Fatalf("created TxFrom = %d, want 1000", tf)
	}
	for _, c := range []struct {
		pin  types.Instant
		want any
	}{{1000, int64(1)}, {1999, int64(1)}, {2000, int64(2)}, {5000, int64(2)}} {
		got, err := g.Temporal().NodeAsOf(n.ID(), c.pin)
		if err != nil || got.PropertiesMap()["v"] != c.want {
			t.Fatalf("NodeAsOf(%d) = %v, %v; want v=%v", c.pin, got, err, c.want)
		}
	}
	if _, err := g.Temporal().NodeAsOf(n.ID(), 999); !errors.Is(err, ErrNoVersionAsOf) {
		t.Fatalf("NodeAsOf(999) = %v, want ErrNoVersionAsOf", err)
	}
	h, err := g.Nodes().History(n.ID())
	if err != nil || len(h) != 1 {
		t.Fatalf("history = %d rows, %v", len(h), err)
	}
	if tf, tt, _ := h[0].TxStamps(); tf != 1000 || tt != 2000 {
		t.Fatalf("history row stamps = [%d,%d), want [1000,2000)", tf, tt)
	}
	raw := rawProps(t, g, n.ID())
	for _, k := range []string{"~tf", "~dt"} {
		if raw[k] {
			t.Fatalf("stored property %s still exists", k)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "v5compat.ledger")); err == nil {
		t.Fatal("side ledger file still written")
	}
	// A caller stamp ahead of the wall clock is refused: no skew allowance.
	if _, err := g.Nodes().AddWithTx(ctx, []string{"A"}, nil, types.Instant(nowMs()+60_000)); !errors.Is(err, ErrInvalidTxFrom) {
		t.Fatalf("future stamp = %v, want ErrInvalidTxFrom", err)
	}
	// The pin survives a reopen with no ledger.
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	g = openT(t, dir)
	got, err := g.Temporal().NodeAsOf(n.ID(), 1999)
	if err != nil || got.PropertiesMap()["v"] != int64(1) {
		t.Fatalf("after reopen NodeAsOf(1999) = %v, %v", got, err)
	}
}

func TestTxBeginCommit(t *testing.T) {
	ctx := context.Background()
	g := openT(t, "")
	before, _ := g.Temporal().CommittedTx()
	tx, err := g.Tx().Begin()
	if err != nil {
		t.Fatal(err)
	}
	n, err := tx.AddNode([]string{"B"}, map[string]any{"k": "v"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.GetNode(n.ID()); err != nil {
		t.Fatalf("a Tx does not see its own write: %v", err)
	}
	if _, err := g.Nodes().Get(ctx, n.ID()); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("uncommitted node visible outside: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := g.Nodes().Get(ctx, n.ID()); err != nil {
		t.Fatalf("committed node not visible: %v", err)
	}
	after, _ := g.Temporal().CommittedTx()
	if after <= before || tx.CommittedLSN() != uint64(after) {
		t.Fatalf("CommittedTx %d -> %d, CommittedLSN %d", before, after, tx.CommittedLSN())
	}
	if err := tx.Commit(); !errors.Is(err, store.ErrTxDone) {
		t.Fatalf("second Commit = %v, want ErrTxDone", err)
	}
	if err := tx.Rollback(); !errors.Is(err, store.ErrTxDone) {
		t.Fatalf("Rollback after Commit = %v, want ErrTxDone", err)
	}
	if _, err := tx.AddNode([]string{"B"}, nil); !errors.Is(err, store.ErrTxDone) {
		t.Fatalf("write after Commit = %v, want ErrTxDone", err)
	}
}

func TestTxBeginRollback(t *testing.T) {
	ctx := context.Background()
	g := openT(t, "")
	keep, _ := g.Nodes().Add(ctx, []string{"B"}, map[string]any{"k": 1})
	before, _ := g.Temporal().CommittedTx()
	tx, err := g.Tx().Begin()
	if err != nil {
		t.Fatal(err)
	}
	n, _ := tx.AddNode([]string{"B"}, nil)
	if _, err := tx.UpdateNode(keep.ID(), map[string]any{"k": 2}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if _, err := g.Nodes().Get(ctx, n.ID()); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("rolled-back node visible: %v", err)
	}
	got, _ := g.Nodes().Get(ctx, keep.ID())
	if got.PropertiesMap()["k"] != int64(1) {
		t.Fatalf("rolled-back update visible: %v", got.PropertiesMap())
	}
	if after, _ := g.Temporal().CommittedTx(); after != before {
		t.Fatalf("rollback moved CommittedTx %d -> %d", before, after)
	}
	if err := tx.Commit(); !errors.Is(err, store.ErrTxDone) {
		t.Fatalf("Commit after Rollback = %v, want ErrTxDone", err)
	}
	// The write lock is released: a plain write goes through.
	if _, err := g.Nodes().Add(ctx, []string{"B"}, nil); err != nil {
		t.Fatalf("write after Rollback: %v", err)
	}
}

func TestCountsEqualLookup(t *testing.T) {
	ctx := context.Background()
	g := openT(t, "")
	st := g.Stats()
	if n, err := st.NodeCount(); err != nil || n != 0 {
		t.Fatalf("empty NodeCount = %d, %v", n, err)
	}
	var ids []types.NodeID
	var nodesAdded []*types.Node
	for i := 0; i < 5; i++ {
		lab := "A"
		if i >= 3 {
			lab = "B"
		}
		n, err := g.Nodes().Add(ctx, []string{lab}, map[string]any{"i": i})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, n.ID())
		nodesAdded = append(nodesAdded, n)
	}
	r1, _ := g.Rels().Add(ctx, "R", nodesAdded[0], nodesAdded[1], nil)
	if _, err := g.Rels().Add(ctx, "S", nodesAdded[1], nodesAdded[2], nil); err != nil {
		t.Fatal(err)
	}
	_ = g.Nodes().Delete(ctx, ids[0])
	_ = g.Nodes().Retract(ctx, ids[3])
	_ = g.Rels().Delete(ctx, r1.ID())
	want := func(label string) int {
		ns, err := g.Nodes().ByLabel(label, store.QueryOpts{})
		if err != nil {
			t.Fatal(err)
		}
		return len(ns)
	}
	if got, err := st.NodeCountByLabel("A"); err != nil || got != want("A") || got != 2 {
		t.Fatalf("NodeCountByLabel(A) = %d, %v; lookup %d", got, err, want("A"))
	}
	if got, err := st.NodeCountByLabel("B"); err != nil || got != want("B") || got != 1 {
		t.Fatalf("NodeCountByLabel(B) = %d, %v; lookup %d", got, err, want("B"))
	}
	if got, err := st.NodeCount(); err != nil || got != want("A")+want("B") {
		t.Fatalf("NodeCount = %d, %v; lookup %d", got, err, want("A")+want("B"))
	}
	rs, _ := g.Rels().ByType("S", store.QueryOpts{})
	rr, _ := g.Rels().ByType("R", store.QueryOpts{})
	if got, err := st.RelCount(); err != nil || got != len(rs)+len(rr) || got != 1 {
		t.Fatalf("RelCount = %d, %v; lookup %d", got, err, len(rs)+len(rr))
	}
	if got, err := st.RelCountByType("R"); err != nil || got != 0 {
		t.Fatalf("RelCountByType(R) = %d, %v; the only R is deleted", got, err)
	}
}

func TestCreateUnique(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	g := openT(t, dir)
	if _, err := g.Nodes().Add(ctx, []string{"U"}, map[string]any{"k": "dup"}); err != nil {
		t.Fatal(err)
	}
	if _, err := g.Nodes().Add(ctx, []string{"U"}, map[string]any{"k": "dup"}); err != nil {
		t.Fatal(err)
	}
	if err := g.Constraints().CreateUnique(ctx, "U", "k"); !errors.Is(err, store.ErrUniqueViolationExisting) {
		t.Fatalf("CreateUnique over duplicates = %v, want ErrUniqueViolationExisting", err)
	}
	g2 := openT(t, "")
	if err := g2.Constraints().CreateUnique(ctx, "U", "k"); err != nil {
		t.Fatal(err)
	}
	if err := g2.Constraints().CreateUnique(ctx, "U", "k"); !errors.Is(err, store.ErrUniqueConstraintExists) {
		t.Fatalf("second CreateUnique = %v, want ErrUniqueConstraintExists", err)
	}
	if _, err := g2.Nodes().Add(ctx, []string{"U"}, map[string]any{"k": "x"}); err != nil {
		t.Fatal(err)
	}
	if _, err := g2.Nodes().Add(ctx, []string{"U"}, map[string]any{"k": "x"}); !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("duplicate claim = %v, want ErrUniqueViolation", err)
	}
	if n, _ := g2.Stats().NodeCountByLabel("U"); n != 1 {
		t.Fatalf("a refused write left %d nodes", n)
	}
	if _, err := g2.Nodes().Add(ctx, []string{"U"}, map[string]any{"k": int64(7)}); err != nil {
		t.Fatal(err)
	}
	if _, err := g2.Nodes().Add(ctx, []string{"U"}, map[string]any{"k": int64(7)}); !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("duplicate int claim = %v, want ErrUniqueViolation", err)
	}
	if n, created, err := g2.Nodes().GetOrCreateByKey(ctx, "U", "k", "x", nil); err != nil || created || n == nil {
		t.Fatalf("GetOrCreateByKey on a unique key: created=%v err=%v", created, err)
	}
	// Persistent: the declaration survives a reopen.
	g3 := openT(t, t.TempDir())
	if err := g3.Constraints().CreateUnique(ctx, "U", "k"); err != nil {
		t.Fatal(err)
	}
	if _, err := g3.Nodes().Add(ctx, []string{"U"}, map[string]any{"k": "y"}); err != nil {
		t.Fatal(err)
	}
	dir3 := g3.cfg.BadgerDir
	if err := g3.Close(); err != nil {
		t.Fatal(err)
	}
	g3 = openT(t, dir3)
	if _, err := g3.Nodes().Add(ctx, []string{"U"}, map[string]any{"k": "y"}); !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("after reopen duplicate = %v, want ErrUniqueViolation", err)
	}
}
