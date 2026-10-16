package graph

import (
	"context"
	"errors"
	"runtime"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/types"
)

func engOf(t testing.TB, g *Graph) *pebbleEngine {
	t.Helper()
	e, ok := g.eng.(*pebbleEngine)
	if !ok {
		t.Fatal("not the pebble engine")
	}
	return e
}

func instOf(t testing.TB, r Receipt) types.Instant {
	t.Helper()
	i, ok := r.KnowledgeInstant()
	if !ok {
		t.Fatal("receipt carries no knowledge instant")
	}
	return i
}

// knownUpTo is a Known scope whose upper bound is at (inclusive or not).
func knownUpTo(t testing.TB, at types.Instant, inclusive bool) temporal.Scope {
	t.Helper()
	ka := knowledgeAxis(t)
	p, err := types.InstantPosition(ka, at, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	hi, err := temporal.FiniteBound(p, inclusive)
	if err != nil {
		t.Fatal(err)
	}
	s, err := temporal.Span(ka, temporal.NegativeInfinity(), hi, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func knownPoint(t testing.TB, at types.Instant) temporal.Scope {
	t.Helper()
	p, err := types.InstantPosition(knowledgeAxis(t), at, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	s, err := temporal.Point(p)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// nOf reads property n of id under a Known scope: -1 when the node has no row.
func nOf(t testing.TB, g *Graph, id types.NodeID, known temporal.Scope) int64 {
	t.Helper()
	s, err := g.OpenSnapshot(context.Background(), Fresh())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	p, err := s.Lookup(context.Background(), LookupRequest{Selector: Selector{View: ViewDeclared, Valid: scopeAll(t), Known: known},
		Projection: Projection{Properties: []string{"n"}}, Nodes: []types.NodeID{id}, Paging: PageLimits{Rows: 5, MaxBytes: 1 << 20}})
	if err != nil {
		t.Fatalf("lookup under %v: %v", known.Kind(), err)
	}
	if len(p.Rows) == 0 {
		return -1
	}
	n, _ := p.Rows[0].Props["n"].AsInt()
	return n
}

// threeCommits: n=1 (commit instant t1), n=2 (t2), n=3 with Recorded = t2 (commit t3 > t2).
func threeCommits(t *testing.T) (g *Graph, id types.NodeID, t1, t2, t3 types.Instant) {
	g = memBare(t, func(c *Config) { c.NodeProperties = []PropertySchema{{Key: "n", Kind: KindInt}} })
	all := scopeAll(t)
	r1 := update(t, g, func(tx Tx) (err error) {
		id, err = tx.CreateNode(CreateNode{Labels: []string{"P"}, Props: map[string]Value{"n": Int(1)}, Scope: all})
		return
	})
	ref, _ := NodeRef(id)
	r2 := update(t, g, func(tx Tx) error { return tx.Set(Set{Target: ref, Key: "n", Value: Int(2), Scope: all}) })
	t1, t2 = instOf(t, r1), instOf(t, r2)
	r3 := update(t, g, func(tx Tx) error {
		return tx.Set(Set{Target: ref, Key: "n", Value: Int(3), Scope: all, Recorded: recAt(t, knowledgeAxis(t), int64(t2))})
	})
	return g, id, t1, t2, instOf(t, r3)
}

func TestKnownBoundExcludesLaterEvidence(t *testing.T) {
	g, id, t1, t2, t3 := threeCommits(t)
	if !(t1 < t2 && t2 < t3) {
		t.Fatalf("instants not increasing: %d %d %d", t1, t2, t3)
	}
	for _, c := range []struct {
		name  string
		known temporal.Scope
		want  int64
	}{
		{"All", knownAll(t), 3},
		{"at t3", knownUpTo(t, t3, true), 3},
		{"exactly at t2: the commit-2 fact and the evidence recorded at t2", knownUpTo(t, t2, true), 3},
		{"Point t2", knownPoint(t, t2), 3},
		{"exclusive t2: neither", knownUpTo(t, t2, false), 1},
		{"just before t2", knownUpTo(t, t2-1, true), 1},
		{"exactly at t1", knownUpTo(t, t1, true), 1},
		{"Point t1", knownPoint(t, t1), 1},
		{"before the first commit", knownUpTo(t, t1-1, true), -1},
		{"after everything", knownUpTo(t, t3+1000, true), 3},
	} {
		if got := nOf(t, g, id, c.known); got != c.want {
			t.Errorf("%s: n = %d, want %d", c.name, got, c.want)
		}
	}
	// Timeline filters the revisions the same way.
	s, _ := g.OpenSnapshot(context.Background(), Fresh())
	defer s.Close()
	ref, _ := NodeRef(id)
	p, err := s.Timeline(context.Background(), TimelineRequest{Ref: ref, Selector: Selector{View: ViewDeclared, Valid: scopeAll(t), Known: knownUpTo(t, t1, true)},
		Projection: Projection{Properties: []string{"n"}}, Paging: PageLimits{Rows: 10, MaxBytes: 1 << 20}})
	if err != nil || len(p.Rows) != 1 {
		t.Fatalf("timeline known<=t1: %v %v", p.Rows, err)
	}
}

func TestCommitKnowledgeInstantMonotonic(t *testing.T) {
	dir := t.TempDir()
	cfg := bareConfig(t, dir)
	ctx := context.Background()
	g, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	e := engOf(t, g)
	const base = int64(5_000_000_000_000)
	now := base
	e.now = func() int64 { return now }
	next := func() types.Instant {
		r := update(t, g, func(tx Tx) error { _, err := tx.CreateNode(CreateNode{Scope: scopeAll(t)}); return err })
		return instOf(t, r)
	}
	a, b := next(), next() // same ms tick
	if a != types.Instant(base) || b != a+1 {
		t.Fatalf("same tick: %d %d", a, b)
	}
	now = base - 10_000 // clock steps back
	if c := next(); c != b+1 {
		t.Fatalf("clock back: %d after %d", c, b)
	}
	now = base + 100
	d := next()
	if d != types.Instant(base+100) {
		t.Fatalf("clock forward: %d", d)
	}
	// An empty Update commits nothing and reports the latest instant.
	r := update(t, g, func(Tx) error { return nil })
	if i := instOf(t, r); i != d {
		t.Fatalf("empty update instant %d, want %d", i, d)
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	g, err = Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	e = engOf(t, g)
	e.now = func() int64 { return 0 } // restarted on a clock far in the past
	if x := next(); x != d+1 {
		t.Fatalf("after reopen: %d, want %d", x, d+1)
	}
}

func TestAtKnownSnapshotPinsByKnowledge(t *testing.T) {
	g, id, t1, t2, t3 := threeCommits(t)
	ctx := context.Background()
	read := func(at types.Instant) (int64, Cut) {
		s, err := g.OpenSnapshot(ctx, AtKnown(at))
		if err != nil {
			t.Fatalf("AtKnown(%d): %v", at, err)
		}
		defer s.Close()
		return nOf2(t, s, id), s.Cut()
	}
	for _, c := range []struct {
		at   types.Instant
		want int64
	}{{t3 + 5000, 3}, {t3, 3}, {t3 - 1, 2}, {t2, 2}, {t2 - 1, 1}, {t1, 1}, {t1 - 1, -1}, {0, -1}} {
		if got, _ := read(c.at); got != c.want {
			t.Errorf("AtKnown(%d): n = %d, want %d", c.at, got, c.want)
		}
	}
	_, cut := read(t2)
	s, err := g.OpenSnapshot(ctx, At(cut))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if nOf2(t, s, id) != 2 {
		t.Fatal("the cut of an AtKnown snapshot does not reopen")
	}
}

func nOf2(t testing.TB, s *Snapshot, id types.NodeID) int64 {
	t.Helper()
	p, err := s.Lookup(context.Background(), LookupRequest{Selector: goodSel(t), Projection: Projection{Properties: []string{"n"}}, Nodes: []types.NodeID{id}, Paging: PageLimits{Rows: 5, MaxBytes: 1 << 20}})
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Rows) == 0 {
		return -1
	}
	n, _ := p.Rows[0].Props["n"].AsInt()
	return n
}

func TestKnownBelowViewBypassesIndexPages(t *testing.T) {
	g := memBare(t, func(c *Config) { c.NodeProperties = []PropertySchema{{Key: "n", Kind: KindInt}} })
	all := scopeAll(t)
	var a, b types.NodeID
	r1 := update(t, g, func(tx Tx) (err error) {
		a, err = tx.CreateNode(CreateNode{Labels: []string{"P"}, Props: map[string]Value{"n": Int(1)}, Scope: all})
		return
	})
	bref := Ref{}
	update(t, g, func(tx Tx) (err error) {
		if b, err = tx.CreateNode(CreateNode{Labels: []string{"P"}, Props: map[string]Value{"n": Int(1)}, Scope: all}); err != nil {
			return
		}
		bref, _ = NodeRef(b)
		return
	})
	update(t, g, func(tx Tx) error { return tx.AddLabel(AddLabel{Target: a, Label: "Z", Scope: all}) })
	_ = bref
	e := engOf(t, g)
	scan := func(known temporal.Scope, label string) []types.NodeID {
		s, err := g.OpenSnapshot(context.Background(), Fresh())
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		p, err := s.Scan(context.Background(), ScanRequest{Selector: Selector{View: ViewDeclared, Valid: all, Known: known, Labels: []string{label}}, Paging: PageLimits{Rows: 50, MaxBytes: 1 << 20}})
		if err != nil {
			t.Fatal(err)
		}
		var ids []types.NodeID
		for _, r := range p.Rows {
			ids = append(ids, r.Ref.Node())
		}
		return ids
	}
	before := e.scanStats()
	if got := scan(knownUpTo(t, instOf(t, r1), true), "P"); len(got) != 1 || got[0] != a {
		t.Fatalf("P known at commit 1: %v", got)
	}
	if got := scan(knownUpTo(t, instOf(t, r1), true), "Z"); len(got) != 0 {
		t.Fatalf("Z (added at commit 3) known at commit 1: %v", got)
	}
	if d := e.scanStats().minus(before); d.candidates != 0 {
		t.Fatalf("a Known bound below the view read %d index candidates", d.candidates)
	}
	before = e.scanStats()
	if got := scan(knownAll(t), "P"); len(got) != 2 {
		t.Fatalf("P known All: %v", got)
	}
	if got := scan(knownUpTo(t, 1<<60, true), "P"); len(got) != 2 {
		t.Fatalf("P known far future: %v", got)
	}
	if d := e.scanStats().minus(before); d.candidates == 0 {
		t.Fatal("a Known bound at or above the view must keep using index pages")
	}
}

func TestTimelinePagesStreamWithoutMaterialising(t *testing.T) {
	n := 10000
	if testing.Short() {
		n = 2000
	}
	g := memBare(t, func(c *Config) { c.NodeProperties = []PropertySchema{{Key: "n", Kind: KindInt}} })
	all := scopeAll(t)
	var id types.NodeID
	update(t, g, func(tx Tx) (err error) {
		id, err = tx.CreateNode(CreateNode{Props: map[string]Value{"n": Int(0)}, Scope: all})
		return
	})
	ref, _ := NodeRef(id)
	grow := func(to int) {
		for i := 1; i <= to; i++ {
			update(t, g, func(tx Tx) error { return tx.Set(Set{Target: ref, Key: "n", Value: Int(int64(i)), Scope: all}) })
		}
	}
	ctx := context.Background()
	firstPage := func() (uint64, Page, *Snapshot) {
		s, err := g.OpenSnapshot(ctx, Fresh())
		if err != nil {
			t.Fatal(err)
		}
		var m0, m1 runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&m0)
		p, err := s.Timeline(ctx, TimelineRequest{Ref: ref, Selector: goodSel(t), Projection: Projection{Properties: []string{"n"}, Revisions: true}, Paging: PageLimits{Rows: 100, MaxBytes: 1 << 20}})
		runtime.ReadMemStats(&m1)
		if err != nil || len(p.Rows) != 100 || p.Next.IsZero() {
			t.Fatalf("first page: %d rows %v", len(p.Rows), err)
		}
		return m1.TotalAlloc - m0.TotalAlloc, p, s
	}
	grow(300)
	small, _, s0 := firstPage()
	s0.Close()
	grow(n - 300)
	big, p, s := firstPage()
	defer s.Close()
	if big > 3*small {
		t.Fatalf("first page allocates %d bytes at %d revisions vs %d at 300: not independent of the history length", big, n, small)
	}
	// Walk the rest: every revision once, oldest first.
	total, last := len(p.Rows), p.Rows[99].Revision
	for !p.Next.IsZero() {
		var err error
		if p, err = s.Resume(ctx, p.Next, PageLimits{Rows: 100, MaxBytes: 1 << 20}); err != nil {
			t.Fatal(err)
		}
		for _, r := range p.Rows {
			if r.Revision <= last {
				t.Fatalf("revision order: %d after %d", r.Revision, last)
			}
			last = r.Revision
		}
		total += len(p.Rows)
	}
	if total != n+1 {
		t.Fatalf("timeline rows = %d, want %d", total, n+1)
	}
	_ = errors.Is
}
