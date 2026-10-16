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

func TestTimelinePagesStreamWithoutMaterialising(t *testing.T) {
	n := 3000
	if testing.Short() {
		n = 1000
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

// A Known-bound scan over N entities shares one reader per index (rotated every
// readsPerView entities) instead of opening a view per entity.
func TestKnownScanSharesReaders(t *testing.T) {
	g := memBare(t, func(c *Config) { c.NodeProperties = []PropertySchema{{Key: "n", Kind: KindInt}} })
	all := scopeAll(t)
	const n, batch = 300, 50
	var ids []types.NodeID
	var r1 Receipt
	for len(ids) < n {
		r1 = update(t, g, func(tx Tx) error {
			for i := 0; i < batch; i++ {
				id, err := tx.CreateNode(CreateNode{Labels: []string{"P"}, Props: map[string]Value{"n": Int(1)}, Scope: all})
				if err != nil {
					return err
				}
				ids = append(ids, id)
			}
			return nil
		})
	}
	ref, _ := NodeRef(ids[0])
	update(t, g, func(tx Tx) error { return tx.Set(Set{Target: ref, Key: "n", Value: Int(2), Scope: all}) })
	e := engOf(t, g)
	s, err := g.OpenSnapshot(context.Background(), Fresh())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	before := e.readers.Load()
	total := 0
	req := ScanRequest{Selector: Selector{View: ViewDeclared, Valid: all, Known: knownUpTo(t, instOf(t, r1), true), Labels: []string{"P"}},
		Projection: Projection{Properties: []string{"n"}}, Paging: PageLimits{Rows: 100, MaxBytes: 1 << 20}}
	p, err := s.Scan(context.Background(), req)
	for err == nil {
		for _, r := range p.Rows {
			if v, _ := r.Props["n"].AsInt(); v != 1 {
				t.Fatalf("row sees n=%d beyond the bound", v)
			}
		}
		total += len(p.Rows)
		if p.Next.IsZero() {
			break
		}
		p, err = s.Resume(context.Background(), p.Next, req.Paging)
	}
	if err != nil || total != n {
		t.Fatalf("scan: %d rows, %v", total, err)
	}
	if opened := e.readers.Load() - before; opened > 30 {
		t.Fatalf("a Known scan over %d entities opened %d readers", n, opened)
	}
}

// A missing commit-instant row at or after the first commit is a corrupt store,
// not "before the bound".
func TestAtKnownMissingInstantRowIsCorruption(t *testing.T) {
	g, _, _, _, _ := threeCommits(t)
	e := engOf(t, g)
	if _, err := g.OpenSnapshot(context.Background(), AtKnown(0)); err != nil {
		t.Fatalf("before the first commit: %v", err)
	}
	e.first = 1 // the store claims commits from index 1, but index 1 has no row
	if _, err := g.OpenSnapshot(context.Background(), AtKnown(0)); !errors.Is(err, ErrCommitNotDurable) {
		t.Fatalf("missing row: %v", err)
	}
}

func validPointMS(t testing.TB, ms int64) temporal.Scope {
	t.Helper()
	p, err := types.InstantPosition(axis(t), types.Instant(ms), temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	s, err := temporal.Point(p)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// TestKnownBoundRelEndpointsMatchNodeLookup: under a Known bound the Effective
// view reads each endpoint at the endpoint's own known prefix, so a node ended
// by evidence known by t masks the relationship exactly as Lookup on the node shows.
func TestKnownBoundRelEndpointsMatchNodeLookup(t *testing.T) {
	g := memBare(t, nil)
	ka := knowledgeAxis(t)
	rec := func(ms int64) temporal.Position { return recAt(t, ka, ms) }
	var n1, n2 types.NodeID
	var rel types.RelID
	life := spanMS(t, 0, 1000)
	update(t, g, func(tx Tx) (err error) {
		if n1, err = tx.CreateNode(CreateNode{Scope: life, Recorded: rec(1)}); err != nil {
			return
		}
		if n2, err = tx.CreateNode(CreateNode{Scope: life, Recorded: rec(1)}); err != nil {
			return
		}
		rel, err = tx.CreateRel(CreateRel{Type: "R", From: n1, To: n2, Scope: life, Recorded: rec(1)})
		return
	})
	r1, _ := NodeRef(n1)
	update(t, g, func(tx Tx) error { return tx.End(End{Target: r1, Scope: spanMS(t, 0, 5), Recorded: rec(8)}) })
	read := func(known temporal.Scope) (nodeRows, relRows, scanRows int) {
		s, err := g.OpenSnapshot(context.Background(), Fresh())
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		sel := Selector{View: ViewEffective, Valid: validPointMS(t, 7), Known: known}
		pg := PageLimits{Rows: 10, MaxBytes: 1 << 20}
		p, err := s.Lookup(context.Background(), LookupRequest{Selector: sel, Nodes: []types.NodeID{n1}, Paging: pg})
		if err != nil {
			t.Fatal(err)
		}
		q, err := s.Lookup(context.Background(), LookupRequest{Selector: sel, Rels: []types.RelID{rel}, Paging: pg})
		if err != nil {
			t.Fatal(err)
		}
		sc := sel
		sc.RelType = "R"
		w, err := s.Scan(context.Background(), ScanRequest{Selector: sc, Paging: pg})
		if err != nil {
			t.Fatal(err)
		}
		return len(p.Rows), len(q.Rows), len(w.Rows)
	}
	// The End is known at 8: the node is absent and so is the relationship.
	for _, c := range []struct {
		name  string
		known temporal.Scope
	}{{"<=10", knownUpTo(t, 10, true)}, {"<=8", knownUpTo(t, 8, true)}} {
		if nr, rr, sr := read(c.known); nr != 0 || rr != 0 || sr != 0 {
			t.Errorf("known %s: node rows %d, rel rows %d, scan rows %d; want 0 0 0", c.name, nr, rr, sr)
		}
	}
	// Before the End is known, both stand.
	if nr, rr, sr := read(knownUpTo(t, 7, true)); nr != 1 || rr != 1 || sr != 1 {
		t.Errorf("known <=7: node rows %d, rel rows %d, scan rows %d; want 1 1 1", nr, rr, sr)
	}
	if nr, rr, sr := read(knownAll(t)); nr != 0 || rr != 0 || sr != 0 {
		t.Errorf("known All: node rows %d, rel rows %d, scan rows %d; want 0 0 0", nr, rr, sr)
	}
}

// TestKnownBelowNewestUsesBackdatedPostings: a Fresh() read with Known below the
// newest commit takes its candidates from the index pages at the newest commit
// the bound admits, plus the owners of backdated operations (Recorded earlier
// than their commit instant); it walks the stored entities only when the
// selector has no index.
func TestKnownBelowNewestUsesBackdatedPostings(t *testing.T) {
	g := memBare(t, nil)
	all := scopeAll(t)
	ka := knowledgeAxis(t)
	var a, b, x types.NodeID
	r1 := update(t, g, func(tx Tx) (err error) {
		a, err = tx.CreateNode(CreateNode{Labels: []string{"P"}, Scope: all})
		return
	})
	r2 := update(t, g, func(tx Tx) (err error) {
		b, err = tx.CreateNode(CreateNode{Labels: []string{"P"}, Scope: all})
		return
	})
	t1, t2 := instOf(t, r1), instOf(t, r2)
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
	// A third commit, not backdated: below it, the index pages serve the scan.
	update(t, g, func(tx Tx) error { return tx.AddLabel(AddLabel{Target: a, Label: "Z", Scope: all}) })
	pages0, st0 := e.scanPages.Load(), e.scanStats()
	if got := scan(knownUpTo(t, t1, true), "P"); len(got) != 1 || got[0] != a {
		t.Fatalf("P known at commit 1: %v", got)
	}
	if got := scan(knownUpTo(t, t2, true), "P"); len(got) != 2 {
		t.Fatalf("P known at commit 2: %v", got)
	}
	if got := scan(knownUpTo(t, t2, true), "Z"); len(got) != 0 {
		t.Fatalf("Z (added at commit 3) known at commit 2: %v", got)
	}
	if n := e.scanPages.Load() - pages0; n != 0 {
		t.Errorf("no backdated operation: %d entity pages walked, want 0", n)
	}
	if d := e.scanStats().minus(st0); d.candidates == 0 || d.candidates > 3 {
		t.Errorf("no backdated operation: %d index candidates, want 1..3", d.candidates)
	}
	// A backdated creation in a later commit (Recorded = t2): its owner joins the candidates.
	update(t, g, func(tx Tx) (err error) {
		x, err = tx.CreateNode(CreateNode{Labels: []string{"P"}, Scope: all, Recorded: recAt(t, ka, int64(t2))})
		return
	})
	pages0, st0 = e.scanPages.Load(), e.scanStats()
	got := scan(knownUpTo(t, t2, true), "P")
	if len(got) != 3 || got[0] != a || got[1] != b || got[2] != x {
		t.Fatalf("P known at commit 2 with a backdated owner: %v", got)
	}
	if got := scan(knownUpTo(t, t1, true), "P"); len(got) != 1 || got[0] != a {
		t.Fatalf("P known at commit 1 with a backdated owner: %v", got)
	}
	if n := e.scanPages.Load() - pages0; n != 0 {
		t.Errorf("backdated owner: %d entity pages walked, want 0", n)
	}
	// A bound at or above the view keeps the plain index pages.
	if got := scan(knownUpTo(t, 1<<60, true), "P"); len(got) != 3 {
		t.Fatalf("P known far future: %v", got)
	}
	// A selector without an index still walks the stored entities.
	pages0 = e.scanPages.Load()
	sBare, _ := g.OpenSnapshot(context.Background(), Fresh())
	defer sBare.Close()
	if p, err := sBare.Scan(context.Background(), ScanRequest{Selector: Selector{View: ViewDeclared, Valid: all, Known: knownUpTo(t, t1, true)}, Paging: PageLimits{Rows: 50, MaxBytes: 1 << 20}}); err != nil || len(p.Rows) != 1 {
		t.Fatalf("unindexed scan known at commit 1: %v %v", p.Rows, err)
	}
	if e.scanPages.Load() == pages0 {
		t.Error("an unindexed scan must walk the stored entities")
	}
}

// TestBackdatedOwnersPageThroughMany: more backdated owners than one posting page
// all reach the candidates, each once, in several commits.
func TestBackdatedOwnersPageThroughMany(t *testing.T) {
	g := memBare(t, nil)
	all := scopeAll(t)
	r0 := update(t, g, func(tx Tx) error {
		_, err := tx.CreateNode(CreateNode{Labels: []string{"P"}, Scope: all})
		return err
	})
	t0 := instOf(t, r0)
	const per, commits = 100, 3
	want := 1
	for c := 0; c < commits; c++ {
		update(t, g, func(tx Tx) error {
			for i := 0; i < per; i++ {
				if _, err := tx.CreateNode(CreateNode{Labels: []string{"P"}, Scope: all, Recorded: recAt(t, knowledgeAxis(t), int64(t0))}); err != nil {
					return err
				}
			}
			return nil
		})
		want += per
	}
	s, err := g.OpenSnapshot(context.Background(), Fresh())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	p, err := s.Scan(context.Background(), ScanRequest{Selector: Selector{View: ViewDeclared, Valid: all, Known: knownUpTo(t, t0, true), Labels: []string{"P"}}, Paging: PageLimits{Rows: 1000, MaxBytes: 4 << 20}})
	if err != nil {
		t.Fatal(err)
	}
	seen := map[types.NodeID]bool{}
	for _, r := range p.Rows {
		seen[r.Ref.Node()] = true
	}
	if len(p.Rows) != want || len(seen) != want {
		t.Fatalf("rows %d distinct %d, want %d", len(p.Rows), len(seen), want)
	}
}
