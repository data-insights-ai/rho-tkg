package graph

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/idalloc"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/types"
)

// bareConfig has no property seeds: every key is declared by its first Set.
func bareConfig(t testing.TB, dir string) Config {
	c := goodConfig(t)
	c.Dir = dir
	return c
}

func lookupProps(t testing.TB, g *Graph, id types.NodeID, keys ...string) map[string]Value {
	t.Helper()
	s, err := g.OpenSnapshot(context.Background(), Fresh())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	p, err := s.Lookup(context.Background(), LookupRequest{
		Selector:   Selector{View: ViewDeclared, Valid: scopeAll(t), Known: knownAll(t)},
		Projection: Projection{Properties: keys}, Nodes: []types.NodeID{id}, Paging: PageLimits{Rows: 5, MaxBytes: 1 << 20}})
	if err != nil || len(p.Rows) != 1 {
		t.Fatalf("lookup: %v %v", p, err)
	}
	return p.Rows[0].Props
}

// TestUndeclaredKeyIsDeclaredOnFirstSet: the first Set of a key declares it
// (kind = the value's kind), the declaration survives a reopen, a later kind
// switch is refused, and a Unique seed is still enforced.
func TestUndeclaredKeyIsDeclaredOnFirstSet(t *testing.T) {
	ctx := context.Background()
	cfg := bareConfig(t, t.TempDir())
	cfg.NodeProperties = []PropertySchema{{Key: "uid", Kind: KindString, Unique: true}}
	g, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	all := scopeAll(t)
	var id types.NodeID
	var rid types.RelID
	update(t, g, func(tx Tx) (err error) {
		if id, err = tx.CreateNode(CreateNode{Props: map[string]Value{"fresh": Int(5)}, Scope: all}); err != nil {
			return
		}
		other, err := tx.CreateNode(CreateNode{Scope: all})
		if err != nil {
			return err
		}
		rid, err = tx.CreateRel(CreateRel{Type: "R", From: id, To: other, Props: map[string]Value{"rw": String("x")}, Scope: all})
		return err
	})
	_ = g.Close()
	if g, err = Open(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	if v := lookupProps(t, g, id, "fresh")["fresh"]; v != Int(5) {
		t.Fatalf("declared key lost across reopen: %v", v)
	}
	ref, _ := NodeRef(id)
	rref, _ := RelRef(rid)
	set := func(r Ref, k string, v Value) error {
		_, err := g.Update(ctx, func(tx Tx) error { return tx.Set(Set{Target: r, Key: k, Value: v, Scope: all}) })
		return err
	}
	if err := set(ref, "fresh", String("no")); !errors.Is(err, ErrInvalidValue) {
		t.Errorf("node kind switch: %v", err)
	}
	if err := set(rref, "rw", Int(1)); !errors.Is(err, ErrInvalidValue) {
		t.Errorf("rel kind switch: %v", err)
	}
	if err := set(ref, "fresh", Int(6)); err != nil {
		t.Errorf("same kind: %v", err)
	}
	if err := set(ref, "later", Null()); !errors.Is(err, ErrInvalidValue) {
		t.Errorf("a null cannot declare a key: %v", err)
	}
	get := func(label, v string) error {
		_, err := g.Update(ctx, func(tx Tx) error {
			_, _, err := tx.GetOrCreate(GetOrCreate{Label: label, Key: "uid", Value: String(v), Scope: all})
			return err
		})
		return err
	}
	if err := get("U", "a"); err != nil {
		t.Fatal(err)
	}
	if err := get("W", "a"); !errors.Is(err, ErrUniqueViolation) {
		t.Errorf("unique seed not enforced: %v", err)
	}
	if err := get("U", "a"); err != nil {
		t.Errorf("hit: %v", err)
	}
}

// TestNumericWriteRule: under a typed key exact numerics coerce, inexact ones
// are refused.
func TestNumericWriteRule(t *testing.T) {
	g, err := Open(context.Background(), func() Config {
		c := bareConfig(t, "")
		c.Dir, c.Memory, c.Durability = "", true, DurabilityBuffered
		return c
	}())
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	all := scopeAll(t)
	var id types.NodeID
	update(t, g, func(tx Tx) (err error) {
		id, err = tx.CreateNode(CreateNode{Props: map[string]Value{"i": Int(1), "f": mustFloat(t, 1.5)}, Scope: all})
		return
	})
	ref, _ := NodeRef(id)
	set := func(k string, v Value) error {
		_, err := g.Update(context.Background(), func(tx Tx) error { return tx.Set(Set{Target: ref, Key: k, Value: v, Scope: all}) })
		return err
	}
	if err := set("i", mustFloat(t, 3)); err != nil {
		t.Fatal(err)
	}
	if err := set("f", Int(2)); err != nil {
		t.Fatal(err)
	}
	p := lookupProps(t, g, id, "i", "f")
	if p["i"] != Int(3) || p["f"] != mustFloat(t, 2) {
		t.Fatalf("coerced to the key kinds: %v", p)
	}
	if err := set("i", mustFloat(t, 2.5)); !errors.Is(err, ErrInvalidValue) {
		t.Errorf("2.5 into an Int key: %v", err)
	}
	if err := set("f", Int(1<<53+1)); !errors.Is(err, ErrInvalidValue) {
		t.Errorf("inexact Int into a Float key: %v", err)
	}
}

func memBare(t testing.TB, mut func(*Config)) *Graph {
	t.Helper()
	c := bareConfig(t, "")
	c.Dir, c.Memory, c.Durability = "", true, DurabilityBuffered
	if mut != nil {
		mut(&c)
	}
	g, err := Open(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close() })
	return g
}

func createN(t testing.TB, g *Graph, n int) ([]types.NodeID, error) {
	var ids []types.NodeID
	_, err := g.Update(context.Background(), func(tx Tx) error {
		for i := 0; i < n; i++ {
			id, err := tx.CreateNode(CreateNode{Scope: scopeAll(t)})
			if err != nil {
				return err
			}
			ids = append(ids, id)
		}
		return nil
	})
	return ids, err
}

// TestIDsComeFromTheAllocator: sequential, no per-Update cap, seeded, durable
// across reopen without reuse, bounded by idalloc.MaxPublicID.
func TestIDsComeFromTheAllocator(t *testing.T) {
	g := memBare(t, nil)
	// One Update is bounded by the engine's stage-record limit (a node writes
	// several records), not by the ID scheme; the counter itself has no cap.
	const n = 400
	ids, err := createN(t, g, n)
	if err != nil {
		t.Fatalf("%d creations in one Update: %v", n, err)
	}
	a, _ := newAllocState(bareConfig(t, ""))
	for i := 1; i <= 5000; i++ {
		if id, err := a.next(); err != nil || id != uint64(i) {
			t.Fatalf("allocator id %d = %d (%v)", i, id, err)
		}
	}
	for i, id := range ids {
		if id != types.NodeID(i+1) {
			t.Fatalf("id %d = %d, want sequential", i, id)
		}
	}
	more, err := createN(t, g, 2)
	if err != nil || more[0] != n+1 {
		t.Fatalf("next Update: %v %v", more, err)
	}

	seeded := memBare(t, func(c *Config) { c.Allocation = Allocation{Seed: 1000} })
	if ids, err := createN(t, seeded, 2); err != nil || ids[0] != 1001 || ids[1] != 1002 {
		t.Fatalf("seed: %v %v", ids, err)
	}

	cfg := bareConfig(t, t.TempDir())
	d, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	first, _ := createN(t, d, 3)
	_ = d.Close()
	d, err = Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	after, err := createN(t, d, 1)
	if err != nil || after[0] != first[2]+1 {
		t.Fatalf("after reopen: %v %v (before %v)", after, err, first)
	}

	edge := memBare(t, func(c *Config) { c.Allocation = Allocation{Seed: idalloc.MaxPublicID - 2} })
	if ids, err := createN(t, edge, 2); err != nil || ids[1] != types.NodeID(idalloc.MaxPublicID) {
		t.Fatalf("up to the bound: %v %v", ids, err)
	}
	if _, err := createN(t, edge, 1); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("exhaustion: %v", err)
	}
	bad := bareConfig(t, "")
	bad.Allocation = Allocation{Seed: idalloc.MaxPublicID}
	if err := bad.Validate(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("seed at the bound: %v", err)
	}
}

func TestNonAllKnownScopeIsNotImplemented(t *testing.T) {
	g := memBare(t, nil)
	ids, _ := createN(t, g, 1)
	s, _ := g.OpenSnapshot(context.Background(), Fresh())
	p, _ := types.InstantPosition(knowledgeAxis(t), 5, temporal.Limits{})
	pt, _ := temporal.Point(p)
	sel := Selector{View: ViewDeclared, Valid: scopeAll(t), Known: pt}
	ref, _ := NodeRef(ids[0])
	if _, err := s.Lookup(context.Background(), LookupRequest{Selector: sel, Nodes: ids, Paging: PageLimits{Rows: 1, MaxBytes: 99}}); !errors.Is(err, ErrNotImplemented) {
		t.Errorf("lookup: %v", err)
	}
	if _, err := s.Timeline(context.Background(), TimelineRequest{Ref: ref, Selector: sel, Paging: PageLimits{Rows: 1, MaxBytes: 99}}); !errors.Is(err, ErrNotImplemented) {
		t.Errorf("timeline: %v", err)
	}
}

func TestTimelineKnownIsTheRecordedEvidence(t *testing.T) {
	g := memBare(t, nil)
	all := scopeAll(t)
	ka := knowledgeAxis(t)
	var id types.NodeID
	update(t, g, func(tx Tx) (err error) {
		id, err = tx.CreateNode(CreateNode{Props: map[string]Value{"n": Int(1)}, Scope: all})
		return
	})
	ref, _ := NodeRef(id)
	update(t, g, func(tx Tx) error {
		return tx.Set(Set{Target: ref, Key: "n", Value: Int(2), Scope: all, Recorded: recAt(t, ka, 1000)})
	})
	s, _ := g.OpenSnapshot(context.Background(), Fresh())
	p, err := s.Timeline(context.Background(), TimelineRequest{Ref: ref, Selector: Selector{View: ViewDeclared, Valid: all, Known: knownAll(t)},
		Projection: Projection{Properties: []string{"n"}, Revisions: true}, Paging: PageLimits{Rows: 10, MaxBytes: 1 << 20}})
	if err != nil || len(p.Rows) != 2 {
		t.Fatalf("%v %v", p, err)
	}
	if p.Rows[0].Known.Kind() != temporal.ScopeAll {
		t.Errorf("a revision without evidence is known All: %v", p.Rows[0].Known.Kind())
	}
	if p.Rows[1].Known.Kind() != temporal.ScopePoint {
		t.Errorf("a revision with Recorded 1000 is known at that point: %v", p.Rows[1].Known.Kind())
	}
}

func TestCorrectAfterEndKeepsTheEnd(t *testing.T) {
	g := memBare(t, nil)
	all := scopeAll(t)
	var id types.NodeID
	update(t, g, func(tx Tx) (err error) {
		id, err = tx.CreateNode(CreateNode{Props: map[string]Value{"n": Int(1)}, Scope: all})
		return
	})
	ref, _ := NodeRef(id)
	update(t, g, func(tx Tx) error { return tx.End(End{Target: ref, Scope: spanMS(t, 0, 100)}) })
	// Wholly inside the ended region: refused. Straddling it: only the live part changes.
	if _, err := g.Update(context.Background(), func(tx Tx) error {
		return tx.Correct(Correct{Target: ref, Set: map[string]Value{"n": Int(9)}, Scope: spanMS(t, 150, 200)})
	}); !errors.Is(err, ErrInvalidScope) {
		t.Errorf("correct inside the end: %v", err)
	}
	update(t, g, func(tx Tx) error {
		return tx.Correct(Correct{Target: ref, Set: map[string]Value{"n": Int(9)}, Scope: spanMS(t, 50, 200)})
	})
	if r := look(t, g, Fresh(), id, spanMS(t, 100, 200)); len(r) != 0 {
		t.Fatalf("Correct revived the ended entity: %+v", r)
	}
	r := look(t, g, Fresh(), id, spanMS(t, 0, 100))
	if len(r) == 0 {
		t.Fatal("the live part vanished")
	}
	if n, _ := r[len(r)-1].Props["n"].AsInt(); n != 9 {
		t.Fatalf("live part not corrected: %+v", r)
	}
}

func TestResumeKeepsTheCursorWhenThePageFails(t *testing.T) {
	g := memBare(t, nil)
	ids, _ := createN(t, g, 3)
	s, _ := g.OpenSnapshot(context.Background(), Fresh())
	sel := Selector{View: ViewDeclared, Valid: scopeAll(t), Known: knownAll(t)}
	p, err := s.Lookup(context.Background(), LookupRequest{Selector: sel, Nodes: ids, Paging: PageLimits{Rows: 1, MaxBytes: 1 << 20}})
	if err != nil || p.Next.IsZero() {
		t.Fatal(p, err)
	}
	dead, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.Resume(dead, p.Next, PageLimits{Rows: 1, MaxBytes: 1 << 20}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled resume: %v", err)
	}
	q, err := s.Resume(context.Background(), p.Next, PageLimits{Rows: 1, MaxBytes: 1 << 20})
	if err != nil || len(q.Rows) != 1 || q.Rows[0].Ref.Node() != ids[1] {
		t.Fatalf("the cursor was burned by the failed page: %v %v", q, err)
	}
}

func TestAllPropertiesProjection(t *testing.T) {
	g := memBare(t, nil)
	var id types.NodeID
	update(t, g, func(tx Tx) (err error) {
		id, err = tx.CreateNode(CreateNode{Props: map[string]Value{"a": Int(1), "b": String("x")}, Scope: scopeAll(t)})
		return
	})
	s, _ := g.OpenSnapshot(context.Background(), Fresh())
	p, err := s.Lookup(context.Background(), LookupRequest{Selector: Selector{View: ViewDeclared, Valid: scopeAll(t), Known: knownAll(t)},
		Projection: Projection{AllProperties: true}, Nodes: []types.NodeID{id}, Paging: PageLimits{Rows: 1, MaxBytes: 1 << 20}})
	if err != nil || len(p.Rows) != 1 || len(p.Rows[0].Props) != 2 {
		t.Fatalf("%+v %v", p, err)
	}
}

// TestRefusedUpdateDeclaresNothing: one Update is one commit. A Set of a new key
// beside an op the planner refuses persists nothing, so the key stays free to
// be declared with another kind, also after a reopen.
func TestRefusedUpdateDeclaresNothing(t *testing.T) {
	ctx := context.Background()
	cfg := bareConfig(t, t.TempDir())
	g, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	all := scopeAll(t)
	var id types.NodeID
	update(t, g, func(tx Tx) (err error) {
		id, err = tx.CreateNode(CreateNode{Scope: all})
		return
	})
	ref, _ := NodeRef(id)
	ghost, _ := NodeRef(id + 1000)
	_, err = g.Update(ctx, func(tx Tx) error {
		if err := tx.Set(Set{Target: ref, Key: "typo", Value: Int(1), Scope: all}); err != nil {
			return err
		}
		return tx.Set(Set{Target: ghost, Key: "other", Value: Int(1), Scope: all})
	})
	if err == nil {
		t.Fatal("a Set on a missing node committed")
	}
	_ = g.Close()
	if g, err = Open(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	if v, ok := lookupProps(t, g, id, "typo")["typo"]; ok {
		t.Fatalf("refused Update wrote a value: %v", v)
	}
	for _, k := range []string{"typo", "other"} {
		if _, err := g.Update(ctx, func(tx Tx) error {
			return tx.Set(Set{Target: ref, Key: k, Value: String("s"), Scope: all})
		}); err != nil {
			t.Errorf("%s was declared by the refused Update: %v", k, err)
		}
	}
}

// TestEndAndCorrectInOneUpdateKeepsTheEnd: an End staged earlier in the same
// Update bounds a later Correct like a committed one; wholly inside it is refused.
func TestEndAndCorrectInOneUpdateKeepsTheEnd(t *testing.T) {
	g := memBare(t, nil)
	all := scopeAll(t)
	var id types.NodeID
	update(t, g, func(tx Tx) (err error) {
		id, err = tx.CreateNode(CreateNode{Props: map[string]Value{"n": Int(1)}, Scope: all})
		return
	})
	ref, _ := NodeRef(id)
	if _, err := g.Update(context.Background(), func(tx Tx) error {
		if err := tx.End(End{Target: ref, Scope: spanMS(t, 0, 100)}); err != nil {
			return err
		}
		return tx.Correct(Correct{Target: ref, Set: map[string]Value{"n": Int(9)}, Scope: spanMS(t, 150, 200)})
	}); !errors.Is(err, ErrInvalidScope) {
		t.Errorf("correct inside an end of the same Update: %v", err)
	}
	update(t, g, func(tx Tx) error {
		if err := tx.End(End{Target: ref, Scope: spanMS(t, 0, 100)}); err != nil {
			return err
		}
		return tx.Correct(Correct{Target: ref, Set: map[string]Value{"n": Int(9)}, Scope: spanMS(t, 50, 200)})
	})
	if r := look(t, g, Fresh(), id, spanMS(t, 100, 200)); len(r) != 0 {
		t.Fatalf("Correct revived the entity ended in the same Update: %+v", r)
	}
	r := look(t, g, Fresh(), id, spanMS(t, 0, 100))
	if len(r) == 0 {
		t.Fatal("the live part vanished")
	}
	if n, _ := r[len(r)-1].Props["n"].AsInt(); n != 9 {
		t.Fatalf("live part not corrected: %+v", r)
	}
}

// TestCorrectFillsRetractedHolesNeverPastAnEnd: Retract withdraws what was
// asserted; a later Correct over that scope is new knowledge and fills it. End
// closes the life: a Correct is clipped at the End and refused wholly after it.
func TestCorrectFillsRetractedHolesNeverPastAnEnd(t *testing.T) {
	ctx := context.Background()
	all := scopeAll(t)
	mk := func(g *Graph) (types.NodeID, Ref) {
		var id types.NodeID
		update(t, g, func(tx Tx) (err error) {
			id, err = tx.CreateNode(CreateNode{Props: map[string]Value{"n": Int(1)}, Scope: all})
			return
		})
		ref, _ := NodeRef(id)
		return id, ref
	}
	n9 := map[string]Value{"n": Int(9)}
	nAt := func(g *Graph, id types.NodeID, from, to int64) []int64 {
		var out []int64
		for _, r := range look(t, g, Fresh(), id, spanMS(t, from, to)) {
			v, _ := r.Props["n"].AsInt()
			out = append(out, v)
		}
		return out
	}

	t.Run("bounded hole", func(t *testing.T) {
		g := memBare(t, nil)
		id, ref := mk(g)
		update(t, g, func(tx Tx) error { return tx.Retract(Retract{Target: ref, Scope: spanMS(t, 100, 200)}) })
		update(t, g, func(tx Tx) error { return tx.Correct(Correct{Target: ref, Set: n9, Scope: spanMS(t, 120, 180)}) })
		if got := nAt(g, id, 150, 151); !slices.Equal(got, []int64{9}) {
			t.Fatalf("hole not filled: %v", got)
		}
		if got := nAt(g, id, 110, 111); len(got) != 0 {
			t.Fatalf("the unfilled part of the hole came back: %v", got)
		}
	})
	t.Run("whole life retracted", func(t *testing.T) {
		g := memBare(t, nil)
		id, ref := mk(g)
		update(t, g, func(tx Tx) error { return tx.Retract(Retract{Target: ref, Scope: all}) })
		update(t, g, func(tx Tx) error { return tx.Correct(Correct{Target: ref, Set: n9, Scope: spanMS(t, 0, 100)}) })
		if got := nAt(g, id, 50, 51); !slices.Equal(got, []int64{9}) {
			t.Fatalf("new assertion after a whole retraction: %v", got)
		}
	})
	t.Run("retract and correct in one Update", func(t *testing.T) {
		g := memBare(t, nil)
		id, ref := mk(g)
		update(t, g, func(tx Tx) error {
			if err := tx.Retract(Retract{Target: ref, Scope: spanMS(t, 100, 200)}); err != nil {
				return err
			}
			return tx.Correct(Correct{Target: ref, Set: n9, Scope: spanMS(t, 100, 200)})
		})
		if got := nAt(g, id, 150, 151); !slices.Equal(got, []int64{9}) {
			t.Fatalf("hole not filled in the same Update: %v", got)
		}
	})
	t.Run("hole touching the End is clipped there", func(t *testing.T) {
		g := memBare(t, nil)
		id, ref := mk(g)
		update(t, g, func(tx Tx) error { return tx.End(End{Target: ref, Scope: spanMS(t, 0, 200)}) })
		update(t, g, func(tx Tx) error { return tx.Retract(Retract{Target: ref, Scope: spanMS(t, 150, 200)}) })
		update(t, g, func(tx Tx) error { return tx.Correct(Correct{Target: ref, Set: n9, Scope: spanMS(t, 100, 300)}) })
		if got := nAt(g, id, 160, 161); !slices.Equal(got, []int64{9}) {
			t.Fatalf("hole before the End not filled: %v", got)
		}
		if got := nAt(g, id, 200, 300); len(got) != 0 {
			t.Fatalf("Correct extended past the End: %v", got)
		}
	})
	t.Run("inside an End region", func(t *testing.T) {
		g := memBare(t, nil)
		_, ref := mk(g)
		update(t, g, func(tx Tx) error { return tx.End(End{Target: ref, Scope: spanMS(t, 0, 200)}) })
		update(t, g, func(tx Tx) error { return tx.Retract(Retract{Target: ref, Scope: spanMS(t, 100, 200)}) })
		if _, err := g.Update(ctx, func(tx Tx) error {
			return tx.Correct(Correct{Target: ref, Set: n9, Scope: spanMS(t, 250, 300)})
		}); !errors.Is(err, ErrInvalidScope) {
			t.Fatalf("Correct after the End: %v", err)
		}
	})
}
