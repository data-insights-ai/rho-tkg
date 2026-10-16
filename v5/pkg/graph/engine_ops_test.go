package graph

import (
	"context"
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/types"
)

func spanMS(t testing.TB, from, to int64) temporal.Scope {
	t.Helper()
	ax := axis(t)
	lo, err := types.InstantPosition(ax, types.Instant(from), temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	hi, err := types.InstantPosition(ax, types.Instant(to), temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	lb, _ := temporal.FiniteBound(lo, true)
	hb, _ := temporal.FiniteBound(hi, false)
	s, err := temporal.Span(ax, lb, hb, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func openMem(t testing.TB) *Graph {
	t.Helper()
	c := engineConfig(t, "")
	c.Dir, c.Memory, c.Durability = "", true, DurabilityBuffered
	c.NodeProperties = append(c.NodeProperties, PropertySchema{Key: "uid", Kind: KindString, Unique: true})
	g, err := Open(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close() })
	return g
}

func look(t testing.TB, g *Graph, spec SnapshotSpec, id types.NodeID, valid temporal.Scope) []Entity {
	t.Helper()
	s, err := g.OpenSnapshot(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	p, err := s.Lookup(context.Background(), LookupRequest{
		Selector:   Selector{View: ViewDeclared, Valid: valid, Known: knownAll(t)},
		Projection: Projection{Properties: []string{"n", "name"}}, Nodes: []types.NodeID{id}, Paging: PageLimits{Rows: 100, MaxBytes: 1 << 20}})
	if err != nil {
		t.Fatal(err)
	}
	return p.Rows
}

func update(t testing.TB, g *Graph, fn func(Tx) error) Receipt {
	t.Helper()
	r, err := g.Update(context.Background(), fn)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// TestUpdateOpsAndCuts: set, label, end and retract, each read at the cut taken
// before it; earlier cuts never change.
func TestUpdateOpsAndCuts(t *testing.T) {
	g := openMem(t)
	all := scopeAll(t)
	var id types.NodeID
	update(t, g, func(tx Tx) (err error) {
		id, err = tx.CreateNode(CreateNode{Labels: []string{"A"}, Props: map[string]Value{"n": Int(1)}, Scope: all})
		return
	})
	s1, _ := g.OpenSnapshot(context.Background(), Fresh())
	c1 := s1.Cut()
	update(t, g, func(tx Tx) error {
		ref, _ := NodeRef(id)
		if err := tx.Set(Set{Target: ref, Key: "n", Value: Int(2), Scope: all}); err != nil {
			return err
		}
		return tx.AddLabel(AddLabel{Target: id, Label: "B", Scope: all})
	})
	s2, _ := g.OpenSnapshot(context.Background(), Fresh())
	c2 := s2.Cut()
	update(t, g, func(tx Tx) error {
		ref, _ := NodeRef(id)
		return tx.End(End{Target: ref, Scope: spanMS(t, 0, 100)})
	})
	c3 := func() Cut { s, _ := g.OpenSnapshot(context.Background(), Fresh()); return s.Cut() }()

	row := func(c Cut, valid temporal.Scope) []Entity { return look(t, g, At(c), id, valid) }
	if r := row(c1, all); len(r) != 1 || r[0].Props["n"] != Int(1) || len(r[0].Labels) != 1 {
		t.Fatalf("c1 = %+v", r)
	}
	if r := row(c2, all); len(r) != 1 || r[0].Props["n"] != Int(2) || len(r[0].Labels) != 2 {
		t.Fatalf("c2 = %+v", r)
	}
	// After End at 100: present before 100, absent from 100 on, at the new cut only.
	r := row(c3, all)
	if len(r) != 1 {
		t.Fatalf("c3 all = %+v", r)
	}
	if r := row(c2, spanMS(t, 100, 200)); len(r) != 1 {
		t.Fatalf("old cut still sees the entity after 100: %+v", r)
	}
	if r := row(c3, spanMS(t, 100, 200)); len(r) != 0 {
		t.Fatalf("ended entity visible after the end: %+v", r)
	}
	// Retract over all removes the belief everywhere at the new cut.
	update(t, g, func(tx Tx) error {
		ref, _ := NodeRef(id)
		return tx.Retract(Retract{Target: ref, Scope: all})
	})
	if r := look(t, g, Fresh(), id, all); len(r) != 0 {
		t.Fatalf("retracted: %+v", r)
	}
	if r := row(c3, all); len(r) != 1 {
		t.Fatalf("old cut lost the entity after a later retract: %+v", r)
	}
}

func TestTimelineListsEveryRevisionOldestFirst(t *testing.T) {
	g := openMem(t)
	all := scopeAll(t)
	var id types.NodeID
	update(t, g, func(tx Tx) (err error) {
		id, err = tx.CreateNode(CreateNode{Props: map[string]Value{"n": Int(1)}, Scope: all})
		return
	})
	for _, v := range []int64{2, 3} {
		update(t, g, func(tx Tx) error {
			ref, _ := NodeRef(id)
			return tx.Set(Set{Target: ref, Key: "n", Value: Int(v), Scope: all})
		})
	}
	s, _ := g.OpenSnapshot(context.Background(), Fresh())
	ref, _ := NodeRef(id)
	p, err := s.Timeline(context.Background(), TimelineRequest{Ref: ref, Selector: Selector{View: ViewDeclared, Valid: all, Known: knownAll(t)},
		Projection: Projection{Properties: []string{"n"}}, Paging: PageLimits{Rows: 10, MaxBytes: 1 << 20}})
	if err != nil || len(p.Rows) != 3 {
		t.Fatalf("timeline: %+v %v", p, err)
	}
	for i, want := range []int64{1, 2, 3} {
		if n, _ := p.Rows[i].Props["n"].AsInt(); n != want {
			t.Fatalf("row %d = %d, want %d", i, n, want)
		}
	}
}

func TestRelationshipAndRefusals(t *testing.T) {
	g := openMem(t)
	all := scopeAll(t)
	var a, b types.NodeID
	var r types.RelID
	rc := update(t, g, func(tx Tx) (err error) {
		if a, err = tx.CreateNode(CreateNode{Scope: all}); err != nil {
			return
		}
		if b, err = tx.CreateNode(CreateNode{Scope: all}); err != nil {
			return
		}
		r, err = tx.CreateRel(CreateRel{Type: "R", From: a, To: b, Props: map[string]Value{"w": Int(5)}, Scope: all})
		return
	})
	s, _ := g.OpenSnapshot(context.Background(), After(rc))
	rr, _ := RelRef(r)
	pg, err := s.Timeline(context.Background(), TimelineRequest{Ref: rr, Selector: Selector{View: ViewDeclared, Valid: all, Known: knownAll(t)},
		Projection: Projection{Properties: []string{"w"}}, Paging: PageLimits{Rows: 5, MaxBytes: 1 << 20}})
	if err != nil || len(pg.Rows) != 1 || pg.Rows[0].RelType != "R" || pg.Rows[0].From != a || pg.Rows[0].To != b {
		t.Fatalf("rel row: %+v %v", pg, err)
	}
	ctx := context.Background()
	refuse := func(name string, want error, fn func(Tx) error) {
		before, _ := g.OpenSnapshot(ctx, Fresh())
		_, err := g.Update(ctx, func(tx Tx) error {
			if _, e := tx.CreateNode(CreateNode{Scope: all}); e != nil {
				return e
			}
			return fn(tx)
		})
		after, _ := g.OpenSnapshot(ctx, Fresh())
		if !errors.Is(err, want) {
			t.Errorf("%s: %v, want %v", name, err, want)
		}
		if string(before.Cut().Bytes()) != string(after.Cut().Bytes()) {
			t.Errorf("%s: a refused Update committed", name)
		}
	}
	na, _ := NodeRef(a)
	refuse("wrong kind", ErrInvalidValue, func(tx Tx) error {
		return tx.Set(Set{Target: na, Key: "n", Value: String("x"), Scope: all})
	})
	refuse("unknown target", ErrNotFound, func(tx Tx) error {
		ghost, _ := NodeRef(424242)
		return tx.Set(Set{Target: ghost, Key: "n", Value: Int(1), Scope: all})
	})
	refuse("wrong axis scope", ErrInvalidScope, func(tx Tx) error {
		return tx.Set(Set{Target: na, Key: "n", Value: Int(1), Scope: otherAxisScope(t)})
	})
	// Tx after Update is finished.
	var kept Tx
	update(t, g, func(tx Tx) error { kept = tx; return nil })
	if _, err := kept.CreateNode(CreateNode{Scope: all}); !errors.Is(err, ErrTxDone) {
		t.Errorf("kept Tx: %v", err)
	}
	// Scan stays not implemented.
	if _, err := s.Scan(ctx, ScanRequest{Selector: Selector{View: ViewDeclared, Valid: all, Known: knownAll(t)}, Paging: PageLimits{Rows: 1, MaxBytes: 1}}); !errors.Is(err, ErrNotImplemented) {
		t.Errorf("scan: %v", err)
	}
	// Closed snapshot and graph.
	_ = s.Close()
	if _, err := s.Lookup(ctx, LookupRequest{Selector: Selector{View: ViewDeclared, Valid: all, Known: knownAll(t)}, Nodes: []types.NodeID{a}, Paging: PageLimits{Rows: 1, MaxBytes: 99}}); !errors.Is(err, ErrClosed) {
		t.Errorf("closed snapshot: %v", err)
	}
	_ = g.Close()
	if _, err := g.Update(ctx, func(Tx) error { return nil }); !errors.Is(err, ErrClosed) {
		t.Errorf("closed graph: %v", err)
	}
}

func TestGetOrCreateIsIdempotentAndSerial(t *testing.T) {
	g := openMem(t)
	all := scopeAll(t)
	get := func(label, v string) (types.NodeID, bool, error) {
		var id types.NodeID
		var created bool
		_, err := g.Update(context.Background(), func(tx Tx) (err error) {
			id, created, err = tx.GetOrCreate(GetOrCreate{Label: label, Key: "uid", Value: String(v), Scope: all})
			return
		})
		return id, created, err
	}
	first, created, err := get("U", "x")
	if err != nil || !created {
		t.Fatal(first, created, err)
	}
	for i := 0; i < 3; i++ {
		again, created, err := get("U", "x")
		if err != nil || created || again != first {
			t.Fatalf("hit %d: %v %v %v", i, again, created, err)
		}
	}
	if _, _, err := get("W", "x"); !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("claim under another label: %v", err)
	}
	other, created, err := get("U", "y")
	if err != nil || !created || other == first {
		t.Fatal(other, created, err)
	}
	// Twice in one Update creates once.
	var a, b types.NodeID
	update(t, g, func(tx Tx) (err error) {
		a, _, err = tx.GetOrCreate(GetOrCreate{Label: "U", Key: "uid", Value: String("z"), Scope: all})
		if err != nil {
			return
		}
		b, _, err = tx.GetOrCreate(GetOrCreate{Label: "U", Key: "uid", Value: String("z"), Scope: all})
		return
	})
	if a != b {
		t.Fatalf("one Update created %d and %d", a, b)
	}
	// Not a declared unique key.
	_, err = g.Update(context.Background(), func(tx Tx) error {
		_, _, err := tx.GetOrCreate(GetOrCreate{Label: "U", Key: "n", Value: Int(1), Scope: all})
		return err
	})
	if !errors.Is(err, ErrInvalidOp) {
		t.Fatalf("non-unique key: %v", err)
	}
}

func TestOpenReopenRejectsDifferentSchema(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfg := engineConfig(t, dir)
	g, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	_ = g.Close()
	bad := cfg
	bad.NodeProperties = []PropertySchema{{Key: "n", Kind: KindString}, {Key: "name", Kind: KindString}}
	if _, err := Open(ctx, bad); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("changed schema: %v", err)
	}
	if g, err = Open(ctx, cfg); err != nil {
		t.Fatalf("same schema: %v", err)
	}
	_ = g.Close()
}

// TestUpdateRefusesOffAxisRecorded: Recorded is evidence on the knowledge axis
// no later than the commit's own instant (the engine has no clock; Update
// supplies it as GraphLimits.RecordedCeiling).
func TestUpdateRefusesOffAxisRecorded(t *testing.T) {
	g := openMem(t)
	ctx := context.Background()
	all := scopeAll(t)
	var id types.NodeID
	update(t, g, func(tx Tx) (err error) {
		id, err = tx.CreateNode(CreateNode{Scope: all})
		return
	})
	ref, _ := NodeRef(id)
	set := func(rec temporal.Position) error {
		_, err := g.Update(ctx, func(tx Tx) error {
			return tx.Set(Set{Target: ref, Key: "n", Value: Int(1), Scope: all, Recorded: rec})
		})
		return err
	}
	known := knowledgeAxis(t)
	if err := set(recAt(t, axis(t), 1000)); !errors.Is(err, ErrInvalidScope) {
		t.Errorf("valid-axis instant: %v", err)
	}
	if err := set(recAt(t, otherAxisScope(t).Axis(), 1000)); !errors.Is(err, ErrInvalidScope) {
		t.Errorf("off-axis instant: %v", err)
	}
	if err := set(recAt(t, known, 1<<50)); err == nil || !errors.Is(err, ErrInvalidScope) {
		t.Errorf("future instant (after the commit): %v", err)
	}
	if err := set(recAt(t, known, 1000)); err != nil {
		t.Errorf("past instant on the knowledge axis: %v", err)
	}
	if err := set(recAt(t, known, 0)); err != nil {
		t.Errorf("instant 0 is a coordinate: %v", err)
	}
	// Unset KnowledgeAxis: any non-zero Recorded is refused, zero still commits.
	cfg := g.cfg
	cfg.KnowledgeAxis = types.DefaultAxisBinding{}
	tx := &engineTx{e: &pebbleEngine{cfg: cfg}, ctx: ctx, next: 99}
	if err := tx.Set(Set{Target: ref, Key: "n", Value: Int(1), Scope: all, Recorded: recAt(t, known, 1000)}); !errors.Is(err, ErrInvalidScope) {
		t.Errorf("unset KnowledgeAxis: %v", err)
	}
	if err := tx.AddLabel(AddLabel{Target: id, Label: "L", Scope: all}); err != nil {
		t.Errorf("unset KnowledgeAxis, zero Recorded: %v", err)
	}
}
