package graphstore

import (
	"errors"
	"reflect"
	"slices"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func pageSchemas() []graphstate.PropertyDefinition {
	var out []graphstate.PropertyDefinition
	for _, owner := range []graphstate.EntityKind{graphstate.Node, graphstate.Relationship} {
		for _, d := range []struct {
			name string
			typ  graphstate.ScalarKind
			card graphstate.Cardinality
		}{
			{"num", graphstate.ScalarI64, graphstate.ScalarCardinality},
			{"fl", graphstate.ScalarF64, graphstate.ScalarCardinality},
			{"txt", graphstate.ScalarString, graphstate.ScalarCardinality},
			{"tags", graphstate.ScalarI64, graphstate.SetCardinality},
		} {
			out = append(out, graphstate.PropertyDefinition{Name: d.name, Owner: owner, Type: d.typ, Cardinality: d.card})
		}
	}
	return out
}

// newIndexFixture is newFullFixture with the index-page test schemas.
func newIndexFixture(t *testing.T, l GraphLimits) *fullFixture {
	t.Helper()
	db := topologyStore(t, vfs.NewMem(), raftlog.DefaultApplicationPolicy(1))
	root := bootstrapRoot(t, db)
	f := &fullFixture{db: db, root: root, index: 1, axis: testAxis(t, 1, temporal.ProfileIntegerZ), limits: l}
	c := f.catalog(t, f.index)
	defer c.view.Close()
	effects, err := InitializeGraphIndexes(t.Context(), c, pageSchemas(), l)
	if err != nil {
		t.Fatal(err)
	}
	f.install(t, effects)
	return f
}

type pageFn func(graphstate.Cursor, graphstate.ReadBudget) (graphstate.EntityPage, error)

func drain(t *testing.T, fn pageFn, rows int) []graphstate.EntityID {
	t.Helper()
	out := []graphstate.EntityID{}
	var token graphstate.Cursor
	for range 1000 {
		page, err := fn(token, graphstate.ReadBudget{Rows: rows, Bytes: 1 << 20})
		if err != nil {
			t.Fatal(err)
		}
		if len(page.Entities) > rows {
			t.Fatal("page over row budget", len(page.Entities), rows)
		}
		out = append(out, page.Entities...)
		if page.Complete {
			if page.Next != 0 {
				t.Fatal("terminal page carries a cursor")
			}
			return out
		}
		if page.Next == 0 || page.Next == token {
			t.Fatal("stalled cursor")
		}
		token = page.Next
	}
	t.Fatal("pagination did not end")
	return nil
}
func labelIDs(t *testing.T, v *ReadView, label string, rows int) []graphstate.EntityID {
	t.Helper()
	return drain(t, func(c graphstate.Cursor, b graphstate.ReadBudget) (graphstate.EntityPage, error) {
		return v.LabelMembers(t.Context(), label, c, b)
	}, rows)
}
func eqIDs(t *testing.T, v *ReadView, owner graphstate.EntityKind, name string, value graphstate.Scalar, vis graphstate.Visibility, rows int) []graphstate.EntityID {
	t.Helper()
	return drain(t, func(c graphstate.Cursor, b graphstate.ReadBudget) (graphstate.EntityPage, error) {
		return v.EqualityMembers(t.Context(), graphstate.EqualityPredicate{Owner: owner, Name: name, Value: value, Visibility: vis}, c, b)
	}, rows)
}
func ids(v ...graphstate.EntityID) []graphstate.EntityID {
	return append([]graphstate.EntityID{}, v...)
}
func node(id graphstate.EntityID, scope temporal.Scope) graphstate.Operation {
	return graphstate.Operation{Kind: graphstate.CreateNode, Owner: id, Life: graphstate.LifeID(id*10 + 1), Scope: scope}
}
func label(kind graphstate.OperationKind, id graphstate.EntityID, scope temporal.Scope, name string) graphstate.Operation {
	return graphstate.Operation{Kind: kind, Owner: id, Life: graphstate.LifeID(id*10 + 1), Scope: scope, Name: name}
}

func TestLabelPageExactSetAfterMutation(t *testing.T) {
	f := newIndexFixture(t, GraphLimits{})
	all := f.span(t, 0, 10)
	f.apply(t, node(1, all), node(2, all), node(3, all), node(4, all), node(5, all))
	f.apply(t, label(graphstate.AddLabel, 3, all, "L"), label(graphstate.AddLabel, 1, all, "L"), label(graphstate.AddLabel, 2, all, "L"), label(graphstate.AddLabel, 5, all, "M"))
	added := f.index
	f.apply(t, label(graphstate.RemoveLabel, 2, all, "L"))
	removed := f.index
	f.apply(t, label(graphstate.AddLabel, 2, all, "L"))
	readded := f.index
	for _, c := range []struct {
		name  string
		index uint64
		want  []graphstate.EntityID
	}{{"added", added, ids(1, 2, 3)}, {"removed", removed, ids(1, 3)}, {"re-added", readded, ids(1, 2, 3)}} {
		v := f.view(t, c.index)
		for _, rows := range []int{1, 2, 3, 4, 100} {
			if got := labelIDs(t, v, "L", rows); !reflect.DeepEqual(got, c.want) {
				t.Fatal(c.name, "rows", rows, got, c.want)
			}
		}
		if got := labelIDs(t, v, "M", 5); !reflect.DeepEqual(got, ids(5)) {
			t.Fatal(c.name, "M", got)
		}
		if got := labelIDs(t, v, "none", 5); len(got) != 0 {
			t.Fatal(c.name, "unknown label", got)
		}
	}
	// A label held over two scopes stays a member until the last scope is gone.
	f.apply(t, label(graphstate.AddLabel, 4, f.span(t, 0, 4), "P"), label(graphstate.AddLabel, 4, f.span(t, 6, 10), "P"))
	f.apply(t, label(graphstate.RemoveLabel, 4, f.span(t, 0, 4), "P"))
	if got := labelIDs(t, f.view(t, f.index), "P", 5); !reflect.DeepEqual(got, ids(4)) {
		t.Fatal("partial removal dropped the member", got)
	}
	f.apply(t, label(graphstate.RemoveLabel, 4, f.span(t, 6, 10), "P"))
	if got := labelIDs(t, f.view(t, f.index), "P", 5); len(got) != 0 {
		t.Fatal("last removal kept the member", got)
	}
	// Two lives of one owner list the owner once.
	f.apply(t, graphstate.Operation{Kind: graphstate.Close, Owner: 1, Life: 11, Scope: f.span(t, 5, 10)}, graphstate.Operation{Kind: graphstate.Reopen, Owner: 1, Life: 12, Scope: f.span(t, 5, 10)}, graphstate.Operation{Kind: graphstate.AddLabel, Owner: 1, Life: 12, Scope: f.span(t, 5, 10), Name: "L"})
	for _, rows := range []int{1, 2, 10} {
		if got := labelIDs(t, f.view(t, f.index), "L", rows); !reflect.DeepEqual(got, ids(1, 2, 3)) {
			t.Fatal("owner with two lives listed twice or lost", rows, got)
		}
	}
}

func TestLabelPageBoundariesAndCursors(t *testing.T) {
	f := newIndexFixture(t, GraphLimits{})
	all := f.span(t, 0, 10)
	f.apply(t, node(1, all), node(2, all), node(3, all))
	f.apply(t, label(graphstate.AddLabel, 1, all, "L"), label(graphstate.AddLabel, 2, all, "L"), label(graphstate.AddLabel, 3, all, "L"))
	old := f.view(t, f.index)
	p := "L"
	b := func(rows int) graphstate.ReadBudget { return graphstate.ReadBudget{Rows: rows, Bytes: 1 << 20} }
	// Exactly the limit: complete, no cursor. One short: a cursor.
	page, err := old.LabelMembers(t.Context(), p, 0, b(3))
	if err != nil || !page.Complete || page.Next != 0 || !reflect.DeepEqual(page.Entities, ids(1, 2, 3)) {
		t.Fatal("exact limit", page, err)
	}
	first, err := old.LabelMembers(t.Context(), p, 0, b(2))
	if err != nil || first.Complete || first.Next == 0 || !reflect.DeepEqual(first.Entities, ids(1, 2)) {
		t.Fatal("limit minus one", first, err)
	}
	if first.View != old.Identity() || first.Version == 0 {
		t.Fatal("page is not bound to its view", first)
	}
	// Mutate: a new view refuses the old cursor; the old view still answers.
	f.apply(t, label(graphstate.RemoveLabel, 3, all, "L"))
	fresh := f.view(t, f.index)
	if _, err := fresh.LabelMembers(t.Context(), p, first.Next, b(2)); !errors.Is(err, ErrInvalid) {
		t.Fatal("cursor accepted after mutation on another cut", err)
	}
	rest, err := old.LabelMembers(t.Context(), p, first.Next, b(2))
	if err != nil || !rest.Complete || !reflect.DeepEqual(rest.Entities, ids(3)) {
		t.Fatal("old cut changed", rest, err)
	}
	if _, err := old.LabelMembers(t.Context(), p, first.Next, b(2)); !errors.Is(err, ErrInvalid) {
		t.Fatal("consumed cursor accepted twice", err)
	}
	other, err := old.LabelMembers(t.Context(), p, 0, b(1))
	if err != nil || other.Next == 0 {
		t.Fatal(other, err)
	}
	q := "M"
	if _, err := old.LabelMembers(t.Context(), q, other.Next, b(1)); !errors.Is(err, ErrInvalid) {
		t.Fatal("cursor accepted for another predicate", err)
	}
	if _, err := old.EqualityMembers(t.Context(), graphstate.EqualityPredicate{Owner: graphstate.Node, Name: "num", Value: graphstate.I64(1), Visibility: graphstate.Declared}, other.Next, b(1)); !errors.Is(err, ErrInvalid) {
		t.Fatal("label cursor accepted by the equality door", err)
	}
	for _, bad := range []struct {
		name string
		p    string
		b    graphstate.ReadBudget
	}{
		{"zero rows", p, graphstate.ReadBudget{Rows: 0, Bytes: 100}},
		{"zero bytes", p, graphstate.ReadBudget{Rows: 1, Bytes: 0}},
		{"empty label", "", b(1)},
	} {
		if _, err := old.LabelMembers(t.Context(), bad.p, 0, bad.b); !errors.Is(err, ErrInvalid) {
			t.Fatal(bad.name, err)
		}
	}
	// A byte budget too small for one entity refuses instead of looping empty.
	if page, err := old.LabelMembers(t.Context(), p, 0, graphstate.ReadBudget{Rows: 3, Bytes: 1}); err == nil && len(page.Entities) == 0 && !page.Complete {
		t.Fatal("empty incomplete page", page)
	}
	closed := f.view(t, f.index)
	_ = closed.Close()
	if _, err := closed.LabelMembers(t.Context(), p, 0, b(1)); !errors.Is(err, ErrClosed) {
		t.Fatal("closed view answered", err)
	}
}

func TestEqualityPageCrossKind(t *testing.T) {
	f := newIndexFixture(t, GraphLimits{})
	all := f.span(t, 0, 10)
	f.apply(t, node(1, all), node(2, all), node(3, all), node(4, all), node(5, all))
	f3, _ := graphstate.F64(3.0)
	f25, _ := graphstate.F64(2.5)
	set := func(id graphstate.EntityID, name string, v graphstate.Scalar, vid graphstate.ValueID) graphstate.Operation {
		return graphstate.Operation{Kind: graphstate.Set, Owner: id, Life: graphstate.LifeID(id*10 + 1), Scope: all, Name: name, Value: v, ValueID: vid}
	}
	f.apply(t, set(1, "fl", f3, 101), set(2, "num", graphstate.I64(3), 102), set(3, "fl", f25, 103), set(4, "txt", graphstate.String("3"), 104), set(5, "num", graphstate.I64(4), 105))
	v := f.view(t, f.index)
	for _, c := range []struct {
		name  string
		prop  string
		value graphstate.Scalar
		want  []graphstate.EntityID
	}{
		{"stored 3.0 found by I64(3)", "fl", graphstate.I64(3), ids(1)},
		{"stored 3.0 found by F64(3.0)", "fl", f3, ids(1)},
		{"stored I64(3) found by F64(3.0)", "num", f3, ids(2)},
		{"stored I64(3) found by I64(3)", "num", graphstate.I64(3), ids(2)},
		{"2.5 not found by I64(2)", "fl", graphstate.I64(2), ids()},
		{"I64(3) finds 3.0 but not 2.5", "fl", graphstate.I64(3), ids(1)},
		{"2.5 found by 2.5", "fl", f25, ids(3)},
		{"string 3 not found by I64(3)", "txt", graphstate.I64(3), ids()},
		{"I64(3) not found by string 3", "num", graphstate.String("3"), ids()},
		{"string 3 found by string 3", "txt", graphstate.String("3"), ids(4)},
		{"Null never", "num", graphstate.Null(), ids()},
		{"unknown property", "nope", graphstate.I64(3), ids()},
	} {
		for _, rows := range []int{1, 5} {
			if got := eqIDs(t, v, graphstate.Node, c.prop, c.value, graphstate.Declared, rows); !reflect.DeepEqual(got, c.want) {
				t.Fatal(c.name, rows, got, c.want)
			}
		}
	}
	if got := eqIDs(t, v, graphstate.Relationship, "num", graphstate.I64(3), graphstate.Declared, 5); len(got) != 0 {
		t.Fatal("wrong owner kind", got)
	}
	// Replacing a value moves the posting; the old cut keeps the old one.
	before := f.index
	f.apply(t, set(2, "num", graphstate.I64(9), 106))
	now := f.view(t, f.index)
	if got := eqIDs(t, now, graphstate.Node, "num", graphstate.I64(3), graphstate.Declared, 5); len(got) != 0 {
		t.Fatal("replaced value still indexed", got)
	}
	if got := eqIDs(t, now, graphstate.Node, "num", graphstate.I64(9), graphstate.Declared, 5); !reflect.DeepEqual(got, ids(2)) {
		t.Fatal("new value missing", got)
	}
	if got := eqIDs(t, f.view(t, before), graphstate.Node, "num", graphstate.I64(3), graphstate.Declared, 5); !reflect.DeepEqual(got, ids(2)) {
		t.Fatal("old cut changed", got)
	}
	// Unset removes it; I64(3) and F64(3.0) on two owners list both in ID order.
	f.apply(t, graphstate.Operation{Kind: graphstate.Unset, Owner: 2, Life: 21, Scope: all, Name: "num"}, set(5, "num", graphstate.I64(3), 107), set(1, "num", graphstate.I64(3), 108))
	if got := eqIDs(t, f.view(t, f.index), graphstate.Node, "num", graphstate.I64(3), graphstate.Declared, 1); !reflect.DeepEqual(got, ids(1, 5)) {
		t.Fatal("two owners, two kinds", got)
	}
	// Set members: equal-by-key members of one owner list the owner once.
	f.apply(t, graphstate.Operation{Kind: graphstate.Add, Owner: 3, Life: 31, Scope: all, Name: "tags", Value: graphstate.I64(7), ValueID: 109}, graphstate.Operation{Kind: graphstate.Add, Owner: 3, Life: 31, Scope: all, Name: "tags", Value: graphstate.I64(8), ValueID: 110})
	f.apply(t, graphstate.Operation{Kind: graphstate.Remove, Owner: 3, Life: 31, Scope: all, Name: "tags", Value: graphstate.I64(7)})
	w := f.view(t, f.index)
	if got := eqIDs(t, w, graphstate.Node, "tags", graphstate.I64(8), graphstate.Declared, 5); !reflect.DeepEqual(got, ids(3)) {
		t.Fatal("set member missing", got)
	}
	if got := eqIDs(t, w, graphstate.Node, "tags", graphstate.I64(7), graphstate.Declared, 5); len(got) != 0 {
		t.Fatal("removed member indexed", got)
	}
	if !slices.IsSorted(eqIDs(t, w, graphstate.Node, "num", graphstate.I64(3), graphstate.Declared, 1)) {
		t.Fatal("unordered")
	}
	// Bad inputs.
	for _, bad := range []graphstate.EqualityPredicate{
		{Owner: graphstate.Node, Value: graphstate.I64(3), Visibility: graphstate.Declared},
		{Name: "num", Value: graphstate.I64(3), Visibility: graphstate.Declared},
		{Owner: graphstate.Node, Name: "num", Value: graphstate.I64(3)},
		{Owner: graphstate.Node, Name: "num", Visibility: graphstate.Declared},
	} {
		if _, err := w.EqualityMembers(t.Context(), bad, 0, graphstate.ReadBudget{Rows: 1, Bytes: 100}); !errors.Is(err, ErrInvalid) {
			t.Fatal("bad predicate accepted", bad, err)
		}
	}
	if _, err := w.EqualityMembers(t.Context(), graphstate.EqualityPredicate{Owner: graphstate.Node, Name: "num", Value: graphstate.I64(3), Visibility: graphstate.Declared}, 0, graphstate.ReadBudget{}); !errors.Is(err, ErrInvalid) {
		t.Fatal("zero budget", err)
	}
}

func TestEffectiveMaskHidesRelWithEndedEndpoint(t *testing.T) {
	f := newIndexFixture(t, GraphLimits{})
	all := f.span(t, 0, 10)
	rel := func(id, src, dst graphstate.EntityID, mode graphstate.ReferenceMode, sl, tl graphstate.LifeID) graphstate.Operation {
		return graphstate.Operation{Kind: graphstate.CreateRelationship, Owner: id, Life: graphstate.LifeID(id*10 + 1), Scope: all, Record: graphstate.EntityRecord{Type: "R", Source: src, Target: dst, Mode: mode}, Binding: graphstate.LifeRecord{SourceLife: sl, TargetLife: tl}}
	}
	f.apply(t, node(1, all), node(2, all))
	f.apply(t, rel(3, 1, 2, graphstate.LifeBound, 11, 21), rel(4, 2, 2, graphstate.IdentityReference, 0, 0), rel(5, 1, 1, graphstate.LifeBound, 11, 11))
	setNum := func(id graphstate.EntityID, vid graphstate.ValueID) graphstate.Operation {
		return graphstate.Operation{Kind: graphstate.Set, Owner: id, Life: graphstate.LifeID(id*10 + 1), Scope: all, Name: "num", Value: graphstate.I64(5), ValueID: vid}
	}
	f.apply(t, setNum(3, 201), setNum(4, 201), setNum(5, 201))
	check := func(name string, index uint64, wantDeclared, wantEffective []graphstate.EntityID) {
		t.Helper()
		v := f.view(t, index)
		for _, rows := range []int{1, 10} {
			if got := eqIDs(t, v, graphstate.Relationship, "num", graphstate.I64(5), graphstate.Declared, rows); !reflect.DeepEqual(got, wantDeclared) {
				t.Fatal(name, "equality declared", got, wantDeclared)
			}
			if got := eqIDs(t, v, graphstate.Relationship, "num", graphstate.I64(5), graphstate.Effective, rows); !reflect.DeepEqual(got, wantEffective) {
				t.Fatal(name, "equality effective", rows, got, wantEffective)
			}
		}
	}
	live := f.index
	check("both endpoints live", live, ids(3, 4, 5), ids(3, 4, 5))
	// End node 2 for good: rels bound to its life, or referring to it, vanish from Effective only.
	f.apply(t, graphstate.Operation{Kind: graphstate.Close, Owner: 2, Life: 21, Scope: all})
	ended := f.index
	check("endpoint ended", ended, ids(3, 4, 5), ids(5))
	// A new life of node 2 revives the identity reference, never the life-bound rel.
	f.apply(t, graphstate.Operation{Kind: graphstate.Reopen, Owner: 2, Life: 22, Scope: all})
	check("endpoint reopened", f.index, ids(3, 4, 5), ids(4, 5))
	check("old cut unchanged", ended, ids(3, 4, 5), ids(5))
	// Partial closing keeps the endpoint valid; nodes are never masked.
	f.apply(t, setNum(1, 202), graphstate.Operation{Kind: graphstate.Close, Owner: 1, Life: 11, Scope: f.span(t, 8, 10)})
	v := f.view(t, f.index)
	if got := eqIDs(t, v, graphstate.Node, "num", graphstate.I64(5), graphstate.Effective, 5); !reflect.DeepEqual(got, ids(1)) {
		t.Fatal("node masked", got)
	}
	if got := eqIDs(t, v, graphstate.Relationship, "num", graphstate.I64(5), graphstate.Effective, 5); !reflect.DeepEqual(got, ids(4, 5)) {
		t.Fatal("partially closed endpoint hid its relationship", got)
	}
	f.apply(t, graphstate.Operation{Kind: graphstate.Close, Owner: 1, Life: 11, Scope: f.span(t, 0, 8)})
	if got := eqIDs(t, f.view(t, f.index), graphstate.Relationship, "num", graphstate.I64(5), graphstate.Effective, 5); !reflect.DeepEqual(got, ids(4)) {
		t.Fatal("fully closed endpoint kept its relationship", got)
	}
}
