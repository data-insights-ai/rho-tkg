package graphstore

import (
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
)

func f64Value(t testing.TB, v float64) graphstate.Scalar {
	t.Helper()
	s, err := graphstate.F64(v)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// The dictionary is kind-preserving: I64(3) and F64(3.0) are distinct values with
// distinct IDs; they are equal only through Scalar.Equal (query evaluation).
func TestValueIdentityCrossKind(t *testing.T) {
	db, root := newStore(t, vfs.NewMem())
	c := openCatalog(t, db, 1, Limits{})
	s := stage(t, c)
	for id, v := range map[uint64]graphstate.Scalar{1: f64Value(t, 3.0), 2: graphstate.I64(3), 3: graphstate.I64(7), 4: f64Value(t, 2.5)} {
		if err := s.Value(t.Context(), refValue(id), v); err != nil {
			t.Fatal(id, err)
		}
	}
	_, index := commitStage(t, db, root, s)
	cat := openCatalog(t, db, index, Limits{})
	for _, tc := range []struct {
		probe graphstate.Scalar
		id    graphstate.ValueID
		found bool
	}{
		{f64Value(t, 3.0), 1, true}, {graphstate.I64(3), 2, true}, {graphstate.I64(7), 3, true},
		{f64Value(t, 7.0), 0, false}, {f64Value(t, 2.5), 4, true}, {graphstate.I64(2), 0, false},
		{graphstate.String("3"), 0, false}, {graphstate.Bool(true), 0, false},
	} {
		e, found, err := cat.LookupLocalValueIdentity(t.Context(), tc.probe)
		if err != nil || found != tc.found || (found && e.Ref.ID != tc.id) {
			t.Fatal(tc.probe.Render(), tc.probe.Kind(), e.Ref, found, err)
		}
	}
	for id, kind := range map[uint64]graphstate.ScalarKind{1: graphstate.ScalarF64, 2: graphstate.ScalarI64} {
		v, found, err := cat.Value(t.Context(), refValue(id))
		if err != nil || !found || v.Value.Kind() != kind {
			t.Fatal(id, v.Value.Kind(), found, err)
		}
	}
	a, _, _ := cat.Value(t.Context(), refValue(1))
	b, _, _ := cat.Value(t.Context(), refValue(2))
	if eq, err := a.Value.Equal(b.Value, graphstate.Limits{}); err != nil || !eq {
		t.Fatal("Equal must still relate them", eq, err)
	}
	// interning keeps both kinds apart
	next := stage(t, cat)
	if got, err := next.InternLocal(t.Context(), refValue(10), graphstate.I64(3)); err != nil || got != refValue(2) {
		t.Fatal(got, err)
	}
	if got, err := next.InternLocal(t.Context(), refValue(11), f64Value(t, 3.0)); err != nil || got != refValue(1) {
		t.Fatal(got, err)
	}
}
