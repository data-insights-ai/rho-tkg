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

func TestValueIdentityCrossKind(t *testing.T) {
	db, root := newStore(t, vfs.NewMem())
	c := openCatalog(t, db, 1, Limits{})
	s := stage(t, c)
	if err := s.Value(t.Context(), refValue(1), f64Value(t, 3.0)); err != nil {
		t.Fatal(err)
	}
	if err := s.Value(t.Context(), refValue(2), graphstate.I64(7)); err != nil {
		t.Fatal(err)
	}
	if err := s.Value(t.Context(), refValue(3), f64Value(t, 2.5)); err != nil {
		t.Fatal(err)
	}
	root, index := commitStage(t, db, root, s)
	cat := openCatalog(t, db, index, Limits{})

	// stored F64(3.0) found by I64(3)
	e, found, err := cat.LookupLocalValueIdentity(t.Context(), graphstate.I64(3))
	if err != nil || !found || e.Ref.ID != 1 {
		t.Fatal(e, found, err)
	}
	// ... and the stored value keeps its kind
	v, found, err := cat.Value(t.Context(), refValue(1))
	if err != nil || !found || v.Value.Kind() != graphstate.ScalarF64 {
		t.Fatal(v, found, err)
	}
	// stored I64(7) found by F64(7.0), keeps I64
	e, found, err = cat.LookupLocalValueIdentity(t.Context(), f64Value(t, 7.0))
	if err != nil || !found || e.Ref.ID != 2 {
		t.Fatal(e, found, err)
	}
	if v, _, _ := cat.Value(t.Context(), refValue(2)); v.Value.Kind() != graphstate.ScalarI64 {
		t.Fatal(v.Value.Kind())
	}
	// non-equal numbers are not found
	for _, miss := range []graphstate.Scalar{graphstate.I64(2), graphstate.I64(4), f64Value(t, 3.5), graphstate.String("3"), graphstate.Bool(true)} {
		if _, found, err := cat.LookupLocalValueIdentity(t.Context(), miss); err != nil || found {
			t.Fatal(miss.Render(), found, err)
		}
	}

	// a second writer interning I64(3) and F64(3.0) gets the existing identity
	next := stage(t, cat)
	got, err := next.InternLocal(t.Context(), refValue(10), graphstate.I64(3))
	if err != nil || got != refValue(1) {
		t.Fatal(got, err)
	}
	got, err = next.InternLocal(t.Context(), refValue(11), f64Value(t, 7.0))
	if err != nil || got != refValue(2) {
		t.Fatal(got, err)
	}
}

func TestValueIdentityTwoWritersOneIdentity(t *testing.T) {
	db, _ := newStore(t, vfs.NewMem())
	c := openCatalog(t, db, 1, Limits{})
	s := stage(t, c)
	a, err := s.InternLocal(t.Context(), refValue(1), f64Value(t, 3.0))
	if err != nil {
		t.Fatal(err)
	}
	b, err := s.InternLocal(t.Context(), refValue(2), graphstate.I64(3))
	if err != nil || a != b {
		t.Fatal(a, b, err)
	}
}
