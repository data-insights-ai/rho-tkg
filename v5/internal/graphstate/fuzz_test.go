package graphstate

import (
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func FuzzGraphstateDeltaAndHistoryParity(f *testing.F) {
	f.Add([]byte{1, 5, 9, 2, 8, 3})
	f.Add([]byte{0, 0, 15, 15})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 24 {
			return
		}
		v := newFixtureView(t)
		v.defs["x"] = PropertyDefinition{"x", Node, ScalarI64, ScalarCardinality, UniqueNone}
		life := testSpan(t, v.axis, -8, 8)
		commitOps(t, v, 1, Operation{Kind: CreateNode, Owner: 1, Life: 1, Scope: life})
		oracle := map[int64]Scalar{}
		present := map[int64]bool{}
		for i := 0; i+1 < len(data); i += 2 {
			lo := int64(data[i]&15) - 8
			hi := int64(data[i+1]&15) - 8
			scope := testSpan(t, v.axis, lo, hi)
			kind := Set
			if data[i]&128 != 0 {
				kind = Unset
			}
			value := I64(int64(data[i]))
			r, _ := state.NewRevision(uint64(i+2), 0)
			before := v.clone()
			d, err := Plan(t.Context(), v, []Operation{{Kind: kind, Owner: 1, Life: 1, Scope: scope, Name: "x", Value: value, ValueID: ValueID(i + 1)}}, r, Limits{})
			if scope.Kind() == temporal.ScopeEmpty {
				if !errors.Is(err, ErrEmptyMutation) {
					t.Fatal(err)
				}
				continue
			}
			if err != nil {
				t.Fatal(err)
			}
			v.install(t, d)
			for at := int64(-8); at < 8; at++ {
				old, err := Project(t.Context(), before, 1, testPosition(t, v.axis, at), Effective, Limits{})
				if err != nil {
					t.Fatal(err)
				}
				if len(old.Properties) > 0 && !present[at] || len(old.Properties) == 0 && present[at] {
					t.Fatal("past view changed")
				}
				if len(old.Properties) > 0 {
					equal, err := old.Properties[0].Scalar.Equal(oracle[at], Limits{})
					if err != nil || !equal {
						t.Fatal("historical value", err)
					}
				}
				if lo <= at && at < hi {
					present[at] = kind == Set
					oracle[at] = value
				}
				next, err := Project(t.Context(), v, 1, testPosition(t, v.axis, at), Effective, Limits{})
				if err != nil {
					t.Fatal(err)
				}
				if (len(next.Properties) > 0) != present[at] {
					t.Fatal("present/absent diverged")
				}
				if present[at] {
					equal, err := next.Properties[0].Scalar.Equal(oracle[at], Limits{})
					if err != nil || !equal {
						t.Fatal("current value", err)
					}
				}
			}
		}
	})
}
