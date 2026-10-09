package graphstate

import (
	"context"
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func clipPage(t testing.TB, p ComponentPage, owned temporal.Scope) ComponentPage {
	t.Helper()
	s, err := state.New(owned.Axis(), state.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	for _, piece := range p.Data.Pieces() {
		common, err := piece.Scope().Intersection(owned, temporal.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		if common.Kind() == temporal.ScopeEmpty {
			continue
		}
		r, err := applyCell(s, common, piece.Cell(), state.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		s = r.State()
	}
	p.Owned = owned
	p.Data = s
	return p
}
func TestOrderedPagesUseOwnedStateWithoutContextCDC(t *testing.T) {
	v := newFixtureView(t)
	v.pageHook = func(q ComponentQuery, c Cursor, p ComponentPage) ComponentPage {
		if q.MergeContext {
			t.Fatal("unused merge context requested")
		}
		if q.Window.Kind() == temporal.ScopePoint {
			return p
		}
		if c == 0 {
			p = clipPage(t, p, testSpan(t, v.axis, 0, 5))
			p.Complete = false
			p.Next = 1
		} else {
			p = clipPage(t, p, testSpan(t, v.axis, 5, 10))
		}
		return p
	}
	r, _ := state.NewRevision(1, 0)
	d, err := Plan(t.Context(), v, []Operation{{Kind: CreateNode, Owner: 1, Life: 1, Scope: testSpan(t, v.axis, 0, 10)}}, r, Limits{Component: state.Limits{MaxPieces: 1, MaxChangePieces: 1}})
	if err != nil {
		t.Fatal(err)
	}
	if len(d.Patches) != 2 {
		t.Fatal(d.Patches)
	}
	v.install(t, d)
	assertActive(t, v, 1, 2, Effective, true)
	assertActive(t, v, 1, 8, Effective, true)
	for _, mode := range []string{"overlap", "gap", "jump", "cursor cycle", "premature complete"} {
		t.Run(mode, func(t *testing.T) {
			v := newFixtureView(t)
			v.pageHook = func(q ComponentQuery, c Cursor, p ComponentPage) ComponentPage {
				if c == 0 {
					p = clipPage(t, p, testSpan(t, v.axis, 0, 5))
					p.Next = 1
					p.Complete = mode == "premature complete"
					if p.Complete {
						p.Next = 0
					}
					return p
				}
				lo := int64(5)
				if mode == "overlap" {
					lo = 4
				}
				if mode == "gap" || mode == "jump" {
					lo = 6
				}
				p = clipPage(t, p, testSpan(t, v.axis, lo, 10))
				if mode == "cursor cycle" {
					p.Next = 1
					p.Complete = false
				}
				return p
			}
			r, _ := state.NewRevision(1, 0)
			_, err := Plan(t.Context(), v, []Operation{{Kind: CreateNode, Owner: 1, Life: 1, Scope: testSpan(t, v.axis, 0, 10)}}, r, Limits{})
			if !errors.Is(err, ErrContradictoryRead) && !errors.Is(err, ErrIncompleteRead) {
				t.Fatal(err)
			}
		})
	}
}
func TestSparseReadOfVirtualMillionPieceHistory(t *testing.T) {
	v := newFixtureView(t)
	all, _ := temporal.All(v.axis)
	v.defs[ownerSchemaKey{Node, "value"}] = PropertyDefinition{"value", Node, ScalarI64, ScalarCardinality, UniqueNone}
	commitOps(t, v, 1, Operation{Kind: CreateNode, Owner: 1, Life: 1, Scope: all})
	v.values[1] = I64(40)
	const historyPieces = 1000000
	calls := 0
	v.pageHook = func(q ComponentQuery, _ Cursor, p ComponentPage) ComponentPage {
		if q.Key.Kind != ScalarProperty {
			return p
		}
		calls++
		lo, hi, _ := q.Window.Bounds()
		lp, _ := lo.Position()
		hp, _ := hi.Position()
		ln, _ := lp.Integer()
		hn, _ := hp.Integer()
		if n, _ := ln.Int64(); n != 40 {
			t.Fatal("whole-history read", n)
		}
		if n, _ := hn.Int64(); n != 40 || n >= historyPieces {
			t.Fatal(n)
		}
		s, _ := state.New(v.axis, state.Limits{})
		ref, _ := state.NewValueRef(1, 9)
		rev, _ := state.NewRevision(1, 0)
		r, err := s.Set(q.Window, ref, rev, state.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		p.Data = r.State()
		return p
	}
	r, _ := state.NewRevision(2, 0)
	d, err := Plan(t.Context(), v, []Operation{{Kind: Set, Owner: 1, Life: 1, Scope: testSpan(t, v.axis, 40, 41), Name: "value", Value: I64(99), ValueID: 2}}, r, Limits{Component: state.Limits{MaxPieces: 1, MaxChangePieces: 1}})
	if err != nil || calls != 1 || len(d.Patches) != 1 {
		t.Fatal(calls, d, err)
	}
	// The backend derives one requested page from a virtual large history;
	// neither this mock nor the planner materializes a million-piece State.
}
func TestNilBoundaryKindsAndEmptyPredicates(t *testing.T) {
	for _, v := range []any{nil, (*fixtureView)(nil), map[int]int(nil), []int(nil), (func())(nil), (chan int)(nil)} {
		if !nilProvider(v) {
			t.Fatal("nil-able provider missed")
		}
	}
	if nilProvider(newFixtureView(t)) || nilProvider(1) {
		t.Fatal("non-nil provider rejected")
	}
	v := newFixtureView(t)
	v.defs[ownerSchemaKey{Node, "tags"}] = PropertyDefinition{"tags", Node, ScalarString, SetCardinality, UniqueMembers}
	all, _ := temporal.All(v.axis)
	d := commitOps(t, v, 1, Operation{Kind: CreateNode, Owner: 1, Life: 1, Scope: all}, Operation{Kind: Add, Owner: 1, Life: 1, Scope: all, Name: "tags", Value: String("x"), ValueID: 1})
	flags := map[DependencyKind]bool{}
	for _, dep := range d.Dependencies {
		flags[dep.Kind] = true
	}
	for _, kind := range []DependencyKind{EntityDependency, LifeDependency, ValueDependency, ValueIdentityDependency, ComponentDependency, PrefixDependency, UniquenessDependency, IncidentDependency} {
		if !flags[kind] {
			t.Fatal("missing negative/predicate dependency", kind)
		}
	}
	r, _ := state.NewRevision(2, 0)
	if _, err := Plan(t.Context(), v, []Operation{{Kind: Add, Owner: 1, Life: 1, Scope: all, Name: "tags", Value: Null()}}, r, Limits{}); !errors.Is(err, ErrUnsupported) {
		t.Fatal(err)
	}
	if _, err := Project(t.Context(), v, 1, testPosition(t, v.axis, 2), Effective, Limits{MaxDeltaBytes: 32}); !errors.Is(err, ErrResourceLimit) {
		t.Fatal("projection omitted output ledger", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	v.pageHook = func(_ ComponentQuery, _ Cursor, p ComponentPage) ComponentPage { cancel(); return p }
	if _, err := Project(ctx, v, 1, testPosition(t, v.axis, 2), Effective, Limits{}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
