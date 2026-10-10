package graphstate

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
)

func TestV1QualifiedPlanAndProjectRefuseForeignNamespace(t *testing.T) {
	v := newFixtureView(t)
	scope := testSpan(t, v.axis, 0, 10)
	rev, _ := state.NewRevision(1, 0)
	ops := []Operation{{Kind: CreateNode, Owner: 1, Life: 1, Scope: scope}}
	for _, graph := range []GraphID{{}, {9}} {
		want := ErrNamespace
		if graph == (GraphID{}) {
			want = ErrInvalidInput
		}
		d, err := PlanQualified(t.Context(), v, graph, ops, rev, Limits{})
		if !errors.Is(err, want) || !reflect.DeepEqual(d, Delta{}) {
			t.Fatal(d, err)
		}
		p, err := ProjectQualified(t.Context(), v, graph, 1, testPosition(t, v.axis, 1), Effective, Limits{})
		if !errors.Is(err, want) || !reflect.DeepEqual(p, Projection{}) {
			t.Fatal(p, err)
		}
	}
	d, err := PlanQualified(t.Context(), v, v.Graph(), ops, rev, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	v.install(t, d)
	old := v.clone()
	commitOps(t, v, 2, Operation{Kind: Close, Owner: 1, Life: 1, Scope: scope})
	for _, tc := range []struct {
		view   *fixtureView
		active bool
	}{{old, true}, {v, false}} {
		p, err := ProjectQualified(t.Context(), tc.view, v.Graph(), 1, testPosition(t, v.axis, 1), Effective, Limits{})
		if err != nil || p.Active != tc.active || !p.Exists {
			t.Fatal(p, err)
		}
	}
	//nolint:staticcheck // SA1012: exercise the nil-context error contract.
	if _, err := PlanQualified(nil, v, v.Graph(), ops, rev, Limits{}); !errors.Is(err, ErrInvalidInput) {
		t.Fatal(err)
	}
	if _, err := ProjectQualified(t.Context(), nil, v.Graph(), 1, testPosition(t, v.axis, 1), Effective, Limits{}); !errors.Is(err, ErrNilView) {
		t.Fatal(err)
	}
	if _, err := PlanQualified(t.Context(), v, v.Graph(), ops, rev, Limits{MaxRows: -1}); !errors.Is(err, ErrInvalidInput) {
		t.Fatal(err)
	}
}

type v1MovingGraph struct {
	*fixtureView
	calls int
}

func (v *v1MovingGraph) Graph() GraphID {
	v.calls++
	if v.calls <= 3 {
		return GraphID{1}
	}
	return GraphID{9}
}
func TestV1QualifiedPlanNeverReturnsAnotherNamespaceAfterProviderChange(t *testing.T) {
	fixture := newFixtureView(t)
	v := &v1MovingGraph{fixtureView: fixture}
	revision, _ := state.NewRevision(1, 0)
	delta, err := PlanQualified(t.Context(), v, GraphID{1}, nil, revision, Limits{})
	if !errors.Is(err, ErrInvalidView) || !reflect.DeepEqual(delta, Delta{}) {
		t.Fatal(delta, err)
	}
}

// The provider returns a genuine delegated source failure, rather than making
// namespace admission itself fail. The error must survive the public wrapper.
type v1QualifiedFailureView struct {
	*fixtureView
	failure     error
	entityCalls int
}

func (v *v1QualifiedFailureView) Entity(context.Context, EntityID) (EntityRead, error) {
	v.entityCalls++
	return EntityRead{}, v.failure
}
func TestV1ProjectQualifiedPreservesDelegatedFailureAndRejectsChangedNamespace(t *testing.T) {
	fixture := newFixtureView(t)
	cause := errors.New("qualified source unavailable")
	failing := &v1QualifiedFailureView{fixtureView: fixture, failure: cause}
	p, err := ProjectQualified(t.Context(), failing, fixture.Graph(), 1, testPosition(t, fixture.axis, 1), Effective, Limits{})
	if !errors.Is(err, cause) || !reflect.DeepEqual(p, Projection{}) || failing.entityCalls != 1 {
		t.Fatal(p, err, failing.entityCalls)
	}
	// This provider passes initial graph1 admission, then presents a stable graph9
	// during the delegate. The wrapper must discard even a completed absence answer.
	moving := &v1MovingGraph{fixtureView: fixture}
	p, err = ProjectQualified(t.Context(), moving, GraphID{1}, 1, testPosition(t, fixture.axis, 1), Effective, Limits{})
	if !errors.Is(err, ErrInvalidView) || !reflect.DeepEqual(p, Projection{}) || moving.calls < 4 {
		t.Fatal(p, err, moving.calls)
	}
}
