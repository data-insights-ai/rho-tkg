package graphstate

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func TestOutputBudgetDirectAdmissionAndExhaustion(t *testing.T) {
	for _, invalid := range []int{-1, 64<<20 + 1} {
		b, e := NewOutputBudget(invalid)
		if b != nil || !errors.Is(e, ErrInvalidInput) {
			t.Fatal(b, e)
		}
	}
	b, e := NewOutputBudget(4)
	if e != nil {
		t.Fatal(e)
	}
	if e = b.Reserve(3); e != nil || b.Used() != 3 || b.Remaining() != 1 {
		t.Fatal(e, b)
	}
	if e = b.Reserve(2); !errors.Is(e, ErrResourceLimit) || b.Used() != 3 {
		t.Fatal(e, b)
	}
	if e = b.Reserve(-1); !errors.Is(e, ErrInvalidInput) {
		t.Fatal(e)
	}
	if e = b.Reserve(1); e != nil || b.Remaining() != 0 {
		t.Fatal(e, b)
	}
	if e = b.Reserve(1); !errors.Is(e, ErrResourceLimit) {
		t.Fatal(e)
	}
	var nilBudget *OutputBudget
	if !errors.Is(nilBudget.Reserve(0), ErrInvalidInput) || nilBudget.Remaining() != 0 || nilBudget.Used() != 0 {
		t.Fatal("nil budget")
	}
	zero, _ := NewOutputBudget(0)
	if !errors.Is(zero.Reserve(1), ErrResourceLimit) {
		t.Fatal("zero resurrected")
	}
}

func TestOutputBudgetScopeReusePreservesTighterPolicyAndNumericAdmission(t *testing.T) {
	v := newFixtureView(t)
	integer, e := temporal.ParseInteger("18446744073709551617", temporal.Limits{})
	if e != nil {
		t.Fatal(e)
	}
	pos, e := temporal.IntegerPosition(v.axis, integer)
	if e != nil {
		t.Fatal(e)
	}
	wide, e := temporal.Point(pos)
	if e != nil {
		t.Fatal(e)
	}
	scope := testSpan(t, v.axis, 0, 100)
	b, _ := NewOutputBudget(1 << 20)
	wire, e := b.ScopeBytes(scope, temporal.Limits{})
	if e != nil {
		t.Fatal(e)
	}
	tight := temporal.Limits{MaxValueBytes: len(wire) - 1}
	if _, e = b.ScopeBytes(scope, tight); !errors.Is(e, temporal.ErrResourceLimit) {
		t.Fatal(e)
	}
	if e = b.Reserve(b.Remaining()); e != nil {
		t.Fatal(e)
	}
	var attempt error
	allocations := testing.AllocsPerRun(3, func() { _, attempt = b.ScopeBytes(wide, temporal.Limits{}) })
	if !errors.Is(attempt, ErrResourceLimit) || allocations != 0 {
		t.Fatal("numeric work before admission", attempt, allocations)
	}
	control := testing.AllocsPerRun(3, func() { _, attempt = temporal.AppendScope(nil, wide, temporal.Limits{}) })
	if attempt != nil || control == 0 {
		t.Fatal("missing allocating control", attempt, control)
	}
}

type budgetComponentView struct {
	*fixtureView
	data  state.State
	calls int
}

func (v *budgetComponentView) ComponentPage(_ context.Context, q ComponentQuery, _ Cursor, _ ReadBudget) (ComponentPage, error) {
	v.calls++
	if q.Key.Kind != Label {
		return v.fixtureView.ComponentPage(context.Background(), q, 0, ReadBudget{})
	}
	return ComponentPage{View: v.id, Version: 1, Owned: q.Window, Data: v.data, Complete: true}, nil
}

func TestBoundedComponentRejectsPrebuiltPiecesBeforeDefensiveClone(t *testing.T) {
	v := newFixtureView(t)
	data, e := state.New(v.axis, state.Limits{})
	if e != nil {
		t.Fatal(e)
	}
	rev, _ := state.NewRevision(1, 0)
	for i := range 512 {
		point, _ := temporal.Point(testPosition(t, v.axis, int64(i*2)))
		result, e := data.Set(point, state.Null(), rev, state.Limits{})
		if e != nil {
			t.Fatal(e)
		}
		data = result.State()
	}
	provider := &budgetComponentView{fixtureView: v, data: data}
	limits := DefaultLimits()
	limits.MaxRows = 2048
	limits.MaxReadBytes = 1 << 20
	engine, e := start(t.Context(), provider, limits, rev)
	if e != nil {
		t.Fatal(e)
	}
	engine.budget, _ = NewOutputBudget(4096)
	all, _ := temporal.All(v.axis)
	pages, e := engine.component(ComponentKey{Owner: 1, Life: 1, Kind: Label, Name: "x"}, all)
	if !errors.Is(e, ErrResourceLimit) || len(pages) != 0 || provider.calls != 1 {
		t.Fatalf("prebuilt512piece clone admitted: pages=%d calls=%d err=%v used=%d", len(pages), provider.calls, e, engine.budget.Used())
	}
	var attempt error
	allocations := testing.AllocsPerRun(3, func() {
		candidate, err := start(t.Context(), provider, limits, rev)
		if err != nil {
			t.Fatal(err)
		}
		candidate.budget, _ = NewOutputBudget(4096)
		_, attempt = candidate.component(ComponentKey{Owner: 1, Life: 1, Kind: Label, Name: "x"}, all)
	})
	if !errors.Is(attempt, ErrResourceLimit) || allocations > 11 {
		t.Fatal("defensive clone/traversal occurred before admission", attempt, allocations)
	}
	t.Logf("refusal allocations=%g", allocations)
	control := testing.AllocsPerRun(3, func() { _ = data.Pieces() })
	if control == 0 {
		t.Fatal("missing allocating Pieces control")
	}
}

func TestPlanOutputBudgetRejectsLargeScopeBeforeSerialization(t *testing.T) {
	v := newFixtureView(t)
	parts := make([]temporal.Scope, 128)
	for i := range parts {
		parts[i] = testSpan(t, v.axis, int64(i*4), int64(i*4+2))
	}
	scope, e := temporal.Region(v.axis, parts, temporal.Limits{})
	if e != nil {
		t.Fatal(e)
	}
	op := Operation{Kind: CreateNode, Owner: 1, Life: 1, Scope: scope}
	rev, _ := state.NewRevision(1, 0)
	b, _ := NewOutputBudget(0)
	var attempt error
	allocations := testing.AllocsPerRun(3, func() { _, attempt = PlanWithOutputBudget(t.Context(), v, []Operation{op}, rev, Limits{}, b) })
	if !errors.Is(attempt, ErrResourceLimit) || allocations != 0 {
		t.Fatal("serialization before exhausted admission", attempt, allocations)
	}
	positive := testing.AllocsPerRun(3, func() { _, attempt = temporal.AppendScope(nil, scope, temporal.Limits{}) })
	if attempt != nil || positive == 0 {
		t.Fatal("missing allocating serialization control", attempt, positive)
	}
	if _, e = PlanWithOutputBudget(t.Context(), v, nil, rev, Limits{}, nil); !errors.Is(e, ErrInvalidInput) {
		t.Fatal(e)
	}
	if _, e = PlanWithOutputBudget(t.Context(), nil, nil, rev, Limits{}, b); !errors.Is(e, ErrNilView) {
		t.Fatal(e)
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, e = PlanWithOutputBudget(canceled, v, nil, rev, Limits{}, b); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
}

func TestPlanOutputBudgetComponentRetentionAndFittingGuard(t *testing.T) {
	v := newFixtureView(t)
	all, _ := temporal.All(v.axis)
	commitOps(t, v, 1, Operation{Kind: CreateNode, Owner: 1, Life: 1, Scope: all})
	data, err := state.New(v.axis, state.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	rev, _ := state.NewRevision(1, 0)
	for i := range 512 {
		point, _ := temporal.Point(testPosition(t, v.axis, int64(i*2)))
		result, err := data.Set(point, state.Null(), rev, state.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		data = result.State()
	}
	provider := &budgetComponentView{fixtureView: v, data: data}
	limits := DefaultLimits()
	limits.MaxRows = 8192
	limits.MaxReadBytes = 8 << 20
	limits.MaxPages = 8192
	next, _ := state.NewRevision(2, 17)
	operation := []Operation{{Kind: RemoveLabel, Owner: 1, Life: 1, Name: "x", Scope: all}}
	budget, _ := NewOutputBudget(64 << 10)
	rejected, err := PlanWithOutputBudget(t.Context(), provider, operation, next, limits, budget)
	if !errors.Is(err, ErrResourceLimit) || len(rejected.Patches) != 0 {
		t.Fatal("retained component bypassed bounded Plan", err, rejected)
	}
	budget, _ = NewOutputBudget(64 << 20)
	delta, err := PlanWithOutputBudget(t.Context(), provider, operation, next, limits, budget)
	if err != nil || len(delta.Patches) != 1 {
		t.Fatal("fitting retained-page guard", err, len(delta.Patches), budget.Used())
	}
	expected, err := Plan(t.Context(), provider, operation, next, limits)
	if err != nil || !reflect.DeepEqual(delta, expected) {
		t.Fatal("bounded Plan changed the complete accepted Delta", err)
	}
	patch := delta.Patches[0]
	if len(patch.Changes) != 1025 {
		t.Fatal("missing changed support", len(patch.Changes))
	}
	for _, change := range patch.Changes {
		if change.After().Present() || change.After().Revision() != next {
			t.Fatal("incorrect retraction", change)
		}
	}
	old, err := data.At(testPosition(t, v.axis, 0), state.Limits{})
	if err != nil || !old.Present() || old.Revision() != rev {
		t.Fatal("old state changed", old, err)
	}
	now, err := patch.State.At(testPosition(t, v.axis, 0), state.Limits{})
	if err != nil || now.Present() || now.Revision() != next {
		t.Fatal("new retraction missing", now, err)
	}
	t.Logf("fitting512piece Plan used=%d changes=%d", budget.Used(), len(patch.Changes))
}

func TestPlanOutputBudgetScopeBoundaryAfterInitialAdmission(t *testing.T) {
	v := newFixtureView(t)
	parts := make([]temporal.Scope, 128)
	for i := range parts {
		parts[i] = testSpan(t, v.axis, int64(i*4), int64(i*4+2))
	}
	scope, err := temporal.Region(v.axis, parts, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	operations := []Operation{{Kind: CreateNode, Owner: 1, Life: 1, Scope: scope}}
	revision, _ := state.NewRevision(1, 0)
	var attempted error
	allocations := testing.AllocsPerRun(3, func() {
		budget, _ := NewOutputBudget(1536)
		_, attempted = PlanWithOutputBudget(t.Context(), v, operations, revision, Limits{}, budget)
		if budget.Used() != 1536 {
			t.Fatal("initial ledger not admitted", budget.Used())
		}
	})
	if !errors.Is(attempted, ErrResourceLimit) || allocations > 1 {
		t.Fatal("scope serialization before its reservation", attempted, allocations)
	}
	control := testing.AllocsPerRun(3, func() { _, attempted = temporal.AppendScope(nil, scope, temporal.Limits{}) })
	if attempted != nil || control == 0 {
		t.Fatal("missing serialization positive control", attempted, control)
	}
}

func TestBoundedMutationRefusesAtReducerBoundaryBeforeWork(t *testing.T) {
	v := newFixtureView(t)
	all, _ := temporal.All(v.axis)
	revision, _ := state.NewRevision(1, 0)
	data, err := state.New(v.axis, state.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	for i := range 3 {
		point, _ := temporal.Point(testPosition(t, v.axis, int64(i*2)))
		next, err := data.Set(point, state.Null(), revision, state.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		data = next.State()
	}
	provider := &budgetComponentView{fixtureView: v, data: data}
	nextRevision, _ := state.NewRevision(2, 91)
	key := ComponentKey{Owner: 1, Life: 1, Kind: Label, Name: "x"}
	makeEngine := func(allowance int) *engine {
		candidate, err := start(t.Context(), provider, DefaultLimits(), nextRevision)
		if err != nil {
			t.Fatal(err)
		}
		candidate.budget, _ = NewOutputBudget(allowance)
		return candidate
	}
	// Calibration admits the real read-owned page and records its consumption.
	// The public checked128-operation test separately proves the same exhausted
	// allowance is reachable at the embedding reader/stager boundary.
	read := makeEngine(1 << 20)
	if _, err := read.component(key, all); err != nil {
		t.Fatal(err)
	}
	boundary := read.budget.Used() + 256*data.Usage().Pieces()
	candidate := makeEngine(boundary)
	if err := candidate.mutate(key, all, state.ValueRef{}, false); !errors.Is(err, ErrResourceLimit) || len(candidate.delta.Patches) != 0 || candidate.budget.Remaining() != 0 {
		t.Fatal("reducer boundary admitted effects", err, len(candidate.delta.Patches), candidate.budget.Remaining())
	}
	readAllocations := testing.AllocsPerRun(3, func() {
		candidate := makeEngine(boundary)
		_, err := candidate.component(key, all)
		if err != nil {
			t.Fatal(err)
		}
	})
	var attempted error
	refusalAllocations := testing.AllocsPerRun(3, func() {
		candidate := makeEngine(boundary)
		attempted = candidate.mutate(key, all, state.ValueRef{}, false)
	})
	if !errors.Is(attempted, ErrResourceLimit) || refusalAllocations > readAllocations+1 {
		t.Fatal("reducer ran before reservation", attempted, readAllocations, refusalAllocations)
	}
	positive := testing.AllocsPerRun(3, func() { _, attempted = data.Unset(all, nextRevision, state.Limits{}) })
	if attempted != nil || positive == 0 {
		t.Fatal("missing allocating reducer control", attempted, positive)
	}
	fitting := makeEngine(1 << 20)
	if err := fitting.mutate(key, all, state.ValueRef{}, false); err != nil || len(fitting.delta.Patches) != 1 {
		t.Fatal("fitting reducer refused", err, len(fitting.delta.Patches))
	}
	expected, err := data.Unset(all, nextRevision, state.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	patch := fitting.delta.Patches[0]
	if !reflect.DeepEqual(patch.State, expected.State()) || !reflect.DeepEqual(patch.Changes, expected.Changes()) || len(patch.Changes) != 7 {
		t.Fatal("reducer exact support/CDC changed")
	}
	old, err := data.At(testPosition(t, v.axis, 0), state.Limits{})
	if err != nil || !old.Present() || old.Revision() != revision {
		t.Fatal("retained old state changed", old, err)
	}
	t.Logf("boundary=%d readalloc=%g refusalalloc=%g positiveSetalloc=%g fittingused=%d", boundary, readAllocations, refusalAllocations, positive, fitting.budget.Used())
}

func TestOutputBudgetChildPreservesLocalAndSharedParentCaps(t *testing.T) {
	var nilBudget *OutputBudget
	if child, err := nilBudget.Child(0); child != nil || !errors.Is(err, ErrInvalidInput) {
		t.Fatal(child, err)
	}
	parent, _ := NewOutputBudget(1024)
	for _, limit := range []int{-1, 64<<20 + 1} {
		if child, err := parent.Child(limit); child != nil || !errors.Is(err, ErrInvalidInput) || parent.Used() != 0 {
			t.Fatal(child, err, parent.Used())
		}
	}
	child, err := parent.Child(512)
	if err != nil || parent.Used() != 128 {
		t.Fatal(err, parent.Used())
	}
	sibling, err := parent.Child(512)
	if err != nil || parent.Used() != 256 {
		t.Fatal(err, parent.Used())
	}
	if err := child.Reserve(500); err != nil || child.Used() != 500 || parent.Used() != 756 {
		t.Fatal(err, child.Used(), parent.Used())
	}
	before := parent.Used()
	if err := child.Reserve(13); !errors.Is(err, ErrResourceLimit) || child.Used() != 500 || parent.Used() != before {
		t.Fatal("child exhaustion changed ancestor", err, child.Used(), parent.Used())
	}
	if err := sibling.Reserve(269); !errors.Is(err, ErrResourceLimit) || sibling.Used() != 0 || parent.Used() != before {
		t.Fatal("parent exhaustion changed child", err, sibling.Used(), parent.Used())
	}
	if err := sibling.Reserve(268); err != nil || sibling.Remaining() != 0 || child.Remaining() != 0 {
		t.Fatal(err, sibling.Remaining(), child.Remaining())
	}
	if err := parent.Reserve(1); !errors.Is(err, ErrResourceLimit) {
		t.Fatal(err)
	}
	exhausted, _ := NewOutputBudget(0)
	if child, err := exhausted.Child(0); child != nil || !errors.Is(err, ErrResourceLimit) {
		t.Fatal(child, err)
	}
	root, _ := NewOutputBudget(512)
	zero, err := root.Child(0)
	if err != nil {
		t.Fatal(err)
	}
	if err := zero.Reserve(1); !errors.Is(err, ErrResourceLimit) || root.Used() != 128 || zero.Used() != 0 {
		t.Fatal("zero child resurrected", err)
	}
	deeper, err := root.Child(256)
	if err != nil {
		t.Fatal(err)
	}
	grandchild, err := deeper.Child(128)
	if err != nil {
		t.Fatal(err)
	}
	if err := grandchild.Reserve(128); err != nil || root.Used() != 512 || deeper.Used() != 256 || grandchild.Used() != 128 {
		t.Fatal(err, root.Used(), deeper.Used(), grandchild.Used())
	}
}

func TestOutputBudgetComponentCodecsDirectNilLimitsAndAdmission(t *testing.T) {
	var nilBudget *OutputBudget
	if _, err := nilBudget.ReserveStateEncoding(state.State{}, state.CodecLimits{}); !errors.Is(err, ErrInvalidInput) {
		t.Fatal(err)
	}
	if _, err := nilBudget.ReserveChangeEncoding(nil, state.CodecLimits{}); !errors.Is(err, ErrInvalidInput) {
		t.Fatal(err)
	}
	view := newFixtureView(t)
	scope := testSpan(t, view.axis, 0, 10)
	before, err := state.New(view.axis, state.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	revision, _ := state.NewRevision(1, 0)
	result, err := before.Set(scope, state.Null(), revision, state.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	budget, _ := NewOutputBudget(1 << 20)
	admitted, err := budget.ReserveStateEncoding(result.State(), state.CodecLimits{})
	if err != nil || admitted < 56 || budget.Used() < admitted {
		t.Fatal(admitted, err)
	}
	admitted, err = budget.ReserveChangeEncoding(result.Changes(), state.CodecLimits{})
	if err != nil || admitted < 56 {
		t.Fatal(admitted, err)
	}
	if _, err = budget.ReserveChangeEncoding(result.Changes(), state.CodecLimits{MaxEncodedBytes: 1}); !errors.Is(err, state.ErrResourceLimit) {
		t.Fatal(err)
	}
	if _, err = budget.ReserveStateEncoding(result.State(), state.CodecLimits{MaxEncodedBytes: -1}); !errors.Is(err, state.ErrInvalidLimits) {
		t.Fatal(err)
	}
	exhausted, _ := NewOutputBudget(0)
	if _, err = exhausted.ReserveStateEncoding(result.State(), state.CodecLimits{}); !errors.Is(err, ErrResourceLimit) {
		t.Fatal(err)
	}
	if _, err = exhausted.ReserveChangeEncoding(result.Changes(), state.CodecLimits{}); !errors.Is(err, ErrResourceLimit) {
		t.Fatal(err)
	}
}

func TestOutputBudgetEmptyWideMultipartReductionSetUnsetAndNonemptyControl(t *testing.T) {
	for _, profile := range []temporal.Profile{temporal.ProfileRationalQ, temporal.ProfileLexicographicQN} {
		t.Run(fmt.Sprint(profile), func(t *testing.T) {
			axis, err := temporal.NewAxis(temporal.AxisDescriptor{ID: temporal.AxisID{91}, Profile: profile, Version: 1, Reference: "wide-empty-reducer", CanonicalUnit: "step"}, temporal.Limits{})
			if err != nil {
				t.Fatal(err)
			}
			base := new(big.Int).Lsh(big.NewInt(1), 2047)
			integer := func(offset int64) temporal.Integer {
				n, err := temporal.ParseInteger(new(big.Int).Add(base, big.NewInt(offset)).String(), temporal.Limits{})
				if err != nil {
					t.Fatal(err)
				}
				return n
			}
			var parts []temporal.Scope
			for _, offset := range []int64{1, 5} {
				q, err := temporal.Fraction(integer(offset), integer(offset+2), temporal.Limits{})
				if err != nil {
					t.Fatal(err)
				}
				position, err := temporal.RationalPosition(axis, q)
				if profile == temporal.ProfileLexicographicQN {
					position, err = temporal.LexPosition(axis, q, integer(0))
				}
				if err != nil {
					t.Fatal(err)
				}
				point, err := temporal.Point(position)
				if err != nil {
					t.Fatal(err)
				}
				parts = append(parts, point)
			}
			window, err := temporal.Region(axis, parts, temporal.Limits{})
			if err != nil {
				t.Fatal(err)
			}
			empty, err := state.New(axis, state.Limits{})
			if err != nil {
				t.Fatal(err)
			}
			limits := DefaultLimits()
			limits.Component.Temporal.MaxMagnitudeBits = 2048
			revision, _ := state.NewRevision(1, 23)
			for _, present := range []bool{true, false} {
				budget, _ := NewOutputBudget(1 << 20)
				engine := engine{budget: budget, limits: limits}
				prototype, err := empty.Unset(window, revision, limits.Component)
				if present {
					prototype, err = empty.Set(window, state.Null(), revision, limits.Component)
				}
				if err != nil {
					t.Fatal(err)
				}
				cell := prototype.Changes()[0].After()
				// Two mutation Parts, two returned Pieces and two CDC Changes are
				// independently required owners, even before numeric scratch.
				ownerFloor := 2 * (outputScopePartsBytes + outputPieceSlotBytes + outputChangeSlotBytes)
				short, _ := NewOutputBudget(ownerFloor - 1)
				under := engine
				under.budget = short
				refused, err := under.reduce(empty, window, cell)
				if !errors.Is(err, ErrResourceLimit) || !reflect.DeepEqual(refused, state.Result{}) || empty.Usage().Pieces() != 0 {
					t.Fatal("empty reducer bypassed independently required owners", present, ownerFloor, refused, err)
				}
				actual, err := engine.reduce(empty, window, cell)
				if err != nil {
					t.Fatalf("empty two-atom wide Set/Unset charged unreachable old-piece traversal present%v used%d: %v", present, budget.Used(), err)
				}
				want, err := applyCell(empty, window, cell, limits.Component)
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(actual, want) || len(actual.Changes()) != 2 || actual.State().Usage().Pieces() != 2 || empty.Usage().Pieces() != 0 {
					t.Fatal("empty reduction exact support/CDC/old state changed", present, err)
				}
				full, _ := NewOutputBudget(1 << 20)
				general := engine
				general.budget = full
				if _, err := general.reduce(actual.State(), window, cell); !errors.Is(err, ErrResourceLimit) {
					t.Fatal("nonempty state escaped general reservation", err)
				}
			}
		})
	}
}

func TestOutputBudgetCodecsTightOwnersAndMultipartNumericalRefusals(t *testing.T) {
	v := newFixtureView(t)
	before, err := state.New(v.axis, state.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	revision, _ := state.NewRevision(1, 23)
	result, err := before.Set(testSpan(t, v.axis, 0, 2), state.Null(), revision, state.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	for _, bytes := range []int{1, 128, 384, 512} {
		budget, _ := NewOutputBudget(bytes)
		if _, err := budget.ReserveStateEncoding(result.State(), state.CodecLimits{}); !errors.Is(err, ErrResourceLimit) {
			t.Fatal(bytes, err)
		}
		budget, _ = NewOutputBudget(bytes)
		if _, err := budget.ReserveChangeEncoding(result.Changes(), state.CodecLimits{}); !errors.Is(err, ErrResourceLimit) {
			t.Fatal(bytes, err)
		}
	}
	second, err := result.State().Set(testSpan(t, v.axis, 4, 6), state.Null(), revision, state.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	budget, _ := NewOutputBudget(1 << 20)
	if _, err := budget.ReserveStateEncoding(second.State(), state.CodecLimits{State: state.Limits{MaxPieces: 1}}); !errors.Is(err, state.ErrResourceLimit) {
		t.Fatal(err)
	}
	if _, err := budget.ReserveChangeEncoding(second.Changes(), state.CodecLimits{MaxEncodedBytes: 56}); !errors.Is(err, state.ErrResourceLimit) {
		t.Fatal(err)
	}
	if _, err := budget.reserveComponentEncoding([]temporal.Scope{{}}, false, state.CodecLimits{}); !errors.Is(err, temporal.ErrInvalidScope) {
		t.Fatal(err)
	}
	var nilBudget *OutputBudget
	if _, err := nilBudget.reserveComponentEncoding(nil, false, state.CodecLimits{}); !errors.Is(err, ErrInvalidInput) {
		t.Fatal(err)
	}
}

func TestOutputBudgetIncidentAndCandidateCollectionsAdmitMapsAndReturnedSlots(t *testing.T) {
	v := newFixtureView(t)
	scope, err := temporal.All(v.axis)
	if err != nil {
		t.Fatal(err)
	}
	definition := PropertyDefinition{Name: "p", Owner: Node, Type: ScalarI64, Cardinality: ScalarCardinality, Unique: UniqueScalar}
	v.defs[ownerSchemaKey{Node, "p"}] = definition
	revision, _ := state.NewRevision(1, 23)
	ops := []Operation{{Kind: CreateNode, Owner: 1, Life: 11, Scope: scope}, {Kind: CreateNode, Owner: 2, Life: 21, Scope: scope}, {Kind: CreateRelationship, Owner: 3, Life: 31, Scope: scope, Record: EntityRecord{Type: "R", Source: 1, Target: 2, Mode: IdentityReference}}, {Kind: Set, Owner: 1, Life: 11, Name: "p", Value: I64(7), ValueID: 99, Scope: scope}}
	planned, err := Plan(t.Context(), v, ops, revision, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	// A real typed plan supplies the final-overlay records/patches. Copy refusal
	// is independently below the existing512-byte patch-owner slots.
	e, err := start(t.Context(), v, DefaultLimits(), revision)
	if err != nil {
		t.Fatal(err)
	}
	e.delta = planned
	e.budget, _ = NewOutputBudget(512*len(planned.Patches) - 1)
	if err := e.validateUniqueness(); !errors.Is(err, ErrResourceLimit) {
		t.Fatal("patch array copied without admission", err)
	}
	v.install(t, planned)
	old := v.components[ComponentKey{Owner: 1, Life: 11, Kind: ScalarProperty, Name: "p"}]
	newEngine := func(bytes int) *engine {
		e, err := start(t.Context(), v, DefaultLimits(), revision)
		if err != nil {
			t.Fatal(err)
		}
		e.budget, err = NewOutputBudget(bytes)
		if err != nil {
			t.Fatal(err)
		}
		return e
	}
	// All encodes exactly53 bytes. One dependency owns1024 bytes. Seen maps
	// have eight portable64-byte ID slots; the returned ID has a16-byte slot.
	incidentPrefix := 53 + outputDependencySlotBytes
	predicate := IncidentPredicate{Endpoint: 1, Life: 11, Window: scope}
	for _, extra := range []int{8*64 - 1, 8*64 + 16 - 1, 8*64 + 16} {
		e := newEngine(incidentPrefix + extra)
		got, err := e.incident(predicate)
		if extra < 8*64+16 {
			if !errors.Is(err, ErrResourceLimit) || got != nil {
				t.Fatal("incident owner floor", extra, got, err)
			}
		} else if err != nil || !reflect.DeepEqual(got, []EntityID{3}) {
			t.Fatal("complete incident set", got, err)
		}
	}
	// The candidate predicate first owns its dependency, then a second entity
	// dependency grows the array to two slots, retaining its earlier backing.
	// The inline I64 identity additionally owns128 fixed+8*9 wire bytes.
	candidatePrefix := 53 + (128 + 8*9) + outputDependencySlotBytes + 2*outputDependencySlotBytes
	unique := UniquePredicate{Definition: definition, Value: I64(7), Window: scope}
	want := []UniqueClaim{{Owner: 1, Life: 11, Key: ComponentKey{Owner: 1, Life: 11, Kind: ScalarProperty, Name: "p"}}}
	for _, extra := range []int{8*192 - 1, 8*192 + 192 - 1, 8*192 + 192} {
		e := newEngine(candidatePrefix + extra)
		got, err := e.candidates(unique)
		if extra < 8*192+192 {
			if !errors.Is(err, ErrResourceLimit) || got != nil {
				t.Fatal("candidate owner floor", extra, got, err)
			}
		} else if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatal("complete candidate set", got, err)
		}
	}
	if !reflect.DeepEqual(v.components[want[0].Key], old) {
		t.Fatal("refusal changed provider-owned history")
	}
}

func TestOutputBudgetDirectScopeStateSliceMapAndScalarOwnership(t *testing.T) {
	v := newFixtureView(t)
	scope, err := temporal.All(v.axis)
	if err != nil {
		t.Fatal(err)
	}
	base, err := state.New(v.axis, state.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	revision, _ := state.NewRevision(1, 23)
	result, err := base.Set(scope, state.Null(), revision, state.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	data := result.State()
	var nilBudget *OutputBudget
	t.Run("ReserveScope", func(t *testing.T) {
		if !errors.Is(nilBudget.ReserveScope(scope, temporal.Limits{}), ErrInvalidInput) {
			t.Fatal("nil receiver")
		}
		b, _ := NewOutputBudget(1 << 20)
		if err := b.ReserveScope(temporal.Scope{}, temporal.Limits{}); !errors.Is(err, temporal.ErrInvalidScope) || b.Used() != 0 {
			t.Fatal(err, b.Used())
		}
		// All owns one768-byte interval, a128-byte header and four53-byte wires.
		cost := 128 + 768 + 4*53
		for _, limit := range []int{0, cost - 1, cost} {
			b, _ := NewOutputBudget(limit)
			err := b.ReserveScope(scope, temporal.Limits{})
			if limit < cost {
				if !errors.Is(err, ErrResourceLimit) || b.Used() != 0 {
					t.Fatal(limit, err, b.Used())
				}
			} else if err != nil || b.Used() != cost || b.Remaining() != 0 {
				t.Fatal(err, b.Used())
			}
		}
	})
	t.Run("ReserveState", func(t *testing.T) {
		if !errors.Is(nilBudget.ReserveState(data), ErrInvalidInput) {
			t.Fatal("nil receiver")
		}
		// Cached usage plus independent fixed Piece/interval/header allowances.
		cost := 128 + 1024*data.Usage().Pieces() + 4*data.Usage().MetadataBytes()
		for _, limit := range []int{0, 1024, cost - 1, cost} {
			b, _ := NewOutputBudget(limit)
			err := b.ReserveState(data)
			if limit < cost {
				if !errors.Is(err, ErrResourceLimit) || b.Used() != 0 {
					t.Fatal(limit, err, b.Used())
				}
			} else if err != nil || b.Used() != cost || b.Remaining() != 0 {
				t.Fatal(err, b.Used())
			}
		}
	})
	t.Run("ReserveSlice", func(t *testing.T) {
		if _, err := nilBudget.ReserveSlice(0, 0, 64); !errors.Is(err, ErrInvalidInput) {
			t.Fatal(err)
		}
		b, _ := NewOutputBudget(192)
		for _, args := range [][3]int{{-1, 0, 64}, {2, 1, 64}, {0, 0, 0}} {
			if _, err := b.ReserveSlice(args[0], args[1], args[2]); !errors.Is(err, ErrInvalidInput) || b.Used() != 0 {
				t.Fatal(args, err)
			}
		}
		if cap, err := b.ReserveSlice(math.MaxInt, math.MaxInt, 64); !errors.Is(err, ErrResourceLimit) || cap != math.MaxInt || b.Used() != 0 {
			t.Fatal(cap, err)
		}
		cap, err := b.ReserveSlice(0, 0, 64)
		if err != nil || cap != 1 || b.Used() != 64 {
			t.Fatal(cap, err, b.Used())
		}
		cap, err = b.ReserveSlice(0, cap, 64)
		if err != nil || cap != 1 || b.Used() != 64 {
			t.Fatal("existing backing charged again", cap, err)
		}
		cap, err = b.ReserveSlice(1, cap, 64)
		if err != nil || cap != 2 || b.Used() != 192 {
			t.Fatal("replacement forgot retired backing", cap, err, b.Used())
		}
		if _, err := b.ReserveSlice(2, 2, 64); !errors.Is(err, ErrResourceLimit) || b.Used() != 192 {
			t.Fatal(err)
		}
	})
	t.Run("ReserveMap", func(t *testing.T) {
		if _, err := nilBudget.ReserveMap(0, 0, 64); !errors.Is(err, ErrInvalidInput) {
			t.Fatal(err)
		}
		b, _ := NewOutputBudget(8*64 + 16*64)
		for _, args := range [][3]int{{-1, 0, 64}, {0, -1, 64}, {0, 0, 0}} {
			if _, err := b.ReserveMap(args[0], args[1], args[2]); !errors.Is(err, ErrInvalidInput) || b.Used() != 0 {
				t.Fatal(args, err)
			}
		}
		if cap, err := b.ReserveMap(math.MaxInt, math.MaxInt, 64); !errors.Is(err, ErrResourceLimit) || cap != math.MaxInt || b.Used() != 0 {
			t.Fatal(cap, err)
		}
		cap, err := b.ReserveMap(0, 0, 64)
		if err != nil || cap != 8 || b.Used() != 8*64 {
			t.Fatal(cap, err)
		}
		cap, err = b.ReserveMap(6, cap, 64)
		if err != nil || cap != 8 || b.Used() != 8*64 {
			t.Fatal("existing slots charged again", cap, err)
		}
		cap, err = b.ReserveMap(7, cap, 64)
		if err != nil || cap != 16 || b.Used() != 8*64+16*64 {
			t.Fatal("replacement forgot retired slots", cap, err)
		}
		if _, err := b.ReserveMap(14, cap, 64); !errors.Is(err, ErrResourceLimit) {
			t.Fatal(err)
		}
	})
	t.Run("ScalarKey", func(t *testing.T) {
		if _, err := nilBudget.ScalarKey(I64(7), Limits{}); !errors.Is(err, ErrInvalidInput) {
			t.Fatal(err)
		}
		b, _ := NewOutputBudget(200)
		if _, err := b.ScalarKey(Scalar{}, Limits{}); !errors.Is(err, ErrInvalidInput) || b.Used() != 0 {
			t.Fatal(err)
		}
		if _, err := b.ScalarKey(I64(7), Limits{MaxOperations: -1}); !errors.Is(err, ErrInvalidInput) || b.Used() != 0 {
			t.Fatal(err)
		}
		if _, err := b.ScalarKey(String("too-long"), Limits{MaxReadBytes: 1}); !errors.Is(err, ErrResourceLimit) || b.Used() != 0 {
			t.Fatal(err)
		}
		want, err := I64(7).EqualityKey(Limits{})
		if err != nil {
			t.Fatal(err)
		}
		// I64 has a9-byte canonical wire;128 fixed+eight copies is200 bytes.
		for _, limit := range []int{0, 199, 200} {
			b, _ := NewOutputBudget(limit)
			key, err := b.ScalarKey(I64(7), Limits{})
			if limit < 200 {
				if !errors.Is(err, ErrResourceLimit) || key != "" || b.Used() != 0 {
					t.Fatal(limit, key, err)
				}
			} else if err != nil || key != want || b.Used() != 200 {
				t.Fatal(key, want, err, b.Used())
			}
		}
	})
}
