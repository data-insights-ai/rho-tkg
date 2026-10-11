package graphapply

import (
	"context"
	"crypto/sha256"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func TestSharedStageThenComposerRefusesConsumedAllowanceWithoutReset(t *testing.T) {
	f := readyGraphFixture(t)
	request := graphCodecRequest(t)
	wire, err := encodeGraphRequest(request, f.m.limits)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeGraphRequest(wire, f.m.limits)
	if err != nil {
		t.Fatal(err)
	}
	index, _, err := f.s.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	view, err := f.s.ApplicationView(index)
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	base, err := view.Root()
	if err != nil {
		t.Fatal(err)
	}
	catalog, root, err := f.m.catalog(view)
	if err != nil {
		t.Fatal(err)
	}
	guard, err := graphstore.NewSemanticGuard(root)
	if err != nil {
		t.Fatal(err)
	}
	retainedInput := compositionMetadataBytes + 4096 + graphRequestMetadataBytes + 640*cap(decoded.operations) + 128*cap(decoded.claims) + 8*len(wire) + 4*cap(base.Image)
	for _, guarded := range []bool{false, true} {
		t.Run(map[bool]string{false: "unconditional", true: "guarded"}[guarded], func(t *testing.T) {
			stage := func(parent *graphstate.OutputBudget) (graphstore.GraphEffects, error) {
				if err := parent.Reserve(retainedInput); err != nil {
					return graphstore.GraphEffects{}, err
				}
				if guarded {
					effects, _, err := graphstore.StageGuardedOperationsWithOutputBudget(t.Context(), catalog, guard, decoded.operations, decoded.revision, f.m.limits.graph, parent)
					return effects, err
				}
				effects, _, err := graphstore.StageOperationsWithOutputBudget(t.Context(), catalog, decoded.operations, decoded.revision, f.m.limits.graph, parent)
				return effects, err
			}
			calibration, _ := graphstate.NewOutputBudget(f.m.limits.outputBytes)
			expectedEffects, err := stage(calibration)
			if err != nil {
				t.Fatal("real stage must fit", err)
			}
			consumed := calibration.Used()
			parent, _ := graphstate.NewOutputBudget(consumed + 1)
			effects, err := stage(parent)
			if err != nil || parent.Remaining() != 1 || !reflect.DeepEqual(effects, expectedEffects) {
				t.Fatal("real checked stage did not reach exhausted composer boundary", err, parent.Used(), consumed)
			}
			changes := graphChanges{ns: f.n, entities: effects.Delta.Entities, lives: effects.Delta.Lives, values: effects.Delta.Values, groups: effects.Groups}
			outcome := outcome{ns: f.n, kind: graphOperations, identity: decoded.identity(), index: index + 1, disposition: applied}
			machine := *f.m
			machine.limits.outputBytes = consumed + 1
			reader := reader{arena: parent, ctx: t.Context(), view: view, ns: f.n, base: base, limits: machine.limits.allocation, stageBytes: 128}
			batch, err := machine.finish(&reader, outcome, sha256.Sum256(wire), true, f.s.ApplicationBudget(), &effects, changes)
			if !errors.Is(err, errLimit) || len(batch.Writes)+len(batch.Image)+len(batch.Changes)+len(batch.Outcome) != 0 {
				t.Fatalf("composer reset stage consumption: consumed=%d remaining=%d batchwrites=%d err=%v", consumed, parent.Remaining(), len(batch.Writes), err)
			}
			fitting, _ := graphstate.NewOutputBudget(f.m.limits.outputBytes)
			effects, err = stage(fitting)
			if err != nil {
				t.Fatal(err)
			}
			reader = readerTypeForBudgetTest(fitting, view, base, f)
			batch, err = f.m.finish(&reader, outcome, sha256.Sum256(wire), true, f.s.ApplicationBudget(), &effects, changes)
			if err != nil || len(batch.Writes) == 0 || len(batch.Changes) == 0 {
				t.Fatal("fitting stage+composition guard", err)
			}
			original, err := f.stage(t, wire)
			if err != nil || !reflect.DeepEqual(original, batch) {
				t.Fatal("shared allowance changed exact legacy effects", err)
			}
		})
	}
}
func readerTypeForBudgetTest(b *graphstate.OutputBudget, v *raftlog.ApplicationView, base raftlog.ApplicationRoot, f *graphFixture) reader {
	return reader{arena: b, ctx: context.Background(), view: v, ns: f.n, base: base, limits: f.m.limits.allocation, stageBytes: 128}
}

func TestGraphOutputJoinAdmitsDecoderOwnedCompactRegionBacking(t *testing.T) {
	limits := defaultMaterializerLimits()
	request := graphCodecRequest(t)
	axis := request.operations[0].Scope.Axis()
	parts := make([]temporal.Scope, 128)
	for i := range parts {
		position, err := temporal.RationalPosition(axis, temporal.RationalInt64(int64(2*i)))
		if err != nil {
			t.Fatal(err)
		}
		parts[i], err = temporal.Point(position)
		if err != nil {
			t.Fatal(err)
		}
	}
	region, err := temporal.Region(axis, parts, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	request.operations = request.operations[:1]
	request.operations[0].Scope = region
	request.claims = request.claims[:2]
	for _, typed := range []bool{false, true} {
		t.Run(map[bool]string{false: "GR2", true: "partition"}[typed], func(t *testing.T) {
			wire, err := encodeTypedGraphRequest(request, limits, typed)
			if err != nil {
				t.Fatal(err)
			}
			decoderOwned := 0
			decoded, err := decodeTypedGraphRequestOwned(wire, limits, typed, &decoderOwned)
			if err != nil {
				t.Fatal(err)
			}
			q := reader{ctx: t.Context()}
			if err := joinGraphOutputBudget(&q, decoded, decoderOwned, limits); err != nil {
				t.Fatal(err)
			}
			minimumBacking := operationMetadataBytes + 768*len(parts)
			if q.arena.Used() < minimumBacking {
				t.Fatalf("decoder interval ownership disappeared at join: wire=%d atoms=%d minimum=%d joined=%d", len(wire), len(parts), minimumBacking, q.arena.Used())
			}
		})
	}
}
func TestSharedComposerWideNumericAdmissionPrecedesLogicalEncoding(t *testing.T) {
	f := readyGraphFixture(t)
	request := graphCodecRequest(t)
	numerator, err := temporal.ParseInteger(strings.Repeat("9", 1000), temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	denominator, err := temporal.ParseInteger(strings.Repeat("1", 999), temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	rational, err := temporal.Fraction(numerator, denominator, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	position, err := temporal.RationalPosition(request.operations[0].Scope.Axis(), rational)
	if err != nil {
		t.Fatal(err)
	}
	point, err := temporal.Point(position)
	if err != nil {
		t.Fatal(err)
	}
	request.operations = request.operations[:1]
	request.operations[0].Scope = point
	request.claims = request.claims[:2]
	wire, err := encodeGraphRequest(request, f.m.limits)
	if err != nil {
		t.Fatal(err)
	}
	index, _, err := f.s.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	view, err := f.s.ApplicationView(index)
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	base, err := view.Root()
	if err != nil {
		t.Fatal(err)
	}
	catalog, _, err := f.m.catalog(view)
	if err != nil {
		t.Fatal(err)
	}
	stage := func(parent *graphstate.OutputBudget) (graphstore.GraphEffects, error) {
		effects, _, err := graphstore.StageOperationsWithOutputBudget(t.Context(), catalog, request.operations, request.revision, f.m.limits.graph, parent)
		return effects, err
	}
	calibration, _ := graphstate.NewOutputBudget(f.m.limits.outputBytes)
	effects, err := stage(calibration)
	if err != nil {
		t.Fatal("wide point stage guard", err)
	}
	consumption := calibration.Used()
	tight, _ := graphstate.NewOutputBudget(consumption + (64 << 10))
	effects, err = stage(tight)
	if err != nil {
		t.Fatal(err)
	}
	changes := graphChanges{ns: f.n, entities: effects.Delta.Entities, lives: effects.Delta.Lives, values: effects.Delta.Values, groups: effects.Groups}
	outcome := outcome{ns: f.n, kind: graphOperations, identity: request.identity(), index: index + 1, disposition: applied}
	q := readerTypeForBudgetTest(tight, view, base, f)
	batch, err := f.m.finish(&q, outcome, sha256.Sum256(wire), true, f.s.ApplicationBudget(), &effects, changes)
	if !errors.Is(err, errLimit) || len(batch.Writes)+len(batch.Changes)+len(batch.Outcome)+len(batch.Image) != 0 {
		t.Fatalf("wide numeric codec bypassed remaining allowance: stageused=%d remaining=%d writes=%d err=%v", consumption, tight.Remaining(), len(batch.Writes), err)
	}
	// Prepare actual staged operations before measuring composer-only work.
	// Four parents cover AllocsPerRun's warmup+three measured invocations.
	var parents [4]*graphstate.OutputBudget
	var staged [4]graphstore.GraphEffects
	for i := range parents {
		parents[i], _ = graphstate.NewOutputBudget(consumption + (64 << 10))
		staged[i], err = stage(parents[i])
		if err != nil {
			t.Fatal(err)
		}
	}
	attempt := 0
	allocations := testing.AllocsPerRun(3, func() {
		owner := parents[attempt]
		effects := staged[attempt]
		attempt++
		reader := readerTypeForBudgetTest(owner, view, base, f)
		_, err = f.m.finish(&reader, outcome, sha256.Sum256(wire), true, f.s.ApplicationBudget(), &effects, changes)
	})
	if !errors.Is(err, errLimit) || allocations > 25 {
		t.Fatal("numeric codec ran before preadmission", err, allocations)
	}
	t.Logf("wide composer refusal allocations=%g", allocations)
	fitting, _ := graphstate.NewOutputBudget(f.m.limits.outputBytes)
	effects, err = stage(fitting)
	if err != nil {
		t.Fatal(err)
	}
	q = readerTypeForBudgetTest(fitting, view, base, f)
	actual, err := f.m.finish(&q, outcome, sha256.Sum256(wire), true, f.s.ApplicationBudget(), &effects, changes)
	if err != nil {
		t.Fatal("wide numeric fitting full-composer guard", err)
	}
	original, err := f.stage(t, wire)
	if err != nil || !reflect.DeepEqual(actual, original) {
		t.Fatal("wide fitting composer changed complete batch semantics", err)
	}

}

func TestSuppliedGraphBudgetHelpersDirectNilAndLocalCeiling(t *testing.T) {
	f := readyGraphFixture(t)
	request := graphCodecRequest(t)
	index, _, err := f.s.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	view, err := f.s.ApplicationView(index)
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	catalog, root, err := f.m.catalog(view)
	if err != nil {
		t.Fatal(err)
	}
	guard, err := graphstore.NewSemanticGuard(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := graphstore.StageOperationsWithOutputBudget(t.Context(), catalog, request.operations, request.revision, f.m.limits.graph, nil); !errors.Is(err, graphstore.ErrInvalid) {
		t.Fatal(err)
	}
	if _, _, err := graphstore.StageGuardedOperationsWithOutputBudget(t.Context(), catalog, guard, request.operations, request.revision, f.m.limits.graph, nil); !errors.Is(err, graphstore.ErrInvalid) {
		t.Fatal(err)
	}
	parent, _ := graphstate.NewOutputBudget(32 << 20)
	if _, _, err := graphstore.StageOperationsWithOutputBudget(t.Context(), nil, request.operations, request.revision, f.m.limits.graph, parent); !errors.Is(err, graphstore.ErrInvalid) {
		t.Fatal(err)
	}
	// A table-driven nil input is intentional API-boundary refusal coverage.
	for _, ctx := range []context.Context{nil} {
		if _, _, err := graphstore.StageOperationsWithOutputBudget(ctx, catalog, request.operations, request.revision, f.m.limits.graph, parent); !errors.Is(err, graphstore.ErrInvalid) {
			t.Fatal(err)
		}
	}
	zero, _ := graphstate.NewOutputBudget(0)
	if _, _, err := graphstore.StageGuardedOperationsWithOutputBudget(t.Context(), catalog, guard, request.operations, request.revision, f.m.limits.graph, zero); !errors.Is(err, graphstore.ErrResourceLimit) {
		t.Fatal(err)
	}
	local := f.m.limits.graph
	local.MaxOutputBytes = 1024
	if effects, _, err := graphstore.StageOperationsWithOutputBudget(t.Context(), catalog, request.operations, request.revision, local, parent); !errors.Is(err, graphstore.ErrResourceLimit) || len(effects.Writes) != 0 || parent.Remaining() < 31<<20 {
		t.Fatal("outer32MiB widened graph1024B cap", err, parent.Used())
	}
	if _, err := view.Root(); err != nil {
		t.Fatal("refusal poisoned borrowed view", err)
	}
}

func TestSharedComposerDirectInvalidAndFiniteCodecAdmission(t *testing.T) {
	if err := outputBudgetFailure(nil); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(outputBudgetFailure(graphstate.ErrInvalidInput), errInvalid) {
		t.Fatal("invalid cause erased")
	}
	q := &reader{arena: new(graphstate.OutputBudget)}
	if err := joinGraphOutputBudget(q, graphRequest{}, 0, defaultMaterializerLimits()); !errors.Is(err, errInvalid) {
		t.Fatal("existing budget reset", err)
	}
	for _, limit := range []int{1, 65 << 20} {
		l := defaultMaterializerLimits()
		l.outputBytes = limit
		q := &reader{}
		err := joinGraphOutputBudget(q, graphRequest{}, 4096, l)
		want := errLimit
		if limit > 64<<20 {
			want = errInvalid
		}
		if !errors.Is(err, want) || q.arena != nil {
			t.Fatal(limit, err)
		}
	}
	f := readyGraphFixture(t)
	request := graphCodecRequest(t)
	q = &reader{}
	if err := reserveLogicalEncoding(q, graphChanges{}, f.m.limits); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"schema", "entity", "value", "scope"} {
		arena, _ := graphstate.NewOutputBudget(256)
		q = &reader{arena: arena}
		changes := graphChanges{}
		switch kind {
		case "schema":
			changes.schemas = []graphstate.PropertyDefinition{{Name: strings.Repeat("s", 257)}}
		case "entity":
			changes.entities = []graphstate.EntityRecord{{Axis: request.operations[0].Scope.Axis(), Type: strings.Repeat("r", 257)}}
		case "value":
			changes.values = []graphstate.ValueWrite{{ID: 1, Value: graphstate.String(strings.Repeat("v", 257))}}
		case "scope":
			changes.values = []graphstate.ValueWrite{{ID: 1, Value: graphstate.ScopeValue(request.operations[0].Scope)}}
		}
		if err := reserveLogicalEncoding(q, changes, f.m.limits); !errors.Is(err, errLimit) {
			t.Fatal(kind, err)
		}
	}
}
