package graphstore

import (
	"errors"
	"reflect"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func recordedAt(t *testing.T, axis temporal.Axis, v int64) temporal.Position {
	t.Helper()
	p, err := temporal.IntegerPosition(axis, temporal.Int64(v))
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func evidenceCoordinate(t *testing.T, p temporal.Position) int64 {
	t.Helper()
	i, ok := p.Integer()
	if !ok {
		t.Fatal("evidence position is not an integer")
	}
	n, ok := i.Int64()
	if !ok {
		t.Fatal("evidence coordinate overflows")
	}
	return n
}

func readEvidence(t *testing.T, f *fullFixture, owner graphstate.EntityID, revision uint64) []RecordedEvidence {
	t.Helper()
	v := f.view(t, f.index)
	got, err := v.Evidence(t.Context(), owner, revision)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func TestOperationRecordedEvidence(t *testing.T) {
	knowledge := testAxis(t, 2, temporal.ProfileIntegerZ)
	create := func(f *fullFixture, rec temporal.Position) graphstate.Operation {
		return graphstate.Operation{Kind: graphstate.CreateNode, Owner: 1, Life: 11, Scope: f.span(t, 0, 10), Recorded: rec}
	}
	stage := func(f *fullFixture, l GraphLimits, ops ...graphstate.Operation) (GraphEffects, error) {
		c := f.catalog(t, f.index)
		defer c.view.Close()
		revision, _ := state.NewRevision(f.index+1, f.index+1000)
		return StageOperations(t.Context(), c, ops, revision, l)
	}

	t.Run("future instant refused by limits", func(t *testing.T) {
		l := GraphLimits{RecordedCeiling: recordedAt(t, knowledge, 100)}
		f := newFullFixture(t, l)
		for _, v := range []int64{101, 1 << 40} {
			effects, err := stage(f, l, create(f, recordedAt(t, knowledge, v)))
			if !errors.Is(err, graphstate.ErrInvalidInput) || !reflect.DeepEqual(effects, GraphEffects{}) {
				t.Fatal("future instant accepted", v, err)
			}
		}
		// boundary: exactly the ceiling is not the future
		if _, err := stage(f, l, create(f, recordedAt(t, knowledge, 100))); err != nil {
			t.Fatal(err)
		}
		// ceiling on another axis than the instant
		if _, err := stage(f, l, create(f, recordedAt(t, testAxis(t, 3, temporal.ProfileIntegerZ), 1))); !errors.Is(err, graphstate.ErrInvalidInput) {
			t.Fatal("instant on a third axis accepted", err)
		}
	})

	t.Run("instant on the valid axis refused", func(t *testing.T) {
		f := newFullFixture(t, GraphLimits{})
		effects, err := stage(f, GraphLimits{}, create(f, recordedAt(t, f.axis, 5)))
		if !errors.Is(err, graphstate.ErrInvalidInput) || !reflect.DeepEqual(effects, GraphEffects{}) {
			t.Fatal("valid-axis instant accepted", err)
		}
	})

	t.Run("zero versus unset are distinct", func(t *testing.T) {
		f := newFullFixture(t, GraphLimits{})
		f.apply(t, create(f, temporal.Position{}))
		if got := readEvidence(t, f, 1, f.index); len(got) != 0 {
			t.Fatal("unset Recorded left evidence", got)
		}
		g := newFullFixture(t, GraphLimits{})
		g.apply(t, create(g, recordedAt(t, knowledge, 0)))
		got := readEvidence(t, g, 1, g.index)
		if len(got) != 1 || evidenceCoordinate(t, got[0].Recorded) != 0 || got[0].Kind != graphstate.CreateNode || got[0].Life != 11 {
			t.Fatal("explicit coordinate 0 lost or confused with unset", got)
		}
	})

	t.Run("survives install and read; order kept; other revision empty", func(t *testing.T) {
		f := newFullFixture(t, GraphLimits{})
		scope := f.span(t, 0, 10)
		f.apply(t,
			graphstate.Operation{Kind: graphstate.CreateNode, Owner: 1, Life: 11, Scope: scope, Recorded: recordedAt(t, knowledge, 7)},
			graphstate.Operation{Kind: graphstate.AddLabel, Owner: 1, Life: 11, Scope: scope, Name: "b", Recorded: recordedAt(t, knowledge, 3)},
			graphstate.Operation{Kind: graphstate.AddLabel, Owner: 1, Life: 11, Scope: scope, Name: "a"},
			graphstate.Operation{Kind: graphstate.AddLabel, Owner: 1, Life: 11, Scope: scope, Name: "c", Recorded: recordedAt(t, knowledge, 9)})
		got := readEvidence(t, f, 1, f.index)
		var coords []int64
		var names []string
		for _, e := range got {
			coords = append(coords, evidenceCoordinate(t, e.Recorded))
			names = append(names, e.Name)
		}
		if !reflect.DeepEqual(coords, []int64{7, 3, 9}) || !reflect.DeepEqual(names, []string{"", "b", "c"}) {
			t.Fatal("evidence reordered, merged or dropped", coords, names)
		}
		if other := readEvidence(t, f, 1, f.index+5); len(other) != 0 {
			t.Fatal("evidence leaked to another revision", other)
		}
		if other := readEvidence(t, f, 2, f.index); len(other) != 0 {
			t.Fatal("evidence leaked to another owner", other)
		}
	})

	t.Run("never changes the cut a revision is visible at", func(t *testing.T) {
		plain, with := newFullFixture(t, GraphLimits{}), newFullFixture(t, GraphLimits{})
		ops := func(f *fullFixture, rec temporal.Position) []graphstate.Operation {
			scope := f.span(t, 0, 10)
			return []graphstate.Operation{
				{Kind: graphstate.CreateNode, Owner: 1, Life: 11, Scope: scope, Recorded: rec},
				{Kind: graphstate.Set, Owner: 1, Life: 11, Scope: scope, Name: "ordinary", Value: graphstate.I64(7), ValueID: 99, Recorded: rec},
			}
		}
		a := plain.apply(t, ops(plain, temporal.Position{})...)
		b := with.apply(t, ops(with, recordedAt(t, knowledge, 1<<30))...)
		if !reflect.DeepEqual(a.Delta, b.Delta) || !reflect.DeepEqual(a.Groups, b.Groups) || !reflect.DeepEqual(a.Base, b.Base) {
			t.Fatal("Recorded changed the planned effects")
		}
		if plain.index != with.index {
			t.Fatal("Recorded advanced the cut")
		}
		for _, at := range []int64{-1, 0, 5, 9, 10} {
			p, w := plain.projection(t, plain.index, 1, at, graphstate.Declared), with.projection(t, with.index, 1, at, graphstate.Declared)
			if !reflect.DeepEqual(p, w) {
				t.Fatal("Recorded changed visibility at", at)
			}
		}
		// the view before the revision still sees nothing, with or without evidence
		if before := with.projection(t, with.index-1, 1, 5, graphstate.Declared); before.Exists {
			t.Fatal("evidence made a revision visible at an earlier cut")
		}
	})

	t.Run("one revision cannot be staged twice for an owner", func(t *testing.T) {
		f := newFullFixture(t, GraphLimits{})
		f.apply(t, create(f, recordedAt(t, knowledge, 1)))
		// same revision id again on the same base fails as a duplicate, not by overwriting evidence
		c := f.catalog(t, f.index)
		defer c.view.Close()
		revision, _ := state.NewRevision(f.index, f.index+1000)
		_, err := StageOperations(t.Context(), c, []graphstate.Operation{{Kind: graphstate.AddLabel, Owner: 1, Life: 11, Scope: f.span(t, 0, 10), Name: "x", Recorded: recordedAt(t, knowledge, 2)}}, revision, f.limits)
		if !errors.Is(err, graphstate.ErrAlreadyExists) {
			t.Fatal("duplicate (owner, revision) evidence overwritten", err)
		}
	})
}

func TestOperationRecordedEvidenceCodec(t *testing.T) {
	f := newFullFixture(t, GraphLimits{})
	knowledge := testAxis(t, 2, temporal.ProfileIntegerZ)
	want := []RecordedEvidence{
		{Kind: graphstate.CreateNode, Life: 11, Recorded: recordedAt(t, knowledge, -5)},
		{Kind: graphstate.AddLabel, Life: 11, Name: "lbl", Recorded: recordedAt(t, knowledge, 1<<50)},
	}
	n := f.root.namespace
	wire, err := encodeEvidence(n, 1, 7, want, f.catalog(t, f.index).limits)
	if err != nil {
		t.Fatal(err)
	}
	lim := f.catalog(t, f.index).limits
	resolve := func(id temporal.AxisID) (temporal.Axis, error) { return knowledge, nil }
	got, err := readEvidenceRecord(wire, n, 1, 7, lim, resolve)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatal("round trip", got, err)
	}
	// break cases: truncation at every length, trailing byte, wrong owner, wrong revision, empty list
	for i := range wire {
		if _, err := readEvidenceRecord(wire[:i], n, 1, 7, lim, resolve); err == nil {
			t.Fatal("truncated record accepted at", i)
		}
	}
	if _, err := readEvidenceRecord(append(append([]byte{}, wire...), 0), n, 1, 7, lim, resolve); err == nil {
		t.Fatal("trailing byte accepted")
	}
	if _, err := readEvidenceRecord(wire, n, 2, 7, lim, resolve); err == nil {
		t.Fatal("wrong owner accepted")
	}
	if _, err := readEvidenceRecord(wire, n, 1, 8, lim, resolve); err == nil {
		t.Fatal("wrong revision accepted")
	}
	if _, err := encodeEvidence(n, 1, 7, nil, lim); err == nil {
		t.Fatal("empty evidence encoded")
	}
}

func TestStagePartitionOperationsRecordedEvidence(t *testing.T) {
	store, d, binding, cfg, at := partitionAuthorityFixture(t)
	view := ownershipView(t, store, at)
	all, err := temporal.All(binding.Axis())
	if err != nil {
		t.Fatal(err)
	}
	knowledge := testAxis(t, 9, temporal.ProfileIntegerZ)
	revision, _ := state.NewRevision(1, 23)
	budget := OwnershipBudget{SourceRows: 1024, SourceBytes: 4 << 20, OutputBytes: 16 << 20}
	stage := func(l GraphLimits, rec temporal.Position) (GraphEffects, error) {
		e, _, err := StagePartitionOperations(t.Context(), view, d, binding, cfg, []graphstate.Operation{{Kind: graphstate.CreateNode, Owner: 1, Life: 2, Scope: all, Recorded: rec}}, revision, Limits{}, l, budget)
		return e, err
	}
	onValid, err := temporal.RationalPosition(binding.Axis(), temporal.RationalInt64(1))
	if err != nil {
		t.Fatal(err)
	}
	plain, err := stage(GraphLimits{}, temporal.Position{})
	if err != nil {
		t.Fatal(err)
	}
	with, err := stage(GraphLimits{RecordedCeiling: recordedAt(t, knowledge, 50)}, recordedAt(t, knowledge, 50))
	if err != nil {
		t.Fatal(err)
	}
	if len(with.Writes) <= len(plain.Writes) || !reflect.DeepEqual(plain.Delta, with.Delta) || !reflect.DeepEqual(plain.Groups, with.Groups) {
		t.Fatal("evidence must be extra writes only, with identical planned effects", len(plain.Writes), len(with.Writes))
	}
	for name, c := range map[string]struct {
		l   GraphLimits
		rec temporal.Position
	}{
		"future":     {GraphLimits{RecordedCeiling: recordedAt(t, knowledge, 50)}, recordedAt(t, knowledge, 51)},
		"valid axis": {GraphLimits{}, onValid},
	} {
		if e, err := stage(c.l, c.rec); !errors.Is(err, graphstate.ErrInvalidInput) || !reflect.DeepEqual(e, GraphEffects{}) {
			t.Fatal(name, err)
		}
	}
}

func TestRangeMoveExportRefusesRangeHoldingEvidence(t *testing.T) {
	sources, d, binding, cfg, stores := transactionViewFixture(t)
	actual, _ := sources[0].Root()
	descriptor := rangeMoveTestPlan(t, sources, d, binding, cfg)
	budget := raftlog.ReadBudget{Rows: 1, Bytes: 64 << 10}
	if _, _, err := ExportRangeMovePage(t.Context(), sources[0], descriptor, RangeMoveCursor{}, budget, Limits{}, GraphLimits{}, transactionTestOutput(t)); err != nil {
		t.Fatal("range without evidence must export", err)
	}
	root, err := DecodeRoot(actual.Image)
	if err != nil {
		t.Fatal(err)
	}
	owner := graphstate.EntityID(descriptor.Spec.Publication.First)
	row := raftlog.KV{Key: evidenceKey(root.namespace, owner, 5), Value: []byte("evidence")}
	held := ownershipView(t, stores[0], ownershipInstallRaw(t, stores[0], actual.Image, []raftlog.KV{row}))
	sources[0] = held
	descriptor = rangeMoveTestPlan(t, sources, d, binding, cfg)
	if _, _, err := ExportRangeMovePage(t.Context(), held, descriptor, RangeMoveCursor{}, budget, Limits{}, GraphLimits{}, transactionTestOutput(t)); !errors.Is(err, graphstate.ErrUnsupported) {
		t.Fatal("range holding evidence was exported", err)
	}
}
