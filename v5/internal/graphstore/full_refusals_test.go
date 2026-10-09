package graphstore

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func TestFullCandidatesEmptySchemaBudgetAndLateCancellation(t *testing.T) {
	f := newFullFixture(t, GraphLimits{})
	w := f.span(t, 0, 10)
	f.apply(t, graphstate.Operation{Kind: graphstate.CreateNode, Owner: 1, Life: 11, Scope: w}, graphstate.Operation{Kind: graphstate.CreateNode, Owner: 2, Life: 11, Scope: w})
	for _, id := range []graphstate.EntityID{3, 4, 5} {
		f.apply(t, graphstate.Operation{Kind: graphstate.CreateRelationship, Owner: id, Life: 31, Scope: w, Record: graphstate.EntityRecord{Type: "R", Source: 1, Target: 2, Mode: graphstate.LifeBound}, Binding: graphstate.LifeRecord{SourceLife: 11, TargetLife: 11}})
	}
	empty, _ := temporal.Empty(f.axis)
	predicate := graphstate.IncidentPredicate{Endpoint: 1, Life: 11, Window: w}
	v := f.view(t, f.index)
	v.limits.MaxOutputBytes = 136
	first, err := v.IncidentRelationships(t.Context(), predicate, 0, graphstate.ReadBudget{Rows: 512, Bytes: 4 << 20})
	if err != nil || first.Next == 0 || first.Complete || !reflect.DeepEqual(first.Entities, []graphstate.EntityID{3}) {
		t.Fatal("incident output-bound page failed to advance exactly", first, err)
	}
	v.limits.MaxOutputBytes = 4 << 20
	saved := v.cursors[first.Next]
	other := predicate
	other.Life++
	if _, err := v.IncidentRelationships(t.Context(), other, first.Next, graphstate.ReadBudget{Rows: 512, Bytes: 1024}); !errors.Is(err, ErrInvalid) || v.cursors[first.Next] != saved {
		t.Fatal("incident continuation changed predicate", err)
	}
	if _, err := v.IncidentRelationships(t.Context(), predicate, first.Next, graphstate.ReadBudget{Rows: 512, Bytes: 128}); !errors.Is(err, ErrResourceLimit) || v.cursors[first.Next] != saved {
		t.Fatal("failed incident call consumed continuation", err)
	}
	rest := fullReadIncidents(t, v, predicate, graphstate.ReadBudget{Rows: 512, Bytes: 4 << 20})
	if !reflect.DeepEqual(rest, []graphstate.EntityID{3, 4, 5}) {
		t.Fatal("parallel relationships omitted/collapsed", rest)
	}
	for _, call := range []func(*ReadView, context.Context) error{
		func(v *ReadView, ctx context.Context) error {
			_, e := v.IncidentRelationships(ctx, graphstate.IncidentPredicate{Endpoint: 1, Window: empty}, 0, graphstate.ReadBudget{Rows: 512, Bytes: 1024})
			return e
		},
		func(v *ReadView, ctx context.Context) error {
			_, e := v.UniqueCandidates(ctx, graphstate.UniquePredicate{Definition: fullSchemas()[0], Value: graphstate.Null(), Window: w}, 0, graphstate.ReadBudget{Rows: 512, Bytes: 4 << 20})
			return e
		},
	} {
		probe := f.view(t, f.index)
		counter := &fullCancelContext{Context: t.Context()}
		if err := call(probe, counter); err != nil {
			t.Fatal(err)
		}
		_ = probe.Close()
		_ = probe.c.view.Close()
		reader := f.view(t, f.index)
		before := reader.Work()
		if err := call(reader, &fullCancelContext{Context: t.Context(), cancelAt: counter.calls}); !errors.Is(err, context.Canceled) || reader.outputBytes != 0 || reader.Work() == before || len(reader.cursors) != 0 {
			t.Fatal("empty candidate late cancellation published output", err)
		}
		_ = reader.Close()
		_ = reader.c.view.Close()
	}
	wrong := fullSchemas()[0]
	wrong.Name = "undeclared"
	if _, err := v.UniqueCandidates(t.Context(), graphstate.UniquePredicate{Definition: wrong, Value: graphstate.I64(7), Window: w}, 0, graphstate.ReadBudget{Rows: 512, Bytes: 4 << 20}); !errors.Is(err, graphstate.ErrSchemaMismatch) {
		t.Fatal(err)
	}
	if _, err := v.IncidentRelationships(t.Context(), graphstate.IncidentPredicate{}, 0, graphstate.ReadBudget{Rows: 512, Bytes: 1024}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	_ = v.Close()
}

func TestFullPrivateDescriptorAndOperationAuthoritiesRefuseInvalidState(t *testing.T) {
	f := newFullFixture(t, GraphLimits{})
	c := f.catalog(t, f.index)
	v, err := OpenReadView(t.Context(), c, GraphLimits{})
	if err != nil {
		t.Fatal(err)
	}
	d := v.descriptor
	for _, invalid := range []fullIndexDescriptor{func() fullIndexDescriptor { x := d; x.unique.id = x.keys.id; return x }(), func() fullIndexDescriptor { x := d; x.unique.id = 0; return x }(), func() fullIndexDescriptor { x := d; x.declared.level = 8; return x }()} {
		if _, err := encodeFullDescriptor(invalid, c.root, c.limits); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
		if _, err := newFullStage(t.Context(), c, invalid, DefaultPageLimits()); !errors.Is(err, ErrTopologyUnsupported) {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := newFullStage(ctx, c, d, DefaultPageLimits()); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	c.limits.MaxStages = 1
	held, err := newFullStage(t.Context(), c, d, DefaultPageLimits())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := newFullStage(t.Context(), c, d, DefaultPageLimits()); !errors.Is(err, ErrResourceLimit) {
		t.Fatal(err)
	}
	_ = held.Close()
	if c.stageBytes != 0 {
		t.Fatal("Full authority retained after Close")
	}
	for _, sentinel := range []error{nil, context.Canceled, ErrCorrupt, ErrClosed} {
		if planFailure(sentinel) != sentinel {
			t.Fatal("planner operational identity changed", sentinel)
		}
	}
	for _, op := range []graphstate.Operation{{Kind: graphstate.CreateNode, Owner: 1, Life: 11, Scope: f.span(t, 0, 10), Name: strings.Repeat("x", c.limits.MaxNameBytes+1)}, {Kind: graphstate.CreateNode, Owner: 1, Life: 11, Scope: f.span(t, 0, 10), Record: graphstate.EntityRecord{Axis: f.axis}}} {
		owned, err := cloneOperations(t.Context(), v, []graphstate.Operation{op})
		if len(op.Name) > c.limits.MaxNameBytes {
			if !errors.Is(err, graphstate.ErrInvalidInput) {
				t.Fatal(err)
			}
		} else if err != nil || owned[0].Record.Axis.DefinitionHash() != op.Record.Axis.DefinitionHash() {
			t.Fatal("valid supplied axis changed", err)
		}
	}
	if _, err := cloneOperations(ctx, v, []graphstate.Operation{{Scope: f.span(t, 0, 10)}}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	v.limits.MaxSourceBytes = v.work.Bytes + 1
	if _, err := cloneOperations(t.Context(), v, []graphstate.Operation{{Scope: f.span(t, 0, 10)}}); !errors.Is(err, ErrResourceLimit) {
		t.Fatal(err)
	}
	_ = v.Close()
}

func TestFullLateWorkAndChangeCapsReturnZeroEffects(t *testing.T) {
	f := newFullFixture(t, GraphLimits{})
	w := f.span(t, 0, 10)
	f.apply(t, graphstate.Operation{Kind: graphstate.CreateNode, Owner: 1, Life: 11, Scope: w})
	revision, _ := state.NewRevision(100, 0)
	ops := []graphstate.Operation{{Kind: graphstate.AddLabel, Owner: 1, Life: 11, Scope: w, Name: "a"}, {Kind: graphstate.AddLabel, Owner: 1, Life: 11, Scope: w, Name: "b"}}
	for _, limits := range []GraphLimits{{MaxSourceBytes: 3000}, {MaxSourceBytes: 6000}, {Pages: PageLimits{MaxPatches: 1}}, {Pages: PageLimits{MaxChangeBytes: 1}}, {MaxOutputBytes: 1024}} {
		c := f.catalog(t, f.index)
		out, err := StageOperations(t.Context(), c, ops, revision, limits)
		if !errors.Is(err, ErrResourceLimit) || !reflect.DeepEqual(out, GraphEffects{}) || c.stageBytes != 0 || c.fullViewBytes != 0 || c.records != 0 {
			t.Fatal("late cap leaked graph effects", out, err)
		}
		_ = c.view.Close()
	}
}

func TestFullDescriptorTruncationAndRawBindingRefusals(t *testing.T) {
	f := newFullFixture(t, GraphLimits{})
	w := f.span(t, 0, 10)
	f.apply(t, graphstate.Operation{Kind: graphstate.CreateNode, Owner: 1, Life: 11, Scope: w})
	c := f.catalog(t, f.index)
	v := f.view(t, f.index)
	wire, err := encodeFullDescriptor(v.descriptor, c.root, c.limits)
	if err != nil {
		t.Fatal(err)
	}
	for end := range len(wire) {
		q, _ := c.reader(t.Context())
		key := componentIndexDescriptorKey(c.root.namespace)
		q.pending = map[string]raftlog.KV{string(key): {Key: key, Value: wire[:end]}}
		if _, _, err := q.fullDescriptor(c.root); !errors.Is(err, ErrCorrupt) {
			t.Fatal("truncated descriptor accepted", end, err)
		}
	}
	for _, key := range []graphstate.ComponentKey{{Owner: 99, Life: 11, Kind: graphstate.ScalarProperty, Name: "scalar"}, {Owner: 1, Life: 11, Kind: graphstate.ScalarProperty, Name: "phantom"}, {Owner: 1, Life: 11, Kind: graphstate.ScalarProperty, Name: "scalar"}} {
		q, _ := c.reader(t.Context())
		p := pageReader{q: q, limits: DefaultPageLimits()}
		value, _ := state.NewValueRef(999, 8)
		initial, _ := state.New(f.axis, state.Limits{})
		revision, _ := state.NewRevision(100, 0)
		result, _ := initial.Set(w, value, revision, state.Limits{})
		cell := result.State().Pieces()[0].Cell()
		if _, _, err := p.rawKey(key, cell); !errors.Is(err, ErrCorrupt) {
			t.Fatal("missing owner/schema/value became absent posting", key, err)
		}
	}
	q, _ := c.reader(t.Context())
	p := pageReader{q: q, limits: DefaultPageLimits()}
	p.q.maxBytes = q.bytes + 1
	s, _ := state.New(f.axis, state.Limits{})
	if _, _, err := p.exactComponentOutput(s, w, graphstate.ReadBudget{Rows: 512, Bytes: 4 << 20}); !errors.Is(err, ErrResourceLimit) {
		t.Fatal(err)
	}
	q, _ = c.reader(t.Context())
	p.q = q
	if _, _, err := p.exactComponentOutput(s, w, graphstate.ReadBudget{Rows: 512, Bytes: 1}); !errors.Is(err, ErrResourceLimit) {
		t.Fatal(err)
	}
	_ = v.Close()
}

func TestFullBindingSourceExhaustionAndInvalidPolicyPublishNothing(t *testing.T) {
	f := newFullFixture(t, GraphLimits{})
	w := f.span(t, 0, 10)
	f.apply(t, graphstate.Operation{Kind: graphstate.CreateNode, Owner: 1, Life: 11, Scope: w}, graphstate.Operation{Kind: graphstate.CreateNode, Owner: 2, Life: 11, Scope: w})
	f.apply(t, graphstate.Operation{Kind: graphstate.CreateRelationship, Owner: 3, Life: 31, Scope: w, Record: graphstate.EntityRecord{Type: "R", Source: 1, Target: 2, Mode: graphstate.LifeBound}, Binding: graphstate.LifeRecord{SourceLife: 11, TargetLife: 11}})
	f.apply(t, graphstate.Operation{Kind: graphstate.Set, Owner: 1, Life: 11, Scope: w, Name: "scalar", Value: graphstate.I64(7), ValueID: 99})
	c := f.catalog(t, f.index)
	q, _ := c.reader(t.Context())
	r, _, err := q.entity(EntityRef{c.root.namespace.Graph, 3})
	if err != nil {
		t.Fatal(err)
	}
	for _, rows := range []int{1, 2, 3, 4, 5} {
		q, _ := c.reader(t.Context())
		q.maxRows = rows
		p := pageReader{q: q, limits: DefaultPageLimits()}
		if err := p.checkCanonical(r); !errors.Is(err, ErrResourceLimit) {
			t.Fatal("binding validation overran source allowance", rows, err)
		}
	}
	value, _ := state.NewValueRef(99, 8)
	initialState, _ := state.New(f.axis, state.Limits{})
	cellRevision, _ := state.NewRevision(100, 0)
	result, _ := initialState.Set(w, value, cellRevision, state.Limits{})
	cell := result.State().Pieces()[0].Cell()
	for _, rows := range []int{1, 2, 3, 4, 5} {
		q, _ := c.reader(t.Context())
		q.maxRows = rows
		p := pageReader{q: q, limits: DefaultPageLimits()}
		if _, _, err := p.rawKey(graphstate.ComponentKey{Owner: 1, Life: 11, Kind: graphstate.ScalarProperty, Name: "scalar"}, cell); !errors.Is(err, ErrResourceLimit) {
			t.Fatal("raw validation overran source allowance", rows, err)
		}
		q, _ = c.reader(t.Context())
		q.maxRows = rows
		p.q = q
		if err := p.checkDeclared(r, graphstate.LifeRecord{Owner: 3, Life: 31, SourceLife: 11, TargetLife: 11}); !errors.Is(err, ErrResourceLimit) {
			t.Fatal(err)
		}
	}
	q, _ = c.reader(t.Context())
	p := pageReader{q: q, limits: DefaultPageLimits()}
	if err := p.validateIncident(postingKey{family: canonicalIncidentRecord, relationship: 99}); !errors.Is(err, ErrCorrupt) {
		t.Fatal("missing relationship became candidate", err)
	}
	revision, _ := state.NewRevision(100, 0)
	v := f.view(t, f.index)
	c.limits.MaxStages = 1
	held, err := newFullStage(t.Context(), c, v.descriptor, DefaultPageLimits())
	if err != nil {
		t.Fatal(err)
	}
	if out, err := StageOperations(t.Context(), c, nil, revision, GraphLimits{}); !errors.Is(err, ErrResourceLimit) || !reflect.DeepEqual(out, GraphEffects{}) {
		t.Fatal("typed stage bypassed shared authority cap", err)
	}
	_ = held.Close()
	_ = v.Close()
	if _, err := StageOperations(t.Context(), c, nil, revision, GraphLimits{MaxOutputBytes: -1}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := StageOperations(ctx, c, nil, revision, GraphLimits{}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	fresh := topologyStore(t, vfs.NewMem(), f.db.ApplicationLimits())
	_ = bootstrapRoot(t, fresh)
	initial := openCatalog(t, fresh, 1, Limits{})
	if _, err := InitializeGraphIndexes(t.Context(), initial, nil, GraphLimits{MaxSourceRows: -1}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := InitializeGraphIndexes(ctx, initial, nil, GraphLimits{}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	initial.limits.MaxStageRecords = 5
	if _, err := InitializeGraphIndexes(t.Context(), initial, fullSchemas()[:1], GraphLimits{}); !errors.Is(err, ErrResourceLimit) {
		t.Fatal(err)
	}
}
