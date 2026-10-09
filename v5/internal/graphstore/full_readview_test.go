package graphstore

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func TestFullReadViewEveryDoorOneIdentityAndNegativeReads(t *testing.T) {
	f := newFullFixture(t, GraphLimits{})
	scope := f.span(t, 0, 10)
	f.apply(t, graphstate.Operation{Kind: graphstate.CreateNode, Owner: 1, Life: 11, Scope: scope}, graphstate.Operation{Kind: graphstate.CreateNode, Owner: 2, Life: 11, Scope: scope}, graphstate.Operation{Kind: graphstate.AddLabel, Owner: 1, Life: 11, Scope: scope, Name: "a\x00"}, graphstate.Operation{Kind: graphstate.Set, Owner: 1, Life: 11, Scope: scope, Name: "scalar", Value: graphstate.I64(7), ValueID: 99})
	v := f.view(t, f.index)
	id := v.Identity()
	if id == (graphstate.ViewID{}) || v.Graph() != testNamespace().Graph || v.pages.Identity() != id {
		t.Fatal("read doors own different identities")
	}
	entity, err := v.Entity(t.Context(), 1)
	if err != nil || !entity.Found || entity.View != id || entity.Version != graphstate.ReadVersion(f.index) {
		t.Fatal(entity, err)
	}
	life, err := v.Life(t.Context(), 2, 11)
	if err != nil || !life.Found || life.Record.Owner != 2 || life.View != id {
		t.Fatal("life identity accidentally global", life, err)
	}
	property, err := v.Property(t.Context(), graphstate.Node, "scalar")
	if err != nil || !property.Found || property.View != id {
		t.Fatal(property, err)
	}
	value, err := v.Value(t.Context(), 99)
	if err != nil || !value.Found || value.View != id {
		t.Fatal(value, err)
	}
	canonical, err := v.ValueIdentity(t.Context(), graphstate.I64(7))
	if err != nil || canonical.ID != 99 || canonical.View != id {
		t.Fatal(canonical, err)
	}
	presence := graphstate.ComponentKey{Owner: 1, Kind: graphstate.Presence}
	component, err := v.ComponentPage(t.Context(), graphstate.ComponentQuery{Key: presence, Window: scope}, 0, graphstate.ReadBudget{Rows: 512, Bytes: 4 << 20})
	if err != nil || component.View != id || !component.Complete || len(component.Data.Pieces()) != 1 {
		t.Fatal(component, err)
	}
	keys, err := v.ComponentKeys(t.Context(), graphstate.KeyPredicate{Owner: 1}, 0, graphstate.ReadBudget{Rows: 512, Bytes: 4 << 20})
	if err != nil || keys.View != id || !keys.Complete || !reflect.DeepEqual(keys.Keys, []graphstate.ComponentKey{{Owner: 1, Kind: graphstate.Presence}, {Owner: 1, Life: 11, Kind: graphstate.Label, Name: "a\x00"}, {Owner: 1, Life: 11, Kind: graphstate.ScalarProperty, Name: "scalar"}}) {
		t.Fatal(keys, err)
	}
	claims, err := v.UniqueCandidates(t.Context(), graphstate.UniquePredicate{Definition: fullSchemas()[0], Value: graphstate.I64(7), Window: scope}, 0, graphstate.ReadBudget{Rows: 512, Bytes: 4 << 20})
	if err != nil || claims.View != id || !claims.Complete || len(claims.Claims) != 1 {
		t.Fatal(claims, err)
	}
	assertFullClaimSet(t, claims.Claims, []graphstate.UniqueClaim{{Owner: 1, Life: 11, Key: graphstate.ComponentKey{Owner: 1, Life: 11, Kind: graphstate.ScalarProperty, Name: "scalar"}}})
	incidents, err := v.IncidentRelationships(t.Context(), graphstate.IncidentPredicate{Endpoint: 1, Life: 11, Window: scope}, 0, graphstate.ReadBudget{Rows: 512, Bytes: 4 << 20})
	if err != nil || incidents.View != id || !incidents.Complete || len(incidents.Entities) != 0 {
		t.Fatal(incidents, err)
	}
	missingEntity, err := v.Entity(t.Context(), 99)
	if err != nil || missingEntity.Found {
		t.Fatal(missingEntity, err)
	}
	missingLife, err := v.Life(t.Context(), 1, 12)
	if err != nil || missingLife.Found {
		t.Fatal(missingLife, err)
	}
	missingProperty, err := v.Property(t.Context(), graphstate.Relationship, "phantom")
	if err != nil || missingProperty.Found {
		t.Fatal(missingProperty, err)
	}
	missingValue, err := v.Value(t.Context(), 100)
	if err != nil || missingValue.Found {
		t.Fatal(missingValue, err)
	}
	missingCanonical, err := v.ValueIdentity(t.Context(), graphstate.I64(100))
	if err != nil || missingCanonical.Found {
		t.Fatal(missingCanonical, err)
	}
	if got := fullReadKeys(t, v, graphstate.KeyPredicate{Owner: 1, Life: 11, Name: "a\x00missing"}); len(got) != 0 {
		t.Fatal("NUL name phantom", got)
	}
	if got := fullReadClaims(t, v, graphstate.UniquePredicate{Definition: fullSchemas()[0], Value: graphstate.I64(100), Window: scope}, graphstate.ReadBudget{Rows: 512, Bytes: 4 << 20}); len(got) != 0 {
		t.Fatal("value phantom", got)
	}
	if err := v.Close(); err != nil {
		t.Fatal(err)
	}
	if err := v.Close(); err != nil {
		t.Fatal(err)
	}
	if v.Graph() != (graphstate.GraphID{}) || v.Identity() != (graphstate.ViewID{}) || v.c.fullViews != 0 || v.c.fullViewBytes != 0 || v.cursors != nil || v.base.Image != nil {
		t.Fatal("close retained complete handles")
	}
	if _, err := v.c.view.Root(); err != nil {
		t.Fatal("complete reader closed borrowed ApplicationView", err)
	}
}
func TestFullNilPartialLifetimeAndInvalidLimits(t *testing.T) {
	if err := DefaultGraphLimits().Validate(); err != nil {
		t.Fatal(err)
	}
	for _, l := range []GraphLimits{{MaxSourceRows: -1}, {MaxSourceBytes: -1}, {MaxOutputBytes: -1}, {Pages: PageLimits{MaxLevels: 9}}, {Planner: graphstate.Limits{MaxOperations: -1}}} {
		if err := l.Validate(); !errors.Is(err, ErrInvalid) {
			t.Fatal(l, err)
		}
	}
	var c *Catalog
	var v *ReadView
	if _, err := OpenReadView(t.Context(), c, GraphLimits{}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	for _, fn := range []func() error{func() error { _, e := v.Entity(t.Context(), 1); return e }, func() error { _, e := v.Life(t.Context(), 1, 1); return e }, func() error { _, e := v.Property(t.Context(), graphstate.Node, "x"); return e }, func() error { _, e := v.Value(t.Context(), 1); return e }, func() error { _, e := v.ValueIdentity(t.Context(), graphstate.I64(1)); return e }, func() error {
		_, e := v.ComponentPage(t.Context(), graphstate.ComponentQuery{}, 0, graphstate.ReadBudget{})
		return e
	}, func() error {
		_, e := v.ComponentKeys(t.Context(), graphstate.KeyPredicate{}, 0, graphstate.ReadBudget{})
		return e
	}, func() error {
		_, e := v.UniqueCandidates(t.Context(), graphstate.UniquePredicate{}, 0, graphstate.ReadBudget{})
		return e
	}, func() error {
		_, e := v.IncidentRelationships(t.Context(), graphstate.IncidentPredicate{}, 0, graphstate.ReadBudget{})
		return e
	}, v.Close} {
		if err := fn(); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
	}
	if v.Graph() != (graphstate.GraphID{}) || v.Identity() != (graphstate.ViewID{}) || v.Work() != (PageWork{}) {
		t.Fatal("nil accessor")
	}
	primitive, _ := newStore(t, vfs.NewMem())
	plain := openCatalog(t, primitive, 1, Limits{})
	if _, err := OpenReadView(t.Context(), plain, GraphLimits{}); !errors.Is(err, ErrTopologyUnsupported) {
		t.Fatal(err)
	}
	db, _, index := emptyIndexed(t, PageLimits{})
	partial := openCatalog(t, db, index, Limits{})
	if _, err := OpenReadView(t.Context(), partial, GraphLimits{}); !errors.Is(err, ErrTopologyUnsupported) {
		t.Fatal("KeysOnly advertised Full", err)
	}
	if _, err := InitializeGraphIndexes(t.Context(), partial, nil, GraphLimits{}); !errors.Is(err, ErrTopologyUnsupported) {
		t.Fatal("populated KeysOnly promoted", err)
	}
	if _, err := StageOperations(t.Context(), c, nil, state.Revision{}, GraphLimits{}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := InitializeGraphIndexes(t.Context(), c, nil, GraphLimits{}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, _, err := Project(t.Context(), c, 1, temporal.Position{}, graphstate.Declared, GraphLimits{}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	f := newFullFixture(t, GraphLimits{})
	c = f.catalog(t, f.index)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := OpenReadView(ctx, c, GraphLimits{}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := OpenReadView(t.Context(), c, GraphLimits{MaxSourceRows: 4}); !errors.Is(err, ErrResourceLimit) || c.fullViews != 0 {
		t.Fatal("constructor failure leaked accounting", err)
	}
	v, err := OpenReadView(t.Context(), c, GraphLimits{})
	if err != nil {
		t.Fatal(err)
	}
	_ = v.Close()
	if _, err := v.Entity(t.Context(), 1); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}
func TestFullSourceRowsExhaustionIsNotOneExtraRead(t *testing.T) {
	f := newFullFixture(t, GraphLimits{})
	c := f.catalog(t, f.index)
	v, err := OpenReadView(t.Context(), c, GraphLimits{MaxSourceRows: 5})
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	before := v.Work()
	if _, err := v.Entity(t.Context(), 1); !errors.Is(err, ErrResourceLimit) || v.Work() != before {
		t.Fatal("exhausted allowance performed extra lookup", v.Work(), err)
	}
	revision, _ := state.NewRevision(1, 0)
	effects, err := StageOperations(t.Context(), c, nil, revision, GraphLimits{MaxSourceRows: 5})
	if err != nil || effects.Work.Records != 5 || effects.Root != c.root {
		t.Fatal("zero-read no-op failed or overspent", effects.Work, err)
	}
	// Initializer's one descriptor-absence lookup leaves zero rows. Its remaining
	// schema/empty-page writes are explicitly read-free rather than granting one.
	fresh := topologyStore(t, vfs.NewMem(), f.db.ApplicationLimits())
	_ = bootstrapRoot(t, fresh)
	freshCatalog := openCatalog(t, fresh, 1, Limits{})
	initialized, err := InitializeGraphIndexes(t.Context(), freshCatalog, nil, GraphLimits{MaxSourceRows: 1})
	if err != nil || initialized.Work.Records != 1 {
		t.Fatal("zero-read initialization overspent", initialized.Work, err)
	}
}
func TestFullDescriptorFamilyCorruptionFailsBeforeAdvertisingCoverage(t *testing.T) {
	for _, offset := range []int{64, 88, 112, 136} {
		f := newFullFixture(t, GraphLimits{})
		c := f.catalog(t, f.index)
		q, _ := c.reader(t.Context())
		d, _, err := q.fullDescriptor(c.root)
		if err != nil {
			t.Fatal(err)
		}
		wire, _ := encodeFullDescriptor(d, c.root, c.limits)
		wire[offset] ^= 1
		// The private assembler commits a fresh correctly checksummed application
		// frame around the corrupted descriptor; failure is semantic, not just CRC.
		base, err := c.view.Root()
		if err != nil {
			t.Fatal(err)
		}
		f.install(t, GraphEffects{Base: base, Root: c.root, Writes: []raftlog.KV{{Key: componentIndexDescriptorKey(c.root.namespace), Value: wire}}})
		bad := f.catalog(t, f.index)
		if _, err := OpenReadView(t.Context(), bad, GraphLimits{}); !errors.Is(err, ErrCorrupt) || bad.fullViews != 0 || bad.fullViewBytes != 0 {
			t.Fatal("wrong first/other family advertised Full", offset, err)
		}
		if _, err := bad.Root(); !errors.Is(err, ErrPoisoned) {
			t.Fatal(err)
		}
	}
}

func TestFullBudgetCursorRefusalsPreservePreviousContinuation(t *testing.T) {
	f := newFullFixture(t, GraphLimits{})
	scope := f.span(t, 0, 10)
	f.apply(t, graphstate.Operation{Kind: graphstate.CreateNode, Owner: 1, Life: 11, Scope: scope})
	for _, name := range []string{"a", "a\x00", "a\x00z", "aa", "å", "😀"} {
		f.apply(t, graphstate.Operation{Kind: graphstate.AddLabel, Owner: 1, Life: 11, Scope: scope, Name: name})
	}
	c := f.catalog(t, f.index)
	v, err := OpenReadView(t.Context(), c, GraphLimits{Pages: PageLimits{MaxCursors: 1}})
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	predicate := graphstate.KeyPredicate{Owner: 1, Life: 11, Kind: graphstate.Label}
	first, err := v.ComponentKeys(t.Context(), predicate, 0, graphstate.ReadBudget{Rows: 12, Bytes: 4 << 20})
	if err != nil || first.Next == 0 || first.Complete || len(first.Keys) == 0 {
		t.Fatal("bounded key page made no progress", first, err)
	}
	saved := v.cursors[first.Next]
	bytes := v.cursorBytes
	outputs := v.outputBytes
	work := v.Work()
	if _, err := v.ComponentKeys(t.Context(), predicate, 0, graphstate.ReadBudget{Rows: 12, Bytes: 4 << 20}); !errors.Is(err, ErrResourceLimit) {
		t.Fatal("shared cursor cap bypassed", err)
	}
	if v.cursors[first.Next] != saved || v.cursorBytes != bytes || v.outputBytes != outputs || v.Work().Records <= work.Records {
		t.Fatal("refusal mutated cursor/output or hid attempted source work")
	}
	if _, err := v.ComponentKeys(t.Context(), graphstate.KeyPredicate{Owner: 2}, first.Next, graphstate.ReadBudget{Rows: 512, Bytes: 4 << 20}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	before := v.Work()
	if _, err := v.ComponentKeys(ctx, predicate, first.Next, graphstate.ReadBudget{Rows: 512, Bytes: 4 << 20}); !errors.Is(err, context.Canceled) || v.Work() != before || v.cursors[first.Next] != saved || v.outputBytes != outputs {
		t.Fatal("cancelled call published/consumed output", err)
	}
	rest, err := v.ComponentKeys(t.Context(), predicate, first.Next, graphstate.ReadBudget{Rows: 512, Bytes: 4 << 20})
	if err != nil || !rest.Complete || rest.Next != 0 {
		t.Fatal(rest, err)
	}
	if _, err := v.ComponentKeys(t.Context(), predicate, first.Next, graphstate.ReadBudget{Rows: 512, Bytes: 4 << 20}); !errors.Is(err, ErrInvalid) {
		t.Fatal("consumed token replay accepted", err)
	}
}
func TestFullCandidateCursorsAndPredicateBindings(t *testing.T) {
	f := newFullFixture(t, GraphLimits{})
	scope := f.span(t, 0, 10)
	// Historical memberships have disjoint effective support, so the same unique
	// value can legitimately produce multiple raw candidates.
	for i := range 6 {
		id := graphstate.EntityID(i + 1)
		w := f.span(t, int64(2*i), int64(2*i+1))
		f.apply(t, graphstate.Operation{Kind: graphstate.CreateNode, Owner: id, Life: 11, Scope: w}, graphstate.Operation{Kind: graphstate.Set, Owner: id, Life: 11, Scope: w, Name: "scalar", Value: graphstate.I64(7), ValueID: graphstate.ValueID(99 + i)})
	}
	v := f.view(t, f.index)
	predicate := graphstate.UniquePredicate{Definition: fullSchemas()[0], Value: graphstate.I64(7), Window: scope}
	first, err := v.UniqueCandidates(t.Context(), predicate, 0, graphstate.ReadBudget{Rows: 40, Bytes: 4 << 20})
	if err != nil || first.Next == 0 || first.Complete || len(first.Claims) == 0 {
		t.Fatal("bounded candidate page made no progress", first, err)
	}
	saved := v.cursors[first.Next]
	output := v.outputBytes
	changed := predicate
	changed.Value = graphstate.I64(8)
	if _, err := v.UniqueCandidates(t.Context(), changed, first.Next, graphstate.ReadBudget{Rows: 512, Bytes: 4 << 20}); !errors.Is(err, ErrInvalid) || v.cursors[first.Next] != saved || v.outputBytes != output {
		t.Fatal("candidate cursor accepted foreign exact value", err)
	}
	if _, err := v.UniqueCandidates(t.Context(), predicate, first.Next, graphstate.ReadBudget{Rows: 1, Bytes: 4 << 20}); !errors.Is(err, ErrResourceLimit) || v.cursors[first.Next] != saved {
		t.Fatal("failed candidate call consumed token", err)
	}
	rest, err := v.UniqueCandidates(t.Context(), predicate, first.Next, graphstate.ReadBudget{Rows: 512, Bytes: 4 << 20})
	if err != nil || !rest.Complete || len(first.Claims)+len(rest.Claims) != 6 {
		t.Fatal("raw candidate omission/duplication", rest, err)
	}
	all := append(append([]graphstate.UniqueClaim{}, first.Claims...), rest.Claims...)
	want := make([]graphstate.UniqueClaim, 6)
	for i := range want {
		id := graphstate.EntityID(i + 1)
		want[i] = graphstate.UniqueClaim{Owner: id, Life: 11, Key: graphstate.ComponentKey{Owner: id, Life: 11, Kind: graphstate.ScalarProperty, Name: "scalar"}}
	}
	assertFullClaimSet(t, all, want)
}
func TestFullPointExactOwnedOutputBoundaryAndCancelledPublication(t *testing.T) {
	f := newFullFixture(t, GraphLimits{})
	scope := f.span(t, 0, 10)
	f.apply(t, graphstate.Operation{Kind: graphstate.CreateNode, Owner: 1, Life: 11, Scope: scope}, graphstate.Operation{Kind: graphstate.Set, Owner: 1, Life: 11, Scope: scope, Name: "ordinary", Value: graphstate.I64(7), ValueID: 99})
	c := f.catalog(t, f.index)
	exact := fullEntityOutputBytes + axisVariableBytes(f.axis)
	v, err := OpenReadView(t.Context(), c, GraphLimits{MaxOutputBytes: exact - 1})
	if err != nil {
		t.Fatal(err)
	}
	before := v.Work()
	if got, err := v.Entity(t.Context(), 1); !errors.Is(err, ErrResourceLimit) || got.Found || v.outputBytes != 0 || v.Work().Records <= before.Records {
		t.Fatal("one-short output leaked or source work omitted", got, err)
	}
	_ = v.Close()
	v, err = OpenReadView(t.Context(), c, GraphLimits{MaxOutputBytes: exact})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := v.Entity(t.Context(), 1); err != nil || !got.Found || v.outputBytes != exact {
		t.Fatal("exact output refused", got, err)
	}
	_ = v.Close()
	v, err = OpenReadView(t.Context(), c, GraphLimits{})
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	before = v.Work()
	if _, err := v.Value(ctx, 99); !errors.Is(err, context.Canceled) || v.outputBytes != 0 || v.Work() != before {
		t.Fatal("cancelled point published quota/work", err)
	}
	if _, err := v.Property(t.Context(), graphstate.Node, " "); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := v.ValueIdentity(t.Context(), graphstate.Scalar{}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
}

func assertFullClaimSet(t *testing.T, got, want []graphstate.UniqueClaim) {
	t.Helper()
	seen := make(map[graphstate.UniqueClaim]bool, len(got))
	for _, claim := range got {
		if seen[claim] {
			t.Fatal("duplicate candidate", claim)
		}
		seen[claim] = true
	}
	actual := slices.Clone(got)
	expected := slices.Clone(want)
	slices.SortFunc(actual, func(a, b graphstate.UniqueClaim) int { return compareComponentKeys(a.Key, b.Key) })
	slices.SortFunc(expected, func(a, b graphstate.UniqueClaim) int { return compareComponentKeys(a.Key, b.Key) })
	if !reflect.DeepEqual(actual, expected) {
		t.Fatal("wrong exact candidate set", actual, expected)
	}
}

type fullCancelContext struct {
	context.Context
	calls, cancelAt int
}

func (c *fullCancelContext) Err() error {
	c.calls++
	if c.cancelAt > 0 && c.calls >= c.cancelAt {
		return context.Canceled
	}
	return c.Context.Err()
}
func TestFullCancellationAtFinalCheckKeepsOutputAndCursorAtomic(t *testing.T) {
	f := newFullFixture(t, GraphLimits{})
	scope := f.span(t, 0, 10)
	f.apply(t, graphstate.Operation{Kind: graphstate.CreateNode, Owner: 1, Life: 11, Scope: scope})
	for _, name := range []string{"a", "b", "c", "d"} {
		f.apply(t, graphstate.Operation{Kind: graphstate.AddLabel, Owner: 1, Life: 11, Scope: scope, Name: name})
	}
	predicate := graphstate.KeyPredicate{Owner: 1, Life: 11, Kind: graphstate.Label}
	budget := graphstate.ReadBudget{Rows: 12, Bytes: 4 << 20}
	probe := f.view(t, f.index)
	first, err := probe.ComponentKeys(t.Context(), predicate, 0, budget)
	if err != nil || first.Next == 0 {
		t.Fatal(first, err)
	}
	count := &fullCancelContext{Context: t.Context()}
	if _, err := probe.ComponentKeys(count, predicate, first.Next, budget); err != nil {
		t.Fatal(err)
	}
	if count.calls < 2 {
		t.Fatal("test did not perform source reads")
	}
	_ = probe.Close()
	_ = probe.c.view.Close()
	v := f.view(t, f.index)
	first, err = v.ComponentKeys(t.Context(), predicate, 0, budget)
	if err != nil || first.Next == 0 {
		t.Fatal(first, err)
	}
	saved := v.cursors[first.Next]
	outputs := v.outputBytes
	before := v.Work()
	ctx := &fullCancelContext{Context: t.Context(), cancelAt: count.calls}
	result, err := v.ComponentKeys(ctx, predicate, first.Next, budget)
	if !errors.Is(err, context.Canceled) || len(result.Keys) != 0 || result.Next != 0 || v.outputBytes != outputs || v.cursors[first.Next] != saved || v.Work().Records <= before.Records {
		t.Fatal("late cancellation published output/cursor or hid work", result, ctx.calls, count.calls, v.Work(), err)
	}
	if _, err := v.ComponentKeys(t.Context(), predicate, first.Next, budget); err != nil {
		t.Fatal("late cancellation consumed prior continuation", err)
	}
}
func TestFullStateResourceSentinelPreservedAtEveryLayer(t *testing.T) {
	f := newFullFixture(t, GraphLimits{})
	c := f.catalog(t, f.index)
	q, err := c.reader(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	p := pageReader{q: q, limits: DefaultPageLimits()}
	p.limits.MaxCells = 1
	source, err := state.New(f.axis, state.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	var changes []state.Change
	for i := range 3 {
		scope := f.span(t, int64(2*i), int64(2*i+1))
		revision, _ := state.NewRevision(uint64(i+1), 0)
		result, err := source.Set(scope, state.Null(), revision, state.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		source = result.State()
		changes = append(changes, result.Changes()...)
	}
	p.q.maxBytes = p.q.bytes + 64
	_, err = p.ownState(source)
	if !errors.Is(err, ErrResourceLimit) || !errors.Is(err, state.ErrResourceLimit) || errors.Is(err, ErrInvalid) {
		t.Fatal("ownState did not preserve resource refusal", err)
	}
	_, err = p.ownChanges(f.span(t, 0, 10), changes)
	if !errors.Is(err, ErrResourceLimit) || !errors.Is(err, state.ErrResourceLimit) || errors.Is(err, ErrInvalid) {
		t.Fatal("ownChanges did not preserve codec resource refusal", err)
	}
	wrapped := callerError(err)
	if !errors.Is(wrapped, ErrResourceLimit) || !errors.Is(wrapped, state.ErrResourceLimit) || errors.Is(wrapped, ErrInvalid) {
		t.Fatal("state resource became caller invalid", wrapped)
	}
	if err := c.failure(wrapped); !errors.Is(err, ErrResourceLimit) || c.poison != nil {
		t.Fatal("state resource poisoned catalog", err)
	}
}

func TestFullComponentContinuationLateCancellationRestoresBorrowedPageCursor(t *testing.T) {
	f := newFullFixture(t, GraphLimits{Pages: PageLimits{MaxCells: 2, MaxTailRecords: 1}})
	whole := f.span(t, 0, 100)
	f.apply(t, graphstate.Operation{Kind: graphstate.CreateNode, Owner: 1, Life: 11, Scope: whole})
	for i := range 16 {
		f.apply(t, graphstate.Operation{Kind: graphstate.AddLabel, Owner: 1, Life: 11, Scope: f.span(t, int64(2*i), int64(2*i+1)), Name: "fragmented"})
	}
	query := graphstate.ComponentQuery{Key: graphstate.ComponentKey{Owner: 1, Life: 11, Kind: graphstate.Label, Name: "fragmented"}, Window: whole}
	budget := graphstate.ReadBudget{Rows: 24, Bytes: 4 << 20}
	probe := f.view(t, f.index)
	first, err := probe.ComponentPage(t.Context(), query, 0, budget)
	if err != nil || first.Next == 0 || first.Complete {
		t.Fatal("component test did not create continuation", first, err)
	}
	count := &fullCancelContext{Context: t.Context()}
	if _, err := probe.ComponentPage(count, query, first.Next, budget); err != nil {
		t.Fatal(err)
	}
	_ = probe.Close()
	_ = probe.c.view.Close()
	v := f.view(t, f.index)
	first, err = v.ComponentPage(t.Context(), query, 0, budget)
	if err != nil {
		t.Fatal(err)
	}
	saved := v.pages.cursors[first.Next]
	bytes, output, work := v.pages.cursorBytes, v.outputBytes, v.Work()
	page, err := v.ComponentPage(&fullCancelContext{Context: t.Context(), cancelAt: count.calls}, query, first.Next, budget)
	if !errors.Is(err, context.Canceled) || page.Next != 0 || len(page.Data.Pieces()) != 0 || v.outputBytes != output || v.pages.cursorBytes != bytes || !reflect.DeepEqual(v.pages.cursors[first.Next], saved) || v.Work().Records <= work.Records {
		t.Fatal("late component cancellation lost cursor/output atomicity", err)
	}
	if _, err := v.ComponentPage(t.Context(), query, first.Next, budget); err != nil {
		t.Fatal("cancelled component continuation cannot resume", err)
	}
}
