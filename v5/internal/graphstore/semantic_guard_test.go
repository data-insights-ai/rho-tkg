package graphstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"reflect"
	"testing"
	"unsafe"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
)

type guardCounters struct{ views, viewBytes, stages, records, stageBytes int }

func readGuardCounters(c *Catalog) guardCounters {
	c.mu.Lock()
	defer c.mu.Unlock()
	return guardCounters{c.fullViews, c.fullViewBytes, c.stages, c.records, c.stageBytes}
}
func assertGuardCounters(t *testing.T, c *Catalog, before guardCounters) {
	t.Helper()
	if got := readGuardCounters(c); got != before {
		t.Fatal("shared ownership changed", before, got)
	}
}
func fixtureGuard(t *testing.T, f *fullFixture) SemanticGuard {
	t.Helper()
	c := f.catalog(t, f.index)
	defer c.view.Close()
	v, err := OpenReadView(t.Context(), c, GraphLimits{})
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	root, err := c.Root()
	if err != nil {
		t.Fatal(err)
	}
	g, err := NewSemanticGuard(root)
	if err != nil {
		t.Fatal(err)
	}
	return g
}
func guardedStage(t *testing.T, f *fullFixture, g SemanticGuard, ops []graphstate.Operation, revision state.Revision, l GraphLimits) (GraphEffects, PageWork, error) {
	t.Helper()
	c := f.catalog(t, f.index)
	defer c.view.Close()
	before := readGuardCounters(c)
	e, w, err := StageGuardedOperations(t.Context(), c, g, ops, revision, l)
	assertGuardCounters(t, c, before)
	if _, err := c.view.Root(); err != nil {
		t.Fatal("borrow closed", err)
	}
	if err != nil && !reflect.DeepEqual(e, GraphEffects{}) {
		t.Fatal("error returned effects", e, err)
	}
	return e, w, err
}
func TestNewSemanticGuardDirectAndFixedRepresentation(t *testing.T) {
	f := newFullFixture(t, GraphLimits{})
	g, err := NewSemanticGuard(f.root)
	if err != nil || g.Namespace != f.root.Namespace() || g.SemanticEpoch != 1 || g.EffectDigest != f.root.EffectDigest() || g.OwnershipEpoch != f.root.owner || g.TopologyEpoch != 1 || g.SchemaVersion != 1 {
		t.Fatal(g, err)
	}
	original := g
	g.Namespace.Graph[0]++
	g.EffectDigest[0]++
	unchanged, err := NewSemanticGuard(f.root)
	if err != nil || unchanged != original {
		t.Fatal("alias", err)
	}
	if unsafe.Sizeof(SemanticGuard{}) > semanticGuardMetadataBytes || unsafe.Sizeof(PageWork{}) > semanticGuardWorkBytes {
		t.Fatal("metadata undercount")
	}
	primitive, _ := NewRoot(testNamespace(), 3)
	primitive, _ = primitive.AdvanceEffects(sha256.Sum256([]byte("primitive")))
	for _, test := range []struct {
		name     string
		root     Root
		expected error
	}{
		{"zero-root", Root{}, ErrNamespace}, {"primitive", primitive, ErrTopologyUnsupported},
		{"zero-epoch", func() Root { r := f.root; r.epoch = 0; return r }(), ErrInvalid},
		{"bad-owner", func() Root { r := f.root; r.owner = 0; return r }(), ErrInvalid},
		{"zero-digest", func() Root { r := f.root; r.effect = [32]byte{}; return r }(), ErrInvalid},
	} {
		t.Run(test.name, func(t *testing.T) {
			out, err := NewSemanticGuard(test.root)
			if !errors.Is(err, test.expected) || out != (SemanticGuard{}) {
				t.Fatal(out, err)
			}
		})
	}
	keysOnly := f.root
	keysOnly.topology = keysOnlyTopology
	same, err := NewSemanticGuard(keysOnly)
	if err != nil || same != original {
		t.Fatal("physical format entered predicate", same, err)
	}
	if err := original.compare(primitive); !errors.Is(err, ErrTopologyUnsupported) {
		t.Fatal(err)
	}
}
func TestGuardedStageChecksAllLogicalFieldsBeforeInputPlan(t *testing.T) {
	f := newFullFixture(t, GraphLimits{})
	base := fixtureGuard(t, f)
	c := f.catalog(t, f.index)
	defer c.view.Close()
	v, err := OpenReadView(t.Context(), c, GraphLimits{})
	if err != nil {
		t.Fatal(err)
	}
	expected := v.Work()
	expected.Bytes += semanticGuardMetadataBytes
	v.Close()
	before := readGuardCounters(c)
	for _, name := range []string{"graph", "partition", "ownership", "topology", "schema", "epoch", "digest"} {
		t.Run(name, func(t *testing.T) {
			g := base
			switch name {
			case "graph":
				g.Namespace.Graph[1]++
			case "partition":
				g.Namespace.Partition++
			case "ownership":
				g.OwnershipEpoch++
			case "topology":
				g.TopologyEpoch++
			case "schema":
				g.SchemaVersion++
			case "epoch":
				g.SemanticEpoch++
			case "digest":
				g.EffectDigest[1]++
			}
			// Invalid input/revision would fail if clone or Plan ran before comparison.
			e, w, err := StageGuardedOperations(t.Context(), c, g, []graphstate.Operation{{}}, state.Revision{}, GraphLimits{})
			if !errors.Is(err, ErrReadConflict) || !reflect.DeepEqual(e, GraphEffects{}) || w != expected {
				t.Fatal(e, w, expected, err)
			}
			assertGuardCounters(t, c, before)
		})
	}
	for _, name := range []string{"graph", "partition", "ownership", "topology", "schema", "epoch", "digest"} {
		t.Run("invalid-"+name, func(t *testing.T) {
			g := base
			expected := ErrInvalid
			switch name {
			case "graph":
				g.Namespace.Graph = [16]byte{}
				expected = ErrNamespace
			case "partition":
				g.Namespace.Partition = 0
				expected = ErrNamespace
			case "ownership":
				g.OwnershipEpoch = 0
			case "topology":
				g.TopologyEpoch = 0
			case "schema":
				g.SchemaVersion = 0
			case "epoch":
				g.SemanticEpoch = 0
			case "digest":
				g.EffectDigest = [32]byte{}
			}
			e, w, err := StageGuardedOperations(t.Context(), c, g, nil, state.Revision{}, GraphLimits{})
			if !errors.Is(err, expected) || !reflect.DeepEqual(e, GraphEffects{}) || w != (PageWork{}) {
				t.Fatal(e, w, err)
			}
			assertGuardCounters(t, c, before)
		})
	}
	if c.poison != nil {
		t.Fatal("conflict poisoned catalog")
	}
}
func TestGuardedStageActualHistoricalMutationAndABA(t *testing.T) {
	f := newFullFixture(t, GraphLimits{})
	scope := f.span(t, 0, 10)
	absent := fixtureGuard(t, f)
	f.apply(t, graphstate.Operation{Kind: graphstate.CreateNode, Owner: 1, Life: 11, Scope: scope}, graphstate.Operation{Kind: graphstate.Set, Owner: 1, Life: 11, Name: "ordinary", Scope: scope, Value: graphstate.I64(7), ValueID: 99})
	revision, _ := state.NewRevision(100, 0)
	if _, w, err := guardedStage(t, f, absent, nil, revision, GraphLimits{}); !errors.Is(err, ErrReadConflict) || w.Records != 6 {
		t.Fatal(w, err)
	}
	beforeIndex := f.index
	a := fixtureGuard(t, f)
	f.apply(t, graphstate.Operation{Kind: graphstate.Set, Owner: 1, Life: 11, Name: "ordinary", Scope: scope, Value: graphstate.I64(8), ValueID: 100})
	middleIndex := f.index
	f.apply(t, graphstate.Operation{Kind: graphstate.Set, Owner: 1, Life: 11, Name: "ordinary", Scope: scope, Value: graphstate.I64(7), ValueID: 101})
	old := f.projection(t, beforeIndex, 1, 5, graphstate.Declared)
	middle := f.projection(t, middleIndex, 1, 5, graphstate.Declared)
	now := f.projection(t, f.index, 1, 5, graphstate.Declared)
	if len(old.Properties) != 1 || len(middle.Properties) != 1 || len(now.Properties) != 1 {
		t.Fatal(old, middle, now)
	}
	oldKey, _ := old.Properties[0].Scalar.EqualityKey(graphstate.DefaultLimits())
	middleKey, _ := middle.Properties[0].Scalar.EqualityKey(graphstate.DefaultLimits())
	newKey, _ := now.Properties[0].Scalar.EqualityKey(graphstate.DefaultLimits())
	if oldKey != newKey || oldKey == middleKey || a.SemanticEpoch == f.root.epoch {
		t.Fatal("ABA did not change retained history", old, middle, now)
	}
	if _, w, err := guardedStage(t, f, a, nil, revision, GraphLimits{}); !errors.Is(err, ErrReadConflict) || w.Records != 6 {
		t.Fatal(w, err)
	}
	current := fixtureGuard(t, f)
	f.apply(t, graphstate.Operation{Kind: graphstate.CreateNode, Owner: 2, Life: 21, Scope: scope})
	if _, _, err := guardedStage(t, f, current, nil, revision, GraphLimits{}); !errors.Is(err, ErrReadConflict) {
		t.Fatal(err)
	}
}
func TestGuardedStageMatchesOrdinaryNoopAndPhysicalOnlyEntries(t *testing.T) {
	f := newFullFixture(t, GraphLimits{})
	scope := f.span(t, 0, 10)
	ops := []graphstate.Operation{{Kind: graphstate.CreateNode, Owner: 1, Life: 11, Scope: scope}}
	g := fixtureGuard(t, f)
	revision, _ := state.NewRevision(100, 0)
	c := f.catalog(t, f.index)
	defer c.view.Close()
	ordinary, err := StageOperations(t.Context(), c, ops, revision, GraphLimits{})
	if err != nil {
		t.Fatal(err)
	}
	actual, w, err := StageGuardedOperations(t.Context(), c, g, ops, revision, GraphLimits{})
	if err != nil || w != actual.Work {
		t.Fatal(w, actual.Work, err)
	}
	normalized := actual
	normalized.Work.Bytes -= semanticGuardMetadataBytes
	normalized.OwnedBytes -= semanticGuardWorkBytes
	if !reflect.DeepEqual(normalized, ordinary) {
		t.Fatal("ordinary effect/accounting changed")
	}
	assertGuardCounters(t, c, guardCounters{})
	f.install(t, actual)
	current := fixtureGuard(t, f)
	noop, w, err := guardedStage(t, f, current, []graphstate.Operation{{Kind: graphstate.Correct, Owner: 1, Life: 11, Scope: scope, Present: true}}, revision, GraphLimits{})
	if err != nil || len(noop.Groups) != 0 || len(noop.Delta.Entities)+len(noop.Delta.Lives)+len(noop.Delta.Values) != 0 || w != noop.Work {
		t.Fatal(noop, w, err)
	}
	f.install(t, noop)
	if got := fixtureGuard(t, f); got != current {
		t.Fatal("noop advanced logical metadata")
	}
	c2 := f.catalog(t, f.index)
	defer c2.view.Close()
	base, err := c2.view.Root()
	if err != nil {
		t.Fatal(err)
	}
	physical, _, err := c2.root.ReservePhysical(3)
	if err != nil {
		t.Fatal(err)
	}
	f.install(t, GraphEffects{Base: base, Root: physical})
	if got := fixtureGuard(t, f); got != current {
		t.Fatal("physical cursor entered predicate")
	}
	after := f.catalog(t, f.index)
	defer after.view.Close()
	newBase, err := after.view.Root()
	if err != nil || newBase.Index == base.Index || newBase.ImageHash == base.ImageHash {
		t.Fatal("physical control unchanged", newBase, err)
	}
	success, w, err := StageGuardedOperations(t.Context(), after, current, nil, revision, GraphLimits{})
	if err != nil || success.Base.Index != f.index || success.Base.ImageHash != newBase.ImageHash || w != success.Work {
		t.Fatal(success, w, err)
	}
}
func TestGuardedStageResourceAndOperationalRefusalsPublishNothing(t *testing.T) {
	f := newFullFixture(t, GraphLimits{})
	scope := f.span(t, 0, 10)
	f.apply(t, graphstate.Operation{Kind: graphstate.CreateNode, Owner: 1, Life: 11, Scope: scope})
	g := fixtureGuard(t, f)
	stale := g
	stale.SemanticEpoch++
	c := f.catalog(t, f.index)
	defer c.view.Close()
	probe, err := OpenReadView(t.Context(), c, GraphLimits{})
	if err != nil {
		t.Fatal(err)
	}
	opened := probe.Work()
	probe.Close()
	beforeIndex, beforeImage, err := f.db.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	revision, _ := state.NewRevision(100, 0)
	for _, dimension := range []string{"source-rows", "source-bytes", "output"} {
		for _, delta := range []int{-1, 0, 1} {
			l := GraphLimits{}
			switch dimension {
			case "source-rows":
				l.MaxSourceRows = opened.Records + delta
			case "source-bytes":
				l.MaxSourceBytes = opened.Bytes + semanticGuardMetadataBytes + delta
			case "output":
				l.MaxOutputBytes = 512 + c.rootImageBytes + semanticGuardWorkBytes + delta
			}
			e, w, err := StageGuardedOperations(t.Context(), c, stale, nil, revision, l)
			expected := ErrReadConflict
			if delta < 0 {
				expected = ErrResourceLimit
			}
			if !errors.Is(err, expected) || !reflect.DeepEqual(e, GraphEffects{}) {
				t.Fatal(dimension, delta, e, w, err)
			}
			if delta >= 0 && w.Records != opened.Records {
				t.Fatal("conflict hid work", w)
			}
			assertGuardCounters(t, c, guardCounters{})
		}
	}
	// Fit exactly the checked Full open and guard comparison, then refuse the
	// first input ownership charge. The physical format changes open cost;
	// production defaults and the refusal boundary itself remain unchanged.
	guardOnlyBytes := opened.Bytes + semanticGuardMetadataBytes
	for _, l := range []GraphLimits{{MaxSourceBytes: guardOnlyBytes}, {Pages: PageLimits{MaxChangeBytes: 1}}, {MaxOutputBytes: 1024}} {
		e, w, err := StageGuardedOperations(t.Context(), c, g, []graphstate.Operation{{Kind: graphstate.AddLabel, Owner: 1, Life: 11, Scope: scope, Name: "late"}}, revision, l)
		if !errors.Is(err, ErrResourceLimit) || !reflect.DeepEqual(e, GraphEffects{}) || w.Records < opened.Records || w.Bytes < opened.Bytes {
			t.Fatal(e, w, err)
		}
		if l.MaxSourceBytes == guardOnlyBytes && (w.Records != opened.Records || w.Bytes != guardOnlyBytes) {
			t.Fatal("post-open refusal lost checked Full/guard work", w, opened, guardOnlyBytes)
		}
		assertGuardCounters(t, c, guardCounters{})
	}
	e, w, err := StageGuardedOperations(t.Context(), c, g, []graphstate.Operation{{Kind: graphstate.CreateNode, Owner: 1, Life: 11, Scope: scope}}, revision, GraphLimits{})
	if !errors.Is(err, graphstate.ErrAlreadyExists) || !reflect.DeepEqual(e, GraphEffects{}) || w.Records <= opened.Records {
		t.Fatal(e, w, err)
	}
	assertGuardCounters(t, c, guardCounters{})
	v, err := OpenReadView(t.Context(), c, GraphLimits{})
	if err != nil {
		t.Fatal(err)
	}
	c.limits.MaxStages = 1
	held, err := newFullStage(t.Context(), c, v.descriptor, DefaultPageLimits())
	if err != nil {
		t.Fatal(err)
	}
	v.Close()
	counters := readGuardCounters(c)
	e, w, err = StageGuardedOperations(t.Context(), c, g, nil, revision, GraphLimits{})
	if !errors.Is(err, ErrResourceLimit) || !reflect.DeepEqual(e, GraphEffects{}) || w.Records != opened.Records {
		t.Fatal(e, w, err)
	}
	assertGuardCounters(t, c, counters)
	held.Close()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	for _, test := range []struct {
		ctx      context.Context
		c        *Catalog
		l        GraphLimits
		expected error
	}{{nil, c, GraphLimits{}, ErrInvalid}, {t.Context(), nil, GraphLimits{}, ErrInvalid}, {ctx, c, GraphLimits{}, context.Canceled}, {t.Context(), c, GraphLimits{MaxSourceRows: -1}, ErrInvalid}} {
		e, w, err := StageGuardedOperations(test.ctx, test.c, g, nil, revision, test.l)
		if !errors.Is(err, test.expected) || !reflect.DeepEqual(e, GraphEffects{}) || w != (PageWork{}) {
			t.Fatal(e, w, err)
		}
	}
	afterIndex, afterImage, err := f.db.Checkpoint()
	if err != nil || beforeIndex != afterIndex || !bytes.Equal(beforeImage, afterImage) {
		t.Fatal("refusal changed persistence", err)
	}
	if err := c.view.Close(); err != nil {
		t.Fatal(err)
	}
	e, w, err = StageGuardedOperations(t.Context(), c, stale, nil, revision, GraphLimits{})
	if !errors.Is(err, raftlog.ErrClosed) || errors.Is(err, ErrReadConflict) || !reflect.DeepEqual(e, GraphEffects{}) || w != (PageWork{}) {
		t.Fatal(e, w, err)
	}
}
func TestGuardedStageEncounteredCorruptionBeatsConflictAndZeroEpoch(t *testing.T) {
	for _, name := range []string{"descriptor-corrupt", "full-zero-epoch", "keys-only"} {
		t.Run(name, func(t *testing.T) {
			f := newFullFixture(t, GraphLimits{})
			g := fixtureGuard(t, f)
			g.SemanticEpoch++
			c := f.catalog(t, f.index)
			base, err := c.view.Root()
			if err != nil {
				t.Fatal(err)
			}
			root := c.root
			var writes []raftlog.KV
			switch name {
			case "descriptor-corrupt":
				q, _ := c.reader(t.Context())
				d, _, err := q.fullDescriptor(root)
				if err != nil {
					t.Fatal(err)
				}
				wire, err := encodeFullDescriptor(d, root, c.limits)
				if err != nil {
					t.Fatal(err)
				}
				binary.BigEndian.PutUint64(wire[32:40], root.owner+1)
				writes = []raftlog.KV{{Key: componentIndexDescriptorKey(root.namespace), Value: wire}}
			case "full-zero-epoch":
				root.epoch = 0
			case "keys-only":
				root.topology = keysOnlyTopology
			}
			f.install(t, GraphEffects{Base: base, Root: root, Writes: writes})
			c.view.Close()
			if name == "full-zero-epoch" && f.root.epoch != 0 {
				t.Fatal("zero epoch fixture advanced")
			}
			bad := f.catalog(t, f.index)
			defer bad.view.Close()
			e, w, err := StageGuardedOperations(t.Context(), bad, g, nil, state.Revision{}, GraphLimits{})
			expected := ErrCorrupt
			if name == "keys-only" {
				expected = ErrTopologyUnsupported
			}
			if !errors.Is(err, expected) || errors.Is(err, ErrReadConflict) || !reflect.DeepEqual(e, GraphEffects{}) {
				t.Fatal(e, w, err)
			}
			if name == "full-zero-epoch" && w.Records != 6 {
				t.Fatal("checked Full work lost", w)
			}
			if name != "full-zero-epoch" && w != (PageWork{}) {
				t.Fatal("failed open invented work", w)
			}
			assertGuardCounters(t, bad, guardCounters{})
		})
	}
}

func TestGuardedStageNegativePredicatesRetainExactHistoricalAbsence(t *testing.T) {
	f := newFullFixture(t, GraphLimits{})
	window := f.span(t, 0, 10)
	c := f.catalog(t, f.index)
	defer c.view.Close()
	view, err := OpenReadView(t.Context(), c, GraphLimits{})
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	guard, err := NewSemanticGuard(c.root)
	if err != nil {
		t.Fatal(err)
	}
	assertEmpty := func() {
		t.Helper()
		entity, err := view.Entity(t.Context(), 1)
		if err != nil || entity.Found {
			t.Fatal(entity, err)
		}
		keys, err := view.ComponentKeys(t.Context(), graphstate.KeyPredicate{Owner: 1}, 0, graphstate.ReadBudget{Rows: 512, Bytes: 4 << 20})
		if err != nil || !keys.Complete || keys.Next != 0 || len(keys.Keys) != 0 {
			t.Fatal(keys, err)
		}
		claims, err := view.UniqueCandidates(t.Context(), graphstate.UniquePredicate{Definition: fullSchemas()[0], Value: graphstate.I64(7), Window: window}, 0, graphstate.ReadBudget{Rows: 512, Bytes: 4 << 20})
		if err != nil || !claims.Complete || claims.Next != 0 || len(claims.Claims) != 0 {
			t.Fatal(claims, err)
		}
		edges, err := view.IncidentRelationships(t.Context(), graphstate.IncidentPredicate{Endpoint: 1, Life: 11, Window: window}, 0, graphstate.ReadBudget{Rows: 512, Bytes: 4 << 20})
		if err != nil || !edges.Complete || edges.Next != 0 || len(edges.Entities) != 0 {
			t.Fatal(edges, err)
		}
	}
	assertEmpty()
	f.apply(t, graphstate.Operation{Kind: graphstate.CreateNode, Owner: 1, Life: 11, Scope: window}, graphstate.Operation{Kind: graphstate.CreateNode, Owner: 2, Life: 21, Scope: window}, graphstate.Operation{Kind: graphstate.Set, Owner: 1, Life: 11, Scope: window, Name: "scalar", Value: graphstate.I64(7), ValueID: 99}, graphstate.Operation{Kind: graphstate.CreateRelationship, Owner: 3, Life: 31, Scope: window, Record: graphstate.EntityRecord{Type: "R", Source: 1, Target: 2, Mode: graphstate.LifeBound}, Binding: graphstate.LifeRecord{SourceLife: 11, TargetLife: 21}})
	// Every old point/predicate still answers exact absence after insertion.
	assertEmpty()
	current := f.view(t, f.index)
	defer current.Close()
	defer current.c.view.Close()
	keys, err := current.ComponentKeys(t.Context(), graphstate.KeyPredicate{Owner: 1}, 0, graphstate.ReadBudget{Rows: 512, Bytes: 4 << 20})
	if err != nil || !keys.Complete || len(keys.Keys) != 2 {
		t.Fatal(keys, err)
	}
	claims, err := current.UniqueCandidates(t.Context(), graphstate.UniquePredicate{Definition: fullSchemas()[0], Value: graphstate.I64(7), Window: window}, 0, graphstate.ReadBudget{Rows: 512, Bytes: 4 << 20})
	if err != nil || !claims.Complete || len(claims.Claims) != 1 || claims.Claims[0].Owner != 1 {
		t.Fatal(claims, err)
	}
	edges, err := current.IncidentRelationships(t.Context(), graphstate.IncidentPredicate{Endpoint: 1, Life: 11, Window: window}, 0, graphstate.ReadBudget{Rows: 512, Bytes: 4 << 20})
	if err != nil || !edges.Complete || !reflect.DeepEqual(edges.Entities, []graphstate.EntityID{3}) {
		t.Fatal(edges, err)
	}
	revision, _ := state.NewRevision(100, 0)
	if e, _, err := guardedStage(t, f, guard, nil, revision, GraphLimits{}); !errors.Is(err, ErrReadConflict) || !reflect.DeepEqual(e, GraphEffects{}) {
		t.Fatal(e, err)
	}
}

func TestGuardedStageCancellationAfterFullBeatsConflict(t *testing.T) {
	f := newFullFixture(t, GraphLimits{})
	g := fixtureGuard(t, f)
	g.SemanticEpoch++
	c := f.catalog(t, f.index)
	defer c.view.Close()
	count := &fullCancelContext{Context: t.Context()}
	_, expected, err := StageGuardedOperations(count, c, g, nil, state.Revision{}, GraphLimits{})
	if !errors.Is(err, ErrReadConflict) {
		t.Fatal(err)
	}
	canceled := &fullCancelContext{Context: t.Context(), cancelAt: count.calls}
	e, w, err := StageGuardedOperations(canceled, c, g, nil, state.Revision{}, GraphLimits{})
	if !errors.Is(err, context.Canceled) || errors.Is(err, ErrReadConflict) || !reflect.DeepEqual(e, GraphEffects{}) || w != expected {
		t.Fatal(e, w, expected, err)
	}
	assertGuardCounters(t, c, guardCounters{})
}
