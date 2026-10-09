package graphstore

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"slices"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/assertion"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func associationFixture(t *testing.T) (*raftlog.Store, Root, *Catalog, temporal.Axis) {
	t.Helper()
	db, root := newStore(t, vfs.NewMem())
	c := openCatalog(t, db, 1, Limits{})
	s := stage(t, c)
	axis := testAxis(t, 1, temporal.ProfileRationalQ)
	for _, r := range []graphstate.EntityRecord{
		{ID: 1, Kind: graphstate.Node, Axis: axis},
		{ID: 2, Kind: graphstate.Node, Axis: axis},
		{ID: 3, Kind: graphstate.Relationship, Axis: axis, Type: "PRECEDES", Source: 1, Target: 2, Mode: graphstate.IdentityReference},
	} {
		if err := s.Entity(t.Context(), EntityRef{testNamespace().Graph, r.ID}, r); err != nil {
			t.Fatal(err)
		}
		if err := s.Life(t.Context(), LifeRef{testNamespace().Graph, r.ID, 1}, graphstate.LifeRecord{Owner: r.ID, Life: 1}); err != nil {
			t.Fatal(err)
		}
	}
	for _, kind := range []graphstate.EntityKind{graphstate.Node, graphstate.Relationship} {
		for _, d := range []graphstate.PropertyDefinition{
			{Name: "scalar", Owner: kind, Type: graphstate.ScalarString, Cardinality: graphstate.ScalarCardinality},
			{Name: "members", Owner: kind, Type: graphstate.ScalarString, Cardinality: graphstate.SetCardinality},
		} {
			if err := s.Property(t.Context(), d); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := s.Value(t.Context(), refValue(7), graphstate.String("member")); err != nil {
		t.Fatal(err)
	}
	root, index := commitStage(t, db, root, s)
	// These are actual durable immutable catalogs, not graph presence/projection.
	return db, root, openCatalog(t, db, index, Limits{}), axis
}

func associationPoint(t testing.TB, axis temporal.Axis, value int64) temporal.Scope {
	t.Helper()
	p, err := temporal.RationalPosition(axis, temporal.RationalInt64(value))
	if err != nil {
		t.Fatal(err)
	}
	s, err := temporal.Point(p)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func associationRevision(t testing.TB, id uint64) state.Revision {
	t.Helper()
	r, err := state.NewRevision(id, 10+id)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func associationSpec(t testing.TB, id assertion.ID, target assertion.Target, kind assertion.PlacementKind, axis temporal.Axis) assertion.Spec {
	t.Helper()
	s := assertion.Spec{Ref: assertion.Ref{Graph: testNamespace().Graph, ID: id}, Target: target, Revision: associationRevision(t, 1), Interpretation: assertion.Occurrence, Placement: assertion.Placement{Kind: kind}, Knowledge: assertion.Knowledge{Kind: assertion.NoKnowledge}}
	switch kind {
	case assertion.NativePlacement:
		s.Role = assertion.OccurrenceTime
		s.Placement.Native = associationPoint(t, axis, 12)
	case assertion.SymbolicPlacement:
		s.Role = "symbolic-occurrence"
		d, err := temporal.PreserveOpaqueDescriptor(temporal.OpaqueDescriptorSpec{ID: temporal.DescriptorID{byte(id)}, Type: "schedule", SchemaVersion: 99, Correlation: temporal.CorrelationID{8}, Payload: []byte("every weekday")}, temporal.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		s.Placement.Symbolic = d
	}
	return s
}

func associationWire(t testing.TB, r assertion.Record) []byte {
	t.Helper()
	b, err := assertion.AppendRecord(nil, r, assertion.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func associationSpecWire(t testing.TB, s assertion.Spec) []byte {
	t.Helper()
	r, err := assertion.New(s, assertion.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	return associationWire(t, r)
}

func associationAssertRead(t *testing.T, c *Catalog, want assertion.Spec) {
	t.Helper()
	r, err := c.Association(t.Context(), want.Ref, AssociationLimits{})
	if err != nil || !r.Found {
		t.Fatal("current association", err)
	}
	expected, err := assertion.New(want, assertion.Limits{})
	if err != nil || !bytes.Equal(associationWire(t, expected), associationWire(t, r.Record)) {
		t.Fatal("complete association changed", err)
	}
}

func TestAssociationAtomicBatchMultiplicityAndNamedPrimaryReplay(t *testing.T) {
	db, root, c, axis := associationFixture(t)
	target := assertion.Target{Kind: assertion.EntityTarget, Entity: 1}
	one := associationSpec(t, 10, target, assertion.NativePlacement, axis)
	two := associationSpec(t, 11, target, assertion.NativePlacement, axis)
	axeless := associationSpec(t, 12, target, assertion.NoAssociation, temporal.Axis{})
	symbolic := associationSpec(t, 13, target, assertion.SymbolicPlacement, temporal.Axis{})
	s := stage(t, c)
	result, err := s.Associations(t.Context(), []AssociationWrite{{Spec: one}, {Spec: two}, {Spec: axeless}, {Spec: symbolic}}, AssociationLimits{})
	if err != nil || len(result.Transitions) != 4 || len(result.Bindings) != 0 {
		t.Fatal("batch collapsed distinct event IDs", err)
	}
	root, index := commitStage(t, db, root, s)
	c = openCatalog(t, db, index, Limits{})
	for _, spec := range []assertion.Spec{one, two, axeless, symbolic} {
		associationAssertRead(t, c, spec)
	}
	if p, err := c.Primary(t.Context(), refEntity(1), AssociationLimits{}); err != nil || p.Bound {
		t.Fatal("first target posting silently became primary", err)
	}
	s = stage(t, c)
	result, err = s.Associations(t.Context(), []AssociationWrite{{Spec: two, PrimaryFor: refEntity(1)}}, AssociationLimits{})
	if err != nil || len(result.Transitions) != 1 || !result.Transitions[0].Replay() || len(result.Bindings) != 1 || result.Bindings[0].Before != (assertion.Ref{}) || result.Bindings[0].After != two.Ref {
		t.Fatal("current replay hid a real binding creation", err)
	}
	rows, err := s.Writes()
	if err != nil || len(rows) != 1 || !bytes.Equal(rows[0].Key, associationPrimaryKey(testNamespace(), 1)) {
		t.Fatal("binding replay rewrote association history", err)
	}
	_, index = commitStage(t, db, root, s)
	c = openCatalog(t, db, index, Limits{})
	p, err := c.Primary(t.Context(), refEntity(1), AssociationLimits{})
	if err != nil || !p.Bound || p.Ref != two.Ref || !p.Association.Found {
		t.Fatal("named primary lookup picked other event", err)
	}
	s = stage(t, c)
	before, _ := s.Writes()
	result, err = s.Associations(t.Context(), []AssociationWrite{{Spec: one, PrimaryFor: refEntity(1)}}, AssociationLimits{})
	after, _ := s.Writes()
	if !errors.Is(err, ErrRebinding) || !reflect.DeepEqual(result, AssociationResult{}) || !reflect.DeepEqual(before, after) {
		t.Fatal("immutable binding rebound", err)
	}
}

func TestAssociationTargetValidationNodeRelationshipAndComponents(t *testing.T) {
	_, _, c, axis := associationFixture(t)
	s := stage(t, c)
	for _, owner := range []graphstate.EntityID{1, 3} {
		for _, key := range []graphstate.ComponentKey{
			{Owner: owner, Kind: graphstate.Presence},
			{Owner: owner, Life: 1, Kind: graphstate.ScalarProperty, Name: "scalar"},
			{Owner: owner, Life: 1, Kind: graphstate.SetMember, Name: "members", Member: 7},
		} {
			id := assertion.ID(100 + owner*10 + graphstate.EntityID(key.Kind))
			spec := associationSpec(t, id, assertion.Target{Kind: assertion.ComponentTarget, Component: key}, assertion.NoAssociation, temporal.Axis{})
			if _, err := s.Associations(t.Context(), []AssociationWrite{{Spec: spec}}, AssociationLimits{}); err != nil {
				t.Fatal("addressable absent component refused", key, err)
			}
		}
	}
	for _, owner := range []graphstate.EntityID{1, 3} {
		relation := associationSpec(t, assertion.ID(200+owner), assertion.Target{Kind: assertion.EntityTarget, Entity: owner}, assertion.NoAssociation, temporal.Axis{})
		relation.Interpretation = assertion.AssertedRelation
		_, err := s.Associations(t.Context(), []AssociationWrite{{Spec: relation}}, AssociationLimits{})
		if owner == 1 {
			if !errors.Is(err, graphstate.ErrTypeMismatch) || !errors.Is(err, ErrInvalid) {
				t.Fatal("asserted_relation accepted node target", err)
			}
		} else if err != nil {
			t.Fatal("explicit relationship fact refused", err)
		}
	}
	for _, tc := range []struct {
		target assertion.Target
		want   error
	}{
		{assertion.Target{Kind: assertion.EntityTarget, Entity: 999}, graphstate.ErrNotFound},
		{assertion.Target{Kind: assertion.ComponentTarget, Component: graphstate.ComponentKey{Owner: 1, Life: 99, Kind: graphstate.Label, Name: "x"}}, graphstate.ErrNotFound},
		{assertion.Target{Kind: assertion.ComponentTarget, Component: graphstate.ComponentKey{Owner: 3, Life: 1, Kind: graphstate.Label, Name: "x"}}, graphstate.ErrTypeMismatch},
		{assertion.Target{Kind: assertion.ComponentTarget, Component: graphstate.ComponentKey{Owner: 1, Life: 1, Kind: graphstate.ScalarProperty, Name: "missing"}}, graphstate.ErrSchemaMismatch},
		{assertion.Target{Kind: assertion.ComponentTarget, Component: graphstate.ComponentKey{Owner: 1, Life: 1, Kind: graphstate.ScalarProperty, Name: "members"}}, graphstate.ErrTypeMismatch},
		{assertion.Target{Kind: assertion.ComponentTarget, Component: graphstate.ComponentKey{Owner: 1, Life: 1, Kind: graphstate.SetMember, Name: "members", Member: 999}}, graphstate.ErrNotFound},
	} {
		spec := associationSpec(t, 240, tc.target, assertion.NativePlacement, axis)
		before, _ := s.Writes()
		result, err := s.Associations(t.Context(), []AssociationWrite{{Spec: spec}}, AssociationLimits{})
		after, _ := s.Writes()
		if !errors.Is(err, tc.want) || !reflect.DeepEqual(result, AssociationResult{}) || !reflect.DeepEqual(before, after) {
			t.Fatal("target refusal mutated stage or lost sentinel", tc.target, err)
		}
	}
}

func TestAssociationBoundQueriesAndCursorIsolation(t *testing.T) {
	db, root, c, axis := associationFixture(t)
	target := assertion.Target{Kind: assertion.EntityTarget, Entity: 1}
	s := stage(t, c)
	for i, placement := range []assertion.PlacementKind{assertion.NoAssociation, assertion.NativePlacement, assertion.SymbolicPlacement} {
		spec := associationSpec(t, assertion.ID(10+i), target, placement, axis)
		if _, err := s.Associations(t.Context(), []AssociationWrite{{Spec: spec}}, AssociationLimits{}); err != nil {
			t.Fatal(err)
		}
	}
	root, index := commitStage(t, db, root, s)
	c = openCatalog(t, db, index, Limits{})
	query := AssociationQuery{Graph: testNamespace().Graph, Target: target}
	budget := graphstate.ReadBudget{Rows: 512, Bytes: 4 << 20}
	page, err := c.Associations(t.Context(), query, nil, budget, AssociationLimits{})
	if err != nil || page.Complete || len(page.Next) == 0 || len(page.Records) != 1 || page.Records[0].Spec().Ref.ID != 10 || page.Visited < 6 || len(page.Next) != cap(page.Next) {
		t.Fatal("first bounded association page", page, err)
	}
	firstCursor := bytes.Clone(page.Next)
	ids := []assertion.ID{10}
	for !page.Complete {
		page, err = c.Associations(t.Context(), query, page.Next, budget, AssociationLimits{})
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range page.Records {
			ids = append(ids, r.Spec().Ref.ID)
		}
	}
	if !slices.Equal(ids, []assertion.ID{10, 11, 12}) || len(page.Next) != 0 {
		t.Fatal("enumeration omitted/duplicated distinct supplied identities", ids)
	}
	for _, edit := range []func(*AssociationQuery){
		func(q *AssociationQuery) { q.Interpretation = assertion.State },
		func(q *AssociationQuery) { q.Placement = assertion.NativePlacement },
		func(q *AssociationQuery) { q.Target.Entity = 2 },
	} {
		other := query
		edit(&other)
		if got, err := c.Associations(t.Context(), other, firstCursor, budget, AssociationLimits{}); !errors.Is(err, ErrInvalid) || !reflect.DeepEqual(got, AssociationPage{}) {
			t.Fatal("cursor crossed complete query", err)
		}
	}
	for _, offset := range []int{0, 19, 27, 35, 67, 99} {
		bad := bytes.Clone(firstCursor)
		bad[offset] ^= 255
		if _, err := c.Associations(t.Context(), query, bad, budget, AssociationLimits{}); !errors.Is(err, ErrInvalid) {
			t.Fatal("cursor framing/root/namespace tamper", offset, err)
		}
	}
	s = stage(t, c)
	if err := s.Entity(t.Context(), refEntity(9), graphstate.EntityRecord{ID: 9, Kind: graphstate.Node, Axis: axis}); err != nil {
		t.Fatal(err)
	}
	root, later := commitStage(t, db, root, s)
	newer := openCatalog(t, db, later, Limits{})
	if _, err := newer.Associations(t.Context(), query, firstCursor, budget, AssociationLimits{}); !errors.Is(err, ErrInvalid) {
		t.Fatal("cursor crossed immutable application view", err)
	}
	// Old view remains resumable after a new commit; source input is unchanged.
	if _, err := c.Associations(t.Context(), query, firstCursor, budget, AssociationLimits{}); err != nil {
		t.Fatal("retained cursor became latest-only", err)
	}
	query.Placement, query.NativeWindow = assertion.NativePlacement, associationPoint(t, axis, 12)
	ids = nil
	var next []byte
	filteredVisits := 0
	for {
		p, err := c.Associations(t.Context(), query, next, budget, AssociationLimits{})
		if err != nil {
			t.Fatal(err)
		}
		filteredVisits += p.Visited
		for _, r := range p.Records {
			ids = append(ids, r.Spec().Ref.ID)
		}
		if p.Complete {
			break
		}
		next = p.Next
	}
	if !slices.Equal(ids, []assertion.ID{11}) || filteredVisits < 18 {
		t.Fatal("native residual/filtered row accounting", ids, filteredVisits)
	}
	native, err := newer.Association(t.Context(), assertion.Ref{Graph: query.Graph, ID: 11}, AssociationLimits{})
	if err != nil {
		t.Fatal(err)
	}
	corrected := native.Record.Spec()
	corrected.Previous, corrected.Revision = 1, associationRevision(t, 2)
	corrected.Placement.Native = associationPoint(t, axis, 13)
	s = stage(t, newer)
	if _, err := s.Associations(t.Context(), []AssociationWrite{{Spec: corrected}}, AssociationLimits{}); err != nil {
		t.Fatal(err)
	}
	_, changedIndex := commitStage(t, db, root, s)
	changed := openCatalog(t, db, changedIndex, Limits{})
	selectIDs := func(view *Catalog, coordinate int64) []assertion.ID {
		windowQuery := query
		windowQuery.NativeWindow = associationPoint(t, axis, coordinate)
		var next []byte
		result := []assertion.ID{}
		for {
			p, err := view.Associations(t.Context(), windowQuery, next, budget, AssociationLimits{})
			if err != nil {
				t.Fatal(err)
			}
			for _, r := range p.Records {
				result = append(result, r.Spec().Ref.ID)
			}
			if p.Complete {
				return result
			}
			next = p.Next
		}
	}
	if !slices.Equal(selectIDs(c, 12), []assertion.ID{11}) || len(selectIDs(changed, 12)) != 0 || !slices.Equal(selectIDs(changed, 13), []assertion.ID{11}) {
		t.Fatal("historical generic selection substituted latest placement")
	}
}

func TestAssociationAdmissionRefusalsDoNotPoisonOrExposePartialOutput(t *testing.T) {
	db, root, c, axis := associationFixture(t)
	spec := associationSpec(t, 10, assertion.Target{Kind: assertion.EntityTarget, Entity: 1}, assertion.NativePlacement, axis)
	s := stage(t, c)
	if _, err := s.Associations(t.Context(), []AssociationWrite{{Spec: spec, PrimaryFor: refEntity(1)}}, AssociationLimits{}); err != nil {
		t.Fatal(err)
	}
	_, index := commitStage(t, db, root, s)
	c = openCatalog(t, db, index, Limits{})
	for _, policy := range []AssociationLimits{
		{Record: assertion.Limits{Temporal: temporal.Limits{MaxMagnitudeBits: 1}}},
		{MaxReadRows: 1}, {MaxReadBytes: 117}, {MaxReadBytes: 213}, {MaxOutputBytes: 64},
	} {
		got, err := c.Association(t.Context(), spec.Ref, policy)
		if !errors.Is(err, ErrResourceLimit) || errors.Is(err, ErrCorrupt) || !reflect.DeepEqual(got, AssociationRead{}) {
			t.Fatal("tighter legal read policy became corruption/partial result", err)
		}
		associationAssertRead(t, c, spec)
	}
	if p, err := c.Primary(t.Context(), refEntity(1), AssociationLimits{MaxOutputBytes: 64}); !errors.Is(err, ErrResourceLimit) || !reflect.DeepEqual(p, PrimaryRead{}) {
		t.Fatal("primary output bound", err)
	}
	if _, err := c.Primary(t.Context(), refEntity(1), AssociationLimits{}); err != nil {
		t.Fatal("primary resource refusal poisoned normal read", err)
	}
	query := AssociationQuery{Graph: testNamespace().Graph, Target: spec.Target}
	if p, err := c.Associations(t.Context(), query, nil, graphstate.ReadBudget{Rows: 1, Bytes: 4096}, AssociationLimits{}); !errors.Is(err, ErrResourceLimit) || !reflect.DeepEqual(p, AssociationPage{}) {
		t.Fatal("budget boundary labeled incomplete materialization complete", err)
	}
	foreign := query
	foreign.Graph[0] = 2
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := c.Associations(ctx, foreign, nil, graphstate.ReadBudget{}, AssociationLimits{}); !errors.Is(err, ErrNamespace) {
		t.Fatal("foreign namespace did work before rejection", err)
	}
	if _, err := c.Association(ctx, assertion.Ref{Graph: foreign.Graph, ID: 10}, AssociationLimits{}); !errors.Is(err, ErrNamespace) {
		t.Fatal("foreign direct lookup did work", err)
	}
	if _, err := c.Association(ctx, spec.Ref, AssociationLimits{}); !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled read lost sentinel", err)
	}
	for _, operation := range []func() error{
		func() error {
			_, err := c.Association(t.Context(), assertion.Ref{Graph: spec.Ref.Graph}, AssociationLimits{})
			return err
		},
		func() error {
			_, err := c.AssociationRevision(t.Context(), spec.Ref, 0, AssociationLimits{})
			return err
		},
		func() error {
			_, err := c.Primary(t.Context(), EntityRef{Graph: spec.Ref.Graph}, AssociationLimits{})
			return err
		},
		func() error {
			_, err := c.Associations(t.Context(), query, nil, graphstate.ReadBudget{}, AssociationLimits{})
			return err
		},
	} {
		if err := operation(); !errors.Is(err, ErrInvalid) {
			t.Fatal("invalid public input lost sentinel", err)
		}
	}
	if r, err := c.Association(t.Context(), assertion.Ref{Graph: spec.Ref.Graph, ID: 999}, AssociationLimits{}); err != nil || r.Found {
		t.Fatal("phantom assertion returns nonempty", err)
	}
	if r, err := c.AssociationRevision(t.Context(), spec.Ref, 999, AssociationLimits{}); err != nil || r.Found {
		t.Fatal("phantom revision returns nonempty", err)
	}
	associationAssertRead(t, c, spec)
}

func TestAssociationStageAdditionalRefusalsAndIndependentAxes(t *testing.T) {
	_, _, c, axis := associationFixture(t)
	s := stage(t, c)
	spec := associationSpec(t, 10, assertion.Target{Kind: assertion.EntityTarget, Entity: 1}, assertion.NativePlacement, axis)
	for _, tc := range []struct {
		writes []AssociationWrite
		limits AssociationLimits
		want   error
	}{
		{[]AssociationWrite{{Spec: spec}}, AssociationLimits{MaxOperations: -1}, ErrInvalid},
		{[]AssociationWrite{{Spec: spec}, {Spec: spec}}, AssociationLimits{MaxOperations: 1}, ErrResourceLimit},
		{[]AssociationWrite{{Spec: spec}}, AssociationLimits{MaxReadBytes: 1}, ErrResourceLimit},
		{[]AssociationWrite{{Spec: spec}}, AssociationLimits{MaxOutputBytes: 1}, ErrResourceLimit},
		{[]AssociationWrite{{Spec: spec, PrimaryFor: EntityRef{Graph: testNamespace().Graph}}}, AssociationLimits{}, ErrInvalid},
		{[]AssociationWrite{{Spec: spec, PrimaryFor: EntityRef{Graph: graphstate.GraphID{2}, ID: 1}}}, AssociationLimits{}, ErrNamespace},
		{[]AssociationWrite{{Spec: spec, PrimaryFor: refEntity(2)}}, AssociationLimits{}, assertion.ErrRebinding},
		{[]AssociationWrite{{Spec: assertion.Spec{Ref: spec.Ref}}}, AssociationLimits{}, assertion.ErrInvalid},
	} {
		before, _ := s.Writes()
		result, err := s.Associations(t.Context(), tc.writes, tc.limits)
		after, _ := s.Writes()
		if !errors.Is(err, tc.want) || !reflect.DeepEqual(result, AssociationResult{}) || !reflect.DeepEqual(before, after) {
			t.Fatal("admission error changed stage or lost sentinel", err)
		}
	}
	other := testAxis(t, 2, temporal.ProfileRationalQ)
	independent := associationSpec(t, 11, spec.Target, assertion.NativePlacement, other)
	second := associationSpec(t, 12, spec.Target, assertion.NativePlacement, other)
	if _, err := s.Associations(t.Context(), []AssociationWrite{{Spec: independent}, {Spec: second}}, AssociationLimits{}); err != nil {
		t.Fatal("independent association inherited owner's different life axis", err)
	}
	rows, err := s.Writes()
	if err != nil {
		t.Fatal(err)
	}
	axisRows := 0
	for _, row := range rows {
		if row.Key[0] == byte(axisRecord) {
			axisRows++
		}
	}
	if axisRows != 1 {
		t.Fatal("shared axis definition staged more than once", axisRows)
	}
	conflicting := other.Descriptor()
	conflicting.Reference = "conflicting-definition"
	changedAxis, err := temporal.NewAxis(conflicting, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	conflict := associationSpec(t, 13, spec.Target, assertion.NativePlacement, changedAxis)
	if result, err := s.Associations(t.Context(), []AssociationWrite{{Spec: conflict}}, AssociationLimits{}); !errors.Is(err, ErrRebinding) || !reflect.DeepEqual(result, AssociationResult{}) {
		t.Fatal("association rebound an immutable axis definition", err)
	}
	before, _ := s.Writes()
	if result, err := s.Associations(t.Context(), []AssociationWrite{{Spec: independent, PrimaryFor: refEntity(1)}}, AssociationLimits{}); !errors.Is(err, temporal.ErrAxisMismatch) || !reflect.DeepEqual(result, AssociationResult{}) {
		t.Fatal("primary rebound to unrelated life axis", err)
	}
	after, _ := s.Writes()
	if !reflect.DeepEqual(before, after) {
		t.Fatal("primary axis refusal changed stage")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if result, err := s.Associations(t.Context(), []AssociationWrite{{Spec: spec}}, AssociationLimits{}); !errors.Is(err, ErrClosed) || !reflect.DeepEqual(result, AssociationResult{}) {
		t.Fatal("closed stage admitted associations", err)
	}
	if _, err := (*Stage)(nil).Associations(t.Context(), nil, AssociationLimits{}); !errors.Is(err, ErrInvalid) {
		t.Fatal("nil stage", err)
	}
	if _, err := (*Catalog)(nil).Association(t.Context(), spec.Ref, AssociationLimits{}); !errors.Is(err, ErrInvalid) {
		t.Fatal("nil catalog", err)
	}
}

func TestAssociationQueryRefusalsDoNotClaimOccupancy(t *testing.T) {
	db, root, c, axis := associationFixture(t)
	u, err := temporal.Unplaced(axis)
	if err != nil {
		t.Fatal(err)
	}
	support := associationPoint(t, axis, 12)
	hard, err := temporal.HardPointKnowledge(support, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	spec := associationSpec(t, 10, assertion.Target{Kind: assertion.EntityTarget, Entity: 1}, assertion.NativePlacement, axis)
	spec.Placement.Native = u
	spec.Knowledge = assertion.Knowledge{Kind: assertion.PointKnowledge, Point: hard}
	s := stage(t, c)
	if _, err := s.Associations(t.Context(), []AssociationWrite{{Spec: spec}}, AssociationLimits{}); err != nil {
		t.Fatal(err)
	}
	_, index := commitStage(t, db, root, s)
	c = openCatalog(t, db, index, Limits{})
	r, err := c.Association(t.Context(), spec.Ref, AssociationLimits{})
	if err != nil || r.Record.Spec().Placement.Native.Kind() != temporal.ScopeUnplaced || r.Record.Spec().Knowledge.Point.Kind() != temporal.PointKnowledgeHardSupport {
		t.Fatal("knowledge became occupied placement", err)
	}
	possible, err := r.Record.Spec().Knowledge.Point.PossibleIn(support, temporal.Limits{})
	if err != nil || !possible.Holds {
		t.Fatal("explicit knowledge predicate changed", err)
	}
	base := AssociationQuery{Graph: testNamespace().Graph, Target: spec.Target}
	budget := graphstate.ReadBudget{Rows: 512, Bytes: 4 << 20}
	for _, tc := range []struct {
		query AssociationQuery
		want  error
	}{
		{AssociationQuery{Graph: base.Graph}, ErrInvalid},
		{AssociationQuery{Graph: base.Graph, Target: base.Target, Interpretation: 7}, ErrInvalid},
		{AssociationQuery{Graph: base.Graph, Target: base.Target, Placement: 9}, ErrInvalid},
		{AssociationQuery{Graph: base.Graph, Target: base.Target, NativeWindow: support}, temporal.ErrUnsupportedPredicate},
		{AssociationQuery{Graph: base.Graph, Target: base.Target, Placement: assertion.NativePlacement, NativeWindow: u}, temporal.ErrUnsupportedPredicate},
		{AssociationQuery{Graph: base.Graph, Target: base.Target, Placement: assertion.NativePlacement, NativeWindow: support}, temporal.ErrUnplacedScope},
	} {
		p, err := c.Associations(t.Context(), tc.query, nil, budget, AssociationLimits{})
		if !errors.Is(err, tc.want) || !reflect.DeepEqual(p, AssociationPage{}) {
			t.Fatal("unsupported/malformed selector made occupancy claim", err)
		}
		associationAssertRead(t, c, spec)
	}
	filtered := base
	filtered.Interpretation = assertion.State
	p, err := c.Associations(t.Context(), filtered, nil, budget, AssociationLimits{})
	if err != nil || !p.Complete || len(p.Records) != 0 || p.Visited < 6 {
		t.Fatal("filtered complete-empty row not accounted", err)
	}
	base.Target.Entity = 999
	p, err = c.Associations(t.Context(), base, nil, budget, AssociationLimits{})
	if err != nil || !p.Complete || len(p.Records) != 0 {
		t.Fatal("phantom target enumeration", err)
	}
}
