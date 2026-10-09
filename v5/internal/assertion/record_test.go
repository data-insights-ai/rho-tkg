package assertion

import (
	"bytes"
	"errors"
	"slices"
	"sync"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func testAxis(t testing.TB, profile temporal.Profile, id byte, reference string) temporal.Axis {
	t.Helper()
	a, err := temporal.NewAxis(temporal.AxisDescriptor{ID: temporal.AxisID{id}, Profile: profile, Version: 1, Reference: reference, CanonicalUnit: "abstract"}, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func testRevision(t testing.TB, id, provenance uint64) state.Revision {
	t.Helper()
	r, err := state.NewRevision(id, provenance)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func testPosition(t testing.TB, axis temporal.Axis, value int64) temporal.Position {
	t.Helper()
	var p temporal.Position
	var err error
	switch axis.Descriptor().Profile {
	case temporal.ProfileIntegerZ:
		p, err = temporal.IntegerPosition(axis, temporal.Int64(value))
	case temporal.ProfileRationalQ:
		p, err = temporal.RationalPosition(axis, temporal.RationalInt64(value))
	case temporal.ProfileLexicographicQN:
		p, err = temporal.LexPosition(axis, temporal.RationalInt64(value), temporal.Int64(0))
	}
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func testPoint(t testing.TB, axis temporal.Axis, value int64) temporal.Scope {
	t.Helper()
	s, err := temporal.Point(testPosition(t, axis, value))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func testDescriptor(t testing.TB, id byte, payload []byte) temporal.OpaqueDescriptor {
	t.Helper()
	d, err := temporal.PreserveOpaqueDescriptor(temporal.OpaqueDescriptorSpec{ID: temporal.DescriptorID{id}, Type: "symbolic-placement", SchemaVersion: 99, Correlation: temporal.CorrelationID{7}, References: []temporal.DescriptorReference{{Role: "latent", ID: temporal.DescriptorID{9}, Type: "shared-clock", SchemaVersion: 3, Integrity: [32]byte{8}}}, Payload: payload}, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func testSpec(t testing.TB, owner graphstate.EntityID, placement Placement) Spec {
	t.Helper()
	s := Spec{Ref: Ref{Graph: graphstate.GraphID{1}, ID: ID(owner)}, Target: Target{Kind: EntityTarget, Entity: owner}, Revision: testRevision(t, 1, 11), Interpretation: Occurrence, Placement: placement, Knowledge: Knowledge{Kind: NoKnowledge}}
	if placement.Kind != NoAssociation {
		s.Role = OccurrenceTime
	}
	return s
}

func mustRecord(t testing.TB, spec Spec) Record {
	t.Helper()
	r, err := New(spec, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func recordWire(t testing.TB, r Record) []byte {
	t.Helper()
	b, err := AppendRecord(nil, r, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func assertRefusal(t testing.TB, r Record, next Spec, l Limits, want error) {
	t.Helper()
	before := recordWire(t, r)
	x, err := Apply(r, next, l)
	if !errors.Is(err, want) || x.After().Spec().Ref != (Ref{}) || x.Replay() {
		t.Fatalf("refusal returned result or wrong sentinel: %v %#v", err, x)
	}
	if _, ok := x.Before(); ok || !bytes.Equal(before, recordWire(t, r)) {
		t.Fatal("refusal exposed a partial transition or changed predecessor")
	}
}

func TestEventMultiplicityAndInterpretationIndependentOfSupport(t *testing.T) {
	axis := testAxis(t, temporal.ProfileIntegerZ, 1, "source")
	point := testPoint(t, axis, 0)
	lo, err := temporal.FiniteBound(testPosition(t, axis, 0), true)
	if err != nil {
		t.Fatal(err)
	}
	hi, err := temporal.FiniteBound(testPosition(t, axis, 1), false)
	if err != nil {
		t.Fatal(err)
	}
	span, err := temporal.Span(axis, lo, hi, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if equal, err := point.SameSupport(span, temporal.Limits{}); err != nil || !equal {
		t.Fatal("Z singleton equivalence", equal, err)
	}
	first := testSpec(t, 1, Placement{Kind: NativePlacement, Native: point})
	second := testSpec(t, 2, Placement{Kind: NativePlacement, Native: point})
	stateSpec := testSpec(t, 3, Placement{Kind: NativePlacement, Native: span})
	stateSpec.Interpretation, stateSpec.Role = State, Validity
	records := []Record{mustRecord(t, first), mustRecord(t, second), mustRecord(t, stateSpec)}
	selected := func(at int64) []ID {
		ids := []ID{}
		for _, r := range records {
			s := r.Spec()
			contains, err := s.Placement.Native.Contains(testPosition(t, axis, at), temporal.Limits{})
			if err != nil {
				t.Fatal(err)
			}
			if contains && !s.Retracted {
				ids = append(ids, s.Ref.ID)
			}
		}
		return ids
	}
	if !slices.Equal(selected(0), []ID{1, 2, 3}) || len(selected(1)) != 0 {
		t.Fatal("event IDs collapsed or point implied persistence")
	}
	if records[0].Spec().Interpretation == records[2].Spec().Interpretation || records[0].Spec().Role == records[2].Spec().Role {
		t.Fatal("equal support erased interpretation/role")
	}
	withdraw := first
	withdraw.Previous, withdraw.Revision, withdraw.Retracted = 1, testRevision(t, 2, 22), true
	x, err := Apply(records[0], withdraw, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	old := records[0]
	records[0] = x.After()
	if !slices.Equal(selected(0), []ID{2, 3}) || old.Spec().Retracted {
		t.Fatal("one retraction affected other event or old revision")
	}
}

func TestTwoPhaseAssociationCorrectionsNodeRelationshipAndComponentTargets(t *testing.T) {
	axis := testAxis(t, temporal.ProfileRationalQ, 1, "clock-v1")
	for _, target := range []Target{
		{Kind: EntityTarget, Entity: 10}, // supplied node identity
		{Kind: EntityTarget, Entity: 20}, // supplied relationship identity
		{Kind: ComponentTarget, Component: graphstate.ComponentKey{Owner: 10, Kind: graphstate.Presence}},
		{Kind: ComponentTarget, Component: graphstate.ComponentKey{Owner: 10, Life: 1, Kind: graphstate.Label, Name: "kind"}},
		{Kind: ComponentTarget, Component: graphstate.ComponentKey{Owner: 20, Life: 1, Kind: graphstate.ScalarProperty, Name: "value"}},
		{Kind: ComponentTarget, Component: graphstate.ComponentKey{Owner: 20, Life: 1, Kind: graphstate.SetMember, Name: "tags", Member: 7}},
	} {
		t.Run(targetName(target), func(t *testing.T) {
			initial := testSpec(t, 1, Placement{Kind: NativePlacement, Native: testPoint(t, axis, 12)})
			initial.Target = target
			knowledge, err := temporal.HardPointKnowledge(testPoint(t, axis, 12), temporal.Limits{})
			if err != nil {
				t.Fatal(err)
			}
			initial.Knowledge = Knowledge{Kind: PointKnowledge, Point: knowledge}
			created, err := Apply(Record{}, initial, Limits{})
			if err != nil || created.Replay() {
				t.Fatal(err)
			}
			if _, exists := created.Before(); exists {
				t.Fatal("initial record has predecessor")
			}
			old := created.After()
			oldWire := recordWire(t, old)
			corrected := initial
			corrected.Revision, corrected.Previous = testRevision(t, 2, 22), 1
			corrected.Interpretation, corrected.Role = Observation, ObservationTime
			corrected.Placement.Native = testPoint(t, axis, 13)
			corrected.Knowledge.Point, err = temporal.NominalPointKnowledge(testPosition(t, axis, 13), temporal.Limits{})
			if err != nil {
				t.Fatal(err)
			}
			changed, err := Apply(old, corrected, Limits{})
			if err != nil {
				t.Fatal(err)
			}
			withdraw := corrected
			withdraw.Revision, withdraw.Previous, withdraw.Retracted = testRevision(t, 3, 33), 2, true
			removed, err := Apply(changed.After(), withdraw, Limits{})
			if err != nil {
				t.Fatal(err)
			}
			// Every earlier read happens after both mutations, and asserts full bytes.
			before, exists := changed.Before()
			if !exists || !bytes.Equal(recordWire(t, before), oldWire) || !bytes.Equal(recordWire(t, old), oldWire) {
				t.Fatal("retained earlier revision changed")
			}
			if changed.After().Spec().Knowledge.Point.Kind() != temporal.PointKnowledgeNominalOnly || old.Spec().Knowledge.Point.Kind() != temporal.PointKnowledgeHardSupport || old.Spec().Revision.Provenance() != 11 || !removed.After().Spec().Retracted {
				t.Fatal("correction/retraction overwrote evidence or provenance")
			}
			reinstated := corrected
			reinstated.Revision, reinstated.Previous = testRevision(t, 4, 44), 3
			restored, err := Apply(removed.After(), reinstated, Limits{})
			if err != nil || restored.After().Spec().Retracted {
				t.Fatal("explicit reinstatement", err)
			}
		})
	}
}

func targetName(k Target) string {
	if k.Kind == EntityTarget {
		if k.Entity == 10 {
			return "node"
		}
		return "relationship"
	}
	return "component-" + string(rune('0'+k.Component.Kind))
}

func TestPlacementAndKnowledgeDistinctions(t *testing.T) {
	axis := testAxis(t, temporal.ProfileRationalQ, 1, "clock")
	point := testPoint(t, axis, 12)
	confidence, err := temporal.ConfidencePointKnowledge(point, temporal.RationalInt64(1), temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	unspecified, err := temporal.UnspecifiedPointKnowledge(axis, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	opaque, err := temporal.OpaquePointKnowledge(axis, testDescriptor(t, 1, []byte("shared-x")), temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range []temporal.PointKnowledge{confidence, unspecified, opaque} {
		s := testSpec(t, 1, Placement{Kind: NativePlacement, Native: point})
		s.Knowledge = Knowledge{Kind: PointKnowledge, Point: k}
		r := mustRecord(t, s)
		if r.Spec().Knowledge.Point.Kind() != k.Kind() {
			t.Fatal("knowledge promoted to hard support")
		}
		if _, err := r.Spec().Knowledge.Point.PossibleIn(point, temporal.Limits{}); !errors.Is(err, temporal.ErrUnsupportedPredicate) {
			t.Fatal("association enabled unsupported predicate", err)
		}
	}
	axisless := testSpec(t, 1, Placement{Kind: NoAssociation})
	axisless.Interpretation = Constraint // supplied causal relation target, no clock invented
	r := mustRecord(t, axisless)
	if r.Spec().Role != "" || r.native.Kind() != temporal.ScopeInvalid || r.knowledge != nil || r.symbolic != nil {
		t.Fatal("axisless association acquired optional metadata")
	}
	for _, kind := range []temporal.ScopeKind{temporal.ScopeUnplaced, temporal.ScopeEmpty, temporal.ScopeAll} {
		var s temporal.Scope
		switch kind {
		case temporal.ScopeUnplaced:
			s, err = temporal.Unplaced(axis)
		case temporal.ScopeEmpty:
			s, err = temporal.Empty(axis)
		case temporal.ScopeAll:
			s, err = temporal.All(axis)
		}
		if err != nil {
			t.Fatal(err)
		}
		r := mustRecord(t, testSpec(t, 1, Placement{Kind: NativePlacement, Native: s}))
		if r.Spec().Placement.Kind != NativePlacement || r.Spec().Placement.Native.Kind() != kind || r.knowledge != nil || r.symbolic != nil {
			t.Fatal("native special scope changed or ordinary record allocated optional data")
		}
	}
	symbolic := testSpec(t, 1, Placement{Kind: SymbolicPlacement, Symbolic: testDescriptor(t, 1, []byte("recurrence"))})
	symbolic.Interpretation = Derived
	r = mustRecord(t, symbolic)
	if r.native.Kind() != temporal.ScopeInvalid || r.symbolic == nil || r.knowledge != nil || r.Spec().Placement.Symbolic.SupportLevel() != temporal.DescriptorPreservationOnly {
		t.Fatal("symbolic descriptor acquired native axis/evaluation")
	}
}

func TestRejectsInactiveFieldsMalformedTargetsAndKnowledgeAxes(t *testing.T) {
	axis := testAxis(t, temporal.ProfileRationalQ, 1, "clock-v1")
	point := testPoint(t, axis, 12)
	knowledge, err := temporal.HardPointKnowledge(point, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	descriptor := testDescriptor(t, 1, nil)
	base := testSpec(t, 1, Placement{Kind: NativePlacement, Native: point})
	tests := []struct {
		name string
		edit func(*Spec)
		want error
	}{
		{"graph-zero", func(s *Spec) { s.Ref.Graph = graphstate.GraphID{} }, ErrInvalid},
		{"identity-zero", func(s *Spec) { s.Ref.ID = 0 }, ErrInvalid},
		{"revision-zero", func(s *Spec) { s.Revision = state.Revision{} }, ErrInvalid},
		{"interpretation-zero", func(s *Spec) { s.Interpretation = 0 }, ErrInvalid},
		{"unknown-interpretation", func(s *Spec) { s.Interpretation = 7 }, ErrInvalid},
		{"self-predecessor", func(s *Spec) { s.Previous = 1 }, ErrPredecessor},
		{"initial-retraction", func(s *Spec) { s.Retracted = true }, ErrPredecessor},
		{"unknown-target", func(s *Spec) { s.Target.Kind = 9 }, ErrInvalid},
		{"entity-zero", func(s *Spec) { s.Target.Entity = 0 }, ErrInvalid},
		{"entity-inactive-component", func(s *Spec) { s.Target.Component.Owner = 1 }, ErrInvalid},
		{"component-inactive-entity", func(s *Spec) {
			s.Target.Kind = ComponentTarget
			s.Target.Component = graphstate.ComponentKey{Owner: 1, Kind: graphstate.Presence}
		}, ErrInvalid},
		{"unknown-component", func(s *Spec) {
			s.Target = Target{Kind: ComponentTarget, Component: graphstate.ComponentKey{Owner: 1, Kind: 9}}
		}, ErrInvalid},
		{"component-owner-zero", func(s *Spec) {
			s.Target = Target{Kind: ComponentTarget, Component: graphstate.ComponentKey{Kind: graphstate.Presence}}
		}, ErrInvalid},
		{"presence-name", func(s *Spec) {
			s.Target = Target{Kind: ComponentTarget, Component: graphstate.ComponentKey{Owner: 1, Kind: graphstate.Presence, Name: "x"}}
		}, ErrInvalid},
		{"property-life-zero", func(s *Spec) {
			s.Target = Target{Kind: ComponentTarget, Component: graphstate.ComponentKey{Owner: 1, Kind: graphstate.ScalarProperty, Name: "x"}}
		}, ErrInvalid},
		{"member-zero", func(s *Spec) {
			s.Target = Target{Kind: ComponentTarget, Component: graphstate.ComponentKey{Owner: 1, Life: 1, Kind: graphstate.SetMember, Name: "x"}}
		}, ErrInvalid},
		{"blank-role", func(s *Spec) { s.Role = " \t" }, ErrInvalid},
		{"invalid-utf8-role", func(s *Spec) { s.Role = TemporalRole(string([]byte{255})) }, ErrInvalid},
		{"unknown-placement", func(s *Spec) { s.Placement.Kind = 0 }, ErrInvalid},
		{"native-missing-scope", func(s *Spec) { s.Placement.Native = temporal.Scope{} }, ErrInvalid},
		{"native-inactive-descriptor", func(s *Spec) { s.Placement.Symbolic = descriptor }, ErrInvalid},
		{"none-active-role", func(s *Spec) { s.Placement = Placement{Kind: NoAssociation} }, ErrInvalid},
		{"none-inactive-native", func(s *Spec) { s.Role = ""; s.Placement.Kind = NoAssociation }, ErrInvalid},
		{"none-inactive-symbolic", func(s *Spec) { s.Role = ""; s.Placement = Placement{Kind: NoAssociation, Symbolic: descriptor} }, ErrInvalid},
		{"symbolic-inactive-native", func(s *Spec) { s.Placement = Placement{Kind: SymbolicPlacement, Native: point, Symbolic: descriptor} }, ErrInvalid},
		{"symbolic-missing-descriptor", func(s *Spec) { s.Placement = Placement{Kind: SymbolicPlacement} }, ErrInvalid},
		{"unknown-knowledge", func(s *Spec) { s.Knowledge.Kind = 0 }, ErrInvalid},
		{"none-inactive-point-knowledge", func(s *Spec) { s.Knowledge.Point = knowledge }, ErrInvalid},
		{"knowledge-no-association", func(s *Spec) {
			s.Role = ""
			s.Placement = Placement{Kind: NoAssociation}
			s.Knowledge = Knowledge{Kind: PointKnowledge, Point: knowledge}
		}, ErrKnowledgeAssociation},
		{"knowledge-symbolic", func(s *Spec) {
			s.Placement = Placement{Kind: SymbolicPlacement, Symbolic: descriptor}
			s.Knowledge = Knowledge{Kind: PointKnowledge, Point: knowledge}
		}, ErrKnowledgeAssociation},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := base
			tt.edit(&s)
			r, err := New(s, Limits{})
			if !errors.Is(err, tt.want) || r.Spec().Ref != (Ref{}) {
				t.Fatal("constructor dropped invalid input", err)
			}
			assertRefusal(t, mustRecord(t, base), s, Limits{}, tt.want)
		})
	}
	for _, other := range []temporal.Axis{
		testAxis(t, temporal.ProfileRationalQ, 2, "clock-v1"),
		testAxis(t, temporal.ProfileRationalQ, 1, "clock-v2"),
		testAxis(t, temporal.ProfileIntegerZ, 1, "clock-v1"),
	} {
		k, err := temporal.HardPointKnowledge(testPoint(t, other, 12), temporal.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		s := base
		s.Knowledge = Knowledge{Kind: PointKnowledge, Point: k}
		_, err = New(s, Limits{})
		if !errors.Is(err, ErrKnowledgeAssociation) || !errors.Is(err, temporal.ErrAxisMismatch) {
			t.Fatal("foreign/conflicting knowledge axis accepted", err)
		}
	}
}

func TestCurrentReplayFullContentAndImmediateChainOnly(t *testing.T) {
	base := testSpec(t, 1, Placement{Kind: NoAssociation})
	base.Interpretation = Constraint
	r := mustRecord(t, base)
	replayed, err := Apply(r, base, Limits{})
	if err != nil || !replayed.Replay() || !bytes.Equal(recordWire(t, r), recordWire(t, replayed.After())) {
		t.Fatal("exact replay failed", err)
	}
	for _, edit := range []func(*Spec){
		func(s *Spec) { s.Revision = testRevision(t, 1, 99) },
		func(s *Spec) { s.Previous = 99 },
		func(s *Spec) { s.Interpretation = Derived },
		func(s *Spec) {
			s.Placement = Placement{Kind: SymbolicPlacement, Symbolic: testDescriptor(t, 3, []byte("v2"))}
			s.Role = "symbolic"
		},
	} {
		s := base
		edit(&s)
		assertRefusal(t, r, s, Limits{}, ErrRevisionReuse)
	}
	changed := base
	changed.Revision, changed.Previous = testRevision(t, 2, 22), 1
	for _, edit := range []struct {
		fn   func(*Spec)
		want error
	}{
		{func(s *Spec) { s.Ref.Graph[0] = 2 }, ErrNamespace},
		{func(s *Spec) { s.Ref.ID = 2 }, ErrRebinding},
		{func(s *Spec) { s.Target.Entity = 2 }, ErrRebinding},
		{func(s *Spec) { s.Previous = 99 }, ErrPredecessor},
	} {
		s := changed
		edit.fn(&s)
		assertRefusal(t, r, s, Limits{}, edit.want)
	}
	if x, err := Apply(Record{}, changed, Limits{}); !errors.Is(err, ErrPredecessor) || x.After().Spec().Ref != (Ref{}) {
		t.Fatal("absent predecessor accepted", err)
	}
	second, err := Apply(r, changed, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	third := base
	third.Previous = 2 // older revision-ID reuse requires future history-store validation
	x, err := Apply(second.After(), third, Limits{})
	if err != nil || x.After().Spec().Revision.ID() != 1 {
		t.Fatal("primitive claimed unseen-history validation", err)
	}
}

func TestLimitsAndConcurrentImmutableReads(t *testing.T) {
	if err := DefaultLimits().Validate(); err != nil {
		t.Fatal(err)
	}
	for _, policy := range []struct {
		limits Limits
		want   error
	}{
		{Limits{MaxRoleBytes: -1}, ErrInvalid},
		{Limits{MaxRoleBytes: 65537}, ErrInvalid},
		{Limits{MaxRecordBytes: -1}, ErrInvalid},
		{Limits{MaxRecordBytes: 1 << 21}, ErrInvalid},
		{Limits{Temporal: temporal.Limits{MaxMagnitudeBits: -1}}, temporal.ErrInvalidLimits},
	} {
		l := policy.limits
		if err := l.Validate(); !errors.Is(err, policy.want) {
			t.Fatal("invalid limits accepted or wrong sentinel", err)
		}
		if r, err := New(Spec{}, l); !errors.Is(err, policy.want) || r.Spec().Ref != (Ref{}) {
			t.Fatal("constructor accepted invalid policy or wrong sentinel", err)
		}
	}
	r := mustRecord(t, testSpec(t, 1, Placement{Kind: SymbolicPlacement, Symbolic: testDescriptor(t, 1, []byte("immutable"))}))
	want := recordWire(t, r)
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			for range 20 {
				s := r.Spec()
				d := s.Placement.Symbolic.Spec()
				d.Payload[0] ^= 255
				d.References[0].Role = "changed"
				b, err := AppendRecord(nil, r, Limits{})
				if err != nil || !bytes.Equal(b, want) {
					t.Error("shared immutable record changed", err)
				}
			}
		})
	}
	wg.Wait()
}
