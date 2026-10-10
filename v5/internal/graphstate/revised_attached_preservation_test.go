package graphstate

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"reflect"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func revisedDeclarations(op revisedOperation) (Interpretation, TemporalRole, error) {
	var i Interpretation
	switch op.Interpretation {
	case "":
	case "occurrence":
		i = InterpretationOccurrence
	case "state":
		i = InterpretationState
	case "observation":
		i = InterpretationObservation
	case "constraint":
		i = InterpretationConstraint
	case "derived_assertion":
		i = InterpretationDerivedAssertion
	case "asserted_relation":
		i = InterpretationAssertedRelation
	default:
		return 0, 0, ErrUnsupported
	}
	var r TemporalRole
	switch op.TemporalRole {
	case "":
	case "validity":
		r = TemporalRoleValidity
	case "occurrence_time":
		r = TemporalRoleOccurrenceTime
	case "source_occurrence":
		r = TemporalRoleSourceOccurrence
	case "observation_time":
		r = TemporalRoleObservationTime
	case "source_receipt":
		r = TemporalRoleSourceReceipt
	case "causal_order":
		r = TemporalRoleCausalOrder
	default:
		return 0, 0, ErrUnsupported
	}
	return i, r, nil
}
func revisedInterpretationName(i Interpretation) string {
	switch i {
	case InterpretationOccurrence:
		return "occurrence"
	case InterpretationState:
		return "state"
	case InterpretationObservation:
		return "observation"
	case InterpretationConstraint:
		return "constraint"
	case InterpretationDerivedAssertion:
		return "derived_assertion"
	case InterpretationAssertedRelation:
		return "asserted_relation"
	}
	return ""
}
func revisedRoleName(r TemporalRole) string {
	switch r {
	case TemporalRoleValidity:
		return "validity"
	case TemporalRoleOccurrenceTime:
		return "occurrence_time"
	case TemporalRoleSourceOccurrence:
		return "source_occurrence"
	case TemporalRoleObservationTime:
		return "observation_time"
	case TemporalRoleSourceReceipt:
		return "source_receipt"
	case TemporalRoleCausalOrder:
		return "causal_order"
	}
	return ""
}
func revisedOperationGraph(local GraphID, ops []revisedOperation) GraphID {
	for _, op := range ops {
		if op.Graph != "" {
			h := sha256.Sum256([]byte("revised-graph:" + op.Graph))
			var id GraphID
			copy(id[:], h[:16])
			return id
		}
	}
	return local
}
func revisedAttachedScalar(record revisedRecord, raw json.RawMessage) (Scalar, error) {
	if len(bytes.TrimSpace(raw)) == 0 || bytes.TrimSpace(raw)[0] != '{' {
		return revisedScalar(raw)
	}
	var allowed []string
	switch record.ID {
	case "E06-preservation":
		allowed = []string{"constraint", "correlation", "interpretation", "schema_version", "sources", "source_correction"}
	case "E10-preservation":
		allowed = []string{"constraints", "interpretation", "schema_version", "source_correction"}
	case "E13-preservation":
		allowed = []string{"ambiguity_policy", "calendar_period", "elapsed_duration", "interpretation", "timezone", "tzdb_version", "source_correction"}
	case "E14-preservation":
		allowed = []string{"axis", "interpretation", "kind", "scope", "source_correction"}
	case "E20-preservation":
		allowed = []string{"input_snapshot", "interpretation", "quantifier", "rule_version", "window", "source_correction"}
	default:
		return Scalar{}, fmt.Errorf("%w: unknown object attachment", errRevisedPending)
	}
	var fields map[string]json.RawMessage
	if err := revisedStrictJSON(raw, &fields); err != nil {
		return Scalar{}, err
	}
	if err := revisedRequireKeys(fields, allowed); err != nil {
		return Scalar{}, err
	}
	var meaning string
	if err := revisedStrictJSON(fields["interpretation"], &meaning); err != nil || meaning == "" {
		return Scalar{}, ErrInvalidInput
	}
	version := uint32(1)
	if field, ok := fields["schema_version"]; ok {
		if err := revisedStrictJSON(field, &version); err != nil || version == 0 {
			return Scalar{}, ErrInvalidInput
		}
	}
	hash := sha256.Sum256([]byte("revised-property-descriptor:" + record.ID))
	var id temporal.DescriptorID
	copy(id[:], hash[:16])
	spec := temporal.OpaqueDescriptorSpec{ID: id, Type: "revised-property:" + meaning, SchemaVersion: version, Payload: raw}
	if field, ok := fields["correlation"]; ok {
		var correlation string
		if err := revisedStrictJSON(field, &correlation); err != nil {
			return Scalar{}, err
		}
		h := sha256.Sum256([]byte("revised-correlation:" + correlation))
		copy(spec.Correlation[:], h[:16])
	}
	if field, ok := fields["sources"]; ok {
		var sources []struct {
			ID      string `json:"id"`
			Latent  string `json:"latent"`
			Nominal string `json:"nominal"`
		}
		if err := revisedStrictJSON(field, &sources); err != nil {
			return Scalar{}, err
		}
		for _, source := range sources {
			h := sha256.Sum256([]byte("revised-source:" + source.ID))
			var sourceID temporal.DescriptorID
			copy(sourceID[:], h[:16])
			spec.References = append(spec.References, temporal.DescriptorReference{Role: "source", ID: sourceID, Type: "raw-observation", SchemaVersion: 1})
		}
	}
	descriptor, err := temporal.PreserveOpaqueDescriptor(spec, temporal.Limits{})
	if err != nil {
		return Scalar{}, err
	}
	return DescriptorValue(descriptor)
}

func TestV1RevisedCompleteAttachedPreservationGoldens(t *testing.T) {
	count := 0
	for _, record := range revisedLoad(t) {
		if record.Kind == "graph" && revisedRules[record.ID].lane == revisedPreservation {
			count++
			t.Run(record.ID, func(t *testing.T) { revisedRunGraph(t, record) })
		}
	}
	if count != 5 {
		t.Fatal("missing complete attached preservation row", count)
	}
}

// This is adapter/type exclusion evidence. Plan(Unplaced) is a distinct typed
// refusal control, never a surrogate successful test of Plan(symbolic).
func TestV1StrictSymbolicAdapterExclusionAndSeparateUnplacedControl(t *testing.T) {
	for _, record := range revisedLoad(t) {
		if record.ID != "strict-symbolic-placement-decline" {
			continue
		}
		op := revisedMustField[revisedOperation](t, record, "operation")
		v := newFixtureView(t)
		scope, err := revisedMutationScope(op, v.axis, revisedAxis{Identity: "irrelevant"})
		if !errors.Is(err, ErrUnsupported) || scope.Kind() != temporal.ScopeInvalid {
			t.Fatal(scope, err)
		}
		rev, _ := state.NewRevision(1, 0)
		unplaced, err := temporal.Unplaced(v.axis)
		if err != nil {
			t.Fatal(err)
		}
		d, err := Plan(t.Context(), v, []Operation{{Kind: Set, Owner: 1, Life: 1, Scope: unplaced, Name: "p"}}, rev, Limits{})
		if !errors.Is(err, ErrUnsupported) || len(d.Patches)+len(d.Entities)+len(d.Values)+len(d.Dependencies) != 0 {
			t.Fatal("caller evidence promoted to enforced placement", d, err)
		}
	}
}
func TestV1TimeSpanPropertyNeverChangesOwnerPlacement(t *testing.T) {
	v := newFixtureView(t)
	owner := testSpan(t, v.axis, 0, 10)
	a, err := temporal.NewAxis(temporal.AxisDescriptor{ID: temporal.AxisID{8}, Profile: temporal.ProfileRationalQ, Version: 1, Reference: "property-axis-Q", CanonicalUnit: "abstract-position"}, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	span := func(lo, hi int64) temporal.Scope {
		l, _ := temporal.RationalPosition(a, temporal.RationalInt64(lo))
		h, _ := temporal.RationalPosition(a, temporal.RationalInt64(hi))
		lb, _ := temporal.FiniteBound(l, true)
		hb, _ := temporal.FiniteBound(h, false)
		s, err := temporal.Span(a, lb, hb, temporal.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	v.defs[ownerSchemaKey{Node, "time"}] = PropertyDefinition{"time", Node, ScalarScope, ScalarCardinality, UniqueNone}
	commitOps(t, v, 1, Operation{Kind: CreateNode, Owner: 1, Life: 1, Scope: owner, Record: EntityRecord{Interpretation: InterpretationObservation, TemporalRole: TemporalRoleSourceOccurrence}}, Operation{Kind: Set, Owner: 1, Life: 1, Scope: owner, Name: "time", Value: ScopeValue(span(50, 60)), ValueID: 1})
	old := v.clone()
	commitOps(t, v, 2, Operation{Kind: Set, Owner: 1, Life: 1, Scope: testSpan(t, v.axis, 3, 7), Name: "time", Value: ScopeValue(span(70, 80)), ValueID: 2})
	for _, tc := range []struct {
		v  *fixtureView
		at int64
		lo int64
	}{{old, 5, 50}, {v, 5, 70}, {v, 8, 50}} {
		p, err := Project(t.Context(), tc.v, 1, testPosition(t, v.axis, tc.at), Effective, Limits{})
		if err != nil || !p.Active || len(p.Properties) != 1 {
			t.Fatal(p, err)
		}
		value, ok := p.Properties[0].Scalar.Scope()
		if !ok || value.Axis().Descriptor() != a.Descriptor() {
			t.Fatal(value)
		}
		same, err := value.SameSupport(span(tc.lo, tc.lo+10), temporal.Limits{})
		if err != nil || !same {
			t.Fatal(value, err)
		}
	}
	p, err := Project(t.Context(), v, 1, testPosition(t, v.axis, 50), Effective, Limits{})
	if err != nil || p.Active {
		t.Fatal("property support became owner validity", p, err)
	}
}

func TestV1E04ExactSourceEventsAndCallerRoundingRemainDistinct(t *testing.T) {
	var record revisedRecord
	for _, r := range revisedLoad(t) {
		if r.ID == "E04-exact-nearby-values" {
			record = r
		}
	}
	values := revisedMustField[[]string](t, record, "input")
	expected := revisedMustField[struct {
		Difference string   `json:"difference_seconds"`
		Equal      bool     `json:"equal"`
		IDs        []string `json:"event_ids"`
		Rounded    []string `json:"explicit_nearest_microsecond"`
	}](t, record, "expected")
	axis, err := revisedAxisValue(revisedAxis{Identity: "E04-supplied-source", Profile: "Q", Reference: "declared-source-seconds-v1", Unit: "second", Version: 1})
	if err != nil {
		t.Fatal(err)
	}
	v := newFixtureView(t)
	v.axis = axis
	v.defs[ownerSchemaKey{Node, "raw"}] = PropertyDefinition{"raw", Node, ScalarString, ScalarCardinality, UniqueNone}
	v.defs[ownerSchemaKey{Node, "rounding"}] = PropertyDefinition{"rounding", Node, ScalarDescriptor, ScalarCardinality, UniqueNone}
	var ops []Operation
	var points []temporal.Position
	for i, text := range values {
		raw, _ := json.Marshal(text)
		position, err := revisedPosition(axis, raw, temporal.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		points = append(points, position)
		scope, err := temporal.Point(position)
		if err != nil {
			t.Fatal(err)
		}
		payload, err := json.Marshal(map[string]string{"source_id": expected.IDs[i], "source_raw_seconds": text, "supplied_policy": "explicit-nearest-microsecond", "supplied_result": expected.Rounded[i]})
		if err != nil {
			t.Fatal(err)
		}
		descriptor, err := DescriptorValue(v1Descriptor(t, byte(i+1), payload))
		if err != nil {
			t.Fatal(err)
		}
		id := EntityID(i + 1)
		ops = append(ops, Operation{Kind: CreateNode, Owner: id, Life: 1, Scope: scope, Record: EntityRecord{Interpretation: InterpretationOccurrence, TemporalRole: TemporalRoleSourceOccurrence}}, Operation{Kind: Set, Owner: id, Life: 1, Scope: scope, Name: "raw", Value: String(text), ValueID: ValueID(2*i + 1)}, Operation{Kind: Set, Owner: id, Life: 1, Scope: scope, Name: "rounding", Value: descriptor, ValueID: ValueID(2*i + 2)})
	}
	commitOps(t, v, 1, ops...)
	old := v.clone()
	closed, _ := temporal.Point(points[0])
	commitOps(t, v, 2, Operation{Kind: Close, Owner: 1, Life: 1, Scope: closed})
	for i, position := range points {
		for j := range points {
			p, err := Project(t.Context(), old, EntityID(j+1), position, Effective, Limits{})
			if err != nil || p.Active != (i == j) {
				t.Fatal("exact event set collapsed", i, j, p, err)
			}
			if p.Active {
				if p.Record.Interpretation != InterpretationOccurrence || p.Record.TemporalRole != TemporalRoleSourceOccurrence || len(p.Properties) != 2 {
					t.Fatal(p)
				}
				for _, property := range p.Properties {
					if property.Name == "raw" {
						raw, ok := property.Scalar.StringValue()
						if !ok || raw != values[i] {
							t.Fatal(property)
						}
					}
					if property.Name == "rounding" {
						d, ok := property.Scalar.Descriptor()
						if !ok {
							t.Fatal(property)
						}
						var supplied map[string]string
						if err := revisedStrictJSON(d.Spec().Payload, &supplied); err != nil || supplied["source_id"] != expected.IDs[i] || supplied["supplied_result"] != expected.Rounded[i] {
							t.Fatal(supplied, err)
						}
					}
				}
			}
		}
	}
	// Independent test arithmetic checks the exact supplied difference. Rho does
	// not implement rounding or derive a metric from storage enumeration order.
	a, ok := new(big.Rat).SetString(values[0])
	if !ok {
		t.Fatal(values)
	}
	b, ok := new(big.Rat).SetString(values[1])
	if !ok {
		t.Fatal(values)
	}
	if new(big.Rat).Sub(b, a).RatString() != expected.Difference || expected.Equal || expected.Rounded[0] != expected.Rounded[1] {
		t.Fatal("source/derived distinction", expected)
	}
	p, err := Project(t.Context(), v, 1, points[0], Effective, Limits{})
	if err != nil || p.Active {
		t.Fatal("close lost", p, err)
	}
	p, err = Project(t.Context(), old, 1, points[0], Effective, Limits{})
	if err != nil || !p.Active {
		t.Fatal("retained event became current", p, err)
	}
}

func TestV1E09SuppliedSourceResultAndReferencesSurviveCorrection(t *testing.T) {
	var record revisedRecord
	for _, r := range revisedLoad(t) {
		if r.ID == "E09-source-history-derived-fraction" {
			record = r
		}
	}
	sources := revisedMustField[[]revisedRegion](t, record, "source_regions")
	result := revisedMustField[revisedRegion](t, record, "supplied_result")
	shifted := revisedMustField[revisedRegion](t, record, "supplied_shifted_result")
	v := newFixtureView(t)
	v.axis = revisedMustScope(t, sources[0], temporal.Limits{}).Axis()
	all, err := temporal.All(v.axis)
	if err != nil {
		t.Fatal(err)
	}
	v.defs[ownerSchemaKey{Node, "support"}] = PropertyDefinition{"support", Node, ScalarScope, ScalarCardinality, UniqueNone}
	v.defs[ownerSchemaKey{Node, "inputs"}] = PropertyDefinition{"inputs", Node, ScalarDescriptor, ScalarCardinality, UniqueNone}
	var ops []Operation
	spec := temporal.OpaqueDescriptorSpec{ID: temporal.DescriptorID{3}, Type: "supplied-derived-inputs", SchemaVersion: 1, Payload: []byte(`{"meaning":"exact supplied shift1/3; no database join/shift","sources":[{"owner":1,"life":1,"component":"support","revision":1,"provenance":0},{"owner":2,"life":1,"component":"support","revision":1,"provenance":0}]}`)}
	for i, source := range sources {
		scope := revisedMustScope(t, source, temporal.Limits{})
		wire, err := temporal.AppendScope(nil, scope, temporal.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		spec.References = append(spec.References, temporal.DescriptorReference{Role: "source@revision:1/provenance:0", ID: temporal.DescriptorID{byte(i + 1)}, Type: "source-region", SchemaVersion: 1, Integrity: sha256.Sum256(wire)})
		id := EntityID(i + 1)
		ops = append(ops, Operation{Kind: CreateNode, Owner: id, Life: 1, Scope: all, Record: EntityRecord{Interpretation: InterpretationObservation, TemporalRole: TemporalRoleSourceOccurrence}}, Operation{Kind: Set, Owner: id, Life: 1, Scope: all, Name: "support", Value: ScopeValue(scope), ValueID: ValueID(i + 1)})
	}
	descriptor, err := temporal.PreserveOpaqueDescriptor(spec, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	inputs, err := DescriptorValue(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	ops = append(ops, Operation{Kind: CreateNode, Owner: 3, Life: 1, Scope: all, Record: EntityRecord{Interpretation: InterpretationDerivedAssertion, TemporalRole: TemporalRoleValidity}}, Operation{Kind: Set, Owner: 3, Life: 1, Scope: all, Name: "support", Value: ScopeValue(revisedMustScope(t, shifted, temporal.Limits{})), ValueID: 3}, Operation{Kind: Set, Owner: 3, Life: 1, Scope: all, Name: "inputs", Value: inputs, ValueID: 4})
	commitOps(t, v, 1, ops...)
	old := v.clone()
	correction := sources[0]
	correction.Pieces = []revisedPiece{{Lower: json.RawMessage(`"8"`), Upper: json.RawMessage(`"10"`), LowerClosed: true}}
	window := revisedMustScope(t, result, temporal.Limits{})
	commitOps(t, v, 2, Operation{Kind: Set, Owner: 1, Life: 1, Scope: window, Name: "support", Value: ScopeValue(revisedMustScope(t, correction, temporal.Limits{})), ValueID: 5})
	at, err := temporal.RationalPosition(v.axis, temporal.RationalInt64(3))
	if err != nil {
		t.Fatal(err)
	}
	// The descriptor's opaque source revision is tied to actual component cells,
	// not merely a string describing a later current-state lookup.
	var bound struct {
		Meaning string `json:"meaning"`
		Sources []struct {
			Owner      EntityID `json:"owner"`
			Life       LifeID   `json:"life"`
			Component  string   `json:"component"`
			Revision   uint64   `json:"revision"`
			Provenance uint64   `json:"provenance"`
		} `json:"sources"`
	}
	if err := revisedStrictJSON(spec.Payload, &bound); err != nil || len(bound.Sources) != 2 {
		t.Fatal(bound, err)
	}
	for _, source := range bound.Sources {
		key := ComponentKey{Owner: source.Owner, Life: source.Life, Kind: ScalarProperty, Name: source.Component}
		cell, err := old.components[key].At(at, state.Limits{})
		if err != nil || cell.Revision().ID() != source.Revision || cell.Revision().Provenance() != source.Provenance {
			t.Fatal("source revision binding lost", cell, source, err)
		}
		current, err := v.components[key].At(at, state.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		expected := source.Revision
		if source.Owner == 1 {
			expected = 2
		}
		if current.Revision().ID() != expected || current.Revision().Provenance() != source.Provenance {
			t.Fatal("current source revision not retained separately", current, source)
		}
	}
	for _, snapshot := range []*fixtureView{old, v} {
		p, err := Project(t.Context(), snapshot, 3, at, Effective, Limits{})
		if err != nil || !p.Active || len(p.Properties) != 2 {
			t.Fatal(p, err)
		}
		for _, property := range p.Properties {
			switch property.Name {
			case "support":
				scope, ok := property.Scalar.Scope()
				if !ok {
					t.Fatal(property)
				}
				revisedAssertScope(t, shifted, scope)
			case "inputs":
				d, ok := property.Scalar.Descriptor()
				if !ok {
					t.Fatal(property)
				}
				got := d.Spec()
				if !reflect.DeepEqual(got, spec) {
					t.Fatal("retained actual source references changed", got, spec)
				}
			}
		}
	}
	for _, tc := range []struct {
		snapshot *fixtureView
		want     revisedRegion
	}{{old, sources[0]}, {v, correction}} {
		p, err := Project(t.Context(), tc.snapshot, 1, at, Effective, Limits{})
		if err != nil || len(p.Properties) != 1 {
			t.Fatal(p, err)
		}
		scope, ok := p.Properties[0].Scalar.Scope()
		if !ok {
			t.Fatal(p)
		}
		revisedAssertScope(t, tc.want, scope)
	}
}
