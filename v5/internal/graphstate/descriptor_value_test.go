package graphstate

import (
	"bytes"
	"errors"
	"reflect"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func v1Descriptor(t testing.TB, id byte, payload []byte) temporal.OpaqueDescriptor {
	t.Helper()
	d, err := temporal.PreserveOpaqueDescriptor(temporal.OpaqueDescriptorSpec{ID: temporal.DescriptorID{id}, Type: "constraint-v1", SchemaVersion: 3, Correlation: temporal.CorrelationID{8}, References: []temporal.DescriptorReference{{Role: "source", ID: temporal.DescriptorID{9}, Type: "observation", SchemaVersion: 2, Integrity: [32]byte{7}}}, Payload: payload}, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	return d
}
func TestV1DescriptorScalarPreservesCompleteEnvelopeIdentity(t *testing.T) {
	source := []byte("x-shared-source")
	d := v1Descriptor(t, 1, source)
	v, err := DescriptorValue(d)
	if err != nil {
		t.Fatal(err)
	}
	source[0] = 'z'
	got, ok := v.Descriptor()
	if !ok || v.Kind() != ScalarDescriptor || got.SupportLevel() != temporal.DescriptorPreservationOnly {
		t.Fatal(got, ok)
	}
	spec := got.Spec()
	if !bytes.Equal(spec.Payload, []byte("x-shared-source")) || spec.Correlation != (temporal.CorrelationID{8}) || len(spec.References) != 1 {
		t.Fatal(spec)
	}
	key, err := v.EqualityKey(Limits{})
	if err != nil {
		t.Fatal(err)
	}
	spec.Payload[0] = 'z'
	spec.References[0].Role = "changed"
	same, err := v.Equal(v, Limits{})
	if err != nil || !same {
		t.Fatal(same, err)
	}
	again, err := v.EqualityKey(Limits{})
	if err != nil || again != key {
		t.Fatal("mutable envelope alias", err)
	}
	different, err := DescriptorValue(v1Descriptor(t, 2, []byte("x-shared-source")))
	if err != nil {
		t.Fatal(err)
	}
	same, err = v.Equal(different, Limits{})
	if err != nil || same {
		t.Fatal("payload equality replaced envelope identity", same, err)
	}
	if v.Render() != "" {
		t.Fatal("descriptor was implicitly interpreted/rendered")
	}
	if _, ok := String("x-shared-source").Descriptor(); ok {
		t.Fatal("string became descriptor")
	}
	if _, err := DescriptorValue(temporal.OpaqueDescriptor{}); !errors.Is(err, ErrInvalidInput) {
		t.Fatal(err)
	}
	for _, delta := range []int{-1, 0, 1} {
		_, err := v.EqualityKey(Limits{MaxReadBytes: len(key) + delta})
		if delta < 0 {
			if !errors.Is(err, ErrResourceLimit) {
				t.Fatal(err)
			}
		} else if err != nil {
			t.Fatal(err)
		}
	}
}
func TestV1DescriptorPropertyCorrectionHistoryAndUnsupportedConstraints(t *testing.T) {
	v := newFixtureView(t)
	scope := testSpan(t, v.axis, 0, 10)
	middle := testSpan(t, v.axis, 3, 7)
	v.defs[ownerSchemaKey{Node, "evidence"}] = PropertyDefinition{Name: "evidence", Owner: Node, Type: ScalarDescriptor, Cardinality: ScalarCardinality}
	first, _ := DescriptorValue(v1Descriptor(t, 1, []byte("source-v1")))
	second, _ := DescriptorValue(v1Descriptor(t, 2, []byte("source-v2")))
	commitOps(t, v, 1, Operation{Kind: CreateNode, Owner: 1, Life: 1, Scope: scope}, Operation{Kind: Set, Owner: 1, Life: 1, Scope: scope, Name: "evidence", Value: first, ValueID: 1})
	old := v.clone()
	commitOps(t, v, 2, Operation{Kind: Set, Owner: 1, Life: 1, Scope: middle, Name: "evidence", Value: second, ValueID: 2})
	for _, tc := range []struct {
		view *fixtureView
		at   int64
		want string
	}{{old, 5, "source-v1"}, {v, 5, "source-v2"}, {v, 8, "source-v1"}} {
		p, err := Project(t.Context(), tc.view, 1, testPosition(t, v.axis, tc.at), Effective, Limits{})
		if err != nil || len(p.Properties) != 1 {
			t.Fatal(p, err)
		}
		descriptor, ok := p.Properties[0].Scalar.Descriptor()
		if !ok || string(descriptor.Spec().Payload) != tc.want {
			t.Fatal(descriptor, ok, tc.want)
		}
	}
	rev, _ := state.NewRevision(3, 0)
	for _, definition := range []PropertyDefinition{{Name: "evidence", Owner: Node, Type: ScalarDescriptor, Cardinality: ScalarCardinality, Unique: UniqueScalar}, {Name: "evidence", Owner: Node, Type: ScalarDescriptor, Cardinality: SetCardinality}} {
		bad := v.clone()
		bad.defs[ownerSchemaKey{Node, "evidence"}] = definition
		delta, err := Plan(t.Context(), bad, []Operation{{Kind: Set, Owner: 1, Life: 1, Scope: scope, Name: "evidence", Value: first, ValueID: 3}}, rev, Limits{})
		if !errors.Is(err, ErrUnsupported) || !reflect.DeepEqual(delta, Delta{}) {
			t.Fatal(delta, err)
		}
	}
}

func TestV1DescriptorConstructorHonorsRaisedAndTighterOperationLimits(t *testing.T) {
	payload := bytes.Repeat([]byte{'x'}, 96<<10)
	raised := temporal.Limits{MaxInputBytes: 128 << 10, MaxValueBytes: 128 << 10, MaxDescriptorBytes: 128 << 10}
	descriptor, err := temporal.PreserveOpaqueDescriptor(temporal.OpaqueDescriptorSpec{ID: temporal.DescriptorID{1}, Type: "large-source", SchemaVersion: 1, Payload: payload}, raised)
	if err != nil {
		t.Fatal(err)
	}
	value, err := DescriptorValue(descriptor)
	if err != nil {
		t.Fatal("constructor reapplied default caps", err)
	}
	l := Limits{}
	l.Component.Temporal = raised
	key, err := value.EqualityKey(l)
	if err != nil || len(key) <= 64<<10 {
		t.Fatal(len(key), err)
	}
	if _, err := value.EqualityKey(Limits{}); !errors.Is(err, temporal.ErrResourceLimit) || !errors.Is(err, ErrResourceLimit) {
		t.Fatal("tighter operation cap ignored", err)
	}
	got, ok := value.Descriptor()
	if !ok || !bytes.Equal(got.Spec().Payload, payload) {
		t.Fatal("refusal lost preserved large descriptor")
	}
}
