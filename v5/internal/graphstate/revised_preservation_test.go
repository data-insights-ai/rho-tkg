package graphstate

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

// The seven records in this lane prove envelope/scalar/supplied-scope
// preservation only. They do not pass graph attachment, event identity,
// correlated solving, calendar evaluation, join/shift or summary acceptance.
func TestRevisedOpaqueEnvelopePreservationDoesNotClaimSemanticOrGraphAcceptance(t *testing.T) {
	count := 0
	for _, record := range revisedLoad(t) {
		if revisedRules[record.ID].lane != revisedPreservation {
			continue
		}
		count++
		t.Run(record.ID, func(t *testing.T) {
			descriptor, err := revisedOpaque(record)
			if err != nil {
				t.Fatal(err)
			}
			if descriptor.SupportLevel() != temporal.DescriptorPreservationOnly {
				t.Fatal("unsupported preservation claim")
			}
			original := descriptor.Spec()
			if !bytes.Equal(original.Payload, record.Raw) {
				t.Fatal("complete raw input was not retained")
			}
			wire, err := temporal.AppendOpaqueDescriptor(nil, descriptor, temporal.Limits{})
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := temporal.DecodeOpaqueDescriptor(wire, temporal.Limits{})
			if err != nil {
				t.Fatal(err)
			}
			roundTrip := decoded.Spec()
			if original.ID != roundTrip.ID || original.Type != roundTrip.Type || original.SchemaVersion != roundTrip.SchemaVersion || original.Correlation != roundTrip.Correlation || len(original.References) != len(roundTrip.References) || !bytes.Equal(original.Payload, roundTrip.Payload) {
				t.Fatal("complete envelope fields changed")
			}
			for i, reference := range original.References {
				if reference != roundTrip.References[i] {
					t.Fatal("reference changed")
				}
			}
			// Payload ownership is also a real codec guarantee, not a graph view.
			owned := decoded.Spec()
			owned.Payload[0] ^= 0xff
			if !bytes.Equal(decoded.Spec().Payload, record.Raw) {
				t.Fatal("decoded payload aliases caller")
			}
			rewire, err := temporal.AppendOpaqueDescriptor(nil, decoded, temporal.Limits{})
			if err != nil || !bytes.Equal(wire, rewire) {
				t.Fatal("canonical envelope bytes differ", err)
			}
			if record.ID == "E04-exact-nearby-values" {
				revisedPreserveNearbyScalars(t, record)
			}
			if record.ID == "E09-source-history-derived-fraction" {
				revisedPreserveSuppliedScopes(t, record)
			}
		})
	}
	if count != 7 {
		t.Fatal("preservation-only count", count)
	}
}

func revisedPreserveNearbyScalars(t *testing.T, record revisedRecord) {
	values := revisedMustField[[]string](t, record, "input")
	if len(values) != 2 {
		t.Fatal("exact input scalar set")
	}
	expected := revisedMustField[struct {
		Equal      bool     `json:"equal"`
		Difference string   `json:"difference_seconds"`
		Rounded    []string `json:"explicit_nearest_microsecond"`
		EventIDs   []string `json:"event_ids"`
	}](t, record, "expected")
	// Units/rounding/event IDs remain uninterpreted in the complete envelope.
	// A declared synthetic codec axis tests these naked rational scalars only.
	definition := revisedAxis{Identity: "standalone-nearby-reference-values", Profile: "Q", Version: 1, Reference: "test-only-value-codec", Unit: "abstract-position"}
	axis, err := revisedAxisValue(definition)
	if err != nil {
		t.Fatal(err)
	}
	positions := make([]temporal.Position, len(values))
	for i, text := range values {
		raw, err := json.Marshal(text)
		if err != nil {
			t.Fatal(err)
		}
		position, err := revisedPosition(axis, raw, temporal.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		wire, err := temporal.AppendPosition(nil, position, temporal.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		positions[i], err = temporal.DecodePosition(wire, axis, temporal.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		actual, ok := positions[i].Rational()
		if !ok || actual.String() != text || positions[i].Axis().Descriptor() != axis.Descriptor() {
			t.Fatal("exact scalar/profile/axis changed")
		}
	}
	order, err := temporal.ComparePositions(positions[0], positions[1], temporal.Limits{})
	if err != nil || (order == temporal.Equal) != expected.Equal || order != temporal.Less {
		t.Fatal("close values were collapsed", order, err)
	}
}

func revisedPreserveSuppliedScopes(t *testing.T, record revisedRecord) {
	sources := revisedMustField[[]revisedRegion](t, record, "source_regions")
	if len(sources) != 2 {
		t.Fatal("exact source region set")
	}
	result := revisedMustField[revisedRegion](t, record, "supplied_result")
	shifted := revisedMustField[revisedRegion](t, record, "supplied_shifted_result")
	for _, region := range append(sources, result, shifted) {
		scope := revisedMustScope(t, region, temporal.Limits{})
		revisedAssertScope(t, region, scope)
	}
	expected := revisedMustField[struct {
		Fraction   string `json:"exact_fraction"`
		NoRounding bool   `json:"no_rounding"`
	}](t, record, "expected")
	if !expected.NoRounding {
		t.Fatal("unknown supplied-value contract")
	}
	fraction, err := temporal.ParseRational(expected.Fraction, temporal.Limits{})
	if err != nil || fraction.String() != "1/3" {
		t.Fatal("exact supplied fraction changed", err)
	}
	// We consume supplied source/result values; no relation join or shift runs.
}
