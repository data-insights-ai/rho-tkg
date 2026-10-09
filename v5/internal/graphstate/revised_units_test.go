package graphstate

import (
	"bytes"
	"encoding/json"
	"errors"
	"math/big"
	"strconv"
	"strings"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

// These seven fixtures execute ConvertUnits/InstantMillis numerical contracts.
// Fixture-qualified axes are explicit; these tests do not register a default
// graph axis, implement the public Instant API or exercise an importer.
func revisedRunUnitMapping(t *testing.T, record revisedRecord) {
	definition := revisedMustField[revisedAxis](t, record, "target_axis")
	axis, err := revisedAxisValue(definition)
	if err != nil {
		t.Fatal(err)
	}
	source := revisedMustField[string](t, record, "source_coordinate")
	unit := revisedMustField[string](t, record, "source_unit")
	if _, exists := record.Fields["mapping"]; exists {
		mapping := revisedMustField[string](t, record, "mapping")
		scaled := mapping == "divide_by_1000" && unit == "microsecond" && definition.Unit == "millisecond"
		identity := mapping == "identity" && unit == definition.Unit
		if !scaled && !identity {
			t.Fatal("unsupported declared mapping", mapping, unit, definition.Unit)
		}
	}
	if _, exists := record.Fields["note"]; exists {
		if revisedMustField[string](t, record, "note") == "" {
			t.Fatal("missing scope note")
		}
	}
	input, err := temporal.ParseRational(source, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	before := input.String()
	descriptor, digest := axis.Descriptor(), axis.DefinitionHash()
	position, err := temporal.ConvertUnits(input, unit, axis, temporal.Limits{})
	if input.String() != before || axis.Descriptor() != descriptor || axis.DefinitionHash() != digest {
		t.Fatal("conversion changed input or target axis")
	}
	if _, refusal := record.Fields["expected_error"]; refusal {
		code := revisedMustField[string](t, record, "expected_error")
		want := revisedUnitError(t, code)
		if !errors.Is(err, want) || position != (temporal.Position{}) {
			t.Fatal("exact conversion refusal", code, position, err)
		}
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	expected := revisedMustField[string](t, record, "expected")
	revisedExactPositionRoundTrip(t, position, axis, expected)
	if record.ID == "V0-default-ms-identity" {
		// The fixture's explicit numeric identity is also exactly extractable;
		// its name supplies neither a registered default nor a reference origin.
		number, err := temporal.InstantMillis(position, temporal.Limits{})
		if err != nil || strconv.FormatInt(number, 10) != expected {
			t.Fatal("explicit millisecond identity", number, err)
		}
		revisedExactPositionRoundTrip(t, position, axis, expected)
	}
}

func revisedUnitError(t testing.TB, code string) error {
	t.Helper()
	switch code {
	case "INCOMPATIBLE_DOMAIN":
		return temporal.ErrIncompatibleDomain
	case "EXPLICIT_MAPPING_REQUIRED":
		return temporal.ErrExplicitMappingRequired
	case "INSTANT_CODEC_RANGE":
		return temporal.ErrInstantCodecRange
	case "NONINTEGRAL_INSTANT_CODEC":
		return temporal.ErrNonintegralInstantCodec
	default:
		t.Fatal("unknown numerical refusal", code)
		return nil
	}
}

// The codec record provides no axis. Reuse complete declarations already in
// the immutable corpus for standalone numeric codec probes; invent no default.
func revisedUnitFixtureAxis(t testing.TB, id string) temporal.Axis {
	t.Helper()
	for _, record := range revisedLoad(t) {
		if record.ID == id {
			axis, err := revisedAxisValue(revisedMustField[revisedAxis](t, record, "target_axis"))
			if err != nil {
				t.Fatal(err)
			}
			return axis
		}
	}
	t.Fatal("missing qualified codec probe axis", id)
	return temporal.Axis{}
}

func revisedExactPositionRoundTrip(t testing.TB, position temporal.Position, axis temporal.Axis, expected string) temporal.Position {
	t.Helper()
	assert := func(value temporal.Position) {
		t.Helper()
		if value.Axis().Descriptor() != axis.Descriptor() || value.Axis().DefinitionHash() != axis.DefinitionHash() || revisedPositionJSON(value) != expected {
			t.Fatal("exact coordinate/profile/identity/definition changed", value, expected)
		}
	}
	assert(position)
	wire, err := temporal.AppendPosition(nil, position, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	before := bytes.Clone(wire)
	decoded, err := temporal.DecodePosition(wire, axis, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	assert(decoded)
	assert(position)
	if !bytes.Equal(before, wire) {
		t.Fatal("decoder changed source bytes")
	}
	reencoded, err := temporal.AppendPosition(nil, decoded, temporal.Limits{})
	if err != nil || !bytes.Equal(wire, reencoded) {
		t.Fatal("canonical generic coordinate round-trip", err)
	}
	return decoded
}

func revisedScalarProbe(t testing.TB, axis temporal.Axis, text string) temporal.Position {
	t.Helper()
	raw, err := json.Marshal(text)
	if err != nil {
		t.Fatal(err)
	}
	position, err := revisedPosition(axis, raw, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	return position
}

func revisedRunInstantCodec(t *testing.T, record revisedRecord) {
	if record.ID != "V0-default-instant-codec-range" || revisedMustField[string](t, record, "semantics") == "" {
		t.Fatal("unknown signed millisecond codec contract")
	}
	expected := revisedMustField[struct {
		Minimum    string `json:"minimum_ms"`
		Maximum    string `json:"maximum_ms"`
		Below      string `json:"below_minimum"`
		Above      string `json:"above_maximum"`
		Fractional string `json:"fractional_ms"`
		DenseQ     string `json:"general_dense_Q_value"`
	}](t, record, "expected")
	axes := []temporal.Axis{
		revisedUnitFixtureAxis(t, "V0-default-ms-identity"),
		revisedUnitFixtureAxis(t, "V0-units-us-to-ms-Q-1"),
	}
	for _, axis := range axes {
		for _, text := range []string{expected.Minimum, expected.Maximum} {
			position := revisedScalarProbe(t, axis, text)
			decoded := revisedExactPositionRoundTrip(t, position, axis, text)
			for _, input := range []temporal.Position{position, decoded} {
				got, err := temporal.InstantMillis(input, temporal.Limits{})
				if err != nil || strconv.FormatInt(got, 10) != text {
					t.Fatal("signed int64 codec boundary", text, got, err)
				}
				revisedExactPositionRoundTrip(t, input, axis, text)
			}
		}
		for _, boundary := range []struct {
			text, code string
			delta      int64
		}{{expected.Minimum, expected.Below, -1}, {expected.Maximum, expected.Above, 1}} {
			integer, ok := new(big.Int).SetString(boundary.text, 10)
			if !ok {
				t.Fatal("noninteger boundary", boundary.text)
			}
			// Independent unbounded arithmetic derives exactly one outside each
			// supplied boundary; the generic mathematical domain remains valid.
			text := integer.Add(integer, big.NewInt(boundary.delta)).String()
			position := revisedScalarProbe(t, axis, text)
			decoded := revisedExactPositionRoundTrip(t, position, axis, text)
			want := revisedUnitError(t, boundary.code)
			for _, input := range []temporal.Position{position, decoded} {
				got, err := temporal.InstantMillis(input, temporal.Limits{})
				if !errors.Is(err, want) || errors.Is(err, temporal.ErrInvalidEncoding) || got != 0 {
					t.Fatal("out-of-range codec refusal", text, got, err)
				}
				revisedExactPositionRoundTrip(t, input, axis, text)
			}
		}
	}
	fraction, exact := strings.CutSuffix(expected.DenseQ, " remains exact")
	if !exact {
		t.Fatal("unknown dense-Q expectation", expected.DenseQ)
	}
	axis := axes[1]
	position := revisedScalarProbe(t, axis, fraction)
	decoded := revisedExactPositionRoundTrip(t, position, axis, fraction)
	want := revisedUnitError(t, expected.Fractional)
	for _, input := range []temporal.Position{position, decoded} {
		got, err := temporal.InstantMillis(input, temporal.Limits{})
		if !errors.Is(err, want) || got != 0 {
			t.Fatal("fractional millisecond codec refusal", got, err)
		}
		revisedExactPositionRoundTrip(t, input, axis, fraction)
	}
}
