package graphstate

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func revisedMustField[T any](t testing.TB, record revisedRecord, name string) T {
	t.Helper()
	value, err := revisedField[T](record, name)
	if err != nil {
		t.Fatal(record.ID, name, err)
	}
	return value
}

func revisedMustScope(t testing.TB, region revisedRegion, limits temporal.Limits) temporal.Scope {
	t.Helper()
	scope, err := revisedScope(region, limits)
	if err != nil {
		t.Fatal(err)
	}
	return scope
}

func TestRevisedExecutableNativeGoldenAnswers(t *testing.T) {
	executed := 0
	for _, record := range revisedLoad(t) {
		if revisedRules[record.ID].lane != revisedNative {
			continue
		}
		executed++
		t.Run(record.ID, func(t *testing.T) {
			switch record.Kind {
			case "graph":
				revisedRunGraph(t, record)
			case "region":
				revisedRunSupport(t, record)
			case "component":
				revisedRunComponent(t, record)
			case "knowledge":
				revisedRunHardKnowledge(t, record)
			case "failure":
				revisedRunAxisMismatch(t, record)
			default:
				t.Fatal("native dispatcher has no complete contract", record.Kind)
			}
		})
	}
	if executed != 14 {
		t.Fatal("native acceptance count", executed)
	}
}

func revisedRunSupport(t *testing.T, record revisedRecord) {
	definition := revisedMustField[revisedAxis](t, record, "axis")
	axis, err := revisedAxisValue(definition)
	if err != nil {
		t.Fatal(err)
	}
	input := revisedMustField[struct {
		Lower json.RawMessage `json:"lower"`
		Upper json.RawMessage `json:"upper"`
	}](t, record, "input")
	expected := revisedMustField[struct {
		Open  revisedRegion `json:"open_interval"`
		Point revisedRegion `json:"point"`
		Half  revisedRegion `json:"half_open"`
		Equal bool          `json:"point_equals_half_open"`
	}](t, record, "expected")
	open, err := revisedSpan(axis, revisedPiece{Lower: input.Lower, Upper: input.Upper}, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	point, err := revisedSpan(axis, revisedPiece{Lower: input.Lower, Upper: input.Lower, LowerClosed: true, UpperClosed: true}, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	half, err := revisedSpan(axis, revisedPiece{Lower: input.Lower, Upper: input.Upper, LowerClosed: true}, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	revisedAssertScope(t, expected.Open, open)
	revisedAssertScope(t, expected.Point, point)
	revisedAssertScope(t, expected.Half, half)
	same, err := point.SameSupport(half, temporal.Limits{})
	if err != nil || same != expected.Equal {
		t.Fatal("same-support equality", same, err)
	}
	// Complete endpoint answers make both inclusion and omission visible.
	for _, probe := range []struct {
		raw               json.RawMessage
		open, point, half bool
	}{
		{input.Lower, false, true, true}, {input.Upper, false, false, false},
	} {
		position, err := revisedPosition(axis, probe.raw, temporal.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		for _, check := range []struct {
			scope temporal.Scope
			want  bool
		}{{open, probe.open}, {point, probe.point}, {half, probe.half}} {
			holds, err := check.scope.Contains(position, temporal.Limits{})
			if err != nil || holds != check.want {
				t.Fatal("endpoint membership", string(probe.raw), holds, check.want, err)
			}
		}
	}
}

func revisedRunComponent(t *testing.T, record revisedRecord) {
	definition := revisedMustField[revisedAxis](t, record, "axis")
	axis, err := revisedAxisValue(definition)
	if err != nil {
		t.Fatal(err)
	}
	refs := newRevisedReferences()
	current, err := state.New(axis, state.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if record.ID == "budget-tight-Z-replay-no-transient-successor" {
		limits := revisedMustField[struct {
			Bits         int `json:"coordinate_bits"`
			Region       int `json:"region_fragments"`
			Component    int `json:"component_fragments"`
			Entities     int `json:"entities"`
			Operations   int `json:"operations"`
			InputJSON    int `json:"input_json_bytes"`
			SnapshotJSON int `json:"snapshot_json_bytes"`
			ChangeJSON   int `json:"change_json_bytes"`
			Total        int `json:"total_fragments"`
		}](t, record, "limits")
		// Only matching mathematical knobs are applied. Oracle JSON envelope
		// budgets do not become Go metadata, wire or heap byte budgets.
		policy := state.Limits{Temporal: temporal.Limits{MaxMagnitudeBits: limits.Bits, MaxRegionPieces: limits.Region}, MaxPieces: limits.Component}
		first := revisedComponentOp{Scope: revisedMustField[revisedRegion](t, record, "initial_scope"), Cell: revisedMustField[revisedCell](t, record, "initial_cell")}
		mutation := revisedComponentOp{Scope: revisedMustField[revisedRegion](t, record, "mutation_scope"), Cell: revisedMustField[revisedCell](t, record, "mutation_cell")}
		result, err := refs.apply(current, first, policy)
		if err != nil {
			t.Fatal(err)
		}
		old := result.State()
		replay, err := refs.apply(old, mutation, policy)
		if err != nil {
			t.Fatal(err)
		}
		expected := revisedMustField[struct {
			Unchanged bool          `json:"state_unchanged"`
			Changes   int           `json:"change_count"`
			Scope     revisedRegion `json:"scope"`
		}](t, record, "expected")
		if !expected.Unchanged || len(replay.Changes()) != expected.Changes || len(replay.State().Pieces()) != 1 {
			t.Fatal("tight replay answer")
		}
		revisedAssertScope(t, expected.Scope, replay.State().Pieces()[0].Scope())
		revisedAssertScope(t, expected.Scope, old.Pieces()[0].Scope())
		revisedAssertCell(t, first.Cell, replay.State().Pieces()[0].Cell(), refs)
		return
	}
	operations := revisedMustField[[]revisedComponentOp](t, record, "operations")
	saved := []state.State{}
	results := []state.Result{}
	for _, operation := range operations {
		result, err := refs.apply(current, operation, state.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		current = result.State()
		saved = append(saved, current)
		results = append(results, result)
	}
	if record.ID == "E02-components-correction-retraction" {
		expected := revisedMustField[struct {
			Old       []revisedCell `json:"old"`
			Corrected []revisedCell `json:"corrected"`
			Retracted []revisedCell `json:"retracted"`
		}](t, record, "expected")
		probes := revisedMustField[[]json.RawMessage](t, record, "probes")
		for phase, cells := range [][]revisedCell{expected.Old, expected.Corrected, expected.Retracted} {
			if len(cells) != len(probes) {
				t.Fatal("incomplete cell oracle")
			}
			for i, raw := range probes {
				position, err := revisedPosition(axis, raw, temporal.Limits{})
				if err != nil {
					t.Fatal(err)
				}
				cell, err := saved[phase].At(position, state.Limits{})
				if err != nil {
					t.Fatal(err)
				}
				revisedAssertCell(t, cells[i], cell, refs)
			}
		}
		expectedChanges := revisedMustField[[]struct {
			Scope  revisedRegion `json:"scope"`
			Before revisedCell   `json:"before"`
			After  revisedCell   `json:"after"`
		}](t, record, "changes")
		changes := results[1].Changes()
		if len(changes) != len(expectedChanges) {
			t.Fatal("exact change set", len(changes), len(expectedChanges))
		}
		for i, change := range changes {
			revisedAssertScope(t, expectedChanges[i].Scope, change.Scope())
			revisedAssertCell(t, expectedChanges[i].Before, change.Before(), refs)
			revisedAssertCell(t, expectedChanges[i].After, change.After(), refs)
		}
		return
	}
	if record.ID != "SEM12-null-revision-replay" {
		t.Fatal("unknown component contract", record.ID)
	}
	expected := revisedMustField[struct {
		Null          revisedCell `json:"null"`
		Corrected     revisedCell `json:"corrected_null"`
		Retracted     revisedCell `json:"retracted"`
		Never         revisedCell `json:"never_asserted"`
		NewChanges    int         `json:"new_revision_change_count"`
		ReplayChanges int         `json:"replay_change_count"`
	}](t, record, "expected")
	position, err := revisedPosition(axis, json.RawMessage("5"), temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	for _, check := range []struct {
		value state.State
		want  revisedCell
	}{{saved[0], expected.Null}, {saved[1], expected.Corrected}, {saved[2], expected.Corrected}, {saved[3], expected.Retracted}} {
		cell, err := check.value.At(position, state.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		revisedAssertCell(t, check.want, cell, refs)
	}
	outside, err := revisedPosition(axis, json.RawMessage("12"), temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	for _, snapshot := range saved {
		never, err := snapshot.At(outside, state.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		revisedAssertCell(t, expected.Never, never, refs)
	}
	if len(results[1].Changes()) != expected.NewChanges || len(results[2].Changes()) != expected.ReplayChanges {
		t.Fatal("revision versus replay")
	}
}

func revisedRunHardKnowledge(t *testing.T, record revisedRecord) {
	support := revisedMustScope(t, revisedMustField[revisedRegion](t, record, "support"), temporal.Limits{})
	window := revisedMustScope(t, revisedMustField[revisedRegion](t, record, "window"), temporal.Limits{})
	knowledge, err := temporal.HardPointKnowledge(support, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	wire, err := temporal.AppendPointKnowledge(nil, knowledge, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := temporal.DecodePointKnowledge(wire, support.Axis(), temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	expected := revisedMustField[map[string]struct {
		Holds       bool   `json:"holds"`
		Consistency string `json:"consistency"`
	}](t, record, "expected")
	if len(expected) != 2 {
		t.Fatal("exact predicate answer set")
	}
	for _, value := range []temporal.PointKnowledge{knowledge, decoded} {
		for _, predicate := range []string{"possible", "definite"} {
			var result temporal.SupportResult
			if predicate == "possible" {
				result, err = value.PossibleIn(window, temporal.Limits{})
			} else {
				result, err = value.DefiniteIn(window, temporal.Limits{})
			}
			if err != nil {
				t.Fatal(err)
			}
			want, exists := expected[predicate]
			if !exists {
				t.Fatal("missing predicate answer", predicate)
			}
			consistency := ""
			switch result.Consistency {
			case temporal.KnowledgeConsistent:
				consistency = "consistent"
			case temporal.KnowledgeInconsistent:
				consistency = "inconsistent"
			default:
				t.Fatal("unknown consistency", result)
			}
			if result.Holds != want.Holds || consistency != want.Consistency {
				t.Fatalf("%s complete answer: %+v, want %+v", predicate, result, want)
			}
		}
	}
}

func revisedRunAxisMismatch(t *testing.T, record revisedRecord) {
	leftDefinition := revisedMustField[revisedAxis](t, record, "left_axis")
	rightDefinition := revisedMustField[revisedAxis](t, record, "right_axis")
	leftAxis, err := revisedAxisValue(leftDefinition)
	if err != nil {
		t.Fatal(err)
	}
	rightAxis, err := revisedAxisValue(rightDefinition)
	if err != nil {
		t.Fatal(err)
	}
	if leftAxis.Descriptor().ID != rightAxis.Descriptor().ID || leftAxis.DefinitionHash() == rightAxis.DefinitionHash() {
		t.Fatal("identity versus definition")
	}
	if revisedMustField[string](t, record, "left_scope") != "empty" || revisedMustField[string](t, record, "right_scope") != "empty" || revisedMustField[string](t, record, "expected_error") != "AXIS_MISMATCH" {
		t.Fatal("unsupported mismatch input")
	}
	left, err := temporal.Empty(leftAxis)
	if err != nil {
		t.Fatal(err)
	}
	right, err := temporal.Empty(rightAxis)
	if err != nil {
		t.Fatal(err)
	}
	for _, operation := range []func() error{
		func() error { _, err := left.Union(right, temporal.Limits{}); return err },
		func() error { _, err := left.Intersection(right, temporal.Limits{}); return err },
		func() error { _, err := left.Difference(right, temporal.Limits{}); return err },
		func() error { _, err := left.SameSupport(right, temporal.Limits{}); return err },
	} {
		if err := operation(); !errors.Is(err, temporal.ErrAxisMismatch) {
			t.Fatal("empty input must still check definitions", err)
		}
	}
}

func revisedOpaque(record revisedRecord) (temporal.OpaqueDescriptor, error) {
	hash := sha256.Sum256([]byte("revised-reference-envelope:" + record.ID))
	var id temporal.DescriptorID
	copy(id[:], hash[:16])
	// This is an explicitly declared reference-JSON envelope, not a translation
	// into a native calendar, constraint, summary or graph property schema.
	return temporal.PreserveOpaqueDescriptor(temporal.OpaqueDescriptorSpec{ID: id, Type: "revised-reference-json", SchemaVersion: 1, Payload: record.Raw}, temporal.Limits{})
}

func TestRevisedUnsupportedNativePredicatesAreActualSentinelRefusals(t *testing.T) {
	count := 0
	for _, record := range revisedLoad(t) {
		if revisedRules[record.ID].lane != revisedRefusal {
			continue
		}
		count++
		t.Run(record.ID, func(t *testing.T) {
			support := revisedMustScope(t, revisedMustField[revisedRegion](t, record, "support"), temporal.Limits{})
			window := revisedMustScope(t, revisedMustField[revisedRegion](t, record, "window"), temporal.Limits{})
			kind := revisedMustField[string](t, record, "knowledge_kind")
			if revisedMustField[string](t, record, "expected_error") != "UNSUPPORTED_PREDICATE" {
				t.Fatal("unknown refusal")
			}
			var knowledge temporal.PointKnowledge
			var err error
			switch kind {
			case "confidence_95_percent", "confidence_100_percent":
				level := "95/100"
				if kind == "confidence_100_percent" {
					level = "1"
				}
				fraction, parseErr := temporal.ParseRational(level, temporal.Limits{})
				if parseErr != nil {
					t.Fatal(parseErr)
				}
				knowledge, err = temporal.ConfidencePointKnowledge(support, fraction, temporal.Limits{})
			case "nominal":
				// The corpus supplies no nominal coordinate. This is only a
				// type-level refusal probe, not an imported source placement.
				lower, _, _ := window.Bounds()
				position, finite := lower.Position()
				if !finite {
					t.Fatal("probe requires a finite bound")
				}
				knowledge, err = temporal.NominalPointKnowledge(position, temporal.Limits{})
			case "opaque_correlated_constraint":
				descriptor, preserveErr := revisedOpaque(record)
				if preserveErr != nil {
					t.Fatal(preserveErr)
				}
				knowledge, err = temporal.OpaquePointKnowledge(support.Axis(), descriptor, temporal.Limits{})
			case "unspecified":
				knowledge, err = temporal.UnspecifiedPointKnowledge(support.Axis(), temporal.Limits{})
			default:
				t.Fatal("unknown knowledge kind", kind)
			}
			if err != nil {
				t.Fatal(err)
			}
			for _, predicate := range []func(temporal.Scope, temporal.Limits) (temporal.SupportResult, error){knowledge.PossibleIn, knowledge.DefiniteIn} {
				result, err := predicate(window, temporal.Limits{})
				if !errors.Is(err, temporal.ErrUnsupportedPredicate) || result != (temporal.SupportResult{}) {
					t.Fatal("actual native refusal", result, err)
				}
			}
		})
	}
	if count != 5 {
		t.Fatal("refusal count", count)
	}
}
