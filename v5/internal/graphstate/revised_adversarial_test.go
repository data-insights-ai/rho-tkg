package graphstate

import (
	"encoding/json"
	"math/big"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

type revisedLiteralPosition struct {
	model *big.Rat
	micro *big.Int
}

func revisedLiteralCoordinate(t testing.TB, profile string, raw json.RawMessage) revisedLiteralPosition {
	t.Helper()
	value := raw
	micro := big.NewInt(0)
	if profile == "QN" {
		var pair []json.RawMessage
		if err := revisedStrictJSON(raw, &pair); err != nil || len(pair) != 2 {
			t.Fatal("invalid literal tuple", err)
		}
		value = pair[0]
		text, err := revisedCoordinateText(pair[1])
		if err != nil {
			t.Fatal(err)
		}
		var ok bool
		micro, ok = new(big.Int).SetString(text, 10)
		if !ok || micro.Sign() < 0 {
			t.Fatal("invalid literal microstep")
		}
	}
	text, err := revisedCoordinateText(value)
	if err != nil {
		t.Fatal(err)
	}
	model, ok := new(big.Rat).SetString(text)
	if !ok {
		t.Fatal("invalid literal rational")
	}
	return revisedLiteralPosition{model: model, micro: micro}
}

func revisedLiteralCompare(left, right revisedLiteralPosition) int {
	if order := left.model.Cmp(right.model); order != 0 {
		return order
	}
	return left.micro.Cmp(right.micro)
}

func TestRevisedBoundaryAlgebraAgainstIndependentLiteralMembership(t *testing.T) {
	for _, profile := range []string{"Q", "Z", "QN"} {
		t.Run(profile, func(t *testing.T) {
			definition := revisedAxis{Identity: "literal-" + profile, Profile: profile, Version: 1, Reference: "independent-literal-test", Unit: "abstract-position"}
			axis, err := revisedAxisValue(definition)
			if err != nil {
				t.Fatal(err)
			}
			endpoints := []any{"-1", "0", "1", "2"}
			probes := []any{"-2", "-1", "0", "1", "2", "3"}
			if profile == "Q" {
				probes = []any{"-2", "-1", "-1/2", "0", "1/3", "1/2", "1", "3/2", "2", "3"}
			}
			if profile == "QN" {
				endpoints = []any{[]any{"-1", 0}, []any{"0", 0}, []any{"0", 1}, []any{"0", 2}, []any{"1", 0}}
				probes = []any{}
				for _, model := range []string{"-1", "-1/2", "0", "1/3", "1/2", "1"} {
					for _, micro := range []int{0, 1, 2, 3, 10000} {
						probes = append(probes, []any{model, micro})
					}
				}
			}
			type probe struct {
				position temporal.Position
				literal  revisedLiteralPosition
			}
			points := make([]probe, len(probes))
			for i, value := range probes {
				raw, err := json.Marshal(value)
				if err != nil {
					t.Fatal(err)
				}
				position, err := revisedPosition(axis, raw, temporal.Limits{})
				if err != nil {
					t.Fatal(err)
				}
				points[i] = probe{position, revisedLiteralCoordinate(t, profile, raw)}
			}
			type sample struct {
				scope      temporal.Scope
				membership []bool
			}
			samples := []sample{}
			for _, lower := range endpoints {
				for _, upper := range endpoints {
					lo, err := json.Marshal(lower)
					if err != nil {
						t.Fatal(err)
					}
					hi, err := json.Marshal(upper)
					if err != nil {
						t.Fatal(err)
					}
					literalLower, literalUpper := revisedLiteralCoordinate(t, profile, lo), revisedLiteralCoordinate(t, profile, hi)
					for _, lc := range []bool{false, true} {
						for _, hc := range []bool{false, true} {
							scope, err := revisedSpan(axis, revisedPiece{Lower: lo, Upper: hi, LowerClosed: lc, UpperClosed: hc}, temporal.Limits{})
							if err != nil {
								t.Fatal(err)
							}
							member := make([]bool, len(points))
							for i, point := range points {
								left, right := revisedLiteralCompare(point.literal, literalLower), revisedLiteralCompare(point.literal, literalUpper)
								member[i] = (left > 0 || left == 0 && lc) && (right < 0 || right == 0 && hc)
								actual, err := scope.Contains(point.position, temporal.Limits{})
								if err != nil || actual != member[i] {
									t.Fatal("literal membership", lower, upper, lc, hc, actual, member[i], err)
								}
							}
							samples = append(samples, sample{scope, member})
						}
					}
				}
			}
			for _, left := range samples {
				for _, right := range samples {
					intersection, err := left.scope.Intersection(right.scope, temporal.Limits{})
					if err != nil {
						t.Fatal(err)
					}
					difference, err := left.scope.Difference(right.scope, temporal.Limits{})
					if err != nil {
						t.Fatal(err)
					}
					union, err := left.scope.Union(right.scope, temporal.Limits{})
					if err != nil {
						t.Fatal(err)
					}
					for i, point := range points {
						for _, check := range []struct {
							scope temporal.Scope
							want  bool
						}{
							{intersection, left.membership[i] && right.membership[i]},
							{difference, left.membership[i] && !right.membership[i]},
							{union, left.membership[i] || right.membership[i]},
						} {
							actual, err := check.scope.Contains(point.position, temporal.Limits{})
							if err != nil || actual != check.want {
								t.Fatal("independent set answer", profile, actual, check.want, err)
							}
						}
					}
				}
			}
		})
	}
}

func TestRevisedFullCellsKeepTypeRevisionAndProvenanceAcrossReplay(t *testing.T) {
	definition := revisedAxis{Identity: "adversarial-cells", Profile: "Q", Version: 1, Reference: "component-test", Unit: "abstract-position"}
	axis, err := revisedAxisValue(definition)
	if err != nil {
		t.Fatal(err)
	}
	scope := revisedRegion{Axis: definition, Pieces: []revisedPiece{{Lower: json.RawMessage("0"), Upper: json.RawMessage("10"), LowerClosed: true}}}
	refs := newRevisedReferences()
	empty, err := state.New(axis, state.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	cells := []revisedCell{
		{Present: true, Value: json.RawMessage("true"), Revision: new("r"), Provenance: new("p")},
		{Present: true, Value: json.RawMessage("1"), Revision: new("r"), Provenance: new("p")},
		{Present: true, Value: json.RawMessage("1"), Revision: new("r"), Provenance: new("p2")},
		{Present: true, Value: json.RawMessage("1"), Revision: new("r"), Provenance: new("p2")},
	}
	saved := []state.State{}
	current := empty
	for i, cell := range cells {
		result, err := refs.apply(current, revisedComponentOp{Scope: scope, Cell: cell}, state.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		wantChanges := 1
		if i == 3 {
			wantChanges = 0
		}
		if len(result.Changes()) != wantChanges {
			t.Fatal("typed/full-cell change disappeared", i, result.Changes())
		}
		current = result.State()
		saved = append(saved, current)
	}
	position, err := revisedPosition(axis, json.RawMessage("5"), temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	for i, snapshot := range saved {
		actual, err := snapshot.At(position, state.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		revisedAssertCell(t, cells[i], actual, refs)
	}
	never, err := empty.At(position, state.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	revisedAssertCell(t, revisedCell{Value: json.RawMessage("null")}, never, refs)
}
