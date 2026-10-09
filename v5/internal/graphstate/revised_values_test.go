package graphstate

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func revisedAxisValue(raw revisedAxis) (temporal.Axis, error) {
	profile := temporal.Profile(0)
	switch raw.Profile {
	case "Z":
		profile = temporal.ProfileIntegerZ
	case "Q":
		profile = temporal.ProfileRationalQ
	case "QN":
		profile = temporal.ProfileLexicographicQN
	default:
		return temporal.Axis{}, temporal.ErrUnknownProfile
	}
	hash := sha256.Sum256([]byte("revised-reference-axis:" + raw.Identity))
	var id temporal.AxisID
	copy(id[:], hash[:16])
	return temporal.NewAxis(temporal.AxisDescriptor{ID: id, Profile: profile, Version: raw.Version, Reference: raw.Reference, CanonicalUnit: raw.Unit}, temporal.Limits{})
}

func revisedCoordinateText(raw json.RawMessage) (string, error) {
	var value any
	if err := revisedStrictJSON(raw, &value); err != nil {
		return "", err
	}
	switch v := value.(type) {
	case string:
		return v, nil
	case json.Number:
		if _, ok := new(big.Int).SetString(v.String(), 10); !ok {
			return "", temporal.ErrInvalidValue
		}
		return v.String(), nil
	default:
		return "", temporal.ErrInvalidValue
	}
}

func revisedPosition(axis temporal.Axis, raw json.RawMessage, limits temporal.Limits) (temporal.Position, error) {
	if axis.Descriptor().Profile == temporal.ProfileLexicographicQN {
		var pair []json.RawMessage
		if err := revisedStrictJSON(raw, &pair); err != nil || len(pair) != 2 {
			return temporal.Position{}, temporal.ErrIncompatibleDomain
		}
		modelText, err := revisedCoordinateText(pair[0])
		if err != nil {
			return temporal.Position{}, err
		}
		microText, err := revisedCoordinateText(pair[1])
		if err != nil {
			return temporal.Position{}, err
		}
		model, err := temporal.ParseRational(modelText, limits)
		if err != nil {
			return temporal.Position{}, err
		}
		micro, err := temporal.ParseInteger(microText, limits)
		if err != nil {
			return temporal.Position{}, err
		}
		return temporal.LexPosition(axis, model, micro)
	}
	text, err := revisedCoordinateText(raw)
	if err != nil {
		return temporal.Position{}, err
	}
	switch axis.Descriptor().Profile {
	case temporal.ProfileIntegerZ:
		value, err := temporal.ParseInteger(text, limits)
		if err != nil {
			return temporal.Position{}, err
		}
		return temporal.IntegerPosition(axis, value)
	case temporal.ProfileRationalQ:
		value, err := temporal.ParseRational(text, limits)
		if err != nil {
			return temporal.Position{}, err
		}
		return temporal.RationalPosition(axis, value)
	default:
		return temporal.Position{}, temporal.ErrUnknownProfile
	}
}

func revisedBound(axis temporal.Axis, raw json.RawMessage, inclusive bool, limits temporal.Limits) (temporal.Bound, error) {
	var symbol string
	if json.Unmarshal(raw, &symbol) == nil {
		if symbol == "-inf" {
			if inclusive {
				return temporal.Bound{}, temporal.ErrInvalidBound
			}
			return temporal.NegativeInfinity(), nil
		}
		if symbol == "+inf" {
			if inclusive {
				return temporal.Bound{}, temporal.ErrInvalidBound
			}
			return temporal.PositiveInfinity(), nil
		}
	}
	position, err := revisedPosition(axis, raw, limits)
	if err != nil {
		return temporal.Bound{}, err
	}
	return temporal.FiniteBound(position, inclusive)
}

func revisedSpan(axis temporal.Axis, piece revisedPiece, limits temporal.Limits) (temporal.Scope, error) {
	lower, err := revisedBound(axis, piece.Lower, piece.LowerClosed, limits)
	if err != nil {
		return temporal.Scope{}, err
	}
	upper, err := revisedBound(axis, piece.Upper, piece.UpperClosed, limits)
	if err != nil {
		return temporal.Scope{}, err
	}
	return temporal.Span(axis, lower, upper, limits)
}

func revisedScope(region revisedRegion, limits temporal.Limits) (temporal.Scope, error) {
	axis, err := revisedAxisValue(region.Axis)
	if err != nil {
		return temporal.Scope{}, err
	}
	parts := make([]temporal.Scope, len(region.Pieces))
	for i, piece := range region.Pieces {
		parts[i], err = revisedSpan(axis, piece, limits)
		if err != nil {
			return temporal.Scope{}, err
		}
	}
	return temporal.Region(axis, parts, limits)
}

func revisedPositionJSON(position temporal.Position) any {
	switch position.Profile() {
	case temporal.ProfileIntegerZ:
		value, _ := position.Integer()
		return value.String()
	case temporal.ProfileRationalQ:
		value, _ := position.Rational()
		return value.String()
	case temporal.ProfileLexicographicQN:
		model, micro, _ := position.Lex()
		return []any{model.String(), json.Number(micro.String())}
	default:
		panic("invalid position in revised test")
	}
}

// Go uses half-open discrete spans while the independent oracle uses closed
// integer endpoints. Conversion below uses independent exact integer arithmetic,
// including the QxN predecessor only when its natural microstep is positive.
func revisedNormalizedScopeJSON(scope temporal.Scope, definition revisedAxis) any {
	pieces := []map[string]any{}
	for _, part := range scope.Parts() {
		lower, upper, _ := part.Bounds()
		value := func(bound temporal.Bound) any {
			switch bound.Kind() {
			case temporal.BoundNegativeInfinity:
				return "-inf"
			case temporal.BoundPositiveInfinity:
				return "+inf"
			default:
				position, _ := bound.Position()
				return revisedPositionJSON(position)
			}
		}
		lo, hi := value(lower), value(upper)
		upperClosed := upper.Inclusive()
		if upper.Kind() == temporal.BoundFinite && !upperClosed {
			switch scope.Axis().Descriptor().Profile {
			case temporal.ProfileIntegerZ:
				n, ok := new(big.Int).SetString(hi.(string), 10)
				if !ok {
					panic("invalid integer rendering")
				}
				hi, upperClosed = n.Sub(n, big.NewInt(1)).String(), true
			case temporal.ProfileLexicographicQN:
				pair := hi.([]any)
				n, ok := new(big.Int).SetString(string(pair[1].(json.Number)), 10)
				if !ok {
					panic("invalid natural rendering")
				}
				if n.Sign() > 0 {
					hi, upperClosed = []any{pair[0], json.Number(n.Sub(n, big.NewInt(1)).String())}, true
				}
			}
		}
		pieces = append(pieces, map[string]any{"lower": lo, "upper": hi, "lower_closed": lower.Inclusive(), "upper_closed": upperClosed})
	}
	return map[string]any{"axis": definition, "pieces": pieces}
}

func revisedAssertScope(t testing.TB, expected revisedRegion, actual temporal.Scope) {
	t.Helper()
	descriptor := actual.Axis().Descriptor()
	axis, err := revisedAxisValue(expected.Axis)
	if err != nil {
		t.Fatal(err)
	}
	if descriptor != axis.Descriptor() {
		t.Fatal("axis definition lost", descriptor, axis.Descriptor())
	}
	raw, err := json.Marshal(expected)
	if err != nil {
		t.Fatal(err)
	}
	revisedJSONEqual(t, raw, revisedNormalizedScopeJSON(actual, expected.Axis))
	wire, err := temporal.AppendScope(nil, actual, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := temporal.DecodeScope(wire, axis, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	revisedJSONEqual(t, raw, revisedNormalizedScopeJSON(decoded, expected.Axis))
}

type revisedReferences struct {
	revisions       map[string]uint64
	provenance      map[string]uint64
	values          map[string]uint64
	revisionNames   map[uint64]string
	provenanceNames map[uint64]string
	valueBytes      map[uint64]json.RawMessage
}

func newRevisedReferences() *revisedReferences {
	return &revisedReferences{revisions: map[string]uint64{}, provenance: map[string]uint64{}, values: map[string]uint64{}, revisionNames: map[uint64]string{}, provenanceNames: map[uint64]string{}, valueBytes: map[uint64]json.RawMessage{}}
}

func revisedIdentify(names map[string]uint64, name string) uint64 {
	if id := names[name]; id != 0 {
		return id
	}
	id := uint64(len(names) + 1)
	names[name] = id
	return id
}

func (refs *revisedReferences) revision(revision, provenance *string) (state.Revision, error) {
	if revision == nil || *revision == "" {
		return state.Revision{}, state.ErrInvalidRevision
	}
	id := revisedIdentify(refs.revisions, *revision)
	refs.revisionNames[id] = *revision
	prov := uint64(0)
	if provenance != nil {
		prov = revisedIdentify(refs.provenance, *provenance)
		refs.provenanceNames[prov] = *provenance
	}
	return state.NewRevision(id, prov)
}

func revisedScalar(raw json.RawMessage) (Scalar, error) {
	var value any
	if err := revisedStrictJSON(raw, &value); err != nil {
		return Scalar{}, err
	}
	switch v := value.(type) {
	case nil:
		return Null(), nil
	case string:
		return String(v), nil
	case bool:
		return Bool(v), nil
	case json.Number:
		n, err := strconv.ParseInt(v.String(), 10, 64)
		if err != nil {
			return Scalar{}, ErrTypeMismatch
		}
		return I64(n), nil
	default:
		return Scalar{}, fmt.Errorf("%w: object/container has no faithful scalar attachment", errRevisedPending)
	}
}

func (refs *revisedReferences) value(raw json.RawMessage) (state.ValueRef, error) {
	scalar, err := revisedScalar(raw)
	if err != nil {
		return state.ValueRef{}, err
	}
	if scalar.Kind() == ScalarNull {
		return state.Null(), nil
	}
	key, err := scalar.EqualityKey(Limits{})
	if err != nil {
		return state.ValueRef{}, err
	}
	id := revisedIdentify(refs.values, key)
	refs.valueBytes[id] = bytes.Clone(raw)
	return state.NewValueRef(id, uint64(len(raw)))
}

func (refs *revisedReferences) apply(current state.State, operation revisedComponentOp, limits state.Limits) (state.Result, error) {
	scope, err := revisedScope(operation.Scope, limits.Temporal)
	if err != nil {
		return state.Result{}, err
	}
	revision, err := refs.revision(operation.Cell.Revision, operation.Cell.Provenance)
	if err != nil {
		return state.Result{}, err
	}
	if !operation.Cell.Present {
		if !bytes.Equal(bytes.TrimSpace(operation.Cell.Value), []byte("null")) {
			return state.Result{}, errors.New("retracted value must be null")
		}
		return current.Unset(scope, revision, limits)
	}
	value, err := refs.value(operation.Cell.Value)
	if err != nil {
		return state.Result{}, err
	}
	return current.Set(scope, value, revision, limits)
}

func (refs *revisedReferences) cell(cell state.Cell) revisedCell {
	raw := json.RawMessage("null")
	if cell.Present() && !cell.Value().IsNull() {
		raw = bytes.Clone(refs.valueBytes[cell.Value().ID()])
		if raw == nil {
			panic("unknown payload handle")
		}
	}
	var revision, provenance *string
	if cell.Revision().ID() != 0 {
		name, exists := refs.revisionNames[cell.Revision().ID()]
		if !exists {
			panic("unknown revision handle")
		}
		revision = new(name)
	}
	if cell.Revision().Provenance() != 0 {
		name, exists := refs.provenanceNames[cell.Revision().Provenance()]
		if !exists {
			panic("unknown provenance handle")
		}
		provenance = new(name)
	}
	return revisedCell{Present: cell.Present(), Value: raw, Revision: revision, Provenance: provenance}
}

func revisedAssertCell(t testing.TB, expected revisedCell, actual state.Cell, refs *revisedReferences) {
	t.Helper()
	raw, err := json.Marshal(expected)
	if err != nil {
		t.Fatal(err)
	}
	revisedJSONEqual(t, raw, refs.cell(actual))
}
