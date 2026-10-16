package graph

import (
	"fmt"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
)

// valueToScalar and scalarToValue are the only conversion between the public
// Value and the engine's Scalar. Kinds are preserved (Int stays I64, Float stays
// F64); the numeric equality rule lives in graphstate.Scalar.Equal and is used
// by Value.Equal through this file.
func valueToScalar(v Value) (graphstate.Scalar, error) {
	switch v.kind {
	case KindNull:
		return graphstate.Null(), nil
	case KindString:
		return graphstate.String(v.s), nil
	case KindBool:
		return graphstate.Bool(v.b), nil
	case KindInt:
		return graphstate.I64(v.i), nil
	case KindFloat:
		s, err := graphstate.F64(v.f)
		if err != nil {
			return graphstate.Scalar{}, fmt.Errorf("%w: float must be finite", ErrInvalidValue)
		}
		return s, nil
	}
	return graphstate.Scalar{}, fmt.Errorf("%w: zero or unknown kind", ErrInvalidValue)
}

func scalarToValue(s graphstate.Scalar) (Value, error) {
	switch s.Kind() {
	case graphstate.ScalarNull:
		return Null(), nil
	case graphstate.ScalarString:
		x, _ := s.StringValue()
		return String(x), nil
	case graphstate.ScalarBool:
		x, _ := s.BoolValue()
		return Bool(x), nil
	case graphstate.ScalarI64:
		x, _ := s.Int64Value()
		return Int(x), nil
	case graphstate.ScalarF64:
		x, _ := s.Float64Value()
		return Float(x)
	}
	return Value{}, fmt.Errorf("%w: engine kind %d has no public Value", ErrInvalidValue, s.Kind())
}

// kindToScalar maps a declared property kind to the engine kind.
func kindToScalar(k ValueKind) (graphstate.ScalarKind, error) {
	switch k {
	case KindString:
		return graphstate.ScalarString, nil
	case KindBool:
		return graphstate.ScalarBool, nil
	case KindInt:
		return graphstate.ScalarI64, nil
	case KindFloat:
		return graphstate.ScalarF64, nil
	}
	return 0, fmt.Errorf("%w: property kind", ErrInvalidConfig)
}
