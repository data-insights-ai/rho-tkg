// Package convert is the one edge where v4's `any` property values become the
// typed graph.Value of v5 and back (WP-11). Nothing else in v5compat switches on
// the dynamic type of a property.
package convert

import (
	"errors"
	"fmt"
	"math"

	"github.com/data-insights-ai/rho-tkg/v5/pkg/graph"
)

// Sentinels. Every refusal wraps ErrUnsupportedValue plus the specific reason.
var (
	// ErrUnsupportedValue is the class of every value v5compat refuses.
	ErrUnsupportedValue = errors.New("v5compat: unsupported property value")
	// ErrOutOfRange: an unsigned integer above math.MaxInt64.
	ErrOutOfRange = errors.New("v5compat: integer above MaxInt64")
	// ErrNotFinite: NaN or an infinity.
	ErrNotFinite = errors.New("v5compat: float is not finite")
	// ErrListNotYetSupported: []byte and list values (v5 has no list Value yet).
	ErrListNotYetSupported = errors.New("v5compat: list and []byte values are not supported until v5 lists land (WP-29)")
	// ErrUnsupportedType: any other Go type.
	ErrUnsupportedType = errors.New("v5compat: value type not supported")
)

func refuse(reason error, format string, a ...any) error {
	return fmt.Errorf("%w: %w: %s", ErrUnsupportedValue, reason, fmt.Sprintf(format, a...))
}

// AnyToValue maps a v4 property value onto graph.Value: every int and uint
// width up to MaxInt64 becomes an Int, finite floats a Float, string, bool, and
// nil becomes Null.
func AnyToValue(x any) (graph.Value, error) {
	switch v := x.(type) {
	case nil:
		return graph.Null(), nil
	case string:
		return graph.String(v), nil
	case bool:
		return graph.Bool(v), nil
	case int:
		return graph.Int(int64(v)), nil
	case int8:
		return graph.Int(int64(v)), nil
	case int16:
		return graph.Int(int64(v)), nil
	case int32:
		return graph.Int(int64(v)), nil
	case int64:
		return graph.Int(v), nil
	case uint:
		return unsigned(uint64(v))
	case uint8:
		return graph.Int(int64(v)), nil
	case uint16:
		return graph.Int(int64(v)), nil
	case uint32:
		return graph.Int(int64(v)), nil
	case uint64:
		return unsigned(v)
	case float32:
		return float(float64(v))
	case float64:
		return float(v)
	// TODO(WP-29): map []byte, []int64, []any lists once v5 has a list Value.
	case []byte, []any, []int64, []int, []string, []float64, []bool, []int32, []float32:
		return graph.Value{}, refuse(ErrListNotYetSupported, "%T", x)
	}
	return graph.Value{}, refuse(ErrUnsupportedType, "%T", x)
}

func unsigned(u uint64) (graph.Value, error) {
	if u > math.MaxInt64 {
		return graph.Value{}, refuse(ErrOutOfRange, "%d", u)
	}
	return graph.Int(int64(u)), nil
}

func float(f float64) (graph.Value, error) {
	v, err := graph.Float(f)
	if err != nil {
		return graph.Value{}, refuse(ErrNotFinite, "%v", f)
	}
	return v, nil
}

// ValueToAny reads a Value back as int64, float64, string, bool or nil.
func ValueToAny(v graph.Value) any {
	switch v.Kind() {
	case graph.KindString:
		s, _ := v.AsString()
		return s
	case graph.KindBool:
		b, _ := v.AsBool()
		return b
	case graph.KindInt:
		i, _ := v.AsInt()
		return i
	case graph.KindFloat:
		f, _ := v.AsFloat()
		return f
	}
	return nil
}
