package importmap

import (
	"math"
	"strconv"
	"strings"

	fixtures "github.com/data-insights-ai/rho-tkg/tools/v5-import-fixtures"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/graph"
)

// Ambiguity kinds of a property value.
const (
	KindUintOverMaxInt64 = "uint_over_maxint64"
	KindNaN              = "nan"
	KindInf              = "inf"
	KindList             = "list"
	KindMap              = "map"
	KindBytes            = "bytes"
	KindTime             = "time"
	KindTemporal         = "temporal"
	KindUnsupported      = "unsupported"
	KindMalformed        = "malformed"
)

var intBits = map[string]int{"int": 64, "int8": 8, "int16": 16, "int32": 32, "int64": 64}
var uintBits = map[string]int{"uint": 64, "uint8": 8, "uint16": 16, "uint32": 32, "uint64": 64}

// MapValue maps one v4 property value. kind is "" when the value maps; else
// the returned Value is the zero Value and kind names the ambiguity.
//
// The numeric rule is the v5 engine's: exact int64, finite float64, no
// coercion between them (Int(3) and Float(3) are different kinds that compare
// equal by Cypher equality). graphstate.Scalar sits in v5/internal and cannot
// be imported from tools/, so the public graph.Value, which mirrors it, is used.
func MapValue(t fixtures.Typed) (graph.Value, string) {
	text, _ := t.V.(string)
	switch {
	case t.T == "nil":
		return graph.Null(), ""
	case t.T == "string":
		return graph.String(text), ""
	case t.T == "bool":
		switch text {
		case "true":
			return graph.Bool(true), ""
		case "false":
			return graph.Bool(false), ""
		}
		return graph.Value{}, KindMalformed
	case intBits[t.T] != 0:
		n, err := strconv.ParseInt(text, 10, intBits[t.T])
		if err != nil {
			return graph.Value{}, KindMalformed
		}
		return graph.Int(n), ""
	case uintBits[t.T] != 0:
		n, err := strconv.ParseUint(text, 10, uintBits[t.T])
		if err != nil {
			return graph.Value{}, KindMalformed
		}
		if n > math.MaxInt64 {
			return graph.Value{}, KindUintOverMaxInt64
		}
		return graph.Int(int64(n)), ""
	case t.T == "float64" || t.T == "float32":
		bits := 64
		if t.T == "float32" {
			bits = 32
		}
		f, err := strconv.ParseFloat(text, bits)
		if err != nil {
			return graph.Value{}, KindMalformed
		}
		switch {
		case math.IsNaN(f):
			return graph.Value{}, KindNaN
		case math.IsInf(f, 0):
			return graph.Value{}, KindInf
		}
		v, err := graph.Float(f)
		if err != nil {
			return graph.Value{}, KindMalformed
		}
		return v, ""
	case strings.HasPrefix(t.T, "[]"):
		return graph.Value{}, KindList
	case t.T == "map":
		return graph.Value{}, KindMap
	case t.T == "bytes":
		return graph.Value{}, KindBytes
	case t.T == "time":
		return graph.Value{}, KindTime
	case strings.HasPrefix(t.T, "temporal"):
		return graph.Value{}, KindTemporal
	}
	return graph.Value{}, KindUnsupported
}
