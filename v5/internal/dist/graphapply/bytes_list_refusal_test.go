package graphapply

import (
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
)

// The op-log codec fails closed on Bytes and List (carrying them is WP-28):
// writing a value or a schema of those kinds is ErrUnsupported and emits nothing;
// a wire holding one is corrupt on read.
func TestOpLogCodecRefusesBytesAndListKinds(t *testing.T) {
	l := defaultMaterializerLimits()
	list, err := graphstate.List([]graphstate.Scalar{graphstate.I64(1)})
	if err != nil {
		t.Fatal(err)
	}
	for name, v := range map[string]graphstate.Scalar{"bytes": graphstate.Bytes([]byte{1}), "empty bytes": graphstate.Bytes(nil), "list": list} {
		w := boundedWriter{max: 1 << 20, sizing: true}
		writeScalar(&w, v, nil, l)
		if !errors.Is(w.err, graphstate.ErrUnsupported) || w.n != 0 {
			t.Errorf("%s value: err %v n %d", name, w.err, w.n)
		}
		w = boundedWriter{max: 1 << 20, sizing: true}
		writeSchema(&w, graphstate.PropertyDefinition{Name: "p", Owner: graphstate.Node, Type: v.Kind(), Cardinality: graphstate.ScalarCardinality})
		if !errors.Is(w.err, graphstate.ErrUnsupported) || w.n != 0 {
			t.Errorf("%s schema: err %v n %d", name, w.err, w.n)
		}
		changes := graphChanges{ns: codecNamespace(), values: []graphstate.ValueWrite{{ID: 1, Value: v}}}
		if wire, err := encodeGraphChanges(changes, l); !errors.Is(err, graphstate.ErrUnsupported) || len(wire) != 0 {
			t.Errorf("%s changes: %x %v", name, wire, err)
		}
	}
	if validSchema(graphstate.PropertyDefinition{Name: "p", Owner: graphstate.Node, Type: graphstate.ScalarBytes, Cardinality: graphstate.ScalarCardinality}, l) {
		t.Error("bytes schema is valid on read")
	}
}

func TestOpLogCodecReadRefusesBytesAndListWire(t *testing.T) {
	for _, k := range []graphstate.ScalarKind{graphstate.ScalarBytes, graphstate.ScalarList} {
		if !oplogRefusesKind(k) {
			t.Errorf("kind %d not refused", k)
		}
	}
	for _, k := range []graphstate.ScalarKind{graphstate.ScalarString, graphstate.ScalarI64, graphstate.ScalarDescriptor, graphstate.ScalarScope} {
		if oplogRefusesKind(k) {
			t.Errorf("kind %d wrongly refused", k)
		}
	}
}
