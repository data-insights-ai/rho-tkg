package graphapply

import (
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func TestV1OlderWireDoorsRefuseNewMetadataWithoutErasure(t *testing.T) {
	l := defaultMaterializerLimits()
	r := graphCodecRequest(t)
	r.operations[0].Record.Interpretation = graphstate.InterpretationOccurrence
	if wire, err := encodeGraphRequest(r, l); !errors.Is(err, graphstate.ErrUnsupported) || len(wire) != 0 {
		t.Fatal(wire, err)
	}
	for _, prior := range []error{nil, errLimit} {
		w := boundedWriter{max: 1 << 20, sizing: true, err: prior}
		writeEntity(&w, r.operations[0].Record, nil)
		want := graphstate.ErrUnsupported
		if prior != nil {
			want = prior
		}
		if !errors.Is(w.err, want) {
			t.Fatal(w.err)
		}
	}
	d, err := temporal.PreserveOpaqueDescriptor(temporal.OpaqueDescriptorSpec{ID: temporal.DescriptorID{1}, Type: "source", SchemaVersion: 1, Payload: []byte("preserved")}, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	value, err := graphstate.DescriptorValue(d)
	if err != nil {
		t.Fatal(err)
	}
	for _, emit := range []func(*boundedWriter){func(w *boundedWriter) { writeScalar(w, value, nil, l) }, func(w *boundedWriter) {
		writeSchema(w, graphstate.PropertyDefinition{Name: "d", Owner: graphstate.Node, Type: graphstate.ScalarDescriptor, Cardinality: graphstate.ScalarCardinality})
	}} {
		for _, prior := range []error{nil, errLimit} {
			w := boundedWriter{max: 1 << 20, sizing: true, err: prior}
			emit(&w)
			want := graphstate.ErrUnsupported
			if prior != nil {
				want = prior
			}
			if !errors.Is(w.err, want) || w.n != 0 {
				t.Fatal(w, prior)
			}
		}
	}
	changes := graphChanges{ns: codecNamespace(), entities: []graphstate.EntityRecord{{ID: 1, Kind: graphstate.Node, Axis: r.operations[0].Scope.Axis(), Interpretation: graphstate.InterpretationOccurrence}}}
	if wire, err := encodeGraphChanges(changes, l); !errors.Is(err, graphstate.ErrUnsupported) || len(wire) != 0 {
		t.Fatal(wire, err)
	}
	changes = graphChanges{ns: codecNamespace(), values: []graphstate.ValueWrite{{ID: 1, Value: value}}}
	if wire, err := encodeGraphChanges(changes, l); !errors.Is(err, graphstate.ErrUnsupported) || len(wire) != 0 {
		t.Fatal(wire, err)
	}
}

func TestV1ActualLegacyInitializationRefusesDescriptorSchema(t *testing.T) {
	r := graphInit(t)
	r.schemas = []graphstate.PropertyDefinition{{Name: "source", Owner: graphstate.Node, Type: graphstate.ScalarDescriptor, Cardinality: graphstate.ScalarCardinality}}
	wire, err := encodeGraphRequest(r, defaultMaterializerLimits())
	if !errors.Is(err, errInvalid) || !errors.Is(err, graphstate.ErrUnsupported) || len(wire) != 0 {
		t.Fatal(wire, err)
	}
}
