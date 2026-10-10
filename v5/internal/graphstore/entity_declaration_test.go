package graphstore

import (
	"bytes"
	"encoding/hex"
	"errors"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func TestV1EntityDeclarationsUseOnlyVersionedEntityPayload(t *testing.T) {
	l, _ := (Limits{}).resolve()
	n := testNamespace()
	axis := testAxis(t, 2, temporal.ProfileIntegerZ)
	r := graphstate.EntityRecord{ID: 1, Kind: graphstate.Node, Axis: axis}
	old, err := encodeEntity(n, r, l)
	if err != nil {
		t.Fatal(err)
	}
	// Independent literal from the retained GC1 contract; never candidate-derived.
	const gc1Golden = "47430103010000000000000000000000000000000000000000000007000000000000000101000000000000000000000000000000000000000000020000000000000000000000000000000100010000000f736f757263652f636c6f636b4076310000000a65786163742d756e69740d98c22da575711f190a8effa5c209dda51c9c67d68c7eb3fffec907fb8b2f3e"
	golden, err := hex.DecodeString(gc1Golden)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(old, golden) {
		t.Fatal("legacy entity bytes changed", old, golden)
	}
	r.Interpretation = graphstate.InterpretationObservation
	r.TemporalRole = graphstate.TemporalRoleSourceOccurrence
	wire, err := encodeEntity(n, r, l)
	if err != nil || len(wire) != len(old)+2 || wire[2] != 2 {
		t.Fatal(wire, err)
	}
	expected := bytes.Clone(old)
	expected[2] = 2
	expected = append(expected, byte(r.Interpretation), byte(r.TemporalRole))
	if !bytes.Equal(wire, expected) {
		t.Fatal("GC2 changed legacy fields", wire, expected)
	}
	got, err := readEntity(wire, n, l)
	if err != nil || got != r {
		t.Fatal(got, err)
	}
	clear(wire)
	if got != r {
		t.Fatal("declarations alias wire")
	}
	for _, tc := range []struct {
		kind   recordKind
		encode func() []byte
		decode func([]byte) error
	}{
		{axisRecord, func() []byte { b, _ := encodeAxis(n, axis, l); return b }, func(b []byte) error { _, e := readAxis(b, n, l); return e }},
		{schemaRecord, func() []byte {
			b, _ := encodeProperty(n, graphstate.PropertyDefinition{Name: "d", Owner: graphstate.Node, Type: graphstate.ScalarString, Cardinality: graphstate.ScalarCardinality}, l)
			return b
		}, func(b []byte) error { _, e := readProperty(b, n, l); return e }},
		{lifeRecord, func() []byte { b, _ := encodeLife(n, graphstate.LifeRecord{Owner: 1, Life: 1}, l); return b }, func(b []byte) error { _, e := readLife(b, n, l); return e }},
		{valueRecord, func() []byte {
			v := graphstate.String("x")
			key, _ := v.EqualityKey(l.valueLimits())
			b, _ := encodeValue(n, refValue(1), v, key, 0, l)
			return b
		}, func(b []byte) error { _, _, _, e := readValue(b, n, l); return e }},
	} {
		b := tc.encode()
		b[2] = 2
		if err := tc.decode(b); !errors.Is(err, ErrCorrupt) {
			t.Fatal("GC2 escaped entity family", tc.kind, err)
		}
	}
	wire, _ = encodeEntity(n, r, l)
	for _, mutate := range []func([]byte){func(b []byte) { b[2] = 3 }, func(b []byte) { b[len(b)-2] = 255 }, func(b []byte) { clear(b[len(b)-2:]) }, func(b []byte) { b[4] = 9 }} {
		b := bytes.Clone(wire)
		mutate(b)
		if _, err := readEntity(b, n, l); !errors.Is(err, ErrCorrupt) {
			t.Fatal("noncanonical declaration record", err)
		}
	}
	for i := range len(wire) {
		if _, err := readEntity(wire[:i], n, l); !errors.Is(err, ErrCorrupt) {
			t.Fatal(i, err)
		}
	}
	if _, err := readEntity(append(bytes.Clone(wire), 0), n, l); !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
}

func v1DeclaredStore(t *testing.T) (*fullFixture, raftlog.Config) {
	t.Helper()
	cfg := raftlog.Config{Dir: t.TempDir(), FS: vfs.Default, Create: true, Application: raftlog.DefaultApplicationPolicy(1)}
	s, err := raftlog.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := BootstrapSinglePartition(s, testNamespace(), 3); err != nil {
		t.Fatal(err)
	}
	_, image, err := s.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	root, err := DecodeRoot(image)
	if err != nil {
		t.Fatal(err)
	}
	f := &fullFixture{db: s, root: root, index: 1, axis: testAxis(t, 1, temporal.ProfileIntegerZ), limits: GraphLimits{}}
	c := f.catalog(t, 1)
	e, err := InitializeGraphIndexes(t.Context(), c, []graphstate.PropertyDefinition{{Name: "evidence", Owner: graphstate.Node, Type: graphstate.ScalarDescriptor, Cardinality: graphstate.ScalarCardinality}, {Name: "evidence", Owner: graphstate.Relationship, Type: graphstate.ScalarDescriptor, Cardinality: graphstate.ScalarCardinality}}, GraphLimits{})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.view.Close(); err != nil {
		t.Fatal(err)
	}
	f.install(t, e)
	return f, cfg
}
func TestV1EntityAndDescriptorActualGraphHistoryReopensExactly(t *testing.T) {
	f, cfg := v1DeclaredStore(t)
	whole, middle := f.span(t, 0, 10), f.span(t, 3, 7)
	first := storeV1Descriptor(t, 1, []byte("shared-x:source-v1"))
	second := storeV1Descriptor(t, 1, []byte("shared-x:source-v2"))
	effects := f.apply(t, graphstate.Operation{Kind: graphstate.CreateNode, Owner: 1, Life: 1, Scope: whole, Record: graphstate.EntityRecord{Interpretation: graphstate.InterpretationObservation, TemporalRole: graphstate.TemporalRoleSourceOccurrence}}, graphstate.Operation{Kind: graphstate.CreateNode, Owner: 2, Life: 1, Scope: whole}, graphstate.Operation{Kind: graphstate.CreateRelationship, Owner: 3, Life: 1, Scope: whole, Record: graphstate.EntityRecord{Type: "PRECEDES", Source: 1, Target: 2, Mode: graphstate.IdentityReference, Interpretation: graphstate.InterpretationAssertedRelation, TemporalRole: graphstate.TemporalRoleCausalOrder}}, graphstate.Operation{Kind: graphstate.Set, Owner: 1, Life: 1, Scope: whole, Name: "evidence", Value: first, ValueID: 10}, graphstate.Operation{Kind: graphstate.Set, Owner: 3, Life: 1, Scope: whole, Name: "evidence", Value: first, ValueID: 11})
	oldIndex := f.index
	if len(effects.Delta.Values) != 1 || effects.Delta.Values[0].ID != 10 {
		t.Fatal("exact envelope identity was not canonical", effects.Delta.Values)
	}
	f.apply(t, graphstate.Operation{Kind: graphstate.Set, Owner: 1, Life: 1, Scope: middle, Name: "evidence", Value: second, ValueID: 12}, graphstate.Operation{Kind: graphstate.Close, Owner: 1, Life: 1, Scope: f.span(t, 8, 10)})
	nowIndex := f.index
	assert := func() {
		t.Helper()
		for _, tc := range []struct {
			index   uint64
			owner   graphstate.EntityID
			at      int64
			active  bool
			payload string
		}{{oldIndex, 1, 5, true, "shared-x:source-v1"}, {nowIndex, 1, 5, true, "shared-x:source-v2"}, {oldIndex, 1, 9, true, "shared-x:source-v1"}, {nowIndex, 1, 9, false, ""}, {nowIndex, 3, 9, true, "shared-x:source-v1"}} {
			p := f.projection(t, tc.index, tc.owner, tc.at, graphstate.Effective)
			if !p.Exists || p.Active != tc.active {
				t.Fatal(p, tc)
			}
			if tc.owner == 1 && (p.Record.Interpretation != graphstate.InterpretationObservation || p.Record.TemporalRole != graphstate.TemporalRoleSourceOccurrence) {
				t.Fatal(p)
			}
			if tc.owner == 3 && (p.Record.Interpretation != graphstate.InterpretationAssertedRelation || p.Record.TemporalRole != graphstate.TemporalRoleCausalOrder) {
				t.Fatal(p)
			}
			if tc.active {
				if len(p.Properties) != 1 {
					t.Fatal(p)
				}
				d, ok := p.Properties[0].Scalar.Descriptor()
				if !ok || string(d.Spec().Payload) != tc.payload {
					t.Fatal(p, tc)
				}
			}
		}
	}
	assert()
	if err := f.db.Close(); err != nil {
		t.Fatal(err)
	}
	cfg.Create = false
	s, err := raftlog.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	f.db = s
	assert()
}
