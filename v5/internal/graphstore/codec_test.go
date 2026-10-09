package graphstore

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func TestCatalogCanonicalRecordBoundaries(t *testing.T) {
	l, _ := (Limits{}).resolve()
	n := testNamespace()
	a := testAxis(t, 2, temporal.ProfileIntegerZ)
	axisBytes, _ := encodeAxis(n, a, l)
	propertyBytes, _ := encodeProperty(n, graphstate.PropertyDefinition{Name: "p", Owner: graphstate.Node, Type: graphstate.ScalarString, Cardinality: graphstate.ScalarCardinality}, l)
	entityBytes, _ := encodeEntity(n, graphstate.EntityRecord{ID: 1, Kind: graphstate.Node, Axis: a}, l)
	lifeBytes, _ := encodeLife(n, graphstate.LifeRecord{Owner: 1, Life: 1}, l)
	cases := []struct {
		data   []byte
		decode func([]byte) error
	}{{axisBytes, func(b []byte) error { _, e := readAxis(b, n, l); return e }}, {propertyBytes, func(b []byte) error { _, e := readProperty(b, n, l); return e }}, {entityBytes, func(b []byte) error { _, e := readEntity(b, n, l); return e }}, {lifeBytes, func(b []byte) error { _, e := readLife(b, n, l); return e }}}
	for _, tc := range cases {
		for i := range len(tc.data) {
			if err := tc.decode(tc.data[:i]); !errors.Is(err, ErrCorrupt) {
				t.Fatal(i, err)
			}
		}
		if err := tc.decode(append(bytes.Clone(tc.data), 0)); !errors.Is(err, ErrCorrupt) {
			t.Fatal(err)
		}
		b := bytes.Clone(tc.data)
		b[2] = 2
		if err := tc.decode(b); !errors.Is(err, ErrCorrupt) {
			t.Fatal(err)
		}
	}
	for _, v := range []graphstate.Scalar{graphstate.Null(), graphstate.String("\x00exact\xff"), graphstate.Bool(false), graphstate.Bool(true), graphstate.I64(math.MinInt64), graphstate.I64(math.MaxInt64)} {
		key, err := v.EqualityKey(l.valueLimits())
		if err != nil {
			t.Fatal(err)
		}
		wire, err := encodeValue(n, refValue(1), v, key, 0, l)
		if err != nil {
			t.Fatal(err)
		}
		entry, stored, ordinal, err := readValue(wire, n, l)
		if err != nil || ordinal != 0 || !bytes.Equal(stored, []byte(key)) {
			t.Fatal(entry, ordinal, err)
		}
		equal, err := entry.Value.Equal(v, l.valueLimits())
		if err != nil || !equal {
			t.Fatal(err)
		}
		for i := range len(wire) {
			if _, _, _, err := readValue(wire[:i], n, l); !errors.Is(err, ErrCorrupt) {
				t.Fatal(i, err)
			}
		}
	}
	for _, bits := range []uint64{1 << 63, math.Float64bits(math.NaN()), math.Float64bits(math.Inf(1))} {
		key := binary.BigEndian.AppendUint64([]byte{byte(graphstate.ScalarF64)}, bits)
		if _, err := decodeScalar(key, temporal.Axis{}, false, l); !errors.Is(err, ErrCorrupt) {
			t.Fatal(err)
		}
	}
	for _, key := range [][]byte{{byte(graphstate.ScalarNull), 0}, {byte(graphstate.ScalarBool), 2}, {byte(graphstate.ScalarI64), 0}, {255}, {byte(graphstate.ScalarScope)}} {
		if _, err := decodeScalar(key, temporal.Axis{}, false, l); !errors.Is(err, ErrCorrupt) {
			t.Fatal(key, err)
		}
	}
	for _, r := range []graphstate.EntityRecord{{}, {ID: 1, Kind: graphstate.Node, Axis: a, Source: 2}, {ID: 1, Kind: graphstate.Relationship, Axis: a, Type: " ", Source: 1, Target: 2, Mode: graphstate.IdentityReference}} {
		if _, err := encodeEntity(n, r, l); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
	}
	for _, d := range []graphstate.PropertyDefinition{{}, {Name: "p", Owner: graphstate.Node, Type: graphstate.ScalarNull, Cardinality: graphstate.ScalarCardinality}, {Name: "p", Owner: graphstate.Node, Type: graphstate.ScalarBool, Cardinality: graphstate.ScalarCardinality, Unique: graphstate.UniqueMembers}} {
		if _, err := encodeProperty(n, d, l); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
	}
	if _, err := encodeLife(n, graphstate.LifeRecord{}, l); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := readNumber(append(encodeNumber(n, bucketRecord, 1), 0), n, bucketRecord, l); !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
}

func TestCanonicalKeyNamespacesAndOwnerKinds(t *testing.T) {
	n := testNamespace()
	other := n
	other.Partition++
	foreign := n
	foreign.Graph[0]++
	keys := [][]byte{axisKey(n, temporal.AxisID{2}), schemaKey(n, graphstate.Node, "same"), entityKey(n, 1), lifeKey(n, 1, 1), valueKey(n, 1), bucketKey(n, [32]byte{1}), memberKey(n, [32]byte{1}, 0)}
	seen := make(map[string]bool)
	for _, key := range keys {
		if seen[string(key)] {
			t.Fatal("namespace alias")
		}
		seen[string(key)] = true
	}
	if bytes.Equal(schemaKey(n, graphstate.Node, "same"), schemaKey(n, graphstate.Relationship, "same")) || bytes.Equal(entityKey(n, 1), entityKey(other, 1)) || bytes.Equal(valueKey(n, 1), valueKey(foreign, 1)) {
		t.Fatal("qualification erased")
	}
}

func FuzzCatalogDecoders(f *testing.F) {
	l, _ := (Limits{}).resolve()
	n := testNamespace()
	r, _ := NewRoot(n, 3)
	root, _ := EncodeRoot(r)
	a, _ := temporal.NewAxis(temporal.AxisDescriptor{ID: temporal.AxisID{2}, Profile: temporal.ProfileRationalQ, Version: 1, Reference: "clock@v1", CanonicalUnit: "unit"}, l.Temporal)
	axis, _ := encodeAxis(n, a, l)
	property, _ := encodeProperty(n, graphstate.PropertyDefinition{Name: "p", Owner: graphstate.Relationship, Type: graphstate.ScalarScope, Cardinality: graphstate.SetCardinality}, l)
	entity, _ := encodeEntity(n, graphstate.EntityRecord{ID: 1, Kind: graphstate.Node, Axis: a}, l)
	life, _ := encodeLife(n, graphstate.LifeRecord{Owner: 1, Life: 2}, l)
	fraction, _ := temporal.Fraction(temporal.Int64(1), temporal.Int64(3), l.Temporal)
	position, _ := temporal.RationalPosition(a, fraction)
	point, _ := temporal.Point(position)
	finite, _ := graphstate.F64(1.25)
	seeds := [][]byte{nil, []byte("GR\x01\x01"), root, axis, property, entity, life, encodeNumber(n, bucketRecord, 2), encodeNumber(n, bucketMemberRecord, 10)}
	for _, v := range []graphstate.Scalar{graphstate.ScopeValue(point), finite, graphstate.String("exact\x00bytes"), graphstate.Bool(true), graphstate.I64(-7), graphstate.Null()} {
		key, _ := v.EqualityKey(l.valueLimits())
		wire, _ := encodeValue(n, refValue(10), v, key, 0, l)
		seeds = append(seeds, wire)
	}
	for _, seed := range seeds {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, input []byte) {
		if len(input) > l.MaxRecordBytes {
			return
		}
		if r, err := DecodeRoot(input); err == nil {
			encoded, e := EncodeRoot(r)
			if e != nil || !bytes.Equal(encoded, input) {
				t.Fatal("root not canonical", e)
			}
		}
		if a, err := readAxis(input, n, l); err == nil {
			encoded, e := encodeAxis(n, a, l)
			if e != nil || !bytes.Equal(encoded, input) {
				t.Fatal("axis not canonical", e)
			}
		}
		if d, err := readProperty(input, n, l); err == nil {
			encoded, e := encodeProperty(n, d, l)
			if e != nil || !bytes.Equal(encoded, input) {
				t.Fatal("schema not canonical", e)
			}
		}
		if r, err := readEntity(input, n, l); err == nil {
			encoded, e := encodeEntity(n, r, l)
			if e != nil || !bytes.Equal(encoded, input) {
				t.Fatal("entity not canonical", e)
			}
		}
		if r, err := readLife(input, n, l); err == nil {
			encoded, e := encodeLife(n, r, l)
			if e != nil || !bytes.Equal(encoded, input) {
				t.Fatal("life not canonical", e)
			}
		}
		if entry, key, ordinal, err := readValue(input, n, l); err == nil {
			canonical, e := entry.Value.EqualityKey(l.valueLimits())
			if e != nil || !bytes.Equal(key, []byte(canonical)) {
				t.Fatal("value equality bytes changed", e)
			}
			encoded, e := encodeValue(n, entry.Ref, entry.Value, canonical, ordinal, l)
			if e != nil || !bytes.Equal(encoded, input) {
				t.Fatal("value not canonical", e)
			}
		}
		for _, kind := range []recordKind{bucketRecord, bucketMemberRecord} {
			if count, err := readNumber(input, n, kind, l); err == nil && !bytes.Equal(encodeNumber(n, kind, count), input) {
				t.Fatal("counter not canonical")
			}
		}
	})
}
