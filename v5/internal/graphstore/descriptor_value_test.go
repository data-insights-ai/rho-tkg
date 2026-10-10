package graphstore

import (
	"bytes"
	"errors"
	"reflect"
	"testing"
	"unsafe"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func storeV1Descriptor(t testing.TB, id byte, payload []byte) graphstate.Scalar {
	t.Helper()
	d, err := temporal.PreserveOpaqueDescriptor(temporal.OpaqueDescriptorSpec{ID: temporal.DescriptorID{id}, Type: "shared-constraint", SchemaVersion: 3, Correlation: temporal.CorrelationID{9}, References: []temporal.DescriptorReference{{Role: "source", ID: temporal.DescriptorID{4}, Type: "observation", SchemaVersion: 2, Integrity: [32]byte{8}}}, Payload: payload}, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	s, err := graphstate.DescriptorValue(d)
	if err != nil {
		t.Fatal(err)
	}
	return s
}
func TestV1DescriptorPrimitiveCatalogAliasesCollisionAndRetainedBindings(t *testing.T) {
	db, root := newStore(t, vfs.NewMem())
	c := openCatalog(t, db, 1, Limits{})
	c.hash = func(string) [32]byte { return [32]byte{1} }
	s := stage(t, c)
	a, b := storeV1Descriptor(t, 1, []byte("source")), storeV1Descriptor(t, 2, []byte("source"))
	for i, value := range []graphstate.Scalar{a, b, a} {
		if err := s.Value(t.Context(), refValue(uint64(i+1)), value); err != nil {
			t.Fatal(err)
		}
	}
	root, index := commitStage(t, db, root, s)
	old := openCatalog(t, db, index, Limits{})
	old.hash = c.hash
	for _, tc := range []struct {
		value graphstate.Scalar
		id    graphstate.ValueID
	}{{a, 1}, {b, 2}} {
		entry, found, err := old.LookupLocalValueIdentity(t.Context(), tc.value)
		if err != nil || !found || entry.Ref.ID != tc.id {
			t.Fatal(entry, err)
		}
	}
	next := stage(t, old)
	if err := next.Value(t.Context(), refValue(1), b); !errors.Is(err, ErrRebinding) {
		t.Fatal(err)
	}
	rows, err := next.Writes()
	if err != nil || len(rows) != 0 {
		t.Fatal(rows, err)
	}
	for _, tc := range []struct{ bad graphstate.PropertyDefinition }{{graphstate.PropertyDefinition{Name: "d", Owner: graphstate.Node, Type: graphstate.ScalarDescriptor, Cardinality: graphstate.ScalarCardinality, Unique: graphstate.UniqueScalar}}, {graphstate.PropertyDefinition{Name: "d", Owner: graphstate.Node, Type: graphstate.ScalarDescriptor, Cardinality: graphstate.SetCardinality}}} {
		if err := next.Property(t.Context(), tc.bad); !errors.Is(err, graphstate.ErrUnsupported) || !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
	}
	if err := next.Value(t.Context(), refValue(4), storeV1Descriptor(t, 1, []byte("correction"))); err != nil {
		t.Fatal(err)
	}
	_, newIndex := commitStage(t, db, root, next)
	for _, readIndex := range []uint64{index, newIndex} {
		current := openCatalog(t, db, readIndex, Limits{})
		current.hash = c.hash
		value, found, err := current.Value(t.Context(), refValue(1))
		if err != nil || !found {
			t.Fatal(value, err)
		}
		equal, err := value.Value.Equal(a, graphstate.Limits{})
		if err != nil || !equal {
			t.Fatal(equal, err)
		}
	}
}
func TestV1DescriptorBackingPrechargesAllCatalogModesBeforeDecode(t *testing.T) {
	db, _ := newStore(t, vfs.NewMem())
	c := openCatalog(t, db, 1, Limits{})
	value := storeV1Descriptor(t, 1, []byte("payload"))
	key, err := value.EqualityKey(c.limits.valueLimits())
	if err != nil {
		t.Fatal(err)
	}
	wire, err := encodeValue(testNamespace(), refValue(1), value, key, 0, c.limits)
	if err != nil {
		t.Fatal(err)
	}
	required := descriptorOwnedBacking(len(key) - 1)
	for _, full := range []bool{false, true} {
		for _, delta := range []int{-1, 0, 1} {
			q := reader{c: c, ctx: t.Context(), bytes: c.rootImageBytes, maxBytes: c.rootImageBytes + required + delta}
			if full {
				q.fullView = new(fullIndexDescriptor)
			}
			before := bytes.Clone(wire)
			err := q.preflightFullValue(wire)
			if delta < 0 {
				if !errors.Is(err, ErrResourceLimit) || q.bytes != c.rootImageBytes {
					t.Fatal(q.bytes, err)
				}
			} else if err != nil || q.bytes != c.rootImageBytes+required {
				t.Fatal(q.bytes, err)
			}
			if !bytes.Equal(wire, before) {
				t.Fatal("preflight mutated delivered record")
			}
		}
	}
	if allocations := testing.AllocsPerRun(100, func() {
		q := reader{c: c, ctx: t.Context(), bytes: c.rootImageBytes, maxBytes: c.rootImageBytes + required - 1}
		if err := q.preflightFullValue(wire); !errors.Is(err, ErrResourceLimit) {
			t.Fatal(err)
		}
	}); allocations != 0 {
		t.Fatal("refusal allocated descriptor backing", allocations)
	}
	decoded, err := decodeScalar([]byte(key), temporal.Axis{}, false, c.limits)
	if err != nil {
		t.Fatal(err)
	}
	d, _ := decoded.Descriptor()
	spec := d.Spec()
	spec.Payload[0] = 'x'
	if !bytes.Equal(d.Spec().Payload, []byte("payload")) {
		t.Fatal("decoded ownership leaked")
	}
	for _, mutate := range []func([]byte){func(b []byte) { b[3] = 2 }, func(b []byte) { clear(b[21:25]) }, func(b []byte) { b[0] = 255 }} {
		bad := []byte(key)
		mutate(bad)
		if _, err := decodeScalar(bad, temporal.Axis{}, false, c.limits); !errors.Is(err, ErrCorrupt) {
			t.Fatal(err)
		}
	}
}
func TestV1DescriptorBackingDerivationCoversTinyReferencesAndWidePayload(t *testing.T) {
	for _, count := range []int{0, 1, 128, 2048} {
		spec := temporal.OpaqueDescriptorSpec{ID: temporal.DescriptorID{1}, Type: "x", SchemaVersion: 1, Payload: bytes.Repeat([]byte{9}, 128)}
		for range count {
			spec.References = append(spec.References, temporal.DescriptorReference{Role: "r", Type: "t", ID: temporal.DescriptorID{1}, SchemaVersion: 1})
		}
		l := temporal.Limits{MaxInputBytes: 256 << 10, MaxValueBytes: 256 << 10, MaxDescriptorBytes: 256 << 10}
		d, err := temporal.PreserveOpaqueDescriptor(spec, l)
		if err != nil {
			t.Fatal(err)
		}
		wire, err := temporal.AppendOpaqueDescriptor(nil, d, l)
		if err != nil {
			t.Fatal(err)
		}
		// Decode owns one child. Conservatively include two independent copies of
		// its Spec/ref-array/string/payload backing plus two wire buffers and Scalar
		// pointer/header. Spec is called only in this independent post-admission test.
		variable := len(spec.Type) + len(spec.Payload)
		for _, r := range spec.References {
			variable += len(r.Role) + len(r.Type)
		}
		one := int(unsafe.Sizeof(temporal.OpaqueDescriptor{})) + int(unsafe.Sizeof(temporal.OpaqueDescriptorSpec{})) + count*int(unsafe.Sizeof(temporal.DescriptorReference{})) + variable
		simultaneous := 2*one + 2*len(wire) + int(unsafe.Sizeof(graphstate.Scalar{})) + int(unsafe.Sizeof(new(temporal.OpaqueDescriptor)))
		if simultaneous > descriptorOwnedBacking(len(wire)) {
			t.Fatal("descriptor allowance undercounts declared live set", count, simultaneous, descriptorOwnedBacking(len(wire)))
		}
		t.Logf("references=%d referenceSlot=%d header=%d liveAllowance=%d reservation=%d", count, unsafe.Sizeof(temporal.DescriptorReference{}), unsafe.Sizeof(temporal.OpaqueDescriptorSpec{}), simultaneous, descriptorOwnedBacking(len(wire)))
	}
}
func TestV1DescriptorFullUniquenessRefusalIsNotEnvelopeSemantics(t *testing.T) {
	f, _ := v1DeclaredStore(t)
	v := f.view(t, f.index)
	defer v.Close()
	query := graphstate.UniquePredicate{Definition: graphstate.PropertyDefinition{Name: "evidence", Owner: graphstate.Node, Type: graphstate.ScalarDescriptor, Cardinality: graphstate.ScalarCardinality, Unique: graphstate.UniqueScalar}, Value: storeV1Descriptor(t, 1, nil), Window: f.span(t, 0, 10)}
	out, err := v.UniqueCandidates(t.Context(), query, 0, graphstate.ReadBudget{Rows: 16, Bytes: 1 << 20})
	if !errors.Is(err, graphstate.ErrUnsupported) || !reflect.DeepEqual(out, graphstate.ClaimPage{}) {
		t.Fatal(out, err)
	}
	if err := v.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := v.UniqueCandidates(t.Context(), query, 0, graphstate.ReadBudget{Rows: 16, Bytes: 1 << 20}); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}
