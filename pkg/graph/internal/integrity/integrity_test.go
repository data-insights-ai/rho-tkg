// Package integrity tests cover ComputeNodeHash, ComputeRelHash, and
// appendPropertyValue at the CALL level (property values round-tripping
// through the hash). appendPropertyValue itself is a one-line forward to
// pkg/types.appendPropertyValueHashBytes — the REAL per-type-tag property-
// value switch lives there, and its own branch coverage lives in
// pkg/types/property_hash_test.go, not here. The tests in this package
// intentionally pin the exact byte layout via fixed-vector SHA-256 anchors:
// any change to the hash bytes invalidates every persisted tkg_hash /
// tkg_prev_hash, so silent algorithm drift must trip a test.
package integrity

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// --- helpers ---

func TestPutHashBufferKeepsReusableSmallBuffers(t *testing.T) {
	buf := make([]byte, 17, initialHashBufferCap*2)
	bp := &[]byte{}

	putHashBuffer(bp, buf)

	if len(*bp) != 0 {
		t.Fatalf("pooled buffer len = %d, want 0", len(*bp))
	}
	if cap(*bp) != cap(buf) {
		t.Fatalf("pooled buffer cap = %d, want original cap %d", cap(*bp), cap(buf))
	}
}

func TestPutHashBufferDropsOversizedBuffers(t *testing.T) {
	buf := make([]byte, 1, maxPooledHashBufferCap+1)
	bp := &[]byte{}

	putHashBuffer(bp, buf)

	if len(*bp) != 0 {
		t.Fatalf("pooled buffer len = %d, want 0", len(*bp))
	}
	if cap(*bp) != initialHashBufferCap {
		t.Fatalf("pooled buffer cap = %d, want reset cap %d", cap(*bp), initialHashBufferCap)
	}
}

// newNodeForHash builds a *types.Node with a fixed snowflake id, a single
// primary label token, and the given properties (already-validated map).
// Properties are wired in via NewPropertySlice to mirror the AddNode path.
func newNodeForHash(t *testing.T, id uint64, version uint32, props map[string]any) *types.Node {
	t.Helper()
	n := types.NewNode(types.NodeID(snowflake.ID(id)), 1, nil)
	n.SetVersion(version)
	if len(props) > 0 {
		ps, err := types.NewPropertySlice(props)
		if err != nil {
			t.Fatalf("NewPropertySlice: %v", err)
		}
		n.SetProperties(ps)
	}
	return n
}

// newRelForHash builds a *types.Relationship analogue of newNodeForHash for
// test parity (Node and Relationship are structural mirrors).
func newRelForHash(t *testing.T, id, startID, endID uint64, version uint32, props map[string]any) *types.Relationship {
	t.Helper()
	r := types.NewRelationship(
		types.RelID(snowflake.ID(id)),
		1,
		types.NodeID(snowflake.ID(startID)),
		types.NodeID(snowflake.ID(endID)),
	)
	r.SetVersion(version)
	if len(props) > 0 {
		ps, err := types.NewPropertySlice(props)
		if err != nil {
			t.Fatalf("NewPropertySlice: %v", err)
		}
		r.SetProperties(ps)
	}
	return r
}

// hashOnce computes ComputeNodeHash twice and asserts identical output.
// Determinism check used by every property-type test.
func hashOnce(t *testing.T, n *types.Node, labels []string) string {
	t.Helper()
	h1 := ComputeNodeHash(n, labels)
	h2 := ComputeNodeHash(n, labels)
	if h1 != h2 {
		t.Fatalf("ComputeNodeHash not deterministic: %q vs %q", h1, h2)
	}
	if len(h1) != 64 {
		t.Fatalf("hash length: got %d want 64", len(h1))
	}
	return h1
}

// hashRelOnce mirrors hashOnce for relationships.
func hashRelOnce(t *testing.T, r *types.Relationship, typeName string) string {
	t.Helper()
	h1 := ComputeRelHash(r, typeName)
	h2 := ComputeRelHash(r, typeName)
	if h1 != h2 {
		t.Fatalf("ComputeRelHash not deterministic: %q vs %q", h1, h2)
	}
	if len(h1) != 64 {
		t.Fatalf("hash length: got %d want 64", len(h1))
	}
	return h1
}

func requirePanicIs(t *testing.T, want error, fn func()) {
	t.Helper()
	defer func() {
		r := recover()
		if r == nil {
			t.Fatalf("expected panic matching %v", want)
		}
		err, ok := r.(error)
		if !ok {
			t.Fatalf("panic value = %T %[1]v, want error", r)
		}
		if !errors.Is(err, want) {
			t.Fatalf("panic error = %v, want errors.Is %v", err, want)
		}
	}()
	fn()
}

// hashPropOnly hashes a single property in isolation by manually constructing
// the buffer prefix (id=1, version=0, no labels) so the resulting digest
// reflects only the property serialization. Used to assert that two values
// of the same property type collide / differ at the property layer.
func hashPropOnly(t *testing.T, key string, value any) string {
	t.Helper()
	n := newNodeForHash(t, 1, 0, map[string]any{key: value})
	return ComputeNodeHash(n, nil)
}

// --- fixed-vector anchors (computed offline, see test commit message) ---
//
// These nail the exact byte sequence emitted by ComputeNodeHash /
// ComputeRelHash. The hex digests were derived by feeding the same buffer
// layout (big-endian id, big-endian version, sorted labels, properties with
// PropertyTypeTag prefix) into crypto/sha256 directly. Any change to the
// hash function's byte layout will fail these tests immediately.

const (
	anchorEmptyNodeID1V0     = "249df6debaad7a2916207fb7f0563ec678fb776144049f157259afadda1dc127"
	anchorNodeID42V1LabelL   = "33cbdc2d7e795f7a6a66513e876c3a7a34ea1bd05f91f2f520d40116aa6c0af7"
	anchorNodeID7V0LBoolTrue = "75a88f786bf55e93fbba765040c81f908a4e434ac7c67800273b1ae624c8b599"
	anchorRelID10V0KNOWS12   = "c210a927c06aa9c640f74b8d5db8e3610571c9483bac72c09b7ecf9ae3c8afdc"
	anchorRelID11V2Score15   = "f7c0ba33a0770b443088e8e9782dd88d44f3a13f1d8e6a16f3f3275de5d14b62"
	anchorAllPrimitives      = "083b8965c025799f668a980c6b7e2578fb771f3cf4b17e77e5a5b16f26cc2c83"
)

// TestComputeNodeHash_FixedVectorEmpty pins the digest for the simplest
// possible node (id=1, version=0, no labels, no properties).
func TestComputeNodeHash_FixedVectorEmpty(t *testing.T) {
	n := newNodeForHash(t, 1, 0, nil)
	got := hashOnce(t, n, nil)
	if got != anchorEmptyNodeID1V0 {
		t.Fatalf("hash drift: got %q want %q (CHANGES TO BYTE LAYOUT BREAK ALL PERSISTED tkg_hash VALUES)", got, anchorEmptyNodeID1V0)
	}
}

// TestComputeNodeHash_FixedVectorWithLabel pins the digest for a node with
// a single label (id=42, v=1, label="L"). Locks the label encoding shape:
// uint32 length prefix + UTF-8 bytes.
func TestComputeNodeHash_FixedVectorWithLabel(t *testing.T) {
	n := types.NewNode(types.NodeID(snowflake.ID(42)), 1, nil)
	n.SetVersion(1)
	got := hashOnce(t, n, []string{"L"})
	if got != anchorNodeID42V1LabelL {
		t.Fatalf("hash drift: got %q want %q", got, anchorNodeID42V1LabelL)
	}
}

// TestComputeNodeHash_FixedVectorBoolProp pins the digest for a node with a
// single bool property. Locks: ptBool tag (=1), bool encoding (1 byte),
// property key encoding (uint32 len + UTF-8 bytes).
func TestComputeNodeHash_FixedVectorBoolProp(t *testing.T) {
	n := types.NewNode(types.NodeID(snowflake.ID(7)), 1, nil)
	n.SetVersion(0)
	if err := n.SetProperty("active", true); err != nil {
		t.Fatalf("SetProperty: %v", err)
	}
	got := hashOnce(t, n, []string{"P"})
	if got != anchorNodeID7V0LBoolTrue {
		t.Fatalf("hash drift: got %q want %q", got, anchorNodeID7V0LBoolTrue)
	}
}

func TestComputeNodeHashFromSuffixMatchesComputeNodeHash(t *testing.T) {
	n := newNodeForHash(t, 99, 3, map[string]any{
		"name": "Alice",
		"age":  int64(42),
	})
	labels := []string{"Person", "Actor"}
	props := n.Properties()
	suffix, err := PrecomputeNodeHashSuffixChecked(labels, props)
	if err != nil {
		t.Fatalf("PrecomputeNodeHashSuffixChecked: %v", err)
	}
	got := ComputeNodeHashFromSuffix(n.ID(), n.Version(), suffix)
	want := ComputeNodeHash(n, labels)
	if got != want {
		t.Fatalf("suffix hash = %q, want ComputeNodeHash %q", got, want)
	}
}

// TestComputeRelHash_FixedVectorEmpty pins the rel digest for KNOWS, no props.
func TestComputeRelHash_FixedVectorEmpty(t *testing.T) {
	r := types.NewRelationship(
		types.RelID(snowflake.ID(10)), 1,
		types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(2)),
	)
	r.SetVersion(0)
	got := hashRelOnce(t, r, "KNOWS")
	if got != anchorRelID10V0KNOWS12 {
		t.Fatalf("hash drift: got %q want %q", got, anchorRelID10V0KNOWS12)
	}
}

// TestComputeRelHash_FixedVectorWithFloat64 pins the rel digest for KNOWS
// with a float64 property. Locks: rel id, version, type, endpoints, and
// ptFloat64 (=13) tag with IEEE-754 big-endian encoding.
func TestComputeRelHash_FixedVectorWithFloat64(t *testing.T) {
	r := types.NewRelationship(
		types.RelID(snowflake.ID(11)), 1,
		types.NodeID(snowflake.ID(3)), types.NodeID(snowflake.ID(4)),
	)
	r.SetVersion(2)
	if err := r.SetProperty("score", 1.5); err != nil {
		t.Fatalf("SetProperty: %v", err)
	}
	got := hashRelOnce(t, r, "KNOWS")
	if got != anchorRelID11V2Score15 {
		t.Fatalf("hash drift: got %q want %q", got, anchorRelID11V2Score15)
	}
}

// --- determinism + sensitivity smoke tests ---

// TestComputeNodeHash_DeterministicAcrossInvocations runs the hash four
// times back-to-back and confirms all match. Detects accidental nondeterminism
// from map iteration order or unsorted slices.
func TestComputeNodeHash_DeterministicAcrossInvocations(t *testing.T) {
	n := newNodeForHash(t, 100, 3, map[string]any{
		"a": int64(1),
		"b": "two",
		"c": map[string]any{"nested": true},
	})
	first := ComputeNodeHash(n, []string{"X", "Y", "Z"})
	for i := 0; i < 3; i++ {
		got := ComputeNodeHash(n, []string{"X", "Y", "Z"})
		if got != first {
			t.Fatalf("iter %d: nondeterministic hash %q vs %q", i, got, first)
		}
	}
}

// TestComputeNodeHash_SensitiveToID asserts that changing the node ID changes
// the hash output. A hash that ignored the ID would silently allow chain
// forgery across entities.
func TestComputeNodeHash_SensitiveToID(t *testing.T) {
	a := newNodeForHash(t, 1, 0, nil)
	b := newNodeForHash(t, 2, 0, nil)
	if ComputeNodeHash(a, nil) == ComputeNodeHash(b, nil) {
		t.Fatal("hash ignored node ID")
	}
}

// TestComputeNodeHash_SensitiveToVersion asserts that bumping the version
// changes the hash. Version is part of the chain identity.
func TestComputeNodeHash_SensitiveToVersion(t *testing.T) {
	a := newNodeForHash(t, 1, 0, nil)
	b := newNodeForHash(t, 1, 1, nil)
	if ComputeNodeHash(a, nil) == ComputeNodeHash(b, nil) {
		t.Fatal("hash ignored version")
	}
}

// TestComputeNodeHash_SensitiveToLabels asserts label list changes flip the
// hash and that label set differences are visible.
func TestComputeNodeHash_SensitiveToLabels(t *testing.T) {
	n := newNodeForHash(t, 1, 0, nil)
	h1 := ComputeNodeHash(n, []string{"A"})
	h2 := ComputeNodeHash(n, []string{"B"})
	h3 := ComputeNodeHash(n, []string{"A", "B"})
	if h1 == h2 || h1 == h3 || h2 == h3 {
		t.Fatalf("hash insensitive to labels: %q %q %q", h1, h2, h3)
	}
}

// TestComputeNodeHash_LabelOrderIndependent asserts the documented behavior
// that ComputeNodeHash sorts labels defensively before hashing.
func TestComputeNodeHash_LabelOrderIndependent(t *testing.T) {
	n := newNodeForHash(t, 1, 0, nil)
	h1 := ComputeNodeHash(n, []string{"A", "B", "C"})
	h2 := ComputeNodeHash(n, []string{"C", "A", "B"})
	h3 := ComputeNodeHash(n, []string{"B", "C", "A"})
	if h1 != h2 || h2 != h3 {
		t.Fatalf("label-order sensitivity: %q %q %q", h1, h2, h3)
	}
}

// TestComputeNodeHash_DoesNotMutateLabelInput asserts that the defensive
// label sort in ComputeNodeHash does not alter the caller's slice — this
// matters because graph layers reuse label slices outside the hash call.
func TestComputeNodeHash_DoesNotMutateLabelInput(t *testing.T) {
	n := newNodeForHash(t, 1, 0, nil)
	labels := []string{"Z", "A", "M"}
	_ = ComputeNodeHash(n, labels)
	if labels[0] != "Z" || labels[1] != "A" || labels[2] != "M" {
		t.Fatalf("ComputeNodeHash mutated label input: %v", labels)
	}
}

// TestComputeRelHash_SensitiveToEndpoints asserts that swapping start/end
// nodes flips the hash. Direction matters for relationship integrity.
func TestComputeRelHash_SensitiveToEndpoints(t *testing.T) {
	a := newRelForHash(t, 10, 1, 2, 0, nil)
	b := newRelForHash(t, 10, 2, 1, 0, nil)
	if ComputeRelHash(a, "KNOWS") == ComputeRelHash(b, "KNOWS") {
		t.Fatal("hash collapses start/end direction")
	}
}

// TestComputeRelHash_SensitiveToType asserts changing the type string
// changes the digest. Type is part of relationship identity.
func TestComputeRelHash_SensitiveToType(t *testing.T) {
	r := newRelForHash(t, 10, 1, 2, 0, nil)
	if ComputeRelHash(r, "KNOWS") == ComputeRelHash(r, "LIKES") {
		t.Fatal("hash ignored type name")
	}
}

// TestComputeNodeHash_NilPanicsWithSentinel documents the unchecked API
// contract: callers that pass nil get the same sentinel used by type-layer
// nil guards, but as a panic because this is the fast unchecked path.
func TestComputeNodeHash_NilPanicsWithSentinel(t *testing.T) {
	requirePanicIs(t, types.ErrNilNode, func() {
		_ = ComputeNodeHash(nil, nil)
	})
}

func TestComputeRelHash_NilPanicsWithSentinel(t *testing.T) {
	requirePanicIs(t, types.ErrNilRelationship, func() {
		_ = ComputeRelHash(nil, "")
	})
}

func TestComputeNodeHashChecked_NilReturnsSentinel(t *testing.T) {
	got, err := ComputeNodeHashChecked(nil, nil)
	if got != "" {
		t.Fatalf("hash = %q, want empty", got)
	}
	if !errors.Is(err, types.ErrNilNode) {
		t.Fatalf("error = %v, want ErrNilNode", err)
	}
}

func TestComputeRelHashChecked_NilReturnsSentinel(t *testing.T) {
	got, err := ComputeRelHashChecked(nil, "")
	if got != "" {
		t.Fatalf("hash = %q, want empty", got)
	}
	if !errors.Is(err, types.ErrNilRelationship) {
		t.Fatalf("error = %v, want ErrNilRelationship", err)
	}
}

func TestComputeNodeHashChecked_MatchesUncheckedFastPath(t *testing.T) {
	n := newNodeForHash(t, 12, 3, map[string]any{
		"name": "alpha",
		"rank": int64(7),
	})
	labels := []string{"Z", "A"}
	want := ComputeNodeHash(n, labels)
	got, err := ComputeNodeHashChecked(n, labels)
	if err != nil {
		t.Fatalf("ComputeNodeHashChecked: %v", err)
	}
	if got != want {
		t.Fatalf("hash = %q, want %q", got, want)
	}
}

func TestComputeRelHashChecked_MatchesUncheckedFastPath(t *testing.T) {
	r := newRelForHash(t, 12, 3, 4, 2, map[string]any{
		"name": "alpha",
		"rank": int64(7),
	})
	want := ComputeRelHash(r, "EDGE")
	got, err := ComputeRelHashChecked(r, "EDGE")
	if err != nil {
		t.Fatalf("ComputeRelHashChecked: %v", err)
	}
	if got != want {
		t.Fatalf("hash = %q, want %q", got, want)
	}
}

// --- per-type-switch-branch tests, exercised through appendPropertyValue ---

// TestAppendPropertyValue drives each branch of pkg/types's
// appendPropertyValueHashBytes type switch (appendPropertyValue forwards to
// it — see the package doc above) through ComputeNodeHash and pins, per case,
// whether two values hash alike: a value change, a type change with the same
// bytes, an order change in a slice, or a lost length prefix must change the
// digest; a map's key order and a value's own bit pattern must not. This is
// INDIRECT coverage of that switch — the direct, canonical branch coverage
// lives in pkg/types/property_hash_test.go
// (TestAppendPropertyValueHashBytesAllBranches and siblings); these cases
// additionally pin that each branch participates correctly in the node hash.
//
// Floats hash by raw IEEE-754 bits: +0 and -0 differ by the sign bit, and
// distinct NaN payloads stay distinct (types.PropertyValueEqual treats every
// NaN as equal, the hash does not). Callers exchanging data with systems that
// canonicalize NaN bits (non-Go msgpack encoders, FFI boundaries) MUST
// canonicalize at the boundary if cross-system hash chains are to verify.
func TestAppendPropertyValue(t *testing.T) {
	nested := func(depth int, leaf string) any {
		var v any = leaf
		for i := 0; i < depth; i++ {
			v = map[string]any{"k": v}
		}
		return v
	}
	blob := make([]byte, 4096)
	for i := range blob {
		blob[i] = byte(i % 256)
	}
	blobFlipped := append([]byte(nil), blob...)
	blobFlipped[0] ^= 1

	cases := []struct {
		name string
		a, b any
		same bool
	}{
		{"Nil_vs_string", nil, "x", false},
		{"Bool", true, false, false},
		{"Int", int(1), int(2), false},
		{"Int8", int8(1), int8(2), false},
		{"Int8_negative", int8(1), int8(-1), false},
		{"Int16", int16(1), int16(2), false},
		{"Int32", int32(1), int32(2), false},
		{"Int64", int64(1), int64(2), false},
		{"IntVsInt64TypeDistinct", int(42), int64(42), false},
		{"Uint", uint(1), uint(2), false},
		{"Uint8", uint8(1), uint8(255), false},
		{"Uint16", uint16(1), uint16(65535), false},
		{"Uint32", uint32(1), uint32(2), false},
		{"Uint64", uint64(1), uint64(2), false},
		{"Float32", float32(1.5), float32(2.5), false},
		{"Float32_NaNDeterministic", float32(math.NaN()), float32(math.NaN()), true},
		{"Float32_NaN_BitsPreserved", math.Float32frombits(0x7FC00001), math.Float32frombits(0x7FC00099), false},
		{"Float64", 1.5, 2.5, false},
		{"Float64_PositiveNegativeZero", 0.0, math.Copysign(0, -1), false},
		{"Float64_NaN_BitsPreserved", math.Float64frombits(0x7FF8000000000001), math.Float64frombits(0x7FF8000000000099), false},
		{"String", "alpha", "beta", false},
		{"String_vs_empty", "alpha", "", false},
		{"String_LengthIsPrefixed", "ab", "abc", false},
		{"SliceString", []string{"a", "b"}, []string{"a", "b", "c"}, false},
		{"SliceString_vs_empty", []string{"a", "b"}, []string{}, false},
		{"SliceString_OrderDependent", []string{"a", "b"}, []string{"b", "a"}, false},
		{"SliceInt", []int{1, 2, 3}, []int{1, 2, 4}, false},
		{"SliceInt64", []int64{1, 2}, []int64{1, 2, 3}, false},
		{"SliceIntVsSliceInt64", []int{1, 2}, []int64{1, 2}, false},
		{"SliceFloat32", []float32{1, 2, 3}, []float32{1, 2, 3.0001}, false},
		{"SliceFloat64", []float64{1, 2}, []float64{1, 2, 3}, false},
		{"SliceByte", []byte("hello"), []byte("world"), false},
		{"SliceByte_vs_empty", []byte("hello"), []byte{}, false},
		{"SliceByte_large_one_bit", blob, blobFlipped, false},
		{"SliceByteVsString", []byte("x"), "x", false},
		{"SliceBool", []bool{true, false, true}, []bool{true, true, true}, false},
		{"SliceAny_Recurses", []any{int64(1), "two", true, 3.5}, []any{int64(1), "two", false, 3.5}, false},
		{"SliceAny_OrderDependent", []any{int64(1), int64(2)}, []any{int64(2), int64(1)}, false},
		{"MapStrAny_KeyOrderIndependent", map[string]any{"x": int64(1), "y": int64(2), "z": int64(3)}, map[string]any{"z": int64(3), "y": int64(2), "x": int64(1)}, true},
		{"MapStrAny_ValueChangeFlipsHash", map[string]any{"x": int64(1), "y": int64(2)}, map[string]any{"x": int64(1), "y": int64(99)}, false},
		{"MapStrAny_DeeplyNested_same", nested(10, "leaf"), nested(10, "leaf"), true},
		{"MapStrAny_DeeplyNested_leaf", nested(10, "leaf"), nested(10, "DIFFERENT"), false},
		{"MapStrStr_KeyOrderIndependent", map[string]string{"x": "1", "y": "2"}, map[string]string{"y": "2", "x": "1"}, true},
		{"MapStrStr_ValueChange", map[string]string{"x": "1", "y": "2"}, map[string]string{"x": "1", "y": "999"}, false},
		{"MapStrAnyVsMapStrStr", map[string]any{"x": "1"}, map[string]string{"x": "1"}, false},
		{"HashableValue_DefaultBranch", hashableStub{X: 1}, hashableStub{X: 2}, false},
		{"HashableValue_Deterministic", hashableStub{X: 7}, hashableStub{X: 7}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ha, hb := hashPropOnly(t, "k", c.a), hashPropOnly(t, "k", c.b)
			if (ha == hb) != c.same {
				t.Fatalf("%v (%T) and %v (%T): equal digests = %v, want %v", c.a, c.a, c.b, c.b, ha == hb, c.same)
			}
		})
	}
}

// --- HashableValue (default branch) ---

// hashableStub implements types.HashableValue and types.DeepCopier so it
// can be registered as a property struct type. The HashBytes return is
// intentionally tied to the X field so two different X values produce
// different bytes.
type hashableStub struct {
	X int
}

func (h hashableStub) HashBytes() []byte {
	// Encode X as 8-byte big-endian — stable across versions.
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(h.X))
	return buf
}

func (h hashableStub) DeepCopyValue() any { return h }

type panicHashableStub struct {
	X int
}

func (p panicHashableStub) HashBytes() []byte {
	panic("panicHashableStub.HashBytes")
}

func (p panicHashableStub) DeepCopyValue() any { return p }

// initHashableStub registers hashableStub once; the registry is global and
// idempotent. A package-level init keeps the registration outside the test
// hot path.
func init() {
	for _, v := range []any{hashableStub{}, panicHashableStub{}} {
		if err := types.RegisterPropertyStructType(v); err != nil {
			panic(fmt.Sprintf("integrity_test: register %T: %v", v, err))
		}
	}
}

func TestPropertyValueNeedsHashRecoverBranches(t *testing.T) {
	tests := []struct {
		name string
		v    any
		want bool
	}{
		{name: "nil", v: nil, want: false},
		{name: "primitive", v: int64(1), want: false},
		{name: "map string string", v: map[string]string{"x": "y"}, want: false},
		{name: "slice any supported", v: []any{int64(1), map[string]any{"ok": "yes"}}, want: false},
		{name: "slice any hashable", v: []any{hashableStub{X: 1}}, want: true},
		{name: "map string any supported", v: map[string]any{"ok": []any{"yes"}}, want: false},
		{name: "map string any hashable", v: map[string]any{"h": hashableStub{X: 1}}, want: true},
		{name: "map string any unsupported", v: map[string]any{"bad": unsupportedStub{Z: 1}}, want: true},
		{name: "hashable", v: hashableStub{X: 1}, want: true},
		{name: "unsupported non hashable", v: unsupportedStub{Z: 1}, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := propertyValueNeedsHashRecover(tt.v)
			if got != tt.want {
				t.Fatalf("propertyValueNeedsHashRecover(%T) = %v, want %v", tt.v, got, tt.want)
			}
		})
	}
}

func TestPropertySliceNeedsHashRecover(t *testing.T) {
	plain, err := types.NewPropertySlice(map[string]any{
		"nested": []any{int64(1), map[string]any{"ok": true}},
	})
	if err != nil {
		t.Fatalf("NewPropertySlice plain: %v", err)
	}
	if propertySliceNeedsHashRecover(plain) {
		t.Fatal("plain property slice required hash recovery")
	}

	custom, err := types.NewPropertySlice(map[string]any{
		"custom": hashableStub{X: 1},
	})
	if err != nil {
		t.Fatalf("NewPropertySlice custom: %v", err)
	}
	if !propertySliceNeedsHashRecover(custom) {
		t.Fatal("custom property slice did not require hash recovery")
	}
}

func TestComputeNodeHashChecked_HashableValueMatchesUnchecked(t *testing.T) {
	n := newNodeForHash(t, 21, 1, map[string]any{
		"custom": hashableStub{X: 7},
	})
	want := ComputeNodeHash(n, []string{"Custom"})
	got, err := ComputeNodeHashChecked(n, []string{"Custom"})
	if err != nil {
		t.Fatalf("ComputeNodeHashChecked: %v", err)
	}
	if got != want {
		t.Fatalf("hash = %q, want %q", got, want)
	}
}

func TestComputeRelHashChecked_HashableValueMatchesUnchecked(t *testing.T) {
	r := newRelForHash(t, 21, 1, 2, 1, map[string]any{
		"custom": hashableStub{X: 7},
	})
	want := ComputeRelHash(r, "CUSTOM")
	got, err := ComputeRelHashChecked(r, "CUSTOM")
	if err != nil {
		t.Fatalf("ComputeRelHashChecked: %v", err)
	}
	if got != want {
		t.Fatalf("hash = %q, want %q", got, want)
	}
}

func TestComputeNodeHashChecked_RecoversHashablePanic(t *testing.T) {
	n := newNodeForHash(t, 22, 1, map[string]any{
		"custom": panicHashableStub{X: 7},
	})
	got, err := ComputeNodeHashChecked(n, nil)
	if got != "" {
		t.Fatalf("hash = %q, want empty", got)
	}
	if !errors.Is(err, types.ErrUnsupportedValueType) {
		t.Fatalf("error = %v, want ErrUnsupportedValueType", err)
	}
	if !strings.Contains(err.Error(), "compute node hash panic") {
		t.Fatalf("error does not identify node hash panic: %v", err)
	}
}

func TestComputeRelHashChecked_RecoversHashablePanic(t *testing.T) {
	r := newRelForHash(t, 22, 1, 2, 1, map[string]any{
		"custom": panicHashableStub{X: 7},
	})
	got, err := ComputeRelHashChecked(r, "CUSTOM")
	if got != "" {
		t.Fatalf("hash = %q, want empty", got)
	}
	if !errors.Is(err, types.ErrUnsupportedValueType) {
		t.Fatalf("error = %v, want ErrUnsupportedValueType", err)
	}
	if !strings.Contains(err.Error(), "compute relationship hash panic") {
		t.Fatalf("error does not identify relationship hash panic: %v", err)
	}
}

// unsupportedStub is intentionally NOT registered — it exists only to
// trigger the panic branch in appendPropertyValue and prove the public entity
// setters reject that value before it can reach hashing.
type unsupportedStub struct{ Z int }

func TestAppendPropertyValue_UnsupportedTypePanicsOnlyOnBrokenInvariant(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic on unsupported property type")
		}
		msg, ok := r.(string)
		if !ok {
			t.Fatalf("panic value not a string: %T %v", r, r)
		}
		if !strings.Contains(msg, "unsupported type") {
			t.Fatalf("panic message missing 'unsupported type': %q", msg)
		}
	}()
	_ = appendPropertyValue(nil, unsupportedStub{Z: 1})
}

func TestComputeNodeHash_PublicSetPropertiesRejectsUnsupportedType(t *testing.T) {
	n := types.NewNode(types.NodeID(snowflake.ID(1)), 1, nil)
	bad := types.PropertySlice{
		{Key: "weird", Value: unsupportedStub{Z: 1}},
	}
	if err := n.SetProperties(bad); !errors.Is(err, types.ErrUnsupportedValueType) {
		t.Fatalf("SetProperties error = %v, want ErrUnsupportedValueType", err)
	}
	_ = ComputeNodeHash(n, nil)
}

// --- edge cases ---

// TestComputeNodeHash_AllSupportedPrimitives pins the digest of a node with
// one property of every primitive type, so the byte layout of each primitive
// (tag and body, in sorted key order) is fixed, not only the few the anchors
// above carry.
func TestComputeNodeHash_AllSupportedPrimitives(t *testing.T) {
	props := map[string]any{
		"b":   true,
		"i":   int(1),
		"i8":  int8(2),
		"i16": int16(3),
		"i32": int32(4),
		"i64": int64(5),
		"u":   uint(6),
		"u8":  uint8(7),
		"u16": uint16(8),
		"u32": uint32(9),
		"u64": uint64(10),
		"f32": float32(11.5),
		"f64": float64(12.5),
		"s":   "hello",
	}
	n := newNodeForHash(t, 1, 0, props)
	if got := hashOnce(t, n, []string{"L"}); got != anchorAllPrimitives {
		t.Fatalf("hash drift: got %q want %q", got, anchorAllPrimitives)
	}
}

// TestComputeNodeHash_PropertyKeyOrder verifies that two nodes built from
// the same property map (Go map iteration is randomized) produce the same
// hash. PropertySlice sorts keys at construction, so the sort order is
// canonical irrespective of insertion order.
func TestComputeNodeHash_PropertyKeyOrder(t *testing.T) {
	n1 := newNodeForHash(t, 1, 0, map[string]any{
		"z": int64(1), "a": int64(2), "m": int64(3),
	})
	n2 := newNodeForHash(t, 1, 0, map[string]any{
		"a": int64(2), "m": int64(3), "z": int64(1),
	})
	if ComputeNodeHash(n1, nil) != ComputeNodeHash(n2, nil) {
		t.Fatal("property key insertion order changed hash")
	}
}

// TestComputeRelHash_SensitiveToProperties asserts that a relationship's
// property hash changes when properties change, exercising the
// appendProperties path through ComputeRelHash.
func TestComputeRelHash_SensitiveToProperties(t *testing.T) {
	a := newRelForHash(t, 1, 1, 2, 0, map[string]any{"x": int64(1)})
	b := newRelForHash(t, 1, 1, 2, 0, map[string]any{"x": int64(2)})
	if ComputeRelHash(a, "T") == ComputeRelHash(b, "T") {
		t.Fatal("rel hash ignored property change")
	}
}

// TestComputeRelHash_DeterministicAndSensitive mirrors the node tests for
// parity (Node and Relationship are structural mirrors).
func TestComputeRelHash_DeterministicAndSensitive(t *testing.T) {
	r := newRelForHash(t, 100, 1, 2, 5, map[string]any{
		"weight": 2.5,
		"label":  "primary",
	})
	first := ComputeRelHash(r, "EDGE")
	for i := 0; i < 3; i++ {
		if got := ComputeRelHash(r, "EDGE"); got != first {
			t.Fatalf("iter %d: nondeterministic rel hash %q vs %q", i, got, first)
		}
	}

	r2 := newRelForHash(t, 100, 1, 2, 5, map[string]any{
		"weight": 2.5,
		"label":  "primaryX", // single-byte change
	})
	if ComputeRelHash(r2, "EDGE") == first {
		t.Fatal("rel hash insensitive to single-byte property change")
	}
}

// TestComputeNodeHash_CrossCheckBufferLayout reconstructs the exact buffer
// layout in this test file (out-of-band) and asserts the hash matches the
// implementation's output. This is a belt-and-suspenders check independent
// of the fixed-vector anchors above — it catches any change to the byte
// layout that happens to land on a previously seen fixed-vector hash.
func TestComputeNodeHash_CrossCheckBufferLayout(t *testing.T) {
	// Node id=99, version=7, two labels "alpha" + "beta", one property
	// "speed"=int64(123).
	n := types.NewNode(types.NodeID(snowflake.ID(99)), 1, nil)
	n.SetVersion(7)
	if err := n.SetProperty("speed", int64(123)); err != nil {
		t.Fatalf("SetProperty: %v", err)
	}

	// Construct the expected buffer manually.
	var buf []byte
	buf = binary.BigEndian.AppendUint64(buf, uint64(99))
	buf = binary.BigEndian.AppendUint32(buf, uint32(7))
	for _, l := range []string{"alpha", "beta"} { // already sorted
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(l)))
		buf = append(buf, l...)
	}
	// property "speed" with int64 value
	buf = binary.BigEndian.AppendUint32(buf, uint32(len("speed")))
	buf = append(buf, "speed"...)
	buf = append(buf, byte(6)) // ptInt64 tag
	buf = binary.BigEndian.AppendUint64(buf, uint64(123))

	expected := sha256.Sum256(buf)
	want := hex.EncodeToString(expected[:])

	got := ComputeNodeHash(n, []string{"alpha", "beta"})
	if got != want {
		t.Fatalf("cross-check: got %q want %q (buffer layout drift)", got, want)
	}
}

// TestComputeRelHash_CrossCheckBufferLayout mirrors the node cross-check.
func TestComputeRelHash_CrossCheckBufferLayout(t *testing.T) {
	r := types.NewRelationship(
		types.RelID(snowflake.ID(55)), 1,
		types.NodeID(snowflake.ID(11)),
		types.NodeID(snowflake.ID(22)),
	)
	r.SetVersion(3)
	if err := r.SetProperty("weight", float32(2.5)); err != nil {
		t.Fatalf("SetProperty: %v", err)
	}

	var buf []byte
	buf = binary.BigEndian.AppendUint64(buf, uint64(55))
	buf = binary.BigEndian.AppendUint32(buf, uint32(3))
	buf = binary.BigEndian.AppendUint32(buf, uint32(len("FOLLOWS")))
	buf = append(buf, "FOLLOWS"...)
	buf = binary.BigEndian.AppendUint64(buf, uint64(11))
	buf = binary.BigEndian.AppendUint64(buf, uint64(22))
	// property "weight" float32
	buf = binary.BigEndian.AppendUint32(buf, uint32(len("weight")))
	buf = append(buf, "weight"...)
	buf = append(buf, byte(12)) // ptFloat32 tag
	buf = binary.BigEndian.AppendUint32(buf, math.Float32bits(2.5))

	expected := sha256.Sum256(buf)
	want := hex.EncodeToString(expected[:])
	got := ComputeRelHash(r, "FOLLOWS")
	if got != want {
		t.Fatalf("cross-check rel: got %q want %q", got, want)
	}
}
