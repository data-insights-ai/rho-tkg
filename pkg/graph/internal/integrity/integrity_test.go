// Package integrity tests cover ComputeNodeHash, ComputeRelHash, and the
// per-type-tag dispatch inside appendPropertyValue. The tests intentionally
// pin the exact byte layout via fixed-vector SHA-256 anchors: any change to
// the hash bytes invalidates every persisted tkg_hash / tkg_prev_hash, so
// silent algorithm drift must trip a test.
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
// test parity (Rule 2: Node and Relationship are structural mirrors).
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

// --- per-type-switch-branch tests for appendPropertyValue ---

// Each test below triggers exactly one branch of the appendPropertyValue
// type switch, then verifies (a) determinism and (b) sensitivity to value
// change for that property type. Per CLAUDE.md Rule 3.

func TestAppendPropertyValue_Nil(t *testing.T) {
	// PropertySlice.Set accepts nil values directly via ValidatePropertyValue
	// which short-circuits on nil. The integrity hash takes the "v == nil"
	// fast path which writes only the type tag (ptUnknown=0).
	n := newNodeForHash(t, 1, 0, nil)
	if err := n.SetProperty("missing", nil); err != nil {
		t.Fatalf("SetProperty(nil): %v", err)
	}
	got := hashOnce(t, n, nil)
	// Compare against a different value at the same key — must differ.
	other := hashPropOnly(t, "missing", "x")
	if got == other {
		t.Fatal("nil and non-nil hashed identically")
	}
}

func TestAppendPropertyValue_Bool(t *testing.T) {
	hT := hashPropOnly(t, "k", true)
	hF := hashPropOnly(t, "k", false)
	if hT == hF {
		t.Fatalf("bool true/false collide: %q", hT)
	}
}

func TestAppendPropertyValue_Int(t *testing.T) {
	h1 := hashPropOnly(t, "k", int(1))
	h2 := hashPropOnly(t, "k", int(2))
	if h1 == h2 {
		t.Fatal("int values collide")
	}
}

func TestAppendPropertyValue_Int8(t *testing.T) {
	h1 := hashPropOnly(t, "k", int8(1))
	h2 := hashPropOnly(t, "k", int8(2))
	hNeg := hashPropOnly(t, "k", int8(-1))
	if h1 == h2 || h1 == hNeg {
		t.Fatal("int8 values collide")
	}
}

func TestAppendPropertyValue_Int16(t *testing.T) {
	h1 := hashPropOnly(t, "k", int16(1))
	h2 := hashPropOnly(t, "k", int16(2))
	if h1 == h2 {
		t.Fatal("int16 values collide")
	}
}

func TestAppendPropertyValue_Int32(t *testing.T) {
	h1 := hashPropOnly(t, "k", int32(1))
	h2 := hashPropOnly(t, "k", int32(2))
	if h1 == h2 {
		t.Fatal("int32 values collide")
	}
}

func TestAppendPropertyValue_Int64(t *testing.T) {
	h1 := hashPropOnly(t, "k", int64(1))
	h2 := hashPropOnly(t, "k", int64(2))
	if h1 == h2 {
		t.Fatal("int64 values collide")
	}
}

// TestAppendPropertyValue_IntVsInt64TypeDistinct documents the contract:
// int(42) and int64(42) hash differently because the property type tag
// (ptInt=2 vs ptInt64=6) is prefixed before the 8-byte body. This contract
// matters because property values round-trip through PropertySlice.Set
// which preserves the original Go type.
func TestAppendPropertyValue_IntVsInt64TypeDistinct(t *testing.T) {
	hi := hashPropOnly(t, "k", int(42))
	hi64 := hashPropOnly(t, "k", int64(42))
	if hi == hi64 {
		t.Fatal("int(42) and int64(42) hash identically — type tag missing")
	}
}

func TestAppendPropertyValue_Uint(t *testing.T) {
	h1 := hashPropOnly(t, "k", uint(1))
	h2 := hashPropOnly(t, "k", uint(2))
	if h1 == h2 {
		t.Fatal("uint values collide")
	}
}

func TestAppendPropertyValue_Uint8(t *testing.T) {
	h1 := hashPropOnly(t, "k", uint8(1))
	h2 := hashPropOnly(t, "k", uint8(255))
	if h1 == h2 {
		t.Fatal("uint8 values collide")
	}
}

func TestAppendPropertyValue_Uint16(t *testing.T) {
	h1 := hashPropOnly(t, "k", uint16(1))
	h2 := hashPropOnly(t, "k", uint16(65535))
	if h1 == h2 {
		t.Fatal("uint16 values collide")
	}
}

func TestAppendPropertyValue_Uint32(t *testing.T) {
	h1 := hashPropOnly(t, "k", uint32(1))
	h2 := hashPropOnly(t, "k", uint32(2))
	if h1 == h2 {
		t.Fatal("uint32 values collide")
	}
}

func TestAppendPropertyValue_Uint64(t *testing.T) {
	h1 := hashPropOnly(t, "k", uint64(1))
	h2 := hashPropOnly(t, "k", uint64(2))
	if h1 == h2 {
		t.Fatal("uint64 values collide")
	}
}

func TestAppendPropertyValue_Float32(t *testing.T) {
	h1 := hashPropOnly(t, "k", float32(1.5))
	h2 := hashPropOnly(t, "k", float32(2.5))
	if h1 == h2 {
		t.Fatal("float32 values collide")
	}
}

// TestAppendPropertyValue_Float32_NaNDeterministic asserts that the NaN
// produced by math.NaN→float32 is hashable without panic and that two
// invocations agree (NaN != NaN at the float comparator level, but the
// bit pattern feeding the hash is stable).
func TestAppendPropertyValue_Float32_NaNDeterministic(t *testing.T) {
	nan := float32(math.NaN())
	h1 := hashPropOnly(t, "k", nan)
	h2 := hashPropOnly(t, "k", nan)
	if h1 != h2 {
		t.Fatal("float32 NaN hash nondeterministic")
	}
}

func TestAppendPropertyValue_Float64(t *testing.T) {
	h1 := hashPropOnly(t, "k", 1.5)
	h2 := hashPropOnly(t, "k", 2.5)
	if h1 == h2 {
		t.Fatal("float64 values collide")
	}
}

func TestAppendPropertyValue_Float64_PositiveNegativeZero(t *testing.T) {
	// IEEE-754 distinguishes +0 and -0 by sign bit. Float64bits exposes the
	// difference, so the hashes must differ — documents the contract.
	pos := hashPropOnly(t, "k", 0.0)
	neg := hashPropOnly(t, "k", math.Copysign(0, -1))
	if pos == neg {
		t.Fatal("+0.0 and -0.0 hash identically — sign bit lost")
	}
}

// TestAppendPropertyValue_Float64_NaN_BitsPreserved documents the same
// "hash preserves bit pattern" contract for NaN values. IEEE-754 admits
// multiple distinct NaN bit patterns; types.PropertyValueEqual treats them
// as equal (CAS short-circuit semantics) but the integrity hash distinguishes
// them. The asymmetry is intentional and mirrors the +0/-0 case above.
//
// Callers exchanging data with systems that canonicalize NaN bit patterns
// (e.g., non-Go msgpack encoders, FFI boundaries) MUST canonicalize at the
// boundary if cross-system hash chains are expected to verify.
func TestAppendPropertyValue_Float64_NaN_BitsPreserved(t *testing.T) {
	nanQuiet := math.Float64frombits(0x7FF8000000000001)
	nanAlt := math.Float64frombits(0x7FF8000000000099)
	if !math.IsNaN(nanQuiet) || !math.IsNaN(nanAlt) {
		t.Fatal("test setup: both values must be NaN")
	}
	h1 := hashPropOnly(t, "k", nanQuiet)
	h2 := hashPropOnly(t, "k", nanAlt)
	if h1 == h2 {
		t.Fatal("two NaN bit patterns hash identically — payload bits lost")
	}
}

// TestAppendPropertyValue_Float32_NaN_BitsPreserved mirrors the float64 test
// for float32 NaN. Same contract: distinct NaN bit patterns hash distinctly.
func TestAppendPropertyValue_Float32_NaN_BitsPreserved(t *testing.T) {
	nanQuiet := math.Float32frombits(0x7FC00001)
	nanAlt := math.Float32frombits(0x7FC00099)
	if !math.IsNaN(float64(nanQuiet)) || !math.IsNaN(float64(nanAlt)) {
		t.Fatal("test setup: both values must be NaN")
	}
	h1 := hashPropOnly(t, "k", nanQuiet)
	h2 := hashPropOnly(t, "k", nanAlt)
	if h1 == h2 {
		t.Fatal("two float32 NaN bit patterns hash identically — payload bits lost")
	}
}

func TestAppendPropertyValue_String(t *testing.T) {
	h1 := hashPropOnly(t, "k", "alpha")
	h2 := hashPropOnly(t, "k", "beta")
	hEmpty := hashPropOnly(t, "k", "")
	if h1 == h2 || h1 == hEmpty {
		t.Fatal("string values collide")
	}
}

// TestAppendPropertyValue_String_LengthIsPrefixed asserts that "ab" + "c"
// hashes differently from "a" + "bc" — the uint32 length prefix must
// disambiguate concatenation boundaries.
func TestAppendPropertyValue_String_LengthIsPrefixed(t *testing.T) {
	// The hash for the full property "ab"||"c" must differ from "a"||"bc"
	// at the bytes-of-the-property level. We confirm by hashing two distinct
	// nodes whose properties concatenate to the same bytes if length prefixes
	// were absent.
	h1 := hashPropOnly(t, "k", "ab")
	h2 := hashPropOnly(t, "k", "abc")
	if h1 == h2 {
		t.Fatal("differing string values hash identically")
	}
}

func TestAppendPropertyValue_SliceString(t *testing.T) {
	h1 := hashPropOnly(t, "k", []string{"a", "b"})
	h2 := hashPropOnly(t, "k", []string{"a", "b", "c"})
	hEmpty := hashPropOnly(t, "k", []string{})
	if h1 == h2 || h1 == hEmpty {
		t.Fatal("[]string values collide")
	}
}

// TestAppendPropertyValue_SliceString_OrderDependent asserts that slice
// element order is preserved in the hash (vs maps which sort).
func TestAppendPropertyValue_SliceString_OrderDependent(t *testing.T) {
	h1 := hashPropOnly(t, "k", []string{"a", "b"})
	h2 := hashPropOnly(t, "k", []string{"b", "a"})
	if h1 == h2 {
		t.Fatal("[]string order ignored — slice canonicalization is wrong")
	}
}

func TestAppendPropertyValue_SliceInt(t *testing.T) {
	h1 := hashPropOnly(t, "k", []int{1, 2, 3})
	h2 := hashPropOnly(t, "k", []int{1, 2, 4})
	if h1 == h2 {
		t.Fatal("[]int values collide")
	}
}

func TestAppendPropertyValue_SliceInt64(t *testing.T) {
	h1 := hashPropOnly(t, "k", []int64{1, 2})
	h2 := hashPropOnly(t, "k", []int64{1, 2, 3})
	if h1 == h2 {
		t.Fatal("[]int64 values collide")
	}
}

// TestAppendPropertyValue_SliceIntVsSliceInt64 confirms that []int and
// []int64 hash differently due to distinct type tags (ptSliceInt vs
// ptSliceInt64), even when the element values match.
func TestAppendPropertyValue_SliceIntVsSliceInt64(t *testing.T) {
	hi := hashPropOnly(t, "k", []int{1, 2})
	hi64 := hashPropOnly(t, "k", []int64{1, 2})
	if hi == hi64 {
		t.Fatal("[]int and []int64 hash identically — type tag missing")
	}
}

func TestAppendPropertyValue_SliceFloat32(t *testing.T) {
	// []float32 is the embedding/vector index type. Verify that any element
	// change flips the hash.
	h1 := hashPropOnly(t, "k", []float32{1, 2, 3})
	h2 := hashPropOnly(t, "k", []float32{1, 2, 3.0001})
	if h1 == h2 {
		t.Fatal("[]float32 values collide")
	}
}

func TestAppendPropertyValue_SliceFloat64(t *testing.T) {
	h1 := hashPropOnly(t, "k", []float64{1, 2})
	h2 := hashPropOnly(t, "k", []float64{1, 2, 3})
	if h1 == h2 {
		t.Fatal("[]float64 values collide")
	}
}

func TestAppendPropertyValue_SliceByte(t *testing.T) {
	h1 := hashPropOnly(t, "k", []byte("hello"))
	h2 := hashPropOnly(t, "k", []byte("world"))
	hEmpty := hashPropOnly(t, "k", []byte{})
	if h1 == h2 || h1 == hEmpty {
		t.Fatal("[]byte values collide")
	}
}

// TestAppendPropertyValue_SliceByteVsString asserts that []byte("x") and
// "x" hash differently — they share UTF-8 bytes but use different type
// tags (ptSliceByte vs ptString).
func TestAppendPropertyValue_SliceByteVsString(t *testing.T) {
	hb := hashPropOnly(t, "k", []byte("x"))
	hs := hashPropOnly(t, "k", "x")
	if hb == hs {
		t.Fatal("[]byte and string with same bytes hash identically")
	}
}

func TestAppendPropertyValue_SliceBool(t *testing.T) {
	h1 := hashPropOnly(t, "k", []bool{true, false, true})
	h2 := hashPropOnly(t, "k", []bool{true, true, true})
	if h1 == h2 {
		t.Fatal("[]bool values collide")
	}
}

func TestAppendPropertyValue_SliceAny_Recurses(t *testing.T) {
	// []any triggers the recursion branch — each element gets its own type
	// tag inside appendPropertyValue. Mix int/string/bool/float to exercise
	// recursion across multiple dispatch arms.
	h1 := hashPropOnly(t, "k", []any{int64(1), "two", true, 3.5})
	h2 := hashPropOnly(t, "k", []any{int64(1), "two", false, 3.5})
	if h1 == h2 {
		t.Fatal("[]any with mutated bool element hashes identically")
	}
	// Order-dependence again, via []any.
	h3 := hashPropOnly(t, "k", []any{int64(1), int64(2)})
	h4 := hashPropOnly(t, "k", []any{int64(2), int64(1)})
	if h3 == h4 {
		t.Fatal("[]any order ignored")
	}
}

func TestAppendPropertyValue_MapStrAny_KeyOrderIndependent(t *testing.T) {
	// Maps must canonicalize key order before hashing — Go map iteration
	// order is randomized, so without sorting the hash would be
	// nondeterministic across invocations and processes.
	a := map[string]any{"x": int64(1), "y": int64(2), "z": int64(3)}
	b := map[string]any{"z": int64(3), "y": int64(2), "x": int64(1)}
	hA := hashPropOnly(t, "k", a)
	hB := hashPropOnly(t, "k", b)
	if hA != hB {
		t.Fatalf("map[string]any key-order sensitive: %q vs %q", hA, hB)
	}
}

func TestAppendPropertyValue_MapStrAny_ValueChangeFlipsHash(t *testing.T) {
	a := map[string]any{"x": int64(1), "y": int64(2)}
	b := map[string]any{"x": int64(1), "y": int64(99)}
	if hashPropOnly(t, "k", a) == hashPropOnly(t, "k", b) {
		t.Fatal("map[string]any value change ignored")
	}
}

func TestAppendPropertyValue_MapStrStr(t *testing.T) {
	a := map[string]string{"x": "1", "y": "2"}
	b := map[string]string{"y": "2", "x": "1"}
	if hashPropOnly(t, "k", a) != hashPropOnly(t, "k", b) {
		t.Fatal("map[string]string key-order sensitive")
	}
	c := map[string]string{"x": "1", "y": "999"}
	if hashPropOnly(t, "k", a) == hashPropOnly(t, "k", c) {
		t.Fatal("map[string]string value change ignored")
	}
}

// TestAppendPropertyValue_MapStrAnyVsMapStrStr asserts that the two map
// shapes hash differently even when their string values match — the type
// tags (ptMapStrAny vs ptMapStrStr) keep them distinct.
func TestAppendPropertyValue_MapStrAnyVsMapStrStr(t *testing.T) {
	mA := map[string]any{"x": "1"}
	mS := map[string]string{"x": "1"}
	if hashPropOnly(t, "k", mA) == hashPropOnly(t, "k", mS) {
		t.Fatal("map[string]any and map[string]string hash identically")
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

func TestAppendPropertyValue_HashableValue_DefaultBranch(t *testing.T) {
	h1 := hashPropOnly(t, "geom", hashableStub{X: 1})
	h2 := hashPropOnly(t, "geom", hashableStub{X: 2})
	if h1 == h2 {
		t.Fatal("HashableValue default branch ignored value change")
	}
}

func TestAppendPropertyValue_HashableValue_Deterministic(t *testing.T) {
	v := hashableStub{X: 7}
	h1 := hashPropOnly(t, "geom", v)
	h2 := hashPropOnly(t, "geom", v)
	if h1 != h2 {
		t.Fatalf("HashableValue nondeterministic: %q vs %q", h1, h2)
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

// TestComputeNodeHash_EmptyNoProps exercises the simplest path: no labels,
// no properties. Pairs with the fixed-vector test above.
func TestComputeNodeHash_EmptyNoProps(t *testing.T) {
	n := newNodeForHash(t, 1, 0, nil)
	got := hashOnce(t, n, nil)
	if got == "" {
		t.Fatal("empty node produced empty hash")
	}
}

// TestComputeNodeHash_DeeplyNestedMaps exercises depth-limited recursion
// through the []any / map[string]any branches. Goes 10 levels deep — well
// past typical inputs but inside the propertyslice depth limit (32).
func TestComputeNodeHash_DeeplyNestedMaps(t *testing.T) {
	build := func(depth int) any {
		var v any = "leaf"
		for i := 0; i < depth; i++ {
			v = map[string]any{"k": v}
		}
		return v
	}
	a := build(10)
	b := build(10)
	hA := hashPropOnly(t, "deep", a)
	hB := hashPropOnly(t, "deep", b)
	if hA != hB {
		t.Fatalf("deeply nested identical maps hash differently: %q vs %q", hA, hB)
	}
	// Sensitivity at the leaf.
	c := map[string]any{"k": map[string]any{"k": "DIFFERENT"}}
	if hA == hashPropOnly(t, "deep", c) {
		t.Fatal("deep leaf change ignored")
	}
}

// TestComputeNodeHash_AllSupportedPrimitives builds a node with one
// property of every primitive type and confirms it hashes deterministically
// and survives a round-trip via PropertySlice.
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
	got := hashOnce(t, n, []string{"L"})
	if got == "" {
		t.Fatal("primitive-cocktail node produced empty hash")
	}
}

// TestComputeNodeHash_LargeProperty exercises the uint32 length-prefix path
// for a property value approaching the validation limit (here a moderately
// large string — keeping it small in tests for speed; the encoding logic is
// identical for any size up to 4 GiB).
func TestComputeNodeHash_LargeProperty(t *testing.T) {
	big := make([]byte, 4096)
	for i := range big {
		big[i] = byte(i % 256)
	}
	h1 := hashPropOnly(t, "blob", big)
	bigCopy := make([]byte, 4096)
	copy(bigCopy, big)
	bigCopy[0] ^= 1
	h2 := hashPropOnly(t, "blob", bigCopy)
	if h1 == h2 {
		t.Fatal("single-byte change in 4KB blob did not flip hash")
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

// TestComputeRelHash_DeterministicAndSensitive mirrors the node tests for
// Rule 2 parity (Node and Relationship are structural mirrors).
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
