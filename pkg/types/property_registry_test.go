package types

import (
	"errors"
	"math"
	"reflect"
	"testing"

	collisionone "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types/internal/collisionone"
	collisiontwo "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types/internal/collisiontwo"
)

// Modification rationale: spatialStub originally had no HashableValue or
// DeepCopier methods. Tests registered it via the old void-returning
// RegisterPropertyStructType, encoding the original buggy behavior where
// any struct could register without satisfying integrity-hash or
// deep-copy contracts. The fix requires registration to verify both
// interfaces; spatialStub now implements both so the same registration-
// surface tests (idempotency, value-vs-pointer acceptance, nil-pointer
// rejection) can still run against valid input.
type spatialStub struct {
	X, Y float64
}

// HashBytes provides a deterministic representation suitable for integrity
// hashing. Trivial (no nested mutable state) — float64 fields shipped as
// big-endian bits via math.Float64bits.
func (s spatialStub) HashBytes() []byte {
	buf := make([]byte, 16)
	x := math.Float64bits(s.X)
	y := math.Float64bits(s.Y)
	for i := 0; i < 8; i++ {
		buf[i] = byte(x >> (8 * (7 - i)))
		buf[8+i] = byte(y >> (8 * (7 - i)))
	}
	return buf
}

// DeepCopyValue is trivial because spatialStub holds only primitive fields.
// Returns the value unchanged — Go copies struct values by assignment.
func (s spatialStub) DeepCopyValue() any { return s }

// unregisteredStub similarly used to be a bare struct. It now implements
// both contracts so it remains valid input for the few tests that need a
// registrable-but-distinct type. The "Unregistered" name still describes
// its role: it's never actually registered in the affected tests.
type unregisteredStub struct {
	A int
}

func (u unregisteredStub) HashBytes() []byte  { return []byte{byte(u.A)} }
func (u unregisteredStub) DeepCopyValue() any { return u }

// hashCopyStub is used by TestRegisteredPropertyStructTypes_Sorted to
// register a third type alongside spatialStub and unregisteredStub. The
// original test used an anonymous struct{ Z int } literal, which under the
// new registration contract would be rejected; this named stub satisfies
// HashableValue + DeepCopier and preserves the test's intent (verifying
// lexicographic ordering in RegisteredPropertyStructTypes).
type hashCopyStub struct {
	Z int
}

func (h hashCopyStub) HashBytes() []byte  { return []byte{byte(h.Z)} }
func (h hashCopyStub) DeepCopyValue() any { return h }

type registeredNamedInt int

func (n registeredNamedInt) HashBytes() []byte  { return []byte{byte(n)} }
func (n registeredNamedInt) DeepCopyValue() any { return n }

type pointerOnlyPropertyStub struct {
	V byte
}

func (p *pointerOnlyPropertyStub) HashBytes() []byte  { return []byte{p.V} }
func (p *pointerOnlyPropertyStub) DeepCopyValue() any { cp := *p; return &cp }

// mustRegister is a test-only helper that fails the test if registration
// returns an error. Tests in this file are happy-path registration tests
// for known-good stubs; an error here is a regression in the registration
// contract itself, which other tests cover explicitly.
func mustRegister(t *testing.T, v any) {
	t.Helper()
	if err := RegisterPropertyStructType(v); err != nil {
		t.Fatalf("RegisterPropertyStructType(%T): unexpected error: %v", v, err)
	}
}

func TestRegisteredPropertyStructWireTypeDirectBranches(t *testing.T) {
	t.Cleanup(resetRegistry)

	if typeName, pointer, ok := RegisteredPropertyStructWireType(nil); ok || typeName != "" || pointer {
		t.Fatalf("nil wire type = (%q, %v, %v), want zero false tuple", typeName, pointer, ok)
	}
	if typeName, pointer, ok := RegisteredPropertyStructWireType(spatialStub{}); ok || typeName != "" || pointer {
		t.Fatalf("unregistered wire type = (%q, %v, %v), want zero false tuple", typeName, pointer, ok)
	}

	mustRegister(t, spatialStub{})
	typeName, pointer, ok := RegisteredPropertyStructWireType(spatialStub{X: 1, Y: 2})
	if !ok || pointer || typeName != "types.spatialStub" {
		t.Fatalf("value wire type = (%q, %v, %v), want types.spatialStub false true", typeName, pointer, ok)
	}
	typeName, pointer, ok = RegisteredPropertyStructWireType(&spatialStub{X: 3, Y: 4})
	if !ok || !pointer || typeName != "types.spatialStub" {
		t.Fatalf("pointer wire type = (%q, %v, %v), want types.spatialStub true true", typeName, pointer, ok)
	}
}

func TestRegisteredPropertyStructWireTypeRejectsPointerReceiverValue(t *testing.T) {
	t.Cleanup(resetRegistry)
	mustRegister(t, (*pointerOnlyPropertyStub)(nil))

	typeName, pointer, ok := RegisteredPropertyStructWireType(pointerOnlyPropertyStub{V: 7})
	if ok || typeName != "" || pointer {
		t.Fatalf("pointer-receiver value wire type = (%q, %v, %v), want zero false tuple", typeName, pointer, ok)
	}

	typeName, pointer, ok = RegisteredPropertyStructWireType(&pointerOnlyPropertyStub{V: 7})
	if !ok || !pointer || typeName != "types.pointerOnlyPropertyStub" {
		t.Fatalf("pointer-receiver pointer wire type = (%q, %v, %v), want types.pointerOnlyPropertyStub true true", typeName, pointer, ok)
	}
}

func TestNewRegisteredPropertyStructPointerDirectBranches(t *testing.T) {
	t.Cleanup(resetRegistry)

	if got, ok := NewRegisteredPropertyStructPointer("types.spatialStub"); ok || got != nil {
		t.Fatalf("unregistered constructor = (%T, %v), want nil false", got, ok)
	}

	mustRegister(t, spatialStub{})
	got, ok := NewRegisteredPropertyStructPointer("types.spatialStub")
	if !ok {
		t.Fatal("registered constructor returned ok=false")
	}
	if _, ok := got.(*spatialStub); !ok {
		t.Fatalf("registered constructor returned %T, want *spatialStub", got)
	}
	if got, ok := NewRegisteredPropertyStructPointer("types.missing"); ok || got != nil {
		t.Fatalf("missing constructor = (%T, %v), want nil false", got, ok)
	}
}

func TestRegisterPropertyStructType_AcceptsValue(t *testing.T) {
	t.Cleanup(resetRegistry)
	mustRegister(t, spatialStub{})

	var ps PropertySlice
	if err := ps.Set("pt", spatialStub{X: 1, Y: 2}); err != nil {
		t.Errorf("registered value type should be accepted: %v", err)
	}
}

func TestRegisterPropertyStructType_AcceptsPointer(t *testing.T) {
	t.Cleanup(resetRegistry)
	mustRegister(t, spatialStub{})

	var ps PropertySlice
	if err := ps.Set("pt", &spatialStub{X: 1, Y: 2}); err != nil {
		t.Errorf("pointer to registered type should be accepted: %v", err)
	}
}

func TestRegisterPropertyStructType_RegisteringPointerAlsoAcceptsValue(t *testing.T) {
	t.Cleanup(resetRegistry)
	mustRegister(t, (*spatialStub)(nil))

	var ps PropertySlice
	if err := ps.Set("pt", spatialStub{X: 1, Y: 2}); err != nil {
		t.Errorf("value form should be accepted after pointer registration: %v", err)
	}
	if err := ps.Set("pt2", &spatialStub{X: 3, Y: 4}); err != nil {
		t.Errorf("pointer form should be accepted: %v", err)
	}
}

func TestRegisterPropertyStructType_UntypedNilRejected(t *testing.T) {
	t.Cleanup(resetRegistry)

	err := RegisterPropertyStructType(nil)
	if !errors.Is(err, ErrUnsupportedValueType) {
		t.Fatalf("RegisterPropertyStructType(nil): got %v, want ErrUnsupportedValueType", err)
	}
	if got := RegisteredPropertyStructTypes(); len(got) != 0 {
		t.Fatalf("RegisterPropertyStructType(nil) mutated registry: %v", got)
	}
}

func TestRegisterPropertyStructType_NonStructRejected(t *testing.T) {
	t.Cleanup(resetRegistry)

	err := RegisterPropertyStructType(registeredNamedInt(1))
	if !errors.Is(err, ErrUnsupportedValueType) {
		t.Fatalf("RegisterPropertyStructType(named int): got %v, want ErrUnsupportedValueType", err)
	}
	if got := RegisteredPropertyStructTypes(); len(got) != 0 {
		t.Fatalf("RegisterPropertyStructType(named int) mutated registry: %v", got)
	}

	err = RegisterPropertyStructType((*registeredNamedInt)(nil))
	if !errors.Is(err, ErrUnsupportedValueType) {
		t.Fatalf("RegisterPropertyStructType(*named int): got %v, want ErrUnsupportedValueType", err)
	}
	if got := RegisteredPropertyStructTypes(); len(got) != 0 {
		t.Fatalf("RegisterPropertyStructType(*named int) mutated registry: %v", got)
	}
}

func TestRegisterPropertyStructType_UnregisteredRejected(t *testing.T) {
	t.Cleanup(resetRegistry)

	var ps PropertySlice
	err := ps.Set("bad", unregisteredStub{A: 1})
	if !errors.Is(err, ErrUnsupportedValueType) {
		t.Errorf("got %v, want ErrUnsupportedValueType", err)
	}
	err = ps.Set("bad", &unregisteredStub{A: 1})
	if !errors.Is(err, ErrUnsupportedValueType) {
		t.Errorf("got %v, want ErrUnsupportedValueType for pointer", err)
	}
}

func TestRegisterPropertyStructType_NilPointerRejected(t *testing.T) {
	t.Cleanup(resetRegistry)
	mustRegister(t, spatialStub{})

	var ps PropertySlice
	var nilPtr *spatialStub
	// A typed nil pointer — isRegisteredPropertyStructType refuses nil pointers
	// because the stored value would carry no data. Upstream code should be
	// using an actual value, not a nil pointer.
	err := ps.Set("pt", nilPtr)
	if !errors.Is(err, ErrUnsupportedValueType) {
		t.Errorf("got %v, want ErrUnsupportedValueType for nil pointer", err)
	}
}

func TestRegisterPropertyStructType_Idempotent(t *testing.T) {
	t.Cleanup(resetRegistry)
	mustRegister(t, spatialStub{})
	mustRegister(t, spatialStub{}) // second call must not error or panic

	names := RegisteredPropertyStructTypes()
	count := 0
	for _, n := range names {
		if n == "types.spatialStub" {
			count++
		}
	}
	if count != 1 {
		t.Errorf("expected 1 registration, found %d: %v", count, names)
	}
}

func TestRegisterPropertyStructType_RejectsWireNameCollision(t *testing.T) {
	t.Cleanup(resetRegistry)

	if err := RegisterPropertyStructType(collisionone.Value{}); err != nil {
		t.Fatalf("RegisterPropertyStructType(collisionone.Value): %v", err)
	}
	err := RegisterPropertyStructType(collisiontwo.Value{})
	if !errors.Is(err, ErrPropertyTypeNameCollision) {
		t.Fatalf("RegisterPropertyStructType(collisiontwo.Value) = %v, want ErrPropertyTypeNameCollision", err)
	}

	names := RegisteredPropertyStructTypes()
	if len(names) != 1 || names[0] != "collision.Value" {
		t.Fatalf("registered names after collision = %v, want [collision.Value]", names)
	}
	typeName, _, ok := RegisteredPropertyStructWireType(collisionone.Value{})
	if !ok || typeName != "collision.Value" {
		t.Fatalf("registered collisionone wire type = (%q, %v), want collision.Value true", typeName, ok)
	}
	if typeName, _, ok := RegisteredPropertyStructWireType(collisiontwo.Value{}); ok || typeName != "" {
		t.Fatalf("rejected collisiontwo wire type = (%q, %v), want empty false", typeName, ok)
	}
}

func TestRegisteredPropertyStructTypes_Sorted(t *testing.T) {
	t.Cleanup(resetRegistry)
	mustRegister(t, hashCopyStub{})
	mustRegister(t, spatialStub{})
	mustRegister(t, unregisteredStub{})

	got := RegisteredPropertyStructTypes()
	if len(got) != 3 {
		t.Fatalf("count: %d", len(got))
	}
	for i := 1; i < len(got); i++ {
		if got[i-1] > got[i] {
			t.Errorf("not sorted: %v", got)
			break
		}
	}
}

func resetRegistry() {
	propertyStructRegistryMu.Lock()
	propertyStructRegistry = make(map[reflect.Type]struct{})
	propertyStructRegistryMu.Unlock()
}
