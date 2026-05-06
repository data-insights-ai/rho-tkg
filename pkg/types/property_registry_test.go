package types

import (
	"errors"
	"math"
	"reflect"
	"testing"
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
