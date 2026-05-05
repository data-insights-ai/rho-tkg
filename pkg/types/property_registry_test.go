package types

import (
	"errors"
	"reflect"
	"testing"
)

type spatialStub struct {
	X, Y float64
}

type unregisteredStub struct {
	A int
}

func TestRegisterPropertyStructType_AcceptsValue(t *testing.T) {
	t.Cleanup(resetRegistry)
	RegisterPropertyStructType(spatialStub{})

	var ps PropertySlice
	if err := ps.Set("pt", spatialStub{X: 1, Y: 2}); err != nil {
		t.Errorf("registered value type should be accepted: %v", err)
	}
}

func TestRegisterPropertyStructType_AcceptsPointer(t *testing.T) {
	t.Cleanup(resetRegistry)
	RegisterPropertyStructType(spatialStub{})

	var ps PropertySlice
	if err := ps.Set("pt", &spatialStub{X: 1, Y: 2}); err != nil {
		t.Errorf("pointer to registered type should be accepted: %v", err)
	}
}

func TestRegisterPropertyStructType_RegisteringPointerAlsoAcceptsValue(t *testing.T) {
	t.Cleanup(resetRegistry)
	RegisterPropertyStructType((*spatialStub)(nil))

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
	RegisterPropertyStructType(spatialStub{})

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
	RegisterPropertyStructType(spatialStub{})
	RegisterPropertyStructType(spatialStub{}) // second call must not panic

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
	RegisterPropertyStructType(struct{ Z int }{})
	RegisterPropertyStructType(spatialStub{})
	RegisterPropertyStructType(unregisteredStub{})

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
