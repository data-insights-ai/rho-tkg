package types

import (
	"errors"
	"testing"
)

// ─── F1 RED: registered struct types break DeepCopy isolation ──────────────
//
// PropertySlice.DeepCopy must produce a fully-independent clone for every
// allowed value. Registered struct/pointer types currently fall through to
// reflectCopyValue, which only clones the outer slice/map but does NOT
// recurse into struct internals. A type like Polygon{Rings []LinearRing}
// has nested mutable state that survives the copy boundary.
//
// This is a real correctness bug: Store.PutNode deep-copies before caching,
// so a caller that retains a post-Put reference and mutates a nested slice
// of a registered struct mutates the cached graph state — outside locks,
// outside index maintenance, outside hash recomputation.

// nestedRingsStub mirrors the shape of pkg/spatial Polygon: an outer slice
// of slices. reflectCopyValue's existing default branch returns the value
// unchanged (it isn't a Slice/Map), so the cached copy ends up sharing the
// nested slice with the caller.
type nestedRingsStub struct {
	Rings [][]int
}

// HashBytes makes nestedRingsStub a valid HashableValue so registration
// passes the F2 hashability check; this test isolates the F1 deep-copy bug.
func (n nestedRingsStub) HashBytes() []byte {
	return []byte("nestedRingsStub")
}

// DeepCopyValue clones the nested rings so DeepCopy isolation holds.
func (n nestedRingsStub) DeepCopyValue() any {
	cp := nestedRingsStub{Rings: make([][]int, len(n.Rings))}
	for i, r := range n.Rings {
		row := make([]int, len(r))
		copy(row, r)
		cp.Rings[i] = row
	}
	return cp
}

func TestF1_DeepCopyIsolatesRegisteredStructInternals(t *testing.T) {
	t.Cleanup(resetRegistry)
	if err := RegisterPropertyStructType(nestedRingsStub{}); err != nil {
		t.Fatalf("RegisterPropertyStructType: %v", err)
	}

	original := nestedRingsStub{Rings: [][]int{{1, 2, 3}, {4, 5, 6}}}

	var ps PropertySlice
	if err := ps.Set("geom", original); err != nil {
		t.Fatalf("Set: %v", err)
	}

	cp := ps.DeepCopy()

	// Mutate the original's nested ring after copy. The copy must NOT see
	// the mutation — that's the deep-copy contract.
	original.Rings[0][0] = 99

	got, ok := cp.Get("geom")
	if !ok {
		t.Fatal("Get(\"geom\") missing in copy")
	}
	cpStub, ok := got.(nestedRingsStub)
	if !ok {
		t.Fatalf("type after copy: got %T, want nestedRingsStub", got)
	}
	if cpStub.Rings[0][0] == 99 {
		t.Fatal("DeepCopy: mutating original's nested slice leaked into the copy " +
			"(reflectCopyValue does not recurse into struct internals)")
	}
}

// pointerRingsStub mirrors nestedRingsStub but uses a pointer receiver for
// DeepCopyValue so the copy round-trips back as *pointerRingsStub. This
// asserts that the framework respects the registered type's chosen
// receiver shape — value receivers return values, pointer receivers
// return pointers.
type pointerRingsStub struct {
	Rings [][]int
}

func (p *pointerRingsStub) HashBytes() []byte { return []byte("pointerRingsStub") }

func (p *pointerRingsStub) DeepCopyValue() any {
	cp := &pointerRingsStub{Rings: make([][]int, len(p.Rings))}
	for i, r := range p.Rings {
		row := make([]int, len(r))
		copy(row, r)
		cp.Rings[i] = row
	}
	return cp
}

func TestF1_DeepCopyIsolatesRegisteredPointerStructInternals(t *testing.T) {
	t.Cleanup(resetRegistry)
	if err := RegisterPropertyStructType((*pointerRingsStub)(nil)); err != nil {
		t.Fatalf("RegisterPropertyStructType: %v", err)
	}

	original := &pointerRingsStub{Rings: [][]int{{10, 20}, {30, 40}}}

	var ps PropertySlice
	if err := ps.Set("geom", original); err != nil {
		t.Fatalf("Set: %v", err)
	}

	cp := ps.DeepCopy()

	// Mutate via the original pointer. The copy must remain independent.
	original.Rings[1][0] = 999

	got, ok := cp.Get("geom")
	if !ok {
		t.Fatal("Get(\"geom\") missing in copy")
	}
	cpPtr, ok := got.(*pointerRingsStub)
	if !ok {
		t.Fatalf("type after copy: got %T, want *pointerRingsStub", got)
	}
	if cpPtr.Rings[1][0] == 999 {
		t.Fatal("DeepCopy: mutating original's nested slice via pointer leaked into the copy")
	}
	if cpPtr == original {
		t.Fatal("DeepCopy: copy still shares pointer identity with original")
	}
}

// ─── F2 RED: registration must reject types that cannot satisfy the contracts ──
//
// Today RegisterPropertyStructType returns nothing and accepts anything.
// A type that doesn't implement HashableValue is happily accepted; the bug
// surfaces later as a panic from appendPropertyValue's default branch when
// AddNode/AddRelationship hash the entity. Registration is the right place
// to fail loudly — the type's invariants must hold before a single value
// reaches the data plane.

// notHashableStub is registrable today (the function takes any), but
// triggers a panic in appendPropertyValue when used as a node property.
type notHashableStub struct {
	X int
}

// DeepCopyValue is provided so this test isolates the F2 hashability check
// from the F1 deep-copy contract.
func (n notHashableStub) DeepCopyValue() any { return n }

func TestF2_RegisterRejectsNonHashableType(t *testing.T) {
	t.Cleanup(resetRegistry)

	err := RegisterPropertyStructType(notHashableStub{})
	if err == nil {
		t.Fatal("RegisterPropertyStructType should reject types that do not implement HashableValue")
	}
	if !errors.Is(err, ErrTypeNotHashable) {
		t.Errorf("errors.Is(err, ErrTypeNotHashable) = false; err = %v", err)
	}
}

func TestF2_RegisterRejectsNonHashablePointerType(t *testing.T) {
	t.Cleanup(resetRegistry)

	err := RegisterPropertyStructType((*notHashableStub)(nil))
	if err == nil {
		t.Fatal("RegisterPropertyStructType should reject pointer types whose element does not implement HashableValue")
	}
	if !errors.Is(err, ErrTypeNotHashable) {
		t.Errorf("errors.Is(err, ErrTypeNotHashable) = false; err = %v", err)
	}
}

// ─── F1+F2: registration must reject types that don't implement DeepCopier ──
//
// Without a DeepCopyValue method, reflectCopyValue cannot reliably clone
// arbitrary struct internals. Reject at registration so the bug never
// reaches a Put*-Get* round-trip.

type hashableButNotCopyable struct {
	Rings [][]int
}

func (h hashableButNotCopyable) HashBytes() []byte { return []byte("hashable") }

func TestF1_RegisterRejectsNonDeepCopyableType(t *testing.T) {
	t.Cleanup(resetRegistry)

	err := RegisterPropertyStructType(hashableButNotCopyable{})
	if err == nil {
		t.Fatal("RegisterPropertyStructType should reject types that do not implement DeepCopier")
	}
	if !errors.Is(err, ErrTypeNotDeepCopyable) {
		t.Errorf("errors.Is(err, ErrTypeNotDeepCopyable) = false; err = %v", err)
	}
}

// pointerOnlyMethods has BOTH HashBytes and DeepCopyValue defined on the
// pointer receiver only. Registering the *value* form of this type must
// be REJECTED — at runtime, ps.Set(pointerOnlyMethods{...}) stores a
// non-addressable value, so v.(HashableValue) returns ok=false and the
// hash path falls back to the panic branch. Same hole for DeepCopier.
//
// Pre-fix the registration check used reflect.PointerTo(elemT).Implements
// in the OR — accepting the value form when methods existed only on the
// pointer receiver. Post-fix the check probes only the form actually
// passed (t) and the elem form (elemT) — both must satisfy the contract
// against the form the caller will store. Callers with pointer-receiver
// methods MUST register a typed nil pointer ((*T)(nil)) so the stored
// values are also pointer-form.
type pointerOnlyMethods struct {
	X int
}

func (p *pointerOnlyMethods) HashBytes() []byte                  { return []byte{byte(p.X)} }
func (p *pointerOnlyMethods) DeepCopyValue() any                 { c := *p; return &c }

// TestF1F2_RegisterRejectsValueFormWithPointerReceiver verifies the
// post-review fix to the registration check. Pre-fix this test passed
// (registration silently accepted), then a runtime ps.Set + hash path
// would panic. Post-fix the registration itself returns
// ErrTypeNotHashable so the bug never reaches the data plane.
func TestF1F2_RegisterRejectsValueFormWithPointerReceiver(t *testing.T) {
	t.Cleanup(resetRegistry)
	// Value form passed, but methods are on *pointerOnlyMethods only.
	err := RegisterPropertyStructType(pointerOnlyMethods{})
	if err == nil {
		t.Fatal("registration must reject value form when only the pointer receiver implements the contract; otherwise ps.Set(value) stores a non-addressable copy and the hash path panics at runtime")
	}
	if !errors.Is(err, ErrTypeNotHashable) {
		t.Fatalf("got %v, want ErrTypeNotHashable", err)
	}

	// Registering the typed nil pointer ((*T)(nil)) is the correct path
	// for pointer-receiver methods and must succeed.
	if err := RegisterPropertyStructType((*pointerOnlyMethods)(nil)); err != nil {
		t.Fatalf("registering pointer form should succeed: %v", err)
	}
}

// Acceptance test: a type that implements both contracts registers cleanly.
func TestF1F2_RegisterAcceptsHashableAndCopyableType(t *testing.T) {
	t.Cleanup(resetRegistry)
	if err := RegisterPropertyStructType(nestedRingsStub{}); err != nil {
		t.Fatalf("RegisterPropertyStructType should accept hashable+copyable type: %v", err)
	}
	// Both forms (value and pointer) should be accepted as property values.
	var ps PropertySlice
	if err := ps.Set("a", nestedRingsStub{Rings: [][]int{{1}}}); err != nil {
		t.Errorf("value form rejected: %v", err)
	}
	if err := ps.Set("b", &nestedRingsStub{Rings: [][]int{{2}}}); err != nil {
		t.Errorf("pointer form rejected: %v", err)
	}
}
