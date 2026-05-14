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

func TestPropertySliceSetPreservesPointerShapeForValueReceiverCustomType(t *testing.T) {
	t.Cleanup(resetRegistry)
	if err := RegisterPropertyStructType(nestedRingsStub{}); err != nil {
		t.Fatalf("RegisterPropertyStructType: %v", err)
	}

	original := &nestedRingsStub{Rings: [][]int{{1, 2}, {3, 4}}}
	var ps PropertySlice
	if err := ps.Set("geom", original); err != nil {
		t.Fatalf("Set: %v", err)
	}

	original.Rings[0][0] = 99
	got, ok := ps.Get("geom")
	if !ok {
		t.Fatal("Get(\"geom\") missing")
	}
	cpPtr, ok := got.(*nestedRingsStub)
	if !ok {
		t.Fatalf("stored custom value type = %T, want *nestedRingsStub", got)
	}
	if cpPtr == original {
		t.Fatal("Set stored original custom pointer")
	}
	if cpPtr.Rings[0][0] != 1 {
		t.Fatalf("Set retained caller nested alias: %v", cpPtr.Rings)
	}
}

type valueReturnsPointerCopyStub struct {
	Items []int
}

func (v valueReturnsPointerCopyStub) HashBytes() []byte { return []byte{byte(len(v.Items))} }
func (v valueReturnsPointerCopyStub) DeepCopyValue() any {
	cp := valueReturnsPointerCopyStub{Items: append([]int(nil), v.Items...)}
	return &cp
}

func TestPropertySliceSetPreservesValueShapeForCustomType(t *testing.T) {
	t.Cleanup(resetRegistry)
	if err := RegisterPropertyStructType(valueReturnsPointerCopyStub{}); err != nil {
		t.Fatalf("RegisterPropertyStructType: %v", err)
	}

	original := valueReturnsPointerCopyStub{Items: []int{1, 2, 3}}
	var ps PropertySlice
	if err := ps.Set("custom", original); err != nil {
		t.Fatalf("Set: %v", err)
	}

	original.Items[0] = 99
	got, ok := ps.Get("custom")
	if !ok {
		t.Fatal("Get(\"custom\") missing")
	}
	cpValue, ok := got.(valueReturnsPointerCopyStub)
	if !ok {
		t.Fatalf("stored custom value type = %T, want valueReturnsPointerCopyStub", got)
	}
	if cpValue.Items[0] != 1 {
		t.Fatalf("Set retained caller nested alias: %v", cpValue.Items)
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

func (p *pointerOnlyMethods) HashBytes() []byte  { return []byte{byte(p.X)} }
func (p *pointerOnlyMethods) DeepCopyValue() any { c := *p; return &c }

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

// TestF3_PointerRegistrationRejectsValueFormAtSet covers F3 from the
// maintainability review. Registering `(*pointerOnlyMethods)(nil)` is
// correct (the type's methods are on the pointer receiver), but the
// pre-fix isRegisteredPropertyStructType only checked the element type
// against the registry, so a later ps.Set("x", pointerOnlyMethods{}) was
// silently accepted — and would later panic in the hash path because the
// non-addressable value form does not satisfy v.(HashableValue).
//
// Post-fix, validateReflectValue must reject the value form even after
// pointer-form registration. Pointer-form Set must still succeed.
func TestF3_PointerRegistrationRejectsValueFormAtSet(t *testing.T) {
	t.Cleanup(resetRegistry)
	if err := RegisterPropertyStructType((*pointerOnlyMethods)(nil)); err != nil {
		t.Fatalf("registering pointer form: %v", err)
	}

	var ps PropertySlice

	// Value form: must be rejected at validation time. Even though the
	// element type is registered, the value form (T{}) does not satisfy
	// HashableValue / DeepCopier — only *T does — so accepting it would
	// admit a property that later panics during entity hashing.
	if err := ps.Set("bad", pointerOnlyMethods{X: 1}); err == nil {
		t.Fatal("Set must reject value form when methods are pointer-receiver only (F3)")
	}

	// Pointer form: must still be accepted.
	if err := ps.Set("ok", &pointerOnlyMethods{X: 2}); err != nil {
		t.Errorf("Set rejected valid pointer-form value: %v", err)
	}
}

type badCopyValueStub struct{}

func (b badCopyValueStub) HashBytes() []byte { return []byte("bad-copy") }
func (b badCopyValueStub) DeepCopyValue() any {
	return map[string]int{"unsupported": 1}
}

func TestPropertySliceRejectsUnsupportedDeepCopyResult(t *testing.T) {
	t.Cleanup(resetRegistry)
	if err := RegisterPropertyStructType(badCopyValueStub{}); err != nil {
		t.Fatalf("RegisterPropertyStructType: %v", err)
	}

	var ps PropertySlice
	err := ps.Set("bad", badCopyValueStub{})
	if !errors.Is(err, ErrUnsupportedMapType) {
		t.Fatalf("Set error = %v, want ErrUnsupportedMapType", err)
	}
	if ps.Len() != 0 {
		t.Fatalf("Set stored unsupported deep-copy result: %v", ps)
	}

	_, err = NewPropertySlice(map[string]any{"bad": badCopyValueStub{}})
	if !errors.Is(err, ErrUnsupportedMapType) {
		t.Fatalf("NewPropertySlice error = %v, want ErrUnsupportedMapType", err)
	}

	n := NewNode(1, 1, nil)
	err = n.SetProperties(PropertySlice{{Key: "bad", Value: badCopyValueStub{}}})
	if !errors.Is(err, ErrUnsupportedMapType) {
		t.Fatalf("Node.SetProperties error = %v, want ErrUnsupportedMapType", err)
	}
	if n.Properties().Len() != 0 {
		t.Fatalf("Node.SetProperties stored unsupported deep-copy result: %v", n.Properties())
	}
}

type panicCopyValueStub struct{}

func (p panicCopyValueStub) HashBytes() []byte { return []byte("panic-copy") }
func (p panicCopyValueStub) DeepCopyValue() any {
	panic("copy failed")
}

func TestPropertySliceRejectsPanickingDeepCopyResult(t *testing.T) {
	t.Cleanup(resetRegistry)
	if err := RegisterPropertyStructType(panicCopyValueStub{}); err != nil {
		t.Fatalf("RegisterPropertyStructType: %v", err)
	}

	var ps PropertySlice
	err := ps.Set("panic", panicCopyValueStub{})
	if !errors.Is(err, ErrUnsupportedValueType) {
		t.Fatalf("Set error = %v, want ErrUnsupportedValueType", err)
	}
	if ps.Len() != 0 {
		t.Fatalf("Set stored value after DeepCopyValue panic: %v", ps)
	}
}

type nilCopyValueStub struct{}

func (n nilCopyValueStub) HashBytes() []byte  { return []byte("nil-copy") }
func (n nilCopyValueStub) DeepCopyValue() any { return nil }

func TestPropertySliceRejectsNilDeepCopyResult(t *testing.T) {
	t.Cleanup(resetRegistry)
	if err := RegisterPropertyStructType(nilCopyValueStub{}); err != nil {
		t.Fatalf("RegisterPropertyStructType: %v", err)
	}

	var ps PropertySlice
	err := ps.Set("nil-copy", nilCopyValueStub{})
	if !errors.Is(err, ErrUnsupportedValueType) {
		t.Fatalf("Set error = %v, want ErrUnsupportedValueType", err)
	}
	if ps.Len() != 0 {
		t.Fatalf("Set stored value after nil DeepCopyValue result: %v", ps)
	}

	_, err = NewPropertySlice(map[string]any{"nil-copy": nilCopyValueStub{}})
	if !errors.Is(err, ErrUnsupportedValueType) {
		t.Fatalf("NewPropertySlice error = %v, want ErrUnsupportedValueType", err)
	}

	n := NewNode(1, 1, nil)
	err = n.SetProperties(PropertySlice{{Key: "nil-copy", Value: nilCopyValueStub{}}})
	if !errors.Is(err, ErrUnsupportedValueType) {
		t.Fatalf("Node.SetProperties error = %v, want ErrUnsupportedValueType", err)
	}
	if n.Properties().Len() != 0 {
		t.Fatalf("Node.SetProperties stored value after nil DeepCopyValue result: %v", n.Properties())
	}

	r := NewRelationship(1, 1, 2, 3)
	err = r.SetProperties(PropertySlice{{Key: "nil-copy", Value: nilCopyValueStub{}}})
	if !errors.Is(err, ErrUnsupportedValueType) {
		t.Fatalf("Relationship.SetProperties error = %v, want ErrUnsupportedValueType", err)
	}
	if r.Properties().Len() != 0 {
		t.Fatalf("Relationship.SetProperties stored value after nil DeepCopyValue result: %v", r.Properties())
	}
}

func TestPropertySliceRejectsNestedNilDeepCopyResult(t *testing.T) {
	t.Cleanup(resetRegistry)
	if err := RegisterPropertyStructType(nilCopyValueStub{}); err != nil {
		t.Fatalf("RegisterPropertyStructType: %v", err)
	}

	var ps PropertySlice
	err := ps.Set("nested", []any{nilCopyValueStub{}})
	if !errors.Is(err, ErrUnsupportedValueType) {
		t.Fatalf("Set nested error = %v, want ErrUnsupportedValueType", err)
	}
	if ps.Len() != 0 {
		t.Fatalf("Set stored nested value after nil DeepCopyValue result: %v", ps)
	}

	_, err = NewPropertySlice(map[string]any{"nested": map[string]any{"custom": nilCopyValueStub{}}})
	if !errors.Is(err, ErrUnsupportedValueType) {
		t.Fatalf("NewPropertySlice nested error = %v, want ErrUnsupportedValueType", err)
	}
}

type otherCopyValueStub struct{}

func (o otherCopyValueStub) HashBytes() []byte { return []byte("other-copy") }
func (o otherCopyValueStub) DeepCopyValue() any {
	return nestedRingsStub{Rings: [][]int{{1, 2, 3}}}
}

func TestPropertySliceRejectsTypeChangingDeepCopyResult(t *testing.T) {
	t.Cleanup(resetRegistry)
	if err := RegisterPropertyStructType(otherCopyValueStub{}); err != nil {
		t.Fatalf("RegisterPropertyStructType other: %v", err)
	}
	if err := RegisterPropertyStructType(nestedRingsStub{}); err != nil {
		t.Fatalf("RegisterPropertyStructType nested: %v", err)
	}

	var ps PropertySlice
	err := ps.Set("custom", otherCopyValueStub{})
	if !errors.Is(err, ErrUnsupportedValueType) {
		t.Fatalf("Set error = %v, want ErrUnsupportedValueType", err)
	}
	if ps.Len() != 0 {
		t.Fatalf("Set stored value after type-changing DeepCopyValue result: %v", ps)
	}
}

type unregisteredPanicCopyValueStub struct{}

func (p unregisteredPanicCopyValueStub) DeepCopyValue() any {
	panic("unregistered copy should not run")
}

func TestPropertySliceAccessorsDoNotInvokeUnregisteredDeepCopier(t *testing.T) {
	t.Cleanup(resetRegistry)
	resetRegistry()

	ps := PropertySlice{{Key: "bad", Value: unregisteredPanicCopyValueStub{}}}

	assertNoPanic := func(name string, fn func()) {
		t.Helper()
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("%s panicked by invoking unregistered DeepCopier: %v", name, r)
			}
		}()
		fn()
	}

	assertNoPanic("Get", func() {
		got, ok := ps.Get("bad")
		if !ok {
			t.Fatal("Get missing manually constructed property")
		}
		if _, ok := got.(unregisteredPanicCopyValueStub); !ok {
			t.Fatalf("Get returned %T, want unregisteredPanicCopyValueStub", got)
		}
	})
	assertNoPanic("DeepCopy", func() {
		cp := ps.DeepCopy()
		if len(cp) != 1 {
			t.Fatalf("DeepCopy len = %d, want 1", len(cp))
		}
		if _, ok := cp[0].Value.(unregisteredPanicCopyValueStub); !ok {
			t.Fatalf("DeepCopy value type = %T, want unregisteredPanicCopyValueStub", cp[0].Value)
		}
	})
	assertNoPanic("ToMap", func() {
		m := ps.ToMap()
		if _, ok := m["bad"].(unregisteredPanicCopyValueStub); !ok {
			t.Fatalf("ToMap value type = %T, want unregisteredPanicCopyValueStub", m["bad"])
		}
	})
}
