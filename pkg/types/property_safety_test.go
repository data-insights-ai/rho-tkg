package types

import (
	"errors"
	"testing"
)

// ─── Registered struct types keep DeepCopy isolation ────────────────────────
//
// PropertySlice.DeepCopy must produce a fully-independent clone for every
// allowed value. reflectCopyValue only clones an outer slice or map and
// returns a struct unchanged, so a type like Polygon{Rings []LinearRing},
// which has nested mutable state, is copied through its own DeepCopyValue
// method instead; registration requires that method (DeepCopier).
//
// Without it Store.PutNode, which deep-copies before caching, would leave a
// caller that retains a post-Put reference able to mutate a nested slice of a
// registered struct and so mutate the cached graph state — outside locks,
// outside index maintenance, outside hash recomputation.

// nestedRingsStub mirrors the shape of pkg/spatial Polygon: an outer slice
// of slices. reflectCopyValue would return the struct unchanged (it isn't a
// Slice/Map), so without DeepCopyValue the cached copy would share the
// nested slice with the caller.
type nestedRingsStub struct {
	Rings [][]int
}

// HashBytes makes nestedRingsStub a valid HashableValue so registration
// passes the hashability check and the tests below isolate deep copying.
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

// ─── Registration rejects types that cannot satisfy the contracts ───────────
//
// RegisterPropertyStructType refuses a type that does not implement
// HashableValue (ErrTypeNotHashable): such a value would reach the entity
// hash with no way to hash it. Registration is where the type's invariants
// are checked, before a single value reaches the data plane.

// notHashableStub implements DeepCopier but not HashableValue.
type notHashableStub struct {
	X int
}

// DeepCopyValue is provided so the tests isolate the hashability check from
// the deep-copy contract.
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

// ─── Registration rejects types that don't implement DeepCopier ─────────────
//
// Without a DeepCopyValue method, reflectCopyValue cannot reliably clone
// arbitrary struct internals, so registration refuses the type
// (ErrTypeNotDeepCopyable) before any value reaches a Put/Get round-trip.

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

// pointerOnlyMethods has both HashBytes and DeepCopyValue on the pointer
// receiver only. A stored value form (pointerOnlyMethods{...}) is not
// addressable, so it satisfies neither HashableValue nor DeepCopier. The
// registration check probes the form actually passed and its element form,
// so the value form is refused; callers with pointer-receiver methods
// register a typed nil pointer ((*T)(nil)) and store pointer-form values.
type pointerOnlyMethods struct {
	X int
}

func (p *pointerOnlyMethods) HashBytes() []byte  { return []byte{byte(p.X)} }
func (p *pointerOnlyMethods) DeepCopyValue() any { c := *p; return &c }

// TestF1F2_RegisterRejectsValueFormWithPointerReceiver pins that registering
// the value form of a type whose methods are on the pointer receiver refuses
// with ErrTypeNotHashable, and that the typed nil pointer form registers.
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

// TestF3_PointerRegistrationRejectsValueFormAtSet pins that after
// registering (*pointerOnlyMethods)(nil), Set refuses the value form (it
// satisfies neither HashableValue nor DeepCopier, so the entity hash could
// not hash it) and accepts the pointer form.
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
		t.Fatal("Set must reject value form when methods are pointer-receiver only")
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
