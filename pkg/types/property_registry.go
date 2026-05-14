package types

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync"
)

// ErrTypeNotHashable is returned by RegisterPropertyStructType when the
// passed-in type (or its pointer element) does not implement HashableValue.
// Without HashableValue, ComputeNodeHash / ComputeRelHash would panic the
// first time a value of this type appears in a node or relationship.
var ErrTypeNotHashable = errors.New("types: registered property type does not implement HashableValue")

// ErrTypeNotDeepCopyable is returned by RegisterPropertyStructType when the
// passed-in type (or its pointer element) does not implement DeepCopier.
// Without DeepCopier, the store boundary cannot reliably isolate caller and
// cache: nested mutable state inside the registered struct (slices, maps,
// pointers) would silently survive a Put/Get round-trip and let callers
// corrupt cached graph state outside locks and index maintenance.
var ErrTypeNotDeepCopyable = errors.New("types: registered property type does not implement DeepCopier")

// ErrPropertyTypeNameCollision is returned by RegisterPropertyStructType when
// two distinct Go types would share the same persisted custom-property wire
// name. The wire/hash format stores reflect.Type.String() for backward
// compatibility, so collisions must be rejected at registration time instead
// of being allowed to make decode and hash identity ambiguous.
var ErrPropertyTypeNameCollision = errors.New("types: registered property type name collision")

// HashableValue is the contract a custom property-value type must satisfy to
// participate in node/relationship integrity hashing. Registered via
// RegisterPropertyStructType.
//
// **Treat HashBytes like a wire format.** The returned bytes feed directly into
// node and relationship hash computation — the representation MUST be stable
// across Go versions, reboots, encoding upgrades, and library updates. Once
// any property of this type has been written into a graph, you cannot change
// HashBytes output without breaking every hash chain that contains the value.
// VerifyNodeHashChain / VerifyRelHashChain will reject existing chains as
// corrupted.
//
// Concrete requirements for a correct implementation:
//   - Deterministic: the same value MUST produce identical bytes every call,
//     regardless of process, host, OS, Go version, or registered map ordering.
//   - Stable across releases: do not switch encodings, add/reorder fields,
//     change endianness, etc. once shipped.
//   - Must never panic: graph mutation paths convert HashBytes panics into
//     errors, but the value is still invalid and direct unchecked hashing or
//     store/wire use can fail.
//   - Should not depend on caller-mutable state (timestamps, random salts,
//     unsorted maps) — same input → same bytes.
//
// For spatial types, HashBytes returns WKB (well-known binary): a fixed OGC
// standard format, deterministic for a given geometry. WKB has been stable
// since the OGC simple features spec, so it satisfies the contract.
type HashableValue interface {
	// HashBytes returns a deterministic binary representation used in node
	// and relationship hashing. Not called on the value's JSON/MsgPack/
	// printable form — only from integrity hashing. Must never panic.
	HashBytes() []byte
}

// DeepCopier is the contract a custom property-value type must satisfy so
// that PropertySlice.DeepCopy can produce a fully-independent clone of the
// value. PropertySlice.DeepCopy underpins the store boundary: PutNode /
// PutRelationship deep-copy before caching and Get* deep-copy on return,
// so the in-cache representation is isolated from the caller. A registered
// struct/pointer type with nested mutable state (slices, maps, embedded
// pointers) MUST implement DeepCopier; otherwise reflectCopyValue clones
// only the outer container and the nested state survives the boundary,
// letting callers silently corrupt cached graph state outside locks and
// index maintenance.
//
// For value types whose fields are all primitives (e.g. a Point with two
// float64 coordinates), a trivial implementation suffices:
//
//	func (p Point) DeepCopyValue() any { return p }
//
// For pointer receivers, DeepCopyValue should return the same kind (value
// or pointer) the caller passed in — the property layer round-trips the
// returned any directly back into PropertySlice and downstream consumers.
type DeepCopier interface {
	// DeepCopyValue returns a deep copy of the receiver. The returned value
	// MUST share no mutable state with the receiver. Must never panic.
	DeepCopyValue() any
}

// propertyStructRegistry holds struct types that external packages have
// registered as acceptable property values. By default, PropertySlice rejects
// anything outside primitives, slices, and maps — spatial geometry, custom
// domain types, and similar need an explicit opt-in via RegisterPropertyStructType.
//
// This is a package-level registry on purpose. Calls happen at init() time
// of the registering package (e.g. tkgd/pkg/spatial), so they are effectively
// one-shot and visible to every subsequent PropertySlice.Set call.
var (
	propertyStructRegistryMu sync.RWMutex
	propertyStructRegistry   = make(map[reflect.Type]struct{})
)

// RegisterPropertyStructType declares that values of v's type (and pointer-to
// that type) are valid property values. Call from an init() in packages that
// ship first-class custom types. Persistent stores encode registered custom
// values through the graph wire format as a registered type name plus a MsgPack
// payload; the value must be MsgPack-encodable and must unmarshal back to the
// same HashBytes representation.
//
// Pass either a zero value or a typed pointer — both forms (value and
// pointer-to) become acceptable. Untyped nil is rejected because it does not
// identify a type to register. Registering the same type twice is a no-op.
//
// The type MUST implement both HashableValue (so integrity hashing works
// without panicking) and DeepCopier (so the store boundary remains a true
// trust boundary). Registration fails loudly with ErrTypeNotHashable or
// ErrTypeNotDeepCopyable rather than letting the bug reach the data plane.
//
// Intended for tkgd-bundled packages like pkg/spatial. Third-party callers
// are welcome, but review carefully: accepting arbitrary struct types in
// properties widens the trust surface (serialisation, hashing, deep-copy
// semantics must all hold).
func RegisterPropertyStructType(v any) error {
	if v == nil {
		return fmt.Errorf("%w: nil property struct registration", ErrUnsupportedValueType)
	}
	t := reflect.TypeOf(v)
	elemT := t
	if elemT.Kind() == reflect.Pointer {
		elemT = elemT.Elem()
	}
	if elemT.Kind() != reflect.Struct {
		return fmt.Errorf("%w: registered property type must be a struct, got %s", ErrUnsupportedValueType, elemT.String())
	}
	// Verify both contracts against the FORM ACTUALLY PASSED. Including
	// reflect.PointerTo(elemT).Implements(...) here would silently accept
	// a value form (T{}) when methods are defined on the pointer receiver
	// (*T) only — registration succeeds, but values stored via ps.Set(T{})
	// are not addressable, so the runtime type-assert to HashableValue /
	// DeepCopier returns ok=false and we fall back to the panic / shallow
	// copy paths the registration check was supposed to prevent. Callers
	// who define methods on a pointer receiver MUST register the typed
	// nil pointer (e.g. RegisterPropertyStructType((*MyType)(nil))).
	hashable := t.Implements(hashableValueType) ||
		elemT.Implements(hashableValueType)
	if !hashable {
		return fmt.Errorf("%w: %s", ErrTypeNotHashable, elemT.String())
	}
	deepCopier := t.Implements(deepCopierType) ||
		elemT.Implements(deepCopierType)
	if !deepCopier {
		return fmt.Errorf("%w: %s", ErrTypeNotDeepCopyable, elemT.String())
	}
	propertyStructRegistryMu.Lock()
	for existing := range propertyStructRegistry {
		if existing != elemT && existing.String() == elemT.String() {
			propertyStructRegistryMu.Unlock()
			return fmt.Errorf("%w: %s already registered from %q; cannot also register %q",
				ErrPropertyTypeNameCollision, elemT.String(), existing.PkgPath(), elemT.PkgPath())
		}
	}
	propertyStructRegistry[elemT] = struct{}{}
	propertyStructRegistryMu.Unlock()
	return nil
}

// hashableValueType and deepCopierType are precomputed reflect.Type values
// for the two contracts every registered property type must satisfy. Used
// by RegisterPropertyStructType for the implements check.
var (
	hashableValueType = reflect.TypeOf((*HashableValue)(nil)).Elem()
	deepCopierType    = reflect.TypeOf((*DeepCopier)(nil)).Elem()
)

// isRegisteredPropertyStructType reports whether rv's type (or the element
// type if rv is a pointer) has been registered via RegisterPropertyStructType
// AND whether the runtime form actually implements HashableValue + DeepCopier.
//
// The interface check is the F3 fix: registering the typed-nil pointer of a
// type whose methods live on the pointer receiver only (`(*T)(nil)`) records
// elemT in the registry, but a later `ps.Set("x", T{})` would otherwise be
// accepted by the type-level lookup. The stored value would then fail the
// runtime `v.(HashableValue)` assertion in appendPropertyValue and trip the
// panic branch. Checking the runtime form here fails fast at validation time
// instead, so the bug never reaches the hash/wire layer.
func isRegisteredPropertyStructType(rv reflect.Value) bool {
	t := rv.Type()
	if t.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return false
		}
		t = t.Elem()
	}
	propertyStructRegistryMu.RLock()
	_, ok := propertyStructRegistry[t]
	propertyStructRegistryMu.RUnlock()
	if !ok {
		return false
	}
	// rv.Interface() requires an exported, addressable value. validateReflectValue
	// is invoked from the reflect.ValueOf(propValue) entry point so rv is
	// addressable for any value the caller supplies through Set; the assertion
	// returns ok=false for pointer-receiver methods on a non-pointer rv,
	// which is exactly what we need to catch.
	iv := rv.Interface()
	if _, hashable := iv.(HashableValue); !hashable {
		return false
	}
	if _, copyable := iv.(DeepCopier); !copyable {
		return false
	}
	return true
}

// RegisteredPropertyStructTypes returns the names of all registered custom
// property types in lexicographic order. For admin and diagnostic use.
func RegisteredPropertyStructTypes() []string {
	propertyStructRegistryMu.RLock()
	out := make([]string, 0, len(propertyStructRegistry))
	for t := range propertyStructRegistry {
		out = append(out, t.String())
	}
	propertyStructRegistryMu.RUnlock()
	sort.Strings(out)
	return out
}

// RegisteredPropertyStructWireType reports the registered element type name for
// v and whether the runtime value was a pointer. It returns ok=false for nil,
// unregistered, or runtime forms that do not satisfy the custom property
// contracts.
func RegisteredPropertyStructWireType(v any) (typeName string, pointer bool, ok bool) {
	if v == nil {
		return "", false, false
	}
	rv := reflect.ValueOf(v)
	if !isRegisteredPropertyStructType(rv) {
		return "", false, false
	}
	t := rv.Type()
	if t.Kind() == reflect.Pointer {
		return t.Elem().String(), true, true
	}
	return t.String(), false, true
}

// NewRegisteredPropertyStructPointer creates a pointer to the registered
// custom property struct type named by typeName. The returned value is suitable
// as a MsgPack unmarshal target.
func NewRegisteredPropertyStructPointer(typeName string) (any, bool) {
	propertyStructRegistryMu.RLock()
	defer propertyStructRegistryMu.RUnlock()
	for t := range propertyStructRegistry {
		if t.String() == typeName {
			return reflect.New(t).Interface(), true
		}
	}
	return nil, false
}
