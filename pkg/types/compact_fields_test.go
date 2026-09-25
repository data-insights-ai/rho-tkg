package types

import (
	"reflect"
	"strings"
	"testing"
)

// Guard against the silent data-loss trap of the compact form (P7): the
// compact frozen copy rebuilds TemporalMetadata, RelIntegrity and
// NodeIntegrity from a hand-written subset of their fields, and
// CompactFrozenCopy copies the entity structs field by field. A field added
// to any of them and not taught to compact.go would be dropped from every
// store-cached first version without an error.
//
// These tests enumerate the fields by reflection, so a new field is covered
// the day it is added: for every field and every value in its kind's value
// set, a row whose metadata differs from a compact-fitting first version in
// exactly that field must come back from CompactFrozenCopy with that field
// intact — either carried by the compact form or rejected into the ordinary
// form. A field of a kind the value sets do not cover, or an unexported field
// without a setter below, fails the test naming the field.

// unexportedMetaSetters sets the unexported metadata fields the tests cannot
// reach by reflection. A new unexported field fails until it is listed here.
var unexportedMetaSetters = map[string]func(v reflect.Value, i int){
	"TemporalMetadata.baseEntityID": func(v reflect.Value, i int) {
		v.Addr().Interface().(*TemporalMetadata).SetBaseEntityID(EntityID(7 + i))
	},
}

// fieldValues returns the values a field of kind k is tried with, or nil
// when the kind is not covered.
func fieldValues(t reflect.Type) []any {
	switch t.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return []any{int64(1), int64(-5), int64(1_787_443_299_999)}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return []any{uint64(1), uint64(200)}
	case reflect.String:
		// Canonical hash, non-canonical hash, plain text, empty.
		return []any{testHashB, strings.ToUpper(testHashB), "x", ""}
	case reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			return []any{[]byte{}, []byte{1, 2}}
		}
	case reflect.Bool:
		return []any{true}
	}
	return nil
}

// forEachFieldVariant calls fn once per (field, value) of the struct type of
// *base, with a copy of *base whose field was set to that value.
func forEachFieldVariant[T any](t *testing.T, base func() *T, fn func(name string, variant *T)) {
	t.Helper()
	typ := reflect.TypeFor[T]()
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		name := typ.Name() + "." + f.Name
		if !f.IsExported() {
			set, ok := unexportedMetaSetters[name]
			if !ok {
				t.Errorf("%s: unexported field with no setter in unexportedMetaSetters — teach compact.go (fit check and rebuild) and this test about it", name)
				continue
			}
			for k := 0; k < 2; k++ {
				v := base()
				set(reflect.ValueOf(v).Elem(), k)
				fn(name, v)
			}
			continue
		}
		vals := fieldValues(f.Type)
		if vals == nil {
			t.Errorf("%s: kind %s has no value set in fieldValues — teach compact.go (fit check and rebuild) and this test about it", name, f.Type)
			continue
		}
		for _, val := range vals {
			v := base()
			fv := reflect.ValueOf(v).Elem().Field(i)
			fv.Set(reflect.ValueOf(val).Convert(f.Type))
			fn(name, v)
		}
	}
}

func baseRelIntegrity() *RelIntegrity {
	return &RelIntegrity{Hash: testHashA, FromNodeHash: testHashB, ToNodeHash: testHashC}
}

func baseNodeIntegrity() *NodeIntegrity { return &NodeIntegrity{Hash: testHashA} }

// checkRelSurvives compacts a relationship with tm and ig and fails, naming
// field, when the compact copy does not hand back the same metadata.
func checkRelSurvives(t *testing.T, field string, tm *TemporalMetadata, ig *RelIntegrity) {
	t.Helper()
	r := buildCompactTestRel(t, compactRelCase{tm: tm, ig: ig})
	cp := r.CompactFrozenCopy()
	if !reflect.DeepEqual(cp.Temporal(), r.Temporal()) {
		t.Errorf("%s: compact relationship dropped temporal data (compact form used: %t): got %+v, want %+v", field, cp.meta != nil, cp.Temporal(), r.Temporal())
	}
	if !reflect.DeepEqual(cp.Integrity(), r.Integrity()) {
		t.Errorf("%s: compact relationship dropped integrity data (compact form used: %t): got %+v, want %+v", field, cp.meta != nil, cp.Integrity(), r.Integrity())
	}
	if !reflect.DeepEqual(cp.DeepCopy(), r.DeepCopy()) {
		t.Errorf("%s: thawed compact relationship differs from the source", field)
	}
}

func checkNodeSurvives(t *testing.T, field string, tm *TemporalMetadata, ig *NodeIntegrity) {
	t.Helper()
	n := buildCompactTestNode(t, compactNodeCase{tm: tm, ig: ig})
	cp := n.CompactFrozenCopy()
	if !reflect.DeepEqual(cp.Temporal(), n.Temporal()) {
		t.Errorf("%s: compact node dropped temporal data (compact form used: %t): got %+v, want %+v", field, cp.meta != nil, cp.Temporal(), n.Temporal())
	}
	if !reflect.DeepEqual(cp.Integrity(), n.Integrity()) {
		t.Errorf("%s: compact node dropped integrity data (compact form used: %t): got %+v, want %+v", field, cp.meta != nil, cp.Integrity(), n.Integrity())
	}
	if !reflect.DeepEqual(cp.DeepCopy(), n.DeepCopy()) {
		t.Errorf("%s: thawed compact node differs from the source", field)
	}
}

// TestCompactFormDropsNoMetadataField: every field of TemporalMetadata,
// RelIntegrity and NodeIntegrity survives CompactFrozenCopy for every value
// in its value set (carried by the compact form or kept in the ordinary one).
func TestCompactFormDropsNoMetadataField(t *testing.T) {
	// The base values must themselves be compact, or the test proves nothing.
	if cp := buildCompactTestRel(t, compactRelCase{tm: firstVersionTM(), ig: baseRelIntegrity()}).CompactFrozenCopy(); cp.meta == nil {
		t.Fatal("base relationship is not compact")
	}
	if cp := buildCompactTestNode(t, compactNodeCase{tm: firstVersionTM(), ig: baseNodeIntegrity()}).CompactFrozenCopy(); cp.meta == nil {
		t.Fatal("base node is not compact")
	}
	forEachFieldVariant(t, firstVersionTM, func(name string, tm *TemporalMetadata) {
		checkRelSurvives(t, name, tm, baseRelIntegrity())
		checkNodeSurvives(t, name, tm, baseNodeIntegrity())
	})
	forEachFieldVariant(t, baseRelIntegrity, func(name string, ig *RelIntegrity) {
		checkRelSurvives(t, name, firstVersionTM(), ig)
	})
	forEachFieldVariant(t, baseNodeIntegrity, func(name string, ig *NodeIntegrity) {
		checkNodeSurvives(t, name, firstVersionTM(), ig)
	})
}

// compactCopiedFields are the entity fields CompactFrozenCopy handles, each
// with how. A field added to Relationship or Node fails
// TestCompactFrozenCopyHandlesEveryEntityField until it is copied there (and
// in DeepCopy) and listed here.
var compactCopiedFields = map[string]string{
	"Relationship.id":          "copied",
	"Relationship.startID":     "copied",
	"Relationship.endID":       "copied",
	"Relationship.properties":  "shared (copy on write)",
	"Relationship.temporal":    "compacted or copied",
	"Relationship.integrity":   "compacted or deep-copied",
	"Relationship.meta":        "reused (immutable)",
	"Relationship.version":     "copied",
	"Relationship.relType":     "copied",
	"Relationship.frozen":      "set",
	"Relationship.sharedProps": "false on the copy, set on an unfrozen source",
	"Node.id":                  "copied",
	"Node.properties":          "shared (copy on write)",
	"Node.extraLabels":         "copied",
	"Node.temporal":            "compacted or copied",
	"Node.integrity":           "compacted or deep-copied",
	"Node.meta":                "reused (immutable)",
	"Node.version":             "copied",
	"Node.primaryLabel":        "copied",
	"Node.frozen":              "set",
	"Node.sharedProps":         "false on the copy, set on an unfrozen source",
}

// TestCompactFrozenCopyHandlesEveryEntityField: CompactFrozenCopy builds its
// copy field by field; an entity field it does not know would be lost.
func TestCompactFrozenCopyHandlesEveryEntityField(t *testing.T) {
	seen := map[string]bool{}
	for _, typ := range []reflect.Type{reflect.TypeFor[Relationship](), reflect.TypeFor[Node]()} {
		for i := 0; i < typ.NumField(); i++ {
			name := typ.Name() + "." + typ.Field(i).Name
			seen[name] = true
			if _, ok := compactCopiedFields[name]; !ok {
				t.Errorf("%s: new entity field — copy it in CompactFrozenCopy and DeepCopy (compact.go, %s) and list it in compactCopiedFields", name, strings.ToLower(typ.Name())+".go")
			}
		}
	}
	for name := range compactCopiedFields {
		if !seen[name] {
			t.Errorf("%s: listed in compactCopiedFields but no longer a field", name)
		}
	}
}
