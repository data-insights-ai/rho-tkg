package index

import (
	"math"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// --- EncodeCompositeKeyTuple collision battery ---

// TestEncodeCompositeKeyTupleCollisionBattery picks adversarial input pairs
// that a NAIVE plain-concatenation or single-separator join WOULD collide
// on, and asserts EncodeCompositeKeyTuple keeps them distinct. See the
// function's doc comment for why length-prefixing is the fix.
func TestEncodeCompositeKeyTupleCollisionBattery(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		a    []string
		b    []string
	}{
		{
			name: "boundary shift — plain concat both produce abc",
			a:    []string{"ab", "c"},
			b:    []string{"a", "bc"},
		},
		{
			name: "three parts, boundary shift",
			a:    []string{"a", "bc", "d"},
			b:    []string{"ab", "c", "d"},
		},
		{
			name: "empty component vs absorbed into neighbor",
			a:    []string{"", "abc"},
			b:    []string{"abc", ""},
		},
		{
			name: "single separator join collision (pipe-style)",
			a:    []string{"a|b", "c"},
			b:    []string{"a", "b|c"},
		},
		{
			name: "different arity, same plain-concat bytes",
			a:    []string{"abcd"},
			b:    []string{"ab", "cd"},
		},
		{
			name: "different arity, three vs one",
			a:    []string{"abcd"},
			b:    []string{"a", "b", "cd"},
		},
		{
			name: "all empty components vs fewer empties",
			a:    []string{"", "", ""},
			b:    []string{"", ""},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ea := EncodeCompositeKeyTuple(tc.a)
			eb := EncodeCompositeKeyTuple(tc.b)
			if ea == eb {
				t.Fatalf("EncodeCompositeKeyTuple(%q) == EncodeCompositeKeyTuple(%q) = %q — collision", tc.a, tc.b, ea)
			}
		})
	}
}

// TestEncodeCompositeKeyTupleDeterministicAndInjective is a broader
// randomized-shape check: many distinct ordered lists must all encode to
// distinct strings, and the SAME list must always encode identically.
func TestEncodeCompositeKeyTupleDeterministicAndInjective(t *testing.T) {
	t.Parallel()

	lists := [][]string{
		{"a", "b"},
		{"a", "bb"},
		{"aa", "b"},
		{"a", "b", "c"},
		{"a", "b", "c", "d"},
		{"", "a"},
		{"a", ""},
		{"", ""},
		{"x:y", "z"},
		{"x", "y:z"},
	}
	seen := make(map[string][]string)
	for _, l := range lists {
		enc := EncodeCompositeKeyTuple(l)
		if prior, ok := seen[enc]; ok {
			t.Fatalf("collision: %q and %q both encode to %q", prior, l, enc)
		}
		seen[enc] = l
		// Determinism: encoding twice yields the same string.
		if enc2 := EncodeCompositeKeyTuple(l); enc2 != enc {
			t.Fatalf("EncodeCompositeKeyTuple(%q) not deterministic: %q vs %q", l, enc, enc2)
		}
	}
}

// --- NodeCompositeValueKey / QueryCompositeValueKey ---

func TestNodeCompositeValueKeyAllKeysPresentRequired(t *testing.T) {
	t.Parallel()
	n := types.NewNode(types.NodeID(snowflake.ID(1)), 10, nil)
	if err := n.SetProperty("first", "Alice"); err != nil {
		t.Fatal(err)
	}
	// "last" is deliberately absent.

	if _, ok := NodeCompositeValueKey([]string{"first", "last"}, n); ok {
		t.Fatal("expected ok=false when a declared key is missing")
	}
	if err := n.SetProperty("last", "Smith"); err != nil {
		t.Fatal(err)
	}
	vk, ok := NodeCompositeValueKey([]string{"first", "last"}, n)
	if !ok || vk == "" {
		t.Fatalf("expected a non-empty composite key once all declared keys are present, got ok=%v vk=%q", ok, vk)
	}
}

func TestNodeCompositeValueKeyNonIndexableValueTreatedAsMissing(t *testing.T) {
	t.Parallel()
	n := types.NewNode(types.NodeID(snowflake.ID(1)), 10, nil)
	if err := n.SetProperty("first", "Alice"); err != nil {
		t.Fatal(err)
	}
	if err := n.SetProperty("tags", []any{"a", "b"}); err != nil {
		t.Fatal(err)
	}
	if _, ok := NodeCompositeValueKey([]string{"first", "tags"}, n); ok {
		t.Fatal("expected ok=false when a declared key holds a non-indexable (slice) value")
	}
}

func TestNodeCompositeValueKeyOrderMatters(t *testing.T) {
	t.Parallel()
	n := types.NewNode(types.NodeID(snowflake.ID(1)), 10, nil)
	if err := n.SetProperty("a", "x"); err != nil {
		t.Fatal(err)
	}
	if err := n.SetProperty("b", "y"); err != nil {
		t.Fatal(err)
	}
	vkAB, _ := NodeCompositeValueKey([]string{"a", "b"}, n)
	vkBA, _ := NodeCompositeValueKey([]string{"b", "a"}, n)
	if vkAB == vkBA {
		t.Fatalf("declared key order must affect the composite value key: %q == %q", vkAB, vkBA)
	}
}

func TestQueryCompositeValueKeyMatchesNodeCompositeValueKey(t *testing.T) {
	t.Parallel()
	n := types.NewNode(types.NodeID(snowflake.ID(1)), 10, nil)
	if err := n.SetProperty("a", int64(5)); err != nil {
		t.Fatal(err)
	}
	if err := n.SetProperty("b", "hello"); err != nil {
		t.Fatal(err)
	}
	nodeKey, ok := NodeCompositeValueKey([]string{"a", "b"}, n)
	if !ok {
		t.Fatal("expected node key ok")
	}
	queryKey, ok := QueryCompositeValueKey([]string{"a", "b"}, map[string]any{"a": int64(5), "b": "hello"})
	if !ok {
		t.Fatal("expected query key ok")
	}
	if nodeKey != queryKey {
		t.Fatalf("node key %q != query key %q for the same logical values", nodeKey, queryKey)
	}
	// A different value must produce a different key.
	otherKey, ok := QueryCompositeValueKey([]string{"a", "b"}, map[string]any{"a": int64(6), "b": "hello"})
	if !ok {
		t.Fatal("expected query key ok")
	}
	if otherKey == nodeKey {
		t.Fatal("different values must not collide onto the same composite key")
	}
}

func TestQueryCompositeValueKeyMissingKeyOrUnindexable(t *testing.T) {
	t.Parallel()
	if _, ok := QueryCompositeValueKey([]string{"a", "b"}, map[string]any{"a": "x"}); ok {
		t.Fatal("expected ok=false when values is missing a declared key")
	}
	if _, ok := QueryCompositeValueKey([]string{"a", "b"}, map[string]any{"a": "x", "b": []any{"y"}}); ok {
		t.Fatal("expected ok=false when a declared key's value is not indexable")
	}
}

// --- Equality across component type combinations (string/int64/bool/temporal, float) ---

func TestNodeCompositeValueKeyAcrossTypeCombinations(t *testing.T) {
	t.Parallel()

	temporalVal := types.TemporalValue{Kind: 0, Value: "2026-01-01"}

	cases := []struct {
		name   string
		values map[string]any
	}{
		{"string+int64", map[string]any{"a": "hello", "b": int64(42)}},
		{"int64+bool", map[string]any{"a": int64(7), "b": true}},
		{"bool+string", map[string]any{"a": false, "b": "world"}},
		{"string+temporal", map[string]any{"a": "loc", "b": temporalVal}},
		{"int64+temporal", map[string]any{"a": int64(-3), "b": temporalVal}},
		{"float64+string", map[string]any{"a": 3.14, "b": "pi"}},
		{"float64+int64", map[string]any{"a": 2.5, "b": int64(2)}},
		{"bool+temporal", map[string]any{"a": true, "b": temporalVal}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			n := types.NewNode(types.NodeID(snowflake.ID(1)), 10, nil)
			for k, v := range tc.values {
				if err := n.SetProperty(k, v); err != nil {
					t.Fatalf("SetProperty(%q, %v): %v", k, v, err)
				}
			}
			vk, ok := NodeCompositeValueKey([]string{"a", "b"}, n)
			if !ok || vk == "" {
				t.Fatalf("expected a composite key for %s, got ok=%v vk=%q", tc.name, ok, vk)
			}
			qk, ok := QueryCompositeValueKey([]string{"a", "b"}, tc.values)
			if !ok || qk != vk {
				t.Fatalf("%s: query key %q != node key %q (ok=%v)", tc.name, qk, vk, ok)
			}
		})
	}
}

// TestNodeCompositeValueKeyFloatBitPatternSemantics documents the design
// choice for K3c: floats are SUPPORTED in composite equality using the SAME
// lesson-25 bit-pattern semantics types.IndexablePropertyValueKey already
// applies to the single-key property index (+0/-0 collapse, NaN pinned,
// float32 vs float64 kept distinct) — chosen over "reject floats" (the
// unique-constraint precedent) because this is an EQUALITY index, not a
// business-identity constraint: exact bit-pattern equality is a sound,
// unsurprising definition for "does this float value match", whereas
// uniqueness has the additional (and here irrelevant) concern of float
// epsilon ambiguity across writers.
func TestNodeCompositeValueKeyFloatBitPatternSemantics(t *testing.T) {
	t.Parallel()

	// +0 and -0 collapse to the same canonical key (matches IndexablePropertyValueKey).
	nPosZero := types.NewNode(types.NodeID(snowflake.ID(1)), 10, nil)
	if err := nPosZero.SetProperty("a", "x"); err != nil {
		t.Fatal(err)
	}
	if err := nPosZero.SetProperty("b", 0.0); err != nil {
		t.Fatal(err)
	}
	nNegZero := types.NewNode(types.NodeID(snowflake.ID(2)), 10, nil)
	if err := nNegZero.SetProperty("a", "x"); err != nil {
		t.Fatal(err)
	}
	if err := nNegZero.SetProperty("b", math.Copysign(0, -1)); err != nil {
		t.Fatal(err)
	}
	posKey, ok := NodeCompositeValueKey([]string{"a", "b"}, nPosZero)
	if !ok {
		t.Fatal("expected ok")
	}
	negKey, ok := NodeCompositeValueKey([]string{"a", "b"}, nNegZero)
	if !ok {
		t.Fatal("expected ok")
	}
	if posKey != negKey {
		t.Fatalf("+0 and -0 must collapse to the same composite key: %q != %q", posKey, negKey)
	}

	// float32 and float64 with the same magnitude stay distinct (type-prefixed).
	n32 := types.NewNode(types.NodeID(snowflake.ID(3)), 10, nil)
	if err := n32.SetProperty("a", "x"); err != nil {
		t.Fatal(err)
	}
	if err := n32.SetProperty("b", float32(1.5)); err != nil {
		t.Fatal(err)
	}
	n64 := types.NewNode(types.NodeID(snowflake.ID(4)), 10, nil)
	if err := n64.SetProperty("a", "x"); err != nil {
		t.Fatal(err)
	}
	if err := n64.SetProperty("b", float64(1.5)); err != nil {
		t.Fatal(err)
	}
	key32, ok := NodeCompositeValueKey([]string{"a", "b"}, n32)
	if !ok {
		t.Fatal("expected ok")
	}
	key64, ok := NodeCompositeValueKey([]string{"a", "b"}, n64)
	if !ok {
		t.Fatal("expected ok")
	}
	if key32 == key64 {
		t.Fatal("float32(1.5) and float64(1.5) must not collide onto the same composite key")
	}
}

// --- NodeMatchesAllProperties ---

func TestNodeMatchesAllPropertiesAndConjunction(t *testing.T) {
	t.Parallel()
	n := types.NewNode(types.NodeID(snowflake.ID(1)), 10, nil)
	if err := n.SetProperty("a", "x"); err != nil {
		t.Fatal(err)
	}
	if err := n.SetProperty("b", int64(5)); err != nil {
		t.Fatal(err)
	}

	if !NodeMatchesAllProperties(n, map[string]any{"a": "x", "b": int64(5)}) {
		t.Fatal("expected match on both properties")
	}
	if NodeMatchesAllProperties(n, map[string]any{"a": "x", "b": int64(6)}) {
		t.Fatal("must NOT match when one of two properties differs")
	}
	if NodeMatchesAllProperties(n, map[string]any{"a": "x", "c": "missing"}) {
		t.Fatal("must NOT match when a requested key is absent from the node")
	}
	if !NodeMatchesAllProperties(n, map[string]any{"a": "x"}) {
		t.Fatal("a subset of the node's properties must still match (AND over the REQUESTED set only)")
	}
}

// --- Maintenance: Add/Remove across defsByLabel, incl. partial-key removal ---

func TestAddAndRemoveNodeFromCompositeIndexes(t *testing.T) {
	t.Parallel()

	n := types.NewNode(types.NodeID(snowflake.ID(1)), 10, []uint16{20})
	if err := n.SetProperty("first", "Alice"); err != nil {
		t.Fatal(err)
	}
	if err := n.SetProperty("last", "Smith"); err != nil {
		t.Fatal(err)
	}

	firstLastIdx := NewCompositePropertyIndex([]string{"first", "last"})
	otherLabelIdx := NewCompositePropertyIndex([]string{"first", "last"})
	indexes := map[CompositeIndexKey]*CompositePropertyIndex{
		{LabelToken: 10, Keys: EncodeCompositeKeyTuple([]string{"first", "last"})}: firstLastIdx,
		{LabelToken: 99, Keys: EncodeCompositeKeyTuple([]string{"first", "last"})}: otherLabelIdx,
	}
	defsByLabel := map[uint16][]CompositeIndexKey{
		10: {{LabelToken: 10, Keys: EncodeCompositeKeyTuple([]string{"first", "last"})}},
		99: {{LabelToken: 99, Keys: EncodeCompositeKeyTuple([]string{"first", "last"})}},
	}

	rawID := snowflake.ID(1)
	AddNodeToCompositeIndexes(indexes, defsByLabel, n, rawID)

	vk, ok := NodeCompositeValueKey([]string{"first", "last"}, n)
	if !ok {
		t.Fatal("expected ok")
	}
	if got := firstLastIdx.NodeIDs(vk); len(got) != 1 || got[0] != types.NodeID(rawID) {
		t.Fatalf("expected node indexed under label 10, got %v", got)
	}
	// Node does not carry label 99 — must NOT be indexed there.
	if got := otherLabelIdx.NodeIDs(vk); len(got) != 0 {
		t.Fatalf("must NOT index a node under a label it does not carry, got %v", got)
	}

	// Remove (using the same still-full snapshot Add used) must clear the entry.
	RemoveNodeFromCompositeIndexes(indexes, defsByLabel, n, rawID)
	if got := firstLastIdx.NodeIDs(vk); len(got) != 0 {
		t.Fatalf("expected entry removed, got %v", got)
	}
}

// TestPartialKeyRemovalViaMutationDoorPattern exercises the ACTUAL
// remove-old-then-add-new pattern every store mutation door uses (see
// memory/badgerstore's ReplaceNode etc.): removing the OLD full snapshot's
// entry, then attempting to add the NEW (key-deleted) snapshot, must leave
// NO entry behind.
func TestPartialKeyRemovalViaMutationDoorPattern(t *testing.T) {
	t.Parallel()

	rawID := snowflake.ID(7)
	old := types.NewNode(types.NodeID(rawID), 10, nil)
	if err := old.SetProperty("first", "Alice"); err != nil {
		t.Fatal(err)
	}
	if err := old.SetProperty("last", "Smith"); err != nil {
		t.Fatal(err)
	}

	idx := NewCompositePropertyIndex([]string{"first", "last"})
	key := CompositeIndexKey{LabelToken: 10, Keys: EncodeCompositeKeyTuple([]string{"first", "last"})}
	indexes := map[CompositeIndexKey]*CompositePropertyIndex{key: idx}
	defsByLabel := map[uint16][]CompositeIndexKey{10: {key}}

	AddNodeToCompositeIndexes(indexes, defsByLabel, old, rawID)
	vk, _ := NodeCompositeValueKey([]string{"first", "last"}, old)
	if got := idx.NodeIDs(vk); len(got) != 1 {
		t.Fatalf("expected entry after Add, got %v", got)
	}

	updated := old.DeepCopy()
	if _, err := updated.DeleteProperty("last"); err != nil {
		t.Fatal(err)
	}

	// Mutation door order: Remove(old) then Add(new).
	RemoveNodeFromCompositeIndexes(indexes, defsByLabel, old, rawID)
	AddNodeToCompositeIndexes(indexes, defsByLabel, updated, rawID)

	if got := idx.NodeIDs(vk); len(got) != 0 {
		t.Fatalf("deleting one component property must remove the composite entry entirely, got %v", got)
	}
	for _, set := range idx.Entries {
		if _, present := set[rawID]; present {
			t.Fatalf("node %d must not appear under any composite entry after losing a required key", rawID)
		}
	}
}

// --- FindCompositeIndexForQuery: key-SET match, order-independent ---

func TestFindCompositeIndexForQueryExactSetMatch(t *testing.T) {
	t.Parallel()

	key := CompositeIndexKey{LabelToken: 10, Keys: EncodeCompositeKeyTuple([]string{"first", "last"})}
	idx := NewCompositePropertyIndex([]string{"first", "last"})
	indexes := map[CompositeIndexKey]*CompositePropertyIndex{key: idx}
	defsByLabel := map[uint16][]CompositeIndexKey{10: {key}}

	// Order-independent: query map has no order, but its KEY SET matches.
	found, ok := FindCompositeIndexForQuery(indexes, defsByLabel, 10, map[string]any{"last": "Smith", "first": "Alice"})
	if !ok || found != idx {
		t.Fatalf("expected exact key-set match regardless of map iteration order, ok=%v", ok)
	}

	// Different label -> no match.
	if _, ok := FindCompositeIndexForQuery(indexes, defsByLabel, 11, map[string]any{"first": "Alice", "last": "Smith"}); ok {
		t.Fatal("expected no match for an unregistered label")
	}

	// Superset of keys -> no match (v1 has no partial-prefix substitution).
	if _, ok := FindCompositeIndexForQuery(indexes, defsByLabel, 10, map[string]any{"first": "Alice", "last": "Smith", "extra": 1}); ok {
		t.Fatal("expected no match when the query supplies MORE keys than any registered definition")
	}

	// Subset of keys -> no match.
	if _, ok := FindCompositeIndexForQuery(indexes, defsByLabel, 10, map[string]any{"first": "Alice"}); ok {
		t.Fatal("expected no match when the query supplies FEWER keys than any registered definition")
	}
}

// --- Register/UnregisterCompositeIndex secondary-index bookkeeping ---

func TestRegisterUnregisterCompositeIndex(t *testing.T) {
	t.Parallel()

	indexes := map[CompositeIndexKey]*CompositePropertyIndex{}
	defsByLabel := map[uint16][]CompositeIndexKey{}

	key1 := CompositeIndexKey{LabelToken: 10, Keys: EncodeCompositeKeyTuple([]string{"a", "b"})}
	key2 := CompositeIndexKey{LabelToken: 10, Keys: EncodeCompositeKeyTuple([]string{"c", "d"})}
	idx1 := NewCompositePropertyIndex([]string{"a", "b"})
	idx2 := NewCompositePropertyIndex([]string{"c", "d"})

	RegisterCompositeIndex(indexes, defsByLabel, key1, idx1)
	RegisterCompositeIndex(indexes, defsByLabel, key2, idx2)

	if len(defsByLabel[10]) != 2 {
		t.Fatalf("expected 2 definitions registered under label 10, got %d", len(defsByLabel[10]))
	}

	UnregisterCompositeIndex(indexes, defsByLabel, key1)
	if _, exists := indexes[key1]; exists {
		t.Fatal("key1 must be removed from indexes")
	}
	if len(defsByLabel[10]) != 1 || defsByLabel[10][0] != key2 {
		t.Fatalf("expected only key2 to remain under label 10, got %v", defsByLabel[10])
	}

	UnregisterCompositeIndex(indexes, defsByLabel, key2)
	if _, exists := defsByLabel[10]; exists {
		t.Fatal("label 10 entry must be fully removed once its last definition is dropped")
	}
}

// --- PurgeNodeFromAllCompositeIndexes (corruption path) ---

func TestPurgeNodeFromAllCompositeIndexes(t *testing.T) {
	t.Parallel()

	idx1 := NewCompositePropertyIndex([]string{"a", "b"})
	idx1.AddKey(snowflake.ID(1), "vk1")
	idx1.AddKey(snowflake.ID(2), "vk2")
	idx2 := NewCompositePropertyIndex([]string{"c", "d"})
	idx2.AddKey(snowflake.ID(1), "vk3")

	indexes := map[CompositeIndexKey]*CompositePropertyIndex{
		{LabelToken: 1}: idx1,
		{LabelToken: 2}: idx2,
	}

	PurgeNodeFromAllCompositeIndexes(indexes, snowflake.ID(1))

	if len(idx1.Entries["vk1"]) != 0 {
		t.Fatal("expected node 1 purged from idx1's vk1 bucket")
	}
	if got := idx1.NodeIDs("vk2"); len(got) != 1 {
		t.Fatalf("node 2 must survive the purge of node 1, got %v", got)
	}
	if len(idx2.Entries["vk3"]) != 0 {
		t.Fatal("expected node 1 purged from idx2 too — purge sweeps EVERY composite index")
	}
}
