package index

import (
	"encoding/binary"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// CompositeIndexKey identifies a composite property index by label token and
// the caller-DECLARED, order-preserving tuple of 2..4 property key names.
// Keys is EncodeCompositeKeyTuple applied to the declared name list — see its
// doc comment for why plain concatenation cannot be used for identity either.
type CompositeIndexKey struct {
	LabelToken uint16
	Keys       string
}

// CompositePropertyIndex stores a reverse mapping from a concatenated
// per-component canonical value key (EncodeCompositeKeyTuple applied to each
// declared key's types.IndexablePropertyValueKey, in DECLARED order) to the
// set of node IDs whose current row carries an indexable value for EVERY
// declared key. v1 is EQUALITY-only: no partial-prefix or range lookup — see
// docs/query-planners.md "Composite property indexes".
type CompositePropertyIndex struct {
	// Keys is the declared, ordered component property-key list — every
	// value key below (and every query key) is built in this same order.
	Keys []string

	Entries map[string]map[snowflake.ID]struct{}

	// Mutated tracks IDs touched by a concurrent write during 3-phase
	// creation (see badger's CreateCompositePropertyIndex). Non-nil only
	// while a creation is in flight; a query must not trust the index while
	// Mutated != nil (mirrors PropertyIndex's identical convention).
	Mutated map[snowflake.ID]struct{}
}

// NewCompositePropertyIndex creates an empty composite index declared over
// keys (2..4, caller already validated — see storepkg.ValidateCompositeIndexKeys).
func NewCompositePropertyIndex(keys []string) *CompositePropertyIndex {
	declared := make([]string, len(keys))
	copy(declared, keys)
	return &CompositePropertyIndex{
		Keys:    declared,
		Entries: make(map[string]map[snowflake.ID]struct{}),
	}
}

// EncodeCompositeKeyTuple builds a length-prefixed, INJECTIVE concatenation
// of an ordered list of strings: each part is preceded by its own 4-byte
// big-endian byte length.
//
// Two DIFFERENT ordered lists of strings can never produce the same
// encoding. A naive plain-concatenation or single-separator join CAN: for
// example ["ab", "c"] and ["a", "bc"] both plain-concatenate to "abc", so a
// composite index built that way would silently alias a 2-key tuple
// {ab, c} with a different 2-key tuple {a, bc} onto the same map slot —
// either as two distinct DEFINITIONS (CompositeIndexKey.Keys, built from the
// declared property-key NAMES) or as two distinct VALUE tuples for the same
// definition (Entries map key, built from each component's canonical
// types.IndexablePropertyValueKey). Length-prefixing each component makes the
// encoding a bijection with the ordered list: parsing back (read a 4-byte
// length, then that many bytes, repeat) is always possible, which is the
// standard proof that no two distinct ordered lists can collide — this
// package never needs to parse, only rely on that injectivity. See
// TestEncodeCompositeKeyTupleCollisionBattery for adversarial inputs chosen
// so naive concatenation WOULD collide.
func EncodeCompositeKeyTuple(parts []string) string {
	total := 0
	for _, p := range parts {
		total += 4 + len(p)
	}
	b := make([]byte, 0, total)
	var lenBuf [4]byte
	for _, p := range parts {
		binary.BigEndian.PutUint32(lenBuf[:], uint32(len(p))) //nolint:gosec // component length, never remotely near uint32 overflow
		b = append(b, lenBuf[:]...)
		b = append(b, p...)
	}
	return string(b)
}

// NodeCompositeValueKey computes the composite entry key for n under keys'
// declared order, or ok=false if n is missing an indexable value for ANY
// declared key — "a node missing any composite key has no entry" is the one
// rule every maintenance/query path in this file agrees on. Exported so
// backend backfill scans (memory/badger CreateCompositePropertyIndex) can
// compute the same key the maintenance path would have used.
func NodeCompositeValueKey(keys []string, n *types.Node) (string, bool) {
	parts := make([]string, len(keys))
	for i, k := range keys {
		vk, found := n.IndexablePropertyValueKey(k)
		if !found || vk == "" {
			return "", false
		}
		parts[i] = vk
	}
	return EncodeCompositeKeyTuple(parts), true
}

// QueryCompositeValueKey computes the composite lookup key for an equality
// query, given the SAME declared key order a CompositePropertyIndex was
// created with and a value for each. ok=false when values is missing an
// entry for any declared key or a value is not indexable — mirrors
// NodeCompositeValueKey's "all keys required" contract on the read side.
func QueryCompositeValueKey(keys []string, values map[string]any) (string, bool) {
	parts := make([]string, len(keys))
	for i, k := range keys {
		v, ok := values[k]
		if !ok {
			return "", false
		}
		vk := types.IndexablePropertyValueKey(v)
		if vk == "" {
			return "", false
		}
		parts[i] = vk
	}
	return EncodeCompositeKeyTuple(parts), true
}

// NodeMatchesAllProperties reports whether n carries an indexable value for
// EVERY (key, value) pair in values (AND-conjunction) — the definition every
// NodesByLabelAndProperties fallback (backend-internal scan-and-filter, and
// the graph-layer mandatory fallback for a store lacking the capability)
// must agree on, whether or not a composite index accelerates the lookup.
func NodeMatchesAllProperties(n *types.Node, values map[string]any) bool {
	for k, want := range values {
		gotKey, found := n.IndexablePropertyValueKey(k)
		if !found || gotKey == "" {
			return false
		}
		wantKey := types.IndexablePropertyValueKey(want)
		if wantKey == "" || gotKey != wantKey {
			return false
		}
	}
	return true
}

// AddKey inserts a node ID into the composite index for a precomputed
// composite value key vk. No-op if vk is empty (not indexable).
func (ci *CompositePropertyIndex) AddKey(id snowflake.ID, vk string) {
	if ci == nil || vk == "" {
		return
	}
	if ci.Entries == nil {
		ci.Entries = make(map[string]map[snowflake.ID]struct{})
	}
	set := ci.Entries[vk]
	if set == nil {
		set = make(map[snowflake.ID]struct{})
		ci.Entries[vk] = set
	}
	set[id] = struct{}{}
	if ci.Mutated != nil {
		ci.Mutated[id] = struct{}{}
	}
}

// removeKey deletes a node ID from the composite index for value key vk.
func (ci *CompositePropertyIndex) removeKey(id snowflake.ID, vk string) {
	if ci == nil || vk == "" {
		return
	}
	if set, ok := ci.Entries[vk]; ok {
		delete(set, id)
		if len(set) == 0 {
			delete(ci.Entries, vk)
		}
	}
	if ci.Mutated != nil {
		ci.Mutated[id] = struct{}{}
	}
}

// NodeIDs returns the node IDs matching a precomputed composite value key vk,
// as a caller-owned slice.
func (ci *CompositePropertyIndex) NodeIDs(vk string) []types.NodeID {
	if ci == nil || vk == "" {
		return nil
	}
	set := ci.Entries[vk]
	if len(set) == 0 {
		return nil
	}
	out := make([]types.NodeID, 0, len(set))
	for id := range set {
		out = append(out, types.NodeID(id))
	}
	return out
}

// RegisterCompositeIndex installs key/idx into both indexes and the
// label->definitions secondary index maintained alongside it. Caller holds
// the store's write lock.
func RegisterCompositeIndex(indexes map[CompositeIndexKey]*CompositePropertyIndex, defsByLabel map[uint16][]CompositeIndexKey, key CompositeIndexKey, idx *CompositePropertyIndex) {
	indexes[key] = idx
	defsByLabel[key.LabelToken] = append(defsByLabel[key.LabelToken], key)
}

// UnregisterCompositeIndex removes key from both indexes and defsByLabel.
// Caller holds the store's write lock.
func UnregisterCompositeIndex(indexes map[CompositeIndexKey]*CompositePropertyIndex, defsByLabel map[uint16][]CompositeIndexKey, key CompositeIndexKey) {
	delete(indexes, key)
	list := defsByLabel[key.LabelToken]
	for i, k := range list {
		if k == key {
			list = append(list[:i], list[i+1:]...)
			break
		}
	}
	if len(list) == 0 {
		delete(defsByLabel, key.LabelToken)
	} else {
		defsByLabel[key.LabelToken] = list
	}
}

// FindCompositeIndexForQuery returns the composite index definition on
// labelToken whose declared key SET exactly equals the keys present in
// values (order-independent — a Go map has no order), or ok=false if no such
// definition is registered. v1 requires an EXACT key-set match: no
// partial-prefix substitution (see docs/query-planners.md).
func FindCompositeIndexForQuery(indexes map[CompositeIndexKey]*CompositePropertyIndex, defsByLabel map[uint16][]CompositeIndexKey, labelToken uint16, values map[string]any) (*CompositePropertyIndex, bool) {
	for _, defKey := range defsByLabel[labelToken] {
		idx, ok := indexes[defKey]
		if !ok || idx == nil || len(idx.Keys) != len(values) {
			continue
		}
		matches := true
		for _, k := range idx.Keys {
			if _, present := values[k]; !present {
				matches = false
				break
			}
		}
		if matches {
			return idx, true
		}
	}
	return nil, false
}

// AddNodeToCompositeIndexes indexes a node into every composite property
// index defined on any label the node carries. defsByLabel enumerates only
// the definitions relevant to the node's labels (avoids an O(all composite
// defs) scan once many labels/definitions exist) — maintained alongside
// indexes via RegisterCompositeIndex/UnregisterCompositeIndex. Caller must
// hold the store's write lock.
func AddNodeToCompositeIndexes(indexes map[CompositeIndexKey]*CompositePropertyIndex, defsByLabel map[uint16][]CompositeIndexKey, n *types.Node, id snowflake.ID) {
	if len(indexes) == 0 {
		return
	}
	labelCount := n.LabelTokenCount()
	var seen map[CompositeIndexKey]struct{}
	for i := 0; i < labelCount; i++ {
		labelToken := n.LabelTokenRawAt(i)
		for _, defKey := range defsByLabel[labelToken] {
			if seen == nil {
				seen = make(map[CompositeIndexKey]struct{})
			} else if _, dup := seen[defKey]; dup {
				continue
			}
			seen[defKey] = struct{}{}
			idx, ok := indexes[defKey]
			if !ok || idx == nil {
				continue
			}
			vk, ok := NodeCompositeValueKey(idx.Keys, n)
			if !ok {
				continue
			}
			idx.AddKey(id, vk)
		}
	}
}

// RemoveNodeFromCompositeIndexes removes a node's entries from every
// composite property index defined on any label the node carries. Caller
// must hold the store's write lock.
func RemoveNodeFromCompositeIndexes(indexes map[CompositeIndexKey]*CompositePropertyIndex, defsByLabel map[uint16][]CompositeIndexKey, n *types.Node, id snowflake.ID) {
	if len(indexes) == 0 {
		return
	}
	labelCount := n.LabelTokenCount()
	var seen map[CompositeIndexKey]struct{}
	for i := 0; i < labelCount; i++ {
		labelToken := n.LabelTokenRawAt(i)
		for _, defKey := range defsByLabel[labelToken] {
			if seen == nil {
				seen = make(map[CompositeIndexKey]struct{})
			} else if _, dup := seen[defKey]; dup {
				continue
			}
			seen[defKey] = struct{}{}
			idx, ok := indexes[defKey]
			if !ok || idx == nil {
				continue
			}
			vk, ok := NodeCompositeValueKey(idx.Keys, n)
			if !ok {
				continue
			}
			idx.removeKey(id, vk)
		}
	}
}

// PurgeNodeFromAllCompositeIndexes is the corruption-path brute-force
// fallback (node data unavailable, so the value can't be recomputed) —
// mirrors PurgeNodeFromAllPropertyIndexes. Caller must hold the store's
// write lock.
func PurgeNodeFromAllCompositeIndexes(indexes map[CompositeIndexKey]*CompositePropertyIndex, id snowflake.ID) {
	for _, idx := range indexes {
		if idx == nil {
			continue
		}
		for vk, set := range idx.Entries {
			delete(set, id)
			if len(set) == 0 {
				delete(idx.Entries, vk)
			}
		}
		if idx.Mutated != nil {
			idx.Mutated[id] = struct{}{}
		}
	}
}
