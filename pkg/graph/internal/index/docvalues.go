package index

import (
	"cmp"
	"slices"
	"strings"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// DocValues (X5) — per-(label, property) columnar value store aligned to a sorted
// NodeID ordinal vector, so grouped aggregation / ORDER BY can stream the column
// without materializing each *types.Node. This file holds the STORE-AGNOSTIC
// column structure and builder; backends provide the membership set (the full
// label index — NOT valid-time filtered) and a property getter.
//
// Correctness invariants (every one is a divergence-suite probe in the consumer):
//   - Membership is the FULL label set the unfiltered scan returns. The ordinal
//     vector covers every label member; a node lacking the property gets a
//     cleared present bit, so count(*) still counts its row.
//   - A column is IMMUTABLE once built. Refresh builds a NEW LabelDocValues and
//     swaps the pointer; an in-flight reader keeps the old immutable snapshot.
//   - Numeric values preserve their Go type (int64 vs float64) so the consumer's
//     int64-precise accumulators stay bit-exact for values above 2^53.
//   - A property whose values are not uniformly numeric or uniformly string
//     (bool/list/map, a typed temporal, or a numeric/string mix) is NOT buildable;
//     the consumer falls back to the per-node path for it. Correctness over reach.

// MaxDocValuesNodes caps a per-label column. Over the cap a store declines to
// build (the consumer falls back to the per-node path), bounding column memory
// rather than silently truncating. 10M numeric rows ≈ 165 MB/column.
const MaxDocValuesNodes = 10_000_000

// MultiLabelKey encodes a label-token tuple into an order-independent cache key
// for a label-intersection column (multi-label patterns like (p:A:B)). Tokens are
// sorted so (A,B) and (B,A) — the same intersection — share one cache entry, then
// packed little-endian into a string (2 bytes per uint16). A 0-length tuple yields
// "" (callers never pass that — a multi-label pattern has ≥2 labels).
func MultiLabelKey(toks []uint16) string {
	s := slices.Clone(toks)
	slices.Sort(s)
	var b strings.Builder
	b.Grow(len(s) * 2)
	for _, t := range s {
		b.WriteByte(byte(t))
		b.WriteByte(byte(t >> 8))
	}
	return b.String()
}

// UnionKeys returns the deduplicated union of two property-key slices, used to keep
// a label's columns at one epoch across rebuilds as queries request different
// property subsets.
func UnionKeys(a, b []string) []string {
	seen := make(map[string]struct{}, len(a)+len(b))
	out := make([]string, 0, len(a)+len(b))
	for _, k := range a {
		if _, ok := seen[k]; !ok {
			seen[k] = struct{}{}
			out = append(out, k)
		}
	}
	for _, k := range b {
		if _, ok := seen[k]; !ok {
			seen[k] = struct{}{}
			out = append(out, k)
		}
	}
	return out
}

// ColType is the storage class of a built column.
type ColType uint8

const (
	colUnbuildable ColType = iota // mixed/unsupported value type — consumer falls back
	ColNumeric                    // int64 and/or float64 (type preserved per row)
	ColString                     // dictionary-encoded strings (covers ISO-string temporals)
)

// docColumn is one immutable, ordinal-aligned value column. Exactly one value
// representation is populated per col type.
type docColumn struct {
	typ     ColType
	present bitset // 1 = node has the property; 0 = absent/null

	// ColNumeric: the int64/float64 value boxed ONCE at build (its dynamic type
	// preserves int64-vs-float64, so the consumer's int64-precise accumulators stay
	// bit-exact above 2^53). Boxing at build, not per read, is what keeps the hot
	// path allocation-free — the per-node path it replaces already holds boxed
	// values, so this matches its memory without re-boxing every row.
	boxed []any

	// ColString: codes[ord] indexes into dict (deduplicated terms, each boxed ONCE
	// at build so valueAt returns an `any` without re-boxing the string per read).
	dict  []any
	codes []uint32
}

// LabelDocValues is the immutable set of aligned columns for one label, built at a
// single store mutation epoch. All columns share NodeIDs (the ordinal vector), so
// ordinal i refers to the same node in every column.
type LabelDocValues struct {
	epoch   uint64
	nodeIDs []types.NodeID
	cols    map[string]*docColumn // propKey -> column (only buildable keys present)
}

// Epoch returns the store node-mutation epoch this snapshot was built at.
func (l *LabelDocValues) Epoch() uint64 { return l.epoch }

// Len is the number of label members (rows) in the ordinal vector.
func (l *LabelDocValues) Len() int { return len(l.nodeIDs) }

// Has reports whether propKey was built as a usable column.
func (l *LabelDocValues) Has(propKey string) bool {
	c, ok := l.cols[propKey]
	return ok && c.typ != colUnbuildable
}

// Keys returns the property keys this snapshot built columns for (buildable or
// not). Used to keep a sticky union across rebuilds so a label's columns stay at
// one epoch even as queries request different property subsets.
func (l *LabelDocValues) Keys() []string {
	keys := make([]string, 0, len(l.cols))
	for k := range l.cols {
		keys = append(keys, k)
	}
	return keys
}

// HasAll reports whether every requested propKey is a usable column.
func (l *LabelDocValues) HasAll(propKeys []string) bool {
	for _, k := range propKeys {
		if !l.Has(k) {
			return false
		}
	}
	return true
}

// ForEachRow streams the requested columns in ordinal order. For each row it calls
// fn with the node ID and, per requested propKey, the typed value (int64, float64,
// or string) and a present flag. A cleared present flag yields a nil value — the
// exact shape GetProperty returns for an absent property, so the consumer
// reproduces count(x)/sum/min/max null semantics. fn returns false to stop.
// Returns false if any requested key is not a usable column (caller must check
// HasAll first; this is a defensive guard).
func (l *LabelDocValues) ForEachRow(propKeys []string, fn func(id types.NodeID, vals []any, present []bool) bool) bool {
	cols := make([]*docColumn, len(propKeys))
	for i, k := range propKeys {
		c, ok := l.cols[k]
		if !ok || c.typ == colUnbuildable {
			return false
		}
		cols[i] = c
	}
	vals := make([]any, len(propKeys))
	present := make([]bool, len(propKeys))
	for ord, id := range l.nodeIDs {
		for i, c := range cols {
			if !c.present.get(ord) {
				vals[i], present[i] = nil, false
				continue
			}
			present[i] = true
			vals[i] = c.valueAt(ord)
		}
		if !fn(id, vals, present) {
			return false
		}
	}
	return true
}

// lookup returns the ordinal of id via binary search on the sorted nodeIDs, or
// (-1, false) if id is not a member. Uses the SAME comparator BuildLabelDocValues
// sorted with (cmp.Compare on SnowflakeID) — a hand-rolled raw-int64 or unsigned
// compare would disagree on the sort order and miss members (Pattern 12 sibling).
func (l *LabelDocValues) lookup(id types.NodeID) (int, bool) {
	return slices.BinarySearchFunc(l.nodeIDs, id, func(a, target types.NodeID) int {
		return cmp.Compare(a.SnowflakeID(), target.SnowflakeID())
	})
}

// PointSnapshot is a LabelDocValues bound to a fixed requested-property order for
// RANDOM-ACCESS point lookups (the expand-aggregation target side). cols[i] is the
// column for the caller's i-th requested propKey, so Row fills the caller's buffers
// in REQUESTED order regardless of internal storage order (buildColumnsLocked unions
// keys and does not preserve request order — Pattern 12). Implements
// types.NodeColumnReader.
type PointSnapshot struct {
	l    *LabelDocValues
	cols []*docColumn
}

// NewPointSnapshot binds propKeys to their columns for point lookups, or returns
// ok=false if ANY requested key is not a usable (numeric/string) column — mirroring
// HasAll exactly, so the consumer declines the WHOLE query to the per-node path
// rather than reading an unbuildable column as spurious nulls (critique Trap B).
func (l *LabelDocValues) NewPointSnapshot(propKeys []string) (*PointSnapshot, bool) {
	cols := make([]*docColumn, len(propKeys))
	for i, k := range propKeys {
		c, ok := l.cols[k]
		if !ok || c.typ == colUnbuildable {
			return nil, false
		}
		cols[i] = c
	}
	return &PointSnapshot{l: l, cols: cols}, true
}

// Row fills vals/present for id's bound columns and reports membership. Returns
// false (buffers untouched) when id is not a label member — the expand path's b:T
// filter. For a member, a cleared present bit yields a nil value (the absent-
// property shape); Row still returns TRUE so the row is counted (an all-absent but
// BUILDABLE column must count every member — critique Trap B').
func (s *PointSnapshot) Row(id types.NodeID, vals []any, present []bool) bool {
	ord, ok := s.l.lookup(id)
	if !ok {
		return false
	}
	for i, c := range s.cols {
		if !c.present.get(ord) {
			vals[i], present[i] = nil, false
			continue
		}
		present[i] = true
		vals[i] = c.valueAt(ord)
	}
	return true
}

// Epoch is the snapshot's build epoch, for the consumer's Gate-2 staleness check.
func (s *PointSnapshot) Epoch() uint64 { return s.l.epoch }

// valueAt returns the typed Go value at an ordinal whose present bit is set. Both
// arms are allocation-free: numeric values were boxed at build, strings are
// returned from the dictionary (a string header copy, no heap allocation).
func (c *docColumn) valueAt(ord int) any {
	switch c.typ {
	case ColNumeric:
		return c.boxed[ord]
	case ColString:
		return c.dict[c.codes[ord]]
	default:
		return nil
	}
}

// BuildLabelDocValues builds aligned columns for propKeys over the membership set.
// nodeIDs is the FULL label membership (unsorted is fine — it is sorted here once,
// the per-query SortNodeIDs the column amortizes away). getProp(id, key) returns
// the raw stored property value exactly as the per-node path reads it. The
// returned LabelDocValues contains only buildable columns; unbuildable propKeys
// are omitted (Has reports false), so the consumer falls back for them.
func BuildLabelDocValues(epoch uint64, nodeIDs []types.NodeID, propKeys []string,
	getProp func(types.NodeID, string) (any, bool)) *LabelDocValues {

	ids := slices.Clone(nodeIDs)
	slices.SortFunc(ids, func(a, b types.NodeID) int {
		return cmp.Compare(a.SnowflakeID(), b.SnowflakeID())
	})
	n := len(ids)

	l := &LabelDocValues{epoch: epoch, nodeIDs: ids, cols: make(map[string]*docColumn, len(propKeys))}
	for _, key := range propKeys {
		if _, done := l.cols[key]; done {
			continue // duplicate requested key
		}
		l.cols[key] = buildColumn(ids, key, n, getProp)
	}
	return l
}

// buildColumn classifies the property's values and builds the typed column, or a
// colUnbuildable marker if the values are not uniformly numeric/string.
func buildColumn(ids []types.NodeID, key string, n int, getProp func(types.NodeID, string) (any, bool)) *docColumn {
	present := newBitset(n)
	// First pass: classify. numeric and string are the only buildable classes.
	sawNumeric, sawString, sawOther := false, false, false
	for ord, id := range ids {
		v, ok := getProp(id, key)
		if !ok || v == nil {
			continue
		}
		present.set(ord)
		switch v.(type) {
		case int64, int, int32, float64, float32:
			sawNumeric = true
		case string:
			sawString = true
		default:
			sawOther = true
		}
	}
	if sawOther || (sawNumeric && sawString) {
		return &docColumn{typ: colUnbuildable}
	}
	if sawString {
		return buildStringColumn(ids, key, n, present, getProp)
	}
	// Numeric (or an all-absent column, which we treat as numeric: every present
	// bit is clear so it never yields a value, and a typed-but-empty column is
	// harmless — count(*) still counts rows, count(x)/sum see only nils).
	return buildNumericColumn(ids, key, n, present, getProp)
}

func buildNumericColumn(ids []types.NodeID, key string, n int, present bitset, getProp func(types.NodeID, string) (any, bool)) *docColumn {
	c := &docColumn{typ: ColNumeric, present: present, boxed: make([]any, n)}
	for ord, id := range ids {
		if !present.get(ord) {
			continue
		}
		v, _ := getProp(id, key)
		// Normalize to int64/float64 (the consumer's accumulator fast-path types)
		// and box once; the boxed dynamic type preserves int64-vs-float64.
		switch x := v.(type) {
		case int64:
			c.boxed[ord] = x
		case int:
			c.boxed[ord] = int64(x)
		case int32:
			c.boxed[ord] = int64(x)
		case float64:
			c.boxed[ord] = x
		case float32:
			c.boxed[ord] = float64(x)
		}
	}
	return c
}

func buildStringColumn(ids []types.NodeID, key string, n int, present bitset, getProp func(types.NodeID, string) (any, bool)) *docColumn {
	c := &docColumn{typ: ColString, present: present, codes: make([]uint32, n)}
	dictIdx := make(map[string]uint32)
	for ord, id := range ids {
		if !present.get(ord) {
			continue
		}
		s, _ := getProp(id, key)
		str := s.(string)
		code, ok := dictIdx[str]
		if !ok {
			code = uint32(len(c.dict))
			c.dict = append(c.dict, s) // box the original `any` once (s holds str)
			dictIdx[str] = code
		}
		c.codes[ord] = code
	}
	return c
}

// bitset is a fixed-size bit vector over ordinals (len = ceil(n/64) words).
type bitset []uint64

func newBitset(n int) bitset { return make(bitset, (n+63)/64) }

func (b bitset) set(i int) { b[i>>6] |= 1 << uint(i&63) }

func (b bitset) get(i int) bool { return b[i>>6]&(1<<uint(i&63)) != 0 }
