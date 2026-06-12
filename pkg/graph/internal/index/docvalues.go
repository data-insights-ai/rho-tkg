package index

import (
	"cmp"
	"slices"

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

	// ColNumeric: numF is always populated; numI + numIsInt preserve int64 type.
	numF     []float64
	numI     []int64
	numIsInt bitset // 1 = this row's value was int64; read back as int64

	// ColString: codes[ord] indexes into dict (sorted, deduplicated terms).
	dict  []string
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

// valueAt reconstructs the typed Go value at an ordinal whose present bit is set.
func (c *docColumn) valueAt(ord int) any {
	switch c.typ {
	case ColNumeric:
		if c.numIsInt.get(ord) {
			return c.numI[ord]
		}
		return c.numF[ord]
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
	c := &docColumn{typ: ColNumeric, present: present, numF: make([]float64, n), numI: make([]int64, n), numIsInt: newBitset(n)}
	for ord, id := range ids {
		if !present.get(ord) {
			continue
		}
		v, _ := getProp(id, key)
		switch x := v.(type) {
		case int64:
			c.numI[ord], c.numF[ord] = x, float64(x)
			c.numIsInt.set(ord)
		case int:
			c.numI[ord], c.numF[ord] = int64(x), float64(x)
			c.numIsInt.set(ord)
		case int32:
			c.numI[ord], c.numF[ord] = int64(x), float64(x)
			c.numIsInt.set(ord)
		case float64:
			c.numF[ord] = x
		case float32:
			c.numF[ord] = float64(x)
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
			c.dict = append(c.dict, str)
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
