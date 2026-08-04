package index

import (
	"cmp"
	"math"
	"slices"
	"strings"
	"sync"

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
//
// Storage is TYPED (`[]int64`/`[]float64`/`[]string`); the boxed `[]any` views the
// older doors hand out are built LAZILY, on first use, and cached. That ordering is
// deliberate and is the whole point of the layout:
//
//   - a typed reader (the column-scan capability) never materialises a boxed view
//     at all, so it costs 8 bytes per numeric row and ZERO heap objects, against
//     the 24 bytes (16B interface header + 8B boxed int64) and one object per row
//     an eagerly-boxed column costs;
//   - a boxed reader pays exactly what it paid when the column was boxed at build —
//     once per snapshot, amortised over every read — so it does NOT regress. Boxing
//     per read instead would allocate on every value, because Go heap-escapes an
//     int64 on interface conversion outside the small-value cache. `[]any` is in the
//     PUBLISHED contract (types.NodeColumnReader, nodes.API.ForEachDocValues), so
//     that regression would land on every existing consumer.
//
// Contains a sync.Once, so a docColumn must only ever be handled as *docColumn
// (go vet's copylocks enforces this).
type docColumn struct {
	typ     ColType
	present bitset // 1 = node has the property; 0 = absent/null
	n       int    // ordinal count (len of the label's nodeIDs vector)

	// ColNumeric: typed storage in one of three exact states, so a column never
	// carries an array it does not need. int64-vs-float64 must be PRESERVED (not
	// collapsed to float64) or a consumer's accumulator stops being bit-exact above
	// 2^53 — the invariant the previous boxed representation held via each value's
	// dynamic type.
	//
	//	isFloat == nil && flts == nil  -> uniformly int64  (values in ints)
	//	isFloat == nil && ints == nil  -> uniformly float64 (values in flts)
	//	isFloat != nil                 -> mixed; isFloat.get(ord) selects flts[ord]
	//
	// The uniform-int64 case is overwhelmingly the common one and stores a single
	// []int64 with no selector bitset.
	ints    []int64
	flts    []float64
	isFloat bitset

	// ColString: codes[ord] indexes into dict (deduplicated terms).
	dict  []string
	codes []uint32

	// Lazily-built boxed views of the typed storage above. Safe without a lock
	// because a docColumn is immutable once built.
	boxOnce   sync.Once
	boxed     []any // ColNumeric, per ordinal
	dictOnce  sync.Once
	dictBoxed []any // ColString, per DISTINCT term (not per row)
}

// LabelDocValues is the immutable set of aligned columns for one label, built at a
// single store mutation epoch. All columns share NodeIDs (the ordinal vector), so
// ordinal i refers to the same node in every column.
type LabelDocValues struct {
	epoch   uint64
	nodeIDs []types.NodeID
	cols    map[string]*docColumn // propKey -> column (only buildable keys present)

	// Validity bounds, ordinal-aligned with nodeIDs, or nil when the builder was
	// given no temporal accessor. A zero validTo means open-ended (the store-wide
	// convention), NOT "ends at the epoch"; an entity with no temporal metadata at
	// all gets zero in both, which is indistinguishable from "valid for all time"
	// and is why hasTemporal is tracked separately rather than inferred.
	//
	// IMPORTANT — these do NOT filter membership. A snapshot's rows remain the FULL
	// unfiltered label set (see the file header); these columns let a reader EVALUATE
	// a valid-time predicate without materialising the entity, they do not pre-apply
	// one. Any reader that wants an as-of view must still test them per row (or skip
	// blocks via zoneBlocks, below).
	validFrom   []int64
	validTo     []int64
	hasTemporal bool

	// Zone map: per-block min/max of validFrom/validTo over zoneBlockSize ordinals,
	// so a valid-time predicate can skip whole blocks on metadata alone instead of
	// testing every row. nil when hasTemporal is false.
	zoneMinFrom []int64
	zoneMaxFrom []int64
	zoneMinTo   []int64
	zoneMaxTo   []int64
	// zoneOpenEnded[b] reports whether block b contains any row with validTo == 0
	// (open-ended). Such a row has no upper bound, so zoneMaxTo cannot represent it
	// and a block containing one can never be skipped on its upper bound. Folding
	// the zero into zoneMaxTo as a literal 0 would make the block look like it ends
	// at the epoch and silently drop live rows.
	zoneOpenEnded []bool
}

// zoneBlockSize is the zone-map granularity, matched to the column-scan batch size
// so a skipped block is exactly a skipped batch.
const zoneBlockSize = 4096

// HasTemporal reports whether this snapshot carries validity columns.
func (l *LabelDocValues) HasTemporal() bool { return l.hasTemporal }

// ValidFrom/ValidTo are the ordinal-aligned validity bounds, or nil when the
// snapshot has none. Immutable; callers must not write to them.
func (l *LabelDocValues) ValidFrom() []int64 { return l.validFrom }
func (l *LabelDocValues) ValidTo() []int64   { return l.validTo }

// BlockCanMatch reports whether the ordinal block starting at `start` can contain
// any row matching the half-open query window [qFrom, qTo). A false means the whole
// block is skippable; a true means "maybe", so the caller still tests rows.
//
// qTo == 0 means NO FILTER (never skip). The row predicate this approximates is
// storeutil's: a row [f,t) matches when f < qTo and (t == 0 || t > qFrom). The
// upper bound is STRICT — a block whose earliest row starts exactly at qTo cannot
// match, and using >= here rather than > would wrongly retain it.
func (l *LabelDocValues) BlockCanMatch(start int, qFrom, qTo int64) bool {
	if !l.hasTemporal || qTo == 0 {
		return true // no zone map, or no filter — never skip
	}
	b := start / zoneBlockSize
	if b < 0 || b >= len(l.zoneMinFrom) {
		return true
	}
	// Every row in the block starts at or after the query's upper bound -> no overlap.
	if l.zoneMinFrom[b] >= qTo {
		return false
	}
	// Every row in the block ended at or before the query's lower bound -> no
	// overlap. Only sound when NO row in the block is open-ended, since an
	// open-ended row has no upper bound to compare.
	if !l.zoneOpenEnded[b] && l.zoneMaxTo[b] != 0 && l.zoneMaxTo[b] <= qFrom {
		return false
	}
	return true
}

// buildZoneMap computes per-block min/max over the validity columns.
func (l *LabelDocValues) buildZoneMap() {
	n := len(l.nodeIDs)
	blocks := (n + zoneBlockSize - 1) / zoneBlockSize
	l.zoneMinFrom = make([]int64, blocks)
	l.zoneMaxFrom = make([]int64, blocks)
	l.zoneMinTo = make([]int64, blocks)
	l.zoneMaxTo = make([]int64, blocks)
	l.zoneOpenEnded = make([]bool, blocks)

	for b := 0; b < blocks; b++ {
		lo := b * zoneBlockSize
		hi := min(lo+zoneBlockSize, n)
		minF, maxF := l.validFrom[lo], l.validFrom[lo]
		var minT, maxT int64
		open, seenClosed := false, false
		for ord := lo; ord < hi; ord++ {
			f, t := l.validFrom[ord], l.validTo[ord]
			minF, maxF = min(minF, f), max(maxF, f)
			if t == 0 {
				open = true
				continue // an open-ended row contributes no upper bound
			}
			if !seenClosed {
				minT, maxT, seenClosed = t, t, true
				continue
			}
			minT, maxT = min(minT, t), max(maxT, t)
		}
		l.zoneMinFrom[b], l.zoneMaxFrom[b] = minF, maxF
		l.zoneMinTo[b], l.zoneMaxTo[b] = minT, maxT
		l.zoneOpenEnded[b] = open
	}
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

// ColumnView is a read-only, allocation-free handle on one built column's TYPED
// storage — the door a columnar reader uses instead of ForEachRow, so it never
// materialises the boxed view.
//
// The slices are the column's own backing arrays and are immutable; a caller must
// not write to them. Which of Ints/Flts is live at an ordinal is decided by
// IsFloat, never by the caller's expectation: a numeric column may be uniformly
// int64, uniformly float64, or mixed, and reading the wrong half yields a
// plausible zero rather than an error.
type ColumnView struct {
	Type ColType
	N    int

	Ints  []int64   // ColNumeric: live where !IsFloat(ord)
	Flts  []float64 // ColNumeric: live where IsFloat(ord)
	Dict  []string  // ColString: distinct terms
	Codes []uint32  // ColString: Codes[ord] indexes Dict

	present bitset
	isFloat bitset
}

// Present reports whether the node at ord has the property (a cleared bit is the
// absent/null case the boxed door renders as a nil value).
func (v ColumnView) Present(ord int) bool { return v.present.get(ord) }

// IsFloat reports whether ord's numeric value lives in Flts rather than Ints.
func (v ColumnView) IsFloat(ord int) bool {
	switch {
	case v.isFloat != nil:
		return v.isFloat.get(ord)
	default:
		return v.Flts != nil // uniformly float64 when the int half was never built
	}
}

// StringAt returns the dictionary term at ord (a header copy, no allocation).
func (v ColumnView) StringAt(ord int) string { return v.Dict[v.Codes[ord]] }

// Mixed reports whether a numeric column holds BOTH integral and floating values,
// so it has no single type. A consumer with a one-kind-per-column output contract
// must refuse such a column rather than pick a half: reading the int array for a
// float row returns a plausible wrong number, not an error.
func (v ColumnView) Mixed() bool { return v.isFloat != nil }

// View returns the typed handle for a usable column, or ok=false for an absent or
// unbuildable key — mirroring Has exactly, so a columnar reader declines the whole
// query rather than reading an unbuildable column as spurious nulls.
func (l *LabelDocValues) View(key string) (ColumnView, bool) {
	c, ok := l.cols[key]
	if !ok || c.typ == colUnbuildable {
		return ColumnView{}, false
	}
	return ColumnView{
		Type: c.typ, N: c.n,
		Ints: c.ints, Flts: c.flts,
		Dict: c.dict, Codes: c.codes,
		present: c.present, isFloat: c.isFloat,
	}, true
}

// NodeIDs is the shared ordinal vector every column of this snapshot aligns to.
// Immutable; callers must not write to it.
func (l *LabelDocValues) NodeIDs() []types.NodeID { return l.nodeIDs }

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
// rather than reading an unbuildable column as spurious nulls.
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
// BUILDABLE column must count every member).
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
// arms are allocation-free in steady state: the boxed views are materialised once,
// on the first boxed read of this column, and cached (see docColumn).
func (c *docColumn) valueAt(ord int) any {
	switch c.typ {
	case ColNumeric:
		return c.boxedNumeric()[ord]
	case ColString:
		return c.boxedDict()[c.codes[ord]]
	default:
		return nil
	}
}

// numericAt returns the typed numeric value at an ordinal whose present bit is
// set, as (int64, float64, isFloat) — the allocation-free accessor the typed
// readers use. Reading the wrong half of the pair is the hazard this signature
// exists to prevent: isFloat, not the caller's expectation, decides which is live.
func (c *docColumn) numericAt(ord int) (int64, float64, bool) {
	switch {
	case c.isFloat != nil: // mixed — the selector decides per row
		if c.isFloat.get(ord) {
			return 0, c.flts[ord], true
		}
		return c.ints[ord], 0, false
	case c.flts != nil: // uniformly float64
		return 0, c.flts[ord], true
	default: // uniformly int64
		return c.ints[ord], 0, false
	}
}

// boxedNumeric materialises and caches the []any view of a numeric column. Absent
// ordinals stay nil — the exact shape GetProperty returns for a missing property.
func (c *docColumn) boxedNumeric() []any {
	c.boxOnce.Do(func() {
		b := make([]any, c.n)
		for ord := 0; ord < c.n; ord++ {
			if !c.present.get(ord) {
				continue
			}
			if iv, fv, isF := c.numericAt(ord); isF {
				b[ord] = fv
			} else {
				b[ord] = iv
			}
		}
		c.boxed = b
	})
	return c.boxed
}

// boxedDict materialises and caches the []any view of a string dictionary. Sized by
// DISTINCT terms, not rows, so it is cheap even for a large column.
func (c *docColumn) boxedDict() []any {
	c.dictOnce.Do(func() {
		b := make([]any, len(c.dict))
		for i, s := range c.dict {
			b[i] = s
		}
		c.dictBoxed = b
	})
	return c.dictBoxed
}

// BuildLabelDocValues builds aligned columns for propKeys over the membership set.
// nodeIDs is the FULL label membership (unsorted is fine — it is sorted here once,
// the per-query SortNodeIDs the column amortizes away). getProp(id, key) returns
// the raw stored property value exactly as the per-node path reads it. The
// returned LabelDocValues contains only buildable columns; unbuildable propKeys
// are omitted (Has reports false), so the consumer falls back for them.
// getTemporal, when non-nil, supplies each node's validity bounds so the snapshot
// can carry them as columns and build a zone map. Pass nil to build value columns
// only; a reader then sees HasTemporal() == false and must fall back for any
// valid-time predicate rather than assuming "no bounds" means "valid for all time".
func BuildLabelDocValues(epoch uint64, nodeIDs []types.NodeID, propKeys []string,
	getProp func(types.NodeID, string) (any, bool),
	getTemporal func(types.NodeID) (validFrom, validTo int64, ok bool)) *LabelDocValues {

	ids := slices.Clone(nodeIDs)
	slices.SortFunc(ids, func(a, b types.NodeID) int {
		return cmp.Compare(a.SnowflakeID(), b.SnowflakeID())
	})
	n := len(ids)

	l := &LabelDocValues{epoch: epoch, nodeIDs: ids, cols: make(map[string]*docColumn, len(propKeys))}

	if getTemporal != nil && n > 0 {
		l.validFrom = make([]int64, n)
		l.validTo = make([]int64, n)
		l.hasTemporal = true
		for ord, id := range ids {
			// A node with no metadata keeps (0,0) — the same shape as an unbounded
			// entity, which is why the column pair alone is not a membership filter.
			if f, t, ok := getTemporal(id); ok {
				l.validFrom[ord], l.validTo[ord] = f, t
			}
		}
		l.buildZoneMap()
	}
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
		case int64, int, int32, int16, int8,
			uint64, uint, uint32, uint16, uint8,
			float64, float32:
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
	c := &docColumn{typ: ColNumeric, present: present, n: n}

	ints := make([]int64, n)
	var flts []float64
	var isFloat bitset
	presentCount, floatCount := 0, 0

	for ord, id := range ids {
		if !present.get(ord) {
			continue
		}
		presentCount++
		v, _ := getProp(id, key)
		iv, fv, isF := normalizeNumeric(v)
		if !isF {
			ints[ord] = iv
			continue
		}
		if flts == nil { // first float seen — allocate the float half lazily
			flts = make([]float64, n)
			isFloat = newBitset(n)
		}
		flts[ord] = fv
		isFloat.set(ord)
		floatCount++
	}

	// Collapse to the cheapest exact representation. Keeping BOTH arrays when the
	// column turns out uniform would silently double its memory, which is the cost
	// this typed layout exists to remove.
	switch {
	case floatCount == 0: // uniformly int64 (incl. an all-absent column)
		c.ints = ints
	case floatCount == presentCount: // uniformly float64 — no selector needed
		c.flts = flts
	default: // genuinely mixed
		c.ints, c.flts, c.isFloat = ints, flts, isFloat
	}
	return c
}

// normalizeNumeric maps any stored numeric width onto the consumer's accumulator
// fast-path types, reporting which half of the (int64, float64) pair is live.
//
// All signed/unsigned integer widths up to 32 bits fit int64 exactly. uint/uint64
// need a range check: only values <= math.MaxInt64 fit, and a larger magnitude
// becomes float64 — the same magnitude-only precision trade-off numericValue in
// property_stats_accumulator.go already documents and accepts, rather than letting
// a raw int64(x) cast silently wrap negative.
func normalizeNumeric(v any) (int64, float64, bool) {
	switch x := v.(type) {
	case int64:
		return x, 0, false
	case int:
		return int64(x), 0, false
	case int32:
		return int64(x), 0, false
	case int16:
		return int64(x), 0, false
	case int8:
		return int64(x), 0, false
	case uint:
		return normalizeUint64(uint64(x))
	case uint64:
		return normalizeUint64(x)
	case uint32:
		return int64(x), 0, false
	case uint16:
		return int64(x), 0, false
	case uint8:
		return int64(x), 0, false
	case float64:
		return 0, x, true
	case float32:
		return 0, float64(x), true
	}
	return 0, 0, false
}

// normalizeUint64 keeps a uint64 as int64 when it fits exactly (every practical
// counter/ID/age value), else widens to float64.
func normalizeUint64(v uint64) (int64, float64, bool) {
	if v <= math.MaxInt64 {
		return int64(v), 0, false
	}
	return 0, float64(v), true
}

func buildStringColumn(ids []types.NodeID, key string, n int, present bitset, getProp func(types.NodeID, string) (any, bool)) *docColumn {
	c := &docColumn{typ: ColString, present: present, n: n, codes: make([]uint32, n)}
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
