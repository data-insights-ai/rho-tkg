package index

// PropertyStatsAccumulator maintains a HyperLogLog sketch (NDV) plus an
// exact min/max pair for one (label, property key) pair. Shared by the
// memory and badger store backends so the min/max-family and NDV-hash logic
// is defined exactly once. It is a pure accumulator — callers own their own
// mutual exclusion around every method call, the same
// caller-serializes-access contract the property-key presence counters
// (NodePropertyKeyStatsCapability) already rely on.
//
// Min/Max only track SCALAR-ORDERED value families: numeric (any of the
// property allowlist's int/uint/float types, compared via a float64
// projection) and string. A property populated only with bool or
// TemporalValue values — or any other indexable-but-unordered value — still
// contributes to Count (the presence counter) and NDV (this accumulator's
// sketch), but Min/Max stay nil: there is no total order defined for it here.
// See docs/query-planners.md "Min/Max value families" for the rationale.
//
// Mixed-family values for the same key (e.g. a property that holds int64 on
// some nodes and string on others) are unusual in a well-typed graph; when it
// happens the FIRST family observed wins and values from a later, different
// family are excluded from Min/Max (still counted via Count/NDV).
type PropertyStatsAccumulator struct {
	sketch   *HyperLogLog
	family   string // "n" numeric, "s" string, "" no ordered value observed yet
	min      any
	max      any
	dirty    bool   // true once a value that MIGHT be the min/max holder was removed
	writeGen uint64 // bumped on every Observe/Forget; see WriteGen
}

// NewPropertyStatsAccumulator constructs an accumulator with a fresh
// DefaultHLLPrecision sketch.
func NewPropertyStatsAccumulator() *PropertyStatsAccumulator {
	sketch, _ := NewHyperLogLog(DefaultHLLPrecision) // DefaultHLLPrecision is always in range
	return &PropertyStatsAccumulator{sketch: sketch}
}

// Observe folds an indexable scalar value into the sketch (NDV) and, if the
// value belongs to a scalar-ordered family, into the exact min/max.
//
// valueKey is the canonical IndexablePropertyValueKey for value (the same
// string the property index and the presence counter's
// ForEachIndexablePropertyValueKey already compute) — used as the NDV hash
// input so distinct Go types carrying an equal display value (e.g. int32(5)
// vs int64(5)) are counted as distinct, matching the property index's own
// equality semantics.
func (a *PropertyStatsAccumulator) Observe(valueKey string, value any) {
	if a == nil {
		return
	}
	a.writeGen++
	a.sketch.AddString(valueKey)
	fam := scalarOrderFamily(value)
	if fam == "" {
		return
	}
	if a.family == "" {
		a.family, a.min, a.max = fam, value, value
		return
	}
	if a.family != fam {
		return
	}
	if scalarLess(value, a.min) {
		a.min = value
	}
	if scalarLess(a.max, value) {
		a.max = value
	}
}

// Forget records that a value is LEAVING the population (a node carrying it
// was deleted, replaced, or lost the label). It does not attempt exact
// multiset bookkeeping — tracking every current holder of the extremal value
// would cost a counter per distinct value — so instead it marks the
// accumulator dirty whenever the departing value equals the current min or
// max, deferring an exact recomputation to the next Stats read (see Dirty /
// Rescan). NDV is intentionally NOT decremented: HyperLogLog has no removal
// operation, so a deleted value's contribution to NDV persists until the
// sketch is rebuilt from scratch (index load / restart rebuilds every
// accumulator from the then-current nodes). See docs/query-planners.md
// "Deletion semantics".
func (a *PropertyStatsAccumulator) Forget(value any) {
	if a == nil {
		return
	}
	a.writeGen++
	fam := scalarOrderFamily(value)
	if fam == "" || fam != a.family {
		return
	}
	if scalarEqual(value, a.min) || scalarEqual(value, a.max) {
		a.dirty = true
	}
}

// Dirty reports whether the next read must Rescan to recompute an exact
// min/max (see Forget). Safe on a nil receiver (returns false).
func (a *PropertyStatsAccumulator) Dirty() bool {
	return a != nil && a.dirty
}

// WriteGen returns a monotonically increasing counter bumped on every
// Observe/Forget. It is the optimistic-concurrency guard for a backend whose
// dirty-Rescan collects the current population WITHOUT holding the
// accumulator's lock continuously (badger — a cache-cold node fetch needs its
// own brief read lock, so the collect window cannot run under one write lock;
// sync.RWMutex is not reentrant). Such a backend reads WriteGen under the lock
// BEFORE the unlocked collect and re-reads it under the lock BEFORE committing
// Rescan: if it moved, a concurrent Observe/Forget landed during the collect
// and the freshly collected values are stale, so the caller must redo the
// collect rather than overwrite the live min/max with a stale snapshot. The
// memory backend holds one lock for the whole read and never consults this.
// Like every other method here it is caller-serialized (read/bumped only under
// the caller's mutual exclusion). Safe on a nil receiver (returns 0).
func (a *PropertyStatsAccumulator) WriteGen() uint64 {
	if a == nil {
		return 0
	}
	return a.writeGen
}

// Rescan replaces the accumulator's min/max/family with a freshly recomputed
// exact extreme pair over values (or clears them if values is empty) and
// clears the dirty flag. NDV is untouched — only min/max are subject to the
// delete-the-extremum rescan; values should be every property value
// currently held by a live node carrying the accumulator's (label, property
// key) pair.
func (a *PropertyStatsAccumulator) Rescan(values []any) {
	if a == nil {
		return
	}
	a.family, a.min, a.max = "", nil, nil
	for _, v := range values {
		fam := scalarOrderFamily(v)
		if fam == "" {
			continue
		}
		if a.family == "" {
			a.family, a.min, a.max = fam, v, v
			continue
		}
		if a.family != fam {
			continue
		}
		if scalarLess(v, a.min) {
			a.min = v
		}
		if scalarLess(a.max, v) {
			a.max = v
		}
	}
	a.dirty = false
}

// Snapshot returns the current NDV estimate plus min/max. It deliberately
// does NOT return Count — the caller (the store's NodePropertyStats) already
// tracks that via the sibling NodePropertyKeyStatsCapability presence
// counter and assembles the full store.PropertyStats itself.
func (a *PropertyStatsAccumulator) Snapshot() (ndv int64, min, max any) {
	if a == nil {
		return 0, nil, nil
	}
	return a.sketch.Estimate(), a.min, a.max
}

// Sketch returns an independent CLONE of the accumulator's raw HyperLogLog
// sketch — NOT the finished Estimate(). A single-shard backend never needs
// this (Snapshot's Estimate() is the answer), but a SHARDED backend (tiered)
// cannot fold NDV by summing per-shard Estimate()s — that over-counts any
// value present on more than one shard — so it must merge the RAW sketches
// register-max (HyperLogLog.Merge) and call Estimate() exactly once on the
// merged result. See docs/adr/0005-tiered-parity.md §3.1 and
// docs/query-planners.md "Tiered NDV fold". A clone is returned (not the
// live pointer) so the caller can Merge/mutate it outside this accumulator's
// lock without racing concurrent Observe/Forget calls. Safe on a nil
// receiver (returns nil).
func (a *PropertyStatsAccumulator) Sketch() *HyperLogLog {
	if a == nil {
		return nil
	}
	return a.sketch.Clone()
}

// CombineExtrema folds one more (min, max) extrema pair — typically another
// shard's or accumulator's Snapshot()/Rescan() result — into a running (min,
// max) pair, using the SAME first-family-wins mixed-family tie-break rule
// Observe/Rescan already use (see scalarOrderFamily/scalarLess above and
// docs/query-planners.md "Min/Max value families"): whichever ordered family
// is adopted FIRST governs from then on, and a later pair from a DIFFERENT
// ordered family is ignored (still folded into Count/NDV by the caller, just
// not Min/Max). This is the reusable comparison helper the tiered backend's
// cross-shard PropertyStats fold calls — see
// docs/adr/0005-tiered-parity.md §3.1 ("reuse the existing comparison helper
// rather than re-implementing").
//
// A nil incomingMin (that shard/accumulator observed no ordered value at
// all) is a safe no-op; a nil runningMin simply adopts the incoming pair.
func CombineExtrema(runningMin, runningMax, incomingMin, incomingMax any) (min, max any) {
	if incomingMin == nil {
		return runningMin, runningMax
	}
	if runningMin == nil {
		return incomingMin, incomingMax
	}
	if scalarOrderFamily(runningMin) != scalarOrderFamily(incomingMin) {
		return runningMin, runningMax
	}
	min, max = runningMin, runningMax
	if scalarLess(incomingMin, min) {
		min = incomingMin
	}
	if scalarLess(max, incomingMax) {
		max = incomingMax
	}
	return min, max
}

// scalarOrderFamily returns the ordering family for min/max tracking: "n"
// for any numeric allowlist type, "s" for string, "" (not tracked) for
// anything else (bool, TemporalValue, nil, slices, maps, custom types).
func scalarOrderFamily(v any) string {
	switch v.(type) {
	case int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64:
		return "n"
	case string:
		return "s"
	default:
		return ""
	}
}

// scalarLess compares two values of the SAME family (as determined by
// scalarOrderFamily; callers must only compare same-family values). Numeric
// values are projected to float64 for comparison — sufficient for planner
// range estimation, though int64/uint64 magnitudes beyond 2^53 lose exact
// comparison precision at the margin; the ORIGINAL, unconverted value is
// always what gets stored as Min/Max (only the comparison uses the
// projection).
func scalarLess(a, b any) bool {
	if as, ok := a.(string); ok {
		bs, _ := b.(string)
		return as < bs
	}
	return numericValue(a) < numericValue(b)
}

// scalarEqual reports whether two same-family values are equal, using the
// same string/numeric-projection rules as scalarLess.
func scalarEqual(a, b any) bool {
	if as, ok := a.(string); ok {
		bs, _ := b.(string)
		return as == bs
	}
	return numericValue(a) == numericValue(b)
}

// numericValue projects any property allowlist numeric type to float64 for
// ordering purposes.
func numericValue(v any) float64 {
	switch n := v.(type) {
	case int:
		return float64(n)
	case int8:
		return float64(n)
	case int16:
		return float64(n)
	case int32:
		return float64(n)
	case int64:
		return float64(n)
	case uint:
		return float64(n)
	case uint8:
		return float64(n)
	case uint16:
		return float64(n)
	case uint32:
		return float64(n)
	case uint64:
		return float64(n)
	case float32:
		return float64(n)
	case float64:
		return n
	default:
		return 0
	}
}
