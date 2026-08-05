package store

import (
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// ColumnScanBatchRows is how many nodes one ColumnBatch carries. Large enough to
// amortise the callback, small enough that the buffers stay cache-resident. Also
// the zone-map block size on a columnar snapshot, so a skipped block is exactly a
// skipped batch.
const ColumnScanBatchRows = 4096

// ScanColumnsFromNodes converts materialised rows into typed column batches and
// feeds them to fn. It is the SHARED row path behind NodeColumnScanCapability: the
// fallback every backend needs for inputs a columnar snapshot cannot serve
// (membership not in RAM, label over cap, a column whose values are not uniformly
// typed).
//
// Backends must not duplicate this. Two copies of the kind-resolution rules below
// would be two chances to disagree about when a column REFUSES versus when it
// reports a row absent, and that disagreement is invisible until a consumer gets
// different answers from two stores holding the same data.
//
// The COLUMN KIND is decided by the first non-absent value in each column and every
// later value must convert to it; a value that does not is treated as absent rather
// than coerced, because silently reading a string column as zero would be worse
// than reporting the row as missing.
func ScanColumnsFromNodes(nodes []*types.Node, props []string,
	fn func(*ColumnBatch) bool) error {

	if len(nodes) == 0 {
		return nil
	}
	batch := &ColumnBatch{
		IDs:        make([]types.NodeID, 0, ColumnScanBatchRows),
		ValidFrom:  make([]int64, 0, ColumnScanBatchRows),
		ValidTo:    make([]int64, 0, ColumnScanBatchRows),
		ColumnData: NewColumnData(len(props)),
	}
	kindKnown := make([]bool, len(props))
	cd := &batch.ColumnData
	reset := func() {
		batch.IDs = batch.IDs[:0]
		batch.ValidFrom = batch.ValidFrom[:0]
		batch.ValidTo = batch.ValidTo[:0]
		batch.reset(len(props))
	}

	for _, n := range nodes {
		batch.IDs = append(batch.IDs, n.InternalID())
		// ValidRange, not Temporal(): the latter must copy the metadata for a frozen
		// entity, and every entity a store scan hands back is frozen — one
		// allocation per row across the whole scan.
		vf, vt, _ := n.ValidRange()
		batch.ValidFrom = append(batch.ValidFrom, int64(vf))
		batch.ValidTo = append(batch.ValidTo, int64(vt))
		for c, key := range props {
			raw, found := n.GetProperty(key)
			kind, i64, f64, str, b, ok := classifyScalar(raw)
			if !found || !ok {
				cd.appendAbsent(c, kindKnown[c])
				continue
			}
			if !kindKnown[c] {
				cd.Kinds[c], kindKnown[c] = kind, true
			}
			if cd.Kinds[c] != kind {
				// Reporting a mismatch absent is right for string-versus-number —
				// reading a string as a zero int would be worse than a missing row.
				// It is NOT right for int64-versus-float64, because the same logical
				// property is routinely stored both ways (one entity's qty is 2,
				// another's 2.0).
				if numericKind(cd.Kinds[c]) && numericKind(kind) {
					// REFUSE, do not widen. Promoting to float64 is lossless for the
					// values and not for the caller: a consumer that decides something
					// from OBSERVING a mixed column sees a uniform one and decides the
					// opposite. A visible refusal is the honest outcome; the caller
					// falls back to the row path.
					return ErrMixedNumericColumn
				}
				cd.appendAbsent(c, true)
				continue
			}
			cd.Null[c] = append(cd.Null[c], false)
			switch kind {
			case ColInt64:
				cd.Ints[c] = append(cd.Ints[c], i64)
			case ColFloat64:
				cd.Flts[c] = append(cd.Flts[c], f64)
			case ColString:
				cd.Strs[c] = append(cd.Strs[c], str)
			case ColBool:
				cd.Bools[c] = append(cd.Bools[c], b)
			}
		}
		if len(batch.IDs) >= ColumnScanBatchRows {
			if !fn(batch) {
				return nil
			}
			reset()
		}
	}
	if len(batch.IDs) > 0 {
		fn(batch)
	}
	return nil
}

// ScanColumnsFromRels is the relationship sibling of ScanColumnsFromNodes and the
// SHARED row path behind RelColumnScanCapability.
//
// It differs from the node version only in what identifies a row — a RelID plus its
// two endpoints instead of a NodeID. Every rule about column KINDS, about when a
// value is reported absent, and about when the scan REFUSES a mixed numeric column
// is ColumnData.appendRow, called by both. That is deliberate: a second copy of
// those rules would let nodes and relationships disagree for identically-shaped
// data, which is the backend-level hazard this file already warns about, one level
// up.
func ScanColumnsFromRels(rels []*types.Relationship, props []string,
	fn func(*RelColumnBatch) bool) error {

	if len(rels) == 0 {
		return nil
	}
	batch := &RelColumnBatch{
		IDs:        make([]types.RelID, 0, ColumnScanBatchRows),
		StartIDs:   make([]types.NodeID, 0, ColumnScanBatchRows),
		EndIDs:     make([]types.NodeID, 0, ColumnScanBatchRows),
		ValidFrom:  make([]int64, 0, ColumnScanBatchRows),
		ValidTo:    make([]int64, 0, ColumnScanBatchRows),
		ColumnData: NewColumnData(len(props)),
	}
	kindKnown := make([]bool, len(props))
	cd := &batch.ColumnData
	reset := func() {
		batch.IDs = batch.IDs[:0]
		batch.StartIDs = batch.StartIDs[:0]
		batch.EndIDs = batch.EndIDs[:0]
		batch.ValidFrom = batch.ValidFrom[:0]
		batch.ValidTo = batch.ValidTo[:0]
		batch.reset(len(props))
	}

	for _, r := range rels {
		batch.IDs = append(batch.IDs, r.InternalID())
		batch.StartIDs = append(batch.StartIDs, r.StartNodeID())
		batch.EndIDs = append(batch.EndIDs, r.EndNodeID())
		vf, vt, _ := r.ValidRange()
		batch.ValidFrom = append(batch.ValidFrom, int64(vf))
		batch.ValidTo = append(batch.ValidTo, int64(vt))
		for c, key := range props {
			raw, found := r.GetProperty(key)
			kind, i64, f64, str, b, ok := classifyScalar(raw)
			if !found || !ok {
				cd.appendAbsent(c, kindKnown[c])
				continue
			}
			if !kindKnown[c] {
				cd.Kinds[c], kindKnown[c] = kind, true
			}
			if cd.Kinds[c] != kind {
				// Reporting a mismatch absent is right for string-versus-number —
				// reading a string as a zero int would be worse than a missing row.
				// It is NOT right for int64-versus-float64, because the same logical
				// property is routinely stored both ways (one entity's qty is 2,
				// another's 2.0).
				if numericKind(cd.Kinds[c]) && numericKind(kind) {
					// REFUSE, do not widen. Promoting to float64 is lossless for the
					// values and not for the caller: a consumer that decides something
					// from OBSERVING a mixed column sees a uniform one and decides the
					// opposite. A visible refusal is the honest outcome; the caller
					// falls back to the row path.
					return ErrMixedNumericColumn
				}
				cd.appendAbsent(c, true)
				continue
			}
			cd.Null[c] = append(cd.Null[c], false)
			switch kind {
			case ColInt64:
				cd.Ints[c] = append(cd.Ints[c], i64)
			case ColFloat64:
				cd.Flts[c] = append(cd.Flts[c], f64)
			case ColString:
				cd.Strs[c] = append(cd.Strs[c], str)
			case ColBool:
				cd.Bools[c] = append(cd.Bools[c], b)
			}
		}
		if len(batch.IDs) >= ColumnScanBatchRows {
			if !fn(batch) {
				return nil
			}
			reset()
		}
	}
	if len(batch.IDs) > 0 {
		fn(batch)
	}
	return nil
}

// NewColumnData allocates the per-column slices for nCols columns.
//
// Exported because a backend with its own columnar path (badger) builds batches
// itself rather than going through the row drivers here, and should allocate the
// payload through one constructor rather than restating the field list.
func NewColumnData(nCols int) ColumnData {
	return ColumnData{
		Kinds: make([]ColumnKind, nCols),
		Ints:  make([][]int64, nCols),
		Flts:  make([][]float64, nCols),
		Strs:  make([][]string, nCols),
		Bools: make([][]bool, nCols),
		Null:  make([][]bool, nCols),
	}
}

// reset truncates every column, keeping the backing arrays for reuse.
func (cd *ColumnData) reset(nCols int) {
	for c := range nCols {
		cd.Ints[c] = cd.Ints[c][:0]
		cd.Flts[c] = cd.Flts[c][:0]
		cd.Strs[c] = cd.Strs[c][:0]
		cd.Bools[c] = cd.Bools[c][:0]
		cd.Null[c] = cd.Null[c][:0]
	}
}

// WHY THE PER-VALUE LOOP IS WRITTEN OUT IN BOTH DRIVERS ABOVE, AND NOT SHARED.
//
// It was shared, three ways, and each cost double-digit percentages on this path
// (interleaved A/B against the pre-change tree, benchstat, allocations identical in
// every variant):
//
//	a getter callback passed to one shared loop   +16%
//	one shared call per VALUE                     +10%
//	one shared call per ROW via a scratch []any   +23%
//
// The rules are cheap; the call is not. At ~20k values per scan a non-inlinable call
// dominates, and boxing into a scratch slice to amortise it costs more than it saves.
//
// So the PRIMITIVES stay shared — classifyScalar, appendAbsent, numericKind, exactly
// as before RC4 — and only the ~25-line orchestration is written twice. The drift
// this risks is real and is pinned by TestColumnDrivers_NodeAndRelAgree, which feeds
// identical property values through BOTH drivers and asserts byte-identical columns
// including the mixed-numeric refusal. An executable equivalence check is a stronger
// guarantee than shared code anyway: it also catches a divergence introduced by a
// change that shared code would have happily compiled.

// numericKind reports whether a column kind participates in float widening.
func numericKind(k ColumnKind) bool { return k == ColInt64 || k == ColFloat64 }

// appendAbsent records a missing value, keeping every column slice the same length
// as IDs so a consumer can index them together.
func (cd *ColumnData) appendAbsent(c int, known bool) {
	cd.Null[c] = append(cd.Null[c], true)
	if !known {
		return
	}
	switch cd.Kinds[c] {
	case ColInt64:
		cd.Ints[c] = append(cd.Ints[c], 0)
	case ColFloat64:
		cd.Flts[c] = append(cd.Flts[c], 0)
	case ColString:
		cd.Strs[c] = append(cd.Strs[c], "")
	case ColBool:
		cd.Bools[c] = append(cd.Bools[c], false)
	}
}

// classifyScalar converts a stored property value to its column kind and typed
// value. The integral widenings here are exactly the ones a consumer would
// otherwise perform after the type was erased, each costing a heap box.
func classifyScalar(v any) (k ColumnKind, i64 int64, f64 float64, s string, b bool, ok bool) {
	switch t := v.(type) {
	case int64:
		return ColInt64, t, 0, "", false, true
	case int:
		return ColInt64, int64(t), 0, "", false, true
	case int32:
		return ColInt64, int64(t), 0, "", false, true
	case types.Instant:
		return ColInt64, int64(t), 0, "", false, true
	case float64:
		return ColFloat64, 0, t, "", false, true
	case float32:
		return ColFloat64, 0, float64(t), "", false, true
	case string:
		return ColString, 0, 0, t, false, true
	case bool:
		return ColBool, 0, 0, "", t, true
	}
	return 0, 0, 0, "", false, false
}
