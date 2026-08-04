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
	nCols := len(props)
	batch := &ColumnBatch{
		IDs:       make([]types.NodeID, 0, ColumnScanBatchRows),
		ValidFrom: make([]int64, 0, ColumnScanBatchRows),
		ValidTo:   make([]int64, 0, ColumnScanBatchRows),
		Kinds:     make([]ColumnKind, nCols),
		Ints:      make([][]int64, nCols),
		Flts:      make([][]float64, nCols),
		Strs:      make([][]string, nCols),
		Bools:     make([][]bool, nCols),
		Null:      make([][]bool, nCols),
	}
	kindKnown := make([]bool, nCols)
	reset := func() {
		batch.IDs = batch.IDs[:0]
		batch.ValidFrom = batch.ValidFrom[:0]
		batch.ValidTo = batch.ValidTo[:0]
		for c := range nCols {
			batch.Ints[c] = batch.Ints[c][:0]
			batch.Flts[c] = batch.Flts[c][:0]
			batch.Strs[c] = batch.Strs[c][:0]
			batch.Bools[c] = batch.Bools[c][:0]
			batch.Null[c] = batch.Null[c][:0]
		}
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
				appendAbsent(batch, c, kindKnown[c])
				continue
			}
			if !kindKnown[c] {
				batch.Kinds[c], kindKnown[c] = kind, true
			}
			if batch.Kinds[c] != kind {
				// Reporting a mismatch absent is right for string-versus-number —
				// reading a string as a zero int would be worse than a missing row.
				// It is NOT right for int64-versus-float64, because the same logical
				// property is routinely stored both ways (one entity's qty is 2,
				// another's 2.0).
				if numericKind(batch.Kinds[c]) && numericKind(kind) {
					// REFUSE, do not widen. Promoting to float64 is lossless for the
					// values and not for the caller: a consumer that decides
					// something from OBSERVING a mixed column sees a uniform one and
					// decides the opposite. A visible refusal is the honest outcome;
					// the caller falls back to the row path.
					return ErrMixedNumericColumn
				}
				appendAbsent(batch, c, true)
				continue
			}
			batch.Null[c] = append(batch.Null[c], false)
			switch kind {
			case ColInt64:
				batch.Ints[c] = append(batch.Ints[c], i64)
			case ColFloat64:
				batch.Flts[c] = append(batch.Flts[c], f64)
			case ColString:
				batch.Strs[c] = append(batch.Strs[c], str)
			case ColBool:
				batch.Bools[c] = append(batch.Bools[c], b)
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

// numericKind reports whether a column kind participates in float widening.
func numericKind(k ColumnKind) bool { return k == ColInt64 || k == ColFloat64 }

// appendAbsent records a missing value, keeping every column slice the same length
// as IDs so a consumer can index them together.
func appendAbsent(b *ColumnBatch, c int, known bool) {
	b.Null[c] = append(b.Null[c], true)
	if !known {
		return
	}
	switch b.Kinds[c] {
	case ColInt64:
		b.Ints[c] = append(b.Ints[c], 0)
	case ColFloat64:
		b.Flts[c] = append(b.Flts[c], 0)
	case ColString:
		b.Strs[c] = append(b.Strs[c], "")
	case ColBool:
		b.Bools[c] = append(b.Bools[c], false)
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
