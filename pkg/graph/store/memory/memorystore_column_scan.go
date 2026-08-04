package memory

import (
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// columnScanBatchRows is how many nodes one ColumnBatch carries. Large enough to
// amortise the callback, small enough that the buffers stay cache-resident.
const columnScanBatchRows = 4096

// ScanNodeColumns implements store.NodeColumnScanCapability.
//
// WHY THIS EXISTS ALONGSIDE NodesByLabel. A consumer that wants scalar columns gets
// []*types.Node and reads each property through GetProperty, which returns `any`.
// Values stored as Instant, int or int32 then cost a heap box each on the way to
// int64, and the caller cannot avoid that because the conversion happens after the
// type has been erased. Here the stored type is still known, so the conversion goes
// straight into a typed slice and no box is created at all.
//
// The COLUMN KIND is decided by the first non-absent value in each column and every
// later value must convert to it; a value that does not is treated as absent rather
// than coerced, because silently reading a string column as zero would be worse
// than reporting the row as missing.
func (ms *Store) ScanNodeColumns(token uint16, props []string, opts storecontract.QueryOpts,
	fn func(*storecontract.ColumnBatch) bool) error {

	if ms == nil {
		return ErrNilStore
	}
	if fn == nil {
		return errNilIterationCallback()
	}
	nodes, err := ms.NodesByLabel(token, opts)
	if err != nil {
		return err
	}
	if len(nodes) == 0 {
		return nil
	}

	nCols := len(props)
	batch := &storecontract.ColumnBatch{
		IDs:       make([]types.NodeID, 0, columnScanBatchRows),
		ValidFrom: make([]int64, 0, columnScanBatchRows),
		ValidTo:   make([]int64, 0, columnScanBatchRows),
		Kinds: make([]storecontract.ColumnKind, nCols),
		Ints:  make([][]int64, nCols),
		Flts:  make([][]float64, nCols),
		Strs:  make([][]string, nCols),
		Bools: make([][]bool, nCols),
		Null:  make([][]bool, nCols),
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
		var vf, vt int64
		if tm := n.Temporal(); tm != nil {
			vf, vt = int64(tm.ValidFrom), int64(tm.ValidTo)
		}
		batch.ValidFrom = append(batch.ValidFrom, vf)
		batch.ValidTo = append(batch.ValidTo, vt)
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
				// A column whose type is not stable cannot be a typed column;
				// report the row absent rather than coerce it.
				appendAbsent(batch, c, true)
				continue
			}
			batch.Null[c] = append(batch.Null[c], false)
			switch kind {
			case storecontract.ColInt64:
				batch.Ints[c] = append(batch.Ints[c], i64)
			case storecontract.ColFloat64:
				batch.Flts[c] = append(batch.Flts[c], f64)
			case storecontract.ColString:
				batch.Strs[c] = append(batch.Strs[c], str)
			case storecontract.ColBool:
				batch.Bools[c] = append(batch.Bools[c], b)
			}
		}
		if len(batch.IDs) >= columnScanBatchRows {
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

// appendAbsent records a missing value, keeping every column slice the same length
// as IDs so a consumer can index them together.
func appendAbsent(b *storecontract.ColumnBatch, c int, known bool) {
	b.Null[c] = append(b.Null[c], true)
	if !known {
		return
	}
	switch b.Kinds[c] {
	case storecontract.ColInt64:
		b.Ints[c] = append(b.Ints[c], 0)
	case storecontract.ColFloat64:
		b.Flts[c] = append(b.Flts[c], 0)
	case storecontract.ColString:
		b.Strs[c] = append(b.Strs[c], "")
	case storecontract.ColBool:
		b.Bools[c] = append(b.Bools[c], false)
	}
}

// classifyScalar converts a stored property value to its column kind and typed
// value. The integral widenings here are exactly the ones a consumer would
// otherwise perform after the type was erased, each costing a heap box.
func classifyScalar(v any) (k storecontract.ColumnKind, i64 int64, f64 float64,
	s string, b bool, ok bool) {

	switch t := v.(type) {
	case int64:
		return storecontract.ColInt64, t, 0, "", false, true
	case int:
		return storecontract.ColInt64, int64(t), 0, "", false, true
	case int32:
		return storecontract.ColInt64, int64(t), 0, "", false, true
	case types.Instant:
		return storecontract.ColInt64, int64(t), 0, "", false, true
	case float64:
		return storecontract.ColFloat64, 0, t, "", false, true
	case float32:
		return storecontract.ColFloat64, 0, float64(t), "", false, true
	case string:
		return storecontract.ColString, 0, 0, t, false, true
	case bool:
		return storecontract.ColBool, 0, 0, "", t, true
	}
	return 0, 0, 0, "", false, false
}
