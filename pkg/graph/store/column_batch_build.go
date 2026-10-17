package store

import (
	"fmt"
	"math"

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
		Ordinals:   make([]uint32, 0, ColumnScanBatchRows),
		ValidFrom:  make([]int64, 0, ColumnScanBatchRows),
		ValidTo:    make([]int64, 0, ColumnScanBatchRows),
		ColumnData: NewColumnData(len(props)),
	}
	kindKnown := make([]bool, len(props))
	cd := &batch.ColumnData
	reset := func() {
		batch.IDs = batch.IDs[:0]
		batch.Ordinals = batch.Ordinals[:0]
		batch.ValidFrom = batch.ValidFrom[:0]
		batch.ValidTo = batch.ValidTo[:0]
		batch.ValidFromZero = resetFlags(batch.ValidFromZero)
		batch.reset(len(props))
	}

	for _, n := range nodes {
		batch.IDs = append(batch.IDs, n.InternalID())
		batch.Ordinals = append(batch.Ordinals, n.Ordinal())
		// ValidRange, not Temporal(): the latter must copy the metadata for a frozen
		// entity, and every entity a store scan hands back is frozen — one
		// allocation per row across the whole scan.
		vf, vt, _ := n.ValidRange()
		_, claimed := n.ValidFromClaim()
		batch.ValidFromZero = AppendValidFromZero(batch.ValidFromZero, len(batch.ValidFrom), claimed && vf == 0)
		batch.ValidFrom = append(batch.ValidFrom, int64(vf))
		batch.ValidTo = append(batch.ValidTo, int64(vt))
		for c, key := range props {
			raw, found := n.GetProperty(key)
			kind, i64, f64, str, b, ok := classifyScalar(raw)
			if !found || !ok {
				cd.appendAbsent(c, kindKnown[c])
				if found {
					cd.markOther(c, len(batch.IDs)-1)
				}
				continue
			}
			if !kindKnown[c] {
				cd.Kinds[c], kindKnown[c] = kind, true
				cd.alignTyped(c, len(cd.Null[c])) // the batch's earlier rows
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
				cd.markOther(c, len(batch.IDs)-1)
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
			cd.seal(len(batch.IDs))
			if !fn(batch) {
				return nil
			}
			reset()
		}
	}
	if len(batch.IDs) > 0 {
		cd.seal(len(batch.IDs))
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
	return scanColumnsFromRels(rels, func(_ int, r *types.Relationship) (int64, int64, bool) {
		vf, vt, _ := r.ValidRange()
		_, claimed := r.ValidFromClaim()
		return int64(vf), int64(vt), claimed && vf == 0
	}, props, fn)
}

// ScanColumnsFromRelSegments is ScanColumnsFromRels over pieces of rows: row
// i is rels[i] over the valid span [validFrom[i], validTo[i]) (an effective
// timeline segment, validTo 0 open), which the batch's ValidFrom / ValidTo
// carry instead of the row's stored interval. A row may repeat (several
// segments of one relationship). len(validFrom) and len(validTo) must equal
// len(rels), else an error (a caller's bug, no sentinel).
func ScanColumnsFromRelSegments(rels []*types.Relationship, validFrom, validTo []types.Instant, props []string,
	fn func(*RelColumnBatch) bool) error {
	if len(validFrom) != len(rels) || len(validTo) != len(rels) {
		return fmt.Errorf("graph: column rows from segments: %d rows, %d valid-from, %d valid-to (lengths must match)", len(rels), len(validFrom), len(validTo))
	}
	// A segment's start is resolved: 0 is the instant 0 (an untimed start
	// resolves to its TxFrom or -inf), so it is always the explicit 0.
	return scanColumnsFromRels(rels, func(i int, _ *types.Relationship) (int64, int64, bool) {
		return int64(validFrom[i]), int64(validTo[i]), validFrom[i] == 0
	}, props, fn)
}

// scanColumnsFromRels is the relationship row path; span gives row i's
// valid interval.
func scanColumnsFromRels(rels []*types.Relationship, span func(i int, r *types.Relationship) (vf, vt int64, zero bool), props []string,
	fn func(*RelColumnBatch) bool) error {

	if len(rels) == 0 {
		return nil
	}
	batch := &RelColumnBatch{
		IDs:      make([]types.RelID, 0, ColumnScanBatchRows),
		Ordinals: make([]uint32, 0, ColumnScanBatchRows),
		StartIDs: make([]types.NodeID, 0, ColumnScanBatchRows),

		StartOrdinals: make([]uint32, 0, ColumnScanBatchRows),
		EndOrdinals:   make([]uint32, 0, ColumnScanBatchRows),
		EndIDs:        make([]types.NodeID, 0, ColumnScanBatchRows),
		ValidFrom:     make([]int64, 0, ColumnScanBatchRows),
		ValidTo:       make([]int64, 0, ColumnScanBatchRows),
		ColumnData:    NewColumnData(len(props)),
	}
	kindKnown := make([]bool, len(props))
	cd := &batch.ColumnData
	reset := func() {
		batch.IDs = batch.IDs[:0]
		batch.Ordinals = batch.Ordinals[:0]
		batch.StartOrdinals = batch.StartOrdinals[:0]
		batch.EndOrdinals = batch.EndOrdinals[:0]
		batch.StartIDs = batch.StartIDs[:0]
		batch.EndIDs = batch.EndIDs[:0]
		batch.ValidFrom = batch.ValidFrom[:0]
		batch.ValidTo = batch.ValidTo[:0]
		batch.ValidFromZero = resetFlags(batch.ValidFromZero)
		batch.reset(len(props))
	}

	for i, r := range rels {
		batch.IDs = append(batch.IDs, r.InternalID())
		batch.Ordinals = append(batch.Ordinals, r.Ordinal())
		batch.StartOrdinals = append(batch.StartOrdinals, r.StartOrdinal())
		batch.EndOrdinals = append(batch.EndOrdinals, r.EndOrdinal())
		batch.StartIDs = append(batch.StartIDs, r.StartNodeID())
		batch.EndIDs = append(batch.EndIDs, r.EndNodeID())
		vf, vt, zero := span(i, r)
		batch.ValidFromZero = AppendValidFromZero(batch.ValidFromZero, len(batch.ValidFrom), zero)
		batch.ValidFrom = append(batch.ValidFrom, vf)
		batch.ValidTo = append(batch.ValidTo, vt)
		for c, key := range props {
			raw, found := r.GetProperty(key)
			kind, i64, f64, str, b, ok := classifyScalar(raw)
			if !found || !ok {
				cd.appendAbsent(c, kindKnown[c])
				if found {
					cd.markOther(c, len(batch.IDs)-1)
				}
				continue
			}
			if !kindKnown[c] {
				cd.Kinds[c], kindKnown[c] = kind, true
				cd.alignTyped(c, len(cd.Null[c])) // the batch's earlier rows
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
				cd.markOther(c, len(batch.IDs)-1)
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
			cd.seal(len(batch.IDs))
			if !fn(batch) {
				return nil
			}
			reset()
		}
	}
	if len(batch.IDs) > 0 {
		cd.seal(len(batch.IDs))
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
	cd.Other = nil // the next batch allocates it again only if it has an Other cell
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

// alignTyped pads column c's typed slice — the one Kinds[c] selects — with zero
// values up to rows. appendAbsent cannot pad before a column's kind is known, so
// the driver calls this when the kind resolves (backfilling the batch's earlier
// rows) and seal calls it before the batch is handed out (a column no row has
// typed keeps Kinds' zero value, ColInt64). Without it a column whose first rows
// are absent had a typed slice SHORTER than Null, and Ints[c][k] read another row.
func (cd *ColumnData) alignTyped(c, rows int) {
	switch cd.Kinds[c] {
	case ColInt64:
		for len(cd.Ints[c]) < rows {
			cd.Ints[c] = append(cd.Ints[c], 0)
		}
	case ColFloat64:
		for len(cd.Flts[c]) < rows {
			cd.Flts[c] = append(cd.Flts[c], 0)
		}
	case ColString:
		for len(cd.Strs[c]) < rows {
			cd.Strs[c] = append(cd.Strs[c], "")
		}
	case ColBool:
		for len(cd.Bools[c]) < rows {
			cd.Bools[c] = append(cd.Bools[c], false)
		}
	}
}

// seal makes every column of a batch parallel to its rows before the callback.
func (cd *ColumnData) seal(rows int) {
	for c := range cd.Kinds {
		cd.alignTyped(c, rows)
	}
	for c := range cd.Other {
		for len(cd.Other[c]) < rows {
			cd.Other[c] = append(cd.Other[c], false)
		}
	}
}

// markOther flags row `row` of column c as held outside the column (ColumnData.
// Other). Other is allocated on a batch's first such cell, so a batch without one
// costs nothing; seal pads every Other column to the batch's rows.
func (cd *ColumnData) markOther(c, row int) {
	if cd.Other == nil {
		cd.Other = make([][]bool, len(cd.Kinds))
	}
	for len(cd.Other[c]) <= row {
		cd.Other[c] = append(cd.Other[c], false)
	}
	cd.Other[c][row] = true
}

// classifyScalar converts a stored property value to its column kind and typed
// value. The integral widenings here are exactly the ones a consumer would
// otherwise perform after the type was erased, each costing a heap box.
//
// The integer widths follow index.normalizeNumeric, which badger's columnar
// snapshot reads them with, so the row path and the snapshot type a column
// alike: every width up to 32 bits is int64; a uint/uint64 is int64 when it fits
// and float64 past MaxInt64 (next to a fitting one the column is mixed and
// refused, on both paths), never a wrapped negative int64.
func classifyScalar(v any) (k ColumnKind, i64 int64, f64 float64, s string, b bool, ok bool) {
	switch t := v.(type) {
	case int64:
		return ColInt64, t, 0, "", false, true
	case int:
		return ColInt64, int64(t), 0, "", false, true
	case int32:
		return ColInt64, int64(t), 0, "", false, true
	case int16:
		return ColInt64, int64(t), 0, "", false, true
	case int8:
		return ColInt64, int64(t), 0, "", false, true
	case uint32:
		return ColInt64, int64(t), 0, "", false, true
	case uint16:
		return ColInt64, int64(t), 0, "", false, true
	case uint8:
		return ColInt64, int64(t), 0, "", false, true
	case uint:
		return classifyUint64(uint64(t))
	case uint64:
		return classifyUint64(t)
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

// classifyUint64 is int64 when v fits exactly, float64 past MaxInt64.
func classifyUint64(v uint64) (ColumnKind, int64, float64, string, bool, bool) {
	if v <= math.MaxInt64 {
		return ColInt64, int64(v), 0, "", false, true
	}
	return ColFloat64, 0, float64(v), "", false, true
}

// RelColumnSource is a relationship row set a backend reads from its own
// columns (not from *types.Relationship values) for ScanColumnsFromRelSource:
// Row(i) gives row i's identity, endpoints and ValidRange (from/to, 0 when
// the row has no temporal metadata), Value(i, c) property c of row i already
// classified with the classifyScalar rules (ClassifyScalar), ok=false when
// the row does not hold it as a column scalar.
type RelColumnSource interface {
	Len() int
	Row(i int) (id types.RelID, start, end types.NodeID, validFrom, validTo int64)
	Value(i, c int) (kind ColumnKind, i64 int64, f64 float64, s string, b bool, ok bool)
}

// ColumnValidFromZeroSource is the OPTIONAL half of a NodeColumnSource or
// RelColumnSource whose rows can be valid from the explicit 0 (round 11):
// ValidFromZero(i) reports that row i's validFrom 0 is the instant 0
// (ColumnBatch.ValidFromZero). A source without it reports none.
type ColumnValidFromZeroSource interface {
	ValidFromZero(i int) bool
}

// AppendValidFromZero appends row n's flag (n rows before it) to a batch's
// ValidFromZero column: nil until the first true, then parallel to the rows.
func AppendValidFromZero(flags []bool, n int, zero bool) []bool {
	if flags == nil {
		if !zero {
			return nil
		}
		flags = make([]bool, n, max(n+1, ColumnScanBatchRows))
	}
	return append(flags, zero)
}

// resetFlags empties a batch's ValidFromZero column for its next rows.
func resetFlags(flags []bool) []bool {
	if flags == nil {
		return nil
	}
	return flags[:0]
}

// RelColumnOtherSource is the OPTIONAL half of a RelColumnSource that fills
// ColumnData.Other: Other(i, c) reports whether row i holds property c all the
// same (a list, a map, a struct) — asked only where Value reported ok=false.
// A source without it scans unchanged and reports no Other (Other stays nil,
// also for a kind mismatch, so a batch never carries a partial Other).
type RelColumnOtherSource interface {
	Other(i, c int) bool
}

// ClassifyScalar exposes the column-kind rule for RelColumnSource
// implementations, so a backend never restates it.
func ClassifyScalar(v any) (k ColumnKind, i64 int64, f64 float64, s string, b bool, ok bool) {
	return classifyScalar(v)
}

// ScanColumnsFromRelSource is ScanColumnsFromRels over a RelColumnSource:
// the same batches, kinds, absences and refusal for the same values in the
// same order (pinned by TestColumnDrivers_NodeAndRelAgree, which runs all
// three drivers). Rows are taken in source order: the source must already be
// in ID order.
func ScanColumnsFromRelSource(src RelColumnSource, nProps int, fn func(*RelColumnBatch) bool) error {
	n := src.Len()
	if n == 0 {
		return nil
	}
	batch := &RelColumnBatch{
		IDs:      make([]types.RelID, 0, ColumnScanBatchRows),
		Ordinals: make([]uint32, 0, ColumnScanBatchRows),
		StartIDs: make([]types.NodeID, 0, ColumnScanBatchRows),

		StartOrdinals: make([]uint32, 0, ColumnScanBatchRows),
		EndOrdinals:   make([]uint32, 0, ColumnScanBatchRows),
		EndIDs:        make([]types.NodeID, 0, ColumnScanBatchRows),
		ValidFrom:     make([]int64, 0, ColumnScanBatchRows),
		ValidTo:       make([]int64, 0, ColumnScanBatchRows),
		ColumnData:    NewColumnData(nProps),
	}
	kindKnown := make([]bool, nProps)
	cd := &batch.ColumnData
	otherSrc, hasOther := src.(RelColumnOtherSource)
	zeroSrc, hasZero := src.(ColumnValidFromZeroSource)
	reset := func() {
		batch.IDs = batch.IDs[:0]
		batch.Ordinals = batch.Ordinals[:0]
		batch.StartOrdinals = batch.StartOrdinals[:0]
		batch.EndOrdinals = batch.EndOrdinals[:0]
		batch.StartIDs = batch.StartIDs[:0]
		batch.EndIDs = batch.EndIDs[:0]
		batch.ValidFrom = batch.ValidFrom[:0]
		batch.ValidTo = batch.ValidTo[:0]
		batch.ValidFromZero = resetFlags(batch.ValidFromZero)
		batch.reset(nProps)
	}
	for i := 0; i < n; i++ {
		id, start, end, vf, vt := src.Row(i)
		batch.ValidFromZero = AppendValidFromZero(batch.ValidFromZero, len(batch.ValidFrom), hasZero && zeroSrc.ValidFromZero(i))
		batch.IDs = append(batch.IDs, id)
		batch.Ordinals = append(batch.Ordinals, 0) // a declared segment type's rows carry none
		batch.StartOrdinals = append(batch.StartOrdinals, 0)
		batch.EndOrdinals = append(batch.EndOrdinals, 0)
		batch.StartIDs = append(batch.StartIDs, start)
		batch.EndIDs = append(batch.EndIDs, end)
		batch.ValidFrom = append(batch.ValidFrom, vf)
		batch.ValidTo = append(batch.ValidTo, vt)
		for c := 0; c < nProps; c++ {
			kind, i64, f64, str, b, ok := src.Value(i, c)
			if !ok {
				cd.appendAbsent(c, kindKnown[c])
				if hasOther && otherSrc.Other(i, c) {
					cd.markOther(c, len(batch.IDs)-1)
				}
				continue
			}
			if !kindKnown[c] {
				cd.Kinds[c], kindKnown[c] = kind, true
				cd.alignTyped(c, len(cd.Null[c])) // the batch's earlier rows
			}
			if cd.Kinds[c] != kind {
				if numericKind(cd.Kinds[c]) && numericKind(kind) {
					return ErrMixedNumericColumn
				}
				cd.appendAbsent(c, true)
				if hasOther {
					cd.markOther(c, len(batch.IDs)-1)
				}
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
			cd.seal(len(batch.IDs))
			if !fn(batch) {
				return nil
			}
			reset()
		}
	}
	if len(batch.IDs) > 0 {
		cd.seal(len(batch.IDs))
		fn(batch)
	}
	return nil
}

// NodeColumnSource is the node sibling of RelColumnSource, read by
// ScanColumnsFromNodeSource: Row(i) gives row i's ID and ValidRange (0 when
// the row has no temporal metadata), Value(i, c) property c of row i already
// classified (ClassifyScalar), ok=false when the row does not hold it as a
// column scalar, and Other(i, c) whether row i holds property c all the same
// (ColumnData.Other). The rows carry no ordinals (Ordinals are 0).
type NodeColumnSource interface {
	Len() int
	Row(i int) (id types.NodeID, validFrom, validTo int64)
	Value(i, c int) (kind ColumnKind, i64 int64, f64 float64, s string, b bool, ok bool)
	Other(i, c int) bool
}

// ScanColumnsFromNodeSource is ScanColumnsFromNodes over a NodeColumnSource:
// the same batches, kinds, absences, Other cells and refusal for the same
// values in the same order (pinned by TestColumnDrivers_NodeAndRelAgree),
// with Ordinals 0. Rows are taken in source order: the source must already be
// in ID order.
func ScanColumnsFromNodeSource(src NodeColumnSource, nProps int, fn func(*ColumnBatch) bool) error {
	n := src.Len()
	if n == 0 {
		return nil
	}
	batch := &ColumnBatch{
		IDs:        make([]types.NodeID, 0, min(n, ColumnScanBatchRows)),
		Ordinals:   make([]uint32, 0, min(n, ColumnScanBatchRows)),
		ValidFrom:  make([]int64, 0, min(n, ColumnScanBatchRows)),
		ValidTo:    make([]int64, 0, min(n, ColumnScanBatchRows)),
		ColumnData: NewColumnData(nProps),
	}
	kindKnown := make([]bool, nProps)
	cd := &batch.ColumnData
	zeroSrc, hasZero := src.(ColumnValidFromZeroSource)
	reset := func() {
		batch.IDs = batch.IDs[:0]
		batch.Ordinals = batch.Ordinals[:0]
		batch.ValidFrom = batch.ValidFrom[:0]
		batch.ValidTo = batch.ValidTo[:0]
		batch.ValidFromZero = resetFlags(batch.ValidFromZero)
		batch.reset(nProps)
	}
	for i := 0; i < n; i++ {
		id, vf, vt := src.Row(i)
		batch.ValidFromZero = AppendValidFromZero(batch.ValidFromZero, len(batch.ValidFrom), hasZero && zeroSrc.ValidFromZero(i))
		batch.IDs = append(batch.IDs, id)
		batch.Ordinals = append(batch.Ordinals, 0)
		batch.ValidFrom = append(batch.ValidFrom, vf)
		batch.ValidTo = append(batch.ValidTo, vt)
		for c := 0; c < nProps; c++ {
			kind, i64, f64, str, b, ok := src.Value(i, c)
			if !ok {
				cd.appendAbsent(c, kindKnown[c])
				if src.Other(i, c) {
					cd.markOther(c, len(batch.IDs)-1)
				}
				continue
			}
			if !kindKnown[c] {
				cd.Kinds[c], kindKnown[c] = kind, true
				cd.alignTyped(c, len(cd.Null[c])) // the batch's earlier rows
			}
			if cd.Kinds[c] != kind {
				if numericKind(cd.Kinds[c]) && numericKind(kind) {
					return ErrMixedNumericColumn
				}
				cd.appendAbsent(c, true)
				cd.markOther(c, len(batch.IDs)-1)
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
			cd.seal(len(batch.IDs))
			if !fn(batch) {
				return nil
			}
			reset()
		}
	}
	if len(batch.IDs) > 0 {
		cd.seal(len(batch.IDs))
		fn(batch)
	}
	return nil
}
