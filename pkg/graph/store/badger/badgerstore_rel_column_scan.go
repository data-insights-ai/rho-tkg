package badger

import (
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// ScanRelColumns implements store.RelColumnScanCapability, mirroring
// ScanNodeColumns: try the columnar snapshot, fall back to the shared row path.
func (bs *Store) ScanRelColumns(token uint16, props []string, opts storecontract.QueryOpts,
	fn func(*storecontract.RelColumnBatch) bool) error {

	if bs == nil {
		return ErrStoreClosed
	}
	if err := bs.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return errNilIterationCallback()
	}

	if bs.scanRelColumnsColumnar(token, props, opts, fn) {
		return nil
	}

	rels, err := bs.RelationshipsByType(token, opts)
	if err != nil {
		return err
	}
	return storecontract.ScanColumnsFromRels(rels, props, fn)
}

// scanRelColumnsColumnar serves the scan from the rel DocValues snapshot, reporting
// whether it did. false means it DECLINED and the caller must use the row path; the
// scan has then emitted NOTHING, so the fallback is a clean retry rather than a
// continuation.
//
// Endpoints come from the snapshot's reserved RelStartColumn / RelEndColumn, which
// buildRelColumns always materialises — so the fast path hands back the same
// (start, end) the row path reads off the relationship, without touching one.
func (bs *Store) scanRelColumnsColumnar(token uint16, props []string, opts storecontract.QueryOpts,
	fn func(*storecontract.RelColumnBatch) bool) (done bool) {

	// Only the temporal shape of QueryOpts is answerable from the snapshot's own
	// columns; anything else is the row path's job. Quietly ignoring an opt would
	// return rows the caller did not ask for.
	window, temporalOnly := columnarValidTimeWindow(opts)
	if !temporalOnly {
		return false
	}

	col, _, ok, err := bs.RelColumnSnapshot(token, props)
	if err != nil || !ok || col == nil {
		return false
	}
	if !col.HasTemporal() {
		return false
	}

	startView, okStart := col.View(RelStartColumn)
	endView, okEnd := col.View(RelEndColumn)
	if !okStart || !okEnd {
		return false
	}

	views := make([]indexpkg.ColumnView, len(props))
	for i, k := range props {
		v, viewOK := col.View(k)
		if !viewOK {
			return false
		}
		// A MIXED numeric column has no single kind and the batch carries exactly one
		// per column. Declining here is what makes the row path's ErrMixedNumericColumn
		// reachable — picking a half would read the int array for a float row and emit
		// a plausible wrong number.
		if v.Mixed() {
			return false
		}
		views[i] = v
	}

	ids, vf, vt := col.IDs(), col.ValidFrom(), col.ValidTo()
	// The filter reads the effective start (an unset ValidFrom is the row's
	// TxFrom, backlog 62); the batch reports the stored one (vf).
	eff := col.EffectiveValidFrom()
	ords, startOrds, endOrds := bs.relColumnOrdinals(token, col, startView.Ints, endView.Ints)
	batch := newRelColumnBatch(len(props))
	for i, v := range views {
		batch.Kinds[i] = columnKindOf(v)
	}

	for start := 0; start < len(ids); start += storecontract.ColumnScanBatchRows {
		// Zone-map skip: the whole block provably cannot match, so it costs nothing.
		if !window.blockCanMatch(col, start) {
			continue
		}
		end := min(start+storecontract.ColumnScanBatchRows, len(ids))
		resetRelColumnBatch(batch, len(props))
		if !window.filtered {
			// Every row of the block: the snapshot's arrays copied as runs
			// (round 6 request 10), no test or append per row.
			batch.IDs = append(batch.IDs, ids[start:end]...)
			batch.Ordinals = append(batch.Ordinals, ords[start:end]...)
			batch.StartOrdinals = append(batch.StartOrdinals, startOrds[start:end]...)
			batch.EndOrdinals = append(batch.EndOrdinals, endOrds[start:end]...)
			batch.StartIDs = appendNodeIDs(batch.StartIDs, startView.Ints[start:end])
			batch.EndIDs = appendNodeIDs(batch.EndIDs, endView.Ints[start:end])
			for ord := start; ord < end; ord++ {
				batch.ValidFromZero = storecontract.AppendValidFromZero(batch.ValidFromZero, ord-start, validFromZeroAt(vf, eff, ord))
			}
			batch.ValidFrom = append(batch.ValidFrom, vf[start:end]...)
			batch.ValidTo = append(batch.ValidTo, vt[start:end]...)
			for c, v := range views {
				for ord := start; ord < end; ord++ {
					appendFromView(&batch.ColumnData, c, v, ord)
				}
			}
		} else {
			for ord := start; ord < end; ord++ {
				if !window.matches(eff[ord], vt[ord]) {
					continue
				}
				batch.IDs = append(batch.IDs, ids[ord])
				batch.Ordinals = append(batch.Ordinals, ords[ord])
				batch.StartOrdinals = append(batch.StartOrdinals, startOrds[ord])
				batch.EndOrdinals = append(batch.EndOrdinals, endOrds[ord])
				batch.StartIDs = append(batch.StartIDs, types.NodeID(startView.Ints[ord]))
				batch.EndIDs = append(batch.EndIDs, types.NodeID(endView.Ints[ord]))
				batch.ValidFromZero = storecontract.AppendValidFromZero(batch.ValidFromZero, len(batch.ValidFrom), validFromZeroAt(vf, eff, ord))
				batch.ValidFrom = append(batch.ValidFrom, vf[ord])
				batch.ValidTo = append(batch.ValidTo, vt[ord])
				for c, v := range views {
					appendFromView(&batch.ColumnData, c, v, ord)
				}
			}
		}
		if len(batch.IDs) == 0 {
			continue
		}
		if !fn(batch) {
			return true
		}
	}
	return true
}

// appendNodeIDs appends raw endpoint IDs of a snapshot's reserved endpoint
// column as node IDs.
func appendNodeIDs(dst []types.NodeID, raw []int64) []types.NodeID {
	for _, id := range raw {
		dst = append(dst, types.NodeID(id))
	}
	return dst
}

func newRelColumnBatch(nCols int) *storecontract.RelColumnBatch {
	return &storecontract.RelColumnBatch{
		IDs:      make([]types.RelID, 0, storecontract.ColumnScanBatchRows),
		Ordinals: make([]uint32, 0, storecontract.ColumnScanBatchRows),
		StartIDs: make([]types.NodeID, 0, storecontract.ColumnScanBatchRows),

		StartOrdinals: make([]uint32, 0, storecontract.ColumnScanBatchRows),
		EndOrdinals:   make([]uint32, 0, storecontract.ColumnScanBatchRows),
		EndIDs:        make([]types.NodeID, 0, storecontract.ColumnScanBatchRows),
		ValidFrom:     make([]int64, 0, storecontract.ColumnScanBatchRows),
		ValidTo:       make([]int64, 0, storecontract.ColumnScanBatchRows),
		ColumnData:    storecontract.NewColumnData(nCols),
	}
}

func resetRelColumnBatch(b *storecontract.RelColumnBatch, nCols int) {
	b.IDs = b.IDs[:0]
	b.Ordinals = b.Ordinals[:0]
	b.StartOrdinals = b.StartOrdinals[:0]
	b.EndOrdinals = b.EndOrdinals[:0]
	b.StartIDs = b.StartIDs[:0]
	b.EndIDs = b.EndIDs[:0]
	b.ValidFrom = b.ValidFrom[:0]
	b.ValidTo = b.ValidTo[:0]
	b.ValidFromZero = b.ValidFromZero[:0]
	for c := range nCols {
		b.Ints[c] = b.Ints[c][:0]
		b.Flts[c] = b.Flts[c][:0]
		b.Strs[c] = b.Strs[c][:0]
		b.Bools[c] = b.Bools[c][:0]
		b.Null[c] = b.Null[c][:0]
	}
}
