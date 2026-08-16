package badger

import (
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	snowflake "github.com/bds421/rho-snowflake-2026"
)

// ScanNodeColumns implements store.NodeColumnScanCapability.
//
// TWO PATHS, and the fallback is not optional. The capability returns only `error`
// — there is no ok=false — so this must always answer. The columnar path can
// legitimately decline (membership not in RAM under LabelIndexOnDisk, an empty or
// over-cap label, a property whose values are not uniformly typed), and every one of
// those falls through to the row path rather than failing the call.
//
// COLUMNAR PATH. Reads the cached DocValues snapshot: values come from the typed
// arrays and validity from the snapshot's temporal columns, so no *types.Node is
// materialised and no value is boxed. A valid-time predicate skips whole blocks via
// the zone map instead of testing every row.
//
// ROW PATH. Materialises nodes and converts, via the same shared builder the memory
// backend uses.
func (bs *Store) ScanNodeColumns(token uint16, props []string, opts storecontract.QueryOpts,
	fn func(*storecontract.ColumnBatch) bool) error {

	if bs == nil {
		return ErrStoreClosed
	}
	if err := bs.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return errNilIterationCallback()
	}

	if bs.scanNodeColumnsColumnar(token, props, opts, fn) {
		return nil
	}

	nodes, err := bs.NodesByLabel(token, opts)
	if err != nil {
		return err
	}
	return storecontract.ScanColumnsFromNodes(nodes, props, fn)
}

// scanNodeColumnsColumnar serves the scan from the DocValues snapshot, reporting
// whether it did. false means it DECLINED and the caller must use the row path; the
// scan has then emitted NOTHING, so the fallback is a clean retry rather than a
// continuation. It returns no error: every way this path can fail to serve a request
// is a decline, never a failure of the request itself.
func (bs *Store) scanNodeColumnsColumnar(token uint16, props []string, opts storecontract.QueryOpts,
	fn func(*storecontract.ColumnBatch) bool) (done bool) {

	if bs.labelOnDisk {
		return false // membership not materialised in RAM
	}
	// Only the temporal shape of QueryOpts can be answered from the snapshot's own
	// columns. Anything else (property predicates, pagination, tx-time) is the row
	// path's job — declining is correct, and quietly ignoring an opt would return
	// rows the caller did not ask for.
	qFrom, qTo, temporalOnly := columnarValidTimeWindow(opts)
	if !temporalOnly {
		return false
	}

	gen := bs.labelEpoch(token)
	bs.docMu.Lock()
	col := bs.docColumns[token]
	bs.docMu.Unlock()
	if col == nil || col.Epoch() != gen || !col.HasAll(props) {
		built, declined := bs.buildLabelColumns(token, props)
		if declined {
			return false
		}
		col = built
	}
	if !col.HasAll(props) || !col.HasTemporal() {
		return false
	}

	views := make([]indexpkg.ColumnView, len(props))
	for i, k := range props {
		v, ok := col.View(k)
		if !ok {
			return false
		}
		// A MIXED numeric column has no single kind, and ColumnBatch carries exactly
		// one per column. Declining here is what makes the row path's refusal
		// (ErrMixedNumericColumn) reachable — picking a half would read the int array
		// for a float row and emit a plausible wrong number instead.
		if v.Mixed() {
			return false
		}
		views[i] = v
	}

	ids, vf, vt := col.IDs(), col.ValidFrom(), col.ValidTo()
	batch := newColumnBatch(len(props))
	for i, v := range views {
		batch.Kinds[i] = columnKindOf(v)
	}

	for start := 0; start < len(ids); start += storecontract.ColumnScanBatchRows {
		// Zone-map skip: the whole block provably cannot match, so it costs nothing.
		if !col.BlockCanMatch(start, qFrom, qTo) {
			continue
		}
		end := min(start+storecontract.ColumnScanBatchRows, len(ids))
		resetColumnBatch(batch, len(props))
		for ord := start; ord < end; ord++ {
			if !validTimeMatches(effectiveValidFrom(vf[ord], ids[ord]), vt[ord], qFrom, qTo) {
				continue
			}
			batch.IDs = append(batch.IDs, ids[ord])
			batch.ValidFrom = append(batch.ValidFrom, vf[ord])
			batch.ValidTo = append(batch.ValidTo, vt[ord])
			for c, v := range views {
				appendFromView(&batch.ColumnData, c, v, ord)
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

// columnarValidTimeWindow normalises opts into a half-open [start, end) window with
// EXACTLY storeutil's semantics, reporting whether the columnar path may serve it.
//
// This must not be re-derived by hand. The canonical rules (storeutil.
// MatchesTemporalFilter) are: ValidAt takes precedence; an interval needs BOTH
// ValidStart and ValidEnd set, and a start >= end matches nothing; the interval test
// is `from < end && (to == 0 || to > start)` — a STRICT upper bound, so a row
// starting exactly at ValidEnd does NOT match. Guessing `from <= end` here made the
// columnar path return one row the row path rejected.
//
// A point filter ValidAt=t is the interval [t, t+1): `from < t+1` is `from <= t`,
// and `to > t` is unchanged — so one predicate serves both shapes.
func columnarValidTimeWindow(opts storecontract.QueryOpts) (start, end int64, ok bool) {
	if opts.ValidAt != 0 {
		return int64(opts.ValidAt), int64(opts.ValidAt) + 1, true
	}
	if opts.ValidStart > 0 && opts.ValidEnd > 0 {
		if opts.ValidStart >= opts.ValidEnd {
			return 0, 0, false // matches nothing — let the row path own the empty answer
		}
		return int64(opts.ValidStart), int64(opts.ValidEnd), true
	}
	return 0, 0, true // no active filter (a lone ValidStart or ValidEnd is not one)
}

// validTimeMatches is the per-row predicate the zone map approximates per block.
// end == 0 means no filter. A validTo of 0 is OPEN-ENDED, not "ends at 0".
func validTimeMatches(f, t, start, end int64) bool {
	if end == 0 {
		return true
	}
	return f < end && (t == 0 || t > start)
}

// effectiveValidFrom is the valid-from a FILTER must use: an entity that carries
// no ValidFrom is treated as valid from its MINT time, which is the rule
// storeutil.MatchesTemporalFilter applies on the row path and the one every
// non-columnar door already agrees on.
//
// SEPARATE FROM THE REPORTED COLUMN, deliberately. The doc-values builder stores
// the RAW bound, because ColumnBatch.ValidFrom is handed to callers as the
// entity's stored validity and must mean the same thing it means through
// ValidRange() and the tkg_valid_from shadow key. Folding the fallback into the
// stored array made those two questions share one answer, and a columnar reader
// then saw [mint, +inf) where the memory backend saw eternal.
//
// Applying it here keeps the filter's behaviour bit-identical to before.
func effectiveValidFrom(f int64, id interface{ SnowflakeID() snowflake.ID }) int64 {
	if f != 0 {
		return f
	}
	return int64(storeutil.SnowflakeInstant(id.SnowflakeID()))
}

func columnKindOf(v indexpkg.ColumnView) storecontract.ColumnKind {
	if v.Type == indexpkg.ColString {
		return storecontract.ColString
	}
	if v.Flts != nil && v.Ints == nil {
		return storecontract.ColFloat64
	}
	return storecontract.ColInt64
}

// appendFromView copies one ordinal's value out of a typed column, padding the
// typed slice on an absent row so column indices stay aligned with IDs.
//
// Takes the shared ColumnData rather than a ColumnBatch so the relationship fast
// path reuses this padding logic instead of restating it — the same one-copy rule
// the row builders follow.
//
// Every column reaching here is UNIFORM: the caller declines the whole scan on
// v.Mixed(). That is load-bearing, not defensive — a mixed column has both
// halves populated, so the default arm below would read the int array for a float
// row and emit a plausible wrong number.
func appendFromView(b *storecontract.ColumnData, c int, v indexpkg.ColumnView, ord int) {
	if !v.Present(ord) {
		b.Null[c] = append(b.Null[c], true)
		switch b.Kinds[c] {
		case storecontract.ColString:
			b.Strs[c] = append(b.Strs[c], "")
		case storecontract.ColFloat64:
			b.Flts[c] = append(b.Flts[c], 0)
		default:
			b.Ints[c] = append(b.Ints[c], 0)
		}
		return
	}
	b.Null[c] = append(b.Null[c], false)
	switch b.Kinds[c] {
	case storecontract.ColString:
		b.Strs[c] = append(b.Strs[c], v.StringAt(ord))
	case storecontract.ColFloat64:
		b.Flts[c] = append(b.Flts[c], v.Flts[ord])
	default:
		b.Ints[c] = append(b.Ints[c], v.Ints[ord])
	}
}

func newColumnBatch(nCols int) *storecontract.ColumnBatch {
	return &storecontract.ColumnBatch{
		IDs:        make([]types.NodeID, 0, storecontract.ColumnScanBatchRows),
		ValidFrom:  make([]int64, 0, storecontract.ColumnScanBatchRows),
		ValidTo:    make([]int64, 0, storecontract.ColumnScanBatchRows),
		ColumnData: storecontract.NewColumnData(nCols),
	}
}

func resetColumnBatch(b *storecontract.ColumnBatch, nCols int) {
	b.IDs = b.IDs[:0]
	b.ValidFrom = b.ValidFrom[:0]
	b.ValidTo = b.ValidTo[:0]
	for c := range nCols {
		b.Ints[c] = b.Ints[c][:0]
		b.Flts[c] = b.Flts[c][:0]
		b.Strs[c] = b.Strs[c][:0]
		b.Bools[c] = b.Bools[c][:0]
		b.Null[c] = b.Null[c][:0]
	}
}
