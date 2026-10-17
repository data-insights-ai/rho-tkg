package badger

import (
	"math"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
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
	window, temporalOnly := columnarValidTimeWindow(opts)
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
	// The filter reads the effective start (an unset ValidFrom is the row's
	// TxFrom, backlog 62); the batch reports the stored one (vf).
	eff := col.EffectiveValidFrom()
	ords := bs.labelColumnOrdinals(token, col)
	batch := newColumnBatch(len(props))
	for i, v := range views {
		batch.Kinds[i] = columnKindOf(v)
	}

	for start := 0; start < len(ids); start += storecontract.ColumnScanBatchRows {
		// Zone-map skip: the whole block provably cannot match, so it costs nothing.
		if !window.blockCanMatch(col, start) {
			continue
		}
		end := min(start+storecontract.ColumnScanBatchRows, len(ids))
		resetColumnBatch(batch, len(props))
		if !window.filtered {
			batch.IDs = append(batch.IDs, ids[start:end]...)
			batch.Ordinals = append(batch.Ordinals, ords[start:end]...)
			for ord := start; ord < end; ord++ {
				batch.ValidFromZero = storecontract.AppendValidFromZero(batch.ValidFromZero, len(batch.IDs)-(end-ord), validFromZeroAt(vf, eff, ord)) // rows before ord: the block's IDs are appended
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

// validWindow is the valid-time filter of a column scan: the half-open
// [from, to) when filtered, every row otherwise. The flag, not a magic bound,
// says "no filter": every instant (0 and negatives included) is a real bound.
type validWindow struct {
	from, to int64
	filtered bool
}

// matches is the per-row predicate the zone map approximates per block: a row
// [f, t) meets the window by the one overlap rule (storeutil.IntervalMeets: a
// validTo of 0 is OPEN-ENDED, not "ends at 0").
func (w validWindow) matches(f, t int64) bool {
	return !w.filtered || storeutil.IntervalMeets(types.Instant(f), types.Instant(t), types.Instant(w.from), types.Instant(w.to))
}

// columnarValidTimeWindow normalises opts into the scan's validWindow with
// EXACTLY storeutil's semantics, reporting whether the columnar path may serve it.
//
// This must not be re-derived by hand. The canonical rules (storeutil.
// MatchesTemporalFilter) are: ValidAt takes precedence; an interval needs BOTH
// ValidStart and ValidEnd set (QueryOpts.HasValidInterval), and a start >= end
// matches nothing; the interval test is `from < end && (to == 0 || to > start)` —
// a STRICT upper bound, so a row starting exactly at ValidEnd does NOT match.
// Guessing `from <= end` here made the columnar path return one row the row path
// rejected.
//
// A point filter ValidAt=t is the interval [t, t+1): `from < t+1` is `from <= t`,
// and `to > t` is unchanged — so one predicate serves both shapes. ValidAt -1 is
// the window [-1, 0), a real one (ledger row 60). t = MaxInt64 saturates.
func columnarValidTimeWindow(opts storecontract.QueryOpts) (w validWindow, ok bool) {
	if opts.HasValidAt() {
		at := int64(opts.ValidAt)
		to := at + 1
		if at == math.MaxInt64 {
			to = at
		}
		return validWindow{from: at, to: to, filtered: true}, true
	}
	if opts.HasValidInterval() {
		if opts.ValidStart >= opts.ValidEnd {
			return validWindow{}, false // matches nothing — let the row path own the empty answer
		}
		return validWindow{from: int64(opts.ValidStart), to: int64(opts.ValidEnd), filtered: true}, true
	}
	return validWindow{}, true // no active filter (a lone ValidStart or ValidEnd is not one)
}

// blockCanMatch reports whether the zone map leaves the block at start to scan:
// always without a filter.
func (w validWindow) blockCanMatch(col interface {
	BlockCanMatch(start int, qFrom, qTo int64) bool
}, start int) bool {
	return !w.filtered || col.BlockCanMatch(start, w.from, w.to)
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

// validFromZeroAt reports that row ord of a column snapshot is valid from the
// explicit instant 0 (round 11): its stored and its effective start are both
// 0. An unset start (stored 0) resolves to a non-zero TxFrom, update or mint
// instant, so a stored 0 with an effective 0 is the claim
// (types.TemporalMetadata.ValidFromZero, which the snapshot does not carry).
func validFromZeroAt(vf, eff []int64, ord int) bool {
	return vf[ord] == 0 && eff[ord] == 0
}

func newColumnBatch(nCols int) *storecontract.ColumnBatch {
	return &storecontract.ColumnBatch{
		IDs:        make([]types.NodeID, 0, storecontract.ColumnScanBatchRows),
		Ordinals:   make([]uint32, 0, storecontract.ColumnScanBatchRows),
		ValidFrom:  make([]int64, 0, storecontract.ColumnScanBatchRows),
		ValidTo:    make([]int64, 0, storecontract.ColumnScanBatchRows),
		ColumnData: storecontract.NewColumnData(nCols),
	}
}

func resetColumnBatch(b *storecontract.ColumnBatch, nCols int) {
	b.IDs = b.IDs[:0]
	b.Ordinals = b.Ordinals[:0]
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
