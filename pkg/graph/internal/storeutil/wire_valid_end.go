package storeutil

import "github.com/data-insights-ai/rho-tkg/v4/pkg/types"

// DecodeValidEndValue returns the selection-scope temporal block (nil when
// the row carries none) of a stored current or history row value: a full row
// through the reflection-free scanner of DecodeWireTemporalMeta, or a
// 'D'-tagged history delta, whose Meta carries every non-property field
// verbatim (node selects the delta decoder). What a valid-end index (backlog
// 51) folds on a row write. A row written by a newer release fails closed
// (store.ErrWireFormatVersionUnsupported).
func DecodeValidEndValue(raw []byte, node bool) (*types.TemporalMetadata, error) {
	if HistoryValueKindOf(raw) != HistoryDelta {
		_, tm, err := DecodeWireTemporalMeta(raw)
		return tm, err
	}
	if node {
		d, err := DecodeNodeHistoryDelta(raw)
		if err != nil {
			return nil, err
		}
		if err := checkRowFormatVersion("valid-end delta row", d.Meta.FormatVersion); err != nil {
			return nil, err
		}
		return SelectionTemporalMetaOfNodeWire(d.Meta), nil
	}
	d, err := DecodeRelHistoryDelta(raw)
	if err != nil {
		return nil, err
	}
	if err := checkRowFormatVersion("valid-end delta row", d.Meta.FormatVersion); err != nil {
		return nil, err
	}
	return SelectionTemporalMetaOfRelWire(d.Meta), nil
}

// DecodeValidEndNodeValue is DecodeValidEndValue for a stored node row that
// also returns the label tokens the row carries (primary first): a full row
// through DecodeWireNodeTemporalMetaLabels (one pass, the scanner capturing the
// labels with the temporal block), a delta through its Meta. The tokens are
// checked by the full decoder's rules; a row that breaks them, or one written
// by a newer release, fails closed. The valid-end index's first call reads a
// node's current labels from its current row with it (R2-2 VEATTRIB).
func DecodeValidEndNodeValue(raw []byte) (*types.TemporalMetadata, []uint16, error) {
	if HistoryValueKindOf(raw) != HistoryDelta {
		_, tm, toks, err := DecodeWireNodeTemporalMetaLabels(raw)
		return tm, toks, err
	}
	d, err := DecodeNodeHistoryDelta(raw)
	if err != nil {
		return nil, nil, err
	}
	if err := checkRowFormatVersion("valid-end delta row", d.Meta.FormatVersion); err != nil {
		return nil, nil, err
	}
	extra := make([]int64, len(d.Meta.ExtraLabels))
	for i, e := range d.Meta.ExtraLabels {
		extra[i] = int64(e)
	}
	toks, err := nodeLabelTokens(int64(d.Meta.PrimaryLabel), extra)
	if err != nil {
		return nil, nil, err
	}
	return SelectionTemporalMetaOfNodeWire(d.Meta), toks, nil
}
