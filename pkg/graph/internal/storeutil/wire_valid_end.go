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
