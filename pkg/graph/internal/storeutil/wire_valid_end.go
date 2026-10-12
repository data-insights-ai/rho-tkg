package storeutil

import "github.com/data-insights-ai/rho-tkg/v4/pkg/types"

// ValidEndWire is what a valid-end index (backlog 51) folds from one stored
// row value: the row's valid end and transaction stamps, and the tokens it
// carries (a relationship's type, a node's labels).
type ValidEndWire struct {
	ValidTo, TxFrom, TxTo, DeletedAt types.Instant
	Tokens                           []uint16
}

// wireValidEndPartial decodes only the fields a valid-end index folds;
// msgpack skips every other map entry (properties included) without
// materializing it. NodeWire and RelWire share these tags; "pl"/"el" are a
// node's labels, "rt" a relationship's type.
type wireValidEndPartial struct {
	FormatVersion uint8 `msgpack:"fv"`
	HasTemporal   bool  `msgpack:"ht"`
	ValidTo       int64 `msgpack:"vt"`
	TxFrom        int64 `msgpack:"tf"`
	TxTo          int64 `msgpack:"tt"`
	DeletedAt     int64 `msgpack:"da"`
	PrimaryLabel  int   `msgpack:"pl"`
	ExtraLabels   []int `msgpack:"el"`
	RelType       int   `msgpack:"rt"`
}

// DecodeValidEndValue decodes a stored current or history row value (a full
// row or a 'D'-tagged history delta, whose Meta carries every non-property
// field verbatim) of a node (node) or relationship. A row written by a newer
// release fails closed (store.ErrWireFormatVersionUnsupported).
func DecodeValidEndValue(raw []byte, node bool) (ValidEndWire, error) {
	var w wireValidEndPartial
	if HistoryValueKindOf(raw) == HistoryDelta {
		if node {
			d, err := DecodeNodeHistoryDelta(raw)
			if err != nil {
				return ValidEndWire{}, err
			}
			m := d.Meta
			w = wireValidEndPartial{FormatVersion: m.FormatVersion, HasTemporal: m.HasTemporal, ValidTo: m.ValidTo, TxFrom: m.TxFrom,
				TxTo: m.TxTo, DeletedAt: m.DeletedAt, PrimaryLabel: m.PrimaryLabel, ExtraLabels: m.ExtraLabels}
		} else {
			d, err := DecodeRelHistoryDelta(raw)
			if err != nil {
				return ValidEndWire{}, err
			}
			m := d.Meta
			w = wireValidEndPartial{FormatVersion: m.FormatVersion, HasTemporal: m.HasTemporal, ValidTo: m.ValidTo, TxFrom: m.TxFrom,
				TxTo: m.TxTo, DeletedAt: m.DeletedAt, RelType: m.RelType}
		}
	} else if err := SafeUnmarshal(raw, &w); err != nil {
		return ValidEndWire{}, err
	}
	if err := checkRowFormatVersion("valid-end row", w.FormatVersion); err != nil {
		return ValidEndWire{}, err
	}
	var out ValidEndWire
	if w.HasTemporal {
		out.ValidTo, out.TxFrom, out.TxTo, out.DeletedAt = types.Instant(w.ValidTo), types.Instant(w.TxFrom), types.Instant(w.TxTo), types.Instant(w.DeletedAt)
	}
	if node {
		for _, l := range append([]int{w.PrimaryLabel}, w.ExtraLabels...) {
			if l > 0 && l <= 0xFFFF {
				out.Tokens = append(out.Tokens, uint16(l)) // #nosec G115 -- range checked above
			}
		}
	} else if w.RelType > 0 && w.RelType <= 0xFFFF {
		out.Tokens = []uint16{uint16(w.RelType)} // #nosec G115 -- range checked above
	}
	return out, nil
}
