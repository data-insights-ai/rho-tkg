package storeutil

import (
	"fmt"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	"github.com/vmihailenco/msgpack/v5"
)

// Selection-scope temporal decode (store.TemporalMetaHistoryCapability).
//
// A historical-pin resolution (NodeAt / NodeAtTx / ByLabel{ValidAt} …) selects
// the winning version of a chain purely from Version + the NUMERIC temporal
// instants — properties, labels, hashes and provenance strings never influence
// selection. Decoding the full wire per walked version (~80 allocs on a wide
// row) just to read those few ints is the valid-time-axis cost a
// depth oracle measured. wireTemporalMetaPartial decodes ONLY the selection
// fields; msgpack skips every other map entry without materializing it.
//
// NodeWire and RelWire share these exact tags, so ONE partial decoder serves
// both entity kinds.
//
// CONTRACT — selection scope, not full fidelity: the returned
// TemporalMetadata carries the numeric instants and the retraction marker only. CreatedBy / UpdatedBy /
// BaseEntityID are deliberately NOT decoded (per-version string allocs for
// fields selection never reads). A consumer must hydrate the full row before
// returning it to any caller — a selection skeleton must never leave the
// resolution seam.
type wireTemporalMetaPartial struct {
	FormatVersion  uint8 `msgpack:"fv"`
	Version        int   `msgpack:"v"`
	HasTemporal    bool  `msgpack:"ht"`
	ValidFrom      int64 `msgpack:"vf"`
	ValidTo        int64 `msgpack:"vt"`
	ValidEndpoints uint8 `msgpack:"ep"`
	TxFrom         int64 `msgpack:"tf"`
	TxTo           int64 `msgpack:"tt"`
	CreatedAt      int64 `msgpack:"ca"`
	UpdatedAt      int64 `msgpack:"ua"`
	DeletedAt      int64 `msgpack:"da"`
	Retracted      bool  `msgpack:"rx"`
}

// DecodeWireTemporalMeta partially decodes a FULL (non-delta) node or rel wire
// row into its version number and selection-scope temporal metadata (nil when
// the row carries no temporal block, mirroring the full decoder). Rows written
// by a newer release fail closed with ErrWireFormatVersionUnsupported, exactly
// like the checked full decode.
//
// Fast path: scanWireTemporalMeta — a single-pass, reflection-free,
// non-recursive token walk (the guardMsgpackDepth machinery extended to
// CAPTURE the ten selection fields while skipping everything else) — the
// no-format-change answer to the valid-time depth ask's residual per-version
// cost. On ANY structural surprise the scanner declines (ok=false) and the
// audited SafeUnmarshal partial decode below remains the authority, so the
// scanner can only ever be a same-answer accelerator.
func DecodeWireTemporalMeta(raw []byte) (uint32, *types.TemporalMetadata, error) {
	w, ok := scanWireTemporalMeta(raw)
	if !ok {
		if err := SafeUnmarshal(raw, &w); err != nil {
			return 0, nil, err
		}
	}
	if err := checkRowFormatVersion("wire temporal meta", w.FormatVersion); err != nil {
		return 0, nil, err
	}
	if w.Version < 0 || int64(w.Version) > maxWireUint32 {
		return 0, nil, fmt.Errorf("wire temporal meta: negative version %d: %w", w.Version, storepkg.ErrCorruptWire)
	}
	if err := validateSelectionTemporal(w); err != nil {
		return 0, nil, err
	}
	return uint32(w.Version), selectionTemporalMeta(w.HasTemporal, w.ValidFrom, w.ValidTo, w.TxFrom, w.TxTo, w.CreatedAt, w.UpdatedAt, w.DeletedAt, w.Retracted, w.ValidEndpoints), nil // #nosec G115 — non-negative checked above
}

// SelectionTemporalMetaOfNodeWire builds the selection-scope temporal metadata
// from an already-decoded NodeWire (a delta row's Meta carries the target
// version's temporal verbatim, so no partial decode is needed there).
func SelectionTemporalMetaOfNodeWire(w NodeWire) *types.TemporalMetadata {
	return selectionTemporalMeta(w.HasTemporal, w.ValidFrom, w.ValidTo, w.TxFrom, w.TxTo, w.CreatedAt, w.UpdatedAt, w.DeletedAt, w.Retracted, w.ValidEndpoints)
}

// SelectionTemporalMetaOfRelWire mirrors SelectionTemporalMetaOfNodeWire.
func SelectionTemporalMetaOfRelWire(w RelWire) *types.TemporalMetadata {
	return selectionTemporalMeta(w.HasTemporal, w.ValidFrom, w.ValidTo, w.TxFrom, w.TxTo, w.CreatedAt, w.UpdatedAt, w.DeletedAt, w.Retracted, w.ValidEndpoints)
}

// selectionTemporalMeta mirrors applyNodeWireFields' temporal construction for
// the numeric instants and the retraction marker (a selection input: it caps
// its life, chainLifeEnds): a temporal block exists iff HasTemporal (the
// checked decoders reject payload-without-ht, so ht is authoritative).
func selectionTemporalMeta(ht bool, vf, vt, tf, tt, ca, ua, da int64, rx bool, ep uint8) *types.TemporalMetadata {
	if !ht {
		return nil
	}
	return &types.TemporalMetadata{
		ValidFrom:      types.Instant(vf),
		ValidTo:        types.Instant(vt),
		ValidEndpoints: types.EndpointPresence(ep),
		TxFrom:         types.Instant(tf),
		TxTo:           types.Instant(tt),
		CreatedAt:      types.Instant(ca),
		UpdatedAt:      types.Instant(ua),
		DeletedAt:      types.Instant(da),
		Retracted:      rx,
	}
}

// DecodeWireTxStamps is DecodeWireTemporalMeta reduced to the transaction
// stamps of a FULL (non-delta) row: TxFrom, TxTo and DeletedAt, zeros when the
// row carries no temporal block. It returns values, so the scanner path
// allocates nothing; the same errors as DecodeWireTemporalMeta.
func DecodeWireTxStamps(raw []byte) (txFrom, txTo, deletedAt types.Instant, err error) {
	w, ok := scanWireTemporalMeta(raw)
	if !ok {
		if w, err = decodeWireTemporalMetaSlow(raw); err != nil {
			return 0, 0, 0, err
		}
	}
	if err := checkRowFormatVersion("wire tx stamps", w.FormatVersion); err != nil {
		return 0, 0, 0, err
	}
	if w.Version < 0 || int64(w.Version) > maxWireUint32 {
		return 0, 0, 0, fmt.Errorf("wire tx stamps: negative version %d: %w", w.Version, storepkg.ErrCorruptWire)
	}
	if err := validateSelectionTemporal(w); err != nil {
		return 0, 0, 0, err
	}
	if !w.HasTemporal {
		return 0, 0, 0, nil
	}
	return types.Instant(w.TxFrom), types.Instant(w.TxTo), types.Instant(w.DeletedAt), nil
}

// HistoryValueTxStamps returns the transaction stamps of one stored history
// value: a full row (DecodeWireTxStamps) or a 'D'-tagged delta, whose Meta
// carries the version's temporal block verbatim (node selects the node or
// relationship delta decoder).
func HistoryValueTxStamps(raw []byte, node bool) (txFrom, txTo, deletedAt types.Instant, err error) {
	if HistoryValueKindOf(raw) != HistoryDelta {
		return DecodeWireTxStamps(raw)
	}
	var tm *types.TemporalMetadata
	if node {
		d, err := DecodeNodeHistoryDelta(raw)
		if err != nil {
			return 0, 0, 0, err
		}
		tm = SelectionTemporalMetaOfNodeWire(d.Meta)
	} else {
		d, err := DecodeRelHistoryDelta(raw)
		if err != nil {
			return 0, 0, 0, err
		}
		tm = SelectionTemporalMetaOfRelWire(d.Meta)
	}
	if tm == nil {
		return 0, 0, 0, nil
	}
	return tm.TxFrom, tm.TxTo, tm.DeletedAt, nil
}

// decodeWireTemporalMetaSlow is the SafeUnmarshal authority behind a declined
// scan, kept out of line so the scanner path's result stays on the stack.
func decodeWireTemporalMetaSlow(raw []byte) (wireTemporalMetaPartial, error) {
	var w wireTemporalMetaPartial
	err := SafeUnmarshal(raw, &w)
	return w, err
}

// The partial/scanner path is a trust boundary too: a malformed endpoint must
// not influence version selection before the checked full row is hydrated.
func validateSelectionTemporal(w wireTemporalMetaPartial) error {
	if err := checkRowFormatVersion("wire temporal meta", w.FormatVersion); err != nil {
		return err
	}
	if w.Version < 0 || int64(w.Version) > maxWireUint32 {
		return fmt.Errorf("%w: invalid temporal version", storepkg.ErrCorruptWire)
	}
	if !w.HasTemporal && (w.ValidEndpoints != 0 || w.ValidFrom != 0 || w.ValidTo != 0 || w.TxFrom != 0 || w.TxTo != 0 || w.CreatedAt != 0 || w.UpdatedAt != 0 || w.DeletedAt != 0 || w.Retracted) {
		return fmt.Errorf("%w: temporal payload without block", storepkg.ErrCorruptWire)
	}
	if w.ValidEndpoints != 0 && w.FormatVersion < EndpointWireFormatVersion {
		return fmt.Errorf("%w: endpoint presence requires fv4", storepkg.ErrCorruptWire)
	}
	tm := selectionTemporalMeta(w.HasTemporal, w.ValidFrom, w.ValidTo, w.TxFrom, w.TxTo, w.CreatedAt, w.UpdatedAt, w.DeletedAt, w.Retracted, w.ValidEndpoints)
	if err := tm.ValidateValidBounds(); err != nil {
		return fmt.Errorf("%w: %w", storepkg.ErrCorruptWire, err)
	}
	if w.ValidEndpoints == 0 && w.ValidFrom != 0 && w.ValidTo != 0 && w.ValidFrom >= w.ValidTo {
		return fmt.Errorf("%w: reversed scalar interval", storepkg.ErrCorruptWire)
	}
	if w.Retracted && w.DeletedAt == 0 {
		return fmt.Errorf("%w: retracted without delete stamp", storepkg.ErrCorruptWire)
	}
	return nil
}
func partialNodeTemporal(w NodeWire) wireTemporalMetaPartial {
	return wireTemporalMetaPartial{w.FormatVersion, w.Version, w.HasTemporal, w.ValidFrom, w.ValidTo, w.ValidEndpoints, w.TxFrom, w.TxTo, w.CreatedAt, w.UpdatedAt, w.DeletedAt, w.Retracted}
}
func partialRelTemporal(w RelWire) wireTemporalMetaPartial {
	return wireTemporalMetaPartial{w.FormatVersion, w.Version, w.HasTemporal, w.ValidFrom, w.ValidTo, w.ValidEndpoints, w.TxFrom, w.TxTo, w.CreatedAt, w.UpdatedAt, w.DeletedAt, w.Retracted}
}

// The fallback retains a partial decode, while sharing the full decoder's
// strict unsigned-byte checks; reflection would silently truncate ep/fv.
func (w *wireTemporalMetaPartial) DecodeMsgpack(dec *msgpack.Decoder) error {
	nf, err := dec.DecodeMapLen()
	if err != nil {
		return err
	}
	kbp := wireKeyBufPool.Get().(*[]byte)
	kb := *kbp
	defer wireKeyBufPool.Put(kbp)
	for range nf {
		kn, err := decodeMapKeyLen(dec, kb)
		if err != nil {
			return err
		}
		switch string(kb[:kn]) {
		case "fv":
			w.FormatVersion, err = decodeWireUint8(dec)
		case "ep":
			w.ValidEndpoints, err = decodeWireUint8(dec)
		case "v":
			w.Version, err = dec.DecodeInt()
		case "ht":
			w.HasTemporal, err = dec.DecodeBool()
		case "vf":
			w.ValidFrom, err = dec.DecodeInt64()
		case "vt":
			w.ValidTo, err = dec.DecodeInt64()
		case "tf":
			w.TxFrom, err = dec.DecodeInt64()
		case "tt":
			w.TxTo, err = dec.DecodeInt64()
		case "ca":
			w.CreatedAt, err = dec.DecodeInt64()
		case "ua":
			w.UpdatedAt, err = dec.DecodeInt64()
		case "da":
			w.DeletedAt, err = dec.DecodeInt64()
		case "rx":
			w.Retracted, err = dec.DecodeBool()
		default:
			err = dec.Skip()
		}
		if err != nil {
			return err
		}
	}
	return nil
}
