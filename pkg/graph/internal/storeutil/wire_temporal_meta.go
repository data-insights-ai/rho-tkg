package storeutil

import (
	"fmt"
	"slices"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
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
	FormatVersion uint8 `msgpack:"fv"`
	Version       int   `msgpack:"v"`
	HasTemporal   bool  `msgpack:"ht"`
	ValidFrom     int64 `msgpack:"vf"`
	ValidTo       int64 `msgpack:"vt"`
	TxFrom        int64 `msgpack:"tf"`
	TxTo          int64 `msgpack:"tt"`
	CreatedAt     int64 `msgpack:"ca"`
	UpdatedAt     int64 `msgpack:"ua"`
	DeletedAt     int64 `msgpack:"da"`
	Retracted     bool  `msgpack:"rx"`
	Void          bool  `msgpack:"vd"`
	// DeleteKeptValidTo decides what the row's ValidTo means at a pin
	// (types.TemporalMetadata.ValidToAtTxTime), a selection input like Retracted.
	DeleteKeptValidTo bool `msgpack:"dk"`
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
	return wireTemporalMetaOf(w)
}

// wireRowTokens is a row's tokens as the wire holds them: a node's labels
// ("pl", "el"; NodeWire.PrimaryLabel, ExtraLabels), a relationship's type
// ("rt"; RelWire.RelType), before nodeLabelTokens or relTypeToken checks them.
type wireRowTokens struct {
	Primary int64   `msgpack:"pl"`
	Extra   []int64 `msgpack:"el"`
	RelType int64   `msgpack:"rt"`
}

// wireTemporalMetaLabelsPartial is the SafeUnmarshal arm of
// DecodeWireNodeTemporalMetaLabels and DecodeWireRelTemporalMetaType: the
// selection fields and the row's tokens.
type wireTemporalMetaLabelsPartial struct {
	wireTemporalMetaPartial `msgpack:",inline"`
	wireRowTokens           `msgpack:",inline"`
}

// DecodeWireNodeTemporalMetaLabels is DecodeWireTemporalMeta for a FULL node
// row that also returns the label tokens the row carries, primary first, in
// the same single pass (the scanner captures "pl" and "el"; a declined scan
// falls back to one SafeUnmarshal of the selection fields and the labels).
// The tokens are checked by the full decoder's rules (validateNodeWireFields):
// a primary token in 1..65535, extra tokens in that range, none repeated; a
// row that breaks them fails closed with store.ErrCorruptWire, as GetNode
// would fail on it. What the valid-end index's first call reads a node's
// current labels from (R2-2 VEATTRIB).
func DecodeWireNodeTemporalMetaLabels(raw []byte) (uint32, *types.TemporalMetadata, []uint16, error) {
	var lab wireRowTokens
	w, ok := scanWireTemporalMetaInto(raw, &lab)
	if !ok {
		var full wireTemporalMetaLabelsPartial
		if err := SafeUnmarshal(raw, &full); err != nil {
			return 0, nil, nil, err
		}
		w, lab = full.wireTemporalMetaPartial, full.wireRowTokens
	}
	version, tm, err := wireTemporalMetaOf(w)
	if err != nil {
		return 0, nil, nil, err
	}
	toks, err := nodeLabelTokens(lab.Primary, lab.Extra)
	if err != nil {
		return 0, nil, nil, err
	}
	return version, tm, toks, nil
}

// DecodeWireRelTemporalMetaType is DecodeWireNodeTemporalMetaLabels for a FULL
// relationship row: its version, temporal block and type token in one pass,
// the token checked by the full decoder's rule (1..65535, else
// store.ErrCorruptWire).
func DecodeWireRelTemporalMetaType(raw []byte) (uint32, *types.TemporalMetadata, uint16, error) {
	var tok wireRowTokens
	w, ok := scanWireTemporalMetaInto(raw, &tok)
	if !ok {
		var full wireTemporalMetaLabelsPartial
		if err := SafeUnmarshal(raw, &full); err != nil {
			return 0, nil, 0, err
		}
		w, tok = full.wireTemporalMetaPartial, full.wireRowTokens
	}
	version, tm, err := wireTemporalMetaOf(w)
	if err != nil {
		return 0, nil, 0, err
	}
	rt, err := relTypeToken(tok.RelType)
	if err != nil {
		return 0, nil, 0, err
	}
	return version, tm, rt, nil
}

// relTypeToken checks a relationship row's type token by the full decoder's
// rule (validateRelWireFields).
func relTypeToken(rt int64) (uint16, error) {
	if rt <= 0 || rt > maxWireUint16 {
		return 0, fmt.Errorf("relationship wire: type token %d outside 1..%d: %w", rt, maxWireUint16, storepkg.ErrCorruptWire)
	}
	return uint16(rt), nil // #nosec G115 -- range checked above
}

// nodeLabelTokens checks a node row's label tokens by the rules the full
// decoder applies (validateNodeWireFields) and returns them, primary first.
func nodeLabelTokens(primary int64, extra []int64) ([]uint16, error) {
	if primary <= 0 || primary > maxWireUint16 {
		return nil, fmt.Errorf("node wire: primary label token %d outside 1..%d: %w", primary, maxWireUint16, storepkg.ErrCorruptWire)
	}
	toks := make([]uint16, 0, 1+len(extra))
	toks = append(toks, uint16(primary)) // #nosec G115 -- range checked above
	for i, e := range extra {
		if e <= 0 || e > maxWireUint16 {
			return nil, fmt.Errorf("node wire: extra label[%d] token %d outside 1..%d: %w", i, e, maxWireUint16, storepkg.ErrCorruptWire)
		}
		tok := uint16(e) // #nosec G115 -- range checked above
		if slices.Contains(toks, tok) {
			return nil, fmt.Errorf("node wire: extra label[%d] repeats token %d: %w", i, tok, storepkg.ErrCorruptWire)
		}
		toks = append(toks, tok)
	}
	return toks, nil
}

// wireTemporalMetaOf checks a decoded partial row and builds its version and
// selection-scope temporal metadata (DecodeWireTemporalMeta).
func wireTemporalMetaOf(w wireTemporalMetaPartial) (uint32, *types.TemporalMetadata, error) {
	if err := checkRowFormatVersion("wire temporal meta", w.FormatVersion); err != nil {
		return 0, nil, err
	}
	if w.Version < 0 {
		return 0, nil, fmt.Errorf("wire temporal meta: negative version %d: %w", w.Version, storepkg.ErrCorruptWire)
	}
	return uint32(w.Version), selectionTemporalMeta(w.HasTemporal, w.ValidFrom, w.ValidTo, w.TxFrom, w.TxTo, w.CreatedAt, w.UpdatedAt, w.DeletedAt, w.Retracted, w.Void, w.DeleteKeptValidTo), nil // #nosec G115 — non-negative checked above
}

// SelectionTemporalMetaOfNodeWire builds the selection-scope temporal metadata
// from an already-decoded NodeWire (a delta row's Meta carries the target
// version's temporal verbatim, so no partial decode is needed there).
func SelectionTemporalMetaOfNodeWire(w NodeWire) *types.TemporalMetadata {
	return selectionTemporalMeta(w.HasTemporal, w.ValidFrom, w.ValidTo, w.TxFrom, w.TxTo, w.CreatedAt, w.UpdatedAt, w.DeletedAt, w.Retracted, w.Void, w.DeleteKeptValidTo)
}

// SelectionTemporalMetaOfRelWire mirrors SelectionTemporalMetaOfNodeWire.
func SelectionTemporalMetaOfRelWire(w RelWire) *types.TemporalMetadata {
	return selectionTemporalMeta(w.HasTemporal, w.ValidFrom, w.ValidTo, w.TxFrom, w.TxTo, w.CreatedAt, w.UpdatedAt, w.DeletedAt, w.Retracted, w.Void, w.DeleteKeptValidTo)
}

// selectionTemporalMeta mirrors applyNodeWireFields' temporal construction for
// the numeric instants and the retraction marker (a selection input: it caps
// its life, chainLifeEnds): a temporal block exists iff HasTemporal (the
// checked decoders reject payload-without-ht, so ht is authoritative).
func selectionTemporalMeta(ht bool, vf, vt, tf, tt, ca, ua, da int64, rx, vd, dk bool) *types.TemporalMetadata {
	if !ht {
		return nil
	}
	return &types.TemporalMetadata{
		ValidFrom: types.Instant(vf),
		ValidTo:   types.Instant(vt),
		TxFrom:    types.Instant(tf),
		TxTo:      types.Instant(tt),
		CreatedAt: types.Instant(ca),
		UpdatedAt: types.Instant(ua),
		DeletedAt: types.Instant(da),
		Retracted: rx,
		Void:      vd,

		DeleteKeptValidTo: dk,
	}
}

// DecodeWireTxStamps is DecodeWireRowStamps without the version: the
// transaction stamps of a FULL (non-delta) row, zeros when the row carries no
// temporal block, the same errors, no allocation on the scanner path.
func DecodeWireTxStamps(raw []byte) (txFrom, txTo, deletedAt types.Instant, err error) {
	s, err := DecodeWireRowStamps(raw)
	return s.TxFrom, s.TxTo, s.DeletedAt, err
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
