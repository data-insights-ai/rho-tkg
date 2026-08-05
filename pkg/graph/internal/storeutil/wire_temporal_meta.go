package storeutil

import (
	"fmt"

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
// TemporalMetadata carries the numeric instants only. CreatedBy / UpdatedBy /
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
	if w.FormatVersion > CurrentWireFormatVersion {
		return 0, nil, fmt.Errorf("wire temporal meta: row format version %d, this binary supports up to %d: %w",
			w.FormatVersion, CurrentWireFormatVersion, storepkg.ErrWireFormatVersionUnsupported)
	}
	if w.Version < 0 {
		return 0, nil, fmt.Errorf("wire temporal meta: negative version %d: %w", w.Version, storepkg.ErrCorruptWire)
	}
	return uint32(w.Version), selectionTemporalMeta(w.HasTemporal, w.ValidFrom, w.ValidTo, w.TxFrom, w.TxTo, w.CreatedAt, w.UpdatedAt, w.DeletedAt), nil // #nosec G115 — non-negative checked above
}

// SelectionTemporalMetaOfNodeWire builds the selection-scope temporal metadata
// from an already-decoded NodeWire (a delta row's Meta carries the target
// version's temporal verbatim, so no partial decode is needed there).
func SelectionTemporalMetaOfNodeWire(w NodeWire) *types.TemporalMetadata {
	return selectionTemporalMeta(w.HasTemporal, w.ValidFrom, w.ValidTo, w.TxFrom, w.TxTo, w.CreatedAt, w.UpdatedAt, w.DeletedAt)
}

// SelectionTemporalMetaOfRelWire mirrors SelectionTemporalMetaOfNodeWire.
func SelectionTemporalMetaOfRelWire(w RelWire) *types.TemporalMetadata {
	return selectionTemporalMeta(w.HasTemporal, w.ValidFrom, w.ValidTo, w.TxFrom, w.TxTo, w.CreatedAt, w.UpdatedAt, w.DeletedAt)
}

// selectionTemporalMeta mirrors applyNodeWireFields' temporal construction for
// the numeric instants: a temporal block exists iff HasTemporal (the checked
// decoders reject payload-without-ht, so ht is authoritative).
func selectionTemporalMeta(ht bool, vf, vt, tf, tt, ca, ua, da int64) *types.TemporalMetadata {
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
	}
}
