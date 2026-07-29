package storeutil

import (
	"encoding/binary"
	"fmt"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// The v2 wire format (CurrentWireFormatVersion >= 2) emits the transaction-time
// tail — the "tf" (TxFrom) and "tt" (TxTo) fields — as a FIXED-WIDTH,
// NON-OMITEMPTY trailing slot, always the last two map entries, in that order.
// Every other field is written first, exactly as in v1. This lets the ingest
// pipeline pre-encode the whole row on the producer thread with a zero tail and
// have the single applier patch in the stamped TxFrom/TxTo with a bounded memory
// write instead of a second msgpack pass (ADR-0006 §4.5).
//
// Layout of the 24-byte tail (msgpack, big-endian int64 values):
//
//	a2 74 66            "tf"  (fixstr len 2)
//	d3 <8 bytes>        int64 TxFrom  (forced full width — msgpcode.Int64)
//	a2 74 74            "tt"  (fixstr len 2)
//	d3 <8 bytes>        int64 TxTo
//
// msgpack's Encoder.EncodeInt64 always uses the 9-byte 0xd3 form (never the
// compact minimal-width form), so a zero value occupies byte-for-byte the same
// space the applier later overwrites — the crown equivalence property in
// wire_temporal_tail_test.go proves Patch(PreEncode(E, 0)) == Encode(E, T).
const (
	// wireTemporalTailLen is the total byte length of the v2 fixed tail.
	wireTemporalTailLen = 24
	// wireTxFromValueEnd/Start bound the TxFrom int64 value bytes, measured
	// from the END of the buffer.
	wireTxFromValueStartFromEnd = 20
	wireTxFromValueEndFromEnd   = 12
	// wireTxToValueStartFromEnd bounds the TxTo int64 value bytes; it ends at
	// the buffer end.
	wireTxToValueStartFromEnd = 8
)

// wireTfMarker/wireTtMarker are the exact 4-byte prefixes (key fixstr + int64
// type code) that MUST precede each slot's 8 value bytes. PatchWireTemporalTail
// verifies them so a buffer that is not a v2 tail is rejected, never blindly
// overwritten.
var (
	wireTfMarker = [4]byte{0xa2, 't', 'f', 0xd3}
	wireTtMarker = [4]byte{0xa2, 't', 't', 0xd3}
)

// PatchWireTemporalTail overwrites the transaction-time tail of a v2 pre-encoded
// wire buffer in place with the applier-stamped txFrom and txTo. It validates
// the fixed slot markers before writing; a buffer that is too short or whose
// trailing bytes are not the expected v2 tail is rejected with ErrCorruptWire
// and left unmodified — a mis-fed buffer is never silently corrupted.
//
// The buffer is mutated in place (the pre-encoded prepare buffer is owned by the
// applier at patch time). Callers that must retain the zero-tail form should
// pass a copy.
func PatchWireTemporalTail(buf []byte, txFrom, txTo int64) error {
	if len(buf) < wireTemporalTailLen {
		return fmt.Errorf("wire temporal tail: buffer %d bytes, shorter than the %d-byte v2 tail: %w",
			len(buf), wireTemporalTailLen, storepkg.ErrCorruptWire)
	}
	L := len(buf)
	tfMark := buf[L-wireTemporalTailLen : L-wireTxFromValueStartFromEnd]
	ttMark := buf[L-wireTxFromValueEndFromEnd : L-wireTxToValueStartFromEnd]
	if [4]byte(tfMark) != wireTfMarker {
		return fmt.Errorf("wire temporal tail: tf slot marker mismatch (% x): %w", tfMark, storepkg.ErrCorruptWire)
	}
	if [4]byte(ttMark) != wireTtMarker {
		return fmt.Errorf("wire temporal tail: tt slot marker mismatch (% x): %w", ttMark, storepkg.ErrCorruptWire)
	}
	binary.BigEndian.PutUint64(buf[L-wireTxFromValueStartFromEnd:L-wireTxFromValueEndFromEnd], uint64(txFrom)) // #nosec G115 — round-trip of an int64
	binary.BigEndian.PutUint64(buf[L-wireTxToValueStartFromEnd:L], uint64(txTo))                               // #nosec G115 — round-trip of an int64
	return nil
}

// HasWireTemporalTail reports whether buf ends with a well-formed v2 fixed
// temporal tail. Used by the store-write consumption path to decide whether a
// pre-encoded buffer is patchable, and by tests.
func HasWireTemporalTail(buf []byte) bool {
	if len(buf) < wireTemporalTailLen {
		return false
	}
	L := len(buf)
	return [4]byte(buf[L-wireTemporalTailLen:L-wireTxFromValueStartFromEnd]) == wireTfMarker &&
		[4]byte(buf[L-wireTxFromValueEndFromEnd:L-wireTxToValueStartFromEnd]) == wireTtMarker
}

// PeekWireTemporalTail reads the transaction-time tail of a v2 wire buffer
// WITHOUT unmarshalling: it validates the fixed slot markers (the same check
// PatchWireTemporalTail performs before writing) and returns the raw TxFrom /
// TxTo int64 values. ok is false when the buffer is too short or its trailing
// bytes are not a well-formed v2 tail (e.g. a legacy v1 omitempty-tail row) —
// callers must then fall back to a full decode.
//
// This is the read-side dividend of the ADR-0006 §4.5 fixed tail: a
// transaction-time classification that needs ONLY TxFrom/TxTo (the as-of
// reverse walk's skip/visible/hidden verdict) can decide per version row in a
// bounded byte-peek instead of a full msgpack decode.
func PeekWireTemporalTail(buf []byte) (txFrom, txTo int64, ok bool) {
	if !HasWireTemporalTail(buf) {
		return 0, 0, false
	}
	L := len(buf)
	txFrom = int64(binary.BigEndian.Uint64(buf[L-wireTxFromValueStartFromEnd : L-wireTxFromValueEndFromEnd])) // #nosec G115 — round-trip of an int64
	txTo = int64(binary.BigEndian.Uint64(buf[L-wireTxToValueStartFromEnd : L]))                               // #nosec G115 — round-trip of an int64
	return txFrom, txTo, true
}

// PreEncodeNodeWireV2 encodes a node's wire with a ZERO transaction-time tail,
// suitable for later PatchWireTemporalTail. The node's own TxFrom/TxTo are
// ignored (forced to zero in the emitted slot); every other field is encoded
// verbatim. The row is stamped at CurrentWireFormatVersion.
func PreEncodeNodeWireV2(w NodeWire) ([]byte, error) {
	w.FormatVersion = CurrentWireFormatVersion
	w.TxFrom = 0
	w.TxTo = 0
	return marshalWirePooled(w)
}

// PreEncodeRelWireV2 is the relationship counterpart of PreEncodeNodeWireV2.
func PreEncodeRelWireV2(w RelWire) ([]byte, error) {
	w.FormatVersion = CurrentWireFormatVersion
	w.TxFrom = 0
	w.TxTo = 0
	return marshalWirePooled(w)
}

// PreEncodeNodeWireV2WithKeys pre-encodes a node's ENTITY-ROW wire exactly as
// the store's marshalNodeBytes (MarshalNodeWireWithKeys) would — property keys
// dictionary-encoded via the same shared property-key registry — but with a
// ZERO transaction-time tail the applier later patches (PatchWireTemporalTail).
//
// This is the tokenized counterpart of PreEncodeNodeWireV2: the plain variant
// keeps property keys as strings (matching the UNTOKENIZED change-log put body),
// while this variant matches the TOKENIZED persisted entity row. The extended
// crown property holds by construction: for the finalized node with the applier's
// stamped TxFrom == T,
//
//	Patch(PreEncodeNodeWireV2WithKeys(node, reg), T, 0) == MarshalNodeWireWithKeys(nodeₜ, reg)
//
// byte-for-byte, because the ONLY difference between the pre-encode and the
// store's own encode is the fixed transaction-time tail, and the tail is not
// hashed. The node MUST already carry its finalized labels, properties, version,
// hash and VT claims (TxFrom/TxTo are ignored — forced to zero in the slot); the
// registry MUST be the SAME instance the store marshals with (Core wires
// c.propKeys into every native backend via SetPropertyKeyRegistry), so a key's
// token is identical here and at flush. Property-key tokens are append-only and
// never change value once allocated, so a token resolved in prepare still matches
// what the store would emit at apply.
func PreEncodeNodeWireV2WithKeys(n *types.Node, reg *registrypkg.PropertyKeyRegistry) ([]byte, error) {
	w, err := NodeToWireChecked(n)
	if err != nil {
		return nil, err
	}
	ApplyPropertyKeyTokens(w.Properties, reg)
	w.FormatVersion = CurrentWireFormatVersion
	w.TxFrom = 0
	w.TxTo = 0
	return marshalWirePooled(w)
}

// PreEncodeRelWireV2WithKeys is the relationship counterpart of
// PreEncodeNodeWireV2WithKeys (matches the store's marshalRelBytes).
func PreEncodeRelWireV2WithKeys(r *types.Relationship, reg *registrypkg.PropertyKeyRegistry) ([]byte, error) {
	w, err := RelToWireChecked(r)
	if err != nil {
		return nil, err
	}
	ApplyPropertyKeyTokens(w.Properties, reg)
	w.FormatVersion = CurrentWireFormatVersion
	w.TxFrom = 0
	w.TxTo = 0
	return marshalWirePooled(w)
}
