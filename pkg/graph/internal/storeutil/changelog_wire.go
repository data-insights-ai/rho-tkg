package storeutil

import "github.com/vmihailenco/msgpack/v5"

// Hand-written msgpack encoders/decoders for the 10 change-log BODY WRAPPER
// types declared in changelog.go (BACKLOG 15s). NodeWire/RelWire/PropertyWire
// already avoid reflection for their OWN field walk (wire_encode.go /
// wire_decode.go); nesting one of them inside a body via a plain
// `msgpack:"w"` struct tag still pays reflection for the body's OWN 1-5
// top-level fields, on every change-log-enabled mutation (NodePutBody/
// RelPutBody are on the hottest path: emitted on every put, decoded on every
// record a replica applies). These methods eliminate that outer-struct
// reflection cost; a nested NodeWire/RelWire dispatches to its own
// EncodeMsgpack/DecodeMsgpack via a direct method call (no reflection either
// way). Byte-identity with the previous pure-reflection encoding is locked by
// the golden vectors in changelog_encode_golden_test.go (captured BEFORE
// these methods existed).
//
// Map-key emission order matches each struct's FIELD DECLARATION order in
// changelog.go, since that is the order Go's reflection-based msgpack walked
// fields in (and what the golden vectors were captured against) — changing
// declaration order in changelog.go without updating these methods (or vice
// versa) would silently desync the two.

var (
	_ msgpack.CustomEncoder = NodePutBody{}
	_ msgpack.CustomEncoder = RelPutBody{}
	_ msgpack.CustomEncoder = NodeDeleteBody{}
	_ msgpack.CustomEncoder = RelDeleteBody{}
	_ msgpack.CustomEncoder = ForeignIncomingDeleteBody{}
	_ msgpack.CustomEncoder = RangePurgeBody{}
	_ msgpack.CustomEncoder = HistoryVersionNodeBody{}
	_ msgpack.CustomEncoder = HistoryVersionRelBody{}
	_ msgpack.CustomEncoder = HistoryTruncateBody{}
	_ msgpack.CustomEncoder = MetaBody{}

	_ msgpack.CustomDecoder = (*NodePutBody)(nil)
	_ msgpack.CustomDecoder = (*RelPutBody)(nil)
	_ msgpack.CustomDecoder = (*NodeDeleteBody)(nil)
	_ msgpack.CustomDecoder = (*RelDeleteBody)(nil)
	_ msgpack.CustomDecoder = (*ForeignIncomingDeleteBody)(nil)
	_ msgpack.CustomDecoder = (*RangePurgeBody)(nil)
	_ msgpack.CustomDecoder = (*HistoryVersionNodeBody)(nil)
	_ msgpack.CustomDecoder = (*HistoryVersionRelBody)(nil)
	_ msgpack.CustomDecoder = (*HistoryTruncateBody)(nil)
	_ msgpack.CustomDecoder = (*MetaBody)(nil)
)

func encodeStringUint64Field(enc *msgpack.Encoder, key string, value uint64) error {
	if err := enc.EncodeString(key); err != nil {
		return err
	}
	return enc.EncodeUint64(value)
}

// encodeRelWireArray writes a []RelWire under key without reflection, mirroring
// encodePropertyArray's array-header-then-per-element-EncodeMsgpack shape.
func encodeRelWireArray(enc *msgpack.Encoder, key string, rels []RelWire) error {
	if err := enc.EncodeString(key); err != nil {
		return err
	}
	if err := enc.EncodeArrayLen(len(rels)); err != nil {
		return err
	}
	for i := range rels {
		if err := rels[i].EncodeMsgpack(enc); err != nil {
			return err
		}
	}
	return nil
}

// encodeInt64Array writes a []int64 under key without reflection.
func encodeInt64Array(enc *msgpack.Encoder, key string, ids []int64) error {
	if err := enc.EncodeString(key); err != nil {
		return err
	}
	if err := enc.EncodeArrayLen(len(ids)); err != nil {
		return err
	}
	for _, id := range ids {
		if err := enc.EncodeInt64(id); err != nil {
			return err
		}
	}
	return nil
}

// --- NodePutBody ---

func (b NodePutBody) EncodeMsgpack(enc *msgpack.Encoder) error {
	fields := 1
	if b.WithHistory {
		fields++
	}
	if err := enc.EncodeMapLen(fields); err != nil {
		return err
	}
	if err := enc.EncodeString("w"); err != nil {
		return err
	}
	if err := b.Wire.EncodeMsgpack(enc); err != nil {
		return err
	}
	if b.WithHistory {
		if err := encodeStringBoolField(enc, "wh", b.WithHistory); err != nil {
			return err
		}
	}
	return nil
}

func (b *NodePutBody) DecodeMsgpack(dec *msgpack.Decoder) error {
	n, err := dec.DecodeMapLen()
	if err != nil {
		return err
	}
	for i := 0; i < n; i++ {
		key, err := dec.DecodeString()
		if err != nil {
			return err
		}
		switch key {
		case "w":
			err = b.Wire.DecodeMsgpack(dec)
		case "wh":
			b.WithHistory, err = dec.DecodeBool()
		default:
			err = dec.Skip()
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// --- RelPutBody ---

func (b RelPutBody) EncodeMsgpack(enc *msgpack.Encoder) error {
	fields := 1
	if b.WithHistory {
		fields++
	}
	if err := enc.EncodeMapLen(fields); err != nil {
		return err
	}
	if err := enc.EncodeString("w"); err != nil {
		return err
	}
	if err := b.Wire.EncodeMsgpack(enc); err != nil {
		return err
	}
	if b.WithHistory {
		if err := encodeStringBoolField(enc, "wh", b.WithHistory); err != nil {
			return err
		}
	}
	return nil
}

func (b *RelPutBody) DecodeMsgpack(dec *msgpack.Decoder) error {
	n, err := dec.DecodeMapLen()
	if err != nil {
		return err
	}
	for i := 0; i < n; i++ {
		key, err := dec.DecodeString()
		if err != nil {
			return err
		}
		switch key {
		case "w":
			err = b.Wire.DecodeMsgpack(dec)
		case "wh":
			b.WithHistory, err = dec.DecodeBool()
		default:
			err = dec.Skip()
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// --- NodeDeleteBody ---

func (b NodeDeleteBody) EncodeMsgpack(enc *msgpack.Encoder) error {
	fields := 1
	if b.WithHistory {
		fields++
	}
	if b.Tombstone != nil {
		fields++
	}
	if len(b.RelTombstones) > 0 {
		fields++
	}
	if len(b.CascadedRelIDs) > 0 {
		fields++
	}
	if err := enc.EncodeMapLen(fields); err != nil {
		return err
	}
	if err := encodeStringInt64Field(enc, "id", b.ID); err != nil {
		return err
	}
	if b.WithHistory {
		if err := encodeStringBoolField(enc, "wh", b.WithHistory); err != nil {
			return err
		}
	}
	if b.Tombstone != nil {
		if err := enc.EncodeString("tn"); err != nil {
			return err
		}
		if err := b.Tombstone.EncodeMsgpack(enc); err != nil {
			return err
		}
	}
	if len(b.RelTombstones) > 0 {
		if err := encodeRelWireArray(enc, "rt", b.RelTombstones); err != nil {
			return err
		}
	}
	if len(b.CascadedRelIDs) > 0 {
		if err := encodeInt64Array(enc, "cr", b.CascadedRelIDs); err != nil {
			return err
		}
	}
	return nil
}

func (b *NodeDeleteBody) DecodeMsgpack(dec *msgpack.Decoder) error {
	n, err := dec.DecodeMapLen()
	if err != nil {
		return err
	}
	for i := 0; i < n; i++ {
		key, err := dec.DecodeString()
		if err != nil {
			return err
		}
		switch key {
		case "id":
			b.ID, err = dec.DecodeInt64()
		case "wh":
			b.WithHistory, err = dec.DecodeBool()
		case "tn":
			var w NodeWire
			if err = w.DecodeMsgpack(dec); err == nil {
				b.Tombstone = &w
			}
		case "rt":
			b.RelTombstones, err = decodeRelWireArray(dec)
		case "cr":
			b.CascadedRelIDs, err = decodeInt64Array(dec)
		default:
			err = dec.Skip()
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// --- RelDeleteBody ---

func (b RelDeleteBody) EncodeMsgpack(enc *msgpack.Encoder) error {
	fields := 1
	if b.WithHistory {
		fields++
	}
	if b.Tombstone != nil {
		fields++
	}
	if err := enc.EncodeMapLen(fields); err != nil {
		return err
	}
	if err := encodeStringInt64Field(enc, "id", b.ID); err != nil {
		return err
	}
	if b.WithHistory {
		if err := encodeStringBoolField(enc, "wh", b.WithHistory); err != nil {
			return err
		}
	}
	if b.Tombstone != nil {
		if err := enc.EncodeString("tr"); err != nil {
			return err
		}
		if err := b.Tombstone.EncodeMsgpack(enc); err != nil {
			return err
		}
	}
	return nil
}

func (b *RelDeleteBody) DecodeMsgpack(dec *msgpack.Decoder) error {
	n, err := dec.DecodeMapLen()
	if err != nil {
		return err
	}
	for i := 0; i < n; i++ {
		key, err := dec.DecodeString()
		if err != nil {
			return err
		}
		switch key {
		case "id":
			b.ID, err = dec.DecodeInt64()
		case "wh":
			b.WithHistory, err = dec.DecodeBool()
		case "tr":
			var w RelWire
			if err = w.DecodeMsgpack(dec); err == nil {
				b.Tombstone = &w
			}
		default:
			err = dec.Skip()
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// --- ForeignIncomingDeleteBody ---

func (b ForeignIncomingDeleteBody) EncodeMsgpack(enc *msgpack.Encoder) error {
	if err := enc.EncodeMapLen(2); err != nil {
		return err
	}
	if err := encodeStringInt64Field(enc, "id", b.RelID); err != nil {
		return err
	}
	return encodeStringInt64Field(enc, "e", b.EndID)
}

func (b *ForeignIncomingDeleteBody) DecodeMsgpack(dec *msgpack.Decoder) error {
	n, err := dec.DecodeMapLen()
	if err != nil {
		return err
	}
	for i := 0; i < n; i++ {
		key, err := dec.DecodeString()
		if err != nil {
			return err
		}
		switch key {
		case "id":
			b.RelID, err = dec.DecodeInt64()
		case "e":
			b.EndID, err = dec.DecodeInt64()
		default:
			err = dec.Skip()
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// --- RangePurgeBody ---

func (b RangePurgeBody) EncodeMsgpack(enc *msgpack.Encoder) error {
	fields := 2
	if b.Mode != 0 {
		fields++
	}
	if err := enc.EncodeMapLen(fields); err != nil {
		return err
	}
	if err := encodeStringUint16Field(enc, "l", b.LabelToken); err != nil {
		return err
	}
	if err := encodeStringInt64Field(enc, "b", b.Before); err != nil {
		return err
	}
	if b.Mode != 0 {
		if err := encodeStringUint8Field(enc, "m", b.Mode); err != nil {
			return err
		}
	}
	return nil
}

func (b *RangePurgeBody) DecodeMsgpack(dec *msgpack.Decoder) error {
	n, err := dec.DecodeMapLen()
	if err != nil {
		return err
	}
	for i := 0; i < n; i++ {
		key, err := dec.DecodeString()
		if err != nil {
			return err
		}
		switch key {
		case "l":
			b.LabelToken, err = dec.DecodeUint16()
		case "b":
			b.Before, err = dec.DecodeInt64()
		case "m":
			b.Mode, err = dec.DecodeUint8()
		default:
			err = dec.Skip()
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// --- HistoryVersionNodeBody ---

func (b HistoryVersionNodeBody) EncodeMsgpack(enc *msgpack.Encoder) error {
	if err := enc.EncodeMapLen(2); err != nil {
		return err
	}
	if err := encodeStringUint64Field(enc, "v", b.Version); err != nil {
		return err
	}
	if err := enc.EncodeString("w"); err != nil {
		return err
	}
	return b.Wire.EncodeMsgpack(enc)
}

func (b *HistoryVersionNodeBody) DecodeMsgpack(dec *msgpack.Decoder) error {
	n, err := dec.DecodeMapLen()
	if err != nil {
		return err
	}
	for i := 0; i < n; i++ {
		key, err := dec.DecodeString()
		if err != nil {
			return err
		}
		switch key {
		case "v":
			b.Version, err = dec.DecodeUint64()
		case "w":
			err = b.Wire.DecodeMsgpack(dec)
		default:
			err = dec.Skip()
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// --- HistoryVersionRelBody ---

func (b HistoryVersionRelBody) EncodeMsgpack(enc *msgpack.Encoder) error {
	if err := enc.EncodeMapLen(2); err != nil {
		return err
	}
	if err := encodeStringUint64Field(enc, "v", b.Version); err != nil {
		return err
	}
	if err := enc.EncodeString("w"); err != nil {
		return err
	}
	return b.Wire.EncodeMsgpack(enc)
}

func (b *HistoryVersionRelBody) DecodeMsgpack(dec *msgpack.Decoder) error {
	n, err := dec.DecodeMapLen()
	if err != nil {
		return err
	}
	for i := 0; i < n; i++ {
		key, err := dec.DecodeString()
		if err != nil {
			return err
		}
		switch key {
		case "v":
			b.Version, err = dec.DecodeUint64()
		case "w":
			err = b.Wire.DecodeMsgpack(dec)
		default:
			err = dec.Skip()
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// --- HistoryTruncateBody ---

func (b HistoryTruncateBody) EncodeMsgpack(enc *msgpack.Encoder) error {
	fields := 2
	if b.IsTrim {
		fields++
	}
	if err := enc.EncodeMapLen(fields); err != nil {
		return err
	}
	if err := encodeStringInt64Field(enc, "id", b.ID); err != nil {
		return err
	}
	if b.IsTrim {
		if err := encodeStringBoolField(enc, "tr", b.IsTrim); err != nil {
			return err
		}
	}
	return encodeStringInt64Field(enc, "b", b.Bound)
}

func (b *HistoryTruncateBody) DecodeMsgpack(dec *msgpack.Decoder) error {
	n, err := dec.DecodeMapLen()
	if err != nil {
		return err
	}
	for i := 0; i < n; i++ {
		key, err := dec.DecodeString()
		if err != nil {
			return err
		}
		switch key {
		case "id":
			b.ID, err = dec.DecodeInt64()
		case "tr":
			b.IsTrim, err = dec.DecodeBool()
		case "b":
			b.Bound, err = dec.DecodeInt64()
		default:
			err = dec.Skip()
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// --- MetaBody ---

func (b MetaBody) EncodeMsgpack(enc *msgpack.Encoder) error {
	fields := 1
	if len(b.Value) > 0 {
		fields++
	}
	if err := enc.EncodeMapLen(fields); err != nil {
		return err
	}
	if err := encodeStringStringField(enc, "k", b.Key); err != nil {
		return err
	}
	if len(b.Value) > 0 {
		if err := encodeStringBytesField(enc, "v", b.Value); err != nil {
			return err
		}
	}
	return nil
}

func (b *MetaBody) DecodeMsgpack(dec *msgpack.Decoder) error {
	n, err := dec.DecodeMapLen()
	if err != nil {
		return err
	}
	for i := 0; i < n; i++ {
		key, err := dec.DecodeString()
		if err != nil {
			return err
		}
		switch key {
		case "k":
			b.Key, err = dec.DecodeString()
		case "v":
			b.Value, err = dec.DecodeBytes()
		default:
			err = dec.Skip()
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// --- shared array decode helpers ---

func decodeRelWireArray(dec *msgpack.Decoder) ([]RelWire, error) {
	n, err := dec.DecodeArrayLen()
	if err != nil {
		return nil, err
	}
	if n <= 0 {
		return nil, nil
	}
	out := make([]RelWire, n)
	for i := 0; i < n; i++ {
		if err := out[i].DecodeMsgpack(dec); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func decodeInt64Array(dec *msgpack.Decoder) ([]int64, error) {
	n, err := dec.DecodeArrayLen()
	if err != nil {
		return nil, err
	}
	if n <= 0 {
		return nil, nil
	}
	out := make([]int64, n)
	for i := 0; i < n; i++ {
		v, err := dec.DecodeInt64()
		if err != nil {
			return nil, err
		}
		out[i] = v
	}
	return out, nil
}
