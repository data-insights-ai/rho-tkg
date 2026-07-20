package storeutil

import "github.com/vmihailenco/msgpack/v5"

// Hand-written msgpack encoders/decoders for NodeHistoryDelta/RelHistoryDelta
// (wire_history_delta.go) and their nested propKeyRef type (BACKLOG 15t). Same
// shape as BACKLOG 15s (changelog_wire.go): the embedded Meta NodeWire/RelWire
// field and PS []PropertyWire elements already dispatch to their own
// hand-written codecs once reflection reaches them, but the outer 3-field
// wrapper struct — and propKeyRef, its own PR-element type, which had no
// custom encoder either — were walked via reflection. Byte-identity with the
// previous pure-reflection encoding is locked by the golden vectors in
// history_delta_encode_golden_test.go (captured BEFORE these methods
// existed). Map-key emission order matches each struct's field DECLARATION
// order in wire_history_delta.go.

var (
	_ msgpack.CustomEncoder = NodeHistoryDelta{}
	_ msgpack.CustomEncoder = RelHistoryDelta{}
	_ msgpack.CustomEncoder = propKeyRef{}

	_ msgpack.CustomDecoder = (*NodeHistoryDelta)(nil)
	_ msgpack.CustomDecoder = (*RelHistoryDelta)(nil)
	_ msgpack.CustomDecoder = (*propKeyRef)(nil)
)

// --- propKeyRef ---

func (r propKeyRef) EncodeMsgpack(enc *msgpack.Encoder) error {
	fields := 0
	if r.Token != 0 {
		fields++
	}
	if r.Key != "" {
		fields++
	}
	if err := enc.EncodeMapLen(fields); err != nil {
		return err
	}
	if r.Token != 0 {
		if err := encodeStringUint16Field(enc, "kt", r.Token); err != nil {
			return err
		}
	}
	if r.Key != "" {
		if err := encodeStringStringField(enc, "k", r.Key); err != nil {
			return err
		}
	}
	return nil
}

func (r *propKeyRef) DecodeMsgpack(dec *msgpack.Decoder) error {
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
		case "kt":
			r.Token, err = dec.DecodeUint16()
		case "k":
			r.Key, err = dec.DecodeString()
		default:
			err = dec.Skip()
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// encodePropKeyRefArray writes a []propKeyRef under key without reflection,
// mirroring encodePropertyArray's array-header-then-per-element shape.
func encodePropKeyRefArray(enc *msgpack.Encoder, key string, refs []propKeyRef) error {
	if err := enc.EncodeString(key); err != nil {
		return err
	}
	if err := enc.EncodeArrayLen(len(refs)); err != nil {
		return err
	}
	for i := range refs {
		if err := refs[i].EncodeMsgpack(enc); err != nil {
			return err
		}
	}
	return nil
}

func decodePropKeyRefArray(dec *msgpack.Decoder) ([]propKeyRef, error) {
	n, err := dec.DecodeArrayLen()
	if err != nil {
		return nil, err
	}
	if n <= 0 {
		return nil, nil
	}
	out := make([]propKeyRef, n)
	for i := 0; i < n; i++ {
		if err := out[i].DecodeMsgpack(dec); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// --- NodeHistoryDelta ---

func (d NodeHistoryDelta) EncodeMsgpack(enc *msgpack.Encoder) error {
	fields := 1
	if len(d.PS) > 0 {
		fields++
	}
	if len(d.PR) > 0 {
		fields++
	}
	if err := enc.EncodeMapLen(fields); err != nil {
		return err
	}
	if err := enc.EncodeString("m"); err != nil {
		return err
	}
	if err := d.Meta.EncodeMsgpack(enc); err != nil {
		return err
	}
	if len(d.PS) > 0 {
		if err := encodePropertyArray(enc, "ps", d.PS); err != nil {
			return err
		}
	}
	if len(d.PR) > 0 {
		if err := encodePropKeyRefArray(enc, "pr", d.PR); err != nil {
			return err
		}
	}
	return nil
}

func (d *NodeHistoryDelta) DecodeMsgpack(dec *msgpack.Decoder) error {
	n, err := dec.DecodeMapLen()
	if err != nil {
		return err
	}
	kbp := wireKeyBufPool.Get().(*[]byte)
	kb := *kbp
	defer wireKeyBufPool.Put(kbp)
	for i := 0; i < n; i++ {
		key, err := dec.DecodeString()
		if err != nil {
			return err
		}
		switch key {
		case "m":
			err = d.Meta.DecodeMsgpack(dec)
		case "ps":
			d.PS, err = decodePropertyArray(dec, kb)
		case "pr":
			d.PR, err = decodePropKeyRefArray(dec)
		default:
			err = dec.Skip()
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// --- RelHistoryDelta ---

func (d RelHistoryDelta) EncodeMsgpack(enc *msgpack.Encoder) error {
	fields := 1
	if len(d.PS) > 0 {
		fields++
	}
	if len(d.PR) > 0 {
		fields++
	}
	if err := enc.EncodeMapLen(fields); err != nil {
		return err
	}
	if err := enc.EncodeString("m"); err != nil {
		return err
	}
	if err := d.Meta.EncodeMsgpack(enc); err != nil {
		return err
	}
	if len(d.PS) > 0 {
		if err := encodePropertyArray(enc, "ps", d.PS); err != nil {
			return err
		}
	}
	if len(d.PR) > 0 {
		if err := encodePropKeyRefArray(enc, "pr", d.PR); err != nil {
			return err
		}
	}
	return nil
}

func (d *RelHistoryDelta) DecodeMsgpack(dec *msgpack.Decoder) error {
	n, err := dec.DecodeMapLen()
	if err != nil {
		return err
	}
	kbp := wireKeyBufPool.Get().(*[]byte)
	kb := *kbp
	defer wireKeyBufPool.Put(kbp)
	for i := 0; i < n; i++ {
		key, err := dec.DecodeString()
		if err != nil {
			return err
		}
		switch key {
		case "m":
			err = d.Meta.DecodeMsgpack(dec)
		case "ps":
			d.PS, err = decodePropertyArray(dec, kb)
		case "pr":
			d.PR, err = decodePropKeyRefArray(dec)
		default:
			err = dec.Skip()
		}
		if err != nil {
			return err
		}
	}
	return nil
}
