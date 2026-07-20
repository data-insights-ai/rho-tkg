package badger

import "github.com/vmihailenco/msgpack/v5"

// Hand-written msgpack encoders/decoders for the index-DEFINITION persistence
// types (hfIdxDef, propIdxDef, vectorIdxDef, compositeIdxDef, relPropIdxDef —
// BACKLOG 15v, same reflection audit as BACKLOG 15s/15t). These are ADMIN/
// GROWTH-path types (persisted on index creation/removal, never per-entity-
// write), so the win is smaller than 15s's hottest paths, but the pattern and
// the underlying reflection cost (a generic per-field struct walk with
// omitempty checks) are identical. Byte-identity with the previous
// pure-reflection encoding is locked by the golden vectors in
// idx_def_wire_golden_test.go (captured BEFORE these methods existed).
// Map-key emission order matches each struct's field DECLARATION order.
//
// These types are persisted as top-level SLICES (msgpack.Marshal(defs), defs
// []hfIdxDef etc.) — the outer slice-of-struct walk still goes through
// msgpack's generic encodeArrayValue/DecodeArrayLen (reflect.Value.Index(i)
// per element), since there is no dedicated slice fast path for a
// caller-defined struct type. That per-element dispatch is cheap (a cached
// type lookup, not a field walk) and is the same residual cost 15s/15t leave
// in place for their own nested struct-slice fields; what these methods
// eliminate is the expensive part — per-FIELD reflection with omitempty
// testing — since each element's EncodeMsgpack/DecodeMsgpack is now called
// directly once the per-element dispatch reaches it.

var (
	_ msgpack.CustomEncoder = hfIdxDef{}
	_ msgpack.CustomEncoder = propIdxDef{}
	_ msgpack.CustomEncoder = vectorIdxDef{}
	_ msgpack.CustomEncoder = compositeIdxDef{}
	_ msgpack.CustomEncoder = relPropIdxDef{}

	_ msgpack.CustomDecoder = (*hfIdxDef)(nil)
	_ msgpack.CustomDecoder = (*propIdxDef)(nil)
	_ msgpack.CustomDecoder = (*vectorIdxDef)(nil)
	_ msgpack.CustomDecoder = (*compositeIdxDef)(nil)
	_ msgpack.CustomDecoder = (*relPropIdxDef)(nil)
)

func idxWireEncodeStringUint16Field(enc *msgpack.Encoder, key string, value uint16) error {
	if err := enc.EncodeString(key); err != nil {
		return err
	}
	return enc.EncodeUint16(value)
}

func idxWireEncodeStringInt64Field(enc *msgpack.Encoder, key string, value int64) error {
	if err := enc.EncodeString(key); err != nil {
		return err
	}
	return enc.EncodeInt64(value)
}

func idxWireEncodeStringIntField(enc *msgpack.Encoder, key string, value int) error {
	if err := enc.EncodeString(key); err != nil {
		return err
	}
	return enc.EncodeInt(int64(value))
}

func idxWireEncodeStringStringField(enc *msgpack.Encoder, key, value string) error {
	if err := enc.EncodeString(key); err != nil {
		return err
	}
	return enc.EncodeString(value)
}

func idxWireEncodeStringBoolField(enc *msgpack.Encoder, key string, value bool) error {
	if err := enc.EncodeString(key); err != nil {
		return err
	}
	return enc.EncodeBool(value)
}

// --- hfIdxDef ---

func (d hfIdxDef) EncodeMsgpack(enc *msgpack.Encoder) error {
	if err := enc.EncodeMapLen(2); err != nil {
		return err
	}
	if err := idxWireEncodeStringUint16Field(enc, "l", d.LabelToken); err != nil {
		return err
	}
	return idxWireEncodeStringInt64Field(enc, "b", d.BucketSizeMillis)
}

func (d *hfIdxDef) DecodeMsgpack(dec *msgpack.Decoder) error {
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
			d.LabelToken, err = dec.DecodeUint16()
		case "b":
			d.BucketSizeMillis, err = dec.DecodeInt64()
		default:
			err = dec.Skip()
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// --- propIdxDef ---

func (d propIdxDef) EncodeMsgpack(enc *msgpack.Encoder) error {
	if err := enc.EncodeMapLen(2); err != nil {
		return err
	}
	if err := idxWireEncodeStringUint16Field(enc, "l", d.LabelToken); err != nil {
		return err
	}
	return idxWireEncodeStringStringField(enc, "p", d.PropertyKey)
}

func (d *propIdxDef) DecodeMsgpack(dec *msgpack.Decoder) error {
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
			d.LabelToken, err = dec.DecodeUint16()
		case "p":
			d.PropertyKey, err = dec.DecodeString()
		default:
			err = dec.Skip()
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// --- vectorIdxDef ---

func (d vectorIdxDef) EncodeMsgpack(enc *msgpack.Encoder) error {
	fields := 4
	if d.UseBruteForce {
		fields++
	}
	if d.M != 0 {
		fields++
	}
	if d.EfConstruction != 0 {
		fields++
	}
	if d.EfSearch != 0 {
		fields++
	}
	if err := enc.EncodeMapLen(fields); err != nil {
		return err
	}
	if err := idxWireEncodeStringUint16Field(enc, "l", d.LabelToken); err != nil {
		return err
	}
	if err := idxWireEncodeStringStringField(enc, "p", d.PropertyKey); err != nil {
		return err
	}
	if err := idxWireEncodeStringIntField(enc, "d", d.Dims); err != nil {
		return err
	}
	if err := enc.EncodeString("m"); err != nil {
		return err
	}
	if err := enc.EncodeUint8(uint8(d.Metric)); err != nil {
		return err
	}
	if d.UseBruteForce {
		if err := idxWireEncodeStringBoolField(enc, "bf", d.UseBruteForce); err != nil {
			return err
		}
	}
	if d.M != 0 {
		if err := idxWireEncodeStringIntField(enc, "hm", d.M); err != nil {
			return err
		}
	}
	if d.EfConstruction != 0 {
		if err := idxWireEncodeStringIntField(enc, "efc", d.EfConstruction); err != nil {
			return err
		}
	}
	if d.EfSearch != 0 {
		if err := idxWireEncodeStringIntField(enc, "efs", d.EfSearch); err != nil {
			return err
		}
	}
	return nil
}

func (d *vectorIdxDef) DecodeMsgpack(dec *msgpack.Decoder) error {
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
			d.LabelToken, err = dec.DecodeUint16()
		case "p":
			d.PropertyKey, err = dec.DecodeString()
		case "d":
			d.Dims, err = dec.DecodeInt()
		case "m":
			var m uint8
			m, err = dec.DecodeUint8()
			d.Metric = DistanceMetric(m)
		case "bf":
			d.UseBruteForce, err = dec.DecodeBool()
		case "hm":
			d.M, err = dec.DecodeInt()
		case "efc":
			d.EfConstruction, err = dec.DecodeInt()
		case "efs":
			d.EfSearch, err = dec.DecodeInt()
		default:
			err = dec.Skip()
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// --- compositeIdxDef ---

func (d compositeIdxDef) EncodeMsgpack(enc *msgpack.Encoder) error {
	if err := enc.EncodeMapLen(2); err != nil {
		return err
	}
	if err := idxWireEncodeStringUint16Field(enc, "l", d.LabelToken); err != nil {
		return err
	}
	// Keys []string already has msgpack's own dedicated zero-per-element-
	// reflection fast path (encodeStringSliceValue) — enc.Encode dispatches to
	// it directly, no further custom codec needed for this field.
	if err := enc.EncodeString("k"); err != nil {
		return err
	}
	return enc.Encode(d.Keys)
}

func (d *compositeIdxDef) DecodeMsgpack(dec *msgpack.Decoder) error {
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
			d.LabelToken, err = dec.DecodeUint16()
		case "k":
			err = dec.Decode(&d.Keys)
		default:
			err = dec.Skip()
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// --- relPropIdxDef ---

func (d relPropIdxDef) EncodeMsgpack(enc *msgpack.Encoder) error {
	if err := enc.EncodeMapLen(2); err != nil {
		return err
	}
	if err := idxWireEncodeStringUint16Field(enc, "t", d.RelTypeToken); err != nil {
		return err
	}
	return idxWireEncodeStringStringField(enc, "p", d.PropertyKey)
}

func (d *relPropIdxDef) DecodeMsgpack(dec *msgpack.Decoder) error {
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
		case "t":
			d.RelTypeToken, err = dec.DecodeUint16()
		case "p":
			d.PropertyKey, err = dec.DecodeString()
		default:
			err = dec.Skip()
		}
		if err != nil {
			return err
		}
	}
	return nil
}
