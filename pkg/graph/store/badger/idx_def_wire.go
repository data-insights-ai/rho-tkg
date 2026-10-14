package badger

import "github.com/vmihailenco/msgpack/v5"

// Hand-written msgpack codecs for the index-definition persistence types
// (hfIdxDef, propIdxDef, vectorIdxDef, compositeIdxDef, relPropIdxDef; BACKLOG
// 15v), admin-path types encoded without per-field reflection. Byte-identity with
// the reflection encoding is locked by idx_def_wire_golden_test.go; map keys follow
// struct field declaration order.

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
	fields := 2
	if d.RangeCounts {
		fields++
	}
	if d.Elements {
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
	if d.RangeCounts {
		if err := idxWireEncodeStringBoolField(enc, "rc", true); err != nil {
			return err
		}
	}
	if d.Elements {
		return idxWireEncodeStringBoolField(enc, "el", true)
	}
	return nil
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
		case "rc":
			d.RangeCounts, err = dec.DecodeBool()
		case "el":
			d.Elements, err = dec.DecodeBool()
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
	// Keys []string has msgpack's own fast path; enc.Encode dispatches to it.
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
	fields := 2
	if d.RangeCounts {
		fields++
	}
	if d.Elements {
		fields++
	}
	if err := enc.EncodeMapLen(fields); err != nil {
		return err
	}
	if err := idxWireEncodeStringUint16Field(enc, "t", d.RelTypeToken); err != nil {
		return err
	}
	if err := idxWireEncodeStringStringField(enc, "p", d.PropertyKey); err != nil {
		return err
	}
	if d.RangeCounts {
		if err := idxWireEncodeStringBoolField(enc, "rc", true); err != nil {
			return err
		}
	}
	if d.Elements {
		return idxWireEncodeStringBoolField(enc, "el", true)
	}
	return nil
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
		case "rc":
			d.RangeCounts, err = dec.DecodeBool()
		case "el":
			d.Elements, err = dec.DecodeBool()
		default:
			err = dec.Skip()
		}
		if err != nil {
			return err
		}
	}
	return nil
}
