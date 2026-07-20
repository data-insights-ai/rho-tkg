package tiered

import "github.com/vmihailenco/msgpack/v5"

// Hand-written msgpack encoders/decoders for the store-level persistence
// types RegistryFileData, vectorIdxDef, tieredHFIdef, and temporalIndexFileData
// (registry_file.go / vector_index_file.go / temporal_index_file.go —
// BACKLOG 15v, same reflection audit as BACKLOG 15s/15t). These are
// ADMIN/GROWTH-path types (registry file rewrite on token growth, index
// definition file rewrite on index create/drop, shard rotation), not
// per-entity-write, so the win is smaller than 15s's hottest paths, but the
// underlying reflection cost (a generic per-field struct walk with omitempty
// checks) is identical. Byte-identity with the previous pure-reflection
// encoding is locked by the golden vectors in wire_defs_wire_golden_test.go
// (captured BEFORE these methods existed). Map-key emission order matches
// each struct's field DECLARATION order.

var (
	_ msgpack.CustomEncoder = RegistryFileData{}
	_ msgpack.CustomEncoder = vectorIdxDef{}
	_ msgpack.CustomEncoder = tieredHFIdef{}
	_ msgpack.CustomEncoder = temporalIndexFileData{}

	_ msgpack.CustomDecoder = (*RegistryFileData)(nil)
	_ msgpack.CustomDecoder = (*vectorIdxDef)(nil)
	_ msgpack.CustomDecoder = (*tieredHFIdef)(nil)
	_ msgpack.CustomDecoder = (*temporalIndexFileData)(nil)
)

func tieredEncodeStringUint16Field(enc *msgpack.Encoder, key string, value uint16) error {
	if err := enc.EncodeString(key); err != nil {
		return err
	}
	return enc.EncodeUint16(value)
}

func tieredEncodeStringInt64Field(enc *msgpack.Encoder, key string, value int64) error {
	if err := enc.EncodeString(key); err != nil {
		return err
	}
	return enc.EncodeInt64(value)
}

func tieredEncodeStringIntField(enc *msgpack.Encoder, key string, value int) error {
	if err := enc.EncodeString(key); err != nil {
		return err
	}
	return enc.EncodeInt(int64(value))
}

func tieredEncodeStringStringField(enc *msgpack.Encoder, key, value string) error {
	if err := enc.EncodeString(key); err != nil {
		return err
	}
	return enc.EncodeString(value)
}

func tieredEncodeStringBoolField(enc *msgpack.Encoder, key string, value bool) error {
	if err := enc.EncodeString(key); err != nil {
		return err
	}
	return enc.EncodeBool(value)
}

// tieredEncodeUint16Array writes a []uint16 under key without the generic
// per-element reflective slice path (no dedicated msgpack fast path exists
// for []uint16, unlike []string/[]byte). Field has no omitempty tag, so a nil
// slice must encode as msgpack nil (matching reflection's own
// encodeSliceValue.IsNil() check) — an empty-array encoding would silently
// change the wire format for a never-populated field.
func tieredEncodeUint16Array(enc *msgpack.Encoder, key string, vals []uint16) error {
	if err := enc.EncodeString(key); err != nil {
		return err
	}
	if vals == nil {
		return enc.EncodeNil()
	}
	if err := enc.EncodeArrayLen(len(vals)); err != nil {
		return err
	}
	for _, v := range vals {
		if err := enc.EncodeUint16(v); err != nil {
			return err
		}
	}
	return nil
}

func tieredDecodeUint16Array(dec *msgpack.Decoder) ([]uint16, error) {
	n, err := dec.DecodeArrayLen()
	if err != nil {
		return nil, err
	}
	if n <= 0 {
		return nil, nil
	}
	out := make([]uint16, n)
	for i := 0; i < n; i++ {
		v, err := dec.DecodeUint16()
		if err != nil {
			return nil, err
		}
		out[i] = v
	}
	return out, nil
}

// tieredEncodeHFIdefArray writes a []tieredHFIdef under key without
// reflection, mirroring storeutil's encodePropertyArray shape. Field has no
// omitempty tag, so a nil slice must encode as msgpack nil — see
// tieredEncodeUint16Array.
func tieredEncodeHFIdefArray(enc *msgpack.Encoder, key string, defs []tieredHFIdef) error {
	if err := enc.EncodeString(key); err != nil {
		return err
	}
	if defs == nil {
		return enc.EncodeNil()
	}
	if err := enc.EncodeArrayLen(len(defs)); err != nil {
		return err
	}
	for i := range defs {
		if err := defs[i].EncodeMsgpack(enc); err != nil {
			return err
		}
	}
	return nil
}

func tieredDecodeHFIdefArray(dec *msgpack.Decoder) ([]tieredHFIdef, error) {
	n, err := dec.DecodeArrayLen()
	if err != nil {
		return nil, err
	}
	if n <= 0 {
		return nil, nil
	}
	out := make([]tieredHFIdef, n)
	for i := 0; i < n; i++ {
		if err := out[i].DecodeMsgpack(dec); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// --- RegistryFileData ---

func (d RegistryFileData) EncodeMsgpack(enc *msgpack.Encoder) error {
	if err := enc.EncodeMapLen(2); err != nil {
		return err
	}
	if err := enc.EncodeString("labels"); err != nil {
		return err
	}
	if err := enc.Encode(d.Labels); err != nil {
		return err
	}
	if err := enc.EncodeString("reltypes"); err != nil {
		return err
	}
	return enc.Encode(d.RelTypes)
}

func (d *RegistryFileData) DecodeMsgpack(dec *msgpack.Decoder) error {
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
		case "labels":
			err = dec.Decode(&d.Labels)
		case "reltypes":
			err = dec.Decode(&d.RelTypes)
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
	if err := tieredEncodeStringUint16Field(enc, "l", d.LabelToken); err != nil {
		return err
	}
	if err := tieredEncodeStringStringField(enc, "p", d.PropertyKey); err != nil {
		return err
	}
	if err := tieredEncodeStringIntField(enc, "d", d.Dims); err != nil {
		return err
	}
	if err := enc.EncodeString("m"); err != nil {
		return err
	}
	if err := enc.EncodeUint8(uint8(d.Metric)); err != nil {
		return err
	}
	if d.UseBruteForce {
		if err := tieredEncodeStringBoolField(enc, "bf", d.UseBruteForce); err != nil {
			return err
		}
	}
	if d.M != 0 {
		if err := tieredEncodeStringIntField(enc, "hm", d.M); err != nil {
			return err
		}
	}
	if d.EfConstruction != 0 {
		if err := tieredEncodeStringIntField(enc, "efc", d.EfConstruction); err != nil {
			return err
		}
	}
	if d.EfSearch != 0 {
		if err := tieredEncodeStringIntField(enc, "efs", d.EfSearch); err != nil {
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

// --- tieredHFIdef ---

func (d tieredHFIdef) EncodeMsgpack(enc *msgpack.Encoder) error {
	if err := enc.EncodeMapLen(2); err != nil {
		return err
	}
	if err := tieredEncodeStringUint16Field(enc, "l", d.LabelToken); err != nil {
		return err
	}
	return tieredEncodeStringInt64Field(enc, "b", d.BucketSizeMillis)
}

func (d *tieredHFIdef) DecodeMsgpack(dec *msgpack.Decoder) error {
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

// --- temporalIndexFileData ---

func (d temporalIndexFileData) EncodeMsgpack(enc *msgpack.Encoder) error {
	if err := enc.EncodeMapLen(2); err != nil {
		return err
	}
	if err := tieredEncodeUint16Array(enc, "t", d.TemporalLabels); err != nil {
		return err
	}
	return tieredEncodeHFIdefArray(enc, "h", d.HighFrequency)
}

func (d *temporalIndexFileData) DecodeMsgpack(dec *msgpack.Decoder) error {
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
			d.TemporalLabels, err = tieredDecodeUint16Array(dec)
		case "h":
			d.HighFrequency, err = tieredDecodeHFIdefArray(dec)
		default:
			err = dec.Skip()
		}
		if err != nil {
			return err
		}
	}
	return nil
}
