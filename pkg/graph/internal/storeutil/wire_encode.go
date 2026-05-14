package storeutil

import "github.com/vmihailenco/msgpack/v5"

// EncodeMsgpack writes NodeWire with the same map keys as the struct tags in
// wire.go, but avoids reflective omitempty checks on hot write paths.
func (w NodeWire) EncodeMsgpack(enc *msgpack.Encoder) error {
	fields := 3 // id, pl, v
	if len(w.ExtraLabels) > 0 {
		fields++
	}
	if len(w.Properties) > 0 {
		fields++
	}
	if w.HasTemporal {
		fields++
	}
	if w.ValidFrom != 0 {
		fields++
	}
	if w.ValidTo != 0 {
		fields++
	}
	if w.TxFrom != 0 {
		fields++
	}
	if w.TxTo != 0 {
		fields++
	}
	if w.CreatedAt != 0 {
		fields++
	}
	if w.UpdatedAt != 0 {
		fields++
	}
	if w.DeletedAt != 0 {
		fields++
	}
	if w.CreatedBy != "" {
		fields++
	}
	if w.UpdatedBy != "" {
		fields++
	}
	if w.BaseEntityID != 0 {
		fields++
	}
	if w.Hash != "" {
		fields++
	}
	if w.PrevHash != "" {
		fields++
	}
	if w.AuthorID != "" {
		fields++
	}
	if len(w.Signature) > 0 {
		fields++
	}
	if w.AuthorizedBy != "" {
		fields++
	}
	if w.AuthorizationLevel != 0 {
		fields++
	}

	if err := enc.EncodeMapLen(fields); err != nil {
		return err
	}
	if err := encodeStringInt64Field(enc, "id", w.ID); err != nil {
		return err
	}
	if err := encodeStringIntField(enc, "pl", w.PrimaryLabel); err != nil {
		return err
	}
	if len(w.ExtraLabels) > 0 {
		if err := encodeStringAnyField(enc, "el", w.ExtraLabels); err != nil {
			return err
		}
	}
	if len(w.Properties) > 0 {
		if err := encodeStringAnyField(enc, "p", w.Properties); err != nil {
			return err
		}
	}
	if err := encodeStringIntField(enc, "v", w.Version); err != nil {
		return err
	}
	if w.HasTemporal {
		if err := encodeStringBoolField(enc, "ht", w.HasTemporal); err != nil {
			return err
		}
	}
	if w.ValidFrom != 0 {
		if err := encodeStringInt64Field(enc, "vf", w.ValidFrom); err != nil {
			return err
		}
	}
	if w.ValidTo != 0 {
		if err := encodeStringInt64Field(enc, "vt", w.ValidTo); err != nil {
			return err
		}
	}
	if w.TxFrom != 0 {
		if err := encodeStringInt64Field(enc, "tf", w.TxFrom); err != nil {
			return err
		}
	}
	if w.TxTo != 0 {
		if err := encodeStringInt64Field(enc, "tt", w.TxTo); err != nil {
			return err
		}
	}
	if w.CreatedAt != 0 {
		if err := encodeStringInt64Field(enc, "ca", w.CreatedAt); err != nil {
			return err
		}
	}
	if w.UpdatedAt != 0 {
		if err := encodeStringInt64Field(enc, "ua", w.UpdatedAt); err != nil {
			return err
		}
	}
	if w.DeletedAt != 0 {
		if err := encodeStringInt64Field(enc, "da", w.DeletedAt); err != nil {
			return err
		}
	}
	if w.CreatedBy != "" {
		if err := encodeStringStringField(enc, "cb", w.CreatedBy); err != nil {
			return err
		}
	}
	if w.UpdatedBy != "" {
		if err := encodeStringStringField(enc, "ub", w.UpdatedBy); err != nil {
			return err
		}
	}
	if w.BaseEntityID != 0 {
		if err := encodeStringInt64Field(enc, "be", w.BaseEntityID); err != nil {
			return err
		}
	}
	if w.Hash != "" {
		if err := encodeStringStringField(enc, "h", w.Hash); err != nil {
			return err
		}
	}
	if w.PrevHash != "" {
		if err := encodeStringStringField(enc, "ph", w.PrevHash); err != nil {
			return err
		}
	}
	if w.AuthorID != "" {
		if err := encodeStringStringField(enc, "aid", w.AuthorID); err != nil {
			return err
		}
	}
	if len(w.Signature) > 0 {
		if err := encodeStringBytesField(enc, "sig", w.Signature); err != nil {
			return err
		}
	}
	if w.AuthorizedBy != "" {
		if err := encodeStringStringField(enc, "aby", w.AuthorizedBy); err != nil {
			return err
		}
	}
	if w.AuthorizationLevel != 0 {
		return encodeStringUint8Field(enc, "al", w.AuthorizationLevel)
	}
	return nil
}

// EncodeMsgpack writes RelWire with the same map keys as the struct tags in
// wire.go, but avoids reflective omitempty checks on hot write paths.
func (w RelWire) EncodeMsgpack(enc *msgpack.Encoder) error {
	fields := 5 // id, rt, s, e, v
	if len(w.Properties) > 0 {
		fields++
	}
	if w.HasTemporal {
		fields++
	}
	if w.ValidFrom != 0 {
		fields++
	}
	if w.ValidTo != 0 {
		fields++
	}
	if w.TxFrom != 0 {
		fields++
	}
	if w.TxTo != 0 {
		fields++
	}
	if w.CreatedAt != 0 {
		fields++
	}
	if w.UpdatedAt != 0 {
		fields++
	}
	if w.DeletedAt != 0 {
		fields++
	}
	if w.CreatedBy != "" {
		fields++
	}
	if w.UpdatedBy != "" {
		fields++
	}
	if w.BaseEntityID != 0 {
		fields++
	}
	if w.Hash != "" {
		fields++
	}
	if w.PrevHash != "" {
		fields++
	}
	if w.FromNodeHash != "" {
		fields++
	}
	if w.ToNodeHash != "" {
		fields++
	}
	if w.AuthorID != "" {
		fields++
	}
	if len(w.Signature) > 0 {
		fields++
	}
	if w.AuthorizedBy != "" {
		fields++
	}
	if w.AuthorizationLevel != 0 {
		fields++
	}

	if err := enc.EncodeMapLen(fields); err != nil {
		return err
	}
	if err := encodeStringInt64Field(enc, "id", w.ID); err != nil {
		return err
	}
	if err := encodeStringIntField(enc, "rt", w.RelType); err != nil {
		return err
	}
	if err := encodeStringInt64Field(enc, "s", w.StartID); err != nil {
		return err
	}
	if err := encodeStringInt64Field(enc, "e", w.EndID); err != nil {
		return err
	}
	if len(w.Properties) > 0 {
		if err := encodeStringAnyField(enc, "p", w.Properties); err != nil {
			return err
		}
	}
	if err := encodeStringIntField(enc, "v", w.Version); err != nil {
		return err
	}
	if w.HasTemporal {
		if err := encodeStringBoolField(enc, "ht", w.HasTemporal); err != nil {
			return err
		}
	}
	if w.ValidFrom != 0 {
		if err := encodeStringInt64Field(enc, "vf", w.ValidFrom); err != nil {
			return err
		}
	}
	if w.ValidTo != 0 {
		if err := encodeStringInt64Field(enc, "vt", w.ValidTo); err != nil {
			return err
		}
	}
	if w.TxFrom != 0 {
		if err := encodeStringInt64Field(enc, "tf", w.TxFrom); err != nil {
			return err
		}
	}
	if w.TxTo != 0 {
		if err := encodeStringInt64Field(enc, "tt", w.TxTo); err != nil {
			return err
		}
	}
	if w.CreatedAt != 0 {
		if err := encodeStringInt64Field(enc, "ca", w.CreatedAt); err != nil {
			return err
		}
	}
	if w.UpdatedAt != 0 {
		if err := encodeStringInt64Field(enc, "ua", w.UpdatedAt); err != nil {
			return err
		}
	}
	if w.DeletedAt != 0 {
		if err := encodeStringInt64Field(enc, "da", w.DeletedAt); err != nil {
			return err
		}
	}
	if w.CreatedBy != "" {
		if err := encodeStringStringField(enc, "cb", w.CreatedBy); err != nil {
			return err
		}
	}
	if w.UpdatedBy != "" {
		if err := encodeStringStringField(enc, "ub", w.UpdatedBy); err != nil {
			return err
		}
	}
	if w.BaseEntityID != 0 {
		if err := encodeStringInt64Field(enc, "be", w.BaseEntityID); err != nil {
			return err
		}
	}
	if w.Hash != "" {
		if err := encodeStringStringField(enc, "h", w.Hash); err != nil {
			return err
		}
	}
	if w.PrevHash != "" {
		if err := encodeStringStringField(enc, "ph", w.PrevHash); err != nil {
			return err
		}
	}
	if w.FromNodeHash != "" {
		if err := encodeStringStringField(enc, "fnh", w.FromNodeHash); err != nil {
			return err
		}
	}
	if w.ToNodeHash != "" {
		if err := encodeStringStringField(enc, "tnh", w.ToNodeHash); err != nil {
			return err
		}
	}
	if w.AuthorID != "" {
		if err := encodeStringStringField(enc, "aid", w.AuthorID); err != nil {
			return err
		}
	}
	if len(w.Signature) > 0 {
		if err := encodeStringBytesField(enc, "sig", w.Signature); err != nil {
			return err
		}
	}
	if w.AuthorizedBy != "" {
		if err := encodeStringStringField(enc, "aby", w.AuthorizedBy); err != nil {
			return err
		}
	}
	if w.AuthorizationLevel != 0 {
		return encodeStringUint8Field(enc, "al", w.AuthorizationLevel)
	}
	return nil
}

// EncodeMsgpack writes PropertyWire with the same map keys as the struct tags
// in wire.go while avoiding per-property reflective omitempty checks.
func (p PropertyWire) EncodeMsgpack(enc *msgpack.Encoder) error {
	fields := 3 // k, v, t
	if p.Nil {
		fields++
	}
	if p.CustomType != "" {
		fields++
	}
	if p.CustomPointer {
		fields++
	}
	if err := enc.EncodeMapLen(fields); err != nil {
		return err
	}
	if err := encodeStringStringField(enc, "k", p.Key); err != nil {
		return err
	}
	if err := enc.EncodeString("v"); err != nil {
		return err
	}
	if err := enc.Encode(p.Value); err != nil {
		return err
	}
	if err := encodeStringUint8Field(enc, "t", p.Type); err != nil {
		return err
	}
	if p.Nil {
		if err := encodeStringBoolField(enc, "n", p.Nil); err != nil {
			return err
		}
	}
	if p.CustomType != "" {
		if err := encodeStringStringField(enc, "ct", p.CustomType); err != nil {
			return err
		}
	}
	if p.CustomPointer {
		return encodeStringBoolField(enc, "cp", p.CustomPointer)
	}
	return nil
}

func encodeStringAnyField(enc *msgpack.Encoder, key string, value any) error {
	if err := enc.EncodeString(key); err != nil {
		return err
	}
	return enc.Encode(value)
}

func encodeStringBoolField(enc *msgpack.Encoder, key string, value bool) error {
	if err := enc.EncodeString(key); err != nil {
		return err
	}
	return enc.EncodeBool(value)
}

func encodeStringBytesField(enc *msgpack.Encoder, key string, value []byte) error {
	if err := enc.EncodeString(key); err != nil {
		return err
	}
	return enc.EncodeBytes(value)
}

func encodeStringIntField(enc *msgpack.Encoder, key string, value int) error {
	if err := enc.EncodeString(key); err != nil {
		return err
	}
	return enc.EncodeInt(int64(value))
}

func encodeStringInt64Field(enc *msgpack.Encoder, key string, value int64) error {
	if err := enc.EncodeString(key); err != nil {
		return err
	}
	return enc.EncodeInt64(value)
}

func encodeStringStringField(enc *msgpack.Encoder, key, value string) error {
	if err := enc.EncodeString(key); err != nil {
		return err
	}
	return enc.EncodeString(value)
}

func encodeStringUint8Field(enc *msgpack.Encoder, key string, value uint8) error {
	if err := enc.EncodeString(key); err != nil {
		return err
	}
	return enc.EncodeUint8(value)
}
