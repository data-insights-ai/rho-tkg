package sharded

import (
	"sort"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/vmihailenco/msgpack/v5"
)

// Hand-written msgpack encoders/decoders for vectorDefBlob (vector_index.go)
// and slotCatalog (catalog.go) — BACKLOG 15v, same reflection audit as
// BACKLOG 15s/15t. Both are ADMIN/GROWTH-path types (vector index
// definitions persist on index create/drop; the catalog persists once at
// store create and is read at every open), not per-entity-write. Byte-
// identity with the previous pure-reflection encoding is locked by the
// golden vectors in store_wire_golden_test.go (captured BEFORE these methods
// existed) — EXCEPT slotCatalog's SlotShard map field: Go map iteration
// order is randomized, so the reflective baseline for a NON-EMPTY map is
// itself non-deterministic across runs and cannot be golden-pinned by byte
// value. This encoder instead emits SlotShard's entries in ascending-key
// sorted order — deterministic and round-trip-correct (map content, not
// encoding order, is what correctness depends on), verified via decode
// round-trip rather than a fixed hex comparison for that one field.

var (
	_ msgpack.CustomEncoder = vectorDefBlob{}
	_ msgpack.CustomEncoder = slotCatalog{}

	_ msgpack.CustomDecoder = (*vectorDefBlob)(nil)
	_ msgpack.CustomDecoder = (*slotCatalog)(nil)
)

func shardedEncodeStringUint16Field(enc *msgpack.Encoder, key string, value uint16) error {
	if err := enc.EncodeString(key); err != nil {
		return err
	}
	return enc.EncodeUint16(value)
}

func shardedEncodeStringUint8Field(enc *msgpack.Encoder, key string, value uint8) error {
	if err := enc.EncodeString(key); err != nil {
		return err
	}
	return enc.EncodeUint8(value)
}

func shardedEncodeStringIntField(enc *msgpack.Encoder, key string, value int) error {
	if err := enc.EncodeString(key); err != nil {
		return err
	}
	return enc.EncodeInt(int64(value))
}

func shardedEncodeStringStringField(enc *msgpack.Encoder, key, value string) error {
	if err := enc.EncodeString(key); err != nil {
		return err
	}
	return enc.EncodeString(value)
}

// --- vectorDefBlob ---

func (b vectorDefBlob) EncodeMsgpack(enc *msgpack.Encoder) error {
	if err := enc.EncodeMapLen(4); err != nil {
		return err
	}
	if err := shardedEncodeStringUint16Field(enc, "l", b.LabelToken); err != nil {
		return err
	}
	if err := shardedEncodeStringStringField(enc, "p", b.PropertyKey); err != nil {
		return err
	}
	if err := shardedEncodeStringIntField(enc, "d", b.Dims); err != nil {
		return err
	}
	if err := enc.EncodeString("m"); err != nil {
		return err
	}
	return enc.EncodeUint8(uint8(b.Metric))
}

func (b *vectorDefBlob) DecodeMsgpack(dec *msgpack.Decoder) error {
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
		case "p":
			b.PropertyKey, err = dec.DecodeString()
		case "d":
			b.Dims, err = dec.DecodeInt()
		case "m":
			var m uint8
			m, err = dec.DecodeUint8()
			b.Metric = storecontract.DistanceMetric(m)
		default:
			err = dec.Skip()
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// --- slotCatalog ---

func (c slotCatalog) EncodeMsgpack(enc *msgpack.Encoder) error {
	if err := enc.EncodeMapLen(5); err != nil {
		return err
	}
	if err := shardedEncodeStringUint8Field(enc, "v", c.FormatVersion); err != nil {
		return err
	}
	if err := shardedEncodeStringUint8Field(enc, "b", c.BaseSlot); err != nil {
		return err
	}
	if err := shardedEncodeStringUint8Field(enc, "n", c.SlotCount); err != nil {
		return err
	}
	if err := shardedEncodeStringUint8Field(enc, "d", c.Discipline); err != nil {
		return err
	}
	if err := enc.EncodeString("m"); err != nil {
		return err
	}
	if c.SlotShard == nil {
		return enc.EncodeNil()
	}
	keys := make([]int, 0, len(c.SlotShard))
	for k := range c.SlotShard {
		keys = append(keys, int(k))
	}
	sort.Ints(keys)
	if err := enc.EncodeMapLen(len(keys)); err != nil {
		return err
	}
	for _, k := range keys {
		if err := enc.EncodeUint8(uint8(k)); err != nil {
			return err
		}
		if err := enc.EncodeInt(int64(c.SlotShard[uint8(k)])); err != nil {
			return err
		}
	}
	return nil
}

func (c *slotCatalog) DecodeMsgpack(dec *msgpack.Decoder) error {
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
			c.FormatVersion, err = dec.DecodeUint8()
		case "b":
			c.BaseSlot, err = dec.DecodeUint8()
		case "n":
			c.SlotCount, err = dec.DecodeUint8()
		case "d":
			c.Discipline, err = dec.DecodeUint8()
		case "m":
			c.SlotShard, err = decodeSlotShardMap(dec)
		default:
			err = dec.Skip()
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func decodeSlotShardMap(dec *msgpack.Decoder) (map[uint8]int, error) {
	n, err := dec.DecodeMapLen()
	if err != nil {
		return nil, err
	}
	if n < 0 {
		return nil, nil
	}
	if n == 0 {
		return map[uint8]int{}, nil
	}
	m := make(map[uint8]int, n)
	for i := 0; i < n; i++ {
		k, err := dec.DecodeUint8()
		if err != nil {
			return nil, err
		}
		v, err := dec.DecodeInt()
		if err != nil {
			return nil, err
		}
		m[k] = v
	}
	return m, nil
}
