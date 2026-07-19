package storeutil

import (
	"encoding/binary"
	"fmt"
	"sync"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/vmihailenco/msgpack/v5"
)

// Hand-written msgpack decoders for the entity wire types, mirroring the
// hand-written encoders in wire_encode.go. DECODE was previously pure reflection
// (msgpack.Unmarshal into the struct); a benchmark showed reflection decode is
// ~1.3-1.9x slower and heavier than these hand decoders (reflect field dispatch +
// incremental reflect.growslice for the property/label slices), and decode is
// ~3x the cost of the already-optimized encode. These custom decoders run on
// every disk-backed read (point reads, history reads, and the loadIndexes open
// scan). They:
//
//   - match each map key WITHOUT allocating a key string, via a pooled scratch
//     buffer + ReadFull + an inline `switch string(kb[:n])` (a conversion the Go
//     compiler proves non-escaping and lowers to a byte compare). The naive
//     DecodeString-per-key allocates a Go string per key only to discard it; the
//     reflection struct decoder avoids that via the unexported decodeStringTemp
//     (a decoder-owned buffer view). The pooled buffer is the public-API
//     equivalent: it is already heap-resident, so ReadFull copies into it with no
//     new allocation (a FRESH stack buffer would instead escape through the
//     io.Reader interface and allocate per read — measured, and worse).
//   - allocate the property/label slices in one exact-size make() instead of
//     reflect's incremental grow steps.
//   - decode the property value `v` via DecodeInterface — the exact-width,
//     non-loose interface path reflection uses for an `any` field by default — so
//     the reconstructed struct is BYTE-IDENTICAL to the old reflection decode and
//     all downstream value reconstruction (reconstructPropertyWireValue) is
//     unchanged.
//
// They are ALSO structurally panic-free on the struct fields (explicit Decode*
// calls return errors, never the reflect "SetString on unaddressable value" panic
// that hostile duplicate-interface-key input triggers under reflection), hardening
// the trust boundary further. SafeUnmarshal keeps guardMsgpackDepth (a deeply-
// nested property `v` still recurses through DecodeInterface) and the recover as
// belt-and-braces. Unknown keys are skipped (forward compatibility — matches
// reflection ignoring unknown struct fields). A msgpack nil top-level value yields
// DecodeMapLen == -1 and leaves the zero struct, matching reflection.
//
// ReadFull cursor-safety: for our decode path d.r == d.s (a *bytes.Reader, no
// bufio wrap — see msgpack ResetReader) so ReadFull stays byte-aligned with the
// Decode* calls, and d.rec is nil (not inside DecodeRaw) so bypassing the record
// buffer is safe. The full wire round-trip + fuzz suites are the correctness gate.

var (
	_ msgpack.CustomDecoder = (*NodeWire)(nil)
	_ msgpack.CustomDecoder = (*RelWire)(nil)
	_ msgpack.CustomDecoder = (*PropertyWire)(nil)
)

// wireKeyScratch bounds the pooled key buffer. Every entity wire key is a short
// fixstr (longest known is 3 bytes: "fnh"/"tnh"/"aid"/"aby"/"sig"); 16 lets any
// unknown short future key still read into the buffer and fall through to the
// default Skip without allocating.
const wireKeyScratch = 16

var wireKeyBufPool = sync.Pool{
	New: func() any { b := make([]byte, wireKeyScratch); return &b },
}

// decodeMapKeyLen reads a msgpack string map-key's bytes into kb (a reused,
// heap-resident buffer) WITHOUT allocating, returning the byte count so the
// caller can `switch string(kb[:n])` at zero cost. kb doubles as scratch for the
// 1-3 byte length header. An over-long key (never a known field, > len(kb)) is
// consumed via a one-off temp and reported as a non-matching empty key so the
// caller Skips its value.
func decodeMapKeyLen(dec *msgpack.Decoder, kb []byte) (int, error) {
	if err := dec.ReadFull(kb[:1]); err != nil {
		return 0, err
	}
	c := kb[0]
	var n int
	switch {
	case c >= 0xa0 && c <= 0xbf: // fixstr
		n = int(c & 0x1f)
	case c == 0xd9: // str8
		if err := dec.ReadFull(kb[:1]); err != nil {
			return 0, err
		}
		n = int(kb[0])
	case c == 0xda: // str16
		if err := dec.ReadFull(kb[:2]); err != nil {
			return 0, err
		}
		n = int(binary.BigEndian.Uint16(kb[:2]))
	default:
		return 0, fmt.Errorf("%w: unexpected map key type 0x%02x", storepkg.ErrCorruptWire, c)
	}
	if n <= len(kb) {
		if err := dec.ReadFull(kb[:n]); err != nil {
			return 0, err
		}
		return n, nil
	}
	// Over-long key (never a known field): consume to keep the cursor aligned and
	// report no match. Allocates only on this cold, unknown-key path.
	tmp := make([]byte, n)
	if err := dec.ReadFull(tmp); err != nil {
		return 0, err
	}
	return 0, nil
}

// DecodeMsgpack decodes a NodeWire from its msgpack map representation.
func (w *NodeWire) DecodeMsgpack(dec *msgpack.Decoder) error {
	nf, err := dec.DecodeMapLen()
	if err != nil {
		return err
	}
	kbp := wireKeyBufPool.Get().(*[]byte)
	kb := *kbp
	defer wireKeyBufPool.Put(kbp)
	for i := 0; i < nf; i++ {
		kn, err := decodeMapKeyLen(dec, kb)
		if err != nil {
			return err
		}
		switch string(kb[:kn]) {
		case "fv":
			w.FormatVersion, err = dec.DecodeUint8()
		case "id":
			w.ID, err = dec.DecodeInt64()
		case "pl":
			w.PrimaryLabel, err = dec.DecodeInt()
		case "el":
			w.ExtraLabels, err = decodeIntSlice(dec)
		case "p":
			w.Properties, err = decodePropertyArray(dec, kb)
		case "v":
			w.Version, err = dec.DecodeInt()
		case "ht":
			w.HasTemporal, err = dec.DecodeBool()
		case "vf":
			w.ValidFrom, err = dec.DecodeInt64()
		case "vt":
			w.ValidTo, err = dec.DecodeInt64()
		case "tf":
			w.TxFrom, err = dec.DecodeInt64()
		case "tt":
			w.TxTo, err = dec.DecodeInt64()
		case "ca":
			w.CreatedAt, err = dec.DecodeInt64()
		case "ua":
			w.UpdatedAt, err = dec.DecodeInt64()
		case "da":
			w.DeletedAt, err = dec.DecodeInt64()
		case "cb":
			w.CreatedBy, err = dec.DecodeString()
		case "ub":
			w.UpdatedBy, err = dec.DecodeString()
		case "be":
			w.BaseEntityID, err = dec.DecodeInt64()
		case "h":
			w.Hash, err = dec.DecodeString()
		case "ph":
			w.PrevHash, err = dec.DecodeString()
		case "aid":
			w.AuthorID, err = dec.DecodeString()
		case "sig":
			w.Signature, err = dec.DecodeBytes()
		case "aby":
			w.AuthorizedBy, err = dec.DecodeString()
		case "al":
			w.AuthorizationLevel, err = dec.DecodeUint8()
		default:
			err = dec.Skip()
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// DecodeMsgpack decodes a RelWire from its msgpack map representation.
func (w *RelWire) DecodeMsgpack(dec *msgpack.Decoder) error {
	nf, err := dec.DecodeMapLen()
	if err != nil {
		return err
	}
	kbp := wireKeyBufPool.Get().(*[]byte)
	kb := *kbp
	defer wireKeyBufPool.Put(kbp)
	for i := 0; i < nf; i++ {
		kn, err := decodeMapKeyLen(dec, kb)
		if err != nil {
			return err
		}
		switch string(kb[:kn]) {
		case "fv":
			w.FormatVersion, err = dec.DecodeUint8()
		case "id":
			w.ID, err = dec.DecodeInt64()
		case "rt":
			w.RelType, err = dec.DecodeInt()
		case "s":
			w.StartID, err = dec.DecodeInt64()
		case "e":
			w.EndID, err = dec.DecodeInt64()
		case "p":
			w.Properties, err = decodePropertyArray(dec, kb)
		case "v":
			w.Version, err = dec.DecodeInt()
		case "ht":
			w.HasTemporal, err = dec.DecodeBool()
		case "vf":
			w.ValidFrom, err = dec.DecodeInt64()
		case "vt":
			w.ValidTo, err = dec.DecodeInt64()
		case "tf":
			w.TxFrom, err = dec.DecodeInt64()
		case "tt":
			w.TxTo, err = dec.DecodeInt64()
		case "ca":
			w.CreatedAt, err = dec.DecodeInt64()
		case "ua":
			w.UpdatedAt, err = dec.DecodeInt64()
		case "da":
			w.DeletedAt, err = dec.DecodeInt64()
		case "cb":
			w.CreatedBy, err = dec.DecodeString()
		case "ub":
			w.UpdatedBy, err = dec.DecodeString()
		case "be":
			w.BaseEntityID, err = dec.DecodeInt64()
		case "h":
			w.Hash, err = dec.DecodeString()
		case "ph":
			w.PrevHash, err = dec.DecodeString()
		case "fnh":
			w.FromNodeHash, err = dec.DecodeString()
		case "tnh":
			w.ToNodeHash, err = dec.DecodeString()
		case "aid":
			w.AuthorID, err = dec.DecodeString()
		case "sig":
			w.Signature, err = dec.DecodeBytes()
		case "aby":
			w.AuthorizedBy, err = dec.DecodeString()
		case "al":
			w.AuthorizationLevel, err = dec.DecodeUint8()
		default:
			err = dec.Skip()
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// DecodeMsgpack decodes a single PropertyWire from its msgpack map. Provided for
// correctness when a PropertyWire is decoded standalone; the NodeWire/RelWire
// property array decodes through decodePropertyWireInto directly (sharing the
// caller's key buffer) to avoid per-element custom-decoder dispatch.
func (p *PropertyWire) DecodeMsgpack(dec *msgpack.Decoder) error {
	kbp := wireKeyBufPool.Get().(*[]byte)
	kb := *kbp
	defer wireKeyBufPool.Put(kbp)
	return decodePropertyWireInto(dec, p, kb)
}

// wireArrayPreallocCap bounds the capacity reserved up front from an untrusted
// msgpack array-length header (lesson 48: allocate proportional to bytes
// DELIVERED, not to an untrusted count field). A declared length is a CLAIM,
// not a fact, until the elements behind it actually decode — pre-sizing a
// make() to the raw claimed count lets a few hostile bytes (a msgpack array
// header claiming e.g. 100M elements) amplify into a huge allocation before a
// single element is read. Mirrors internal/core's importPreallocLimit (4096):
// large enough that every realistic entity (bounded by
// ValidationLimits.MaxLabelsPerNode / MaxPropertiesPerEntity, both far under
// 4096 by default) hits the fast pre-sized path with zero extra allocation,
// small enough that a hostile claim costs only a few KiB up front. Once past
// the cap, append grows incrementally with elements that actually decoded.
const wireArrayPreallocCap = 4096

// decodeIntSlice decodes a msgpack array of ints (ExtraLabels).
func decodeIntSlice(dec *msgpack.Decoder) ([]int, error) {
	n, err := dec.DecodeArrayLen()
	if err != nil {
		return nil, err
	}
	if n < 0 {
		return nil, nil
	}
	out := make([]int, 0, min(n, wireArrayPreallocCap))
	for i := 0; i < n; i++ {
		v, err := dec.DecodeInt()
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}

// decodePropertyArray decodes the "p" array of PropertyWire elements, reusing
// the caller's key buffer for every element's keys.
func decodePropertyArray(dec *msgpack.Decoder, kb []byte) ([]PropertyWire, error) {
	n, err := dec.DecodeArrayLen()
	if err != nil {
		return nil, err
	}
	if n < 0 {
		return nil, nil
	}
	out := make([]PropertyWire, 0, min(n, wireArrayPreallocCap))
	for i := 0; i < n; i++ {
		var pw PropertyWire
		if err := decodePropertyWireInto(dec, &pw, kb); err != nil {
			return nil, err
		}
		out = append(out, pw)
	}
	return out, nil
}

// decodePropertyWireInto decodes one PropertyWire map into p. The value `v` is
// decoded via DecodeInterface — the exact-width, non-loose interface path that
// reflection uses for an `any` field by default — so the reconstructed dynamic
// types are identical to the previous reflection decode.
func decodePropertyWireInto(dec *msgpack.Decoder, p *PropertyWire, kb []byte) error {
	nf, err := dec.DecodeMapLen()
	if err != nil {
		return err
	}
	for i := 0; i < nf; i++ {
		kn, err := decodeMapKeyLen(dec, kb)
		if err != nil {
			return err
		}
		switch string(kb[:kn]) {
		case "k":
			p.Key, err = dec.DecodeString()
		case "kt":
			p.KeyToken, err = dec.DecodeUint16()
		case "v":
			p.Value, err = dec.DecodeInterface()
		case "t":
			p.Type, err = dec.DecodeUint8()
		case "n":
			p.Nil, err = dec.DecodeBool()
		case "ct":
			p.CustomType, err = dec.DecodeString()
		case "cp":
			p.CustomPointer, err = dec.DecodeBool()
		default:
			err = dec.Skip()
		}
		if err != nil {
			return err
		}
	}
	return nil
}
