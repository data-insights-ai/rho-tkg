package storeutil

import "encoding/binary"

// scanWireTemporalMeta is the reflection-free fast path behind
// DecodeWireTemporalMeta: a SINGLE non-recursive pass over the wire bytes that
// captures the selection fields (fv, v, ht, vf, vt, tf, tt, ca, ua, da) from
// the TOP-LEVEL map and skips every other value with the same
// explicit-stack/cursor-alignment machinery guardMsgpackDepth uses. It exists
// because the SafeUnmarshal partial decode walks the buffer twice (depth guard
// + reflection decode); on a deep-history valid-time resolution that double
// walk is the dominant per-version cost.
//
// FAIL-OPEN CONTRACT: ok=false on ANY structural surprise — truncation,
// over-deep nesting, an invalid type byte, a non-map top level, an unexpected
// value encoding for a captured field. The caller then falls back to
// SafeUnmarshal, which remains the audited authority for both the decode AND
// the error classification of malformed input. The scanner therefore can only
// ever be a same-answer accelerator, never a divergent decoder — locked by
// TestScanWireTemporalMeta_MatchesSafeUnmarshal's randomized + adversarial
// equivalence battery.
//
// Duplicate keys: last one wins, matching the reflect decoder's behavior on
// this struct (no interface-typed fields, so no decoder panic class here).
func scanWireTemporalMeta(data []byte) (wireTemporalMetaPartial, bool) {
	var out wireTemporalMetaPartial
	pos := 0

	// readByte/-N helpers return ok=false on truncation; every read is bounds
	// checked so the scanner is panic-free by construction.
	readByte := func() (byte, bool) {
		if pos >= len(data) {
			return 0, false
		}
		b := data[pos]
		pos++
		return b, true
	}

	// readInt decodes one msgpack integer value (any width, signed or
	// unsigned, incl. fixint) or reports ok=false without moving past
	// unknown encodings.
	readInt := func() (int64, bool) {
		c, ok := readByte()
		if !ok {
			return 0, false
		}
		switch {
		case c <= 0x7f: // positive fixint
			return int64(c), true
		case c >= 0xe0: // negative fixint
			return int64(int8(c)), true
		}
		need := 0
		switch c {
		case 0xcc, 0xd0:
			need = 1
		case 0xcd, 0xd1:
			need = 2
		case 0xce, 0xd2:
			need = 4
		case 0xcf, 0xd3:
			need = 8
		default:
			return 0, false
		}
		if pos+need > len(data) {
			return 0, false
		}
		var v int64
		switch c {
		case 0xcc:
			v = int64(data[pos])
		case 0xd0:
			v = int64(int8(data[pos]))
		case 0xcd:
			v = int64(binary.BigEndian.Uint16(data[pos:]))
		case 0xd1:
			v = int64(int16(binary.BigEndian.Uint16(data[pos:]))) // #nosec G115 — deliberate signed reinterpretation
		case 0xce:
			v = int64(binary.BigEndian.Uint32(data[pos:]))
		case 0xd2:
			v = int64(int32(binary.BigEndian.Uint32(data[pos:]))) // #nosec G115 — deliberate signed reinterpretation
		case 0xcf:
			u := binary.BigEndian.Uint64(data[pos:])
			if u > 1<<63-1 {
				return 0, false // would overflow int64 — let the real decoder classify
			}
			v = int64(u)
		case 0xd3:
			v = int64(binary.BigEndian.Uint64(data[pos:])) // #nosec G115 — round-trip of an int64
		}
		pos += need
		return v, true
	}

	// skipValue advances the cursor past ONE msgpack value of any shape using
	// the same pending-count stack discipline as guardMsgpackDepth (bounded
	// depth, no recursion).
	skipValue := func() bool {
		var pending [maxWireDecodeDepth + 1]int
		pending[0] = 1
		sp := 1
		for sp > 0 {
			if pending[sp-1] == 0 {
				sp--
				continue
			}
			pending[sp-1]--
			c, ok := readByte()
			if !ok {
				return false
			}
			push := func(n int) bool {
				if sp >= len(pending) {
					return false
				}
				pending[sp] = n
				sp++
				return true
			}
			switch {
			case c <= 0x7f, c >= 0xe0: // fixints
			case c >= 0xa0 && c <= 0xbf: // fixstr
				pos += int(c & 0x1f)
			case c >= 0x90 && c <= 0x9f: // fixarray
				if !push(int(c & 0x0f)) {
					return false
				}
			case c >= 0x80 && c <= 0x8f: // fixmap
				if !push(2 * int(c&0x0f)) {
					return false
				}
			case c == 0xc0, c == 0xc2, c == 0xc3: // nil / false / true
			case c == 0xcc, c == 0xd0:
				pos++
			case c == 0xcd, c == 0xd1:
				pos += 2
			case c == 0xce, c == 0xd2, c == 0xca:
				pos += 4
			case c == 0xcf, c == 0xd3, c == 0xcb:
				pos += 8
			case c == 0xd9, c == 0xc4: // str8 / bin8
				if pos >= len(data) {
					return false
				}
				pos += 1 + int(data[pos])
			case c == 0xda, c == 0xc5: // str16 / bin16
				if pos+2 > len(data) {
					return false
				}
				pos += 2 + int(binary.BigEndian.Uint16(data[pos:]))
			case c == 0xdb, c == 0xc6: // str32 / bin32
				if pos+4 > len(data) {
					return false
				}
				pos += 4 + int(binary.BigEndian.Uint32(data[pos:]))
			case c == 0xdc: // array16
				if pos+2 > len(data) {
					return false
				}
				n := int(binary.BigEndian.Uint16(data[pos:]))
				pos += 2
				if !push(n) {
					return false
				}
			case c == 0xdd: // array32
				if pos+4 > len(data) {
					return false
				}
				n := int(binary.BigEndian.Uint32(data[pos:]))
				pos += 4
				if !push(n) {
					return false
				}
			case c == 0xde: // map16
				if pos+2 > len(data) {
					return false
				}
				n := int(binary.BigEndian.Uint16(data[pos:]))
				pos += 2
				if !push(2 * n) {
					return false
				}
			case c == 0xdf: // map32
				if pos+4 > len(data) {
					return false
				}
				n := int(binary.BigEndian.Uint32(data[pos:]))
				pos += 4
				if !push(2 * n) {
					return false
				}
			case c == 0xd4:
				pos += 1 + 1
			case c == 0xd5:
				pos += 1 + 2
			case c == 0xd6:
				pos += 1 + 4
			case c == 0xd7:
				pos += 1 + 8
			case c == 0xd8:
				pos += 1 + 16
			case c == 0xc7: // ext8
				if pos >= len(data) {
					return false
				}
				pos += 1 + 1 + int(data[pos])
			case c == 0xc8: // ext16
				if pos+2 > len(data) {
					return false
				}
				pos += 2 + 1 + int(binary.BigEndian.Uint16(data[pos:]))
			case c == 0xc9: // ext32
				if pos+4 > len(data) {
					return false
				}
				pos += 4 + 1 + int(binary.BigEndian.Uint32(data[pos:]))
			default: // 0xc1
				return false
			}
			if pos > len(data) {
				return false
			}
		}
		return true
	}

	// Top level MUST be a map (every NodeWire/RelWire row is).
	c, ok := readByte()
	if !ok {
		return out, false
	}
	var entries int
	switch {
	case c >= 0x80 && c <= 0x8f:
		entries = int(c & 0x0f)
	case c == 0xde:
		if pos+2 > len(data) {
			return out, false
		}
		entries = int(binary.BigEndian.Uint16(data[pos:]))
		pos += 2
	case c == 0xdf:
		if pos+4 > len(data) {
			return out, false
		}
		entries = int(binary.BigEndian.Uint32(data[pos:]))
		pos += 4
	default:
		return out, false
	}

	for i := 0; i < entries; i++ {
		// Key: our encoders emit short fixstr keys; tolerate str8 too.
		kc, ok := readByte()
		if !ok {
			return out, false
		}
		var klen int
		switch {
		case kc >= 0xa0 && kc <= 0xbf:
			klen = int(kc & 0x1f)
		case kc == 0xd9:
			lb, ok := readByte()
			if !ok {
				return out, false
			}
			klen = int(lb)
		default:
			return out, false // non-string key — not our wire; fall back
		}
		if pos+klen > len(data) {
			return out, false
		}
		key := data[pos : pos+klen]
		pos += klen

		captureInt := func(dst *int64) bool {
			v, ok := readInt()
			if !ok {
				return false
			}
			*dst = v
			return true
		}
		handled := true
		switch string(key) {
		case "fv":
			v, ok := readInt()
			if !ok || v < 0 || v > 255 {
				return out, false
			}
			out.FormatVersion = uint8(v)
		case "v":
			v, ok := readInt()
			if !ok {
				return out, false
			}
			if v < int64(minInt) || v > int64(maxInt) {
				return out, false
			}
			out.Version = int(v)
		case "ht":
			b, ok := readByte()
			if !ok {
				return out, false
			}
			switch b {
			case 0xc3:
				out.HasTemporal = true
			case 0xc2:
				out.HasTemporal = false
			default:
				return out, false
			}
		case "vf":
			if !captureInt(&out.ValidFrom) {
				return out, false
			}
		case "vt":
			if !captureInt(&out.ValidTo) {
				return out, false
			}
		case "tf":
			if !captureInt(&out.TxFrom) {
				return out, false
			}
		case "tt":
			if !captureInt(&out.TxTo) {
				return out, false
			}
		case "ca":
			if !captureInt(&out.CreatedAt) {
				return out, false
			}
		case "ua":
			if !captureInt(&out.UpdatedAt) {
				return out, false
			}
		case "da":
			if !captureInt(&out.DeletedAt) {
				return out, false
			}
		default:
			handled = false
		}
		if !handled {
			if !skipValue() {
				return out, false
			}
		}
	}
	// Trailing garbage after the top-level map means this is not a clean row —
	// let the real decoder classify it.
	if pos != len(data) {
		return out, false
	}
	return out, true
}

const (
	maxInt = int(^uint(0) >> 1)
	minInt = -maxInt - 1
)
