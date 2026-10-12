package index

import (
	"encoding/binary"
	"errors"
	"math"
)

// Binary form of a PropertyStatsAccumulator, for a store that persists its
// planner statistics across a clean restart (badger's open image, backlog 54).
// It carries the whole state — HyperLogLog registers, ordered family, typed
// min/max, dirty flag — so a decoded accumulator answers exactly as the one
// encoded. Decoding is fail-closed: any malformed byte is an error.

const statsCodecVersion byte = 1

// errStatsCodec reports an accumulator encoding this binary cannot read.
var errStatsCodec = errors.New("index: unreadable property statistics encoding")

// Value kinds for min/max: the exact Go type is kept, as Observe stored it.
const (
	statsKindNil byte = iota
	statsKindInt
	statsKindInt8
	statsKindInt16
	statsKindInt32
	statsKindInt64
	statsKindUint
	statsKindUint8
	statsKindUint16
	statsKindUint32
	statsKindUint64
	statsKindFloat32
	statsKindFloat64
	statsKindString
)

// AppendBinary appends the accumulator's encoding to b. Caller holds the
// accumulator's lock, as for every other method.
func (a *PropertyStatsAccumulator) AppendBinary(b []byte) []byte {
	b = append(b, statsCodecVersion)
	b = appendStatsString(b, a.family)
	dirty := byte(0)
	if a.dirty {
		dirty = 1
	}
	b = append(b, dirty)
	b = appendStatsValue(b, a.min)
	b = appendStatsValue(b, a.max)
	h := a.sketch
	b = append(b, h.precision)
	if h.dense != nil {
		b = append(b, 1)
		return append(b, h.dense...)
	}
	b = append(b, 0)
	b = binary.AppendUvarint(b, uint64(len(h.sparse)))
	for idx, rho := range h.sparse {
		b = binary.AppendUvarint(b, uint64(idx))
		b = append(b, rho)
	}
	return b
}

// DecodePropertyStatsAccumulator decodes one AppendBinary encoding from the
// front of b and returns it with the number of bytes it used.
func DecodePropertyStatsAccumulator(b []byte) (*PropertyStatsAccumulator, int, error) {
	r := statsReader{b: b}
	if r.byte() != statsCodecVersion {
		return nil, 0, errStatsCodec
	}
	a := &PropertyStatsAccumulator{family: r.str()}
	switch r.byte() {
	case 0:
	case 1:
		a.dirty = true
	default:
		r.fail()
	}
	a.min, a.max = r.value(), r.value()
	if a.family != "" && a.family != "n" && a.family != "s" {
		r.fail()
	}
	if scalarOrderFamily(a.min) != a.family || scalarOrderFamily(a.max) != a.family {
		r.fail() // min/max are both of the family, or both nil with no family
	}
	h, err := NewHyperLogLog(r.byte())
	if err != nil {
		return nil, 0, errStatsCodec
	}
	maxRho := 64 - h.precision + 1
	switch r.byte() {
	case 1:
		dense := r.bytes(int(h.m))
		if r.err == nil {
			h.dense = append([]uint8(nil), dense...)
			h.sparse = nil
			for _, rho := range h.dense {
				if rho > maxRho {
					r.fail()
				}
			}
		}
	case 0:
		n := r.uvarint()
		if n > uint64(h.m) {
			r.fail()
		}
		for i := uint64(0); i < n && r.err == nil; i++ {
			idx, rho := r.uvarint(), r.byte()
			if idx >= uint64(h.m) || rho == 0 || rho > maxRho {
				r.fail()
				break
			}
			h.setRegister(uint32(idx), rho) // #nosec G115 -- idx < h.m <= 2^18
		}
	default:
		r.fail()
	}
	if r.err != nil {
		return nil, 0, r.err
	}
	a.sketch = h
	return a, len(b) - len(r.b), nil
}

func appendStatsString(b []byte, s string) []byte {
	b = binary.AppendUvarint(b, uint64(len(s)))
	return append(b, s...)
}

func appendStatsValue(b []byte, v any) []byte {
	switch x := v.(type) {
	case nil:
		return append(b, statsKindNil)
	case int:
		return binary.AppendVarint(append(b, statsKindInt), int64(x))
	case int8:
		return binary.AppendVarint(append(b, statsKindInt8), int64(x))
	case int16:
		return binary.AppendVarint(append(b, statsKindInt16), int64(x))
	case int32:
		return binary.AppendVarint(append(b, statsKindInt32), int64(x))
	case int64:
		return binary.AppendVarint(append(b, statsKindInt64), x)
	case uint:
		return binary.AppendUvarint(append(b, statsKindUint), uint64(x)) // #nosec G115 -- uint widens
	case uint8:
		return binary.AppendUvarint(append(b, statsKindUint8), uint64(x))
	case uint16:
		return binary.AppendUvarint(append(b, statsKindUint16), uint64(x))
	case uint32:
		return binary.AppendUvarint(append(b, statsKindUint32), uint64(x))
	case uint64:
		return binary.AppendUvarint(append(b, statsKindUint64), x)
	case float32:
		return binary.BigEndian.AppendUint32(append(b, statsKindFloat32), math.Float32bits(x))
	case float64:
		return binary.BigEndian.AppendUint64(append(b, statsKindFloat64), math.Float64bits(x))
	case string:
		return appendStatsString(append(b, statsKindString), x)
	default:
		// Observe stores only scalarOrderFamily values; anything else is a
		// bug upstream. Encode it as an unreadable kind so a decode fails
		// (the store then rebuilds) rather than inventing a value.
		return append(b, 0xff)
	}
}

type statsReader struct {
	b   []byte
	err error
}

func (r *statsReader) fail() {
	if r.err == nil {
		r.err = errStatsCodec
	}
}

func (r *statsReader) byte() byte {
	if r.err != nil || len(r.b) == 0 {
		r.fail()
		return 0
	}
	v := r.b[0]
	r.b = r.b[1:]
	return v
}

func (r *statsReader) bytes(n int) []byte {
	if r.err != nil || n < 0 || n > len(r.b) {
		r.fail()
		return nil
	}
	v := r.b[:n]
	r.b = r.b[n:]
	return v
}

func (r *statsReader) uvarint() uint64 {
	if r.err != nil {
		return 0
	}
	v, n := binary.Uvarint(r.b)
	if n <= 0 {
		r.fail()
		return 0
	}
	r.b = r.b[n:]
	return v
}

func (r *statsReader) varint() int64 {
	if r.err != nil {
		return 0
	}
	v, n := binary.Varint(r.b)
	if n <= 0 {
		r.fail()
		return 0
	}
	r.b = r.b[n:]
	return v
}

func (r *statsReader) str() string {
	n := r.uvarint()
	if n > uint64(len(r.b)) {
		r.fail()
		return ""
	}
	return string(r.bytes(int(n))) // #nosec G115 -- bounded by len(r.b)
}

// value decodes one appendStatsValue encoding. An integer that does not fit
// its kind is malformed.
func (r *statsReader) value() any {
	switch k := r.byte(); k {
	case statsKindNil:
		return nil
	case statsKindInt, statsKindInt8, statsKindInt16, statsKindInt32, statsKindInt64:
		v := r.varint()
		switch k {
		case statsKindInt:
			if int64(int(v)) != v { // #nosec G115 -- detects truncation
				r.fail()
			}
			return int(v) // #nosec G115 -- round trip checked
		case statsKindInt8:
			if v < math.MinInt8 || v > math.MaxInt8 {
				r.fail()
			}
			return int8(v) // #nosec G115 -- range checked
		case statsKindInt16:
			if v < math.MinInt16 || v > math.MaxInt16 {
				r.fail()
			}
			return int16(v) // #nosec G115 -- range checked
		case statsKindInt32:
			if v < math.MinInt32 || v > math.MaxInt32 {
				r.fail()
			}
			return int32(v) // #nosec G115 -- range checked
		default:
			return v
		}
	case statsKindUint, statsKindUint8, statsKindUint16, statsKindUint32, statsKindUint64:
		v := r.uvarint()
		switch k {
		case statsKindUint:
			if uint64(uint(v)) != v { // #nosec G115 -- detects truncation
				r.fail()
			}
			return uint(v) // #nosec G115 -- round trip checked
		case statsKindUint8:
			if v > math.MaxUint8 {
				r.fail()
			}
			return uint8(v) // #nosec G115 -- range checked
		case statsKindUint16:
			if v > math.MaxUint16 {
				r.fail()
			}
			return uint16(v) // #nosec G115 -- range checked
		case statsKindUint32:
			if v > math.MaxUint32 {
				r.fail()
			}
			return uint32(v) // #nosec G115 -- range checked
		default:
			return v
		}
	case statsKindFloat32:
		raw := r.bytes(4)
		if r.err != nil {
			return nil
		}
		return math.Float32frombits(binary.BigEndian.Uint32(raw))
	case statsKindFloat64:
		raw := r.bytes(8)
		if r.err != nil {
			return nil
		}
		return math.Float64frombits(binary.BigEndian.Uint64(raw))
	case statsKindString:
		return r.str()
	default:
		r.fail()
		return nil
	}
}
