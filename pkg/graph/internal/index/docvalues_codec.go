package index

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

// Column serialisation (CP1).
//
// WHY THIS NEEDS NO FORMAT CONTRACT. A persisted column is a REBUILD ACCELERATOR,
// never a read authority — the same rule Config.TemporalIndexOnDisk already follows
// ("maintained alongside, never instead of, the RAM structure"). So every failure
// mode here, without exception, is "discard and rebuild": a blob written by a newer
// binary, a truncated row, a corrupt dictionary, a stamp that no longer matches the
// label's epoch. None of them can produce a wrong answer, only a slower one.
//
// That is the entire reason this carries its own small version byte instead of
// riding the store's wire-format version: raising CurrentWireFormatVersion makes an
// OLDER binary refuse to open the directory at all, which would be an absurd price
// for a cache. An unrecognised blob version here is simply not decoded.
//
// If persisted columns ever became authoritative, all of that inverts and this would
// need a real format contract, a compat marker and a migration path. It is not that,
// deliberately.

// columnBlobVersion is the encoding version of ONE persisted column blob. It is
// local to this cache and unrelated to the store's wire-format version.
const columnBlobVersion uint8 = 1

var (
	// ErrColumnBlobUnreadable means a persisted column could not be decoded and the
	// caller must rebuild. It is never surfaced to a query.
	ErrColumnBlobUnreadable = errors.New("index: persisted column unreadable")
)

// maxColumnBlobEntries bounds what a decode will allocate from a length prefix, so
// a corrupt count cannot turn into an enormous allocation before the checksum-free
// truncation check catches it.
const maxColumnBlobEntries = MaxDocValuesNodes

type blobWriter struct{ b []byte }

func (w *blobWriter) u8(v uint8)   { w.b = append(w.b, v) }
func (w *blobWriter) u32(v uint32) { w.b = binary.LittleEndian.AppendUint32(w.b, v) }
func (w *blobWriter) u64(v uint64) { w.b = binary.LittleEndian.AppendUint64(w.b, v) }
func (w *blobWriter) i64(v int64)  { w.u64(uint64(v)) }
func (w *blobWriter) str(s string) { w.u32(uint32(len(s))); w.b = append(w.b, s...) }

func (w *blobWriter) i64s(xs []int64) {
	w.u32(uint32(len(xs)))
	for _, v := range xs {
		w.i64(v)
	}
}

func (w *blobWriter) f64s(xs []float64) {
	w.u32(uint32(len(xs)))
	for _, v := range xs {
		w.u64(math.Float64bits(v)) // bit-exact: NaN and -0.0 must survive
	}
}

func (w *blobWriter) bits(b bitset) {
	w.u32(uint32(len(b)))
	for _, word := range b {
		w.u64(word)
	}
}

type blobReader struct {
	b   []byte
	pos int
	err error
}

func (r *blobReader) fail() { r.err = ErrColumnBlobUnreadable }

func (r *blobReader) take(n int) []byte {
	if r.err != nil {
		return nil
	}
	if n < 0 || r.pos+n > len(r.b) {
		r.fail()
		return nil
	}
	out := r.b[r.pos : r.pos+n]
	r.pos += n
	return out
}

func (r *blobReader) u8() uint8 {
	p := r.take(1)
	if p == nil {
		return 0
	}
	return p[0]
}

func (r *blobReader) u32() uint32 {
	p := r.take(4)
	if p == nil {
		return 0
	}
	return binary.LittleEndian.Uint32(p)
}

func (r *blobReader) u64() uint64 {
	p := r.take(8)
	if p == nil {
		return 0
	}
	return binary.LittleEndian.Uint64(p)
}

func (r *blobReader) i64() int64 { return int64(r.u64()) }

// count reads a length prefix, refusing one larger than the blob could possibly
// contain. Without this a corrupt prefix allocates before truncation is noticed.
func (r *blobReader) count(bytesPerEntry int) int {
	n := int(r.u32())
	if r.err != nil {
		return 0
	}
	if n < 0 || n > maxColumnBlobEntries || n*bytesPerEntry > len(r.b)-r.pos {
		r.fail()
		return 0
	}
	return n
}

func (r *blobReader) str() string {
	n := r.count(1)
	p := r.take(n)
	if p == nil {
		return ""
	}
	return string(p)
}

func (r *blobReader) i64s() []int64 {
	n := r.count(8)
	if r.err != nil || n == 0 {
		return nil
	}
	out := make([]int64, n)
	for i := range out {
		out[i] = r.i64()
	}
	return out
}

func (r *blobReader) f64s() []float64 {
	n := r.count(8)
	if r.err != nil || n == 0 {
		return nil
	}
	out := make([]float64, n)
	for i := range out {
		out[i] = math.Float64frombits(r.u64())
	}
	return out
}

func (r *blobReader) bits() bitset {
	n := r.count(8)
	if r.err != nil || n == 0 {
		return nil
	}
	out := make(bitset, n)
	for i := range out {
		out[i] = r.u64()
	}
	return out
}

// EncodeColumns serialises a built snapshot. Endianness is fixed little-endian and
// floats are written bit-exact (math.Float64bits), so a decoded column is identical
// to the built one rather than merely equal-ish — NaN and -0.0 included.
//
// The ordinal vector is written as raw int64 rather than the generic T: every
// EntityID is snowflake-backed, and the caller reconstructs the concrete type.
func EncodeColumns[T EntityID](l *DocValues[T]) []byte {
	w := &blobWriter{b: make([]byte, 0, 1024)}
	w.u8(columnBlobVersion)
	w.u64(l.epoch)

	w.u32(uint32(len(l.ids)))
	for _, id := range l.ids {
		w.i64(int64(id.SnowflakeID()))
	}

	if l.hasTemporal {
		w.u8(1)
		w.i64s(l.validFrom)
		w.i64s(l.validTo)
	} else {
		w.u8(0)
	}

	// Column order is not stable across map iterations, and it does not need to be:
	// decode rebuilds a map. Nothing downstream depends on blob bytes matching
	// between two encodes of the same snapshot.
	w.u32(uint32(len(l.cols)))
	for key, c := range l.cols {
		w.str(key)
		w.u8(uint8(c.typ))
		w.u32(uint32(c.n))
		w.bits(c.present)
		switch c.typ {
		case ColNumeric:
			w.i64s(c.ints)
			w.f64s(c.flts)
			w.bits(c.isFloat)
		case ColString:
			w.u32(uint32(len(c.dict)))
			for _, s := range c.dict {
				w.str(s)
			}
			w.u32(uint32(len(c.codes)))
			for _, code := range c.codes {
				w.u32(code)
			}
		}
	}
	return w.b
}

// DecodeColumns rebuilds a snapshot from EncodeColumns output. mk converts a raw
// snowflake back into the caller's ID type.
//
// Returns ErrColumnBlobUnreadable for anything it cannot decode confidently —
// unknown version, truncation, an impossible length, a code pointing outside the
// dictionary. The caller's only correct response is to rebuild, which is why no
// error here is worth distinguishing.
func DecodeColumns[T EntityID](blob []byte, mk func(int64) T) (*DocValues[T], error) {
	r := &blobReader{b: blob}
	if v := r.u8(); r.err != nil || v != columnBlobVersion {
		return nil, ErrColumnBlobUnreadable
	}
	out := &DocValues[T]{epoch: r.u64()}

	n := r.count(8)
	if r.err != nil {
		return nil, ErrColumnBlobUnreadable
	}
	out.ids = make([]T, n)
	for i := range out.ids {
		out.ids[i] = mk(r.i64())
	}

	if r.u8() == 1 {
		out.hasTemporal = true
		out.validFrom = r.i64s()
		out.validTo = r.i64s()
		if r.err != nil || len(out.validFrom) != n || len(out.validTo) != n {
			return nil, ErrColumnBlobUnreadable
		}
	}

	ncols := r.count(1)
	if r.err != nil {
		return nil, ErrColumnBlobUnreadable
	}
	out.cols = make(map[string]*docColumn, ncols)
	for i := 0; i < ncols; i++ {
		key := r.str()
		c := &docColumn{typ: ColType(r.u8()), n: int(r.u32())}
		c.present = r.bits()
		switch c.typ {
		case ColNumeric:
			c.ints, c.flts, c.isFloat = r.i64s(), r.f64s(), r.bits()
		case ColString:
			nd := r.count(1)
			if r.err != nil {
				return nil, ErrColumnBlobUnreadable
			}
			c.dict = make([]string, nd)
			for j := range c.dict {
				c.dict[j] = r.str()
			}
			nc := r.count(4)
			if r.err != nil {
				return nil, ErrColumnBlobUnreadable
			}
			c.codes = make([]uint32, nc)
			for j := range c.codes {
				c.codes[j] = r.u32()
				// A code outside the dictionary would panic on the first read.
				// Catch it here, where the answer is still "rebuild".
				if c.codes[j] >= uint32(nd) {
					return nil, fmt.Errorf("%w: dictionary code out of range", ErrColumnBlobUnreadable)
				}
			}
		case colUnbuildable:
			// Nothing to carry; the consumer falls back for this key either way.
		default:
			return nil, ErrColumnBlobUnreadable
		}
		if r.err != nil {
			return nil, ErrColumnBlobUnreadable
		}
		out.cols[key] = c
	}
	if r.err != nil || r.pos != len(r.b) {
		// Trailing bytes mean the blob is not what this version wrote.
		return nil, ErrColumnBlobUnreadable
	}

	if out.hasTemporal {
		out.buildZoneMap() // derived, not stored: cheaper to recompute than to verify
	}
	return out, nil
}
