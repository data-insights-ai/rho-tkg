// Package timeblock studies bounded immutable coordinate blocks, independently
// of a storage engine. The provisional format stores exact int64 coordinates
// on Z or Q axes. It never infers events or converts points to durations.
// Row ordinals join externally held assertion identities/revisions; the block
// owns no identity, revision, property, catalog, index, or axis dictionary.
package timeblock

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"math/bits"

	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

// Layout selects a measured candidate; it is not an engine format decision.
type Layout uint8

const (
	// Partitioned groups point starts then span starts, with span-only endpoints
	// and 2-bit boundaries. Mixed input pays a 2-byte original-to-physical join.
	Partitioned Layout = iota + 1
	// Sparse keeps original starts, 3-bit kinds and span-only endpoints. Mixed
	// input pays a 2-byte prefix rank per 64 rows for bounded random lookup.
	Sparse
	// Tagged keeps original starts, 3-bit kinds and a dense endpoint column.
	Tagged
)

// Kind preserves the caller's finite boundary shape. Point is independent of
// the four span shapes, even when a closed singleton has identical support.
type Kind uint8

// Finite shapes retain boundary flags independently of set-support equivalence.
const (
	Point Kind = iota
	OpenOpen
	ClosedOpen
	OpenClosed
	ClosedClosed
)

// Row is pointer-free input/materialization. Points require End=0; zero is an
// ordinary coordinate, never infinity. Span endpoints are finite and explicit.
// Assertion interpretation and multiplicity are outside this payload.
type Row struct {
	Start, End int64
	Kind       Kind
}

// Hard bounds limit every block and all writer/decoder staging work.
const (
	MaxRows     = 4096
	MaxBytes    = 1 << 20
	headerBytes = 104
	rankStep    = 64
	magic       = "TKGTIME\x00"
)

// Errors identify invalid input, policy, unsupported exact representations,
// malformed physical bytes and caller mistakes. Temporal axis errors propagate.
var (
	ErrUnsupported = errors.New("timeblock: exact representation unsupported")
	ErrInvalidRow  = errors.New("timeblock: invalid row")
	ErrLayout      = errors.New("timeblock: invalid layout")
	ErrLimits      = errors.New("timeblock: invalid limits")
	ErrLimit       = errors.New("timeblock: resource limit")
	ErrCorrupt     = errors.New("timeblock: corrupt block")
	ErrVersion     = errors.New("timeblock: unsupported version")
	ErrRange       = errors.New("timeblock: row out of range or nil block")
	ErrCallback    = errors.New("timeblock: nil context or callback")
)

// Limits can tighten hard block bounds. MaxBytes bounds encoded bytes, not
// writer scratch: staging is separately bounded by MaxRows (at most two int64
// columns plus packed kinds/join). OpenOwned additionally copies encoded bytes.
// Zero selects the hard bound.
type Limits struct{ MaxRows, MaxBytes int }

func (l Limits) resolved() (Limits, error) {
	l.MaxRows = cmp.Or(l.MaxRows, MaxRows)
	l.MaxBytes = cmp.Or(l.MaxBytes, MaxBytes)
	if l.MaxRows < 1 || l.MaxRows > MaxRows || l.MaxBytes < 1 || l.MaxBytes > MaxBytes {
		return Limits{}, ErrLimits
	}
	return l, nil
}

// ByteLedger counts consumed physical coordinate-block bytes. The external axis
// descriptor/dictionary, logical handles, revisions, graph/property/index/WAL
// costs are excluded: Total/rows is NOT database bytes per fact. Join includes
// the row permutation or sparse rank directory, when needed.
type ByteLedger struct{ Total, Envelope, Starts, Ends, Kinds, Join int }

// Block retains only bounded bytes and constant-size column views, with no row
// objects or resident maps. Safe for concurrent reads under its ownership rule.
type Block struct {
	data         []byte
	axisHash     [32]byte
	profile      temporal.Profile
	layout       Layout
	rows, spans  int
	starts, ends column
	kinds, join  []byte
	ledger       ByteLedger
}

func checkAxis(a temporal.Axis) error {
	d := a.Descriptor()
	if d.Profile == 0 || a.DefinitionHash() == ([32]byte{}) {
		return temporal.ErrInvalidAxis
	}
	if d.Profile != temporal.ProfileIntegerZ && d.Profile != temporal.ProfileRationalQ {
		return ErrUnsupported
	}
	return nil
}
func validRow(r Row, p temporal.Profile) bool {
	if r.Kind == Point {
		return r.End == 0
	}
	if r.Kind > ClosedClosed || r.Start > r.End {
		return false
	}
	if r.Start == r.End {
		return r.Kind == ClosedClosed
	}
	// An open/open adjacent pair has no integer member. Unsigned subtraction
	// preserves the exact positive distance across the complete signed range.
	if p == temporal.ProfileIntegerZ && r.Kind == OpenOpen && uint64(r.End)-uint64(r.Start) == 1 {
		return false
	} // #nosec G115 -- exact wrapping distance for ordered signed endpoints.
	return true
}

// FromScope accepts canonical finite Point/Span support on a matching axis,
// exactly when every coordinate is int64 (Q denominators must be one). Scope
// normalization has already discarded source syntax; no event identity is
// inferred. Unsupported fractions, wide values, infinities and regions decline.
func FromScope(axis temporal.Axis, s temporal.Scope) (Row, error) {
	if err := checkAxis(axis); err != nil {
		return Row{}, err
	}
	if axis.DefinitionHash() != s.Axis().DefinitionHash() {
		return Row{}, temporal.ErrAxisMismatch
	}
	if s.Kind() != temporal.ScopePoint && s.Kind() != temporal.ScopeSpan {
		return Row{}, ErrUnsupported
	}
	lo, hi, ok := s.Bounds()
	if !ok {
		return Row{}, ErrUnsupported
	}
	lp, lok := lo.Position()
	hp, hok := hi.Position()
	if !lok || !hok {
		return Row{}, ErrUnsupported
	}
	start, ok := intCoordinate(lp)
	if !ok {
		return Row{}, ErrUnsupported
	}
	if s.Kind() == temporal.ScopePoint {
		return Row{Start: start, Kind: Point}, nil
	}
	end, ok := intCoordinate(hp)
	if !ok {
		return Row{}, ErrUnsupported
	}
	k := OpenOpen
	if lo.Inclusive() {
		k++
	}
	if hi.Inclusive() {
		k += 2
	}
	r := Row{Start: start, End: end, Kind: k}
	if !validRow(r, axis.Descriptor().Profile) {
		return Row{}, ErrInvalidRow
	}
	return r, nil
}
func intCoordinate(p temporal.Position) (int64, bool) {
	if n, ok := p.Integer(); ok {
		return n.Int64()
	}
	r, ok := p.Rational()
	if !ok {
		return 0, false
	}
	den, ok := r.Denominator().Int64()
	if !ok || den != 1 {
		return 0, false
	}
	return r.Numerator().Int64()
}

// Encode preserves original row ordinals and source boundary shape. Inputs are
// validated before allocating bounded staging columns. No semantic hash or
// authentication claim is made by the physical checksum/digest.
func Encode(axis temporal.Axis, rows []Row, layout Layout, l Limits) ([]byte, error) {
	l, err := l.resolved()
	if err != nil {
		return nil, err
	}
	if err = checkAxis(axis); err != nil {
		return nil, err
	}
	if layout < Partitioned || layout > Tagged {
		return nil, ErrLayout
	}
	if len(rows) > l.MaxRows {
		return nil, ErrLimit
	}
	spans := 0
	for _, r := range rows {
		if !validRow(r, axis.Descriptor().Profile) {
			return nil, ErrInvalidRow
		}
		if r.Kind != Point {
			spans++
		}
	}
	n := len(rows)
	starts := make([]int64, n)
	ends := make([]int64, spans)
	var kinds, join []byte
	if layout == Tagged {
		ends = make([]int64, n)
	}
	if layout == Partitioned {
		kinds = make([]byte, packedLen(spans, 2))
		if spans > 0 && spans < n {
			join = make([]byte, 2*n)
		}
		point, span := 0, 0
		for i, r := range rows {
			pos := point
			if r.Kind != Point {
				pos = n - spans + span
				ends[span] = r.End
				putBits(kinds, span, 2, uint64(r.Kind-1))
				span++
			} else {
				point++
			}
			starts[pos] = r.Start
			if len(join) > 0 {
				binary.LittleEndian.PutUint16(join[2*i:], uint16(pos))
			}
		} // #nosec G115 -- pos < MaxRows.
	} else {
		if spans > 0 {
			kinds = make([]byte, packedLen(n, 3))
		}
		if layout == Sparse && spans > 0 && spans < n {
			join = make([]byte, 2*((n+rankStep-1)/rankStep+1))
		}
		span := 0
		for i, r := range rows {
			starts[i] = r.Start
			if len(join) > 0 && i%rankStep == 0 {
				binary.LittleEndian.PutUint16(join[2*(i/rankStep):], uint16(span))
			}
			if len(kinds) > 0 {
				putBits(kinds, i, 3, uint64(r.Kind))
			}
			if r.Kind != Point {
				if layout == Tagged {
					ends[i] = r.End
				} else {
					ends[span] = r.End
				}
				span++
			}
		} // #nosec G115 -- span <= MaxRows.
		if len(join) > 0 {
			binary.LittleEndian.PutUint16(join[len(join)-2:], uint16(span))
		} // #nosec G115 -- span <= MaxRows.
	}
	sc, ec := encodeColumn(starts), encodeColumn(ends)
	total := headerBytes + len(sc) + len(ec) + len(kinds) + len(join)
	if total > l.MaxBytes {
		return nil, ErrLimit
	}
	out := make([]byte, total)
	copy(out, magic)
	binary.LittleEndian.PutUint16(out[8:], 1)
	out[10] = byte(layout)
	out[11] = byte(axis.Descriptor().Profile)
	binary.LittleEndian.PutUint16(out[12:], uint16(n))     // #nosec G115 -- n <= MaxRows.
	binary.LittleEndian.PutUint16(out[14:], uint16(spans)) // #nosec G115 -- row counts <= MaxRows.
	off := headerBytes
	for i, s := range [][]byte{sc, ec, kinds, join} {
		binary.LittleEndian.PutUint32(out[16+4*i:], uint32(len(s)))
		copy(out[off:], s)
		off += len(s)
	} // #nosec G115 -- sections bounded by MaxBytes.
	hash := axis.DefinitionHash()
	copy(out[32:64], hash[:])
	sealDigest(out)
	return out, nil
}

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

func sealDigest(data []byte) {
	binary.LittleEndian.PutUint32(data[64:], crc32.Checksum(data[headerBytes:], castagnoli))
	h := sha256.New()
	_, _ = h.Write(data[:72])
	_, _ = h.Write(data[headerBytes:])
	copy(data[72:104], h.Sum(nil))
}

// OpenOwned validates before cloning the exact consumed bytes. Caller mutation
// afterward cannot affect this immutable block. It does not retain excess
// capacity in the supplied buffer.
func OpenOwned(data []byte, axis temporal.Axis, l Limits) (*Block, error) {
	b, err := OpenBorrowed(data, axis, l)
	if err != nil {
		return nil, err
	}
	owned := bytes.Clone(data)
	return openValidated(owned, b), nil
}
func openValidated(data []byte, b *Block) *Block {
	offset := headerBytes
	b.data = data[:len(data):len(data)]
	sc := b.ledger.Starts
	ec := b.ledger.Ends
	b.starts, _ = parseColumn(data[offset:offset+sc], b.rows)
	offset += sc
	endCount := b.spans
	if b.layout == Tagged {
		endCount = b.rows
	}
	b.ends, _ = parseColumn(data[offset:offset+ec], endCount)
	offset += ec
	b.kinds = data[offset : offset+b.ledger.Kinds : offset+b.ledger.Kinds]
	offset += b.ledger.Kinds
	b.join = data[offset:len(data):len(data)]
	return b
}

// OpenBorrowed validates and retains a read-only view. Caller must keep the
// bytes alive and unchanged while any read uses the block. This is a lifetime
// precondition, not a mutation detector. Exact input tiling rejects trailing data.
func OpenBorrowed(data []byte, axis temporal.Axis, l Limits) (*Block, error) {
	l, err := l.resolved()
	if err != nil {
		return nil, err
	}
	if err = checkAxis(axis); err != nil {
		return nil, err
	}
	if len(data) > l.MaxBytes {
		return nil, ErrLimit
	}
	if len(data) < headerBytes || string(data[:8]) != magic {
		return nil, ErrCorrupt
	}
	if binary.LittleEndian.Uint16(data[8:]) != 1 {
		return nil, ErrVersion
	}
	layout := Layout(data[10])
	if layout < Partitioned || layout > Tagged {
		return nil, ErrCorrupt
	}
	profile := temporal.Profile(data[11])
	hash := axis.DefinitionHash()
	if profile != axis.Descriptor().Profile || !bytes.Equal(data[32:64], hash[:]) {
		return nil, temporal.ErrAxisMismatch
	}
	n, spans := int(binary.LittleEndian.Uint16(data[12:])), int(binary.LittleEndian.Uint16(data[14:]))
	if n > l.MaxRows {
		return nil, ErrLimit
	}
	if spans > n || binary.LittleEndian.Uint32(data[68:]) != 0 {
		return nil, ErrCorrupt
	}
	ledger := ByteLedger{Total: len(data), Envelope: headerBytes}
	lengths := [4]int{}
	off := headerBytes
	for i := range lengths {
		v := binary.LittleEndian.Uint32(data[16+4*i:])
		if uint64(v) > uint64(len(data)-off) {
			return nil, ErrCorrupt
		}
		lengths[i] = int(v)
		off += lengths[i]
	} // #nosec G115 -- lengths bounded by supplied bytes <= MaxBytes.
	if off != len(data) || crc32.Checksum(data[headerBytes:], castagnoli) != binary.LittleEndian.Uint32(data[64:]) {
		return nil, ErrCorrupt
	}
	h := sha256.New()
	_, _ = h.Write(data[:72])
	_, _ = h.Write(data[headerBytes:])
	if !bytes.Equal(data[72:104], h.Sum(nil)) {
		return nil, ErrCorrupt
	}
	ledger.Starts, ledger.Ends, ledger.Kinds, ledger.Join = lengths[0], lengths[1], lengths[2], lengths[3]
	b := &Block{data: data[:len(data):len(data)], axisHash: hash, profile: profile, layout: layout, rows: n, spans: spans, ledger: ledger}
	off = headerBytes
	b.starts, err = parseColumn(data[off:off+lengths[0]], n)
	if err != nil {
		return nil, err
	}
	off += lengths[0]
	endCount := spans
	if layout == Tagged {
		endCount = n
	}
	b.ends, err = parseColumn(data[off:off+lengths[1]], endCount)
	if err != nil {
		return nil, err
	}
	off += lengths[1]
	b.kinds = data[off : off+lengths[2] : off+lengths[2]]
	off += lengths[2]
	b.join = data[off:len(data):len(data)]
	if err = b.validateKinds(); err != nil {
		return nil, err
	}
	return b, nil
}
func (b *Block) validateKinds() error {
	expectedKinds, expectedJoin := 0, 0
	if b.layout == Partitioned {
		expectedKinds = packedLen(b.spans, 2)
		if b.spans > 0 && b.spans < b.rows {
			expectedJoin = 2 * b.rows
		}
	} else {
		if b.spans > 0 {
			expectedKinds = packedLen(b.rows, 3)
		}
		if b.layout == Sparse && b.spans > 0 && b.spans < b.rows {
			expectedJoin = 2 * ((b.rows+rankStep-1)/rankStep + 1)
		}
	}
	if len(b.kinds) != expectedKinds || len(b.join) != expectedJoin {
		return ErrCorrupt
	}
	if len(b.kinds) > 0 {
		width, count := 3, b.rows
		if b.layout == Partitioned {
			width, count = 2, b.spans
		}
		if !zeroPadding(b.kinds, count, width) {
			return ErrCorrupt
		}
	}
	var seen [MaxRows / 64]uint64
	spans := 0
	for i := range b.rows {
		if b.layout == Partitioned && len(b.join) > 0 {
			pos := int(binary.LittleEndian.Uint16(b.join[2*i:]))
			if pos >= b.rows || seen[pos/64]&(uint64(1)<<uint(pos%64)) != 0 {
				return ErrCorrupt
			}
			seen[pos/64] |= uint64(1) << uint(pos%64)
		}
		if b.layout == Sparse && len(b.join) > 0 && i%rankStep == 0 && int(binary.LittleEndian.Uint16(b.join[2*(i/rankStep):])) != spans {
			return ErrCorrupt
		}
		r := b.row(i)
		if !validRow(r, b.profile) {
			return ErrCorrupt
		}
		if r.Kind != Point {
			spans++
		}
		if b.layout == Tagged && r.Kind == Point && b.ends.at(i) != 0 {
			return ErrCorrupt
		}
	}
	if spans != b.spans {
		return ErrCorrupt
	}
	if b.layout == Sparse && len(b.join) > 0 && int(binary.LittleEndian.Uint16(b.join[len(b.join)-2:])) != spans {
		return ErrCorrupt
	}
	return nil
}

// Len returns coordinate rows; identities and revisions are external.
func (b *Block) Len() int { return b.rows }

// Layout returns the candidate's physical organization.
func (b *Block) Layout() Layout { return b.layout }

// AxisHash binds the complete shared axis definition once per block.
func (b *Block) AxisHash() [32]byte { return b.axisHash }

// Ledger returns the exact consumed physical byte accounting.
func (b *Block) Ledger() ByteLedger { return b.ledger }

// Row returns an independent value at the original row ordinal. Sparse mixed
// lookup scans at most 63 kind tags after its encoded rank anchor.
func (b *Block) Row(i int) (Row, error) {
	if b == nil || i < 0 || i >= b.rows {
		return Row{}, ErrRange
	}
	return b.row(i), nil
}
func (b *Block) row(i int) Row {
	if b.layout == Partitioned {
		pos := i
		if len(b.join) > 0 {
			pos = int(binary.LittleEndian.Uint16(b.join[2*i:]))
		}
		r := Row{Start: b.starts.at(pos)}
		if pos >= b.rows-b.spans {
			k := pos - (b.rows - b.spans)
			r.Kind = Kind(getBits(b.kinds, k, 2)) + 1 // #nosec G115 -- a 2-bit value is <= 3.
			r.End = b.ends.at(k)
		}
		return r
	}
	r := Row{Start: b.starts.at(i)}
	if len(b.kinds) > 0 {
		r.Kind = Kind(getBits(b.kinds, i, 3)) // #nosec G115 -- a 3-bit value is <= 7.
	}
	if r.Kind == Point {
		return r
	}
	end := i
	if b.layout == Sparse {
		end = i
		if len(b.join) > 0 {
			group := i / rankStep
			end = int(binary.LittleEndian.Uint16(b.join[2*group:]))
			for k := group * rankStep; k < i; k++ {
				if getBits(b.kinds, k, 3) != 0 {
					end++
				}
			}
		}
	}
	if end >= b.spans && b.layout == Sparse {
		return Row{Kind: Kind(255)}
	}
	r.End = b.ends.at(end)
	return r
}

// ContainsAt tests exact membership of an int64 coordinate on this block's
// shared axis, with no tick arithmetic or inference of event interpretation.
// The int64 query is an exact subset of Z/Q; fractional queries are unsupported
// by this candidate and require the general materialized temporal API.
func (b *Block) ContainsAt(i int, x int64) (bool, error) {
	r, err := b.Row(i)
	if err != nil {
		return false, err
	}
	if r.Kind == Point {
		return x == r.Start, nil
	}
	lo := x > r.Start || (x == r.Start && (r.Kind == ClosedOpen || r.Kind == ClosedClosed))
	hi := x < r.End || (x == r.End && (r.Kind == OpenClosed || r.Kind == ClosedClosed))
	return lo && hi, nil
}

// Scan visits original ordinals in order without allocating/retaining rows or
// exposing mutable slices. False stops successfully; cancellation returns
// context.Err, checked before each row. A nil context/callback is ErrCallback.
func (b *Block) Scan(ctx context.Context, fn func(int, Row) bool) error {
	if b == nil {
		return ErrRange
	}
	if ctx == nil || fn == nil {
		return ErrCallback
	}
	span := 0
	for i := range b.rows {
		if err := ctx.Err(); err != nil {
			return err
		}
		var r Row
		if b.layout == Sparse {
			r.Start = b.starts.at(i)
			if len(b.kinds) > 0 {
				r.Kind = Kind(getBits(b.kinds, i, 3)) // #nosec G115 -- a 3-bit value is <= 7.
			}
			if r.Kind != Point {
				r.End = b.ends.at(span)
				span++
			}
		} else {
			r = b.row(i)
		}
		if !fn(i, r) {
			return nil
		}
	}
	return ctx.Err()
}

type column struct {
	base   int64
	width  int
	packed []byte
}

func encodeColumn(v []int64) []byte {
	if len(v) == 0 {
		return nil
	}
	base := v[0]
	for _, x := range v {
		base = min(base, x)
	}
	var maximum uint64
	for _, x := range v {
		maximum = max(maximum, uint64(x)-uint64(base))
	}
	w := bits.Len64(maximum)
	out := make([]byte, 9+packedLen(len(v), w))
	out[0] = byte(w)
	binary.LittleEndian.PutUint64(out[1:], uint64(base))
	for i, x := range v {
		putBits(out[9:], i, w, uint64(x)-uint64(base))
	}
	return out
} // #nosec G115 -- unsigned offset/bit pattern preserves full signed range.
func parseColumn(data []byte, n int) (column, error) {
	if n == 0 {
		if len(data) != 0 {
			return column{}, ErrCorrupt
		}
		return column{}, nil
	}
	if len(data) < 9 || data[0] > 64 {
		return column{}, ErrCorrupt
	}
	w := int(data[0])
	if len(data) != 9+packedLen(n, w) || !zeroPadding(data[9:], n, w) {
		return column{}, ErrCorrupt
	}
	c := column{base: int64(binary.LittleEndian.Uint64(data[1:])), width: w, packed: data[9:]}
	var maximum uint64
	hasBase := false
	for i := range n {
		x := getBits(c.packed, i, w)
		maximum = max(maximum, x)
		hasBase = hasBase || x == 0
		if c.at(i) < c.base {
			return column{}, ErrCorrupt
		}
	}
	if !hasBase || bits.Len64(maximum) != w {
		return column{}, ErrCorrupt
	}
	return c, nil
}                               // #nosec G115 -- exact int64 bit-pattern decoding.
func (c column) at(i int) int64 { return int64(uint64(c.base) + getBits(c.packed, i, c.width)) } // #nosec G115 -- bit pattern, validated signed range.
func packedLen(n, w int) int    { return (n*w + 7) / 8 }
func zeroPadding(data []byte, n, w int) bool {
	used := n * w % 8
	return used == 0 || data[len(data)-1]>>uint(used) == 0
}
func putBits(dst []byte, i, w int, v uint64) {
	offset := i * w
	for w > 0 {
		at, shift := offset/8, uint(offset%8)
		take := min(w, 8-int(shift))
		mask := uint64(1)<<uint(take) - 1
		dst[at] |= byte(v&mask) << shift // #nosec G115 -- take <= 8, mask <= 255.
		v >>= uint(take)
		w -= take
		offset += take
	}
}
func getBits(src []byte, i, w int) uint64 {
	if w == 0 {
		return 0
	}
	bit := i * w
	at, shift := bit/8, uint(bit%8)
	var v uint64
	if at+8 <= len(src) {
		v = binary.LittleEndian.Uint64(src[at:])
	} else {
		for k := 0; k < 8 && at+k < len(src); k++ {
			v |= uint64(src[at+k]) << uint(8*k)
		}
	}
	v >>= shift
	if int(shift)+w > 64 && at+8 < len(src) {
		v |= uint64(src[at+8]) << (64 - shift)
	}
	if w < 64 {
		v &= uint64(1)<<uint(w) - 1
	}
	return v
}
