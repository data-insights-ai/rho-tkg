package segment

import (
	"encoding/binary"
	"fmt"
)

// An int column holds n int64 values in pages of pageRows values:
//
//	u32 n | u16 pageRows | u32 pageCount | (pageCount+1) × u32 page offset | pages
//
// and each page is
//
//	u8 transform | u8 width | i64 base | [i64 first, deltaPrev only] | packed
//
// The page stores d[k] - base, bit-packed at width bits (width 0: every d[k]
// equals base, no bytes). d is the page's values after its transform:
//
//	transformNone      d[k] = v[k]
//	transformMinusRef  d[k] = v[k] - ref[k]          (valid_to, tx_from vs valid_from)
//	transformDeltaPrev d[0] = 0, d[k] = v[k]-v[k-1]  (sorted columns; v[0] = first)
//	transformDeltaRun  d[k] = v[k] at a run start or page start, else v[k]-v[k-1]
//	                                                  (end ordinals inside a start run)
//
// All arithmetic wraps (two's complement), so every int64 sequence
// round-trips exactly. The writer tries each transform the column allows and
// keeps the smallest page.
const (
	transformNone      byte = 0
	transformMinusRef  byte = 1
	transformDeltaPrev byte = 2
	transformDeltaRun  byte = 3

	intColHeader  = 4 + 2 + 4
	pageHeaderMin = 1 + 1 + 8
)

func allow(ts ...byte) uint8 {
	var m uint8
	for _, t := range ts {
		m |= 1 << t
	}
	return m
}

// intEncodeCtx carries what a transform needs besides the values.
type intEncodeCtx struct {
	ref      []int64            // transformMinusRef: reference values, same length
	runStart func(row int) bool // transformDeltaRun: row starts a run
	allowed  uint8              // bitmask of transforms to try
}

func allZero(vals []int64) bool {
	for _, v := range vals {
		if v != 0 {
			return false
		}
	}
	return true
}

// encodeIntCol encodes vals. pageRows must be 1..4096.
func encodeIntCol(vals []int64, pageRows int, ctx intEncodeCtx) []byte {
	n := len(vals)
	pages := (n + pageRows - 1) / pageRows
	out := make([]byte, 0, intColHeader+4*(pages+1)+n)
	out = binary.LittleEndian.AppendUint32(out, uint32(n))        // #nosec G115 -- row counts are bounded by the writer
	out = binary.LittleEndian.AppendUint16(out, uint16(pageRows)) // #nosec G115 -- pageRows <= 4096
	out = binary.LittleEndian.AppendUint32(out, uint32(pages))    // #nosec G115 -- bounded by n
	offAt := len(out)
	out = append(out, make([]byte, 4*(pages+1))...)
	base := len(out)
	d := make([]int64, pageRows)
	best := make([]int64, pageRows)
	u := make([]uint64, pageRows)
	for p := 0; p < pages; p++ {
		binary.LittleEndian.PutUint32(out[offAt+4*p:], uint32(len(out)-base)) // #nosec G115 -- section sizes fit u32 (see writer bound)
		lo := p * pageRows
		hi := min(lo+pageRows, n)
		cnt := hi - lo
		bestT, bestW, bestSize := byte(0), 0, -1
		var bestBase, bestFirst int64
		for t := byte(0); t < 8; t++ {
			if ctx.allowed&(1<<t) == 0 {
				continue
			}
			first := applyTransform(t, vals[lo:hi], lo, ctx, d[:cnt])
			mn := d[0]
			for _, x := range d[1:cnt] {
				mn = min(mn, x)
			}
			var mx uint64
			for _, x := range d[:cnt] {
				mx = max(mx, uint64(x)-uint64(mn)) // #nosec G115 -- wrapping offset from the page minimum
			}
			w := bitWidth(mx)
			size := pageHeaderMin + packedLen(cnt, w)
			if t == transformDeltaPrev {
				size += 8
			}
			if bestSize < 0 || size < bestSize {
				bestT, bestW, bestSize, bestBase, bestFirst = t, w, size, mn, first
				copy(best, d[:cnt])
			}
		}
		out = append(out, bestT, byte(bestW))
		out = binary.LittleEndian.AppendUint64(out, uint64(bestBase)) // #nosec G115 -- bit pattern
		if bestT == transformDeltaPrev {
			out = binary.LittleEndian.AppendUint64(out, uint64(bestFirst)) // #nosec G115 -- bit pattern
		}
		for k := 0; k < cnt; k++ {
			u[k] = uint64(best[k]) - uint64(bestBase) // #nosec G115 -- wrapping offset
		}
		out = appendPacked(out, u[:cnt], bestW)
	}
	binary.LittleEndian.PutUint32(out[offAt+4*pages:], uint32(len(out)-base)) // #nosec G115 -- see above
	return out
}

// applyTransform fills d with the transformed page values and returns the
// page's first value (used by transformDeltaPrev).
func applyTransform(t byte, v []int64, lo int, ctx intEncodeCtx, d []int64) int64 {
	switch t {
	case transformMinusRef:
		for k := range v {
			d[k] = v[k] - ctx.ref[lo+k]
		}
	case transformDeltaPrev:
		d[0] = 0
		for k := 1; k < len(v); k++ {
			d[k] = v[k] - v[k-1]
		}
	case transformDeltaRun:
		for k := range v {
			if k == 0 || ctx.runStart(lo+k) {
				d[k] = v[k]
			} else {
				d[k] = v[k] - v[k-1]
			}
		}
	default:
		copy(d, v)
	}
	return v[0]
}

// intCol is a parsed, validated int column. The zero value (absent) reads
// every value as 0.
type intCol struct {
	name      string
	present   bool
	n         int
	pageRows  int
	pageCount int
	offs      []byte // (pageCount+1) × u32
	pages     []byte
}

func corrupt(section, format string, args ...any) error {
	return &CorruptError{Section: section, Reason: fmt.Sprintf(format, args...)}
}

// parseIntCol validates an int column completely — header, every page
// offset, every page header and packed length — without allocating, so a
// later read cannot run out of bounds. want is the value count the caller
// expects (-1: take it from the section).
func parseIntCol(name string, b []byte, want, pageRows int, allowed uint8) (intCol, error) {
	if len(b) < intColHeader {
		return intCol{}, corrupt(name, "int column header truncated (%d bytes)", len(b))
	}
	n := int(binary.LittleEndian.Uint32(b))
	pr := int(binary.LittleEndian.Uint16(b[4:]))
	pc := int(binary.LittleEndian.Uint32(b[6:]))
	if want >= 0 && n != want {
		return intCol{}, corrupt(name, "holds %d values, want %d", n, want)
	}
	if pr != pageRows {
		return intCol{}, corrupt(name, "page size %d, want %d", pr, pageRows)
	}
	if pc != (n+pageRows-1)/pageRows {
		return intCol{}, corrupt(name, "%d pages for %d values of %d per page", pc, n, pageRows)
	}
	if (len(b)-intColHeader)/4 < pc+1 {
		return intCol{}, corrupt(name, "page directory truncated")
	}
	c := intCol{name: name, present: true, n: n, pageRows: pageRows, pageCount: pc}
	c.offs = b[intColHeader : intColHeader+4*(pc+1)]
	c.pages = b[intColHeader+4*(pc+1):]
	if binary.LittleEndian.Uint32(c.offs) != 0 || int(binary.LittleEndian.Uint32(c.offs[4*pc:])) != len(c.pages) {
		return intCol{}, corrupt(name, "page directory does not span the pages")
	}
	for p := 0; p < pc; p++ {
		lo, hi := c.pageBounds(p)
		if hi < lo || hi > len(c.pages) {
			return intCol{}, corrupt(name, "page %d offsets out of order", p)
		}
		pg := c.pages[lo:hi]
		if len(pg) < pageHeaderMin {
			return intCol{}, corrupt(name, "page %d header truncated", p)
		}
		t, w := pg[0], int(pg[1])
		if t > 7 || allowed&(1<<t) == 0 {
			return intCol{}, corrupt(name, "page %d transform %d not allowed", p, t)
		}
		if w > 64 {
			return intCol{}, corrupt(name, "page %d width %d", p, w)
		}
		hdr := pageHeaderMin
		if t == transformDeltaPrev {
			hdr += 8
		}
		if len(pg) != hdr+packedLen(c.pageLen(p), w) {
			return intCol{}, corrupt(name, "page %d is %d bytes, want %d", p, len(pg), hdr+packedLen(c.pageLen(p), w))
		}
	}
	return c, nil
}

func (c *intCol) pageBounds(p int) (int, int) {
	return int(binary.LittleEndian.Uint32(c.offs[4*p:])), int(binary.LittleEndian.Uint32(c.offs[4*p+4:]))
}

func (c *intCol) pageLen(p int) int {
	if p == c.pageCount-1 {
		return c.n - p*c.pageRows
	}
	return c.pageRows
}

type pageView struct {
	transform byte
	width     int
	base      int64
	first     int64
	packed    []byte
	n         int
}

func (c *intCol) page(p int) pageView {
	lo, hi := c.pageBounds(p)
	pg := c.pages[lo:hi]
	v := pageView{transform: pg[0], width: int(pg[1]), base: int64(binary.LittleEndian.Uint64(pg[2:])), n: c.pageLen(p)} // #nosec G115 -- bit pattern
	pg = pg[pageHeaderMin:]
	if v.transform == transformDeltaPrev {
		v.first = int64(binary.LittleEndian.Uint64(pg)) // #nosec G115 -- bit pattern
		pg = pg[8:]
	}
	v.packed = pg
	return v
}

// d returns the stored (transformed) value k of the page.
func (v *pageView) d(k int) int64 {
	return int64(uint64(v.base) + unpackAt(v.packed, k, v.width)) // #nosec G115 -- wrapping bit pattern
}

// atNoRef reads value i of a column whose pages use only transformNone or
// transformDeltaPrev (O(1) for the former, O(page) for the latter).
func (c *intCol) atNoRef(i int) int64 {
	if !c.present {
		return 0
	}
	p := i / c.pageRows
	v := c.page(p)
	k := i - p*c.pageRows
	switch v.transform {
	case transformDeltaPrev:
		x := v.first
		for j := 1; j <= k; j++ {
			x += v.d(j)
		}
		return x
	default:
		return v.d(k)
	}
}

// decodePage fills out[:n] with the n values of page p. ref holds the
// reference values of the same rows (transformMinusRef) and runStart[k]
// whether row k of the page starts a run (transformDeltaRun).
func (c *intCol) decodePage(p, n int, out []int64, ref []int64, runStart []bool) {
	if !c.present {
		clear(out[:n])
		return
	}
	v := c.page(p)
	switch v.transform {
	case transformMinusRef:
		for k := 0; k < v.n; k++ {
			out[k] = v.d(k) + ref[k]
		}
	case transformDeltaPrev:
		x := v.first
		out[0] = x
		for k := 1; k < v.n; k++ {
			x += v.d(k)
			out[k] = x
		}
	case transformDeltaRun:
		for k := 0; k < v.n; k++ {
			if k == 0 || runStart[k] {
				out[k] = v.d(k)
			} else {
				out[k] = out[k-1] + v.d(k)
			}
		}
	default:
		for k := 0; k < v.n; k++ {
			out[k] = v.d(k)
		}
	}
}
