package segment

import (
	"encoding/binary"
	"math/bits"
)

// Bit packing: value k of a page occupies bits [k*w, (k+1)*w) of the packed
// area, least significant bit first, bytes little-endian. Width 0 stores no
// bytes at all (a constant page).

func packedLen(n, width int) int { return (n*width + 7) / 8 }

func bitWidth(maxOffset uint64) int { return bits.Len64(maxOffset) }

// appendPacked appends vals packed at width bits each.
func appendPacked(dst []byte, vals []uint64, width int) []byte {
	if width == 0 {
		return dst
	}
	w := uint(width)
	var acc uint64
	var nb uint
	for _, v := range vals {
		if w < 64 {
			v &= 1<<w - 1
		}
		acc |= v << nb
		nb += w
		if nb >= 64 {
			dst = binary.LittleEndian.AppendUint64(dst, acc)
			nb -= 64
			acc = v >> (w - nb) // the bits of v that did not fit (0 when none)
		}
	}
	for ; nb > 0; nb -= min(nb, 8) {
		dst = append(dst, byte(acc))
		acc >>= 8
	}
	return dst
}

// unpackAt reads value i. The caller guarantees len(src) >=
// packedLen(i+1, width); bytes past the end of src read as zero.
func unpackAt(src []byte, i, width int) uint64 {
	if width == 0 {
		return 0
	}
	bit := uint64(i) * uint64(width) // #nosec G115 -- i and width are validated non-negative
	b := int(bit >> 3)               // #nosec G115 -- bounded by len(src)*8
	s := uint(bit & 7)
	var lo uint64
	if b+8 <= len(src) {
		lo = binary.LittleEndian.Uint64(src[b:])
	} else {
		for k := 0; k < 8 && b+k < len(src); k++ {
			lo |= uint64(src[b+k]) << (8 * uint(k)) // #nosec G115 -- k < 8
		}
	}
	v := lo >> s
	if int(s)+width > 64 && b+8 < len(src) { // #nosec G115 -- s < 8
		v |= uint64(src[b+8]) << (64 - s)
	}
	if width < 64 {
		v &= 1<<uint(width) - 1 // #nosec G115 -- width in [1,63]
	}
	return v
}
