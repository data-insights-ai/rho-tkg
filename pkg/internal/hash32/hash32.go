// Package hash32 is the one conversion between an integrity hash's public
// form — 64 lowercase hex characters (types.NodeIntegrity / RelIntegrity) —
// and its 32 raw bytes, the form rows keep resident (types/compact.go), on
// disk (storeutil wire) and in segments (graph/internal/segment). Only the
// canonical form converts, so the round trip back to the string is exact; any
// other string is not a hash this library produced and stays a string.
package hash32

import "encoding/hex"

// Decode decodes s into dst and reports whether s is canonical: 64 lowercase
// hex characters (what hex.EncodeToString of a SHA-256 sum produces).
func Decode(s string, dst *[32]byte) bool {
	if len(s) != 64 {
		return false
	}
	for i := 0; i < 32; i++ {
		hi, ok1 := nibble(s[2*i])
		lo, ok2 := nibble(s[2*i+1])
		if !ok1 || !ok2 {
			return false
		}
		dst[i] = hi<<4 | lo
	}
	return true
}

// Canonical reports whether s is a canonical hash (see Decode).
func Canonical(s string) bool {
	var b [32]byte
	return Decode(s, &b)
}

// String returns the canonical string of b's first 32 bytes, in one
// allocation (hex.EncodeToString makes two).
func String(b []byte) string {
	var buf [64]byte
	hex.Encode(buf[:], b[:32])
	return string(buf[:])
}

func nibble(c byte) (byte, bool) {
	switch {
	case c >= '0' && c <= '9':
		return c - '0', true
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10, true
	}
	return 0, false
}
