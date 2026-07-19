package storeutil

import (
	"runtime"
	"testing"
)

// fixmapNodeWireHostileEL builds a 1-entry msgpack fixmap { "el": <array of n
// elements> } where element 0 is a byte NOT decodable as an int (0x80, a
// fixmap-open byte — a type mismatch for DecodeInt) and every other element
// is a valid single-byte fixint (0x00) filler. The filler is REQUIRED even
// though decodeIntSlice itself fails on element 0 and never reads past it:
// SafeUnmarshal's guardMsgpackDepth pre-scan walks the WHOLE claimed
// structure before the real decoder ever runs, and rejects (as truncated) any
// claimed element count not backed by that many real bytes — so a "lying tiny
// header" alone never reaches decodeIntSlice at all. The exploitable shape is
// instead "N real bytes of cheap filler, structurally valid, semantically
// wrong at element 0" — the depth-guard accepts it (it doesn't validate
// per-element semantics), and only the untrusted len(n)-sized make() inside
// decodeIntSlice amplifies it.
func fixmapNodeWireHostileEL(n uint32) []byte {
	buf := []byte{0x81, 0xa2, 'e', 'l'} // fixmap(1), fixstr(2) "el"
	buf = append(buf, 0xdd,             // array32
		byte(n>>24), byte(n>>16), byte(n>>8), byte(n))
	buf = append(buf, 0x80) // element 0: fixmap-open byte — DecodeInt fails here
	for i := uint32(1); i < n; i++ {
		buf = append(buf, 0x00) // filler: valid fixint 0, never actually reached
	}
	return buf
}

// fixmapNodeWireHostileP is the PropertyWire ("p", decodePropertyArray)
// mirror of fixmapNodeWireHostileEL: element 0 is 0x05 (a fixint, NOT a map —
// DecodeMapLen fails), every other element is 0x80 (a valid empty-fixmap
// PropertyWire, needed only so guardMsgpackDepth's structural pre-scan
// accepts the full claimed count).
func fixmapNodeWireHostileP(n uint32) []byte {
	buf := []byte{0x81, 0xa1, 'p'} // fixmap(1), fixstr(1) "p"
	buf = append(buf, 0xdd,
		byte(n>>24), byte(n>>16), byte(n>>8), byte(n))
	buf = append(buf, 0x05) // element 0: fixint — DecodeMapLen fails here
	for i := uint32(1); i < n; i++ {
		buf = append(buf, 0x80) // filler: valid empty fixmap, never actually reached
	}
	return buf
}

// TestDecodeIntSlice_HostileArrayLenDoesNotAmplifyAllocation is the BACKLOG
// 15a regression: decodeIntSlice (backing NodeWire's ExtraLabels) previously
// did make([]int, n) directly from the untrusted msgpack array-length header
// BEFORE decoding a single element — a lesson-48-class allocation-
// amplification DoS. A payload structurally valid enough to pass
// SafeUnmarshal's depth pre-scan (so decodeIntSlice actually runs), but that
// fails semantically on its very FIRST element, must not have already paid
// for the full N-sized allocation. runtime.TotalAlloc is cumulative (GC never
// lowers it), so the measured delta is exactly what decoding allocated.
//
// Mutation check: reverting decodeIntSlice to make([]int, n) makes the delta
// jump to ~N*8 bytes (16 MB for N=2M) and fails the ceiling.
func TestDecodeIntSlice_HostileArrayLenDoesNotAmplifyAllocation(t *testing.T) {
	const hostileN = 2_000_000 // ~2 MB of filler input; pre-fix would allocate ~16 MB immediately
	data := fixmapNodeWireHostileEL(hostileN)

	var m0, m1 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m0)
	var w NodeWire
	err := SafeUnmarshal(data, &w)
	runtime.ReadMemStats(&m1)

	if err == nil {
		t.Fatal("decode of an ExtraLabels array with an invalid first element succeeded, want a decode error")
	}

	const ceiling = 4 << 20 // 4 MiB — far below the ~16 MB an eager make([]int, 2e6) costs
	if delta := m1.TotalAlloc - m0.TotalAlloc; delta > ceiling {
		t.Fatalf("decode allocated %d bytes for a %d-element hostile ExtraLabels claim that fails on element 0; want < %d "+
			"(eager make([]int, n) amplification regression)", delta, hostileN, ceiling)
	}
}

// TestDecodePropertyArray_HostileArrayLenDoesNotAmplifyAllocation is the
// PropertyWire mirror of the above — PropertyWire is a wider struct
// (~50+ bytes per element per the package doc comment, vs 1 wire byte per
// empty-fixmap filler element), so the amplification ratio for a hostile "p"
// array claim is even larger than for ExtraLabels.
func TestDecodePropertyArray_HostileArrayLenDoesNotAmplifyAllocation(t *testing.T) {
	const hostileN = 2_000_000 // ~2 MB of filler input; pre-fix would allocate 50+ MB immediately
	data := fixmapNodeWireHostileP(hostileN)

	var m0, m1 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m0)
	var w NodeWire
	err := SafeUnmarshal(data, &w)
	runtime.ReadMemStats(&m1)

	if err == nil {
		t.Fatal("decode of a Properties array with an invalid first element succeeded, want a decode error")
	}

	const ceiling = 8 << 20 // 8 MiB — far below the 50+ MB an eager make([]PropertyWire, 2e6) costs
	if delta := m1.TotalAlloc - m0.TotalAlloc; delta > ceiling {
		t.Fatalf("decode allocated %d bytes for a %d-element hostile Properties claim that fails on element 0; want < %d "+
			"(eager make([]PropertyWire, n) amplification regression)", delta, hostileN, ceiling)
	}
}

// TestDecodeIntSlice_RealisticSizeStillDecodesCorrectly proves the fix does
// not regress the common case: a legitimate small ExtraLabels array (well
// under wireArrayPreallocCap) still decodes to the exact values.
func TestDecodeIntSlice_RealisticSizeStillDecodesCorrectly(t *testing.T) {
	// fixmap(1) { "el": [1, 2, 3] } — fixarray(3) of fixint elements.
	data := []byte{0x81, 0xa2, 'e', 'l', 0x93, 0x01, 0x02, 0x03}
	var w NodeWire
	if err := SafeUnmarshal(data, &w); err != nil {
		t.Fatalf("SafeUnmarshal: %v", err)
	}
	if len(w.ExtraLabels) != 3 || w.ExtraLabels[0] != 1 || w.ExtraLabels[1] != 2 || w.ExtraLabels[2] != 3 {
		t.Fatalf("ExtraLabels = %v, want [1 2 3]", w.ExtraLabels)
	}
}
