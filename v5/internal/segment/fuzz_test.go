package segment

import (
	"encoding/binary"
	"hash/crc32"
	"runtime"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/pkg/types"
)

// Ported from v4 fuzz_test.go (FuzzOpen, FuzzOpenResealed, reseal).

// exercise drives every reader door over an opened segment, bounded.
func exercise(seg *Segment) {
	rows := 0
	_ = seg.Scan(func(int, *Row) bool { rows++; return rows < 20000 })
	for i := 0; i < seg.Len() && i < 64; i++ {
		if r, err := seg.Row(i); err == nil {
			_, _ = seg.Lookup(r.ID)
			_, _, _ = seg.OutRows(r.Start)
			_, _ = seg.InRows(r.End)
		}
	}
	b := 0
	_ = seg.ScanBatches(func(*Batch) bool { b++; return b < 8 })
	_ = seg.ForEachID(func(types.RelID, int) bool { return true })
	_ = seg.Verify()
}

// FuzzOpen feeds arbitrary bytes to the decode boundary: Open and every
// reader door must return a value or an error, never panic, and Open must not
// allocate in proportion to a count the bytes only claim.
func FuzzOpen(f *testing.F) {
	for i, n := range []int{0, 1, 7, 70} {
		rows := corpus(f, n, uint64(100+i))
		for _, pr := range []int{4, 64} {
			data, err := encode(testSchema(), rows, Options{IntegrityBlockRows: 2}, pr)
			if err != nil {
				f.Fatal(err)
			}
			f.Add(data)
		}
	}
	f.Add([]byte{})
	f.Add([]byte("TKG5SEG\x01"))
	f.Fuzz(func(t *testing.T, data []byte) {
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		seg, err := Open(data)
		runtime.ReadMemStats(&after)
		if d := after.TotalAlloc - before.TotalAlloc; d > 1<<16+32*uint64(len(data)) {
			t.Fatalf("Open allocated %d bytes for %d input bytes", d, len(data))
		}
		if err != nil {
			return
		}
		exercise(seg)
	})
}

// FuzzOpenResealed mutates a valid segment and then recomputes every CRC
// (header, sections, footer), so the mutations reach the structural decoder
// below the CRC layer: an adversary can compute CRCs too.
func FuzzOpenResealed(f *testing.F) {
	for i, n := range []int{1, 9, 70} {
		data, err := encode(testSchema(), corpus(f, n, uint64(200+i)), Options{IntegrityBlockRows: 4}, 8)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(data, uint32(0), byte(0))
	}
	f.Fuzz(func(t *testing.T, data []byte, at uint32, x byte) {
		if len(data) == 0 {
			return
		}
		data = append([]byte(nil), data...)
		data[int(at)%len(data)] ^= x
		reseal(data)
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		seg, err := Open(data)
		runtime.ReadMemStats(&after)
		if d := after.TotalAlloc - before.TotalAlloc; d > 1<<16+32*uint64(len(data)) {
			t.Fatalf("Open allocated %d bytes for %d input bytes", d, len(data))
		}
		if err != nil {
			return
		}
		exercise(seg)
	})
}

// FuzzEncodeRoundTrip builds rows from fuzz bytes (any IDs, coordinates,
// property values) and requires Encode to either refuse with a typed error or
// produce a segment that decodes to the sorted input and verifies. This is
// new in v5: v4 fuzzed only the decoder.
func FuzzEncodeRoundTrip(f *testing.F) {
	f.Add([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28})
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})
	f.Add(binary.LittleEndian.AppendUint64(binary.LittleEndian.AppendUint64(nil, 1<<63-1), 1<<63-1))
	f.Fuzz(func(t *testing.T, in []byte) {
		rows := rowsFromBytes(in)
		data, err := Encode(testSchema(), rows, Options{IntegrityBlockRows: 4})
		if err != nil {
			if !isTyped(err) {
				t.Fatalf("untyped error: %v", err)
			}
			return
		}
		seg, err := Open(data)
		if err != nil {
			t.Fatalf("own output rejected: %v", err)
		}
		if seg.Len() != len(rows) {
			t.Fatalf("Len %d, want %d", seg.Len(), len(rows))
		}
		want := byKey(rows)
		if err := seg.Scan(func(_ int, r *Row) bool {
			assertSameRow(t, want[rowKey{r.ID, r.Revision}], *r)
			return true
		}); err != nil {
			t.Fatal(err)
		}
		if err := seg.Verify(); err != nil {
			t.Fatal(err)
		}
	})
}

// reseal recomputes the header CRC, every section CRC the directory names and
// the footer CRC, in place, as far as the structure allows.
func reseal(data []byte) {
	if len(data) < headerSize+trailerSize {
		return
	}
	binary.LittleEndian.PutUint32(data[56:], crc32.Checksum(data[:56], castagnoli))
	n := len(data)
	flen := int(binary.LittleEndian.Uint32(data[n-trailerSize:]))
	if flen > n-headerSize-trailerSize {
		return
	}
	fs, fe := n-trailerSize-flen, n-trailerSize
	sealFooter := func() {
		binary.LittleEndian.PutUint32(data[n-trailerSize+4:], crc32.Checksum(data[fs:fe], castagnoli))
	}
	sealFooter()
	secs, err := sections(data)
	if err != nil {
		return
	}
	for _, s := range secs {
		binary.LittleEndian.PutUint32(data[s.crcAt:], crc32.Checksum(data[s.offset:s.offset+s.length], castagnoli))
	}
	sealFooter()
}
