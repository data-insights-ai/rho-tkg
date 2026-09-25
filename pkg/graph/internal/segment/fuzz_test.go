package segment

import (
	"encoding/binary"
	"hash/crc32"
	"runtime"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// FuzzOpen feeds arbitrary bytes to the decode boundary (lessons 47, 48):
// Open and every reader door must return a value or an error, never panic,
// and Open must not allocate in proportion to a count the bytes only claim.
func FuzzOpen(f *testing.F) {
	for i, n := range []int{0, 1, 7, 70} {
		rows := corpus(f, n, uint64(100+i))
		for _, pr := range []int{4, 64} {
			data, err := encode(testSchema(), rows, Options{IntegrityBlockRows: 2}, pr, nil)
			if err != nil {
				f.Fatal(err)
			}
			f.Add(data)
		}
	}
	f.Add([]byte{})
	f.Add([]byte("TKGSEG\x00\x01"))
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
		rows := 0
		_ = seg.Scan(func(int, *types.Relationship) bool { rows++; return rows < 20000 })
		for i := 0; i < seg.Len() && i < 64; i++ {
			r, err := seg.Row(i)
			if err == nil {
				_, _ = seg.Lookup(r.ID())
				_, _, _ = seg.OutRows(r.StartNodeID())
				_, _ = seg.InRows(r.EndNodeID())
			}
		}
		_ = seg.Verify()
	})
}

// FuzzOpenResealed mutates a valid segment and then recomputes every CRC
// (header, sections, footer), so the mutations reach the structural decoder
// below the CRC layer — an adversary can compute CRCs too.
func FuzzOpenResealed(f *testing.F) {
	for i, n := range []int{1, 9, 70} {
		data, err := encode(testSchema(), corpus(f, n, uint64(200+i)), Options{IntegrityBlockRows: 4}, 8, nil)
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
		rows := 0
		_ = seg.Scan(func(int, *types.Relationship) bool { rows++; return rows < 20000 })
		for i := 0; i < seg.Len() && i < 64; i++ {
			if r, err := seg.Row(i); err == nil {
				_, _ = seg.Lookup(r.ID())
				_, _, _ = seg.OutRows(r.StartNodeID())
				_, _ = seg.InRows(r.EndNodeID())
			}
		}
		_ = seg.Verify()
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
