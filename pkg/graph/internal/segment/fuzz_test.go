package segment

import (
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
			data, err := encode(testSchema(), rows, Options{IntegrityBlockRows: 2}, pr)
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
