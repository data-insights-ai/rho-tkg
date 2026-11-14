package badger

import (
	"strings"
	"testing"
	"time"
)

// The same source runs against the pre-index baseline. Eight ordinary
// versions per identity keep the measured append shape fixed; normal pending
// write bounds and backpressure are included in both measurements.
func BenchmarkHistoryAsOfOrdinaryAppend(b *testing.B) {
	for _, k := range asOfBudgetKinds() {
		b.Run(k.name, func(b *testing.B) {
			bs, err := New(Config{InMemory: true, FlushInterval: time.Hour, MaxPendingWrites: 4096})
			if err != nil {
				b.Fatal(err)
			}
			defer bs.Close()
			payload := strings.Repeat("x", 512)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := k.put(bs, int64(100+i/8), uint32(i%8), payload); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			if err := bs.Flush(); err != nil {
				b.Fatal(err)
			}
		})
	}
}
