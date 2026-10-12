package badger

import (
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BenchmarkReplaceNodeColumnRecord is the write cost of round 5 request 4:
// ReplaceNode of a member of a label nobody reads columns of (the removal
// seam poisons, as before) and of a label with a cached column snapshot (the
// seam records the node for the next read's patch). Each arm reads columns
// once per 1,000 writes (so the record stays below its cap), outside the
// timer.
func BenchmarkReplaceNodeColumnRecord(b *testing.B) {
	for _, arm := range []string{"unread", "snapshot"} {
		b.Run(arm, func(b *testing.B) {
			bs, err := New(Config{InMemory: true})
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = bs.Close() })
			const n = 10_000
			for i := int64(1); i <= n; i++ {
				nd := adNode(i, i)
				if err := bs.PutNode(nd); err != nil {
					b.Fatal(err)
				}
			}
			read := func() {
				if _, _, err := bs.ForEachDocValues(adLabel, []string{"qty"}, func(types.NodeID, []any, []bool) bool { return true }); err != nil {
					b.Fatal(err)
				}
			}
			if arm == "snapshot" {
				read()
			}
			i := 0
			for b.Loop() {
				id := int64(1 + i%n)
				if err := bs.ReplaceNode(adNode(id, int64(i))); err != nil {
					b.Fatal(err)
				}
				i++
				if arm == "snapshot" && i%1000 == 0 {
					b.StopTimer()
					read()
					b.StartTimer()
				}
			}
		})
	}
}
