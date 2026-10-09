package graph_test

import (
	"strconv"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
)

// BenchmarkDurableCommit measures GraphTx commit latency on a disk badger store
// with Config.DurableCommit off (today: no flush on commit) and on (one
// WriteBatch + one WAL fsync per commit). Each op is one transaction of
// nodesPerTx creates. b.TempDir is on the host's real disk (ext4 on NVMe on the
// reference machine), so the "on" numbers include a real fsync.
func BenchmarkDurableCommit(b *testing.B) {
	for _, nodesPerTx := range []int{1, 10, 100} {
		for _, durable := range []bool{false, true} {
			name := "off"
			if durable {
				name = "on"
			}
			b.Run(name+"/nodes="+strconv.Itoa(nodesPerTx), func(b *testing.B) {
				g, err := graphpkg.New(graphpkg.Config{BadgerDir: b.TempDir(), DurableCommit: durable})
				if err != nil {
					b.Fatalf("New: %v", err)
				}
				defer func() { _ = g.Close() }()
				props := map[string]any{"k": "v"}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if err := g.Tx().Run(func(tx *graphpkg.GraphTx) error {
						for j := 0; j < nodesPerTx; j++ {
							if _, err := tx.AddNode([]string{"Signal"}, props); err != nil {
								return err
							}
						}
						return nil
					}); err != nil {
						b.Fatalf("Run: %v", err)
					}
				}
			})
		}
	}
}
