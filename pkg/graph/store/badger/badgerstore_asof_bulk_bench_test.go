package badger

import (
	"fmt"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BenchmarkNodesAsOfBulk / BenchmarkRelsAsOfBulk measure the bulk as-of doors
// over 20 K entities whose current row is visible at the pin, with no history
// row ("none") or one history row each ("one"). NodesAsOf/RelsAsOf look for a
// row above the current version per entity (the pin-stable as-of rule); the
// history presence set answers "none" without a key read.
//
//	go test ./pkg/graph/store/badger/ -run '^$' -bench 'AsOfBulk$' -benchmem
func BenchmarkNodesAsOfBulk(b *testing.B) { benchAsOfBulk(b, true) }
func BenchmarkRelsAsOfBulk(b *testing.B)  { benchAsOfBulk(b, false) }

const asOfBulkBenchEntities = 20_000

func benchAsOfBulk(b *testing.B, node bool) {
	for _, history := range []bool{false, true} {
		name := "history=none"
		if history {
			name = "history=one"
		}
		b.Run(name, func(b *testing.B) {
			bs, err := New(Config{Dir: b.TempDir(), FlushInterval: time.Hour})
			if err != nil {
				b.Fatalf("open: %v", err)
			}
			b.Cleanup(func() { _ = bs.Close() })
			seedAsOfBulkBench(b, bs, node, history)
			pin := types.Instant(250)
			run := func() (int, error) {
				if node {
					got, err := bs.NodesAsOf(pin)
					return len(got), err
				}
				got, err := bs.RelsAsOf(pin)
				return len(got), err
			}
			if n, err := run(); err != nil || n != asOfBulkBenchEntities { // warm-up (builds the presence set)
				b.Fatalf("warm-up: %d entities, %v", n, err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if n, err := run(); err != nil || n != asOfBulkBenchEntities {
					b.Fatalf("%d entities, %v", n, err)
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/asOfBulkBenchEntities, "ns/entity")
		})
	}
}

func seedAsOfBulkBench(b *testing.B, bs *Store, node, history bool) {
	b.Helper()
	if !node {
		for _, ep := range []types.NodeID{types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(2))} {
			if err := bs.PutNode(types.NewNode(ep, 1, nil)); err != nil {
				b.Fatalf("endpoint: %v", err)
			}
		}
	}
	for i := 0; i < asOfBulkBenchEntities; i++ {
		id := int64(100 + i)
		if err := putAsOfBulkBenchEntity(bs, node, history, id); err != nil {
			b.Fatal(fmt.Errorf("seed %d: %w", id, err))
		}
	}
	if err := bs.Flush(); err != nil {
		b.Fatalf("flush: %v", err)
	}
}

// putAsOfBulkBenchEntity writes one entity: version 0 (TxFrom 100) as the
// current row, or, with history, version 0 replaced by version 1 (TxFrom 200).
func putAsOfBulkBenchEntity(bs *Store, node, history bool, id int64) error {
	if node {
		nid := types.NodeID(snowflake.ID(id))
		v0 := types.NewNode(nid, 1, nil)
		v0.SetTemporal(&types.TemporalMetadata{TxFrom: 100})
		if err := bs.PutNode(v0); err != nil || !history {
			return err
		}
		v1 := types.NewNode(nid, 1, nil)
		v1.SetVersion(1)
		v1.SetTemporal(&types.TemporalMetadata{TxFrom: 200})
		return bs.ReplaceNodeWithHistory(v1, 0, v0)
	}
	rid := types.RelID(snowflake.ID(id))
	start, end := types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(2))
	v0 := types.NewRelationship(rid, 1, start, end)
	v0.SetTemporal(&types.TemporalMetadata{TxFrom: 100})
	if err := bs.PutRelationship(v0); err != nil || !history {
		return err
	}
	v1 := types.NewRelationship(rid, 1, start, end)
	v1.SetVersion(1)
	v1.SetTemporal(&types.TemporalMetadata{TxFrom: 200})
	return bs.ReplaceRelWithHistory(v1, 0, v0)
}
