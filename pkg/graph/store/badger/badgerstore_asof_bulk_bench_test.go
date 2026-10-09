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
// row ("none"), one ("one") or three ("three") history rows each, all below the
// current version. NodesAsOf/RelsAsOf look for a
// row above the current version per entity (the pin-stable as-of rule); the
// history presence set (highest history version per ID) answers it without a
// key read.
//
//	go test ./pkg/graph/store/badger/ -run '^$' -bench 'AsOfBulk$' -benchmem
func BenchmarkNodesAsOfBulk(b *testing.B) { benchAsOfBulk(b, true) }
func BenchmarkRelsAsOfBulk(b *testing.B)  { benchAsOfBulk(b, false) }

const asOfBulkBenchEntities = 20_000

func benchAsOfBulk(b *testing.B, node bool) {
	for _, history := range []int{0, 1, 3} {
		name := map[int]string{0: "history=none", 1: "history=one", 3: "history=three"}[history]
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

func seedAsOfBulkBench(b *testing.B, bs *Store, node bool, history int) {
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

// putAsOfBulkBenchEntity writes one entity: `history` rows (versions 0..) below
// the current row, which carries version `history`; version v has TxFrom
// 10+10*v. Each Replace moves the previous current row into history.
func putAsOfBulkBenchEntity(bs *Store, node bool, history int, id int64) error {
	start, end := types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(2))
	var prevN *types.Node
	var prevR *types.Relationship
	for v := 0; v <= history; v++ {
		tm := &types.TemporalMetadata{TxFrom: types.Instant(10 + 10*v)}
		var err error
		if node {
			n := types.NewNode(types.NodeID(snowflake.ID(id)), 1, nil)
			n.SetVersion(uint32(v))
			n.SetTemporal(tm)
			if v == 0 {
				err = bs.PutNode(n)
			} else {
				err = bs.ReplaceNodeWithHistory(n, uint32(v-1), prevN)
			}
			prevN = n
		} else {
			r := types.NewRelationship(types.RelID(snowflake.ID(id)), 1, start, end)
			r.SetVersion(uint32(v))
			r.SetTemporal(tm)
			if v == 0 {
				err = bs.PutRelationship(r)
			} else {
				err = bs.ReplaceRelWithHistory(r, uint32(v-1), prevR)
			}
			prevR = r
		}
		if err != nil {
			return err
		}
	}
	return nil
}
