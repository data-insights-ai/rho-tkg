package badger

import (
	"fmt"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BenchmarkRelHistory / BenchmarkNodeHistory measure GetRelHistory /
// GetNodeHistory for a PLAIN entity (no history) in a reopened on-disk store of
// 200 K entities where a fraction ("density") of the entities holds three
// history rows. Backlog 20 / handover 1a: without opts.Prefix the iterator
// prefetched the values of other entities' history rows, 5.9 us (density 0) to
// 57 us (0.1 %) per call against ~1.7 us with the prefix; target <= 2 us flat.
//
//	go test ./pkg/graph/store/badger/ -run '^$' -bench 'Benchmark(Rel|Node)History$' -benchmem
const historyBenchEntities = 200_000

var historyBenchDensities = []struct {
	name   string
	stride int // every stride-th entity holds history; 0 = none
}{
	{"density=0", 0},
	{"density=0.1%", 1000},
	{"density=1%", 100},
}

// seedHistoryBenchStore writes 200 K current rows plus history rows on every
// stride-th entity, closes the store and reopens it so reads hit SSTables.
func seedHistoryBenchStore(b *testing.B, node bool, stride int) *Store {
	b.Helper()
	return seedHistoryBenchStoreN(b, node, stride, historyBenchEntities)
}

// seedHistoryBenchRows writes entities current rows plus three history rows
// on every stride-th entity (stride 0 = none).
func seedHistoryBenchRows(bs *Store, node bool, stride, entities int) error {
	const batch = 5000
	if !node {
		for _, id := range []int{1, 2} { // rel endpoints
			if err := bs.PutNode(types.NewNode(types.NodeID(snowflake.ID(id)), 1, nil)); err != nil {
				return fmt.Errorf("PutNode: %w", err)
			}
		}
	}
	for lo := 1; lo <= entities; lo += batch {
		if node {
			ns := make([]*types.Node, 0, batch)
			for i := lo; i < lo+batch && i <= entities; i++ {
				ns = append(ns, types.NewNode(types.NodeID(snowflake.ID(i)), 1, nil))
			}
			if err := bs.PutNodesBatch(ns); err != nil {
				return fmt.Errorf("PutNodesBatch: %w", err)
			}
		} else {
			rs := make([]*types.Relationship, 0, batch)
			for i := lo; i < lo+batch && i <= entities; i++ {
				rs = append(rs, types.NewRelationship(types.RelID(snowflake.ID(i)), 5,
					types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(2))))
			}
			if err := bs.PutRelationshipsBatch(rs); err != nil {
				return fmt.Errorf("PutRelationshipsBatch: %w", err)
			}
		}
	}
	if stride <= 0 {
		return nil
	}
	for i := stride; i <= entities; i += stride {
		for ver := uint32(0); ver < 3; ver++ {
			if node {
				n := types.NewNode(types.NodeID(snowflake.ID(i)), 1, nil)
				n.SetVersion(ver)
				if err := bs.PutNodeVersion(n.ID(), ver, n); err != nil {
					return fmt.Errorf("PutNodeVersion: %w", err)
				}
			} else {
				r := types.NewRelationship(types.RelID(snowflake.ID(i)), 5,
					types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(2)))
				r.SetVersion(ver)
				if err := bs.PutRelVersion(r.ID(), ver, r); err != nil {
					return fmt.Errorf("PutRelVersion: %w", err)
				}
			}
		}
	}
	return nil
}

// plainHistoryBenchIDs returns 1024 ids spread over the key space that hold no
// history for the given stride (never a multiple of the stride).
func plainHistoryBenchIDs(stride int) []int64 {
	return plainHistoryBenchIDsN(stride, historyBenchEntities)
}

func benchHistory(b *testing.B, node bool) {
	for _, d := range historyBenchDensities {
		b.Run(d.name, func(b *testing.B) {
			bs := seedHistoryBenchStore(b, node, d.stride)
			ids := plainHistoryBenchIDs(d.stride)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				id := ids[i%len(ids)]
				var n int
				var err error
				if node {
					var h []*types.Node
					h, err = bs.GetNodeHistory(types.NodeID(snowflake.ID(id)))
					n = len(h)
				} else {
					var h []*types.Relationship
					h, err = bs.GetRelHistory(types.RelID(snowflake.ID(id)))
					n = len(h)
				}
				if err != nil || n != 0 {
					b.Fatalf("plain entity %d: %d rows, err %v", id, n, err)
				}
			}
		})
	}
}

func BenchmarkRelHistory(b *testing.B)  { benchHistory(b, false) }
func BenchmarkNodeHistory(b *testing.B) { benchHistory(b, true) }
