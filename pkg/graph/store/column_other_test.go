package store

import (
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestColumnData_OtherPerBatch pins Other's lifetime across the batches of one
// scan (backlog 48): nil on a batch without an Other cell — no allocation on the
// common path, and no mark carried over from an earlier batch.
func TestColumnData_OtherPerBatch(t *testing.T) {
	for _, tc := range []struct {
		name  string
		first any   // row 0's value; every other row holds an int64
		want  []int // per batch: Other cells, -1 = Other must be nil
	}{
		{"no_other_cell", int64(0), []int{-1, -1}},
		{"other_in_first_batch_only", []any{int64(1)}, []int{1, -1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			nodes := make([]*types.Node, ColumnScanBatchRows+1)
			for i := range nodes {
				nodes[i] = types.NewNode(types.NodeID(i+1), 1, nil)
				v := any(int64(i))
				if i == 0 {
					v = tc.first
				}
				if err := nodes[i].SetProperty("x", v); err != nil {
					t.Fatal(err)
				}
			}
			var got []int
			if err := ScanColumnsFromNodes(nodes, []string{"x"}, func(b *ColumnBatch) bool {
				if b.Other == nil {
					got = append(got, -1)
					return true
				}
				n := 0
				for _, o := range b.Other[0] {
					if o {
						n++
					}
				}
				got = append(got, n)
				return true
			}); err != nil {
				t.Fatal(err)
			}
			if len(got) != len(tc.want) || got[0] != tc.want[0] || got[1] != tc.want[1] {
				t.Fatalf("Other cells per batch = %v, want %v", got, tc.want)
			}
		})
	}
}
