package store

import (
	"fmt"
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

// noOtherSource is a RelColumnSource written before ColumnData.Other existed:
// no Other method.
type noOtherSource struct{ src relSliceSource }

func (s noOtherSource) Len() int { return s.src.Len() }

func (s noOtherSource) Row(i int) (types.RelID, types.NodeID, types.NodeID, int64, int64) {
	return s.src.Row(i)
}

func (s noOtherSource) Value(i, c int) (ColumnKind, int64, float64, string, bool, bool) {
	return s.src.Value(i, c)
}

// TestScanColumnsFromRelSource_OtherOptional pins that Other is optional for a
// RelColumnSource: a source without it still scans with the same Null, and
// reports no Other at all (not a partial one from the driver's kind check).
func TestScanColumnsFromRelSource_OtherOptional(t *testing.T) {
	vals := []any{[]any{int64(1)}, int64(5), "five"} // held list, int, kind mismatch
	rels := make([]*types.Relationship, len(vals))
	for i, v := range vals {
		rels[i] = types.NewRelationship(types.RelID(i+1), 1, 1, 2)
		if err := rels[i].SetProperty("x", v); err != nil {
			t.Fatal(err)
		}
	}
	scan := func(src RelColumnSource) (null, other string) {
		if err := ScanColumnsFromRelSource(src, 1, func(b *RelColumnBatch) bool {
			null, other = fmt.Sprint(b.Null), fmt.Sprint(b.Other)
			return true
		}); err != nil {
			t.Fatal(err)
		}
		return null, other
	}
	base := relSliceSource{rels, []string{"x"}}
	for _, tc := range []struct {
		name      string
		src       RelColumnSource
		wantOther string
	}{
		{"with_other", base, "[[true false true]]"},
		{"without_other", noOtherSource{base}, "[]"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			null, other := scan(tc.src)
			if null != "[[true false true]]" || other != tc.wantOther {
				t.Fatalf("Null=%s Other=%s, want Null=[[true false true]] Other=%s", null, other, tc.wantOther)
			}
		})
	}
}
