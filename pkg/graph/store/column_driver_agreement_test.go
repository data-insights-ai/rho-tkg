package store

import (
	"errors"
	"fmt"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestColumnDrivers_NodeAndRelAgree is the anti-drift guarantee for the per-value
// loop that ScanColumnsFromNodes and ScanColumnsFromRels each write out.
//
// Sharing that loop cost 10-23% (see the note in column_batch_build.go), so the two
// copies are deliberate and this test is what keeps them honest: identical property
// values must produce byte-identical columns and the identical refusal through both
// drivers. It is strictly stronger than shared code, which can only guarantee the
// copies are the same — not that either is right — and would happily compile a
// change that broke both.
func TestColumnDrivers_NodeAndRelAgree(t *testing.T) {
	props := []string{"a", "b", "c"}

	for _, tc := range []struct {
		name    string
		rows    []map[string]any
		wantErr error
	}{
		{
			name: "typed_mix_and_absent",
			rows: []map[string]any{
				{"a": int64(1), "b": "x", "c": true},
				{"a": int64(2), "b": "y"},            // c absent
				{"b": "z", "c": false},               // a absent
				{"a": int64(4), "b": "w", "c": true}, //
				{"a": int64(5)},                      // b, c absent
			},
		},
		{
			// String-versus-number is reported ABSENT, not refused: reading a string
			// as a zero int would be worse than a missing row.
			name: "string_vs_number_is_absent",
			rows: []map[string]any{
				{"a": int64(1), "b": "x", "c": true},
				{"a": "not-a-number", "b": "y", "c": false},
			},
		},
		{
			// int64-versus-float64 REFUSES: the same logical property is routinely
			// stored both ways, and widening changes what a consumer's equality test
			// matches.
			name: "mixed_numeric_refuses",
			rows: []map[string]any{
				{"a": int64(1), "b": "x", "c": true},
				{"a": 2.5, "b": "y", "c": false},
			},
			wantErr: ErrMixedNumericColumn,
		},
		{
			// Integral widenings the drivers must treat identically.
			name: "widened_integrals",
			rows: []map[string]any{
				{"a": int32(7), "b": "x", "c": true},
				{"a": 9, "b": "y", "c": false},
				// types.Instant is deliberately absent: SetProperty rejects it, so it
				// reaches classifyScalar only through internal paths and cannot be
				// staged here.
			},
		},
		{
			name: "all_absent",
			rows: []map[string]any{{}, {}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			nodes := make([]*types.Node, len(tc.rows))
			rels := make([]*types.Relationship, len(tc.rows))
			for i, r := range tc.rows {
				n := types.NewNode(types.NodeID(i+1), 1, nil)
				rel := types.NewRelationship(types.RelID(i+1), 1, types.NodeID(1), types.NodeID(2))
				for k, v := range r {
					if err := n.SetProperty(k, v); err != nil {
						t.Fatalf("node SetProperty %s: %v", k, err)
					}
					if err := rel.SetProperty(k, v); err != nil {
						t.Fatalf("rel SetProperty %s: %v", k, err)
					}
				}
				meta := &types.TemporalMetadata{ValidFrom: types.Instant(i + 1)}
				n.SetTemporal(meta)
				rel.SetTemporal(meta)
				nodes[i], rels[i] = n, rel
			}

			var nodeCols, relCols string
			nodeErr := ScanColumnsFromNodes(nodes, props, func(b *ColumnBatch) bool {
				nodeCols += renderColumns(&b.ColumnData, len(props), len(b.IDs))
				return true
			})
			relErr := ScanColumnsFromRels(rels, props, func(b *RelColumnBatch) bool {
				relCols += renderColumns(&b.ColumnData, len(props), len(b.IDs))
				return true
			})

			if !errors.Is(nodeErr, relErr) && !errors.Is(relErr, nodeErr) {
				t.Fatalf("drivers disagree on the ERROR: node=%v rel=%v", nodeErr, relErr)
			}
			if tc.wantErr != nil {
				if !errors.Is(nodeErr, tc.wantErr) {
					t.Fatalf("node driver: got err=%v, want %v", nodeErr, tc.wantErr)
				}
				if !errors.Is(relErr, tc.wantErr) {
					t.Fatalf("rel driver: got err=%v, want %v", relErr, tc.wantErr)
				}
				return
			}
			if nodeErr != nil {
				t.Fatalf("unexpected node error: %v", nodeErr)
			}
			if nodeCols != relCols {
				t.Errorf("drivers produced DIFFERENT columns for identical values:\n"+
					" node: %s\n  rel: %s", nodeCols, relCols)
			}
		})
	}
}

// renderColumns flattens a batch's typed columns into a comparable string, including
// each column's resolved KIND and the null flags — the two things a drifting copy
// would get wrong without changing row counts.
func renderColumns(cd *ColumnData, nCols, nRows int) string {
	out := ""
	for c := range nCols {
		out += fmt.Sprintf("[col%d kind=%d", c, cd.Kinds[c])
		ii := 0
		for r := range nRows {
			if cd.Null[c][r] {
				out += " null"
				ii++
				continue
			}
			switch cd.Kinds[c] {
			case ColInt64:
				out += fmt.Sprintf(" i:%d", cd.Ints[c][ii])
			case ColFloat64:
				out += fmt.Sprintf(" f:%v", cd.Flts[c][ii])
			case ColString:
				out += fmt.Sprintf(" s:%s", cd.Strs[c][ii])
			case ColBool:
				out += fmt.Sprintf(" b:%v", cd.Bools[c][ii])
			}
			ii++
		}
		out += "]"
	}
	return out
}
