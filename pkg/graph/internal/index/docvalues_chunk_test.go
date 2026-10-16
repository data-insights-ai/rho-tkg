package index

import (
	"fmt"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// chunkPart builds one part snapshot over ids [base, base+len(rows)) whose
// row k holds rows[k] (key -> value; a missing key is absent).
func chunkPart(t *testing.T, base int64, keys []string, rows []map[string]any) types.NodeColumnReader {
	t.Helper()
	ids := make([]types.NodeID, len(rows))
	for k := range rows {
		ids[k] = types.NodeID(base + int64(k))
	}
	l := BuildLabelDocValues(1, ids, keys, func(id types.NodeID, key string) (any, bool) {
		v, ok := rows[int64(id)-base][key]
		return v, ok
	}, nil)
	snap, ok := l.NewPointSnapshot(keys)
	if !ok {
		t.Fatalf("part at %d: columns not buildable", base)
	}
	return snap
}

// TestChunkConcatAcrossParts: a concatenation of parts reads each row as the
// part's RowAt does, with the kinds unified (an integer part and a float
// part make a Numeric column, a part without a value takes any kind) and the
// parts' string dictionaries merged (the same term one code; distinct terms
// distinct codes), across every part boundary; a string column against a
// numeric one declines.
func TestChunkConcatAcrossParts(t *testing.T) {
	keys := []string{"n", "s"}
	parts := []types.NodeColumnReader{
		chunkPart(t, 100, keys, []map[string]any{{"n": int64(1), "s": "a"}, {"n": int64(2)}, {"s": "b"}}),
		nil,
		chunkPart(t, 200, keys, []map[string]any{{"n": 2.5, "s": "b"}, {"n": 3.5, "s": "c"}}),
		chunkPart(t, 300, keys, []map[string]any{{}, {}}), // no value: any kind
		chunkPart(t, 400, keys, []map[string]any{{"n": int64(7), "s": "a"}, {"n": 0.25, "s": "z"}}),
	}
	c, ok := NewChunkConcat(parts)
	if !ok {
		t.Fatal("compatible parts declined")
	}
	if c.Len() != 9 {
		t.Fatalf("Len %d, want 9", c.Len())
	}
	if c.kinds[0] != types.ColumnVectorNumeric || c.kinds[1] != types.ColumnVectorString {
		t.Fatalf("kinds %v", c.kinds)
	}
	var want []string
	vals, present := make([]any, 2), make([]bool, 2)
	for _, p := range parts {
		if p == nil {
			continue
		}
		rr := p.(types.NodeColumnRowReader)
		for i := range rr.Len() {
			id := rr.RowAt(i, vals, present)
			want = append(want, fmt.Sprintf("%d %T %v %v", id, vals[0], vals[0], vals[1]))
		}
	}
	var chunk types.ColumnChunk
	for from := range 10 {
		for to := from; to <= 9; to++ {
			c.ReadChunk(from, to, &chunk)
			for i, id := range chunk.IDs {
				var n, s any
				col := chunk.Cols[0]
				if col.Present[i] {
					if col.IsFloat[i] {
						n = col.Flts[i]
					} else {
						n = col.Ints[i]
					}
				}
				if chunk.Cols[1].Present[i] {
					s = chunk.Cols[1].Dict[chunk.Cols[1].Codes[i]]
				}
				if got := fmt.Sprintf("%d %T %v %v", id, n, n, s); got != want[from+i] {
					t.Fatalf("[%d,%d) row %d: %s, want %s", from, to, i, got, want[from+i])
				}
			}
		}
	}
	if d := c.dicts[1]; len(d) != 4 {
		t.Fatalf("merged dictionary %v, want 4 distinct terms", d)
	}

	mixed := []types.NodeColumnReader{
		chunkPart(t, 100, keys, []map[string]any{{"n": int64(1)}}),
		chunkPart(t, 200, keys, []map[string]any{{"n": "x"}}),
	}
	if _, ok := NewChunkConcat(mixed); ok {
		t.Fatal("strings on one part and numbers on another: not declined")
	}
	if _, ok := NewChunkConcat([]types.NodeColumnReader{fakeReader{}}); ok {
		t.Fatal("a foreign reader: not declined")
	}
	empty, ok := NewChunkConcat(nil)
	if !ok || empty.Len() != 0 {
		t.Fatal("no parts: want an empty reader")
	}
	empty.ReadChunk(0, 0, &chunk)
	if len(chunk.IDs) != 0 || len(chunk.Cols) != 0 {
		t.Fatalf("empty reader chunk: %d rows %d cols", len(chunk.IDs), len(chunk.Cols))
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("out of range: no panic")
			}
		}()
		c.ReadChunk(0, 10, &chunk)
	}()
}

type fakeReader struct{}

func (fakeReader) Row(types.NodeID, []any, []bool) bool { return false }
func (fakeReader) Epoch() uint64                        { return 0 }
