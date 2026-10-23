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
	t.Parallel()
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

	// Strings on one part and numbers on another: a mixed vector (round 10),
	// each row tagged with its own kind, codes through the merged dictionary.
	mixed := []types.NodeColumnReader{
		chunkPart(t, 100, keys, []map[string]any{{"n": int64(1)}, {"n": 2.5}, {}}),
		chunkPart(t, 200, keys, []map[string]any{{"n": "x"}, {"n": "y"}}),
		chunkPart(t, 300, keys, []map[string]any{{"n": "y"}, {}}),
		chunkPart(t, 400, keys, []map[string]any{{"n": int64(-4)}}),
	}
	mc, ok := NewChunkConcat(mixed)
	if !ok || mc.kinds[0] != types.ColumnVectorMixed {
		t.Fatalf("strings and numbers: ok=%v kinds %v, want mixed", ok, mc.kinds)
	}
	mc.ReadChunk(0, mc.Len(), &chunk)
	m := chunk.Cols[0]
	wantMixed := []any{int64(1), 2.5, nil, "x", "y", "y", nil, int64(-4)}
	for i, w := range wantMixed {
		var got any
		if m.Present[i] {
			switch m.Tags[i] {
			case types.ColumnVectorInt:
				got = m.Ints[i]
			case types.ColumnVectorFloat:
				got = m.Flts[i]
			case types.ColumnVectorString:
				got = m.Dict[m.Codes[i]]
			}
		} else if m.Tags[i] != 0 {
			t.Fatalf("absent row %d tagged %d", i, m.Tags[i])
		}
		if fmt.Sprintf("%T %v", got, got) != fmt.Sprintf("%T %v", w, w) {
			t.Fatalf("mixed row %d: %T %v, want %T %v", i, got, got, w, w)
		}
	}
	if len(m.Dict) != 2 || m.Codes[4] != m.Codes[5] {
		t.Fatalf("mixed dictionary %v codes %v: one code per term", m.Dict, m.Codes)
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

// TestChunkConcatOrdinals (round 10): a concatenation of ordinal snapshots
// fills ColumnChunk.Ordinals across part boundaries; one plain part leaves
// every chunk without ordinals; a plain snapshot clears a reused chunk's
// ordinals; NewOrdinalSnapshot refuses ordinals of another length.
func TestChunkConcatOrdinals(t *testing.T) {
	t.Parallel()
	keys := []string{"n"}
	part := func(base int64, n int, ord0 uint32) (*OrdinalSnapshot, *PointSnapshot[types.NodeID]) {
		rows := make([]map[string]any, n)
		ords := make([]uint32, n)
		for k := range rows {
			rows[k] = map[string]any{"n": int64(k)}
			ords[k] = ord0 + uint32(k)
		}
		p := chunkPart(t, base, keys, rows).(*PointSnapshot[types.NodeID])
		os, ok := NewOrdinalSnapshot(p, ords)
		if !ok {
			t.Fatal("ordinals refused")
		}
		return os, p
	}
	a, _ := part(100, 3, 10)
	b, bPlain := part(200, 2, 50)
	c, ok := NewChunkConcat([]types.NodeColumnReader{a, nil, b})
	if !ok {
		t.Fatal("ordinal parts declined")
	}
	var chunk types.ColumnChunk
	c.ReadChunk(1, 5, &chunk)
	if want := []uint32{11, 12, 50, 51}; fmt.Sprint(chunk.Ordinals) != fmt.Sprint(want) {
		t.Fatalf("ordinals %v, want %v", chunk.Ordinals, want)
	}
	a.ReadChunk(0, 2, &chunk)
	if fmt.Sprint(chunk.Ordinals) != "[10 11]" {
		t.Fatalf("single ordinal snapshot: %v", chunk.Ordinals)
	}
	plain, ok := NewChunkConcat([]types.NodeColumnReader{a, bPlain})
	if !ok {
		t.Fatal("mixed ordinal and plain parts declined")
	}
	if !c.HasOrdinals() || plain.HasOrdinals() {
		t.Fatalf("HasOrdinals: ordinal parts %v, with a plain part %v", c.HasOrdinals(), plain.HasOrdinals())
	}
	plain.ReadChunk(0, plain.Len(), &chunk)
	if len(chunk.Ordinals) != 0 {
		t.Fatalf("a plain part: %d ordinals handed out", len(chunk.Ordinals))
	}
	a.ReadChunk(0, 3, &chunk)
	bPlain.ReadChunk(0, 1, &chunk)
	if len(chunk.Ordinals) != 0 {
		t.Fatal("a plain snapshot left a reused chunk's ordinals")
	}
	if _, ok := NewOrdinalSnapshot(bPlain, []uint32{1}); ok {
		t.Fatal("ordinals of another length accepted")
	}
	if _, ok := NewOrdinalSnapshot(nil, nil); ok {
		t.Fatal("a nil snapshot accepted")
	}
}
