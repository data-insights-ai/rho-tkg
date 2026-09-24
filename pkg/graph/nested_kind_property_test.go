package graph_test

import (
	"bytes"
	"context"
	"math"
	"reflect"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Backlog item 3, end to end: a property value NESTED in an []any /
// map[string]any keeps its exact Go kind through the badger store (write,
// close, reopen, read) and through export/import of a memory-store graph, and
// the hash chain still verifies. Before the fix a nested int16 read back as
// int64 (and int as int64, uint8 as uint64, []string as []any, typed nil as
// nil ...), so VerifyRelChain / VerifyNodeChain returned false after a reopen.

func nestedKindGraphValues() map[string]any {
	return map[string]any{
		"int": int(-5), "int8": int8(math.MinInt8), "int16": int16(2), "int32": int32(math.MaxInt32),
		"int64": int64(math.MinInt64),
		"uint":  uint(7), "uint8": uint8(math.MaxUint8), "uint16": uint16(2), "uint32": uint32(math.MaxUint32),
		"uint64":  uint64(math.MaxUint64),
		"float32": float32(1.25), "float64": 2.5,
		"[]string": []string{"a"}, "[]int": []int{1}, "[]int64": []int64{1}, "[]float32": []float32{1.5},
		"[]float64": []float64{1.5}, "[]bool": []bool{true}, "[]byte": []byte{1},
		"map[string]string": map[string]string{"a": "b"},
		"[]string(nil)":     []string(nil), "[]any(nil)": []any(nil), "map[string]any(nil)": map[string]any(nil),
	}
}

func nestedKindGraphWraps() map[string]func(any) any {
	return map[string]func(any) any{
		"slice":       func(v any) any { return []any{v} },
		"map":         func(v any) any { return map[string]any{"k": v} },
		"slice/slice": func(v any) any { return []any{[]any{v}} },
		"map/slice":   func(v any) any { return map[string]any{"k": []any{v}} },
		"slice/map":   func(v any) any { return []any{map[string]any{"k": v}} },
	}
}

type nestedKindEntity struct {
	name string
	want any
	node types.NodeID
	rel  types.RelID
}

// writeNestedKindEntities stores one node and one relationship per (kind,
// wrapper) so a failure names the exact kind and depth.
func writeNestedKindEntities(t *testing.T, g *graphpkg.Graph) []nestedKindEntity {
	t.Helper()
	ctx := context.Background()
	var out []nestedKindEntity
	for kind, v := range nestedKindGraphValues() {
		for wname, wrap := range nestedKindGraphWraps() {
			want := wrap(v)
			a, err := g.Nodes().Add(ctx, []string{"N"}, map[string]any{"p": want})
			if err != nil {
				t.Fatalf("%s/%s: node Add: %v", kind, wname, err)
			}
			b, err := g.Nodes().Add(ctx, []string{"N"}, nil)
			if err != nil {
				t.Fatalf("%s/%s: node Add: %v", kind, wname, err)
			}
			r, err := g.Rels().Add(ctx, "R", a, b, map[string]any{"p": want})
			if err != nil {
				t.Fatalf("%s/%s: rel Add: %v", kind, wname, err)
			}
			out = append(out, nestedKindEntity{name: kind + "/" + wname, want: want, node: a.ID(), rel: r.ID()})
		}
	}
	return out
}

func checkNestedKindEntities(t *testing.T, g *graphpkg.Graph, es []nestedKindEntity) {
	t.Helper()
	ctx := context.Background()
	for _, e := range es {
		n, err := g.Nodes().Get(ctx, e.node)
		if err != nil {
			t.Fatalf("%s: node Get: %v", e.name, err)
		}
		if v, _ := n.GetProperty("p"); !reflect.DeepEqual(v, e.want) {
			t.Errorf("%s: node property read back as %#v, want %#v", e.name, v, e.want)
		}
		if ok, err := g.Hash().VerifyNodeChain(e.node); err != nil || !ok {
			t.Errorf("%s: VerifyNodeChain = (%v, %v), want (true, nil)", e.name, ok, err)
		}
		r, err := g.Rels().Get(ctx, e.rel)
		if err != nil {
			t.Fatalf("%s: rel Get: %v", e.name, err)
		}
		if v, _ := r.GetProperty("p"); !reflect.DeepEqual(v, e.want) {
			t.Errorf("%s: rel property read back as %#v, want %#v", e.name, v, e.want)
		}
		if ok, err := g.Hash().VerifyRelChain(e.rel); err != nil || !ok {
			t.Errorf("%s: VerifyRelChain = (%v, %v), want (true, nil)", e.name, ok, err)
		}
	}
}

func TestNestedKinds_BadgerReopenKeepsKindAndChain(t *testing.T) {
	dir := t.TempDir()
	g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 5, BadgerDir: dir})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	es := writeNestedKindEntities(t, g)
	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	g2, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 5, BadgerDir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer g2.Close()
	checkNestedKindEntities(t, g2, es)
}

func TestNestedKinds_MemoryExportImportKeepsKindAndChain(t *testing.T) {
	g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 6, Store: memory.New()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()
	es := writeNestedKindEntities(t, g)

	var buf bytes.Buffer
	if err := g.IO().Export(&buf); err != nil {
		t.Fatalf("Export: %v", err)
	}
	g2, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 7, Store: memory.New()})
	if err != nil {
		t.Fatalf("New g2: %v", err)
	}
	defer g2.Close()
	if err := g2.IO().Import(bytes.NewReader(buf.Bytes()), tkgio.ImportOptions{}); err != nil {
		t.Fatalf("Import: %v", err)
	}
	checkNestedKindEntities(t, g2, es)
}
