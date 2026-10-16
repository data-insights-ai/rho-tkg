package graph_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5compat/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v5compat/pkg/graph/store"
)

// Bytes and flat lists (WP-29) round-trip through the façade and are found by
// the equality door; a nil element and a '~' key are refused.
func TestBytesAndListsThroughTheFacade(t *testing.T) {
	ctx := context.Background()
	g, err := graph.New(graph.Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	n, err := g.Nodes().Add(ctx, []string{"V"}, map[string]any{"b": []byte("xy"), "l": []int64{1, 2}, "s": "xy"})
	if err != nil {
		t.Fatal(err)
	}
	got, err := g.Nodes().Get(ctx, n.ID())
	if err != nil {
		t.Fatal(err)
	}
	p := got.PropertiesMap()
	if !reflect.DeepEqual(p["b"], []byte("xy")) || !reflect.DeepEqual(p["l"], []any{int64(1), int64(2)}) || p["s"] != "xy" {
		t.Fatalf("read back %#v", p)
	}
	for _, c := range []struct {
		key string
		v   any
		n   int
	}{{"b", []byte("xy"), 1}, {"b", "xy", 0}, {"s", []byte("xy"), 0}, {"l", []any{1, 2}, 1}, {"l", []int64{2, 1}, 0}} {
		ns, err := g.Nodes().ByLabelAndProperty("V", c.key, c.v, store.QueryOpts{})
		if err != nil || len(ns) != c.n {
			t.Fatalf("lookup %s=%#v: %d rows, %v; want %d", c.key, c.v, len(ns), err, c.n)
		}
	}
	if _, err := g.Nodes().Add(ctx, []string{"V"}, map[string]any{"l": []any{1, nil}}); err == nil {
		t.Fatal("nil list element accepted")
	}
	_, err = g.Nodes().Add(ctx, []string{"V"}, map[string]any{"a~b": 1})
	if !errors.Is(err, graph.ErrReservedKey) {
		t.Fatalf("'~' key: %v", err)
	}
	if ns, _ := g.Nodes().ByLabel("V", store.QueryOpts{}); len(ns) != 1 {
		t.Fatalf("refused writes left nodes: %d", len(ns))
	}
}
