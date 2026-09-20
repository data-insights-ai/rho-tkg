package graph_test

import (
	"bytes"
	"context"
	"reflect"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func nestedTemporalListFixture() []any {
	return []any{
		types.TemporalValue{Kind: types.TemporalDate, Value: "2024-01-01"},
		types.TemporalValue{Kind: types.TemporalDuration, Value: "P1DT2H"},
		types.TemporalValue{Kind: types.TemporalDateTime, Value: "2024-01-01T12:00:00+02:00"},
		types.TemporalValue{Kind: types.TemporalDateTime, Value: "2024-01-01T10:00:00Z"},
		"2024-01-01", // a genuine string that LOOKS like a date
		[]any{types.TemporalValue{Kind: types.TemporalLocalTime, Value: "12:30:00"}},
		map[string]any{"at": types.TemporalValue{Kind: types.TemporalTime, Value: "12:30:00Z"}},
	}
}

// End-to-end proof through the real Badger-backed graph: a list property with
// nested temporals is stored, read back with every kind intact, verifies
// against the hash chain, and survives export/import. Catches the erasure bug
// (elements come back as strings), a hash chain that cannot cover the nested
// value, and an export/import that drops the type.
func TestNode_NestedTemporalListRoundTrip(t *testing.T) {
	g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 11, BadgerInMemory: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()
	ctx := context.Background()

	want := nestedTemporalListFixture()
	n, err := g.Nodes().Add(ctx, []string{"Event"}, map[string]any{"stamps": want})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}

	got, err := g.Nodes().Get(ctx, n.ID())
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	v, ok := got.GetProperty("stamps")
	if !ok {
		t.Fatal("stamps property missing")
	}
	if !reflect.DeepEqual(v, want) {
		t.Fatalf("stamps read back as:\n got %#v\nwant %#v", v, want)
	}

	if ok, err := g.Hash().VerifyNodeChain(n.ID()); err != nil || !ok {
		t.Fatalf("VerifyNodeChain = (%v, %v), want (true, nil)", ok, err)
	}

	var buf bytes.Buffer
	if err := g.IO().Export(&buf); err != nil {
		t.Fatalf("Export: %v", err)
	}
	g2, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 12, BadgerInMemory: true})
	if err != nil {
		t.Fatalf("New g2: %v", err)
	}
	defer g2.Close()
	if err := g2.IO().Import(bytes.NewReader(buf.Bytes()), tkgio.ImportOptions{}); err != nil {
		t.Fatalf("Import: %v", err)
	}
	got2, err := g2.Nodes().Get(ctx, n.ID())
	if err != nil {
		t.Fatalf("Get after import: %v", err)
	}
	v2, _ := got2.GetProperty("stamps")
	if !reflect.DeepEqual(v2, want) {
		t.Fatalf("imported stamps:\n got %#v\nwant %#v", v2, want)
	}
	if ok, err := g2.Hash().VerifyNodeChain(n.ID()); err != nil || !ok {
		t.Fatalf("VerifyNodeChain after import = (%v, %v), want (true, nil)", ok, err)
	}
}

// The identity property a MERGE depends on, observed through the public graph
// API: storing the same nested temporal list twice yields the same content
// hash, while a list whose elements are the ISO STRINGS of those temporals
// yields a different one. Catches the erasure bug, under which the two are
// indistinguishable and a repeated MERGE cannot tell the node it already
// wrote from a new one.
func TestNode_NestedTemporalListHashIdentity(t *testing.T) {
	g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 13, BadgerInMemory: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()
	ctx := context.Background()

	typed := []any{types.TemporalValue{Kind: types.TemporalDate, Value: "2024-01-01"}}
	stringy := []any{"2024-01-01"}

	// The hash is taken from a node read back through export/import, i.e. from
	// the bytes that actually persist — an in-process Get can be served from a
	// cache that never exercised the wire encoding.
	add := func(list []any) []byte {
		n, err := g.Nodes().Add(ctx, []string{"E"}, map[string]any{"stamps": list})
		if err != nil {
			t.Fatalf("Add: %v", err)
		}
		var buf bytes.Buffer
		if err := g.IO().Export(&buf); err != nil {
			t.Fatalf("Export: %v", err)
		}
		g2, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 14, BadgerInMemory: true})
		if err != nil {
			t.Fatalf("New g2: %v", err)
		}
		defer g2.Close()
		if err := g2.IO().Import(bytes.NewReader(buf.Bytes()), tkgio.ImportOptions{}); err != nil {
			t.Fatalf("Import: %v", err)
		}
		stored, err := g2.Nodes().Get(ctx, n.ID())
		if err != nil {
			t.Fatalf("Get after import: %v", err)
		}
		return types.AppendPropertyValueHashBytes(nil, mustProperty(t, stored, "stamps"))
	}

	a := add(typed)
	b := add(typed)
	c := add(stringy)
	if !bytes.Equal(a, b) {
		t.Fatal("two identical nested temporal lists produced different content hashes")
	}
	if bytes.Equal(a, c) {
		t.Fatal("a nested temporal list and a nested string list hash identically")
	}
}

func mustProperty(t *testing.T, n *types.Node, key string) any {
	t.Helper()
	v, ok := n.GetProperty(key)
	if !ok {
		t.Fatalf("property %q missing", key)
	}
	return v
}
