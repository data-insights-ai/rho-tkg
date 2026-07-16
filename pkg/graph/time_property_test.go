package graph_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Ask 3 — a time.Time property value round-trips through a real graph: create
// with a time.Time, read back the canonical TemporalValue, hash-chain verifies,
// and an export/import reproduces it byte-for-byte (the canonical form is what
// persists, so no new wire type is introduced).
func TestNode_TimePropertyRoundTrip(t *testing.T) {
	g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 3, BadgerInMemory: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()
	ctx := context.Background()

	tm := time.Date(2024, 3, 14, 9, 26, 53, 0, time.UTC)
	n, err := g.Nodes().Add(ctx, []string{"Event"}, map[string]any{"seen": tm, "name": "e1"})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}

	got, err := g.Nodes().Get(ctx, n.ID())
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	v, ok := got.GetProperty("seen")
	if !ok {
		t.Fatal("seen property missing")
	}
	tv, ok := v.(types.TemporalValue)
	if !ok {
		t.Fatalf("seen type = %T, want TemporalValue", v)
	}
	if tv.Kind != types.TemporalDateTime || tv.Value != tm.Format(time.RFC3339Nano) {
		t.Fatalf("seen = %#v, want DateTime %q", tv, tm.Format(time.RFC3339Nano))
	}

	// Hash chain over the canonical value verifies (no unhashable type leaked in).
	if ok, err := g.Hash().VerifyNodeChain(n.ID()); err != nil || !ok {
		t.Fatalf("VerifyNodeChain = (%v, %v), want (true, nil)", ok, err)
	}

	// Export/import round-trip preserves it (canonical TemporalValue persists).
	var buf bytes.Buffer
	if err := g.IO().Export(&buf); err != nil {
		t.Fatalf("Export: %v", err)
	}
	g2, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 4, BadgerInMemory: true})
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
	v2, _ := got2.GetProperty("seen")
	if v2 != v {
		t.Fatalf("imported seen = %#v, want %#v", v2, v)
	}
}
