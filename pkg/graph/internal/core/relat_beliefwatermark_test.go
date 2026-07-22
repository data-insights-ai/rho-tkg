package core

import (
	"context"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestRelAt_BeliefWatermark_DeclinesWhenHistoryOutranksCurrent mirrors
// TestNodeAt_BeliefWatermark_DeclinesWhenHistoryOutranksCurrent for
// relationships (rule 2: structural parity). See that test for the full
// rationale.
func TestRelAt_BeliefWatermark_DeclinesWhenHistoryOutranksCurrent(t *testing.T) {
	t.Parallel()
	for name, cfg := range map[string]Config{
		"memory": {},
		"badger": {BadgerInMemory: true},
	} {
		t.Run(name, func(t *testing.T) {
			testRelAtBeliefWatermarkDeclinesWhenHistoryOutranksCurrent(t, cfg)
		})
	}
}

func testRelAtBeliefWatermarkDeclinesWhenHistoryOutranksCurrent(t *testing.T, cfg Config) {
	g, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()
	ctx := context.Background()

	a, err := g.Nodes.Add(ctx, []string{"P"}, nil)
	if err != nil {
		t.Fatalf("add node a: %v", err)
	}
	b, err := g.Nodes.Add(ctx, []string{"P"}, nil)
	if err != nil {
		t.Fatalf("add node b: %v", err)
	}
	r, err := g.Rels.Add(ctx, "KNOWS", a, b, map[string]any{
		"tkg_valid_from": types.Instant(1000),
		"state":          "A",
	})
	if err != nil {
		t.Fatalf("add rel: %v", err)
	}
	current, err := g.getCurrentRelationship(r.ID())
	if err != nil {
		t.Fatalf("getCurrentRelationship: %v", err)
	}
	txT0 := current.Temporal().TxFrom

	if !g.relCurrentAnswersAt(current, 2000, 0) {
		t.Fatalf("relCurrentAnswersAt = false before injection, want true")
	}

	injected := current.DeepCopy()
	injected.SetVersion(current.Version() + 1)
	if err := injected.SetProperty("state", "INJECTED"); err != nil {
		t.Fatalf("SetProperty: %v", err)
	}
	injectedTx := txT0 + 1_000_000
	injected.Temporal().ValidFrom = 1500
	injected.Temporal().ValidTo = 0
	injected.Temporal().TxFrom = injectedTx
	injected.Temporal().UpdatedAt = injectedTx
	if err := g.store.PutRelVersion(r.ID(), injected.Version(), injected); err != nil {
		t.Fatalf("PutRelVersion (raw injection): %v", err)
	}

	watermark, ok := g.relBeliefWatermark.RelBeliefWatermark(r.ID())
	if !ok {
		t.Fatalf("RelBeliefWatermark: not found after injection")
	}
	if watermark != injectedTx {
		t.Fatalf("watermark = %d, want %d (the injected row's TxFrom)", watermark, injectedTx)
	}
	if g.relCurrentAnswersAt(current, 2000, 0) {
		t.Fatalf("relCurrentAnswersAt = true after injecting a higher-belief history row, want false")
	}

	got, err := g.Temporal.RelAt(r.ID(), 2000)
	if err != nil {
		t.Fatalf("RelAt(2000): %v", err)
	}
	if v, _ := got.GetProperty("state"); v != "INJECTED" {
		t.Fatalf("RelAt(2000) state = %v, want INJECTED (the higher-belief history row must win)", v)
	}
}
