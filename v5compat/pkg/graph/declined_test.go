package graph_test

import (
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5compat/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v5compat/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v5compat/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v5compat/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v5compat/pkg/graph/store/tiered"
)

// The X list returns the typed sentinel, never a panic or a silent success.
func TestDeclinedDoorsAreTyped(t *testing.T) {
	if _, err := tiered.New(tiered.Config{InMemory: true}); !errors.Is(err, graph.ErrNotSupportedInV5) {
		t.Fatalf("tiered.New: %v", err)
	}
	if _, err := sharded.New(sharded.Config{}); !errors.Is(err, graph.ErrNotSupportedInV5) {
		t.Fatalf("sharded.New: %v", err)
	}
	if _, err := badger.New(badger.Config{Dir: "x"}); !errors.Is(err, graph.ErrNotSupportedInV5) {
		t.Fatalf("badger.New: %v", err)
	}
	for name, cfg := range map[string]graph.Config{
		"UntimedFromTxFrom": {UntimedValidStart: graph.UntimedFromTxFrom},
		"ReadOnlyReplica":   {ReadOnlyReplica: true},
		"Store":             {Store: &tiered.Store{}},
	} {
		if g, err := graph.New(cfg); !errors.Is(err, graph.ErrNotSupportedInV5) || g != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	// Tuning knobs are accepted and ignored.
	for _, cfg := range []graph.Config{{SyncWrites: true}, {CacheCapacity: 1, CacheBudgetBytes: 1, ResidentCache: true}, {AllowRetentionPurge: true}, {SnowflakeNodeID: 3}} {
		g, err := graph.New(cfg)
		if err != nil {
			t.Fatalf("%+v: %v", cfg, err)
		}
		_ = g.Close()
	}
	// The accepted forms still open: memory.New, BadgerInMemory, SnowflakeNodeID.
	for _, cfg := range []graph.Config{{Store: memory.New()}, {BadgerInMemory: true, SnowflakeNodeID: 7}, {}} {
		g, err := graph.New(cfg)
		if err != nil {
			t.Fatalf("%+v: %v", cfg, err)
		}
		_ = g.Close()
	}
	g, _ := graph.New(graph.Config{})
	_ = g.Close()
	if err := g.Close(); !errors.Is(err, graph.ErrGraphClosed) {
		t.Fatalf("second Close: %v", err)
	}
}
