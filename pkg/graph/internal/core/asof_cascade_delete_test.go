package core

import (
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Shared helpers for the as-of delete tests (bitemporal_delete_closed_test.go):
// the memory, badger and tiered backends (badger exercises the native
// reverse-scan as-of resolver, tiered the core fallback) and the set-door
// lookups nodeInAsOf / relInAsOf.
//
// The as-of cascade-then-delete regression that used to live here (a delete
// that tombstones the corrected version of an open-ended cascade must not let
// the still-open genesis answer present) is the "open-cascade" shape of
// ccShapes, run through every delete door and as-of door on four backends by
// TestAsOfBoundedCascadeThenDelete.

type asofBackend struct {
	name    string
	newCore func(t *testing.T) *Core
}

func asofCascadeBackends() []asofBackend {
	return []asofBackend{
		{"memory", func(t *testing.T) *Core {
			g, err := New(Config{})
			if err != nil {
				t.Fatalf("New(memory): %v", err)
			}
			t.Cleanup(func() { _ = g.Close() })
			return g
		}},
		{"badger", func(t *testing.T) *Core {
			g, err := New(Config{BadgerInMemory: true})
			if err != nil {
				t.Fatalf("New(badger): %v", err)
			}
			t.Cleanup(func() { _ = g.Close() })
			return g
		}},
		{"tiered", func(t *testing.T) *Core {
			g, _ := newTestTieredGraph(t)
			return g
		}},
	}
}

// nodeInAsOf reports the version of id present in NodesAsOf(pin), or ok=false.
func nodeInAsOf(t *testing.T, g *Core, id types.NodeID, pin types.Instant) (*types.Node, bool) {
	t.Helper()
	rows, err := g.Temporal.NodesAsOf(pin)
	if err != nil {
		t.Fatalf("NodesAsOf(%d): %v", pin, err)
	}
	for _, n := range rows {
		if n.ID() == id {
			return n, true
		}
	}
	return nil, false
}

// relInAsOf reports the version of id present in RelsAsOf(pin), or ok=false.
func relInAsOf(t *testing.T, g *Core, id types.RelID, pin types.Instant) (*types.Relationship, bool) {
	t.Helper()
	rows, err := g.Temporal.RelsAsOf(pin)
	if err != nil {
		t.Fatalf("RelsAsOf(%d): %v", pin, err)
	}
	for _, r := range rows {
		if r.ID() == id {
			return r, true
		}
	}
	return nil, false
}
