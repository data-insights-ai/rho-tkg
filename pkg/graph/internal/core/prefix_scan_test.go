package core

import (
	"context"
	"errors"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// collectPrefix runs the prefix scan and returns the emitted node IDs in the
// order the door produced them (value order; the caller asserts against the
// expected sequence).
func collectPrefix(t *testing.T, g *Core, label, key, prefix string, desc bool) []types.NodeID {
	t.Helper()
	var got []types.NodeID
	err := g.Nodes.ForEachByLabelPropertyPrefix(label, key, prefix, desc, storepkg.QueryOpts{}, func(n *types.Node) bool {
		got = append(got, n.ID())
		return true
	})
	if err != nil {
		t.Fatalf("ForEachByLabelPropertyPrefix(%q, desc=%v): %v", prefix, desc, err)
	}
	return got
}

func eqIDs(a, b []types.NodeID) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestPrefixScan_ValueOrderContract asserts the exact lexicographic value-order
// contract (asc + desc), ties broken by node ID ascending in BOTH directions,
// the empty prefix (all strings), no-match, and the boundary case (a value equal
// to the prefix successor must NOT match) — across memory, badger-RAM, and
// badger-disk (PropertyIndexOnDisk) ordered string views.
func TestPrefixScan_ValueOrderContract(t *testing.T) {
	t.Parallel()
	const label, key = "Word", "text"
	ctx := context.Background()

	for _, be := range orderedBackends() {
		be := be
		t.Run(be.name, func(t *testing.T) {
			t.Parallel()
			g, err := New(be.cfg)
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			t.Cleanup(func() { _ = g.Close() })
			if err := g.Index.CreateProperty(label, key); err != nil {
				t.Fatalf("CreateProperty: %v", err)
			}

			// Create in an order that makes the id tie-break observable: the two
			// "apple" nodes are created adjacent so the earlier has the smaller id.
			add := func(text string) types.NodeID {
				n, err := g.Nodes.Add(ctx, []string{label}, map[string]any{key: text})
				if err != nil {
					t.Fatalf("Add(%s): %v", text, err)
				}
				return n.ID()
			}
			app := add("app")
			apple1 := add("apple")
			apple2 := add("apple") // tie value, larger id
			apricot := add("apricot")
			add("banana")   // outside "ap"
			aq := add("aq") // == prefixSuccessor("ap"): must be EXCLUDED from "ap"

			// asc: app < apple(id-asc: apple1, apple2) < apricot
			if got := collectPrefix(t, g, label, key, "ap", false); !eqIDs(got, []types.NodeID{app, apple1, apple2, apricot}) {
				t.Errorf("asc 'ap' = %v, want [app apple1 apple2 apricot]", got)
			}
			// desc: apricot > apple(TIES STILL id-asc: apple1, apple2) > app
			if got := collectPrefix(t, g, label, key, "ap", true); !eqIDs(got, []types.NodeID{apricot, apple1, apple2, app}) {
				t.Errorf("desc 'ap' = %v, want [apricot apple1 apple2 app]", got)
			}
			// narrower prefix
			if got := collectPrefix(t, g, label, key, "app", false); !eqIDs(got, []types.NodeID{app, apple1, apple2}) {
				t.Errorf("asc 'app' = %v, want [app apple1 apple2]", got)
			}
			// boundary: "aq" is the successor of "ap" and must scan only itself.
			if got := collectPrefix(t, g, label, key, "aq", false); !eqIDs(got, []types.NodeID{aq}) {
				t.Errorf("asc 'aq' = %v, want [aq]", got)
			}
			// no match
			if got := collectPrefix(t, g, label, key, "zzz", false); len(got) != 0 {
				t.Errorf("asc 'zzz' = %v, want empty", got)
			}
			// empty prefix = every string value in value order.
			if got := collectPrefix(t, g, label, key, "", false); len(got) != 6 {
				t.Errorf("asc '' returned %d nodes, want 6 (all string values)", len(got))
			}
		})
	}
}

// TestPrefixScan_TopKPushdown verifies fn returning false stops the scan (LIMIT
// pushdown) and that current-state maintenance is correct through Update: a
// node's OLD string value stops matching and its NEW value starts matching.
func TestPrefixScan_TopKPushdown(t *testing.T) {
	t.Parallel()
	const label, key = "Word", "text"
	ctx := context.Background()

	for _, be := range orderedBackends() {
		be := be
		t.Run(be.name, func(t *testing.T) {
			t.Parallel()
			g, err := New(be.cfg)
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			t.Cleanup(func() { _ = g.Close() })
			if err := g.Index.CreateProperty(label, key); err != nil {
				t.Fatalf("CreateProperty: %v", err)
			}
			for _, s := range []string{"alpha", "alps", "alto", "amber", "anvil"} {
				if _, err := g.Nodes.Add(ctx, []string{label}, map[string]any{key: s}); err != nil {
					t.Fatalf("Add(%s): %v", s, err)
				}
			}

			// Top-2 of prefix "al" ascending: alpha, alps — stop after 2.
			var got []string
			n := 0
			err = g.Nodes.ForEachByLabelPropertyPrefix(label, key, "al", false, storepkg.QueryOpts{}, func(nd *types.Node) bool {
				got = append(got, nd.PropertiesMap()[key].(string))
				n++
				return n < 2 // stop after collecting 2
			})
			if err != nil {
				t.Fatalf("prefix scan: %v", err)
			}
			if len(got) != 2 || got[0] != "alpha" || got[1] != "alps" {
				t.Errorf("top-2 'al' asc = %v, want [alpha alps]", got)
			}
		})
	}
}

// TestPrefixScan_MaintenanceThroughUpdate is the two-phase current-state check:
// after Update, the ordered string view reflects the NEW value, not the old.
func TestPrefixScan_MaintenanceThroughUpdate(t *testing.T) {
	t.Parallel()
	const label, key = "Word", "text"
	ctx := context.Background()

	for _, be := range orderedBackends() {
		be := be
		t.Run(be.name, func(t *testing.T) {
			t.Parallel()
			g, err := New(be.cfg)
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			t.Cleanup(func() { _ = g.Close() })
			if err := g.Index.CreateProperty(label, key); err != nil {
				t.Fatalf("CreateProperty: %v", err)
			}
			nd, err := g.Nodes.Add(ctx, []string{label}, map[string]any{key: "alpha"})
			if err != nil {
				t.Fatalf("Add: %v", err)
			}
			if got := collectPrefix(t, g, label, key, "al", false); !eqIDs(got, []types.NodeID{nd.ID()}) {
				t.Fatalf("before update: 'al' = %v, want [node]", got)
			}
			if _, err := g.Nodes.Update(ctx, nd.ID(), map[string]any{key: "beta"}); err != nil {
				t.Fatalf("Update: %v", err)
			}
			// OLD value no longer matches; NEW value now matches.
			if got := collectPrefix(t, g, label, key, "al", false); len(got) != 0 {
				t.Errorf("after update: 'al' = %v, want empty (old value gone)", got)
			}
			if got := collectPrefix(t, g, label, key, "be", false); !eqIDs(got, []types.NodeID{nd.ID()}) {
				t.Errorf("after update: 'be' = %v, want [node] (new value)", got)
			}
		})
	}
}

// TestPrefixScan_Declines covers the two decline contracts: temporal opts are
// refused with ErrOrderedScanTemporal, and a missing index yields ErrIndexNotFound.
func TestPrefixScan_Declines(t *testing.T) {
	t.Parallel()
	const label, key = "Word", "text"
	ctx := context.Background()

	g, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	// Label exists (node added) but no property index -> ErrIndexNotFound. (An
	// UNKNOWN label returns nil, mirroring the numeric ordered door.)
	if _, err := g.Nodes.Add(ctx, []string{label}, map[string]any{key: "apple"}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	err = g.Nodes.ForEachByLabelPropertyPrefix(label, key, "a", false, storepkg.QueryOpts{}, func(*types.Node) bool { return true })
	if !errors.Is(err, storepkg.ErrIndexNotFound) {
		t.Errorf("no index: err = %v, want ErrIndexNotFound", err)
	}

	if err := g.Index.CreateProperty(label, key); err != nil {
		t.Fatalf("CreateProperty: %v", err)
	}
	// Temporal opts are SERVED (Stage B), not declined — the fold resolves at the
	// pin. (Correct temporal ordering: TestTemporalOrderedScan_Prefix.)
	err = g.Nodes.ForEachByLabelPropertyPrefix(label, key, "a", false, storepkg.QueryOpts{ValidAt: 123}, func(*types.Node) bool { return true })
	if errors.Is(err, storepkg.ErrOrderedScanTemporal) || err != nil {
		t.Errorf("temporal opts: err = %v, want nil (served by fold)", err)
	}
}
