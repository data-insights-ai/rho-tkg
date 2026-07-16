package core

import (
	"context"
	"errors"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// relPrefixBackends: rel property indexes are RAM-only (no on-disk entries), so
// there is no badger-disk variant here — memory and badger-RAM cover the surface.
func relPrefixBackends() []orderedBackend {
	return []orderedBackend{
		{name: "memory", cfg: Config{Store: memory.New(), SnowflakeNodeID: 0}},
		{name: "badger-ram", cfg: Config{BadgerInMemory: true, SnowflakeNodeID: 1}},
	}
}

func collectRelPrefix(t *testing.T, g *Core, typeName, key, prefix string, desc bool) []types.RelID {
	t.Helper()
	var got []types.RelID
	err := g.Rels.ForEachByTypePropertyPrefix(typeName, key, prefix, desc, storepkg.QueryOpts{}, func(r *types.Relationship) bool {
		got = append(got, r.ID())
		return true
	})
	if err != nil {
		t.Fatalf("ForEachByTypePropertyPrefix(%q, desc=%v): %v", prefix, desc, err)
	}
	return got
}

func eqRelIDs(a, b []types.RelID) bool {
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

// TestRelPrefixScan_ValueOrderContract is the relationship mirror of
// TestPrefixScan_ValueOrderContract (rule 2 parity): exact lex value order
// asc/desc with rel-ID tie-break in both directions, boundary-successor
// exclusion, no-match, and the empty prefix — on memory and badger-RAM.
func TestRelPrefixScan_ValueOrderContract(t *testing.T) {
	t.Parallel()
	const typeName, key = "LINK", "tag"
	ctx := context.Background()

	for _, be := range relPrefixBackends() {
		be := be
		t.Run(be.name, func(t *testing.T) {
			t.Parallel()
			g, err := New(be.cfg)
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			t.Cleanup(func() { _ = g.Close() })
			if err := g.Index.CreateRelProperty(typeName, key); err != nil {
				t.Fatalf("CreateRelProperty: %v", err)
			}
			// Two endpoint nodes for every relationship.
			a, err := g.Nodes.Add(ctx, []string{"N"}, nil)
			if err != nil {
				t.Fatalf("Add node: %v", err)
			}
			b, err := g.Nodes.Add(ctx, []string{"N"}, nil)
			if err != nil {
				t.Fatalf("Add node: %v", err)
			}
			add := func(tag string) types.RelID {
				r, err := g.Rels.Add(ctx, typeName, a, b, map[string]any{key: tag})
				if err != nil {
					t.Fatalf("Add rel(%s): %v", tag, err)
				}
				return r.ID()
			}
			app := add("app")
			apple1 := add("apple")
			apple2 := add("apple") // tie value, larger id
			apricot := add("apricot")
			add("banana")
			aq := add("aq") // == prefixSuccessor("ap")

			if got := collectRelPrefix(t, g, typeName, key, "ap", false); !eqRelIDs(got, []types.RelID{app, apple1, apple2, apricot}) {
				t.Errorf("asc 'ap' = %v, want [app apple1 apple2 apricot]", got)
			}
			if got := collectRelPrefix(t, g, typeName, key, "ap", true); !eqRelIDs(got, []types.RelID{apricot, apple1, apple2, app}) {
				t.Errorf("desc 'ap' = %v, want [apricot apple1 apple2 app]", got)
			}
			if got := collectRelPrefix(t, g, typeName, key, "aq", false); !eqRelIDs(got, []types.RelID{aq}) {
				t.Errorf("asc 'aq' = %v, want [aq]", got)
			}
			if got := collectRelPrefix(t, g, typeName, key, "zzz", false); len(got) != 0 {
				t.Errorf("asc 'zzz' = %v, want empty", got)
			}
			if got := collectRelPrefix(t, g, typeName, key, "", false); len(got) != 6 {
				t.Errorf("asc '' returned %d rels, want 6", len(got))
			}
		})
	}
}

// TestRelPrefixScan_Declines mirrors the node decline contracts.
func TestRelPrefixScan_Declines(t *testing.T) {
	t.Parallel()
	const typeName, key = "LINK", "tag"
	ctx := context.Background()

	g, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	a, _ := g.Nodes.Add(ctx, []string{"N"}, nil)
	b, _ := g.Nodes.Add(ctx, []string{"N"}, nil)
	if _, err := g.Rels.Add(ctx, typeName, a, b, map[string]any{key: "apple"}); err != nil {
		t.Fatalf("Add rel: %v", err)
	}
	// Type exists but no rel property index -> ErrIndexNotFound.
	err = g.Rels.ForEachByTypePropertyPrefix(typeName, key, "a", false, storepkg.QueryOpts{}, func(*types.Relationship) bool { return true })
	if !errors.Is(err, storepkg.ErrIndexNotFound) {
		t.Errorf("no index: err = %v, want ErrIndexNotFound", err)
	}
	if err := g.Index.CreateRelProperty(typeName, key); err != nil {
		t.Fatalf("CreateRelProperty: %v", err)
	}
	// Temporal opts -> declined.
	err = g.Rels.ForEachByTypePropertyPrefix(typeName, key, "a", false, storepkg.QueryOpts{ValidAt: 123}, func(*types.Relationship) bool { return true })
	if !errors.Is(err, storepkg.ErrOrderedScanTemporal) {
		t.Errorf("temporal opts: err = %v, want ErrOrderedScanTemporal", err)
	}
}
