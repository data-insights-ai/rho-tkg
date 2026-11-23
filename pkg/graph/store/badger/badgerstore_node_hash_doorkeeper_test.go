package badger

import (
	"fmt"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestNodeHashFillsOnTheSecondLookup (HASHFILL): an event shard's nodes are
// endpoints once, so 5,000 nodes hashed once each fill no cache entry (before,
// each filled one: 5,000 entries); a node hashed a second time is filled and
// its third lookup is a hit. Every answer equals the hash recomputed from the
// row, and the account names the doorkeeper's 128 KiB.
func TestNodeHashFillsOnTheSecondLookup(t *testing.T) {
	bs, err := New(Config{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	const n = 5000
	ids := make([]types.NodeID, n)
	for i := range ids {
		ids[i] = types.NodeID(snowflake.ID(1<<30 + i))
		nd := types.NewNode(ids[i], 1, nil)
		nd.SetIntegrity(&types.NodeIntegrity{Hash: fmt.Sprintf("%064x", i), PrevHash: "p"})
		if err := bs.PutNode(nd); err != nil {
			t.Fatal(err)
		}
	}
	want := map[types.NodeID]string{}
	for _, id := range ids {
		h, err := bs.NodeIntegrityHash(id)
		if err != nil || h == "" {
			t.Fatalf("hash %v = %q, %v", id, h, err)
		}
		want[id] = h
	}
	a := bs.MemoryAccount()
	if a.NodeHashEntries != 0 || a.NodeHashHits != 0 {
		t.Fatalf("after one lookup each: %d entries, %d hits; want none (a node looked up once fills nothing)", a.NodeHashEntries, a.NodeHashHits)
	}
	if a.NodeHashes != nodeHashDoorBits/8 {
		t.Errorf("NodeHashes %d B; want the doorkeeper's %d", a.NodeHashes, nodeHashDoorBits/8)
	}
	for round := range 2 {
		for _, id := range ids[:100] {
			if h, err := bs.NodeIntegrityHash(id); err != nil || h != want[id] {
				t.Fatalf("round %d: hash %v = %q, %v; want %q", round, id, h, err, want[id])
			}
		}
	}
	if a = bs.MemoryAccount(); a.NodeHashEntries != 100 || a.NodeHashHits != 100 {
		t.Fatalf("after two more lookups of 100: %d entries, %d hits; want 100 and 100 (filled on the second, hit on the third)", a.NodeHashEntries, a.NodeHashHits)
	}
}
