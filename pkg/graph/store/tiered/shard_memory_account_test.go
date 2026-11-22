package tiered

import (
	"errors"
	"fmt"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/internal/testtier"
	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestShardMemoryIsAccounted pins MEMACCT (sigma R7-I5's cycle package, ai-soc
// D9): MemoryStats reports per tier what each open store holds by
// holder. The reference shard is written past a lowered node hash cap and
// past 4,096 values of one property: the hashes stay at their cap with every
// lookup counted as a hit or a miss, the property's sketch turns dense and
// stays flat, the ID sets grow by at least 8 B a node and 16 B a rel, and the badger arenas
// are the configured MemTableSize (with badger's two batch reserves) and
// caches; a hot shard's account moves to cold with the demotion and is 0 once
// its handle closes, its read-only reopen has no memtable arena, the whole
// account 0 after Close. Catches a holder left
// out, a cache past its cap, a closed shard still counted and the arena not
// reachable through tiered (D9: "badger's Store.MemTableSize is not reachable
// through tiered").
func TestShardMemoryIsAccounted(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	cfg := volumeCfg(t.TempDir(), t.TempDir(), "")
	cfg.MemTableSize = 16 << 20
	cfg.MaxOpenColdShards = 1 // a cold read keeps its read-only handle
	ts, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = ts.Close() })
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	caseTok, _ := reg.GetOrCreate("Case")
	sigTok, _ := reg.GetOrCreate("Signal")
	ref := ts.RefShardForTest()
	const hashCap = 64
	ref.SetNodeHashCapForTest(hashCap)

	st := ts.MemoryStats()
	m := st.Reference
	wantArena := int64(16<<20) + 2*(15*int64(16<<20)/100)
	if m.MemTableArena < wantArena-int64(1<<10) || m.MemTableArena > wantArena || m.BlockCache <= 0 ||
		st.Hot.MemTableArena <= 0 || m.Revisions != 2*128<<10 || m.MemTableArenaMax != 7*m.MemTableArena {
		t.Fatalf("new store: reference %+v, hot %+v; want an arena of about %d B, 7 of them as the bound (NumMemtables 5), a block cache and 256 KiB of revisions", m, st.Hot, wantArena)
	}

	gen := tieredNodeGen(t)
	var ids []types.NodeID
	put := func(from, to int) {
		t.Helper()
		batch := make([]*types.Node, 0, to-from)
		for i := from; i < to; i++ {
			n := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
			if err := n.SetProperty("ref", fmt.Sprintf("v%06d", i)); err != nil {
				t.Fatal(err)
			}
			n.SetIntegrity(&types.NodeIntegrity{Hash: fmt.Sprintf("%064x", i)})
			batch = append(batch, n)
			ids = append(ids, n.ID())
		}
		if err := ts.PutNodesBatch(batch); err != nil {
			t.Fatalf("PutNodesBatch: %v", err)
		}
	}
	put(0, 6000) // past 4,096 values: a dense sketch
	dense := ts.MemoryStats().Reference
	if dense.SketchesDense != 1 || dense.Sketches < 16384 || dense.Sketches > 16384+1024 {
		t.Fatalf("6,000 values: %d dense sketches of %d B, want one of 16,384 B and its struct", dense.SketchesDense, dense.Sketches)
	}
	if grown := dense.IDSets - m.IDSets; grown < 8*6000 {
		t.Fatalf("6,000 nodes grew the ID sets by %d B, want at least 8 B a node", grown)
	}
	put(6000, 8000)
	if again := ts.MemoryStats().Reference; again.Sketches != dense.Sketches || again.SketchesDense != 1 {
		t.Fatalf("2,000 more values moved the dense sketch: %d B (%d dense), was %d B", again.Sketches, again.SketchesDense, dense.Sketches)
	}

	var atCap int64
	for i, id := range ids[:3*hashCap] {
		if _, err := ts.NodeIntegrityHash(id); err != nil {
			t.Fatalf("NodeIntegrityHash: %v", err)
		}
		if i == hashCap-1 {
			atCap = ts.MemoryStats().Reference.NodeHashes
		}
	}
	full := ts.MemoryStats().Reference
	if full.NodeHashEntries != hashCap || full.NodeHashCap != hashCap || full.NodeHashMisses != 3*hashCap ||
		full.NodeHashHits != 0 || full.NodeHashes != atCap || atCap < int64(hashCap)*(8+16+64) {
		t.Fatalf("%d distinct lookups: hashes %+v; want %d entries at the cap, every lookup a miss, the %d B held at the cap (at least 88 B an entry)",
			3*hashCap, full, hashCap, atCap)
	}
	last := ids[3*hashCap-1] // the latest fill, never the victim of a later one
	if _, err := ts.NodeIntegrityHash(last); err != nil {
		t.Fatal(err)
	}
	if hit := ts.MemoryStats().Reference; hit.NodeHashHits != 1 || hit.NodeHashes != full.NodeHashes {
		t.Fatalf("a repeated lookup: %d hits, %d B; want 1 hit, %d B", hit.NodeHashHits, hit.NodeHashes, full.NodeHashes)
	}

	before := ts.MemoryStats().Reference.IDSets
	rgen := tieredRelGen(t)
	rels := make([]*types.Relationship, 0, 2000)
	for i := range 2000 {
		rels = append(rels, types.NewRelationship(types.RelID(rgen.Generate()), 1, ids[i], ids[i+1]))
	}
	if err := ts.PutRelationshipsBatch(rels); err != nil {
		t.Fatalf("PutRelationshipsBatch: %v", err)
	}
	if grown := ts.MemoryStats().Reference.IDSets - before; grown < 16*2000 {
		t.Fatalf("2,000 rels of one type grew the ID sets by %d B, want at least 16 B a rel (its ID and its type index entry)", grown)
	}

	sig := types.NewNode(types.NodeID(gen.Generate()), sigTok, nil)
	if err := ts.PutNode(sig); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	// The handle sweep would close the demoted handle on its own goroutine
	// (TAGFLAKES): held until the cold account is read.
	ts.SetSweepFaultForTest(func(step string) error {
		if step == "gated" {
			return errors.New("demoted handle held for the cold account")
		}
		return nil
	})
	old := ts.HotShardForTest().Name()
	forceRotation(t, ts)
	demoteToCold(ts, old)
	if cold := ts.MemoryStats().Cold; cold.IDSets <= 0 || cold.MemTableArena <= 0 {
		t.Fatalf("demoted shard with its handle open: cold account %+v, want its ID sets and arena", cold)
	}
	ts.SetSweepFaultForTest(nil)
	closeEventShardStore(t, ts, old)
	if cold := ts.MemoryStats().Cold; cold != (MemoryAccount{}) {
		t.Fatalf("closed cold shard: account %+v, want 0", cold)
	}
	if _, err := ts.GetNode(sig.ID()); err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if cold := ts.MemoryStats().Cold; cold.IDSets <= 0 || cold.MemTableArena != 0 || cold.MemTableArenaMax != 0 {
		t.Fatalf("cold shard opened read-only by a read: account %+v, want its ID sets and no memtable arena", cold)
	}
	if err := ts.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	st = ts.MemoryStats()
	if st.Reference.Bytes() != 0 || st.Hot.Bytes() != 0 || st.Warm.Bytes() != 0 {
		t.Fatalf("closed store: reference %+v hot %+v warm %+v, want 0", st.Reference, st.Hot, st.Warm)
	}
}
