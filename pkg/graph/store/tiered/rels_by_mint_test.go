package tiered

import (
	"errors"
	"slices"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Row 94 (R1): Config.RelsByMint places a relationship between two
// reference-shard endpoints on the event shard of its ID's mint instant
// (entity + both legs), so a cold shard's close frees its edges.

func mintCfg(dir string, on bool) Config {
	return Config{
		DataDir:       dir,
		InMemory:      dir == "",
		RefLabels:     []string{"Case", "User"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
		RelsByMint:    on,
	}
}

// openMintStore opens cfg with the default test label registry (Case=1,
// User=2, Signal=3 — the same tokens on every reopen).
func openMintStore(t *testing.T, cfg Config) *Store {
	t.Helper()
	ts, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = ts.Close() })
	installDefaultTestLabelRegistry(t, ts)
	return ts
}

const (
	mintCaseTok   uint16 = 1
	mintSignalTok uint16 = 3
)

func putMintNode(t *testing.T, ts *Store, id types.NodeID, tok uint16) types.NodeID {
	t.Helper()
	if err := ts.PutNode(types.NewNode(id, tok, nil)); err != nil {
		t.Fatalf("PutNode %d: %v", id, err)
	}
	return id
}

func putMintRel(t *testing.T, ts *Store, id types.RelID, tok uint16, a, b types.NodeID) {
	t.Helper()
	if err := ts.PutRelationship(types.NewRelationship(id, tok, a, b)); err != nil {
		t.Fatalf("PutRelationship %d: %v", id, err)
	}
}

func hotShardName(ts *Store) string {
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	return ts.hotShard.name
}

func eventShardStore(t *testing.T, ts *Store, name string) *BadgerStore {
	t.Helper()
	ts.mu.RLock()
	es := ts.eventShards[name]
	ts.mu.RUnlock()
	if es == nil {
		t.Fatalf("no event shard %q", name)
	}
	es.shardMu.Lock()
	defer es.shardMu.Unlock()
	return es.store
}

func shardRelCount(t *testing.T, ts *Store, name string) int {
	t.Helper()
	infos, err := ts.ListShards()
	if err != nil {
		t.Fatalf("ListShards: %v", err)
	}
	for _, in := range infos {
		if in.Name == name {
			return in.Rels
		}
	}
	t.Fatalf("ListShards: no shard %q", name)
	return 0
}

func wantRelIDs(t *testing.T, what string, got []*types.Relationship, want ...types.RelID) {
	t.Helper()
	ids := relIDsOf(got)
	want = slices.Clone(want)
	slices.Sort(want)
	if !slices.Equal(ids, want) {
		t.Fatalf("%s = %v, want %v", what, ids, want)
	}
}

func containsRaw(ids []snowflake.ID, id types.RelID) bool {
	return slices.Contains(ids, id.SnowflakeID())
}

func rotate(t *testing.T, ts *Store) {
	t.Helper()
	time.Sleep(2 * time.Millisecond)
	if err := ts.RotateHotShard(); err != nil {
		t.Fatalf("RotateHotShard: %v", err)
	}
	time.Sleep(2 * time.Millisecond)
}

func TestRelsByMint_PlacementOnMintShard(t *testing.T) {
	t.Parallel()
	ng, rg := tieredNodeGen(t), tieredRelGen(t)
	ts := openMintStore(t, mintCfg("", true))
	a := putMintNode(t, ts, types.NodeID(ng.Generate()), mintCaseTok)
	b := putMintNode(t, ts, types.NodeID(ng.Generate()), mintCaseTok)
	s := putMintNode(t, ts, types.NodeID(ng.Generate()), mintSignalTok)
	hot := hotShardName(ts)

	r := types.RelID(rg.Generate())
	putMintRel(t, ts, r, 1, a, b)
	if got := shardRelCount(t, ts, hot); got != 1 {
		t.Fatalf("hot shard %s Rels = %d, want 1 (the ref->ref rel on its mint shard)", hot, got)
	}
	if got := shardRelCount(t, ts, "reference"); got != 0 {
		t.Fatalf("reference Rels = %d, want 0", got)
	}
	if containsRaw(ts.refShard.OutgoingRelIDs(a.SnowflakeID()), r) || containsRaw(ts.refShard.IncomingRelIDs(b.SnowflakeID(), 0), r) {
		t.Fatal("reference shard holds a leg of the mint-placed rel")
	}
	hs := eventShardStore(t, ts, hot)
	if !containsRaw(hs.OutgoingRelIDs(a.SnowflakeID()), r) || !containsRaw(hs.IncomingRelIDs(b.SnowflakeID(), 0), r) {
		t.Fatal("mint shard misses a leg of the rel")
	}
	if got, err := ts.GetRelationship(r); err != nil || got.ID() != r {
		t.Fatalf("GetRelationship = %v, %v", got, err)
	}

	// Break: one event-class endpoint keeps the legacy split write (the in/
	// leg of S->B on the reference shard, the entity of A->S on it).
	es, se := types.RelID(rg.Generate()), types.RelID(rg.Generate())
	putMintRel(t, ts, se, 1, s, b)
	putMintRel(t, ts, es, 1, a, s)
	if !containsRaw(ts.refShard.IncomingRelIDs(b.SnowflakeID(), 0), se) {
		t.Fatal("E->R rel lost its in/ leg on the reference shard")
	}
	if got := shardRelCount(t, ts, "reference"); got != 1 {
		t.Fatalf("reference Rels = %d after R->E, want 1 (legacy)", got)
	}

	// Break: self-loop on a reference node is a ref->ref rel: mint shard.
	loop := types.RelID(rg.Generate())
	putMintRel(t, ts, loop, 2, a, a)
	if !containsRaw(hs.OutgoingRelIDs(a.SnowflakeID()), loop) || containsRaw(ts.refShard.OutgoingRelIDs(a.SnowflakeID()), loop) {
		t.Fatal("self-loop not on its mint shard")
	}
	out, err := ts.OutgoingRelationships(a, 2)
	if err != nil {
		t.Fatal(err)
	}
	wantRelIDs(t, "Outgoing(a, 2)", out, loop)
	in, err := ts.IncomingRelationships(a, 2)
	if err != nil {
		t.Fatal(err)
	}
	wantRelIDs(t, "Incoming(a, 2)", in, loop)

	// Break: a cold mint shard falls back to the reference shard.
	old := types.RelID(rg.Generate())
	rotate(t, ts)
	demoteToCold(ts, hot)
	putMintRel(t, ts, old, 1, a, b)
	if !containsRaw(ts.refShard.OutgoingRelIDs(a.SnowflakeID()), old) {
		t.Fatal("rel minted in a cold window not placed on the reference shard")
	}

	// Break: option off: legacy reference placement.
	off := openMintStore(t, mintCfg("", false))
	oa := putMintNode(t, off, types.NodeID(ng.Generate()), mintCaseTok)
	ob := putMintNode(t, off, types.NodeID(ng.Generate()), mintCaseTok)
	or := types.RelID(rg.Generate())
	putMintRel(t, off, or, 1, oa, ob)
	if got := shardRelCount(t, off, "reference"); got != 1 {
		t.Fatalf("option off: reference Rels = %d, want 1", got)
	}
	if got := shardRelCount(t, off, hotShardName(off)); got != 0 {
		t.Fatalf("option off: hot Rels = %d, want 0", got)
	}
}

func TestRelsByMint_FallbackCounted(t *testing.T) {
	t.Parallel()
	ng, rg := tieredNodeGen(t), tieredRelGen(t)
	ts := openMintStore(t, mintCfg("", true))
	a := putMintNode(t, ts, types.NodeID(ng.Generate()), mintCaseTok)
	b := putMintNode(t, ts, types.NodeID(ng.Generate()), mintCaseTok)
	s := putMintNode(t, ts, types.NodeID(ng.Generate()), mintSignalTok)
	first := hotShardName(ts)
	warmID, coldID := types.RelID(rg.Generate()), types.RelID(rg.Generate())
	rotate(t, ts)
	second := hotShardName(ts)
	warm2 := types.RelID(rg.Generate())
	rotate(t, ts)

	// A warm mint shard is writable: placed there, not counted.
	putMintRel(t, ts, warm2, 1, a, b)
	if got := ts.RelsByMintFallbacks(); got != 0 {
		t.Fatalf("fallbacks after a warm placement = %d, want 0", got)
	}
	if !containsRaw(eventShardStore(t, ts, second).OutgoingRelIDs(a.SnowflakeID()), warm2) {
		t.Fatal("rel minted in a warm window not on that shard")
	}
	// An event endpoint is not a candidate: not counted.
	putMintRel(t, ts, types.RelID(rg.Generate()), 1, a, s)
	if got := ts.RelsByMintFallbacks(); got != 0 {
		t.Fatalf("fallbacks after an R->E rel = %d, want 0", got)
	}
	demoteToCold(ts, first)
	putMintRel(t, ts, coldID, 1, a, b)
	if got := ts.RelsByMintFallbacks(); got != 1 {
		t.Fatalf("fallbacks after a cold mint shard = %d, want 1", got)
	}
	if err := ts.PutRelationshipsBatch([]*types.Relationship{types.NewRelationship(warmID, 2, b, a)}); err != nil {
		t.Fatalf("PutRelationshipsBatch: %v", err)
	}
	if got := ts.RelsByMintFallbacks(); got != 2 {
		t.Fatalf("fallbacks after a batched cold placement = %d, want 2", got)
	}
	if !containsRaw(ts.refShard.OutgoingRelIDs(b.SnowflakeID()), warmID) {
		t.Fatal("batched fallback rel not on the reference shard")
	}

	off := openMintStore(t, mintCfg("", false))
	oa := putMintNode(t, off, types.NodeID(ng.Generate()), mintCaseTok)
	ob := putMintNode(t, off, types.NodeID(ng.Generate()), mintCaseTok)
	putMintRel(t, off, types.RelID(rg.Generate()), 1, oa, ob)
	if got := off.RelsByMintFallbacks(); got != 0 {
		t.Fatalf("option off: fallbacks = %d, want 0", got)
	}
}

// adjacencySnapshot is every adjacency door's answer for the probe nodes.
type adjacencySnapshot struct {
	out, in       map[uint16][]types.RelID
	outDeg, inDeg map[uint16]int
	outFor, inFor map[types.NodeID][]types.RelID
}

func takeAdjacency(t *testing.T, ts *Store, nid types.NodeID, batch []types.NodeID) adjacencySnapshot {
	t.Helper()
	s := adjacencySnapshot{
		out: map[uint16][]types.RelID{}, in: map[uint16][]types.RelID{},
		outDeg: map[uint16]int{}, inDeg: map[uint16]int{},
		outFor: map[types.NodeID][]types.RelID{}, inFor: map[types.NodeID][]types.RelID{},
	}
	for _, tok := range []uint16{0, 1, 2, 9} {
		out, err := ts.OutgoingRelationships(nid, tok)
		if err != nil {
			t.Fatalf("Outgoing(%d, %d): %v", nid, tok, err)
		}
		in, err := ts.IncomingRelationships(nid, tok)
		if err != nil {
			t.Fatalf("Incoming(%d, %d): %v", nid, tok, err)
		}
		s.out[tok], s.in[tok] = relIDsOf(out), relIDsOf(in)
		if s.outDeg[tok], err = ts.OutgoingDegree(nid, tok); err != nil {
			t.Fatal(err)
		}
		if s.inDeg[tok], err = ts.IncomingDegree(nid, tok); err != nil {
			t.Fatal(err)
		}
	}
	outFor, err := ts.OutgoingRelationshipsForNodes(batch, 0)
	if err != nil {
		t.Fatalf("OutgoingForNodes: %v", err)
	}
	inFor, err := ts.IncomingRelationshipsForNodes(batch, 0)
	if err != nil {
		t.Fatalf("IncomingForNodes: %v", err)
	}
	for k, v := range outFor {
		s.outFor[k] = relIDsOf(v)
	}
	for k, v := range inFor {
		s.inFor[k] = relIDsOf(v)
	}
	return s
}

func equalAdjacency(t *testing.T, what string, got, want adjacencySnapshot) {
	t.Helper()
	for tok, w := range want.out {
		if !slices.Equal(got.out[tok], w) {
			t.Fatalf("%s: Outgoing tok %d = %v, want %v", what, tok, got.out[tok], w)
		}
		if !slices.Equal(got.in[tok], want.in[tok]) {
			t.Fatalf("%s: Incoming tok %d = %v, want %v", what, tok, got.in[tok], want.in[tok])
		}
		if got.outDeg[tok] != want.outDeg[tok] || got.inDeg[tok] != want.inDeg[tok] {
			t.Fatalf("%s: degree tok %d = %d/%d, want %d/%d", what, tok, got.outDeg[tok], got.inDeg[tok], want.outDeg[tok], want.inDeg[tok])
		}
	}
	if len(got.outFor) != len(want.outFor) || len(got.inFor) != len(want.inFor) {
		t.Fatalf("%s: ForNodes keys = %d/%d, want %d/%d", what, len(got.outFor), len(got.inFor), len(want.outFor), len(want.inFor))
	}
	for k, w := range want.outFor {
		if !slices.Equal(got.outFor[k], w) {
			t.Fatalf("%s: OutgoingForNodes[%d] = %v, want %v", what, k, got.outFor[k], w)
		}
	}
	for k, w := range want.inFor {
		if !slices.Equal(got.inFor[k], w) {
			t.Fatalf("%s: IncomingForNodes[%d] = %v, want %v", what, k, got.inFor[k], w)
		}
	}
}

// TestRelsByMint_AdjacencyReadsEqual: a mixed store (legacy rels from an
// open without the option, mint rels from one with it, then an open without
// it again) answers every adjacency door like a store that never had it, and
// the ByDepth doors scope the mint rels by tier.
func TestRelsByMint_AdjacencyReadsEqual(t *testing.T) {
	ng, rg := tieredNodeGen(t), tieredRelGen(t)
	dir := t.TempDir()
	plain := openMintStore(t, mintCfg("", false))
	mixed := openMintStore(t, mintCfg(dir, false))
	both := func(fn func(ts *Store)) { fn(plain); fn(mixed) }

	a, b, c := types.NodeID(ng.Generate()), types.NodeID(ng.Generate()), types.NodeID(ng.Generate())
	s := types.NodeID(ng.Generate())
	both(func(ts *Store) {
		putMintNode(t, ts, a, mintCaseTok)
		putMintNode(t, ts, b, mintCaseTok)
		putMintNode(t, ts, c, mintCaseTok)
		putMintNode(t, ts, s, mintSignalTok)
	})
	legacy := types.RelID(rg.Generate())
	both(func(ts *Store) { putMintRel(t, ts, legacy, 1, a, b) })

	// Reopen the mixed store with the option: the next rels go by mint.
	if err := mixed.Close(); err != nil {
		t.Fatal(err)
	}
	mixed = openMintStore(t, mintCfg(dir, true))
	mintA, mintB, loop, re, er := types.RelID(rg.Generate()), types.RelID(rg.Generate()), types.RelID(rg.Generate()), types.RelID(rg.Generate()), types.RelID(rg.Generate())
	both(func(ts *Store) {
		putMintRel(t, ts, mintA, 2, a, b)
		putMintRel(t, ts, mintB, 1, b, a)
		putMintRel(t, ts, loop, 2, a, a)
		putMintRel(t, ts, re, 1, a, s)
		putMintRel(t, ts, er, 2, s, a)
	})
	firstHot := hotShardName(mixed)
	if containsRaw(mixed.refShard.OutgoingRelIDs(a.SnowflakeID()), mintA) {
		t.Fatal("setup: mintA on the reference shard")
	}
	batch := []types.NodeID{a, b, a, s, c}
	equalAdjacency(t, "mixed", takeAdjacency(t, mixed, a, batch), takeAdjacency(t, plain, a, batch))
	equalAdjacency(t, "mixed b", takeAdjacency(t, mixed, b, batch), takeAdjacency(t, plain, b, batch))

	// Reopen without the option: the sticky marker keeps the fan-out on, and
	// a new rel goes to the reference shard.
	if err := mixed.Close(); err != nil {
		t.Fatal(err)
	}
	mixed = openMintStore(t, mintCfg(dir, false))
	afterOff := types.RelID(rg.Generate())
	both(func(ts *Store) { putMintRel(t, ts, afterOff, 1, a, c) })
	if !containsRaw(mixed.refShard.OutgoingRelIDs(a.SnowflakeID()), afterOff) {
		t.Fatal("open without the option still placed by mint")
	}
	equalAdjacency(t, "reopened off", takeAdjacency(t, mixed, a, batch), takeAdjacency(t, plain, a, batch))

	// Unknown node: same sentinel from every door.
	unknown := types.NodeID(ng.Generate())
	if _, err := mixed.OutgoingRelationships(unknown, 0); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("Outgoing(unknown) err = %v, want ErrNodeNotFound", err)
	}
	if _, err := mixed.IncomingRelationshipsByDepth(unknown, 0, DepthWarm); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("IncomingByDepth(unknown) err = %v, want ErrNodeNotFound", err)
	}
	if _, err := mixed.OutgoingRelationshipsForNodes([]types.NodeID{a, unknown}, 0); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("OutgoingForNodes(unknown) err = %v, want ErrNodeNotFound", err)
	}
	if _, err := mixed.OutgoingDegreeByDepth(a, 0, ShardDepth(9)); !errors.Is(err, ErrInvalidShardDepth) {
		t.Fatalf("OutgoingDegreeByDepth(bad depth) err = %v, want ErrInvalidShardDepth", err)
	}

	// Depth: rotate (first hot -> warm), one more mint rel on the new hot shard.
	if err := mixed.Close(); err != nil {
		t.Fatal(err)
	}
	mixed = openMintStore(t, mintCfg(dir, true))
	rotate(t, mixed)
	late := types.RelID(rg.Generate())
	putMintRel(t, mixed, late, 1, a, b)
	type door func(*Store, ShardDepth) ([]*types.Relationship, error)
	outA := func(ts *Store, d ShardDepth) ([]*types.Relationship, error) {
		return ts.OutgoingRelationshipsByDepth(a, 0, d)
	}
	inA := func(ts *Store, d ShardDepth) ([]*types.Relationship, error) {
		return ts.IncomingRelationshipsByDepth(a, 0, d)
	}
	cases := []struct {
		name  string
		door  door
		depth ShardDepth
		want  []types.RelID
	}{
		{"out hot", outA, DepthHot, []types.RelID{legacy, re, afterOff, late}},
		{"out warm", outA, DepthWarm, []types.RelID{legacy, mintA, loop, re, afterOff, late}},
		{"out all", outA, DepthAll, []types.RelID{legacy, mintA, loop, re, afterOff, late}},
		{"in hot", inA, DepthHot, []types.RelID{er}},
		{"in warm", inA, DepthWarm, []types.RelID{mintB, loop, er}},
	}
	for _, tc := range cases {
		got, err := tc.door(mixed, tc.depth)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		wantRelIDs(t, tc.name, got, tc.want...)
	}
	if n, err := mixed.OutgoingDegreeByDepth(a, 0, DepthHot); err != nil || n != 4 {
		t.Fatalf("OutgoingDegreeByDepth(hot) = %d, %v, want 4", n, err)
	}
	if n, err := mixed.IncomingDegreeByDepth(a, 2, DepthWarm); err != nil || n != 2 {
		t.Fatalf("IncomingDegreeByDepth(warm, tok 2) = %d, %v, want 2 (loop, er)", n, err)
	}
	m, err := mixed.OutgoingRelationshipsForNodesByDepth([]types.NodeID{a, b, a}, 1, DepthHot)
	if err != nil {
		t.Fatal(err)
	}
	wantRelIDs(t, "OutForNodes(hot)[a]", m[a], legacy, re, afterOff, late)
	if len(m[b]) != 0 {
		t.Fatalf("OutForNodes(hot)[b] = %v, want none (mintB is on the warm shard)", relIDsOf(m[b]))
	}
	mi, err := mixed.IncomingRelationshipsForNodesByDepth([]types.NodeID{b}, 0, DepthWarm)
	if err != nil {
		t.Fatal(err)
	}
	wantRelIDs(t, "InForNodes(warm)[b]", mi[b], legacy, mintA, late)
	_ = firstHot
}

func TestRelsByMint_ColdCloseFreesLegs(t *testing.T) {
	t.Parallel()
	ng, rg := tieredNodeGen(t), tieredRelGen(t)
	ts := openMintStore(t, mintCfg(t.TempDir(), true))
	a := putMintNode(t, ts, types.NodeID(ng.Generate()), mintCaseTok)
	b := putMintNode(t, ts, types.NodeID(ng.Generate()), mintCaseTok)
	w := hotShardName(ts)
	r := types.RelID(rg.Generate())
	putMintRel(t, ts, r, 1, a, b)
	rotate(t, ts)

	// Break: a warm W still lists the edge.
	got, err := ts.OutgoingRelationshipsByDepth(a, 0, DepthWarm)
	if err != nil {
		t.Fatal(err)
	}
	wantRelIDs(t, "warm W: Outgoing(DepthWarm)", got, r)

	demoteToCold(ts, w)
	if closeColdStores(t, ts) != 1 {
		t.Fatal("setup: cold W not closed")
	}
	if containsRaw(ts.refShard.OutgoingRelIDs(a.SnowflakeID()), r) || containsRaw(ts.refShard.IncomingRelIDs(b.SnowflakeID(), 0), r) {
		t.Fatal("reference shard keeps a leg of the edge after W closed")
	}
	got, err = ts.OutgoingRelationshipsByDepth(a, 0, DepthWarm)
	if err != nil {
		t.Fatal(err)
	}
	wantRelIDs(t, "cold W: Outgoing(DepthWarm)", got)
	got, err = ts.IncomingRelationshipsByDepth(b, 0, DepthWarm)
	if err != nil {
		t.Fatal(err)
	}
	wantRelIDs(t, "cold W: Incoming(DepthWarm)", got)
	if coldShardsWithOpenStore(ts) != 0 {
		t.Fatal("a DepthWarm read opened the cold shard")
	}
	if rel, err := ts.GetRelationship(r); err != nil || rel.ID() != r {
		t.Fatalf("GetRelationship after close = %v, %v", rel, err)
	}
	got, err = ts.OutgoingRelationships(a, 0)
	if err != nil {
		t.Fatal(err)
	}
	wantRelIDs(t, "cold W: Outgoing (DepthAll)", got, r)
}

func TestRelsByMint_DeleteNoResidue(t *testing.T) {
	t.Parallel()
	ng, rg := tieredNodeGen(t), tieredRelGen(t)
	ts := openMintStore(t, mintCfg("", true))
	a := putMintNode(t, ts, types.NodeID(ng.Generate()), mintCaseTok)
	b := putMintNode(t, ts, types.NodeID(ng.Generate()), mintCaseTok)
	rotate(t, ts) // the rels' mint shard is not a or b's mint shard
	hot := hotShardName(ts)
	hs := eventShardStore(t, ts, hot)
	r1, r2, r3 := types.RelID(rg.Generate()), types.RelID(rg.Generate()), types.RelID(rg.Generate())
	putMintRel(t, ts, r1, 1, a, b)
	putMintRel(t, ts, r2, 1, a, b)
	putMintRel(t, ts, r3, 2, b, a)

	if err := ts.DeleteRelationship(r1); err != nil {
		t.Fatalf("DeleteRelationship: %v", err)
	}
	if hs.HasRelID(r1.SnowflakeID()) || containsRaw(hs.OutgoingRelIDs(a.SnowflakeID()), r1) || containsRaw(hs.IncomingRelIDs(b.SnowflakeID(), 0), r1) {
		t.Fatal("delete left a row or leg on the mint shard")
	}
	var ex []types.RelID
	if err := ts.ForEachExAdjacentRelID(a, false, func(id types.RelID) bool { ex = append(ex, id); return true }); err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(ex, r1) {
		t.Fatalf("ex-adjacency of a = %v, want it to hold the deleted mint rel %d", ex, r1)
	}

	// Healthy store: repair finds nothing and grows nothing on the reference shard.
	res, err := ts.RunRepair()
	if err != nil {
		t.Fatal(err)
	}
	if res.MissingInEntries != 0 || res.StaleInEntries != 0 || res.OrphanedInEntries != 0 {
		t.Fatalf("repair on a healthy mint store = %+v, want no findings", res)
	}
	if !containsRaw(hs.IncomingRelIDs(b.SnowflakeID(), 0), r2) {
		t.Fatal("repair dropped the mint rel's in/ leg")
	}
	if len(ts.refShard.IncomingRelIDs(b.SnowflakeID(), 0)) != 0 || len(ts.refShard.IncomingRelIDs(a.SnowflakeID(), 0)) != 0 {
		t.Fatal("repair grew in/ legs on the reference shard")
	}

	// Break: crash residue (entity + out without in/): repaired on the mint shard.
	if err := hs.DeleteIncomingByRelID(b.SnowflakeID(), r2.SnowflakeID()); err != nil {
		t.Fatal(err)
	}
	res, err = ts.RunRepair()
	if err != nil {
		t.Fatal(err)
	}
	if res.MissingInEntries != 1 {
		t.Fatalf("repair MissingInEntries = %d, want 1", res.MissingInEntries)
	}
	if !containsRaw(hs.IncomingRelIDs(b.SnowflakeID(), 0), r2) || containsRaw(ts.refShard.IncomingRelIDs(b.SnowflakeID(), 0), r2) {
		t.Fatal("repair put the missing in/ leg elsewhere than the mint shard")
	}

	// DeleteNodeWithHistory cascades over the mint rels.
	node, err := ts.GetNode(a)
	if err != nil {
		t.Fatal(err)
	}
	tombs := make([]storecontract.RelTombstone, 0, 2)
	for _, id := range []types.RelID{r2, r3} {
		rel, err := ts.GetRelationship(id)
		if err != nil {
			t.Fatal(err)
		}
		tombs = append(tombs, storecontract.RelTombstone{ID: id, PrevVersion: rel.Version(), Tombstone: rel.DeepCopy()})
	}
	if err := ts.DeleteNodeWithHistory(a, node.Version(), node.DeepCopy(), tombs); err != nil {
		t.Fatalf("DeleteNodeWithHistory: %v", err)
	}
	if hs.HasRelID(r2.SnowflakeID()) || hs.HasRelID(r3.SnowflakeID()) || len(hs.OutgoingRelIDs(b.SnowflakeID())) != 0 || len(hs.IncomingRelIDs(b.SnowflakeID(), 0)) != 0 {
		t.Fatal("node delete left mint rows or legs")
	}
	if _, err := ts.GetRelHistory(r2); err != nil {
		t.Fatalf("history of a deleted mint rel: %v", err)
	}
}

func TestRelsByMint_DeleteRefNodeRefuses(t *testing.T) {
	t.Parallel()
	ng, rg := tieredNodeGen(t), tieredRelGen(t)
	ts := openMintStore(t, mintCfg(t.TempDir(), true))
	a := putMintNode(t, ts, types.NodeID(ng.Generate()), mintCaseTok)
	b := putMintNode(t, ts, types.NodeID(ng.Generate()), mintCaseTok)
	lone := putMintNode(t, ts, types.NodeID(ng.Generate()), mintCaseTok)
	w := hotShardName(ts)
	r := types.RelID(rg.Generate())
	putMintRel(t, ts, r, 1, a, b)
	rotate(t, ts)
	demoteToCold(ts, w)
	closeColdStores(t, ts)

	if err := ts.DeleteNode(a); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("DeleteNode(start of a cold mint rel) err = %v, want ErrInvalidStoreMutation", err)
	}
	if err := ts.DeleteNodesBatch([]types.NodeID{lone, b}); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("DeleteNodesBatch(end of a cold mint rel) err = %v, want ErrInvalidStoreMutation", err)
	}
	if _, err := ts.GetNode(lone); err != nil {
		t.Fatalf("refused batch deleted an unconnected node: %v", err)
	}
	// Break: an unconnected node still deletes; after the rel is gone, so do a and b.
	if err := ts.DeleteNode(lone); err != nil {
		t.Fatalf("DeleteNode(unconnected): %v", err)
	}
	if err := ts.DeleteRelationship(r); err != nil {
		t.Fatal(err)
	}
	if err := ts.DeleteNode(a); err != nil {
		t.Fatalf("DeleteNode(a) after its rel: %v", err)
	}
	if err := ts.DeleteNodesBatch([]types.NodeID{b}); err != nil {
		t.Fatalf("DeleteNodesBatch(b) after its rel: %v", err)
	}
}

func TestRelsByMint_ArchiveRestoreSeesMintRels(t *testing.T) {
	t.Parallel()
	ng, rg := tieredNodeGen(t), tieredRelGen(t)
	ts := openMintStore(t, mintCfg("", true))
	a := putMintNode(t, ts, types.NodeID(ng.Generate()), mintCaseTok)
	b := putMintNode(t, ts, types.NodeID(ng.Generate()), mintCaseTok)
	hs := eventShardStore(t, ts, hotShardName(ts))
	r1, r2 := types.RelID(rg.Generate()), types.RelID(rg.Generate())
	putMintRel(t, ts, r1, 1, a, b)
	putMintRel(t, ts, r2, 2, b, a)

	check := func(phase string) {
		t.Helper()
		out, err := ts.OutgoingRelationships(a, 0)
		if err != nil {
			t.Fatalf("%s: Outgoing(a): %v", phase, err)
		}
		wantRelIDs(t, phase+": Outgoing(a)", out, r1)
		in, err := ts.IncomingRelationships(a, 0)
		if err != nil {
			t.Fatalf("%s: Incoming(a): %v", phase, err)
		}
		wantRelIDs(t, phase+": Incoming(a)", in, r2)
		in, err = ts.IncomingRelationships(b, 0)
		if err != nil {
			t.Fatal(err)
		}
		wantRelIDs(t, phase+": Incoming(b)", in, r1)
		if n, err := ts.OutgoingDegree(a, 0); err != nil || n != 1 {
			t.Fatalf("%s: OutgoingDegree(a) = %d, %v", phase, n, err)
		}
		m, err := ts.IncomingRelationshipsForNodes([]types.NodeID{a}, 0)
		if err != nil {
			t.Fatal(err)
		}
		wantRelIDs(t, phase+": IncomingForNodes(a)", m[a], r2)
	}
	check("before")
	if err := ts.ArchiveNode(a); err != nil {
		t.Fatalf("ArchiveNode: %v", err)
	}
	check("archived")
	res, err := ts.RunRepair()
	if err != nil {
		t.Fatal(err)
	}
	if res.MissingInEntries != 0 || res.StaleInEntries != 0 {
		t.Fatalf("repair with an archived endpoint = %+v, want no findings", res)
	}
	archive := ts.refArchive.Load()
	if len(ts.refShard.IncomingRelIDs(b.SnowflakeID(), 0)) != 0 || len(archive.IncomingRelIDs(a.SnowflakeID(), 0)) != 0 {
		t.Fatal("repair grew in/ legs off the mint shard")
	}
	if err := ts.RestoreNode(a); err != nil {
		t.Fatalf("RestoreNode: %v", err)
	}
	check("restored")

	// Delete while archived: no residue on the mint shard.
	if err := ts.ArchiveNode(a); err != nil {
		t.Fatal(err)
	}
	if err := ts.DeleteRelationship(r1); err != nil {
		t.Fatalf("DeleteRelationship with an archived start: %v", err)
	}
	if hs.HasRelID(r1.SnowflakeID()) || containsRaw(hs.IncomingRelIDs(b.SnowflakeID(), 0), r1) || containsRaw(hs.OutgoingRelIDs(a.SnowflakeID()), r1) {
		t.Fatal("delete with an archived start left mint residue")
	}
}

func TestRelsByMint_ChangelogOneRecord(t *testing.T) {
	t.Parallel()
	ng, rg := tieredNodeGen(t), tieredRelGen(t)
	cfg := mintCfg("", true)
	cfg.ChangeLog = true
	ts := openMintStore(t, cfg)
	a := putMintNode(t, ts, types.NodeID(ng.Generate()), mintCaseTok)
	b := putMintNode(t, ts, types.NodeID(ng.Generate()), mintCaseTok)
	first := hotShardName(ts)
	coldID := types.RelID(rg.Generate())
	r := types.RelID(rg.Generate())
	putMintRel(t, ts, r, 1, a, b)

	tags := func() []storecontract.ChangeTag {
		feed, err := ts.ChangeFeed(0, 0)
		if err != nil {
			t.Fatal(err)
		}
		out := make([]storecontract.ChangeTag, 0, len(feed))
		for _, c := range feed {
			out = append(out, c.Tag)
		}
		return out
	}
	want := []storecontract.ChangeTag{storecontract.ChangeNodePut, storecontract.ChangeNodePut, storecontract.ChangeRelPut}
	if got := tags(); !slices.Equal(got, want) {
		t.Fatalf("feed after a mint put = %v, want %v", got, want)
	}
	if err := ts.DeleteRelationship(r); err != nil {
		t.Fatal(err)
	}
	want = append(want, storecontract.ChangeRelDelete)
	if got := tags(); !slices.Equal(got, want) {
		t.Fatalf("feed after a mint delete = %v, want %v", got, want)
	}
	// Break: the fallback placement is one record too.
	rotate(t, ts)
	demoteToCold(ts, first)
	putMintRel(t, ts, coldID, 1, a, b)
	want = append(want, storecontract.ChangeRelPut)
	if got := tags(); !slices.Equal(got, want) {
		t.Fatalf("feed after a fallback put = %v, want %v", got, want)
	}
}

// TestRelsByMint_BatchAndClear: a batch of caller-ID rels goes by mint with
// the duplicate probe on the mint shard; Clear keeps the option's marker only
// while the option is on; the ByDepth ex-adjacency door validates its input.
func TestRelsByMint_BatchAndClear(t *testing.T) {
	t.Parallel()
	ng, rg := tieredNodeGen(t), tieredRelGen(t)
	ts := openMintStore(t, mintCfg("", true))
	a := putMintNode(t, ts, types.NodeID(ng.Generate()), mintCaseTok)
	b := putMintNode(t, ts, types.NodeID(ng.Generate()), mintCaseTok)
	hs := eventShardStore(t, ts, hotShardName(ts))
	r1, r2 := types.RelID(rg.Generate()), types.RelID(rg.Generate())
	if err := ts.PutRelationshipsBatch([]*types.Relationship{types.NewRelationship(r1, 1, a, b), types.NewRelationship(r2, 2, b, a)}); err != nil {
		t.Fatalf("PutRelationshipsBatch: %v", err)
	}
	if !containsRaw(hs.OutgoingRelIDs(a.SnowflakeID()), r1) || !containsRaw(hs.IncomingRelIDs(a.SnowflakeID(), 0), r2) {
		t.Fatal("batched rels not on their mint shard")
	}
	if err := ts.PutRelationshipsBatch([]*types.Relationship{types.NewRelationship(r1, 1, a, b)}); !errors.Is(err, ErrRelExists) {
		t.Fatalf("batch re-put of a mint rel err = %v, want ErrRelExists", err)
	}
	if err := ts.ForEachExAdjacentRelIDByDepth(a, false, DepthAll, nil); err == nil {
		t.Fatal("ForEachExAdjacentRelIDByDepth(nil fn) = nil")
	}
	if err := ts.ForEachExAdjacentRelIDByDepth(a, false, ShardDepth(9), func(types.RelID) bool { return true }); !errors.Is(err, ErrInvalidShardDepth) {
		t.Fatalf("ForEachExAdjacentRelIDByDepth(bad depth) err = %v, want ErrInvalidShardDepth", err)
	}

	if err := ts.Clear(); err != nil {
		t.Fatalf("Clear: %v", err)
	}
	if !ts.relsByMint.fanOut.Load() {
		t.Fatal("Clear dropped the marker of an open with the option")
	}
	if raw, err := ts.MetaGet(relsByMintMarkerKey); err != nil || raw == nil {
		t.Fatalf("marker after Clear = %v, %v", raw, err)
	}

	// Break: an open without the option on a marked store: Clear unmarks it.
	dir := t.TempDir()
	on := openMintStore(t, mintCfg(dir, true))
	if err := on.Close(); err != nil {
		t.Fatal(err)
	}
	off := openMintStore(t, mintCfg(dir, false))
	if !off.relsByMint.fanOut.Load() {
		t.Fatal("marked store opened without the option does not fan out")
	}
	if err := off.Clear(); err != nil {
		t.Fatal(err)
	}
	if off.relsByMint.fanOut.Load() {
		t.Fatal("Clear without the option kept the fan-out")
	}
}

// TestRelsByMint_DeleteNodeCascadeNoDangling (row 97): store.DeleteNodeCascade
// of a reference node deletes its mint-placed rels with their row and both
// legs, as DeleteNodeWithHistory does. Break cases: the rel on a cold (closed)
// mint shard, both directions (a->b and b->a), a self-loop.
func TestRelsByMint_DeleteNodeCascadeNoDangling(t *testing.T) {
	t.Parallel()
	ng, rg := tieredNodeGen(t), tieredRelGen(t)
	ts := openMintStore(t, mintCfg(t.TempDir(), true))
	a := putMintNode(t, ts, types.NodeID(ng.Generate()), mintCaseTok)
	b := putMintNode(t, ts, types.NodeID(ng.Generate()), mintCaseTok)
	w := hotShardName(ts)
	cold := types.RelID(rg.Generate())
	putMintRel(t, ts, cold, 1, a, b)
	rotate(t, ts)
	demoteToCold(ts, w)
	if closeColdStores(t, ts) != 1 {
		t.Fatal("setup: cold mint shard not closed")
	}
	hot := hotShardName(ts)
	out, in, loop := types.RelID(rg.Generate()), types.RelID(rg.Generate()), types.RelID(rg.Generate())
	putMintRel(t, ts, out, 1, a, b)
	putMintRel(t, ts, in, 2, b, a)
	putMintRel(t, ts, loop, 3, a, a)

	if err := ts.DeleteNodeCascade(a); err != nil {
		t.Fatalf("DeleteNodeCascade(a): %v", err)
	}
	if _, err := ts.GetNode(a); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("GetNode(a) after cascade err = %v, want ErrNodeNotFound", err)
	}
	for _, c := range []struct {
		what string
		id   types.RelID
	}{{"cold a->b", cold}, {"hot a->b", out}, {"hot b->a", in}, {"self-loop", loop}} {
		if rel, err := ts.GetRelationship(c.id); !errors.Is(err, ErrRelNotFound) {
			t.Errorf("%s: GetRelationship after cascade = %v, %v, want ErrRelNotFound", c.what, rel, err)
		}
	}
	got, err := ts.IncomingRelationships(b, 0)
	if err != nil {
		t.Fatal(err)
	}
	wantRelIDs(t, "IncomingRelationships(b)", got)
	got, err = ts.OutgoingRelationships(b, 0)
	if err != nil {
		t.Fatal(err)
	}
	wantRelIDs(t, "OutgoingRelationships(b)", got)
	hs := eventShardStore(t, ts, hot)
	if len(hs.OutgoingRelIDs(a.SnowflakeID())) != 0 || len(hs.IncomingRelIDs(a.SnowflakeID(), 0)) != 0 ||
		len(hs.OutgoingRelIDs(b.SnowflakeID())) != 0 || len(hs.IncomingRelIDs(b.SnowflakeID(), 0)) != 0 {
		t.Error("hot mint shard keeps a leg of a deleted rel")
	}
	if _, err := ts.GetNode(b); err != nil {
		t.Fatalf("GetNode(b) after cascade of a: %v", err)
	}
	res, err := ts.RunRepair()
	if err != nil {
		t.Fatal(err)
	}
	if res.MissingInEntries != 0 || res.StaleInEntries != 0 || res.OrphanedInEntries != 0 {
		t.Fatalf("repair after cascade = %+v, want no findings", res)
	}
}

// TestRelsByMint_ColdEdgeOutgoingCost (PREEPOCHREL, sigma's question): Outgoing
// of a reference node finds an edge on a closed cold shard, and opens every
// cold shard to do so, per call: the read cannot tell which shards hold its
// node's edges. MaxOpenColdShards keeps the handles for the next call.
func TestRelsByMint_ColdEdgeOutgoingCost(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	const days = 30
	for _, keep := range []int{0, days} {
		cfg := mintCfg(t.TempDir(), true)
		cfg.MaxOpenColdShards = keep
		ng, rg := tieredNodeGen(t), tieredRelGen(t)
		ts := openMintStore(t, cfg)
		a := putMintNode(t, ts, types.NodeID(ng.Generate()), mintCaseTok)
		b := putMintNode(t, ts, types.NodeID(ng.Generate()), mintCaseTok)
		r := types.RelID(rg.Generate())
		putMintRel(t, ts, r, 1, a, b)
		var names []string
		for range days {
			names = append(names, hotShardName(ts))
			rotate(t, ts)
		}
		for _, n := range names {
			demoteToCold(ts, n)
		}
		closeColdStores(t, ts)
		if n := coldShardsWithOpenStore(ts); n != 0 {
			t.Fatalf("setup: %d cold shards open", n)
		}
		for call := range 3 {
			start := time.Now()
			got, err := ts.OutgoingRelationships(a, 0)
			if err != nil {
				t.Fatal(err)
			}
			wantRelIDs(t, "cold edge: Outgoing", got, r)
			t.Logf("MaxOpenColdShards %d, %d cold shards, call %d: %v, %d left open", keep, days, call, time.Since(start), coldShardsWithOpenStore(ts))
		}
		if got := coldShardsWithOpenStore(ts); keep > 0 && got == 0 {
			t.Fatalf("MaxOpenColdShards %d: no cold handle kept", keep)
		}
		if err := ts.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
