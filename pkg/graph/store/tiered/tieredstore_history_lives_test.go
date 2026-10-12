package tiered

import (
	"strings"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/generatedcreate"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Backlog 49 at the store: a relationship whose earlier life lies on an event
// shard that rotated out and went cold, re-imported with a start node on the
// new hot shard, answers its whole history on every store history door; the
// truncation keeps the newest versions across both lives; Clear forgets the
// record. The cross-backend door test is
// core.TestReimportAcrossShardsKeepsEarlierLife.
func TestHistoryLives_RelReimportOntoNewEventShard(t *testing.T) {
	ts := newTestTieredStore(t)
	_, _, signalTok := installDefaultTestLabelRegistry(t, ts)
	gen := tieredNodeGen(t)

	a := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	e := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	for _, n := range []*types.Node{a, e} {
		if err := ts.PutNode(n); err != nil {
			t.Fatal(err)
		}
	}
	r1 := types.NewRelationship(types.RelID(tieredRelGen(t).Generate()), 1, a.ID(), e.ID())
	r1.SetVersion(1)
	rid := r1.ID()
	if err := ts.PutRelationship(r1); err != nil {
		t.Fatal(err)
	}
	if err := ts.PutRelVersion(rid, 1, r1); err != nil {
		t.Fatal(err)
	}
	if err := ts.DeleteRelationship(rid); err != nil {
		t.Fatal(err)
	}

	ts.MuForTest().RLock()
	oldName := ts.HotShardForTest().Name()
	ts.MuForTest().RUnlock()
	forceRotation(t, ts)
	demoteToCold(ts, oldName)

	b := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	if err := ts.PutNode(b); err != nil {
		t.Fatal(err)
	}
	r2 := types.NewRelationship(rid, 1, b.ID(), e.ID())
	r2.SetVersion(2)
	if err := ts.PutRelationship(r2); err != nil {
		t.Fatalf("re-import: %v", err)
	}
	if err := ts.PutRelVersion(rid, 2, r2); err != nil {
		t.Fatal(err)
	}
	if !ts.relLives.has(rid.SnowflakeID()) {
		t.Fatal("re-import onto another shard not recorded")
	}

	versions := func(rows []*types.Relationship) []uint32 {
		var out []uint32
		for _, r := range rows {
			out = append(out, r.Version())
		}
		return out
	}
	assertVersions := func(door string, rows []*types.Relationship, err error, want ...uint32) {
		t.Helper()
		if err != nil {
			t.Fatalf("%s: %v", door, err)
		}
		if got := versions(rows); len(got) != len(want) || (len(got) > 0 && (got[0] != want[0] || got[len(got)-1] != want[len(want)-1])) {
			t.Fatalf("%s: versions %v, want %v", door, got, want)
		}
	}
	h, err := ts.GetRelHistory(rid)
	assertVersions("GetRelHistory", h, err, 1, 2)
	page, err := ts.RelHistoryVersionsFrom(rid, 0, 10)
	assertVersions("RelHistoryVersionsFrom", page, err, 1, 2)
	if v1, err := ts.GetRelVersion(rid, 1); err != nil || v1.StartNodeID() != a.ID() {
		t.Fatalf("GetRelVersion(1) = %v, %v; want the earlier life from %d", v1, err, a.ID())
	}
	if has, err := ts.HasRelHistory(rid); err != nil || !has {
		t.Fatalf("HasRelHistory = %v, %v", has, err)
	}
	if _, _, has, err := ts.RelHistoryStamps(rid); err != nil || !has {
		t.Fatalf("RelHistoryStamps: has %v err %v", has, err)
	}

	// Truncation keeps the newest version across both lives.
	if err := ts.TruncateRelHistory(rid, 1); err != nil {
		t.Fatal(err)
	}
	h, err = ts.GetRelHistory(rid)
	assertVersions("GetRelHistory after TruncateRelHistory(1)", h, err, 2)

	if err := ts.Clear(); err != nil {
		t.Fatal(err)
	}
	if ts.relLives.has(rid.SnowflakeID()) || ts.relLives.n.Load() != 0 {
		t.Fatal("Clear kept the earlier-lives record")
	}
}

// A re-import whose earlier history its new row's reads already reach is not
// recorded (the history reads keep their one-shard route): the same event
// shard, or the reference shard over history in the archive. A node whose
// earlier life lies in the archive and whose new row is an event row is
// recorded.
func TestHistoryLives_RecordsOnlyUnreachedHistory(t *testing.T) {
	ts, caseTok, signalTok := newArchiveWriteTestStore(t)
	archive := mustArchiveStore(t, ts)
	gen := tieredNodeGen(t)

	ev := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	ref := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	for _, n := range []*types.Node{ev, ref} {
		if err := ts.PutNode(n); err != nil {
			t.Fatal(err)
		}
	}

	// Same event shard.
	same := types.NewRelationship(types.RelID(tieredRelGen(t).Generate()), 1, ev.ID(), ev.ID())
	if err := ts.PutRelationship(same); err != nil {
		t.Fatal(err)
	}
	if err := ts.PutRelVersion(same.ID(), 0, same); err != nil {
		t.Fatal(err)
	}
	if err := ts.DeleteRelationship(same.ID()); err != nil {
		t.Fatal(err)
	}
	if err := ts.PutRelationship(same); err != nil {
		t.Fatal(err)
	}

	// Reference row over archive history.
	pair := types.NewRelationship(types.RelID(tieredRelGen(t).Generate()), 1, ref.ID(), ref.ID())
	if err := archive.PutRelVersion(pair.ID(), 0, pair); err != nil {
		t.Fatal(err)
	}
	if err := ts.PutRelationshipsBatch([]*types.Relationship{pair}); err != nil {
		t.Fatal(err)
	}
	for name, id := range map[string]types.RelID{"same shard": same.ID(), "reference over archive": pair.ID()} {
		if ts.relLives.has(id.SnowflakeID()) {
			t.Errorf("%s: recorded", name)
		}
		if h, err := ts.GetRelHistory(id); err != nil || len(h) != 1 {
			t.Errorf("%s: GetRelHistory = %d rows, %v; want 1", name, len(h), err)
		}
	}

	// Event node over archive history.
	n := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	if err := archive.PutNodeVersion(n.ID(), 0, types.NewNode(n.ID(), caseTok, nil)); err != nil {
		t.Fatal(err)
	}
	if err := ts.PutNodesBatch([]*types.Node{n}); err != nil {
		t.Fatal(err)
	}
	if !ts.nodeLives.has(n.ID().SnowflakeID()) {
		t.Fatal("event node over archive history: not recorded")
	}
	if h, err := ts.GetNodeHistory(n.ID()); err != nil || len(h) != 1 {
		t.Fatalf("GetNodeHistory = %d rows, %v; want 1", len(h), err)
	}
}

// A store written before the record existed (a cross-shard re-import with no
// record and no backfill marker) loses the earlier lives until its next open,
// whose one-time backfill records the re-imported IDs — and only those: an ID
// deleted and never re-imported, and a same-shard re-import, stay unrecorded.
// The marker is then set, so the walk runs once.
func TestHistoryLives_BackfillAtOpen(t *testing.T) {
	dir := t.TempDir()
	open := func() *Store {
		t.Helper()
		ts, err := New(Config{DataDir: dir, RefLabels: []string{"Case", "User"}, ShardWindow: 7 * 24 * time.Hour, FlushInterval: 1<<63 - 1})
		if err != nil {
			t.Fatal(err)
		}
		return ts
	}
	ts := open()
	caseTok, _, signalTok := installDefaultTestLabelRegistry(t, ts)
	gen := tieredNodeGen(t)
	fresh := generatedcreate.FreshGraphID() // writes without the record: the pre-fix layout

	a := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	e := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	ref := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	for _, n := range []*types.Node{a, e, ref} {
		if err := ts.PutNode(n); err != nil {
			t.Fatal(err)
		}
	}
	lifeOver := func(r *types.Relationship) {
		t.Helper()
		if err := ts.PutRelVersion(r.ID(), 0, r); err != nil {
			t.Fatal(err)
		}
		if err := ts.DeleteRelationship(r.ID()); err != nil {
			t.Fatal(err)
		}
	}
	moved := types.NewRelationship(types.RelID(tieredRelGen(t).Generate()), 1, a.ID(), e.ID())
	same := types.NewRelationship(types.RelID(tieredRelGen(t).Generate()), 1, a.ID(), e.ID())
	gone := types.NewRelationship(types.RelID(tieredRelGen(t).Generate()), 1, a.ID(), e.ID())
	for _, r := range []*types.Relationship{moved, same, gone} {
		if err := ts.PutRelationship(r); err != nil {
			t.Fatal(err)
		}
		lifeOver(r)
	}
	if err := ts.PutRelationshipGeneratedID(types.NewRelationship(moved.ID(), 1, ref.ID(), e.ID()), fresh); err != nil {
		t.Fatal(err)
	}
	if err := ts.PutRelationshipGeneratedID(same, fresh); err != nil {
		t.Fatal(err)
	}
	node := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	if err := ts.PutNode(node); err != nil {
		t.Fatal(err)
	}
	if err := ts.PutNodeVersion(node.ID(), 0, node); err != nil {
		t.Fatal(err)
	}
	if err := ts.DeleteNode(node.ID()); err != nil {
		t.Fatal(err)
	}
	if err := ts.PutNodeGeneratedID(types.NewNode(node.ID(), caseTok, nil), fresh); err != nil {
		t.Fatal(err)
	}
	if err := ts.MetaSet(historyLivesBackfilledKey, nil); err != nil { // a store from before the marker
		t.Fatal(err)
	}
	histories := func(ts *Store) (int, int) {
		t.Helper()
		rh, err := ts.GetRelHistory(moved.ID())
		if err != nil {
			t.Fatal(err)
		}
		nh, err := ts.GetNodeHistory(node.ID())
		if err != nil {
			t.Fatal(err)
		}
		return len(rh), len(nh)
	}
	if r, n := histories(ts); r != 0 || n != 0 {
		t.Fatalf("pre-fix layout: %d rel and %d node history rows reachable, want 0 and 0", r, n)
	}
	if err := ts.Close(); err != nil {
		t.Fatal(err)
	}

	ts = open()
	t.Cleanup(func() { _ = ts.Close() })
	if r, n := histories(ts); r != 1 || n != 1 {
		t.Fatalf("after the backfill: %d rel and %d node history rows, want 1 and 1", r, n)
	}
	if !ts.relLives.has(moved.ID().SnowflakeID()) || ts.relLives.n.Load() != 1 || ts.nodeLives.n.Load() != 1 {
		t.Fatalf("backfill recorded %d rels / %d nodes, want exactly the moved ones", ts.relLives.n.Load(), ts.nodeLives.n.Load())
	}
	if v, err := ts.MetaGet(historyLivesBackfilledKey); err != nil || len(v) == 0 {
		t.Fatalf("backfill marker = %v, %v; want set", v, err)
	}
}

// The generated-ID batch door with an invalid proof is the caller-ID door:
// it records a re-import onto another shard.
func TestHistoryLives_GeneratedBatchWithoutProofRecords(t *testing.T) {
	ts := newTestTieredStore(t)
	caseTok, _, signalTok := installDefaultTestLabelRegistry(t, ts)
	gen := tieredNodeGen(t)
	a := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	ref := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	for _, n := range []*types.Node{a, ref} {
		if err := ts.PutNode(n); err != nil {
			t.Fatal(err)
		}
	}
	r := types.NewRelationship(types.RelID(tieredRelGen(t).Generate()), 1, a.ID(), a.ID())
	if err := ts.PutRelationship(r); err != nil {
		t.Fatal(err)
	}
	if err := ts.PutRelVersion(r.ID(), 0, r); err != nil {
		t.Fatal(err)
	}
	if err := ts.DeleteRelationship(r.ID()); err != nil {
		t.Fatal(err)
	}
	moved := types.NewRelationship(r.ID(), 1, ref.ID(), a.ID())
	if err := ts.PutRelationshipsBatchGeneratedID([]*types.Relationship{moved}, generatedcreate.Proof{}); err != nil {
		t.Fatal(err)
	}
	if !ts.relLives.has(r.ID().SnowflakeID()) {
		t.Fatal("batch without proof: re-import onto another shard not recorded")
	}
}

// The record survives a restart; a malformed record fails the open.
func TestHistoryLives_PersistedAndValidatedAtOpen(t *testing.T) {
	// write opens a fresh store, applies fn, closes it; then the store is
	// opened again.
	reopen := func(t *testing.T, fn func(ts *Store) error) (*Store, error) {
		t.Helper()
		dir := t.TempDir()
		open := func() (*Store, error) {
			return New(Config{DataDir: dir, RefLabels: []string{"Case"}, ShardWindow: 7 * 24 * time.Hour})
		}
		ts, err := open()
		if err != nil {
			t.Fatal(err)
		}
		if err := fn(ts); err != nil {
			t.Fatal(err)
		}
		if err := ts.Close(); err != nil {
			t.Fatal(err)
		}
		ts, err = open()
		if ts != nil {
			t.Cleanup(func() { _ = ts.Close() })
		}
		return ts, err
	}
	ts, err := reopen(t, func(ts *Store) error {
		if err := ts.recordHistoryLives(&ts.relLives, relLivesMetaPrefix, 42); err != nil {
			return err
		}
		return ts.recordHistoryLives(&ts.nodeLives, nodeLivesMetaPrefix, 43)
	})
	if err != nil {
		t.Fatal(err)
	}
	if !ts.relLives.has(42) || !ts.nodeLives.has(43) || ts.relLives.has(43) || ts.nodeLives.has(42) {
		t.Fatal("records not reloaded into their own sets")
	}
	for _, bad := range []string{"rel/x", "node/-1", "rel/0", "edge/7", ""} {
		_, err := reopen(t, func(ts *Store) error { return ts.MetaSet(historyLivesMetaPrefix+bad, []byte{1}) })
		if err == nil || !strings.Contains(err.Error(), "malformed") {
			t.Fatalf("open with record %q: err %v, want malformed", bad, err)
		}
	}
}
