package tiered

import (
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// EMPTYBYID, writes: a write that names an ID the proven-empty shard's window
// owns, and changes only a row that ID already has (an update, a delete, a
// relationship's endpoint, a history trim), finds no row and answers its miss.
// It must not open the shard, wake its no-rows mark and save the catalog to
// answer "not found" (every such miss was an open, a mark save and, at the
// check-in or the next idle sweep, a close and a re-seal). A write that creates a
// row in the window still opens and wakes it.
// Every count is a shard open or a checkout of ShardAccessStats, never time.

// byIDWriteProbe is one write door naming an ID the empty shard would own.
type byIDWriteProbe struct {
	name string
	run  func(f *emptyByID) error
	want error // the sentinel its miss answers; nil: it succeeds as a no-op
}

// byIDWriteProbes lists the write doors that name a missing ID. The sentinels
// are what each door answered when it read the shard (red, before the skip).
func byIDWriteProbes() []byIDWriteProbe {
	missNode := func(f *emptyByID) *types.Node { return types.NewNode(f.missNode, mintSignalTok, nil) }
	missRel := func(f *emptyByID) *types.Relationship { return types.NewRelationship(f.missRel, 1, f.sig, f.a) }
	return []byIDWriteProbe{
		// Node updates and deletes.
		{"ReplaceNode", func(f *emptyByID) error { return f.ts.ReplaceNode(missNode(f)) }, ErrNodeNotFound},
		{"DeleteNode", func(f *emptyByID) error { return f.ts.DeleteNode(f.missNode) }, ErrNodeNotFound},
		{"DeleteNodeCascade", func(f *emptyByID) error { return f.ts.DeleteNodeCascade(f.missNode) }, ErrNodeNotFound},
		{"DeleteNodesBatch", func(f *emptyByID) error { return f.ts.DeleteNodesBatch([]types.NodeID{f.missNode}) }, ErrNodeNotFound},
		{"DeleteNodesBatch beside a stored node", func(f *emptyByID) error {
			return f.ts.DeleteNodesBatch([]types.NodeID{f.missNode, f.lone})
		}, ErrNodeNotFound},
		{"AddNodeLabelToken", func(f *emptyByID) error {
			return f.ts.AddNodeLabelToken(f.missNode, mintSignalTok, missNode(f))
		}, ErrNodeNotFound},
		{"RemoveNodeLabelToken", func(f *emptyByID) error {
			return f.ts.RemoveNodeLabelToken(f.missNode, mintSignalTok, missNode(f))
		}, ErrNodeNotFound},
		{"AddNodeLabelTokenWithHistory", func(f *emptyByID) error {
			return f.ts.AddNodeLabelTokenWithHistory(f.missNode, mintSignalTok, missNode(f), missNode(f).Version(), missNode(f))
		}, ErrNodeNotFound},
		{"RemoveNodeLabelTokenWithHistory", func(f *emptyByID) error {
			return f.ts.RemoveNodeLabelTokenWithHistory(f.missNode, mintSignalTok, missNode(f), missNode(f).Version(), missNode(f))
		}, ErrNodeNotFound},
		{"ReplaceNodeWithHistory", func(f *emptyByID) error {
			return f.ts.ReplaceNodeWithHistory(missNode(f), missNode(f).Version(), missNode(f))
		}, ErrNodeNotFound},
		{"DeleteNodeWithHistory", func(f *emptyByID) error {
			return f.ts.DeleteNodeWithHistory(f.missNode, missNode(f).Version(), missNode(f), nil)
		}, ErrNodeNotFound},
		{"TruncateNodeHistory", func(f *emptyByID) error { return f.ts.TruncateNodeHistory(f.missNode, 1) }, nil},
		// Relationship updates and deletes by ID.
		{"ReplaceRelationship", func(f *emptyByID) error { return f.ts.ReplaceRelationship(missRel(f)) }, ErrRelNotFound},
		{"DeleteRelationship", func(f *emptyByID) error { return f.ts.DeleteRelationship(f.missRel) }, ErrRelNotFound},
		{"UndoRelationshipCreate", func(f *emptyByID) error { return f.ts.UndoRelationshipCreate(f.missRel) }, ErrRelNotFound},
		{"DeleteRelationshipsBatch", func(f *emptyByID) error {
			return f.ts.DeleteRelationshipsBatch([]types.RelID{f.missRel})
		}, ErrRelNotFound},
		{"DeleteRelationshipsBatch beside a stored rel", func(f *emptyByID) error {
			return f.ts.DeleteRelationshipsBatch([]types.RelID{f.missRel, f.rel})
		}, ErrRelNotFound},
		{"ReplaceRelWithHistory", func(f *emptyByID) error {
			return f.ts.ReplaceRelWithHistory(missRel(f), missRel(f).Version(), missRel(f))
		}, ErrRelNotFound},
		{"DeleteRelWithHistory", func(f *emptyByID) error {
			return f.ts.DeleteRelWithHistory(f.missRel, missRel(f).Version(), missRel(f))
		}, ErrRelNotFound},
		{"TruncateRelHistory", func(f *emptyByID) error { return f.ts.TruncateRelHistory(f.missRel, 1) }, nil},
		// A relationship create whose endpoint is the missing node.
		{"PutRelationship missing start", func(f *emptyByID) error {
			return f.ts.PutRelationship(types.NewRelationship(f.lateRel, 1, f.missNode, f.a))
		}, ErrNodeNotFound},
		{"PutRelationship missing end", func(f *emptyByID) error {
			return f.ts.PutRelationship(types.NewRelationship(f.lateRel, 1, f.sig, f.missNode))
		}, ErrNodeNotFound},
		{"PutRelationshipsBatch missing end", func(f *emptyByID) error {
			return f.ts.PutRelationshipsBatch([]*types.Relationship{types.NewRelationship(f.lateRel, 1, f.sig, f.missNode)})
		}, ErrNodeNotFound},
		{"PutRelationshipsBatch missing start", func(f *emptyByID) error {
			return f.ts.PutRelationshipsBatch([]*types.Relationship{types.NewRelationship(f.lateRel, 1, f.missNode, f.a)})
		}, ErrNodeNotFound},
	}
}

// answers reports whether err is the answer a probe pins: its sentinel, or no
// error for a no-op door.
func (p byIDWriteProbe) answers(err error) bool {
	if p.want == nil {
		return err == nil
	}
	return errors.Is(err, p.want)
}

// assertNoColdWork fails when the cold tier opened, was checked out or was
// written to since before.
func assertNoColdWork(t *testing.T, f *emptyByID, before ShardAccessStats, what string) {
	t.Helper()
	after := f.ts.ShardAccessStats()
	if d := after.ColdOpens - before.ColdOpens; d != 0 {
		t.Errorf("%s: the empty shard was opened %d times", what, d)
	}
	if d := after.Cold - before.Cold; d != 0 {
		t.Errorf("%s: the empty shard was checked out %d times", what, d)
	}
	if d := after.ColdWrites - before.ColdWrites; d != 0 {
		t.Errorf("%s: the empty shard was checked out writable %d times", what, d)
	}
}

// TestEmptyByID_WritesNamingAMissKeepTheShardClosed: every write door that names
// a missing ID in the empty shard's window opens nothing, checks nothing out and
// leaves the mark, in memory and in the saved catalog, and answers its miss (the
// sentinel the door pins). Then the same doors run once with the mark off, the
// shard read as before the skip: the answers are the same.
func TestEmptyByID_WritesNamingAMissKeepTheShardClosed(t *testing.T) {
	f := newEmptyByID(t)
	probes := byIDWriteProbes()
	got := make([]string, len(probes))
	for i, p := range probes {
		t.Run(p.name, func(t *testing.T) {
			f.reseal(t)
			before := f.ts.ShardAccessStats()
			err := p.run(f)
			got[i] = ans(err)
			assertNoColdWork(t, f, before, "marked")
			if !f.es.noRows.Load() {
				t.Error("the write dropped the empty shard's mark")
			}
			if !f.ts.catalogEntryNoRows(t, f.cfg, f.es.name) {
				t.Error("the saved catalog no longer marks the empty shard")
			}
			if !p.answers(err) {
				t.Errorf("answered %q, want %v", got[i], p.want)
			}
		})
	}

	f.reseal(t)
	f.es.noRows.Store(false)
	for i, p := range probes {
		if want := ans(p.run(f)); got[i] != want {
			t.Errorf("%s: answer with the mark %q, with the shard read %q", p.name, got[i], want)
		}
	}
	f.es.noRows.Store(true) // the control wrote nothing: the shard is still empty
	f.reseal(t)
	// A batch that names a miss and a stored node deletes neither.
	if _, err := f.ts.GetNode(f.lone); err != nil {
		t.Errorf("the stored node of the failed batch delete is gone: %v", err)
	}
}

// TestEmptyByID_RepeatedWriteMissesAndSweepsOpenNothing: three rounds of every
// write miss, with an idle sweep between, cost no open of the empty shard and
// leave it closed, marked, without a handle in the census.
func TestEmptyByID_RepeatedWriteMissesAndSweepsOpenNothing(t *testing.T) {
	f := newEmptyByID(t)
	before := f.ts.ShardAccessStats()
	for round := range 3 {
		for _, p := range byIDWriteProbes() {
			if err := p.run(f); !p.answers(err) {
				t.Fatalf("round %d: %s answered %v, want %v", round, p.name, err, p.want)
			}
		}
		f.ts.closeIdleShards()
		assertNoColdWork(t, f, before, "round")
	}
	if st := f.ts.ShardAccessStats(); st.ColdHandles != (TierHandles{}) {
		t.Errorf("after the rounds %+v cold handles are open; want none", st.ColdHandles)
	}
	f.es.shardMu.Lock()
	open := f.es.store != nil
	f.es.shardMu.Unlock()
	if open || !f.es.noRows.Load() || !f.ts.catalogEntryNoRows(t, f.cfg, f.es.name) {
		t.Errorf("after the rounds the empty shard is open %v, marked %v; want closed and marked", open, f.es.noRows.Load())
	}
}

// TestEmptyByID_WritesToACrossShardRelFindItWithoutOpening: a relationship minted
// in the empty window but stored on its start node's shard is found by the write
// doors that name it, replaced, deleted, re-created and undone, and the empty
// shard stays closed.
func TestEmptyByID_WritesToACrossShardRelFindItWithoutOpening(t *testing.T) {
	f := newEmptyByID(t)
	before := f.ts.ShardAccessStats()
	if err := f.ts.ReplaceRelationship(types.NewRelationship(f.crossRel, 1, f.sig, f.a)); err != nil {
		t.Fatalf("ReplaceRelationship of the cross-shard rel: %v", err)
	}
	if err := f.ts.DeleteRelationship(f.crossRel); err != nil {
		t.Fatalf("DeleteRelationship of the cross-shard rel: %v", err)
	}
	if _, err := f.ts.GetRelationship(f.crossRel); !errors.Is(err, ErrRelNotFound) {
		t.Errorf("GetRelationship after the delete = %v, want ErrRelNotFound", err)
	}
	if rels, err := f.ts.IncomingRelationships(f.a, 0); err != nil || len(rels) != 0 {
		t.Errorf("IncomingRelationships(a) = %s, %v; want no rel after the delete", fmtRels(rels), err)
	}
	// Create it again under the same ID (the create probes skip the shard too),
	// undo the create, create it a third time and delete it in a batch with a
	// rel the hot shard owns.
	if err := f.ts.PutRelationship(types.NewRelationship(f.crossRel, 1, f.sig, f.a)); err != nil {
		t.Fatalf("PutRelationship of the cross-shard rel again: %v", err)
	}
	if err := f.ts.UndoRelationshipCreate(f.crossRel); err != nil {
		t.Fatalf("UndoRelationshipCreate of the cross-shard rel: %v", err)
	}
	if err := f.ts.PutRelationship(types.NewRelationship(f.crossRel, 1, f.sig, f.a)); err != nil {
		t.Fatalf("PutRelationship of the cross-shard rel a third time: %v", err)
	}
	if err := f.ts.DeleteRelationshipsBatch([]types.RelID{f.crossRel, f.rel}); err != nil {
		t.Fatalf("DeleteRelationshipsBatch of the cross-shard rel: %v", err)
	}
	for _, id := range []types.RelID{f.crossRel, f.rel} {
		if _, err := f.ts.GetRelationship(id); !errors.Is(err, ErrRelNotFound) {
			t.Errorf("GetRelationship(%d) after the batch delete = %v, want ErrRelNotFound", id, err)
		}
	}
	assertNoColdWork(t, f, before, "cross-shard rel writes")
	if !f.es.noRows.Load() {
		t.Error("a write that found its rel elsewhere dropped the empty shard's mark")
	}
}

// TestEmptyByID_WritesFindACrossShardRelOnAnotherClosedShard: a relationship
// minted in the empty window but stored with its start node on another closed
// cold shard is found by the write doors through the probe of the other shards
// (the hot shard, which stands in for the empty one, does not hold it): the shard
// that holds it opens, the empty shard does not, and the rel is replaced, then
// deleted.
func TestEmptyByID_WritesFindACrossShardRelOnAnotherClosedShard(t *testing.T) {
	cfg := mintCfg(t.TempDir(), true)
	ts := openMintStore(t, cfg)
	ng, rg := tieredNodeGen(t), tieredRelGen(t)
	relID := types.RelID(rg.Generate()) // minted in the first window, which stays empty
	empty := hotShardName(ts)
	rotate(t, ts)
	holder := hotShardName(ts)
	a := putMintNode(t, ts, types.NodeID(ng.Generate()), mintCaseTok)
	s := putMintNode(t, ts, types.NodeID(ng.Generate()), mintSignalTok)
	putMintRel(t, ts, relID, 1, s, a) // stored on the second window's shard, with s
	rotate(t, ts)
	demoteToCold(ts, empty)
	demoteToCold(ts, holder)
	closeColdStores(t, ts)
	ts.mu.RLock()
	es, other := ts.eventShards[empty], ts.eventShards[holder]
	ts.mu.RUnlock()
	if !es.noRows.Load() || other.noRows.Load() {
		t.Fatalf("setup: empty shard marked %v, holder marked %v", es.noRows.Load(), other.noRows.Load())
	}
	if ts.timestampToEventShardEntry(relID.SnowflakeID()) != es {
		t.Fatal("setup: the rel was not minted in the empty shard's window")
	}

	opens, otherOpens := es.coldOpens.Load(), other.coldOpens.Load()
	if err := ts.ReplaceRelationship(types.NewRelationship(relID, 1, s, a)); err != nil {
		t.Fatalf("ReplaceRelationship of the rel on the other shard: %v", err)
	}
	if err := ts.DeleteRelationship(relID); err != nil {
		t.Fatalf("DeleteRelationship of the rel on the other shard: %v", err)
	}
	if _, err := ts.GetRelationship(relID); !errors.Is(err, ErrRelNotFound) {
		t.Errorf("GetRelationship after the delete = %v, want ErrRelNotFound", err)
	}
	if d := es.coldOpens.Load() - opens; d != 0 {
		t.Errorf("the empty shard was opened %d times", d)
	}
	if d := other.coldOpens.Load() - otherOpens; d == 0 {
		t.Error("the shard that holds the rel was not opened: the write did not find it there")
	}
	if !es.noRows.Load() {
		t.Error("the empty shard lost its mark")
	}
}

// TestEmptyByID_WriteDoorsThatCreateARowStillWake: a write door that stores a row
// under an ID the empty shard's window owns (a history version of an ID with no
// current row) is not an update of an existing row: it opens the shard, saves
// the cleared mark before the write and the row reads back from the shard.
func TestEmptyByID_WriteDoorsThatCreateARowStillWake(t *testing.T) {
	t.Run("PutNodeVersion", func(t *testing.T) {
		f := newEmptyByID(t)
		v := types.NewNode(f.missNode, mintSignalTok, nil)
		before := f.coldOpens()
		if err := f.ts.PutNodeVersion(f.missNode, v.Version(), v); err != nil {
			t.Fatalf("PutNodeVersion: %v", err)
		}
		if d := f.coldOpens() - before; d != 1 {
			t.Errorf("PutNodeVersion opened the shard %d times, want 1", d)
		}
		if f.es.noRows.Load() || f.ts.catalogEntryNoRows(t, f.cfg, f.es.name) {
			t.Error("the write left the shard marked empty, in memory or in the saved catalog")
		}
		closeColdStores(t, f.ts)
		if f.es.noRows.Load() {
			t.Error("a shard holding a history row is marked empty after its close")
		}
		if h, err := f.ts.GetNodeHistory(f.missNode); err != nil || len(h) != 1 {
			t.Errorf("GetNodeHistory after the write = %d rows, %v; want 1", len(h), err)
		}
	})
	t.Run("PutRelVersion", func(t *testing.T) {
		f := newEmptyByID(t)
		v := types.NewRelationship(f.missRel, 1, f.sig, f.a)
		before := f.coldOpens()
		if err := f.ts.PutRelVersion(f.missRel, v.Version(), v); err != nil {
			t.Fatalf("PutRelVersion: %v", err)
		}
		if f.coldOpens()-before == 0 {
			t.Error("PutRelVersion wrote the history row without opening the window's shard")
		}
		if f.es.noRows.Load() || f.ts.catalogEntryNoRows(t, f.cfg, f.es.name) {
			t.Error("the write left the shard marked empty, in memory or in the saved catalog")
		}
		closeColdStores(t, f.ts)
		if h, err := f.ts.GetRelHistory(f.missRel); err != nil || len(h) != 1 {
			t.Errorf("GetRelHistory after the write = %d rows, %v; want 1", len(h), err)
		}
	})
}

// TestEmptyByID_UpdatesAfterALateWriteReachTheWokenShard: after a create wakes the
// shard, the update and delete doors that name the new ID route to it again, not
// to the hot shard that answers a miss.
func TestEmptyByID_UpdatesAfterALateWriteReachTheWokenShard(t *testing.T) {
	f := newEmptyByID(t)
	if err := f.ts.PutNode(types.NewNode(f.lateNode, mintSignalTok, nil)); err != nil {
		t.Fatalf("late PutNode: %v", err)
	}
	if f.es.noRows.Load() {
		t.Fatal("the late write left the shard marked empty")
	}
	changed := types.NewNode(f.lateNode, mintSignalTok, nil)
	_ = changed.SetProperty("k", "v")
	if err := f.ts.ReplaceNode(changed); err != nil {
		t.Fatalf("ReplaceNode of the late node: %v", err)
	}
	if n, err := f.ts.GetNode(f.lateNode); err != nil {
		t.Fatalf("GetNode(late) after ReplaceNode: %v", err)
	} else if p, _ := n.GetProperty("k"); p != "v" {
		t.Fatalf("GetNode(late) after ReplaceNode: property %v, want v", p)
	}
	if err := f.ts.PutRelationship(types.NewRelationship(f.lateRel, 1, f.lateNode, f.a)); err != nil {
		t.Fatalf("PutRelationship from the late node: %v", err)
	}
	if err := f.ts.DeleteRelationship(f.lateRel); err != nil {
		t.Fatalf("DeleteRelationship of the late rel: %v", err)
	}
	if err := f.ts.DeleteNode(f.lateNode); err != nil {
		t.Fatalf("DeleteNode of the late node: %v", err)
	}
	if _, err := f.ts.GetNode(f.lateNode); !errors.Is(err, ErrNodeNotFound) {
		t.Errorf("GetNode(late) after DeleteNode = %v, want ErrNodeNotFound", err)
	}
}
