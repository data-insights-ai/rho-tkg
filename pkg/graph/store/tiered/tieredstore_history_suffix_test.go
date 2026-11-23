package tiered

import (
	"errors"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	badgerstore "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestNodeHistoryNewestFirst_TwoHoldersReadNothing (TAILSPAN): an archived
// reference node whose history sits on the reference shard (the versions
// before the move) and on the archive (a version after it) ends the
// newest-first walk with ErrHistorySpansShards before any row is visited,
// charged or decoded, so the caller's fold of the list reads each row once.
// The control: the same node before the move, one holder, is walked newest
// first and charged per row visited.
func TestNodeHistoryNewestFirst_TwoHoldersReadNothing(t *testing.T) {
	m := newMoveRace(t, false)
	walk := func() (visited int, history uint64, decodes int64, err error) {
		_, meter := storecontract.WithReadBudget(t.Context(), storecontract.ReadBudget{})
		decodes = badgerstore.CountRowDecodesForTest(func() {
			_, err = m.ts.NodeHistoryNewestFirst(m.a, meter.Tally("suffix"), func(storecontract.VersionTemporalMeta, bool) bool {
				visited++
				return true
			})
		})
		return visited, meter.Work().HistoryExamined, decodes, err
	}
	_, one, _, _ := walk()
	if v, h, _, err := walk(); err != nil || v != 1 || h == 0 {
		t.Fatalf("one holder: %d rows visited, %d charged, %v; want the history row visited and charged", v, h, err)
	}
	if err := m.ts.ArchiveNode(m.a); err != nil {
		t.Fatal(err)
	}
	// Archived, the history still on the reference shard: the sole holder is
	// not the owner and is walked under its own checkout, charged as before.
	if v, h, _, err := walk(); err != nil || v != 1 || h != one {
		t.Fatalf("archived, history on the reference shard: %d rows visited, %d charged, %v; want 1 and %d", v, h, err, one)
	}
	// An event node without history on its own shard: nothing walked.
	_, _, signalTok := installDefaultTestLabelRegistry(t, m.ts)
	ev := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), signalTok, nil)
	if err := m.ts.PutNode(ev); err != nil {
		t.Fatal(err)
	}
	if s, err := m.ts.ShardForNodeIDForTest(ev.ID()); err != nil || s == m.ts.refShard {
		t.Fatalf("the event node routes to the reference shard (%v); want an event shard", err)
	}
	_, em := storecontract.WithReadBudget(t.Context(), storecontract.ReadBudget{})
	ordered, err := m.ts.NodeHistoryNewestFirst(ev.ID(), em.Tally("suffix"), func(storecontract.VersionTemporalMeta, bool) bool {
		t.Error("a node without history was visited")
		return true
	})
	if w := em.Work(); err != nil || !ordered || w.HistoryExamined != 0 || w.DecodedBytes != 0 || w.BufferBytes != 0 {
		t.Fatalf("event node without history: ordered %v, %v, work %+v; want true and no history work", ordered, err, w)
	}
	cur, err := m.ts.GetNode(m.a)
	if err != nil {
		t.Fatal(err)
	}
	next := cur.DeepCopy()
	next.SetVersion(cur.Version() + 1)
	if err := m.ts.ReplaceNodeWithHistory(next, cur.Version(), cur); err != nil {
		t.Fatal(err)
	}
	onRef, err := m.ts.refShard.HasNodeHistory(m.a)
	if err != nil || !onRef {
		t.Fatalf("reference shard holds history: %v, %v; want true (moves leave history on its shard)", onRef, err)
	}
	if rows, err := m.ts.GetNodeHistory(m.a); err != nil || len(rows) != 2 {
		t.Fatalf("history = %d rows, %v; want 2 (one per shard)", len(rows), err)
	}
	v, h, d, err := walk()
	if !errors.Is(err, storecontract.ErrHistorySpansShards) || v != 0 || h != 0 || d != 0 {
		t.Fatalf("two holders: %d rows visited, %d charged, %d decoded, %v; want ErrHistorySpansShards with nothing read", v, h, d, err)
	}
}
