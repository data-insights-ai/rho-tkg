package sharded

import (
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BACKLOG 20f: sharded.Store had no ForEachDeletedNodeID/ForEachDeletedRelID
// at all — storecontract.DeletedIterationCapability (unlike
// TransactionTimeQuery, HistoryRollbackTrim, label/rel-type-tx membership,
// and depth iteration, which sharded's package doc comment explicitly
// documents as intentional declines) was simply never implemented, an
// oversight rather than a design decision: sharded routes every entity to
// exactly ONE shard by ID (never by time window like tiered), so folding
// deleted-only iteration across shards needs no cross-shard dedup and is a
// direct fan-out to each shard's own (already-implemented, badger-native)
// DeletedIterationCapability. Without it, internal/core's
// forEachDeletedNodeIDByDepth/forEachDeletedRelIDByDepth silently fall back
// to O(total history) full-history iteration for every temporal adjacency
// query on a sharded deployment.
//
// This proves the capability is now present and folds correctly across
// multiple shards: nodes/rels live on different slots, some deleted (history
// row, no current row) and some still live, spread across shards.

func TestForEachDeletedNodeID_FoldsAcrossShards(t *testing.T) {
	st := newMemStore(t, 0, 4)

	var deleted, live []types.NodeID
	for slot := uint8(0); slot < 4; slot++ {
		id := mkNodeID(slot, 1)
		putNode(t, st, id, 10)
		live = append(live, id)
	}
	for slot := uint8(0); slot < 4; slot++ {
		id := mkNodeID(slot, 2)
		putNode(t, st, id, 10)
		cur, err := st.GetNode(id)
		if err != nil {
			t.Fatalf("GetNode: %v", err)
		}
		if err := st.DeleteNodeWithHistory(id, cur.Version(), cur, nil); err != nil {
			t.Fatalf("DeleteNodeWithHistory: %v", err)
		}
		deleted = append(deleted, id)
	}

	var got []types.NodeID
	if err := st.ForEachDeletedNodeID(func(id types.NodeID) bool {
		got = append(got, id)
		return true
	}); err != nil {
		t.Fatalf("ForEachDeletedNodeID: %v", err)
	}

	if len(got) != len(deleted) {
		t.Fatalf("ForEachDeletedNodeID visited %d IDs, want %d (deleted set from every shard) — BACKLOG 20f regression", len(got), len(deleted))
	}
	gotSet := map[types.NodeID]bool{}
	for _, id := range got {
		gotSet[id] = true
	}
	for _, id := range deleted {
		if !gotSet[id] {
			t.Fatalf("ForEachDeletedNodeID missed deleted node %v", id)
		}
	}
	for _, id := range live {
		if gotSet[id] {
			t.Fatalf("ForEachDeletedNodeID visited still-live node %v — must only yield entities with history but no current row", id)
		}
	}
}

func TestForEachDeletedRelID_FoldsAcrossShards(t *testing.T) {
	st := newMemStore(t, 0, 4)
	a := mkNodeID(0, 100)
	b := mkNodeID(1, 100)
	putNode(t, st, a, 10)
	putNode(t, st, b, 10)

	var deleted, live []types.RelID
	for slot := uint8(0); slot < 4; slot++ {
		id := mkRelID(slot, 1)
		putRel(t, st, id, 5, a, b)
		live = append(live, id)
	}
	for slot := uint8(0); slot < 4; slot++ {
		id := mkRelID(slot, 2)
		putRel(t, st, id, 5, a, b)
		cur, err := st.GetRelationship(id)
		if err != nil {
			t.Fatalf("GetRelationship: %v", err)
		}
		if err := st.DeleteRelWithHistory(id, cur.Version(), cur); err != nil {
			t.Fatalf("DeleteRelWithHistory: %v", err)
		}
		deleted = append(deleted, id)
	}

	var got []types.RelID
	if err := st.ForEachDeletedRelID(func(id types.RelID) bool {
		got = append(got, id)
		return true
	}); err != nil {
		t.Fatalf("ForEachDeletedRelID: %v", err)
	}

	if len(got) != len(deleted) {
		t.Fatalf("ForEachDeletedRelID visited %d IDs, want %d — BACKLOG 20f regression", len(got), len(deleted))
	}
	gotSet := map[types.RelID]bool{}
	for _, id := range got {
		gotSet[id] = true
	}
	for _, id := range deleted {
		if !gotSet[id] {
			t.Fatalf("ForEachDeletedRelID missed deleted rel %v", id)
		}
	}
	for _, id := range live {
		if gotSet[id] {
			t.Fatalf("ForEachDeletedRelID visited still-live rel %v", id)
		}
	}
}

// TestForEachDeletedNodeID_EarlyStopAndNilCallback pins the same contract
// every other ForEach* door on this store honors: fn==nil is rejected, and
// returning false from fn stops iteration early without a full-shard scan.
func TestForEachDeletedNodeID_EarlyStopAndNilCallback(t *testing.T) {
	st := newMemStore(t, 0, 2)
	for slot := uint8(0); slot < 2; slot++ {
		id := mkNodeID(slot, 1)
		putNode(t, st, id, 10)
		cur, _ := st.GetNode(id)
		if err := st.DeleteNodeWithHistory(id, cur.Version(), cur, nil); err != nil {
			t.Fatalf("DeleteNodeWithHistory: %v", err)
		}
	}

	if err := st.ForEachDeletedNodeID(nil); err == nil {
		t.Fatalf("ForEachDeletedNodeID(nil): want error, got nil")
	}

	count := 0
	if err := st.ForEachDeletedNodeID(func(types.NodeID) bool {
		count++
		return false
	}); err != nil {
		t.Fatalf("ForEachDeletedNodeID early-stop: %v", err)
	}
	if count != 1 {
		t.Fatalf("early-stop visited %d, want exactly 1 (fn returned false on first call)", count)
	}
}
