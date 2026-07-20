package badger

import (
	"sort"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BACKLOG 13m: CollectShardDropResidue (the tiered whole-shard fast-drop
// primitive, ADR-0008 R4) had zero direct badger-package tests — only
// indirectly exercised via tiered, which violates Rule 1 ("indirect
// coverage via delegation does NOT count"). These tests exercise the
// method directly against a badger.Store, covering both label-index modes
// (RAM and on-disk — hasForeignLabelTokensLocked has separate code paths
// for each) and the onlyLabel true/false branches plus rel dedup.

const (
	shardDropLabelA uint16 = 1
	shardDropLabelB uint16 = 2
	shardDropKnows  uint16 = 10
)

func newShardDropTestStore(t *testing.T, labelOnDisk bool) *Store {
	t.Helper()
	bs, err := New(Config{InMemory: true, LabelIndexOnDisk: labelOnDisk})
	if err != nil {
		t.Fatalf("New(labelOnDisk=%v): %v", labelOnDisk, err)
	}
	t.Cleanup(func() { bs.Close() })
	return bs
}

func sortedNodeIDs(ids []types.NodeID) []int64 {
	out := make([]int64, len(ids))
	for i, id := range ids {
		out[i] = int64(id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func sortedRelIDs(rels []storepkg.PurgedRel) []int64 {
	out := make([]int64, len(rels))
	for i, r := range rels {
		out[i] = int64(r.ID)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// runShardDropSingleLabelTest is shared by the RAM and on-disk label-index
// variants: a shard holding ONLY labelA nodes, with an internal relationship
// between two of them, must report onlyLabel=true, every labelA node ID, and
// the internal rel exactly ONCE despite touching it from both endpoints.
func runShardDropSingleLabelTest(t *testing.T, labelOnDisk bool) {
	t.Helper()
	bs := newShardDropTestStore(t, labelOnDisk)

	n1 := types.NewNode(types.NodeID(1), shardDropLabelA, nil)
	n2 := types.NewNode(types.NodeID(2), shardDropLabelA, nil)
	n3 := types.NewNode(types.NodeID(3), shardDropLabelA, nil)
	for _, n := range []*types.Node{n1, n2, n3} {
		if err := bs.PutNode(n); err != nil {
			t.Fatalf("PutNode(%d): %v", n.ID(), err)
		}
	}
	rel := types.NewRelationship(types.RelID(100), shardDropKnows, n1.ID(), n2.ID())
	if err := bs.PutRelationship(rel); err != nil {
		t.Fatalf("PutRelationship: %v", err)
	}

	onlyLabel, nodeIDs, rels, err := bs.CollectShardDropResidue(shardDropLabelA)
	if err != nil {
		t.Fatalf("CollectShardDropResidue: %v", err)
	}
	if !onlyLabel {
		t.Fatal("onlyLabel = false, want true (shard holds only labelA)")
	}
	wantNodes := []int64{1, 2, 3}
	if got := sortedNodeIDs(nodeIDs); !equalInt64Slices(got, wantNodes) {
		t.Fatalf("nodeIDs = %v, want %v", got, wantNodes)
	}
	wantRels := []int64{100}
	if got := sortedRelIDs(rels); !equalInt64Slices(got, wantRels) {
		t.Fatalf("rels = %v (deduped from both endpoints), want %v", got, wantRels)
	}
	// n3 has no relationships, so touching it must not add spurious entries —
	// already implied by the exact wantRels assertion above, but assert the
	// count explicitly so a future regression that double-counts is caught
	// even if IDs happened to collide.
	if len(rels) != 1 {
		t.Fatalf("len(rels) = %d, want 1 (the internal rel counted once, not twice from n1+n2)", len(rels))
	}
}

func TestCollectShardDropResidue_SingleLabelShard_RAMIndex(t *testing.T) {
	t.Parallel()
	runShardDropSingleLabelTest(t, false)
}

func TestCollectShardDropResidue_SingleLabelShard_OnDiskIndex(t *testing.T) {
	t.Parallel()
	runShardDropSingleLabelTest(t, true)
}

// runShardDropForeignLabelTest is shared by the RAM and on-disk variants: a
// shard holding a foreign label (labelB) alongside labelA must decline
// (onlyLabel=false) with empty nodeIDs/rels — the conservative
// over-decline the doc comment promises.
func runShardDropForeignLabelTest(t *testing.T, labelOnDisk bool) {
	t.Helper()
	bs := newShardDropTestStore(t, labelOnDisk)

	n1 := types.NewNode(types.NodeID(1), shardDropLabelA, nil)
	n2 := types.NewNode(types.NodeID(2), shardDropLabelB, nil)
	for _, n := range []*types.Node{n1, n2} {
		if err := bs.PutNode(n); err != nil {
			t.Fatalf("PutNode(%d): %v", n.ID(), err)
		}
	}

	onlyLabel, nodeIDs, rels, err := bs.CollectShardDropResidue(shardDropLabelA)
	if err != nil {
		t.Fatalf("CollectShardDropResidue: %v", err)
	}
	if onlyLabel {
		t.Fatal("onlyLabel = true, want false (shard also holds labelB)")
	}
	if len(nodeIDs) != 0 {
		t.Fatalf("nodeIDs = %v, want empty when onlyLabel is false", nodeIDs)
	}
	if len(rels) != 0 {
		t.Fatalf("rels = %v, want empty when onlyLabel is false", rels)
	}
}

func TestCollectShardDropResidue_ForeignLabelPresent_RAMIndex(t *testing.T) {
	t.Parallel()
	runShardDropForeignLabelTest(t, false)
}

func TestCollectShardDropResidue_ForeignLabelPresent_OnDiskIndex(t *testing.T) {
	t.Parallel()
	runShardDropForeignLabelTest(t, true)
}

// TestCollectShardDropResidue_EmptyShardIsOnlyLabel covers the degenerate
// case: a shard with NO nodes of any label reports onlyLabel=true (there is
// no foreign label present, vacuously) with empty nodeIDs/rels.
func TestCollectShardDropResidue_EmptyShardIsOnlyLabel(t *testing.T) {
	t.Parallel()
	bs := newShardDropTestStore(t, false)

	onlyLabel, nodeIDs, rels, err := bs.CollectShardDropResidue(shardDropLabelA)
	if err != nil {
		t.Fatalf("CollectShardDropResidue: %v", err)
	}
	if !onlyLabel {
		t.Fatal("onlyLabel = false, want true (empty shard has no foreign label)")
	}
	if len(nodeIDs) != 0 || len(rels) != 0 {
		t.Fatalf("nodeIDs=%v rels=%v, want both empty on an empty shard", nodeIDs, rels)
	}
}

// TestCollectShardDropResidue_ReadOnlyStoreFailsClosed mirrors the
// checkWritable guard every other mutation-adjacent badger door respects
// (this method mutates nothing itself, but is a maintenance primitive gated
// the same way as the fast-drop it supports — a warm/cold-tier shard opened
// ReadOnly must reject it rather than silently return stale-looking data).
func TestCollectShardDropResidue_ReadOnlyStoreFailsClosed(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	seed, err := New(Config{Dir: dir, SyncWrites: true})
	if err != nil {
		t.Fatalf("New seed store: %v", err)
	}
	if err := seed.PutNode(types.NewNode(types.NodeID(1), shardDropLabelA, nil)); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	if err := seed.Close(); err != nil {
		t.Fatalf("Close seed store: %v", err)
	}

	ro, err := New(Config{Dir: dir, ReadOnly: true})
	if err != nil {
		t.Fatalf("New read-only store: %v", err)
	}
	t.Cleanup(func() { ro.Close() })

	if _, _, _, err := ro.CollectShardDropResidue(shardDropLabelA); err == nil {
		t.Fatal("CollectShardDropResidue on a read-only store = nil error, want checkWritable failure")
	}
}

func equalInt64Slices(a, b []int64) bool {
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
