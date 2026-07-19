package tiered

import "testing"

// BACKLOG 19l: ShardCatalog.RemoveShard had ZERO direct test coverage —
// existing coverage was entirely indirect, exercised through
// retention_purge_drop.go's dropOneShard (Testing Rule 1: indirect coverage
// via delegation does not count). This also covers the RemoveShard +
// snapshotShards/restoreShards transactional pairing that BACKLOG 19b's fix
// relies on: dropOneShard calls snapshotShards() before RemoveShard(), then
// restoreShards(snapshot) if the subsequent catalog.Save() fails — a rollback
// discipline that needs its own direct proof, not just an assumption that
// composing two already-tested primitives works.

func TestShardCatalog_RemoveShard_RemovesExistingEntry(t *testing.T) {
	sc := NewShardCatalog("")
	sc.AddShard(ShardEntry{Name: "hot", Kind: ShardEvent, Tier: TierHot})

	removed := sc.RemoveShard("hot")
	if !removed {
		t.Fatal("RemoveShard(hot) = false, want true")
	}
	if _, ok := sc.GetShard("hot"); ok {
		t.Fatal("shard still present after RemoveShard")
	}
}

func TestShardCatalog_RemoveShard_MissingNameReturnsFalse(t *testing.T) {
	sc := NewShardCatalog("")
	sc.AddShard(ShardEntry{Name: "hot", Kind: ShardEvent, Tier: TierHot})

	removed := sc.RemoveShard("nope")
	if removed {
		t.Fatal("RemoveShard(nope) = true, want false (name not present)")
	}
	// The catalog must be left completely unchanged.
	if _, ok := sc.GetShard("hot"); !ok {
		t.Fatal("RemoveShard on a missing name mutated an unrelated entry")
	}
	if got := len(sc.EventShards()); got != 1 {
		t.Fatalf("event shard count = %d, want 1 (unchanged)", got)
	}
}

// TestShardCatalog_RemoveShard_OnlyRemovesTheNamedEntry proves RemoveShard
// with multiple entries present removes EXACTLY the named one — a bug here
// (e.g. an off-by-one in the slice-splice) would silently delete the wrong
// shard's catalog entry, a data-loss-adjacent bug for a real deployment.
func TestShardCatalog_RemoveShard_OnlyRemovesTheNamedEntry(t *testing.T) {
	sc := NewShardCatalog("")
	sc.AddShard(ShardEntry{Name: "events-2026-W01", Kind: ShardEvent, Tier: TierWarm})
	sc.AddShard(ShardEntry{Name: "events-2026-W02", Kind: ShardEvent, Tier: TierWarm})
	sc.AddShard(ShardEntry{Name: "events-2026-W03", Kind: ShardEvent, Tier: TierHot})

	if removed := sc.RemoveShard("events-2026-W02"); !removed {
		t.Fatal("RemoveShard(events-2026-W02) = false, want true")
	}

	if _, ok := sc.GetShard("events-2026-W02"); ok {
		t.Fatal("events-2026-W02 still present after removal")
	}
	for _, want := range []string{"events-2026-W01", "events-2026-W03"} {
		if _, ok := sc.GetShard(want); !ok {
			t.Fatalf("%s was wrongly removed alongside the target", want)
		}
	}
	if got := len(sc.EventShards()); got != 2 {
		t.Fatalf("event shard count after removal = %d, want 2", got)
	}
}

func TestShardCatalog_RemoveShard_DoubleRemoveIsIdempotentFalse(t *testing.T) {
	sc := NewShardCatalog("")
	sc.AddShard(ShardEntry{Name: "hot", Kind: ShardEvent, Tier: TierHot})

	if removed := sc.RemoveShard("hot"); !removed {
		t.Fatal("first RemoveShard(hot) = false, want true")
	}
	if removed := sc.RemoveShard("hot"); removed {
		t.Fatal("second RemoveShard(hot) = true, want false (already gone)")
	}
}

// TestShardCatalog_RemoveShard_SnapshotRestoreRollsBack directly proves the
// snapshotShards -> RemoveShard -> restoreShards rollback discipline
// dropOneShard depends on (BACKLOG 19b/19e's failure-recovery paths): a
// removal followed by a restore from the pre-removal snapshot must bring the
// entry fully back, indistinguishable from the removal never having happened.
func TestShardCatalog_RemoveShard_SnapshotRestoreRollsBack(t *testing.T) {
	sc := NewShardCatalog("")
	sc.AddShard(ShardEntry{
		Name:     "events-2026-W01",
		Kind:     ShardEvent,
		Tier:     TierWarm,
		Labels:   []string{"Signal"},
		RelTypes: []string{"TRIGGERED"},
	})
	sc.AddShard(ShardEntry{Name: "reference", Kind: ShardReference, Tier: TierHot})

	snapshot := sc.snapshotShards()

	if removed := sc.RemoveShard("events-2026-W01"); !removed {
		t.Fatal("RemoveShard = false, want true")
	}
	if _, ok := sc.GetShard("events-2026-W01"); ok {
		t.Fatal("shard still present immediately after RemoveShard")
	}

	// Simulate a failed catalog.Save() after the in-memory removal — roll back.
	sc.restoreShards(snapshot)

	got, ok := sc.GetShard("events-2026-W01")
	if !ok {
		t.Fatal("shard not restored after restoreShards(snapshot) — BACKLOG 19b/19l rollback discipline broken")
	}
	if got.Kind != ShardEvent || got.Tier != TierWarm {
		t.Fatalf("restored entry = %+v, want Kind=%v Tier=%v", got, ShardEvent, TierWarm)
	}
	if len(got.Labels) != 1 || got.Labels[0] != "Signal" {
		t.Fatalf("restored entry Labels = %v, want [Signal]", got.Labels)
	}
	if len(got.RelTypes) != 1 || got.RelTypes[0] != "TRIGGERED" {
		t.Fatalf("restored entry RelTypes = %v, want [TRIGGERED]", got.RelTypes)
	}
	// The OTHER entry, never removed, must also be untouched by the rollback.
	if _, ok := sc.GetShard("reference"); !ok {
		t.Fatal("unrelated entry lost across the snapshot/restore round-trip")
	}
	if got := len(sc.Shards); got != 2 {
		t.Fatalf("catalog entry count after rollback = %d, want 2", got)
	}
}

// TestShardCatalog_SnapshotShards_IsADeepCopy guards the "snapshot" half of
// the rollback contract: snapshotShards must return an INDEPENDENT copy, or a
// later in-place mutation of a ShardEntry field (UpdateShardTier etc.) after
// the snapshot was taken would corrupt the rollback target too.
func TestShardCatalog_SnapshotShards_IsADeepCopy(t *testing.T) {
	sc := NewShardCatalog("")
	sc.AddShard(ShardEntry{Name: "hot", Kind: ShardEvent, Tier: TierHot, Labels: []string{"Signal"}})

	snapshot := sc.snapshotShards()
	sc.UpdateShardTier("hot", TierWarm)
	if entry, ok := sc.GetShard("hot"); !ok || entry.Tier != TierWarm {
		t.Fatalf("test setup: UpdateShardTier did not apply, got %+v ok=%v", entry, ok)
	}

	if snapshot[0].Tier != TierHot {
		t.Fatalf("snapshot entry mutated by a later catalog update: Tier = %v, want %v (snapshot must be independent)", snapshot[0].Tier, TierHot)
	}
}
