package tiered

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestShardCatalog_SaveLoadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "catalog.json")

	sc := NewShardCatalog(path)
	now := time.Now().Truncate(time.Second) // JSON loses sub-second precision in some configs.

	sc.AddShard(ShardEntry{
		Name:     "reference",
		Kind:     ShardReference,
		Tier:     TierHot,
		Path:     "data/reference",
		Labels:   []string{"Case", "User"},
		RelTypes: []string{},
		Verified: true,
	})
	sc.AddShard(ShardEntry{
		Name:      "2026-W09",
		Kind:      ShardEvent,
		Tier:      TierHot,
		Path:      "data/events/2026-W09",
		TimeStart: now,
		TimeEnd:   now.Add(7 * 24 * time.Hour),
		Labels:    []string{"Signal"},
		RelTypes:  []string{"RELATES_TO"},
	})

	if err := sc.Save(); err != nil {
		t.Fatalf("Save: %v", err)
	}

	// Load into fresh catalog.
	sc2 := NewShardCatalog(path)
	if err := sc2.Load(); err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(sc2.Shards) != 2 {
		t.Fatalf("Load: got %d shards, want 2", len(sc2.Shards))
	}

	ref, ok := sc2.GetShard("reference")
	if !ok {
		t.Fatal("GetShard(reference) not found")
	}
	if ref.Kind != ShardReference {
		t.Errorf("ref.Kind = %q, want %q", ref.Kind, ShardReference)
	}
	if ref.Tier != TierHot {
		t.Errorf("ref.Tier = %q, want %q", ref.Tier, TierHot)
	}
	if len(ref.Labels) != 2 {
		t.Errorf("ref.Labels = %v, want 2 entries", ref.Labels)
	}

	ev, ok := sc2.GetShard("2026-W09")
	if !ok {
		t.Fatal("GetShard(2026-W09) not found")
	}
	if ev.Kind != ShardEvent {
		t.Errorf("ev.Kind = %q, want %q", ev.Kind, ShardEvent)
	}
	if ev.TimeStart.IsZero() {
		t.Error("ev.TimeStart is zero")
	}
}

func TestShardCatalog_LoadMissing(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nonexistent.json")

	sc := NewShardCatalog(path)
	if err := sc.Load(); err != nil {
		t.Fatalf("Load missing file: %v", err)
	}
	if len(sc.Shards) != 0 {
		t.Errorf("Load missing: got %d shards, want 0", len(sc.Shards))
	}
}

func TestShardCatalog_GetShard_NotFound(t *testing.T) {
	sc := NewShardCatalog("")
	_, ok := sc.GetShard("nope")
	if ok {
		t.Error("GetShard should return false for missing shard")
	}
}

func TestShardCatalog_EventShards(t *testing.T) {
	sc := NewShardCatalog("")
	sc.AddShard(ShardEntry{Name: "reference", Kind: ShardReference})
	sc.AddShard(ShardEntry{Name: "2026-W08", Kind: ShardEvent, Tier: TierWarm})
	sc.AddShard(ShardEntry{Name: "2026-W09", Kind: ShardEvent, Tier: TierHot})

	events := sc.EventShards()
	if len(events) != 2 {
		t.Fatalf("EventShards() = %d, want 2", len(events))
	}
}

func TestShardCatalog_HotEventShard(t *testing.T) {
	sc := NewShardCatalog("")
	sc.AddShard(ShardEntry{Name: "reference", Kind: ShardReference})
	sc.AddShard(ShardEntry{Name: "2026-W08", Kind: ShardEvent, Tier: TierWarm})
	sc.AddShard(ShardEntry{Name: "2026-W09", Kind: ShardEvent, Tier: TierHot})

	hot, ok := sc.HotEventShard()
	if !ok {
		t.Fatal("HotEventShard not found")
	}
	if hot.Name != "2026-W09" {
		t.Errorf("HotEventShard.Name = %q, want 2026-W09", hot.Name)
	}
}

func TestShardCatalog_HotEventShard_None(t *testing.T) {
	sc := NewShardCatalog("")
	sc.AddShard(ShardEntry{Name: "reference", Kind: ShardReference})

	_, ok := sc.HotEventShard()
	if ok {
		t.Error("HotEventShard should return false when no hot event shard exists")
	}
}

func TestShardCatalog_AddLabel_Idempotent(t *testing.T) {
	sc := NewShardCatalog("")
	sc.AddShard(ShardEntry{Name: "ref", Kind: ShardReference})

	sc.AddLabel("ref", "Case")
	sc.AddLabel("ref", "Case") // duplicate
	sc.AddLabel("ref", "User")

	shard, _ := sc.GetShard("ref")
	if len(shard.Labels) != 2 {
		t.Errorf("Labels = %v, want [Case User]", shard.Labels)
	}
}

func TestShardCatalog_AddRelType_Idempotent(t *testing.T) {
	sc := NewShardCatalog("")
	sc.AddShard(ShardEntry{Name: "ev", Kind: ShardEvent})

	sc.AddRelType("ev", "RELATES_TO")
	sc.AddRelType("ev", "RELATES_TO") // duplicate
	sc.AddRelType("ev", "BELONGS_TO")

	shard, _ := sc.GetShard("ev")
	if len(shard.RelTypes) != 2 {
		t.Errorf("RelTypes = %v, want [RELATES_TO BELONGS_TO]", shard.RelTypes)
	}
}

func TestShardCatalog_AddLabel_MissingShard(t *testing.T) {
	sc := NewShardCatalog("")
	// Should be a no-op, not a panic.
	sc.AddLabel("nonexistent", "Test")
}

func TestShardCatalog_SaveAtomicity(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "catalog.json")

	sc := NewShardCatalog(path)
	sc.AddShard(ShardEntry{Name: "ref", Kind: ShardReference})
	if err := sc.Save(); err != nil {
		t.Fatalf("Save: %v", err)
	}

	// Verify no tmp files left behind.
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if e.Name() != "catalog.json" {
			t.Errorf("unexpected file: %s", e.Name())
		}
	}
}
