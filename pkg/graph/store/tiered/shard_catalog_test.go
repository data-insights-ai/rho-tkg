package tiered

import (
	"bytes"
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

func TestShardCatalog_LoadEmptyPathNoop(t *testing.T) {
	sc := NewShardCatalog("")
	if err := sc.Load(); err != nil {
		t.Fatalf("Load empty path: %v", err)
	}
	if len(sc.Shards) != 0 {
		t.Fatalf("Load empty path populated shards: %v", sc.Shards)
	}
}

func TestShardCatalog_LoadRejectsInvalidPersistedTopology(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		json string
	}{
		{
			name: "duplicate names",
			json: `{"shards":[` +
				`{"name":"reference","kind":"reference","tier":"hot","path":"reference"},` +
				`{"name":"reference","kind":"reference","tier":"hot","path":"reference-copy"}` +
				`]}`,
		},
		{
			name: "multiple hot event shards",
			json: `{"shards":[` +
				`{"name":"2026-W01","kind":"event","tier":"hot","path":"events/2026-W01","time_start":"2026-01-01T00:00:00Z","time_end":"2026-01-08T00:00:00Z"},` +
				`{"name":"2026-W02","kind":"event","tier":"hot","path":"events/2026-W02","time_start":"2026-01-08T00:00:00Z","time_end":"2026-01-15T00:00:00Z"}` +
				`]}`,
		},
		{
			name: "path traversal",
			json: `{"shards":[` +
				`{"name":"reference","kind":"reference","tier":"hot","path":"../outside"}` +
				`]}`,
		},
		{
			name: "normalizing path alias",
			json: `{"shards":[` +
				`{"name":"reference","kind":"reference","tier":"hot","path":"events/../reference"}` +
				`]}`,
		},
		{
			name: "negative stats",
			json: `{"shards":[` +
				`{"name":"reference","kind":"reference","tier":"hot","path":"reference","approx_nodes":-1}` +
				`]}`,
		},
		{
			name: "invalid event window",
			json: `{"shards":[` +
				`{"name":"2026-W01","kind":"event","tier":"warm","path":"events/2026-W01","time_start":"2026-01-08T00:00:00Z","time_end":"2026-01-01T00:00:00Z"}` +
				`]}`,
		},
		{
			name: "invalid kind",
			json: `{"shards":[` +
				`{"name":"reference","kind":"other","tier":"hot","path":"reference"}` +
				`]}`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "catalog.json")
			if err := os.WriteFile(path, []byte(tc.json), 0o600); err != nil {
				t.Fatalf("write catalog: %v", err)
			}
			sc := NewShardCatalog(path)
			if err := sc.Load(); err == nil {
				t.Fatal("Load returned nil, want validation error")
			}
		})
	}
}

func TestShardCatalogLoadRejectsInvalidFileWithoutMutatingCatalog(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.json")
	sc := NewShardCatalog(path)
	sc.AddShard(ShardEntry{
		Name: "reference",
		Kind: ShardReference,
		Tier: TierHot,
		Path: "reference",
	})
	invalid := `{"shards":[` +
		`{"name":"reference","kind":"reference","tier":"hot","path":"reference"},` +
		`{"name":"reference","kind":"reference","tier":"hot","path":"reference-copy"}` +
		`]}`
	if err := os.WriteFile(path, []byte(invalid), 0o600); err != nil {
		t.Fatalf("write invalid catalog: %v", err)
	}

	if err := sc.Load(); err == nil {
		t.Fatal("Load returned nil for invalid catalog")
	}
	got, ok := sc.GetShard("reference")
	if !ok {
		t.Fatal("existing reference shard missing after rejected Load")
	}
	if got.Path != "reference" {
		t.Fatalf("rejected Load mutated catalog path: got %q", got.Path)
	}
}

func TestShardCatalogSaveRejectsInvalidCurrentTopology(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.json")
	original := []byte(`{"shards":[]}`)
	if err := atomicWriteFile(path, original, "test catalog setup"); err != nil {
		t.Fatalf("write original catalog: %v", err)
	}

	sc := NewShardCatalog(path)
	sc.AddShard(ShardEntry{
		Name: "reference",
		Kind: ShardReference,
		Tier: TierCold,
		Path: "reference",
	})

	if err := sc.Save(); err == nil {
		t.Fatal("Save returned nil for invalid current topology")
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read catalog after rejected save: %v", err)
	}
	if !bytes.Equal(got, original) {
		t.Fatalf("rejected Save changed catalog bytes: got %q want %q", got, original)
	}
}

func TestShardCatalog_GetShard_NotFound(t *testing.T) {
	sc := NewShardCatalog("")
	_, ok := sc.GetShard("nope")
	if ok {
		t.Error("GetShard should return false for missing shard")
	}
}

func TestShardCatalog_AddShardCopiesSliceFields(t *testing.T) {
	labels := []string{"Case"}
	relTypes := []string{"RELATES_TO"}
	sc := NewShardCatalog("")
	sc.AddShard(ShardEntry{
		Name:     "reference",
		Kind:     ShardReference,
		Tier:     TierHot,
		Labels:   labels,
		RelTypes: relTypes,
	})

	labels[0] = "Mutated"
	relTypes[0] = "MUTATED"

	got, ok := sc.GetShard("reference")
	if !ok {
		t.Fatal("GetShard(reference) not found")
	}
	if got.Labels[0] != "Case" {
		t.Fatalf("catalog label aliases caller slice: got %q", got.Labels[0])
	}
	if got.RelTypes[0] != "RELATES_TO" {
		t.Fatalf("catalog reltype aliases caller slice: got %q", got.RelTypes[0])
	}
}

func TestShardCatalogReturnedEntriesDoNotAliasSliceFields(t *testing.T) {
	sc := NewShardCatalog("")
	sc.AddShard(ShardEntry{
		Name:     "hot",
		Kind:     ShardEvent,
		Tier:     TierHot,
		Labels:   []string{"Signal"},
		RelTypes: []string{"RELATES_TO"},
	})
	sc.AddShard(ShardEntry{
		Name:     "cold",
		Kind:     ShardEvent,
		Tier:     TierCold,
		Labels:   []string{"ArchiveEvent"},
		RelTypes: []string{"ARCHIVED_BY"},
	})

	got, ok := sc.GetShard("hot")
	if !ok {
		t.Fatal("GetShard(hot) not found")
	}
	got.Labels[0] = "MutatedGet"
	got.RelTypes[0] = "MUTATED_GET"

	hot, ok := sc.HotEventShard()
	if !ok {
		t.Fatal("HotEventShard not found")
	}
	hot.Labels[0] = "MutatedHot"
	hot.RelTypes[0] = "MUTATED_HOT"

	events := sc.EventShards()
	if len(events) != 2 {
		t.Fatalf("EventShards len = %d, want 2", len(events))
	}
	events[0].Labels[0] = "MutatedEvents"
	events[0].RelTypes[0] = "MUTATED_EVENTS"

	cold := sc.ColdEventShards()
	if len(cold) != 1 {
		t.Fatalf("ColdEventShards len = %d, want 1", len(cold))
	}
	cold[0].Labels[0] = "MutatedCold"
	cold[0].RelTypes[0] = "MUTATED_COLD"

	got, ok = sc.GetShard("hot")
	if !ok {
		t.Fatal("GetShard(hot) after mutations not found")
	}
	if got.Labels[0] != "Signal" {
		t.Fatalf("hot label aliases returned slice: got %q", got.Labels[0])
	}
	if got.RelTypes[0] != "RELATES_TO" {
		t.Fatalf("hot reltype aliases returned slice: got %q", got.RelTypes[0])
	}

	got, ok = sc.GetShard("cold")
	if !ok {
		t.Fatal("GetShard(cold) after mutations not found")
	}
	if got.Labels[0] != "ArchiveEvent" {
		t.Fatalf("cold label aliases returned slice: got %q", got.Labels[0])
	}
	if got.RelTypes[0] != "ARCHIVED_BY" {
		t.Fatalf("cold reltype aliases returned slice: got %q", got.RelTypes[0])
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
	sc.AddShard(ShardEntry{Name: "ref", Kind: ShardReference, Tier: TierHot, Path: "ref"})
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

func TestShardCatalogFileRollbackRestoresPreviousBytes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.json")
	original := []byte(`{"shards":[{"name":"old"}]}`)
	if err := atomicWriteFile(path, original, "test catalog setup"); err != nil {
		t.Fatalf("write original catalog: %v", err)
	}
	snapshot, err := snapshotShardCatalogFile(path)
	if err != nil {
		t.Fatalf("snapshotShardCatalogFile: %v", err)
	}
	if err := atomicWriteFile(path, []byte(`{"shards":[{"name":"new"}]}`), "test catalog overwrite"); err != nil {
		t.Fatalf("write changed catalog: %v", err)
	}

	if err := restoreShardCatalogFile(snapshot); err != nil {
		t.Fatalf("restoreShardCatalogFile: %v", err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read restored catalog: %v", err)
	}
	if !bytes.Equal(got, original) {
		t.Fatalf("restored catalog bytes = %q, want %q", got, original)
	}
}

func TestShardCatalogFileRollbackRemovesNewFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.json")
	snapshot, err := snapshotShardCatalogFile(path)
	if err != nil {
		t.Fatalf("snapshotShardCatalogFile: %v", err)
	}
	if err := atomicWriteFile(path, []byte(`{"shards":[]}`), "test catalog create"); err != nil {
		t.Fatalf("write new catalog: %v", err)
	}

	if err := restoreShardCatalogFile(snapshot); err != nil {
		t.Fatalf("restoreShardCatalogFile: %v", err)
	}
	if _, err := os.ReadFile(path); !os.IsNotExist(err) {
		t.Fatalf("catalog file after rollback error = %v, want not exist", err)
	}
}
