package graph

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// ShardKind describes the functional role of a shard.
type ShardKind string

const (
	// ShardReference holds long-lived reference entities (e.g., Case, User).
	ShardReference ShardKind = "reference"
	// ShardEvent holds time-windowed event entities (e.g., Signal, Alert).
	ShardEvent ShardKind = "event"
	// ShardArchive holds archived reference entities (closed cases).
	ShardArchive ShardKind = "archive"
)

// ShardTier describes the temperature/lifecycle stage of an event shard.
type ShardTier string

const (
	// TierHot is the current write-active shard.
	TierHot ShardTier = "hot"
	// TierWarm is a recently rotated shard (still frequently queried).
	TierWarm ShardTier = "warm"
	// TierCold is an older shard (infrequently accessed, may be lazily opened).
	TierCold ShardTier = "cold"
)

// ShardEntry describes a single shard in the catalog.
type ShardEntry struct {
	Name        string    `json:"name"`
	Kind        ShardKind `json:"kind"`
	Tier        ShardTier `json:"tier"`
	Path        string    `json:"path"`
	TimeStart   time.Time `json:"time_start,omitempty"`
	TimeEnd     time.Time `json:"time_end,omitempty"`
	Labels      []string  `json:"labels"`
	RelTypes    []string  `json:"rel_types"`
	ApproxNodes int       `json:"approx_nodes"`
	ApproxRels  int       `json:"approx_rels"`
	Verified    bool      `json:"verified"`
}

// ShardCatalog tracks all shards in a tiered store. It is persisted as JSON.
type ShardCatalog struct {
	Shards []ShardEntry `json:"shards"`
	path   string
}

// NewShardCatalog creates a new catalog that will persist to the given path.
func NewShardCatalog(path string) *ShardCatalog {
	return &ShardCatalog{
		path: path,
	}
}

// Load reads the catalog from disk. Returns nil if the file doesn't exist.
func (sc *ShardCatalog) Load() error {
	data, err := os.ReadFile(sc.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("shard catalog: read: %w", err)
	}
	if err := json.Unmarshal(data, sc); err != nil {
		return fmt.Errorf("shard catalog: unmarshal: %w", err)
	}
	return nil
}

// Save writes the catalog to disk atomically (write-tmp + rename).
func (sc *ShardCatalog) Save() error {
	data, err := json.MarshalIndent(sc, "", "  ")
	if err != nil {
		return fmt.Errorf("shard catalog: marshal: %w", err)
	}

	dir := filepath.Dir(sc.path)
	tmp, err := os.CreateTemp(dir, "shard_catalog_*.tmp")
	if err != nil {
		return fmt.Errorf("shard catalog: create temp: %w", err)
	}
	tmpName := tmp.Name()

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return fmt.Errorf("shard catalog: write temp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("shard catalog: close temp: %w", err)
	}
	if err := os.Rename(tmpName, sc.path); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("shard catalog: rename: %w", err)
	}
	return nil
}

// AddShard appends a new shard entry to the catalog.
func (sc *ShardCatalog) AddShard(entry ShardEntry) {
	sc.Shards = append(sc.Shards, entry)
}

// GetShard looks up a shard by name.
func (sc *ShardCatalog) GetShard(name string) (*ShardEntry, bool) {
	for i := range sc.Shards {
		if sc.Shards[i].Name == name {
			return &sc.Shards[i], true
		}
	}
	return nil, false
}

// EventShards returns all shards with Kind == ShardEvent.
func (sc *ShardCatalog) EventShards() []ShardEntry {
	var out []ShardEntry
	for _, s := range sc.Shards {
		if s.Kind == ShardEvent {
			out = append(out, s)
		}
	}
	return out
}

// ColdEventShards returns all shards with Kind == ShardEvent and Tier == TierCold.
func (sc *ShardCatalog) ColdEventShards() []ShardEntry {
	var out []ShardEntry
	for _, s := range sc.Shards {
		if s.Kind == ShardEvent && s.Tier == TierCold {
			out = append(out, s)
		}
	}
	return out
}

// HotEventShard returns the hot event shard, if any.
func (sc *ShardCatalog) HotEventShard() (*ShardEntry, bool) {
	for i := range sc.Shards {
		if sc.Shards[i].Kind == ShardEvent && sc.Shards[i].Tier == TierHot {
			return &sc.Shards[i], true
		}
	}
	return nil, false
}

// UpdateShardTier changes the tier of a shard by name. Returns true if found.
func (sc *ShardCatalog) UpdateShardTier(name string, tier ShardTier) bool {
	for i := range sc.Shards {
		if sc.Shards[i].Name == name {
			sc.Shards[i].Tier = tier
			return true
		}
	}
	return false
}

// UpdateShardTimeEnd changes the time window end of a shard by name. Returns true if found.
func (sc *ShardCatalog) UpdateShardTimeEnd(name string, timeEnd time.Time) bool {
	for i := range sc.Shards {
		if sc.Shards[i].Name == name {
			sc.Shards[i].TimeEnd = timeEnd
			return true
		}
	}
	return false
}

// AddLabel tracks a label in the given shard entry (idempotent).
func (sc *ShardCatalog) AddLabel(shardName, label string) {
	for i := range sc.Shards {
		if sc.Shards[i].Name == shardName {
			for _, l := range sc.Shards[i].Labels {
				if l == label {
					return
				}
			}
			sc.Shards[i].Labels = append(sc.Shards[i].Labels, label)
			return
		}
	}
}

// AddRelType tracks a relationship type in the given shard entry (idempotent).
func (sc *ShardCatalog) AddRelType(shardName, relType string) {
	for i := range sc.Shards {
		if sc.Shards[i].Name == shardName {
			for _, rt := range sc.Shards[i].RelTypes {
				if rt == relType {
					return
				}
			}
			sc.Shards[i].RelTypes = append(sc.Shards[i].RelTypes, relType)
			return
		}
	}
}
