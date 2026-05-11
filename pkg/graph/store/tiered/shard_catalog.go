package tiered

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
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
// All methods are safe for concurrent access via an internal RWMutex.
type ShardCatalog struct {
	mu     sync.RWMutex
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
	sc.mu.Lock()
	defer sc.mu.Unlock()
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
	if err := sc.validateLoadedLocked(); err != nil {
		return err
	}
	return nil
}

func (sc *ShardCatalog) validateLoadedLocked() error {
	seenNames := make(map[string]struct{}, len(sc.Shards))
	hotEvents := 0
	referenceShards := 0
	archiveShards := 0

	for _, entry := range sc.Shards {
		if entry.Name == "" {
			return fmt.Errorf("shard catalog: empty shard name")
		}
		if _, exists := seenNames[entry.Name]; exists {
			return fmt.Errorf("shard catalog: duplicate shard name %q", entry.Name)
		}
		seenNames[entry.Name] = struct{}{}
		if !isSafeCatalogShardPath(entry.Path) {
			return fmt.Errorf("shard catalog: invalid path for shard %q: %q", entry.Name, entry.Path)
		}
		if entry.ApproxNodes < 0 || entry.ApproxRels < 0 {
			return fmt.Errorf("shard catalog: negative stats for shard %q", entry.Name)
		}

		switch entry.Kind {
		case ShardReference:
			referenceShards++
			if entry.Tier != TierHot {
				return fmt.Errorf("shard catalog: reference shard %q has invalid tier %q", entry.Name, entry.Tier)
			}
		case ShardArchive:
			archiveShards++
			if entry.Tier != TierCold {
				return fmt.Errorf("shard catalog: archive shard %q has invalid tier %q", entry.Name, entry.Tier)
			}
		case ShardEvent:
			switch entry.Tier {
			case TierHot:
				hotEvents++
			case TierWarm, TierCold:
			default:
				return fmt.Errorf("shard catalog: event shard %q has invalid tier %q", entry.Name, entry.Tier)
			}
			if entry.TimeStart.IsZero() || entry.TimeEnd.IsZero() || !entry.TimeEnd.After(entry.TimeStart) {
				return fmt.Errorf("shard catalog: event shard %q has invalid time window", entry.Name)
			}
		default:
			return fmt.Errorf("shard catalog: shard %q has invalid kind %q", entry.Name, entry.Kind)
		}
	}
	if referenceShards > 1 {
		return fmt.Errorf("shard catalog: multiple reference shards")
	}
	if archiveShards > 1 {
		return fmt.Errorf("shard catalog: multiple archive shards")
	}
	if hotEvents > 1 {
		return fmt.Errorf("shard catalog: multiple hot event shards")
	}
	return nil
}

func isSafeCatalogShardPath(path string) bool {
	if path == "" || filepath.IsAbs(path) {
		return false
	}
	clean := filepath.Clean(path)
	if clean == "." || clean == ".." {
		return false
	}
	return !strings.HasPrefix(clean, ".."+string(filepath.Separator))
}

// Save writes the catalog to disk atomically (write-tmp + fsync + rename).
func (sc *ShardCatalog) Save() error {
	if sc.path == "" {
		return nil
	}
	sc.mu.RLock()
	data, err := json.MarshalIndent(sc, "", "  ")
	sc.mu.RUnlock()
	if err != nil {
		return fmt.Errorf("shard catalog: marshal: %w", err)
	}
	snapshot, err := snapshotShardCatalogFile(sc.path)
	if err != nil {
		return err
	}
	if err := atomicWriteFile(sc.path, data, "shard catalog"); err != nil {
		if restoreErr := restoreShardCatalogFile(snapshot); restoreErr != nil {
			return fmt.Errorf("shard catalog: save: %w (rollback failed: %v)", err, restoreErr)
		}
		return err
	}
	return nil
}

type shardCatalogFileSnapshot struct {
	path   string
	data   []byte
	exists bool
}

func snapshotShardCatalogFile(path string) (shardCatalogFileSnapshot, error) {
	data, err := os.ReadFile(path) // #nosec G304 — path derived from trusted Store config
	if err != nil {
		if os.IsNotExist(err) {
			return shardCatalogFileSnapshot{path: path}, nil
		}
		return shardCatalogFileSnapshot{}, fmt.Errorf("shard catalog: snapshot: %w", err)
	}
	return shardCatalogFileSnapshot{path: path, data: data, exists: true}, nil
}

func restoreShardCatalogFile(snapshot shardCatalogFileSnapshot) error {
	if snapshot.path == "" {
		return nil
	}
	if snapshot.exists {
		return atomicWriteFile(snapshot.path, snapshot.data, "shard catalog rollback")
	}
	if err := os.Remove(snapshot.path); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("shard catalog rollback: remove: %w", err)
	}
	if err := syncParentDir(snapshot.path, "shard catalog rollback"); err != nil {
		return err
	}
	return nil
}

// AddShard appends a new shard entry to the catalog.
func (sc *ShardCatalog) AddShard(entry ShardEntry) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.Shards = append(sc.Shards, entry)
}

// snapshotShards returns a deep copy of the current Shards slice. Used by
// transactional mutators (e.g. RotateHotShard) that need to roll back
// in-memory catalog mutations when a subsequent Save() fails.
func (sc *ShardCatalog) snapshotShards() []ShardEntry {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	out := make([]ShardEntry, len(sc.Shards))
	for i := range sc.Shards {
		out[i] = sc.Shards[i]
		// Defensive copy of the slice fields so a later mutation on the
		// live catalog cannot bleed into the snapshot.
		if sc.Shards[i].Labels != nil {
			out[i].Labels = append([]string(nil), sc.Shards[i].Labels...)
		}
		if sc.Shards[i].RelTypes != nil {
			out[i].RelTypes = append([]string(nil), sc.Shards[i].RelTypes...)
		}
	}
	return out
}

// restoreShards replaces the catalog's Shards slice with the given snapshot.
// Pair with snapshotShards() to wrap a sequence of catalog mutations in a
// transactional "save-or-roll-back" block.
func (sc *ShardCatalog) restoreShards(snapshot []ShardEntry) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.Shards = snapshot
}

// GetShard looks up a shard by name.
func (sc *ShardCatalog) GetShard(name string) (*ShardEntry, bool) {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	for i := range sc.Shards {
		if sc.Shards[i].Name == name {
			entry := sc.Shards[i] // copy
			return &entry, true
		}
	}
	return nil, false
}

// EventShards returns all shards with Kind == ShardEvent.
func (sc *ShardCatalog) EventShards() []ShardEntry {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	var out []ShardEntry
	for _, s := range sc.Shards {
		if s.Kind == ShardEvent {
			out = append(out, s)
		}
	}
	return out
}

// HotEventShard returns the hot event shard, if any.
func (sc *ShardCatalog) HotEventShard() (*ShardEntry, bool) {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	for i := range sc.Shards {
		if sc.Shards[i].Kind == ShardEvent && sc.Shards[i].Tier == TierHot {
			entry := sc.Shards[i] // copy
			return &entry, true
		}
	}
	return nil, false
}

// UpdateShardTier changes the tier of a shard by name. Returns true if found.
func (sc *ShardCatalog) UpdateShardTier(name string, tier ShardTier) bool {
	sc.mu.Lock()
	defer sc.mu.Unlock()
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
	sc.mu.Lock()
	defer sc.mu.Unlock()
	for i := range sc.Shards {
		if sc.Shards[i].Name == name {
			sc.Shards[i].TimeEnd = timeEnd
			return true
		}
	}
	return false
}

// UpdateShardVerified sets the Verified flag on a shard by name. Returns true if found.
func (sc *ShardCatalog) UpdateShardVerified(name string, verified bool) bool {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	for i := range sc.Shards {
		if sc.Shards[i].Name == name {
			sc.Shards[i].Verified = verified
			return true
		}
	}
	return false
}

// UpdateShardStats sets the approximate node and relationship counts for a shard.
// Returns true if found.
func (sc *ShardCatalog) UpdateShardStats(name string, nodes, rels int) bool {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	for i := range sc.Shards {
		if sc.Shards[i].Name == name {
			sc.Shards[i].ApproxNodes = nodes
			sc.Shards[i].ApproxRels = rels
			return true
		}
	}
	return false
}

// ColdEventShards returns all shards with Kind == ShardEvent and Tier == TierCold.
func (sc *ShardCatalog) ColdEventShards() []ShardEntry {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	var out []ShardEntry
	for _, s := range sc.Shards {
		if s.Kind == ShardEvent && s.Tier == TierCold {
			out = append(out, s)
		}
	}
	return out
}

// AddLabel tracks a label in the given shard entry (idempotent).
func (sc *ShardCatalog) AddLabel(shardName, label string) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
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
	sc.mu.Lock()
	defer sc.mu.Unlock()
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
