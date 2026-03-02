package graph

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
)

// Default configuration values for TieredStore.
const (
	defaultShardWindow = 7 * 24 * time.Hour // 1 week
)

// TieredStoreConfig configures a TieredStore instance.
type TieredStoreConfig struct {
	// DataDir is the root data directory. Required unless InMemory is true.
	DataDir string
	// InMemory creates in-memory shards (testing only).
	InMemory bool
	// RefLabels are entity labels classified as reference (long-lived).
	// All other labels default to event classification.
	RefLabels []string
	// ShardWindow is the time window for event shards. Default: 1 week.
	ShardWindow time.Duration
	// CacheCapacity is the per-shard LRU capacity. Default: 10,000.
	CacheCapacity int
	// FlushInterval is the per-shard flush interval. Default: 100ms.
	FlushInterval time.Duration
}

// eventShard wraps a BadgerStore with metadata for an event shard.
type eventShard struct {
	name      string
	store     *BadgerStore
	tier      ShardTier
	timeStart time.Time // shard window start (inclusive)
	timeEnd   time.Time // shard window end (exclusive)
	readOnly  bool      // warm/cold shards are read-only
}

// TieredStore implements the Store interface by routing entities across
// multiple BadgerStore instances based on ontology classification.
//
// Reference entities (Case, Organization, User) live in refShard.
// Event entities (Signal, Alert) live in time-windowed event shards.
// Phase 3a: exactly one hot event shard. Phases 3b-3e add warm/cold/archive.
type TieredStore struct {
	mu          sync.RWMutex            // protects hotShard + eventShards during rotation
	refShard    *BadgerStore            // reference shard (always hot)
	refArchive  *BadgerStore            // nil until Phase 3d
	eventShards map[string]*eventShard  // name -> event shard
	hotShard    *eventShard             // convenience pointer to current hot shard
	ontology    *OntologyMapping
	catalog     *ShardCatalog
	regFile     string // path to registry.msgpack
	dataDir     string
	inMemory    bool
	shardWindow time.Duration
	cacheCap    int
	flushInt    time.Duration
	closeOnce   sync.Once
}

// NewTieredStore creates a TieredStore with a reference shard and one hot event shard.
func NewTieredStore(cfg TieredStoreConfig) (*TieredStore, error) {
	if !cfg.InMemory && cfg.DataDir == "" {
		return nil, fmt.Errorf("graph: TieredStoreConfig.DataDir required unless InMemory")
	}

	window := cfg.ShardWindow
	if window == 0 {
		window = defaultShardWindow
	}
	cacheCap := cfg.CacheCapacity
	if cacheCap <= 0 {
		cacheCap = defaultCacheCapacity
	}
	flushInt := cfg.FlushInterval
	if flushInt == 0 {
		flushInt = defaultFlushInterval
	}

	ts := &TieredStore{
		eventShards: make(map[string]*eventShard),
		ontology:    NewOntologyMapping(cfg.RefLabels),
		dataDir:     cfg.DataDir,
		inMemory:    cfg.InMemory,
		shardWindow: window,
		cacheCap:    cacheCap,
		flushInt:    flushInt,
	}

	// Create directory layout for disk-backed stores.
	if !cfg.InMemory {
		dirs := []string{
			filepath.Join(cfg.DataDir, "meta"),
			filepath.Join(cfg.DataDir, "reference"),
			filepath.Join(cfg.DataDir, "events"),
		}
		for _, d := range dirs {
			if err := os.MkdirAll(d, 0o755); err != nil {
				return nil, fmt.Errorf("graph: create dir %s: %w", d, err)
			}
		}
		ts.regFile = filepath.Join(cfg.DataDir, "meta", "registry.msgpack")
		ts.catalog = NewShardCatalog(filepath.Join(cfg.DataDir, "meta", "shard_catalog.json"))
	} else {
		ts.catalog = NewShardCatalog("") // in-memory catalog (never persisted)
	}

	// Load existing catalog.
	if !cfg.InMemory {
		if err := ts.catalog.Load(); err != nil {
			return nil, fmt.Errorf("graph: load catalog: %w", err)
		}
	}

	// Open reference shard.
	refStore, err := ts.openBadgerStore("reference", false)
	if err != nil {
		return nil, fmt.Errorf("graph: open reference shard: %w", err)
	}
	ts.refShard = refStore

	// Register reference shard in catalog if new.
	if _, ok := ts.catalog.GetShard("reference"); !ok {
		ts.catalog.AddShard(ShardEntry{
			Name:   "reference",
			Kind:   ShardReference,
			Tier:   TierHot,
			Path:   "data/reference",
			Labels: cfg.RefLabels,
		})
	}

	// Open hot event shard.
	now := time.Now()
	hotName := shardWindowName(now, window)
	windowStart := shardWindowStart(now, window)
	windowEnd := windowStart.Add(window)

	// Check if a hot shard already exists in catalog (mid-window restart).
	if existing, ok := ts.catalog.HotEventShard(); ok {
		hotName = existing.Name
		windowStart = existing.TimeStart
		windowEnd = existing.TimeEnd
	}

	hotDir := hotName
	if !cfg.InMemory {
		hotDir = filepath.Join("events", hotName)
	}
	hotStore, err := ts.openBadgerStore(hotDir, false)
	if err != nil {
		_ = refStore.Close()
		return nil, fmt.Errorf("graph: open hot event shard: %w", err)
	}

	es := &eventShard{
		name:      hotName,
		store:     hotStore,
		tier:      TierHot,
		timeStart: windowStart,
		timeEnd:   windowEnd,
		readOnly:  false,
	}
	ts.eventShards[hotName] = es
	ts.hotShard = es

	// Register hot shard in catalog if new.
	if _, ok := ts.catalog.GetShard(hotName); !ok {
		ts.catalog.AddShard(ShardEntry{
			Name:      hotName,
			Kind:      ShardEvent,
			Tier:      TierHot,
			Path:      hotDir,
			TimeStart: windowStart,
			TimeEnd:   windowEnd,
		})
	}

	// Reopen warm event shards from catalog.
	for _, entry := range ts.catalog.EventShards() {
		if entry.Tier == TierWarm {
			// Read-only on disk; in-memory shards stay read-write (Badger limitation).
			warmReadOnly := !cfg.InMemory
			warmStore, err := ts.openBadgerStore(entry.Path, warmReadOnly)
			if err != nil {
				_ = hotStore.Close()
				_ = refStore.Close()
				return nil, fmt.Errorf("graph: open warm shard %s: %w", entry.Name, err)
			}
			warmES := &eventShard{
				name:      entry.Name,
				store:     warmStore,
				tier:      TierWarm,
				timeStart: entry.TimeStart,
				timeEnd:   entry.TimeEnd,
				readOnly:  true,
			}
			ts.eventShards[entry.Name] = warmES
		}
	}

	// Persist catalog.
	if !cfg.InMemory {
		if err := ts.catalog.Save(); err != nil {
			_ = hotStore.Close()
			_ = refStore.Close()
			return nil, fmt.Errorf("graph: save catalog: %w", err)
		}
	}

	return ts, nil
}

// Close closes all shards and saves the catalog. Idempotent via sync.Once.
func (ts *TieredStore) Close() error {
	var closeErr error
	ts.closeOnce.Do(func() {
		// Save catalog.
		if !ts.inMemory && ts.catalog != nil {
			if err := ts.catalog.Save(); err != nil {
				closeErr = fmt.Errorf("graph: save catalog on close: %w", err)
			}
		}

		// Close all event shards.
		for _, es := range ts.eventShards {
			if err := es.store.Close(); err != nil {
				closeErr = fmt.Errorf("graph: close event shard %s: %w", es.name, err)
			}
		}

		// Close reference archive if open.
		if ts.refArchive != nil {
			if err := ts.refArchive.Close(); err != nil {
				closeErr = fmt.Errorf("graph: close ref archive: %w", err)
			}
		}

		// Close reference shard.
		if ts.refShard != nil {
			if err := ts.refShard.Close(); err != nil {
				closeErr = fmt.Errorf("graph: close ref shard: %w", err)
			}
		}
	})
	return closeErr
}

// Clear clears all open shards.
func (ts *TieredStore) Clear() error {
	ts.mu.RLock()
	shards := make([]*eventShard, 0, len(ts.eventShards))
	for _, es := range ts.eventShards {
		shards = append(shards, es)
	}
	ts.mu.RUnlock()

	if err := ts.refShard.Clear(); err != nil {
		return fmt.Errorf("graph: clear ref shard: %w", err)
	}
	for _, es := range shards {
		if err := es.store.Clear(); err != nil {
			return fmt.Errorf("graph: clear event shard %s: %w", es.name, err)
		}
	}
	return nil
}

// --- Shard routing helpers ---

// shardForNode routes a node to the correct BadgerStore by its primary label.
// For event nodes, always routes to the hot shard (new writes).
func (ts *TieredStore) shardForNode(primaryLabel uint16) *BadgerStore {
	if ts.ontology.ClassifyByToken(primaryLabel) == ClassReference {
		return ts.refShard
	}
	ts.mu.RLock()
	s := ts.hotShard.store
	ts.mu.RUnlock()
	return s
}

// shardForNodeID resolves which shard owns a node ID.
// O(1): try ref (hasNodeID), miss -> timestamp extraction -> event shard.
func (ts *TieredStore) shardForNodeID(id snowflake.ID) *BadgerStore {
	if ts.refShard.hasNodeID(id) {
		return ts.refShard
	}
	return ts.timestampToEventShard(id)
}

// shardForRelID resolves which shard owns a relationship ID (entity + out/).
// O(1): try ref (hasRelID), miss -> try timestamp extraction -> probe all event shards.
// Probe is needed because cross-shard relationship entities may be stored in a
// shard that doesn't match their creation timestamp (e.g., a rel created after
// rotation whose entity lives in the start node's warm shard).
func (ts *TieredStore) shardForRelID(id snowflake.ID) *BadgerStore {
	if ts.refShard.hasRelID(id) {
		return ts.refShard
	}

	// Try timestamp-based resolution first (fast path).
	candidate := ts.timestampToEventShard(id)
	if candidate.hasRelID(id) {
		return candidate
	}

	// Probe all event shards (handles cross-shard entities in mismatched windows).
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	for _, es := range ts.eventShards {
		if es.store.hasRelID(id) {
			return es.store
		}
	}
	return candidate // fallback to timestamp-based (will likely return ErrRelNotFound)
}

// classifyNodeID determines the EntityClass of a node by probing shards.
func (ts *TieredStore) classifyNodeID(id snowflake.ID) EntityClass {
	if ts.refShard.hasNodeID(id) {
		return ClassReference
	}
	return ClassEvent
}

// timestampToEventShard extracts the creation timestamp from a snowflake ID
// and maps it to the correct event shard. Falls back to the hot shard if no
// shard window matches (entity from before the oldest shard).
func (ts *TieredStore) timestampToEventShard(id snowflake.ID) *BadgerStore {
	epochMs := snowflakeEpoch.UnixMilli()
	timeMs := int64(uint64(id) >> 22)
	created := time.UnixMilli(epochMs + timeMs)

	ts.mu.RLock()
	defer ts.mu.RUnlock()

	for _, es := range ts.eventShards {
		if !created.Before(es.timeStart) && created.Before(es.timeEnd) {
			return es.store
		}
	}
	return ts.hotShard.store // fallback: newest shard
}

// --- Rotation and depth helpers ---

// RotateHotShard demotes the current hot shard to warm and creates a new hot shard.
// Caller must hold ts.mu.Lock.
func (ts *TieredStore) RotateHotShard() error {
	oldHot := ts.hotShard
	now := time.Now()

	// Flush pending writes to the old hot shard.
	if err := oldHot.store.Flush(); err != nil {
		return fmt.Errorf("graph: flush hot shard before rotation: %w", err)
	}

	// Mark old hot as warm. Adjust timeEnd to the actual rotation time so that
	// entities created during this shard's tenure still resolve correctly via
	// snowflake ID timestamp extraction.
	oldHot.tier = TierWarm
	oldHot.readOnly = true
	// Align boundary to millisecond precision + 1ms: snowflake IDs encode time at
	// millisecond resolution. Adding 1ms ensures entities created in the same
	// millisecond as the rotation (which are in the old shard) resolve correctly.
	boundary := now.Truncate(time.Millisecond).Add(time.Millisecond)
	oldHot.timeEnd = boundary
	ts.catalog.UpdateShardTier(oldHot.name, TierWarm)
	ts.catalog.UpdateShardTimeEnd(oldHot.name, boundary)

	// Create new hot shard.
	newName := shardWindowName(now, ts.shardWindow)
	windowStart := boundary // new shard starts at next ms after rotation (contiguous)
	windowEnd := shardWindowStart(now, ts.shardWindow).Add(ts.shardWindow)

	// If the computed name collides with the old hot shard (rotation within
	// the same window — can happen with forced rotation or edge timing),
	// append a disambiguating suffix.
	if _, exists := ts.eventShards[newName]; exists {
		newName = fmt.Sprintf("%s-%d", newName, now.UnixNano())
	}

	newDir := newName
	if !ts.inMemory {
		newDir = filepath.Join("events", newName)
	}
	newStore, err := ts.openBadgerStore(newDir, false)
	if err != nil {
		return fmt.Errorf("graph: open new hot shard: %w", err)
	}

	es := &eventShard{
		name:      newName,
		store:     newStore,
		tier:      TierHot,
		timeStart: windowStart,
		timeEnd:   windowEnd,
		readOnly:  false,
	}
	ts.eventShards[newName] = es
	ts.hotShard = es

	// Register in catalog and persist.
	ts.catalog.AddShard(ShardEntry{
		Name:      newName,
		Kind:      ShardEvent,
		Tier:      TierHot,
		Path:      newDir,
		TimeStart: windowStart,
		TimeEnd:   windowEnd,
	})

	if !ts.inMemory {
		if err := ts.catalog.Save(); err != nil {
			return fmt.Errorf("graph: save catalog after rotation: %w", err)
		}
	}

	return nil
}

// checkRotation checks if the hot shard's time window has expired and rotates if needed.
// Fast path: single time comparison (~1ns). Slow path: acquire Lock, double-check, rotate.
func (ts *TieredStore) checkRotation() error {
	ts.mu.RLock()
	hotEnd := ts.hotShard.timeEnd
	ts.mu.RUnlock()

	if time.Now().Before(hotEnd) {
		return nil // fast path: within window
	}

	// Slow path: acquire write lock and rotate.
	ts.mu.Lock()
	defer ts.mu.Unlock()

	// Double-check after acquiring write lock.
	if time.Now().Before(ts.hotShard.timeEnd) {
		return nil // another goroutine already rotated
	}

	return ts.RotateHotShard()
}

// eventShardSnapshot returns a snapshot of event shard stores filtered by depth.
// Caller must hold at least ts.mu.RLock.
func (ts *TieredStore) eventShardSnapshot(depth ShardDepth) []*BadgerStore {
	var stores []*BadgerStore
	for _, es := range ts.eventShards {
		switch depth {
		case DepthHot:
			if es.tier == TierHot {
				stores = append(stores, es.store)
			}
		case DepthWarm:
			if es.tier == TierHot || es.tier == TierWarm {
				stores = append(stores, es.store)
			}
		default: // DepthAll
			stores = append(stores, es.store)
		}
	}
	return stores
}

// --- Registry file integration ---

// SaveLabelRegistry persists the label registry to the registry file.
func (ts *TieredStore) SaveLabelRegistry(reg *labelRegistry) error {
	if ts.inMemory {
		return nil
	}
	labels := reg.ExportNames()
	// Load existing relTypes to preserve them.
	_, existingRelTypes, err := loadRegistryFile(ts.regFile)
	if err != nil {
		return err
	}
	return saveRegistryFile(ts.regFile, labels, existingRelTypes)
}

// LoadLabelRegistry restores the label registry from the registry file.
// Returns the number of labels imported (excluding reserved token 0).
func (ts *TieredStore) LoadLabelRegistry(reg *labelRegistry) (int, error) {
	if ts.inMemory {
		return 0, nil
	}
	labels, _, err := loadRegistryFile(ts.regFile)
	if err != nil {
		return 0, err
	}
	if labels == nil {
		return 0, nil // no file yet
	}
	if err := reg.ImportNames(labels); err != nil {
		return 0, err
	}
	return len(labels) - 1, nil
}

// SaveRelTypeRegistry persists the reltype registry to the registry file.
func (ts *TieredStore) SaveRelTypeRegistry(reg *relTypeRegistry) error {
	if ts.inMemory {
		return nil
	}
	relTypes := reg.ExportNames()
	// Load existing labels to preserve them.
	existingLabels, _, err := loadRegistryFile(ts.regFile)
	if err != nil {
		return err
	}
	return saveRegistryFile(ts.regFile, existingLabels, relTypes)
}

// LoadRelTypeRegistry restores the reltype registry from the registry file.
// Returns the number of relTypes imported (excluding reserved token 0).
func (ts *TieredStore) LoadRelTypeRegistry(reg *relTypeRegistry) (int, error) {
	if ts.inMemory {
		return 0, nil
	}
	_, relTypes, err := loadRegistryFile(ts.regFile)
	if err != nil {
		return 0, err
	}
	if relTypes == nil {
		return 0, nil
	}
	if err := reg.ImportNames(relTypes); err != nil {
		return 0, err
	}
	return len(relTypes) - 1, nil
}

// SetLabelRegistry wires the ontology to the label registry for token resolution.
func (ts *TieredStore) SetLabelRegistry(reg *labelRegistry) {
	ts.ontology.SetLabelRegistry(reg)
}

// --- Internal helpers ---

// openBadgerStore creates a new BadgerStore with the configured defaults.
// For disk-backed stores, name is the relative path under DataDir.
// readOnly opens Badger in read-only mode (no flushLoop, no gcLoop).
func (ts *TieredStore) openBadgerStore(name string, readOnly bool) (*BadgerStore, error) {
	cfg := BadgerStoreConfig{
		InMemory:      ts.inMemory,
		CacheCapacity: ts.cacheCap,
		FlushInterval: ts.flushInt,
		ReadOnly:      readOnly,
	}
	if !ts.inMemory {
		cfg.Dir = filepath.Join(ts.dataDir, name)
	}
	return NewBadgerStore(cfg)
}

// shardWindowName computes the canonical name for a time window.
func shardWindowName(t time.Time, window time.Duration) string {
	switch {
	case window >= 7*24*time.Hour:
		// Weekly: "2026-W09"
		year, week := t.ISOWeek()
		return fmt.Sprintf("%04d-W%02d", year, week)
	case window >= 24*time.Hour:
		// Daily: "2026-03-02"
		return t.Format("2006-01-02")
	default:
		// Monthly fallback: "2026-03"
		return t.Format("2006-01")
	}
}

// shardWindowStart computes the start time for a shard window containing t.
func shardWindowStart(t time.Time, window time.Duration) time.Time {
	switch {
	case window >= 7*24*time.Hour:
		// ISO week start (Monday).
		year, week := t.ISOWeek()
		// Compute day 1 of ISO week.
		jan1 := time.Date(year, 1, 1, 0, 0, 0, 0, t.Location())
		jan1Weekday := jan1.Weekday()
		if jan1Weekday == time.Sunday {
			jan1Weekday = 7
		}
		// Days from Jan 1 to Monday of ISO week 1.
		daysToMonday := int(time.Monday - jan1Weekday)
		if daysToMonday > 0 {
			daysToMonday -= 7
		}
		isoWeek1Monday := jan1.AddDate(0, 0, daysToMonday)
		return isoWeek1Monday.AddDate(0, 0, (week-1)*7)
	case window >= 24*time.Hour:
		// Day start.
		return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
	default:
		// Month start.
		return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, t.Location())
	}
}
