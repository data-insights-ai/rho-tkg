package tiered

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
)

// --- Rotation and depth helpers ---

// RotateHotShard demotes the current hot shard to warm and creates a new hot
// shard. The catalog is persisted BEFORE any live topology mutation
// (hotShard switch, eventShards insert, tier flips, warm->cold demotions); on
// a persist failure the catalog is rolled back from a snapshot, the new shard
// is closed and removed, and the topology is untouched (no split-brain between
// the live hot shard and the durable catalog). See docs/architecture.md (Shard
// Lifecycle).
func (ts *Store) RotateHotShard() error {
	releaseLifecycle, err := ts.beginSequentialStoreWideOperation()
	if err != nil {
		return err
	}
	defer releaseLifecycle()

	ts.mu.Lock()
	defer ts.mu.Unlock()
	return ts.rotateHotShardLocked()
}

func (ts *Store) rotateHotShardLocked() error {
	if err := ts.checkOpen(); err != nil {
		return err
	}
	oldHot := ts.hotShard
	now := time.Now().In(ts.shardLoc)

	// Flush pending writes to the old hot shard. Failure here is benign:
	// no live state has changed yet.
	if err := oldHot.store.Flush(); err != nil {
		return fmt.Errorf("graph: flush hot shard before rotation: %w", err)
	}

	// New shard name and window (rotationWindow): it starts at the boundary,
	// now at millisecond precision + 1 ms (snowflake IDs have ms resolution, so
	// same-millisecond entities still resolve to the old shard). A name
	// colliding with the old hot shard (same window) gets a disambiguating
	// suffix.
	newName, windowStart, windowEnd := rotationWindow(now, ts.shardWindow)
	boundary := windowStart
	if _, exists := ts.eventShards[newName]; exists {
		newName = fmt.Sprintf("%s-%d", newName, now.UnixNano())
	}
	newDir := newName
	if !ts.inMemory {
		newDir = filepath.Join("events", newName)
	}

	// Open the new shard's badger store. Failure here is also benign:
	// the catalog has not been mutated yet.
	newStore, err := ts.openBadgerStore(newDir)
	if err != nil {
		return fmt.Errorf("graph: open new hot shard: %w", err)
	}

	// Set up tracked temporal indexes on the new hot shard before it begins
	// accepting writes.
	err = ts.applyTrackedTemporalIndexes(newStore)
	if err == nil {
		err = ts.syncAnchoredIndexes(newStore, true)
	}
	if err != nil {
		if cerr := newStore.Close(); cerr != nil {
			slog.Error("graph: rollback close new hot shard after index setup failure", "shard", newName, "error", cerr)
		}
		if !ts.inMemory {
			if rerr := os.RemoveAll(ts.shardDir(newDir)); rerr != nil {
				slog.Error("graph: rollback remove new hot shard after index setup failure", "dir", newDir, "error", rerr)
			}
		}
		return fmt.Errorf("graph: create temporal indexes on new hot shard: %w", err)
	}

	// Snapshot the catalog before any mutation for rollback if Save() fails.
	catalogSnapshot := ts.catalog.snapshotShards()

	// Compute warm→cold demotions. Captured separately so the live
	// EventShard.tier flip can be applied only after the catalog persists.
	var coldDemotions []*EventShard
	if ts.coldAfter > 0 {
		coldThreshold := now.Add(-ts.coldAfter)
		for _, es := range ts.eventShards {
			if es.currentTier() == TierWarm && es.timeEnd.Before(coldThreshold) {
				coldDemotions = append(coldDemotions, es)
			}
		}
	}

	// Tentative catalog mutations (in-memory only).
	ts.catalog.UpdateShardTier(oldHot.name, TierWarm)
	ts.catalog.UpdateShardTimeEnd(oldHot.name, boundary)
	ts.catalog.AddShard(ShardEntry{
		Name:      newName,
		Kind:      ShardEvent,
		Tier:      TierHot,
		Path:      newDir,
		TimeStart: windowStart,
		TimeEnd:   windowEnd,
	})
	for _, es := range coldDemotions {
		ts.catalog.UpdateShardTier(es.name, TierCold)
	}

	// Persist the catalog; on failure undo the catalog mutations and remove the
	// new shard (live topology is not yet touched).
	if !ts.inMemory {
		if err := ts.catalog.Save(); err != nil {
			ts.catalog.restoreShards(catalogSnapshot)
			if cerr := newStore.Close(); cerr != nil {
				slog.Error("graph: rollback close new hot shard", "shard", newName, "error", cerr)
			}
			if !ts.inMemory {
				if rerr := os.RemoveAll(ts.shardDir(newDir)); rerr != nil {
					slog.Error("graph: rollback remove new hot shard files", "dir", newDir, "error", rerr)
				}
			}
			return fmt.Errorf("graph: save catalog after rotation: %w", err)
		}
	}

	// Catalog persisted. From here we commit the live in-memory topology.
	oldHot.setTier(TierWarm)
	oldHot.readOnly = true
	oldHot.timeEnd = boundary

	newES := &EventShard{
		name:      newName,
		store:     newStore,
		tier:      TierHot,
		timeStart: windowStart,
		timeEnd:   windowEnd,
		readOnly:  false,
		path:      newDir,
	}
	newES.initTier(TierHot)
	ts.eventShards[newName] = newES
	ts.reindexShardsLocked()
	ts.hotShard = newES

	for _, es := range coldDemotions {
		es.setTier(TierCold)
		// Do NOT close here: idle-close handles it (in-flight reads may hold
		// pointers). The open handle now counts against MaxOpenColdShards.
		es.shardMu.Lock()
		es.countColdStoreLocked(ts)
		es.shardMu.Unlock()
		freeColdRelTemporalIndexes(es.store, es.name)
	}

	// BACKLOG 19h: a rotation triggered mid-scope (checkRotation in a tx/batch)
	// hands the new shard into the open change-log scope IMMEDIATELY, so even the
	// triggering write is buffered instead of self-committing outside the scope
	// (unreachable by DiscardLogScope). ts.scopeMu is a leaf lock, safe under
	// ts.mu.Lock.
	ts.scopeMu.Lock()
	scopeOpen, scopeDivertOn := ts.scopeOpen, ts.scopeDivertOn
	ts.scopeMu.Unlock()
	if scopeOpen {
		if err := newStore.BeginLogScope(); err != nil {
			slog.Error("graph: begin change-log scope on newly-rotated shard", "shard", newName, "error", err)
		} else if scopeDivertOn {
			newStore.SetLogDivert(true)
		}
	}

	return nil
}

// checkRotation checks if the hot shard's time window has expired and rotates if needed.
// Fast path: single time comparison (~1ns). Slow path: acquire Lock, double-check, rotate.
func (ts *Store) checkRotation() error {
	if err := ts.checkOpen(); err != nil {
		return err
	}
	ts.mu.RLock()
	hotEnd := ts.hotShard.timeEnd
	ts.mu.RUnlock()

	if time.Now().Before(hotEnd) {
		return nil // fast path: within window
	}

	releaseLifecycle, err := ts.beginSequentialStoreWideOperation()
	if err != nil {
		return err
	}
	defer releaseLifecycle()

	// Slow path: acquire write lock and rotate.
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if err := ts.checkOpen(); err != nil {
		return err
	}

	// Double-check after acquiring write lock.
	if time.Now().Before(ts.hotShard.timeEnd) {
		return nil // another goroutine already rotated
	}

	return ts.rotateHotShardLocked()
}

// --- Registry file integration ---

// SaveRegistries persists both registries atomically in one call (no
// read-modify-write race between the two halves).
func (ts *Store) SaveRegistries(labelReg *registrypkg.LabelRegistry, relTypeReg *registrypkg.RelTypeRegistry) error {
	if err := ts.checkOpen(); err != nil {
		return err
	}
	if labelReg == nil {
		return errNilLabelRegistry()
	}
	if relTypeReg == nil {
		return errNilRelTypeRegistry()
	}
	if ts.inMemory {
		return nil
	}
	return saveRegistryFile(ts.regFile, labelReg.ExportNames(), relTypeReg.ExportNames())
}

// SaveLabelRegistry persists the label registry to the registry file.
//
// Deprecated: use SaveRegistries to avoid read-modify-write race; removed in v5.0.0.
func (ts *Store) SaveLabelRegistry(reg *registrypkg.LabelRegistry) error {
	if err := ts.checkOpen(); err != nil {
		return err
	}
	if reg == nil {
		return errNilLabelRegistry()
	}
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
func (ts *Store) LoadLabelRegistry(reg *registrypkg.LabelRegistry) (int, error) {
	if err := ts.checkOpen(); err != nil {
		return 0, err
	}
	if reg == nil {
		return 0, errNilLabelRegistry()
	}
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
//
// Deprecated: use SaveRegistries to avoid read-modify-write race; removed in v5.0.0.
func (ts *Store) SaveRelTypeRegistry(reg *registrypkg.RelTypeRegistry) error {
	if err := ts.checkOpen(); err != nil {
		return err
	}
	if reg == nil {
		return errNilRelTypeRegistry()
	}
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
func (ts *Store) LoadRelTypeRegistry(reg *registrypkg.RelTypeRegistry) (int, error) {
	if err := ts.checkOpen(); err != nil {
		return 0, err
	}
	if reg == nil {
		return 0, errNilRelTypeRegistry()
	}
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
func (ts *Store) SetLabelRegistry(reg *registrypkg.LabelRegistry) {
	if ts.closed.Load() {
		return
	}
	if reg == nil {
		return
	}
	ts.ontology.SetLabelRegistry(reg)
}

// --- Reference archive ---

// openRefArchive lazily opens the reference archive. Caller holds
// ts.archiveMu.Lock (serializes openers only, not field access).
func (ts *Store) openRefArchive() error {
	if ts.refArchive.Load() != nil {
		return nil
	}
	openArchive := ts.openBadgerStore // writable: archive needs writes
	if ts.hasArchiveShard() {
		openArchive = ts.openExistingBadgerStore // cataloged: its directory must be there
	}
	store, err := openArchive("archive")
	if err != nil {
		return fmt.Errorf("graph: open ref archive: %w", err)
	}
	if err := ts.applyTrackedTemporalIndexes(store); err != nil {
		_ = store.Close()
		return fmt.Errorf("graph: initialise ref archive indexes: %w", err)
	}
	if err := ts.syncAnchoredIndexes(store, false); err != nil {
		_ = store.Close()
		return fmt.Errorf("graph: initialise ref archive indexes: %w", err)
	}

	// Register archive shard in catalog if new.
	if _, ok := ts.catalog.GetShard("archive"); !ok {
		catalogSnapshot := ts.catalog.snapshotShards()
		ts.catalog.AddShard(ShardEntry{
			Name:   "archive",
			Kind:   ShardArchive,
			Tier:   TierCold,
			Path:   "archive",
			Volume: ts.archiveVolume,
		})
		if !ts.inMemory {
			if err := ts.catalog.Save(); err != nil {
				ts.catalog.restoreShards(catalogSnapshot)
				var rollbackErr error
				if cerr := store.Close(); cerr != nil {
					rollbackErr = errors.Join(rollbackErr, fmt.Errorf("close archive store: %w", cerr))
				}
				if rerr := os.RemoveAll(ts.shardDir("archive")); rerr != nil {
					rollbackErr = errors.Join(rollbackErr, fmt.Errorf("remove archive store: %w", rerr))
				}
				if rollbackErr != nil {
					return fmt.Errorf("graph: save archive catalog: %w (rollback failed: %v)", err, rollbackErr)
				}
				return fmt.Errorf("graph: save archive catalog: %w", err)
			}
		}
	}
	ts.refArchive.Store(store)
	return nil
}

// ensureRefArchive opens the archive if needed (archiveMu). Returns
// ErrStoreClosed after Close: `closed` is set under archiveMu, so a loser of
// the lock race never opens an archive that would never be closed.
func (ts *Store) ensureRefArchive() error {
	ts.archiveMu.Lock()
	defer ts.archiveMu.Unlock()
	if ts.closed.Load() {
		return ErrStoreClosed
	}
	return ts.openRefArchive()
}

// hasArchiveShard returns true if the catalog has an archive shard entry.
func (ts *Store) hasArchiveShard() bool {
	_, ok := ts.catalog.GetShard("archive")
	return ok
}
