package tiered

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	registrypkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/registry"
)

// --- Rotation and depth helpers ---

// RotateHotShard demotes the current hot shard to warm and creates a new hot shard.
// Caller must hold ts.mu.Lock.
//
// Persistence order is transactional: the new badger shard is opened, the
// catalog is mutated in-memory, and the catalog is persisted to disk
// BEFORE any live in-memory topology mutation (hotShard pointer switch,
// eventShards map insert, oldHot tier flip, warm→cold demotions). If the
// persist fails, the in-memory catalog is rolled back from a snapshot, the
// new badger shard is closed and removed, and the live topology is left
// untouched. Without this discipline, a Save failure would leave the
// process running with a switched-over hotShard but a durable catalog
// describing the old topology — split-brain on restart.
func (ts *Store) RotateHotShard() error {
	oldHot := ts.hotShard
	now := time.Now()

	// Flush pending writes to the old hot shard. Failure here is benign:
	// no live state has changed yet.
	if err := oldHot.store.Flush(); err != nil {
		return fmt.Errorf("graph: flush hot shard before rotation: %w", err)
	}

	// Align boundary to millisecond precision + 1ms: snowflake IDs encode
	// time at millisecond resolution. Adding 1ms ensures entities created
	// in the same millisecond as the rotation (which are in the old
	// shard) resolve correctly.
	boundary := now.Truncate(time.Millisecond).Add(time.Millisecond)

	// Compute the new shard's name and on-disk location. If the computed
	// name collides with the old hot shard (rotation within the same
	// window — forced rotation or edge timing), append a disambiguating
	// suffix.
	newName := shardWindowName(now, ts.shardWindow)
	windowStart := boundary
	windowEnd := shardWindowStart(now, ts.shardWindow).Add(ts.shardWindow)
	if _, exists := ts.eventShards[newName]; exists {
		newName = fmt.Sprintf("%s-%d", newName, now.UnixNano())
	}
	newDir := newName
	if !ts.inMemory {
		newDir = filepath.Join("events", newName)
	}

	// Open the new shard's badger store. Failure here is also benign:
	// the catalog has not been mutated yet.
	newStore, err := ts.openBadgerStore(newDir, false)
	if err != nil {
		return fmt.Errorf("graph: open new hot shard: %w", err)
	}

	// Set up temporal indexes on the new hot shard before it begins
	// accepting writes.
	ts.tempIdxMu.Lock()
	for _, tok := range ts.tempIdxLabels {
		// Errors other than ErrTemporalIndexExists are unexpected on a
		// fresh shard.
		if err := newStore.CreateTemporalIndex(tok); err != nil && !errors.Is(err, ErrTemporalIndexExists) {
			slog.Error("graph: create temporal index on new hot shard", "token", tok, "error", err)
		}
	}
	ts.tempIdxMu.Unlock()

	// Snapshot the catalog before any mutation so we can roll back if
	// Save() fails. After this point, every catalog mutation is
	// considered tentative until persisted.
	catalogSnapshot := ts.catalog.snapshotShards()

	// Compute warm→cold demotions. Captured separately so the live
	// EventShard.tier flip can be applied only after the catalog persists.
	var coldDemotions []*EventShard
	if ts.coldAfter > 0 {
		coldThreshold := now.Add(-ts.coldAfter)
		for _, es := range ts.eventShards {
			if es.tier == TierWarm && es.timeEnd.Before(coldThreshold) {
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

	// Persist the catalog. On failure, undo every tentative catalog
	// mutation, close + remove the new badger shard, and return the
	// error. Live in-memory topology has NOT been touched yet, so no
	// rollback is needed there.
	if !ts.inMemory {
		if err := ts.catalog.Save(); err != nil {
			ts.catalog.restoreShards(catalogSnapshot)
			if cerr := newStore.Close(); cerr != nil {
				slog.Error("graph: rollback close new hot shard", "shard", newName, "error", cerr)
			}
			if !ts.inMemory {
				if rerr := os.RemoveAll(filepath.Join(ts.dataDir, newDir)); rerr != nil {
					slog.Error("graph: rollback remove new hot shard files", "dir", newDir, "error", rerr)
				}
			}
			return fmt.Errorf("graph: save catalog after rotation: %w", err)
		}
	}

	// Catalog persisted. From here we commit the live in-memory topology.
	oldHot.tier = TierWarm
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
	ts.eventShards[newName] = newES
	ts.hotShard = newES

	for _, es := range coldDemotions {
		es.tier = TierCold
		// Do NOT close the store here — let idle-close handle it safely
		// (avoids closing while in-flight reads hold pointers from
		// snapshots).
	}

	return nil
}

// checkRotation checks if the hot shard's time window has expired and rotates if needed.
// Fast path: single time comparison (~1ns). Slow path: acquire Lock, double-check, rotate.
func (ts *Store) checkRotation() error {
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

// --- Registry file integration ---

// SaveRegistries persists both registries atomically to the registry file.
// Writing both halves in a single call eliminates the read-modify-write race
// that existed when SaveLabelRegistry and SaveRelTypeRegistry were separate.
func (ts *Store) SaveRegistries(labelReg *registrypkg.LabelRegistry, relTypeReg *registrypkg.RelTypeRegistry) error {
	if ts.inMemory {
		return nil
	}
	return saveRegistryFile(ts.regFile, labelReg.ExportNames(), relTypeReg.ExportNames())
}

// SaveLabelRegistry persists the label registry to the registry file.
// Deprecated: use SaveRegistries to avoid read-modify-write race.
// Kept for backward compatibility with single-registry callers.
func (ts *Store) SaveLabelRegistry(reg *registrypkg.LabelRegistry) error {
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
// Deprecated: use SaveRegistries to avoid read-modify-write race.
// Kept for backward compatibility with single-registry callers.
func (ts *Store) SaveRelTypeRegistry(reg *registrypkg.RelTypeRegistry) error {
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
	ts.ontology.SetLabelRegistry(reg)
}

// --- Reference archive ---

// openRefArchive lazily opens the reference archive BadgerStore.
// Caller must hold ts.archiveMu.Lock to serialize against other openers.
// The Load/Store pattern still applies for readers — archiveMu only protects
// the open operation itself, not field access.
func (ts *Store) openRefArchive() error {
	if ts.refArchive.Load() != nil {
		return nil
	}
	store, err := ts.openBadgerStore("archive", false) // NOT read-only: archive needs writes
	if err != nil {
		return fmt.Errorf("graph: open ref archive: %w", err)
	}
	ts.refArchive.Store(store)

	// Register archive shard in catalog if new.
	if _, ok := ts.catalog.GetShard("archive"); !ok {
		ts.catalog.AddShard(ShardEntry{
			Name: "archive",
			Kind: ShardArchive,
			Tier: TierCold,
			Path: "archive",
		})
		if !ts.inMemory {
			_ = ts.catalog.Save() // best-effort persist
		}
	}
	return nil
}

// ensureRefArchive opens the archive if needed. Thread-safe via archiveMu.
// Refuses to re-open after Close has run — `closed` is set under archiveMu,
// so an open call that wins the lock against Close completes first; one that
// loses sees `closed == true` and returns ErrStoreClosed instead of opening
// a fresh archive that would never be closed.
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
