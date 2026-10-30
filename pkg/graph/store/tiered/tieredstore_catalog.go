package tiered

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
)

// --- Rotation and depth helpers ---

// ErrManualRotation is returned by RotateHotShard (and ForceRotate) on a store
// with Config.MintAtValidFrom: its windows are calendar windows, and a cut
// inside one would strand the rows already minted past the cut.
var ErrManualRotation = errors.New("graph: manual rotation refused: shard windows follow the clock (MintAtValidFrom)")

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
	if ts.calendar {
		return ErrManualRotation
	}

	ts.mu.Lock()
	defer ts.mu.Unlock()
	return ts.rotateHotShardLocked()
}

func (ts *Store) rotateHotShardLocked() error {
	if err := ts.checkOpen(); err != nil {
		return err
	}
	oldHot := ts.hotShard
	now := ts.clock().In(ts.shardLoc)

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
	var promote *EventShard // the window's shard, made when data reached it first (ADR-0013)
	if ts.calendar {
		// Calendar windows (ADR-0013): the new hot shard is the window holding
		// the clock, from its start; the old one keeps its own window's end, so
		// a clock jump leaves the skipped windows without a shard instead of
		// stretching the old one over them.
		newName, windowStart, windowEnd = shardWindowName(now, ts.shardWindow), shardWindowStart(now, ts.shardWindow), shardWindowEnd(now, ts.shardWindow)
		boundary = oldHot.timeEnd
		if promote = ts.eventShards[newName]; promote != nil && promote.currentTier() != TierWarm {
			return fmt.Errorf("graph: rotation to window %s: its shard is %v, not warm", newName, promote.currentTier())
		}
	} else if _, exists := ts.eventShards[newName]; exists {
		newName = fmt.Sprintf("%s-%d", newName, now.UnixNano())
	}
	newDir := newName
	if !ts.inMemory {
		newDir = filepath.Join("events", newName)
	}

	// Open the new shard's badger store. Failure here is also benign:
	// the catalog has not been mutated yet.
	var newStore *BadgerStore
	if promote == nil {
		var err error
		if newStore, err = ts.openNewEventShard(newName, newDir); err != nil {
			return err
		}
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
	// The old hot shard turns warm, or straight cold when the clock jumped past
	// its end plus ColdAfter (a replay's clock, ADR-0013).
	oldTier := TierWarm
	if ts.coldAfter > 0 && boundary.Before(now.Add(-ts.coldAfter)) {
		oldTier = TierCold
	}
	ts.catalog.UpdateShardTier(oldHot.name, oldTier)
	ts.catalog.UpdateShardTimeEnd(oldHot.name, boundary)
	if promote != nil {
		ts.catalog.UpdateShardTier(newName, TierHot)
	} else {
		ts.catalog.AddShard(ShardEntry{
			Name:      newName,
			Kind:      ShardEvent,
			Tier:      TierHot,
			Path:      newDir,
			TimeStart: windowStart,
			TimeEnd:   windowEnd,
		})
	}
	for _, es := range coldDemotions {
		ts.catalog.UpdateShardTier(es.name, TierCold)
	}

	// Persist the catalog; on failure undo the catalog mutations and remove the
	// new shard (live topology is not yet touched).
	if !ts.inMemory {
		if err := ts.catalog.Save(); err != nil {
			ts.catalog.restoreShards(catalogSnapshot)
			if newStore != nil {
				ts.discardNewEventShard(newStore, newName, newDir)
			}
			return fmt.Errorf("graph: save catalog after rotation: %w", err)
		}
	}

	// Catalog persisted. From here we commit the live in-memory topology.
	oldHot.setTier(oldTier)
	oldHot.readOnly = true
	oldHot.timeEnd = boundary

	if promote != nil {
		promote.setTier(TierHot)
		promote.readOnly = false
		ts.hotShard = promote
	} else {
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
	}

	for _, es := range coldDemotions {
		es.setTier(TierCold)
		// Do NOT close here: idle-close handles it (in-flight reads may hold
		// pointers). The open handle now counts against MaxOpenColdShards.
		es.shardMu.Lock()
		es.countColdStoreLocked(ts)
		es.shardMu.Unlock()
		freeColdRelTemporalIndexes(es.store, es.name)
	}

	if newStore != nil {
		ts.handNewShardToLogScope(newStore, newName)
	}
	return nil
}

// openNewEventShard opens a new event shard's badger store at dir and sets up
// the tracked temporal and anchored indexes before it takes writes. On failure
// it removes what it made; the catalog is not touched.
func (ts *Store) openNewEventShard(name, dir string) (*BadgerStore, error) {
	store, err := ts.openBadgerStore(dir)
	if err != nil {
		return nil, fmt.Errorf("graph: open new event shard %s: %w", name, err)
	}
	err = ts.applyTrackedTemporalIndexes(store)
	if err == nil {
		err = ts.syncAnchoredIndexes(store, true)
	}
	if err != nil {
		ts.discardNewEventShard(store, name, dir)
		return nil, fmt.Errorf("graph: create temporal indexes on new event shard %s: %w", name, err)
	}
	return store, nil
}

// discardNewEventShard closes and removes a new shard the catalog never took.
func (ts *Store) discardNewEventShard(store *BadgerStore, name, dir string) {
	if cerr := store.Close(); cerr != nil {
		slog.Error("graph: rollback close new event shard", "shard", name, "error", cerr)
	}
	if !ts.inMemory {
		if rerr := os.RemoveAll(ts.shardDir(dir)); rerr != nil {
			slog.Error("graph: rollback remove new event shard files", "dir", dir, "error", rerr)
		}
	}
}

// handNewShardToLogScope (BACKLOG 19h): a shard made mid-scope (a rotation or
// an on-demand window shard triggered in a tx/batch) joins the open change-log
// scope IMMEDIATELY, so even the triggering write is buffered instead of
// self-committing outside the scope (unreachable by DiscardLogScope).
// ts.scopeMu is a leaf lock, safe under ts.mu.Lock.
func (ts *Store) handNewShardToLogScope(store *BadgerStore, name string) {
	ts.scopeMu.Lock()
	scopeOpen, scopeDivertOn := ts.scopeOpen, ts.scopeDivertOn
	ts.scopeMu.Unlock()
	if scopeOpen {
		if err := store.BeginLogScope(); err != nil {
			slog.Error("graph: begin change-log scope on a new shard", "shard", name, "error", err)
		} else if scopeDivertOn {
			store.SetLogDivert(true)
		}
	}
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

	if ts.clock().Before(hotEnd) {
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
	if ts.clock().Before(ts.hotShard.timeEnd) {
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
