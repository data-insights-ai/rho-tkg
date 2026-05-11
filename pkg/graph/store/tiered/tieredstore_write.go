package tiered

import (
	"errors"
	"fmt"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	indexpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/index"
	snowflakepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/snowflake"
	storeutil "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/storeutil"
	storecontract "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// --- Property indexes ---

// ErrEventPropertyIndex is returned when attempting to create a property index
// on an event label in a Store. Only reference entities support indexes.
var ErrEventPropertyIndex = errors.New("graph: property indexes only supported for reference entities in Store")

// ErrPrimaryLabelClassMutation is returned when a label mutation would change a
// node's primary-label ontology class (reference vs event). Such a change would
// move the live entity to a different shard while leaving prior history on the
// original shard, fragmenting the version chain.
var ErrPrimaryLabelClassMutation = errors.New("graph: label mutation would change primary-label class (reference<->event)")

// ensurePrimaryLabelClassUnchanged compares the primary-label classes of the
// pre-mutation and post-mutation nodes and rejects the mutation if they differ.
// old may be nil (entity already gone) — in that case the check is skipped.
func (ts *Store) ensurePrimaryLabelClassUnchanged(old, updated *types.Node) error {
	if old == nil || updated == nil {
		return nil
	}
	oldClass := ts.ontology.ClassifyByToken(old.PrimaryLabelToken().Value())
	newClass := ts.ontology.ClassifyByToken(updated.PrimaryLabelToken().Value())
	if oldClass != newClass {
		return ErrPrimaryLabelClassMutation
	}
	return nil
}

func (ts *Store) CreatePropertyIndex(labelToken uint16, propertyKey string) error {
	if err := ts.checkOpen(); err != nil {
		return err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return err
	}
	if err := storecontract.ValidateIndexPropertyKey(propertyKey); err != nil {
		return err
	}
	if ts.ontology.ClassifyByToken(labelToken) != ClassReference {
		return ErrEventPropertyIndex
	}
	return ts.refShard.CreatePropertyIndex(labelToken, propertyKey)
}

func (ts *Store) DropPropertyIndex(labelToken uint16, propertyKey string) error {
	if err := ts.checkOpen(); err != nil {
		return err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return err
	}
	if err := storecontract.ValidateIndexPropertyKey(propertyKey); err != nil {
		return err
	}
	if ts.ontology.ClassifyByToken(labelToken) != ClassReference {
		return ErrEventPropertyIndex
	}
	return ts.refShard.DropPropertyIndex(labelToken, propertyKey)
}

// --- Temporal indexes ---

// CreateTemporalIndex creates a temporal index on nodes with the given label token
// across all shards (reference + all event shards). New hot shards created via
// rotation will also inherit the index.
func (ts *Store) CreateTemporalIndex(labelToken uint16) error {
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return err
	}
	if err := ts.checkOpen(); err != nil {
		return err
	}

	ts.mu.RLock()
	defer ts.mu.RUnlock()

	stores, release, err := ts.allShardStoresWithLazyOpenLocked()
	if err != nil {
		return err
	}
	defer release()

	ts.tempIdxMu.Lock()
	defer ts.tempIdxMu.Unlock()

	for _, tok := range ts.tempIdxLabels {
		if tok == labelToken {
			return nil
		}
	}
	if _, exists := ts.hfIdxBuckets[labelToken]; exists {
		return ErrTemporalIndexExists
	}

	for _, ns := range stores {
		_, hasHighFrequency, _, err := ns.store.TemporalIndexState(labelToken)
		if err != nil {
			return err
		}
		if hasHighFrequency {
			return ErrTemporalIndexExists
		}
	}

	created := make([]namedStore, 0, len(stores))
	for _, ns := range stores {
		hasTemporal, hasHighFrequency, _, stateErr := ns.store.TemporalIndexState(labelToken)
		if stateErr != nil {
			if rollbackErr := rollbackTemporalIndexCreates(created, labelToken); rollbackErr != nil {
				return fmt.Errorf("graph: inspect temporal index state on shard %q: %w (rollback failed: %v)", ns.name, stateErr, rollbackErr)
			}
			return stateErr
		}
		if hasHighFrequency {
			if rollbackErr := rollbackTemporalIndexCreates(created, labelToken); rollbackErr != nil {
				return fmt.Errorf("graph: create temporal index on shard %q: %w (rollback failed: %v)", ns.name, ErrTemporalIndexExists, rollbackErr)
			}
			return ErrTemporalIndexExists
		}
		if hasTemporal {
			continue
		}
		if err := ns.store.CreateTemporalIndex(labelToken); err != nil {
			if rollbackErr := rollbackTemporalIndexCreates(created, labelToken); rollbackErr != nil {
				return fmt.Errorf("graph: create temporal index on shard %q: %w (rollback failed: %v)", ns.name, err, rollbackErr)
			}
			return fmt.Errorf("graph: create temporal index on shard %q: %w", ns.name, err)
		}
		created = append(created, ns)
	}

	temporalFileSnapshot, err := snapshotTemporalIndexFile(ts.temporalIdxFile)
	if err != nil {
		if rollbackErr := rollbackTemporalIndexCreates(created, labelToken); rollbackErr != nil {
			return fmt.Errorf("graph: snapshot temporal index definitions: %w (rollback failed: %v)", err, rollbackErr)
		}
		return err
	}

	// Record label token if not already tracked.
	for _, tok := range ts.tempIdxLabels {
		if tok == labelToken {
			return nil
		}
	}
	ts.tempIdxLabels = append(ts.tempIdxLabels, labelToken)
	if err := ts.persistTemporalIndexDefsLocked(); err != nil {
		ts.tempIdxLabels = ts.tempIdxLabels[:len(ts.tempIdxLabels)-1]
		if rollbackErr := rollbackTemporalIndexCreates(created, labelToken); rollbackErr != nil {
			if restoreErr := restoreTemporalIndexFile(temporalFileSnapshot); restoreErr != nil {
				return fmt.Errorf("graph: persist temporal index definitions: %w (rollback failed: %v; file restore failed: %v)", err, rollbackErr, restoreErr)
			}
			return fmt.Errorf("graph: persist temporal index definitions: %w (rollback failed: %v)", err, rollbackErr)
		}
		if restoreErr := restoreTemporalIndexFile(temporalFileSnapshot); restoreErr != nil {
			return fmt.Errorf("graph: persist temporal index definitions: %w (file restore failed: %v)", err, restoreErr)
		}
		return err
	}
	return nil
}

// DropTemporalIndex removes the temporal index for the given label token
// from all shards.
func (ts *Store) DropTemporalIndex(labelToken uint16) error {
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return err
	}
	if err := ts.checkOpen(); err != nil {
		return err
	}

	ts.mu.RLock()
	defer ts.mu.RUnlock()

	stores, release, err := ts.allShardStoresWithLazyOpenLocked()
	if err != nil {
		return err
	}
	defer release()

	ts.tempIdxMu.Lock()
	defer ts.tempIdxMu.Unlock()

	temporalFileSnapshot, err := snapshotTemporalIndexFile(ts.temporalIdxFile)
	if err != nil {
		return err
	}

	found := false
	dropped := make([]namedStore, 0, len(stores))
	for _, ns := range stores {
		shard := ns.store
		if err := shard.DropTemporalIndex(labelToken); err != nil {
			if errors.Is(err, ErrTemporalIndexNotFound) {
				continue
			}
			if rollbackErr := rollbackTemporalIndexDrops(dropped, labelToken); rollbackErr != nil {
				return fmt.Errorf("graph: drop temporal index on shard %q: %w (rollback failed: %v)", ns.name, err, rollbackErr)
			}
			return fmt.Errorf("graph: drop temporal index on shard %q: %w", ns.name, err)
		}
		found = true
		dropped = append(dropped, ns)
	}

	hadTrackedLabel := false
	for i, tok := range ts.tempIdxLabels {
		if tok == labelToken {
			ts.tempIdxLabels = append(ts.tempIdxLabels[:i], ts.tempIdxLabels[i+1:]...)
			hadTrackedLabel = true
			break
		}
	}

	if !found {
		return ErrTemporalIndexNotFound
	}
	if err := ts.persistTemporalIndexDefsLocked(); err != nil {
		if hadTrackedLabel {
			ts.tempIdxLabels = append(ts.tempIdxLabels, labelToken)
		}
		if rollbackErr := rollbackTemporalIndexDrops(dropped, labelToken); rollbackErr != nil {
			if restoreErr := restoreTemporalIndexFile(temporalFileSnapshot); restoreErr != nil {
				return fmt.Errorf("graph: persist temporal index definition drop: %w (rollback failed: %v; file restore failed: %v)", err, rollbackErr, restoreErr)
			}
			return fmt.Errorf("graph: persist temporal index definition drop: %w (rollback failed: %v)", err, rollbackErr)
		}
		if restoreErr := restoreTemporalIndexFile(temporalFileSnapshot); restoreErr != nil {
			return fmt.Errorf("graph: persist temporal index definition drop: %w (file restore failed: %v)", err, restoreErr)
		}
		return err
	}
	return nil
}

// --- High-frequency indexes ---

// CreateHighFrequencyIndex creates a time-bucketed high-frequency index on nodes
// with the given label token across all shards (reference + all event shards).
// New hot shards created via rotation and lazily opened archives inherit the index.
// Returns ErrInvalidTemporalIndexConfig if bucketSize is not a positive whole millisecond.
// Returns ErrTemporalIndexExists if any temporal index already exists for this label.
func (ts *Store) CreateHighFrequencyIndex(labelToken uint16, bucketSize time.Duration) error {
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return err
	}
	if err := storecontract.ValidateHighFrequencyBucketSize(bucketSize); err != nil {
		return err
	}
	if err := ts.checkOpen(); err != nil {
		return err
	}

	ts.mu.RLock()
	defer ts.mu.RUnlock()

	stores, release, err := ts.allShardStoresWithLazyOpenLocked()
	if err != nil {
		return err
	}
	defer release()

	ts.tempIdxMu.Lock()
	defer ts.tempIdxMu.Unlock()

	for _, tok := range ts.tempIdxLabels {
		if tok == labelToken {
			return ErrTemporalIndexExists
		}
	}
	if existingBucket, exists := ts.hfIdxBuckets[labelToken]; exists {
		if existingBucket != bucketSize {
			return ErrTemporalIndexExists
		}
		return nil
	}

	for _, ns := range stores {
		hasTemporal, hasHighFrequency, existingBucket, err := ns.store.TemporalIndexState(labelToken)
		if err != nil {
			return err
		}
		if hasTemporal {
			return ErrTemporalIndexExists
		}
		if hasHighFrequency && existingBucket != bucketSize {
			return ErrTemporalIndexExists
		}
	}

	created := make([]namedStore, 0, len(stores))
	for _, ns := range stores {
		hasTemporal, hasHighFrequency, existingBucket, stateErr := ns.store.TemporalIndexState(labelToken)
		if stateErr != nil {
			if rollbackErr := rollbackHighFrequencyIndexCreates(created, labelToken); rollbackErr != nil {
				return fmt.Errorf("graph: inspect temporal index state on shard %q: %w (rollback failed: %v)", ns.name, stateErr, rollbackErr)
			}
			return stateErr
		}
		if hasTemporal {
			if rollbackErr := rollbackHighFrequencyIndexCreates(created, labelToken); rollbackErr != nil {
				return fmt.Errorf("graph: create high-frequency index on shard %q: %w (rollback failed: %v)", ns.name, ErrTemporalIndexExists, rollbackErr)
			}
			return ErrTemporalIndexExists
		}
		if hasHighFrequency {
			if existingBucket != bucketSize {
				if rollbackErr := rollbackHighFrequencyIndexCreates(created, labelToken); rollbackErr != nil {
					return fmt.Errorf("graph: create high-frequency index on shard %q: %w (rollback failed: %v)", ns.name, ErrTemporalIndexExists, rollbackErr)
				}
				return ErrTemporalIndexExists
			}
			continue
		}
		if err := ns.store.CreateHighFrequencyIndex(labelToken, bucketSize); err != nil {
			if rollbackErr := rollbackHighFrequencyIndexCreates(created, labelToken); rollbackErr != nil {
				return fmt.Errorf("graph: create high-frequency index on shard %q: %w (rollback failed: %v)", ns.name, err, rollbackErr)
			}
			return fmt.Errorf("graph: create high-frequency index on shard %q: %w", ns.name, err)
		}
		created = append(created, ns)
	}
	temporalFileSnapshot, err := snapshotTemporalIndexFile(ts.temporalIdxFile)
	if err != nil {
		if rollbackErr := rollbackHighFrequencyIndexCreates(created, labelToken); rollbackErr != nil {
			return fmt.Errorf("graph: snapshot temporal index definitions: %w (rollback failed: %v)", err, rollbackErr)
		}
		return err
	}
	if ts.hfIdxBuckets == nil {
		ts.hfIdxBuckets = make(map[uint16]time.Duration)
	}
	ts.hfIdxBuckets[labelToken] = bucketSize
	if err := ts.persistTemporalIndexDefsLocked(); err != nil {
		delete(ts.hfIdxBuckets, labelToken)
		if rollbackErr := rollbackHighFrequencyIndexCreates(created, labelToken); rollbackErr != nil {
			if restoreErr := restoreTemporalIndexFile(temporalFileSnapshot); restoreErr != nil {
				return fmt.Errorf("graph: persist high-frequency index definitions: %w (rollback failed: %v; file restore failed: %v)", err, rollbackErr, restoreErr)
			}
			return fmt.Errorf("graph: persist high-frequency index definitions: %w (rollback failed: %v)", err, rollbackErr)
		}
		if restoreErr := restoreTemporalIndexFile(temporalFileSnapshot); restoreErr != nil {
			return fmt.Errorf("graph: persist high-frequency index definitions: %w (file restore failed: %v)", err, restoreErr)
		}
		return err
	}
	return nil
}

// DropHighFrequencyIndex removes the high-frequency index for the given label token
// from all shards. Returns ErrTemporalIndexNotFound if no index exists on any shard.
func (ts *Store) DropHighFrequencyIndex(labelToken uint16) error {
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return err
	}
	if err := ts.checkOpen(); err != nil {
		return err
	}

	ts.mu.RLock()
	defer ts.mu.RUnlock()

	stores, release, err := ts.allShardStoresWithLazyOpenLocked()
	if err != nil {
		return err
	}
	defer release()

	ts.tempIdxMu.Lock()
	defer ts.tempIdxMu.Unlock()

	temporalFileSnapshot, err := snapshotTemporalIndexFile(ts.temporalIdxFile)
	if err != nil {
		return err
	}

	found := false
	dropped := make([]droppedHFI, 0, len(stores))
	for _, ns := range stores {
		shard := ns.store
		_, hasHFI, bucketSize, bucketErr := shard.TemporalIndexState(labelToken)
		if bucketErr != nil {
			if rollbackErr := rollbackHighFrequencyIndexDrops(dropped, labelToken); rollbackErr != nil {
				return fmt.Errorf("graph: inspect high-frequency index on shard %q: %w (rollback failed: %v)", ns.name, bucketErr, rollbackErr)
			}
			return bucketErr
		}
		if err := shard.DropHighFrequencyIndex(labelToken); err != nil {
			if errors.Is(err, ErrTemporalIndexNotFound) {
				continue
			}
			if rollbackErr := rollbackHighFrequencyIndexDrops(dropped, labelToken); rollbackErr != nil {
				return fmt.Errorf("graph: drop high-frequency index on shard %q: %w (rollback failed: %v)", ns.name, err, rollbackErr)
			}
			return fmt.Errorf("graph: drop high-frequency index on shard %q: %w", ns.name, err)
		}
		found = true
		if hasHFI {
			dropped = append(dropped, droppedHFI{shard: ns, bucketSize: bucketSize})
		}
	}
	previousBucket, hadTrackedBucket := ts.hfIdxBuckets[labelToken]
	if found {
		delete(ts.hfIdxBuckets, labelToken)
	}
	if !found {
		return ErrTemporalIndexNotFound
	}
	if err := ts.persistTemporalIndexDefsLocked(); err != nil {
		if hadTrackedBucket {
			ts.hfIdxBuckets[labelToken] = previousBucket
		}
		if rollbackErr := rollbackHighFrequencyIndexDrops(dropped, labelToken); rollbackErr != nil {
			if restoreErr := restoreTemporalIndexFile(temporalFileSnapshot); restoreErr != nil {
				return fmt.Errorf("graph: persist high-frequency index definition drop: %w (rollback failed: %v; file restore failed: %v)", err, rollbackErr, restoreErr)
			}
			return fmt.Errorf("graph: persist high-frequency index definition drop: %w (rollback failed: %v)", err, rollbackErr)
		}
		if restoreErr := restoreTemporalIndexFile(temporalFileSnapshot); restoreErr != nil {
			return fmt.Errorf("graph: persist high-frequency index definition drop: %w (file restore failed: %v)", err, restoreErr)
		}
		return err
	}
	return nil
}

func rollbackTemporalIndexCreates(stores []namedStore, labelToken uint16) error {
	var rollbackErr error
	for i := len(stores) - 1; i >= 0; i-- {
		shard := stores[i]
		if err := shard.store.DropTemporalIndex(labelToken); err != nil && !errors.Is(err, ErrTemporalIndexNotFound) {
			rollbackErr = errors.Join(rollbackErr, fmt.Errorf("drop temporal index on shard %q: %w", shard.name, err))
		}
	}
	return rollbackErr
}

func rollbackHighFrequencyIndexCreates(stores []namedStore, labelToken uint16) error {
	var rollbackErr error
	for i := len(stores) - 1; i >= 0; i-- {
		shard := stores[i]
		if err := shard.store.DropHighFrequencyIndex(labelToken); err != nil && !errors.Is(err, ErrTemporalIndexNotFound) {
			rollbackErr = errors.Join(rollbackErr, fmt.Errorf("drop high-frequency index on shard %q: %w", shard.name, err))
		}
	}
	return rollbackErr
}

func rollbackTemporalIndexDrops(stores []namedStore, labelToken uint16) error {
	var rollbackErr error
	for i := len(stores) - 1; i >= 0; i-- {
		shard := stores[i]
		if err := shard.store.CreateTemporalIndex(labelToken); err != nil && !errors.Is(err, ErrTemporalIndexExists) {
			rollbackErr = errors.Join(rollbackErr, fmt.Errorf("recreate temporal index on shard %q: %w", shard.name, err))
		}
	}
	return rollbackErr
}

type droppedHFI struct {
	shard      namedStore
	bucketSize time.Duration
}

func rollbackHighFrequencyIndexDrops(dropped []droppedHFI, labelToken uint16) error {
	var rollbackErr error
	for i := len(dropped) - 1; i >= 0; i-- {
		item := dropped[i]
		if err := item.shard.store.CreateHighFrequencyIndex(labelToken, item.bucketSize); err != nil && !errors.Is(err, ErrTemporalIndexExists) {
			rollbackErr = errors.Join(rollbackErr, fmt.Errorf("recreate high-frequency index on shard %q: %w", item.shard.name, err))
		}
	}
	return rollbackErr
}

type appliedTrackedTemporalIndexes struct {
	store    *BadgerStore
	temporal []uint16
	hfi      []uint16
}

func (ts *Store) applyTrackedTemporalIndexes(store *BadgerStore) error {
	_, err := ts.applyTrackedTemporalIndexesTracked(store)
	return err
}

func (ts *Store) applyTrackedTemporalIndexesTracked(store *BadgerStore) (appliedTrackedTemporalIndexes, error) {
	ts.tempIdxMu.Lock()
	tempLabels := append([]uint16(nil), ts.tempIdxLabels...)
	hfBuckets := make(map[uint16]time.Duration, len(ts.hfIdxBuckets))
	for tok, bucket := range ts.hfIdxBuckets {
		hfBuckets[tok] = bucket
	}
	ts.tempIdxMu.Unlock()

	applied := appliedTrackedTemporalIndexes{store: store}
	createdTemporal := make([]uint16, 0, len(tempLabels))
	createdHFI := make([]uint16, 0, len(hfBuckets))
	for _, tok := range tempLabels {
		created, err := ensureTemporalIndexKind(store, tok)
		if err != nil {
			if rollbackErr := rollbackAppliedTrackedTemporalIndexes(store, createdTemporal, createdHFI); rollbackErr != nil {
				return appliedTrackedTemporalIndexes{}, fmt.Errorf("%w (rollback failed: %v)", err, rollbackErr)
			}
			return appliedTrackedTemporalIndexes{}, err
		}
		if created {
			createdTemporal = append(createdTemporal, tok)
		}
	}
	for tok, bucket := range hfBuckets {
		created, err := ensureHighFrequencyIndexKind(store, tok, bucket)
		if err != nil {
			if rollbackErr := rollbackAppliedTrackedTemporalIndexes(store, createdTemporal, createdHFI); rollbackErr != nil {
				return appliedTrackedTemporalIndexes{}, fmt.Errorf("%w (rollback failed: %v)", err, rollbackErr)
			}
			return appliedTrackedTemporalIndexes{}, err
		}
		if created {
			createdHFI = append(createdHFI, tok)
		}
	}
	applied.temporal = createdTemporal
	applied.hfi = createdHFI
	return applied, nil
}

func rollbackAppliedTrackedTemporalIndexes(store *BadgerStore, temporal []uint16, hfi []uint16) error {
	var rollbackErr error
	for i := len(hfi) - 1; i >= 0; i-- {
		if err := store.DropHighFrequencyIndex(hfi[i]); err != nil && !errors.Is(err, ErrTemporalIndexNotFound) {
			rollbackErr = errors.Join(rollbackErr, fmt.Errorf("drop high-frequency index %d: %w", hfi[i], err))
		}
	}
	for i := len(temporal) - 1; i >= 0; i-- {
		if err := store.DropTemporalIndex(temporal[i]); err != nil && !errors.Is(err, ErrTemporalIndexNotFound) {
			rollbackErr = errors.Join(rollbackErr, fmt.Errorf("drop temporal index %d: %w", temporal[i], err))
		}
	}
	return rollbackErr
}

func rollbackAppliedTrackedTemporalIndexStores(applied []appliedTrackedTemporalIndexes) error {
	var rollbackErr error
	for i := len(applied) - 1; i >= 0; i-- {
		item := applied[i]
		rollbackErr = errors.Join(rollbackErr, rollbackAppliedTrackedTemporalIndexes(item.store, item.temporal, item.hfi))
	}
	return rollbackErr
}

func ensureTemporalIndexKind(store *BadgerStore, labelToken uint16) (bool, error) {
	hasTemporal, hasHighFrequency, _, err := store.TemporalIndexState(labelToken)
	if err != nil {
		return false, err
	}
	if hasHighFrequency {
		return false, ErrTemporalIndexExists
	}
	if hasTemporal {
		return false, nil
	}
	if err := store.CreateTemporalIndex(labelToken); err != nil {
		return false, err
	}
	return true, nil
}

func ensureHighFrequencyIndexKind(store *BadgerStore, labelToken uint16, bucketSize time.Duration) (bool, error) {
	hasTemporal, hasHighFrequency, existingBucket, err := store.TemporalIndexState(labelToken)
	if err != nil {
		return false, err
	}
	if hasTemporal {
		return false, ErrTemporalIndexExists
	}
	if hasHighFrequency {
		if existingBucket != bucketSize {
			return false, ErrTemporalIndexExists
		}
		return false, nil
	}
	if err := store.CreateHighFrequencyIndex(labelToken, bucketSize); err != nil {
		return false, err
	}
	return true, nil
}

// CreateVectorIndex creates a vector similarity index spanning all shards.
// The index is maintained at the Store level (not per-shard).
// Scans existing nodes across all shards to populate the index.
// Returns ErrVectorIndexExists on duplicate.
func (ts *Store) CreateVectorIndex(labelToken uint16, propertyKey string, dims int, metric DistanceMetric) error {
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return err
	}
	if err := storecontract.ValidateIndexPropertyKey(propertyKey); err != nil {
		return err
	}
	if err := indexpkg.ValidateVectorIndexConfig(dims, metric); err != nil {
		return err
	}
	if err := ts.checkOpen(); err != nil {
		return err
	}

	key := indexpkg.VectorIndexKey{LabelToken: labelToken, PropertyKey: propertyKey}

	ts.vectorIdxMu.Lock()
	if _, exists := ts.vectorIndexes[key]; exists {
		ts.vectorIdxMu.Unlock()
		return ErrVectorIndexExists
	}
	vi := &indexpkg.VectorIndex{Dims: dims, Metric: metric, Mutated: make(map[snowflake.ID]struct{})}
	ts.vectorIndexes[key] = vi
	ts.vectorIdxMu.Unlock()

	// Populate from all existing nodes across all shards.
	nodes, err := ts.AllNodes(QueryOpts{})
	if err != nil {
		ts.vectorIdxMu.Lock()
		indexpkg.DeleteVectorIndexIfCurrent(ts.vectorIndexes, key, vi)
		ts.vectorIdxMu.Unlock()
		return err
	}
	type vectorBackfillEntry struct {
		id  snowflake.ID
		vec []float32
	}
	backfill := make([]vectorBackfillEntry, 0, len(nodes))
	for _, n := range nodes {
		if !n.HasLabelTokenRaw(labelToken) {
			continue
		}
		val, ok := n.GetProperty(propertyKey)
		if !ok {
			continue
		}
		vec, ok := indexpkg.ToFloat32Slice(val)
		if !ok {
			continue
		}
		id := n.ID().SnowflakeID()
		cp := make([]float32, len(vec))
		copy(cp, vec)
		backfill = append(backfill, vectorBackfillEntry{id: id, vec: cp})
	}

	ts.vectorIdxMu.Lock()
	defer ts.vectorIdxMu.Unlock()
	if err := indexpkg.RequireVectorIndexCurrentForCreate(ts.vectorIndexes, key, vi); err != nil {
		return err
	}
	for _, entry := range backfill {
		if vi.WasMutated(entry.id) {
			continue
		}
		if _, err := ts.GetNode(types.NodeID(entry.id)); err != nil {
			if errors.Is(err, ErrNodeNotFound) {
				continue
			}
			indexpkg.DeleteVectorIndexIfCurrent(ts.vectorIndexes, key, vi)
			return fmt.Errorf("graph: create vector index: verify node %d: %w", entry.id, err)
		}
		if err := vi.Add(entry.id, entry.vec); err != nil {
			indexpkg.DeleteVectorIndexIfCurrent(ts.vectorIndexes, key, vi)
			return fmt.Errorf("graph: create vector index: node %d: %w", entry.id, err)
		}
	}
	vi.ClearMutationTracking()
	vectorFileSnapshot, err := snapshotVectorIndexFile(ts.vectorIdxFile)
	if err != nil {
		indexpkg.DeleteVectorIndexIfCurrent(ts.vectorIndexes, key, vi)
		return err
	}
	if err := ts.persistVectorIndexDefsLocked(); err != nil {
		indexpkg.DeleteVectorIndexIfCurrent(ts.vectorIndexes, key, vi)
		if restoreErr := restoreVectorIndexFile(vectorFileSnapshot); restoreErr != nil {
			return fmt.Errorf("graph: persist vector index definition: %w (rollback failed: %v)", err, restoreErr)
		}
		return fmt.Errorf("graph: persist vector index definition: %w", err)
	}
	return nil
}

// DropVectorIndex removes a vector index.
// Returns ErrVectorIndexNotFound if the index does not exist.
func (ts *Store) DropVectorIndex(labelToken uint16, propertyKey string) error {
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return err
	}
	if err := storecontract.ValidateIndexPropertyKey(propertyKey); err != nil {
		return err
	}
	if err := ts.checkOpen(); err != nil {
		return err
	}

	ts.vectorIdxMu.Lock()
	defer ts.vectorIdxMu.Unlock()

	key := indexpkg.VectorIndexKey{LabelToken: labelToken, PropertyKey: propertyKey}
	if _, exists := ts.vectorIndexes[key]; !exists {
		return ErrVectorIndexNotFound
	}
	vectorFileSnapshot, err := snapshotVectorIndexFile(ts.vectorIdxFile)
	if err != nil {
		return err
	}
	idx := ts.vectorIndexes[key]
	delete(ts.vectorIndexes, key)
	if err := ts.persistVectorIndexDefsLocked(); err != nil {
		ts.vectorIndexes[key] = idx
		if restoreErr := restoreVectorIndexFile(vectorFileSnapshot); restoreErr != nil {
			return fmt.Errorf("graph: persist vector index definition drop: %w (rollback failed: %v)", err, restoreErr)
		}
		return fmt.Errorf("graph: persist vector index definition drop: %w", err)
	}
	return nil
}

// SearchNearestNodes returns the k nodes with vectors closest to query
// under the index defined for labelToken+propertyKey.
// Results are ordered by ascending distance (closest first).
// Returns ErrVectorIndexNotFound if no index exists.
// Returns ErrDimensionMismatch if query length differs from the index's dims.
// Returns nil, nil if the index exists but has no entries.
func (ts *Store) SearchNearestNodes(labelToken uint16, propertyKey string, query []float32, k int, opts QueryOpts) ([]*types.Node, error) {
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateIndexPropertyKey(propertyKey); err != nil {
		return nil, err
	}
	if err := ts.checkOpen(); err != nil {
		return nil, err
	}
	if err := validateQueryOpts(opts); err != nil {
		return nil, err
	}

	ts.vectorIdxMu.RLock()
	key := indexpkg.VectorIndexKey{LabelToken: labelToken, PropertyKey: propertyKey}
	vi, exists := ts.vectorIndexes[key]
	ts.vectorIdxMu.RUnlock()

	if !exists {
		return nil, ErrVectorIndexNotFound
	}
	if vi.IsBuilding() {
		return nil, ErrVectorIndexNotFound
	}

	// Depth gating mirrors NodesByLabel/AllNodes/RelationshipsByType:
	// DepthHot/DepthWarm include live reference nodes but exclude refArchive,
	// and event-shard nodes must match the requested hot/warm tier.
	//
	// Filtering happens BEFORE the heap selection (passed into searchNearest
	// as the filter callback): otherwise a near-but-archived candidate
	// could push a farther-but-eligible candidate out of the top-k.
	depthFilter, releaseDepthFilter, err := ts.depthFilter(opts.Depth)
	if err != nil {
		return nil, err
	}
	defer releaseDepthFilter()
	temporalFilter, err := ts.vectorTemporalFilter(vi, opts)
	if err != nil {
		return nil, err
	}
	filter := depthFilter
	if temporalFilter != nil {
		if depthFilter == nil {
			filter = temporalFilter
		} else {
			filter = func(id snowflake.ID) bool {
				return depthFilter(id) && temporalFilter(id)
			}
		}
	}
	ids, err := vi.SearchNearest(query, k, filter)
	if err != nil {
		return nil, err
	}
	if err := ts.checkOpen(); err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, nil
	}
	// Fetch nodes in distance order — do NOT sort by ID (would destroy distance ranking).
	hasTemporal := storeutil.HasTemporalFilter(opts)
	result := make([]*types.Node, 0, len(ids))
	for _, id := range ids {
		n, err := ts.GetNode(types.NodeID(id))
		if err != nil {
			if errors.Is(err, ErrNodeNotFound) {
				continue // node may have been deleted concurrently
			}
			return nil, fmt.Errorf("graph: resolve vector search candidate: %w", err)
		}
		if hasTemporal && !storeutil.MatchesTemporalFilter(id, n.Temporal(), opts) {
			continue
		}
		result = append(result, n)
	}
	if len(result) == 0 {
		return nil, nil
	}
	return storeutil.PaginateNodesInOrder(result, opts.After, opts.Limit), nil
}

func (ts *Store) vectorTemporalFilter(vi *indexpkg.VectorIndex, opts QueryOpts) (func(snowflake.ID) bool, error) {
	if !storeutil.HasTemporalFilter(opts) {
		return nil, nil
	}
	eligible := make(map[snowflake.ID]struct{})
	for _, id := range vi.IDs() {
		n, err := ts.GetNode(types.NodeID(id))
		if err != nil {
			if errors.Is(err, ErrNodeNotFound) {
				continue
			}
			return nil, err
		}
		if storeutil.MatchesTemporalFilter(id, n.Temporal(), opts) {
			eligible[id] = struct{}{}
		}
	}
	return func(id snowflake.ID) bool {
		_, ok := eligible[id]
		return ok
	}, nil
}

// SearchNearestFiltered is the package-internal entry point used by the
// Graph layer's TEMPORAL path to perform vector search with an
// eligibility filter applied BEFORE the k-cut. The filter is invoked while
// the vector index is being scanned; it should avoid mutating or re-entering
// the same vector index.
//
// Depth gating is applied by Store.SearchNearestNodes. The graph layer uses
// this filtered hook for temporal DepthAll queries and falls back to
// SearchNearestNodes with QueryOpts.Depth when depth filtering is also needed.
//
// Returns raw snowflake.IDs in ascending distance order; the caller is
// responsible for resolving entities (current or historical version).
func (ts *Store) SearchNearestFiltered(labelToken uint16, propertyKey string, query []float32, k int, filter func(snowflake.ID) bool) ([]snowflake.ID, error) {
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateIndexPropertyKey(propertyKey); err != nil {
		return nil, err
	}
	if err := ts.checkOpen(); err != nil {
		return nil, err
	}

	ts.vectorIdxMu.RLock()
	key := indexpkg.VectorIndexKey{LabelToken: labelToken, PropertyKey: propertyKey}
	vi, exists := ts.vectorIndexes[key]
	ts.vectorIdxMu.RUnlock()

	if !exists {
		return nil, ErrVectorIndexNotFound
	}
	if vi.IsBuilding() {
		return nil, ErrVectorIndexNotFound
	}
	ids, err := vi.SearchNearest(query, k, filter)
	if err != nil {
		return nil, err
	}
	if err := ts.checkOpen(); err != nil {
		return nil, err
	}
	return ids, nil
}

// depthFilter returns an eligibility predicate matching AllNodes/NodesByLabel:
// DepthAll accepts every indexed node; DepthHot accepts live reference nodes
// plus hot event-shard nodes; DepthWarm accepts live reference nodes plus
// hot/warm event-shard nodes; refArchive is excluded from DepthHot/DepthWarm.
// Filtering happens before vector heap selection so near-but-ineligible
// candidates cannot crowd out farther eligible candidates from the top-k.
func (ts *Store) depthFilter(depth ShardDepth) (func(snowflake.ID) bool, func(), error) {
	noop := func() {}
	if err := validateDepth(depth); err != nil {
		return nil, noop, err
	}
	if depth == DepthAll {
		return nil, noop, nil
	}

	type eventWindow struct {
		start time.Time
		end   time.Time
		tier  ShardTier
	}

	archive, archiveCheckin, archiveErr := ts.checkoutArchive()
	if archiveErr != nil {
		return nil, noop, archiveErr
	}
	releaseArchive := archiveCheckin

	ts.mu.RLock()
	windows := make([]eventWindow, 0, len(ts.eventShards))
	for _, es := range ts.eventShards {
		windows = append(windows, eventWindow{
			start: es.timeStart,
			end:   es.timeEnd,
			tier:  es.tier,
		})
	}
	ts.mu.RUnlock()

	return func(id snowflake.ID) bool {
		if ts.refShard.HasNodeID(id) {
			return true
		}
		if archive != nil && archive.HasNodeID(id) {
			return false
		}

		created := snowflakepkg.Layout.CreatedAt(id)
		for _, w := range windows {
			if created.Before(w.start) || !created.Before(w.end) {
				continue
			}
			switch depth {
			case DepthHot:
				return w.tier == TierHot
			case DepthWarm:
				return w.tier == TierHot || w.tier == TierWarm
			default:
				return false
			}
		}

		return true
	}, releaseArchive, nil
}
