package tiered

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/vmihailenco/msgpack/v5"
	storecontract "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
)

// temporalIndexFileData is the store-level serialization format for temporal
// index tracking. Shard-level Badger stores persist their own index definitions;
// this file preserves the TieredStore tracking needed for new/lazy shards.
type temporalIndexFileData struct {
	TemporalLabels []uint16       `msgpack:"t"`
	HighFrequency  []tieredHFIdef `msgpack:"h"`
}

type tieredHFIdef struct {
	LabelToken       uint16 `msgpack:"l"`
	BucketSizeMillis int64  `msgpack:"b"`
}

const maxTieredHFBucketMillis = int64(1<<63-1) / int64(time.Millisecond)

func tieredHFBucketDuration(bucketMillis int64) (time.Duration, error) {
	if bucketMillis <= 0 || bucketMillis > maxTieredHFBucketMillis {
		return 0, fmt.Errorf("%w: high-frequency bucket size must be a positive whole millisecond, got %dms",
			ErrInvalidTemporalIndexConfig, bucketMillis)
	}
	bucketSize := time.Duration(bucketMillis) * time.Millisecond
	if err := storecontract.ValidateHighFrequencyBucketSize(bucketSize); err != nil {
		return 0, err
	}
	return bucketSize, nil
}

func saveTemporalIndexFile(path string, data temporalIndexFileData) error {
	if len(data.TemporalLabels) == 0 && len(data.HighFrequency) == 0 {
		if err := os.Remove(path); err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return fmt.Errorf("temporal index file: remove: %w", err)
		}
		if err := syncParentDir(path, "temporal index file"); err != nil {
			return err
		}
		return nil
	}
	encoded, err := msgpack.Marshal(&data)
	if err != nil {
		return fmt.Errorf("temporal index file: marshal: %w", err)
	}
	return atomicWriteFile(path, encoded, "temporal index file")
}

type temporalIndexFileSnapshot struct {
	path   string
	data   []byte
	exists bool
}

func snapshotTemporalIndexFile(path string) (temporalIndexFileSnapshot, error) {
	if path == "" {
		return temporalIndexFileSnapshot{}, nil
	}
	data, err := os.ReadFile(path) // #nosec G304 -- path derived from trusted Store config
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return temporalIndexFileSnapshot{path: path}, nil
		}
		return temporalIndexFileSnapshot{}, fmt.Errorf("temporal index file: snapshot: %w", err)
	}
	return temporalIndexFileSnapshot{path: path, data: data, exists: true}, nil
}

func restoreTemporalIndexFile(snapshot temporalIndexFileSnapshot) error {
	if snapshot.path == "" {
		return nil
	}
	if snapshot.exists {
		return atomicWriteFile(snapshot.path, snapshot.data, "temporal index rollback")
	}
	if err := os.Remove(snapshot.path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("temporal index rollback: remove: %w", err)
	}
	if err := syncParentDir(snapshot.path, "temporal index rollback"); err != nil {
		return err
	}
	return nil
}

func loadTemporalIndexFile(path string) (temporalIndexFileData, error) {
	data, err := os.ReadFile(path) // #nosec G304 -- path derived from caller-provided Config.DataDir
	if err != nil {
		if os.IsNotExist(err) {
			return temporalIndexFileData{}, nil
		}
		return temporalIndexFileData{}, fmt.Errorf("temporal index file: read: %w", err)
	}
	var out temporalIndexFileData
	if err := msgpack.Unmarshal(data, &out); err != nil {
		return temporalIndexFileData{}, fmt.Errorf("temporal index file: unmarshal: %w", err)
	}
	return out, nil
}

func (ts *Store) loadTemporalIndexDefs() error {
	data, err := loadTemporalIndexFile(ts.temporalIdxFile)
	if err != nil {
		return err
	}
	if len(data.TemporalLabels) == 0 && len(data.HighFrequency) == 0 {
		return nil
	}

	labels := make([]uint16, 0, len(data.TemporalLabels))
	seenTemporal := make(map[uint16]struct{}, len(data.TemporalLabels))
	for _, tok := range data.TemporalLabels {
		if err := storecontract.ValidateLabelToken(tok); err != nil {
			return fmt.Errorf("temporal index file: invalid temporal label %d: %w", tok, err)
		}
		if _, exists := seenTemporal[tok]; exists {
			continue
		}
		seenTemporal[tok] = struct{}{}
		labels = append(labels, tok)
	}

	hfBuckets := make(map[uint16]time.Duration, len(data.HighFrequency))
	for _, def := range data.HighFrequency {
		if err := storecontract.ValidateLabelToken(def.LabelToken); err != nil {
			return fmt.Errorf("temporal index file: invalid high-frequency label %d: %w", def.LabelToken, err)
		}
		if _, conflict := seenTemporal[def.LabelToken]; conflict {
			return fmt.Errorf("temporal index file: label %d has both temporal and high-frequency definitions: %w",
				def.LabelToken, ErrTemporalIndexExists)
		}
		bucketSize, err := tieredHFBucketDuration(def.BucketSizeMillis)
		if err != nil {
			return fmt.Errorf("temporal index file: invalid high-frequency label %d: %w", def.LabelToken, err)
		}
		if existing, exists := hfBuckets[def.LabelToken]; exists && existing != bucketSize {
			return fmt.Errorf("temporal index file: label %d has conflicting high-frequency buckets: %w",
				def.LabelToken, ErrTemporalIndexExists)
		}
		hfBuckets[def.LabelToken] = bucketSize
	}

	ts.tempIdxMu.Lock()
	previousLabels := append([]uint16(nil), ts.tempIdxLabels...)
	previousHFBuckets := cloneHFBuckets(ts.hfIdxBuckets)
	ts.tempIdxLabels = labels
	ts.hfIdxBuckets = hfBuckets
	ts.tempIdxMu.Unlock()

	applied := make([]appliedTrackedTemporalIndexes, 0)
	for _, shard := range ts.openTemporalIndexShardStores() {
		created, err := ts.applyTrackedTemporalIndexesTracked(shard)
		if err != nil {
			ts.tempIdxMu.Lock()
			ts.tempIdxLabels = previousLabels
			ts.hfIdxBuckets = previousHFBuckets
			ts.tempIdxMu.Unlock()
			if rollbackErr := rollbackAppliedTrackedTemporalIndexStores(applied); rollbackErr != nil {
				return fmt.Errorf("temporal index file: apply tracked indexes: %w (rollback failed: %v)", err, rollbackErr)
			}
			return err
		}
		if len(created.temporal) > 0 || len(created.hfi) > 0 {
			applied = append(applied, created)
		}
	}
	return nil
}

func cloneHFBuckets(src map[uint16]time.Duration) map[uint16]time.Duration {
	if src == nil {
		return nil
	}
	out := make(map[uint16]time.Duration, len(src))
	for tok, bucket := range src {
		out[tok] = bucket
	}
	return out
}

func (ts *Store) openTemporalIndexShardStores() []*BadgerStore {
	stores := []*BadgerStore{ts.refShard}
	if archive := ts.refArchive.Load(); archive != nil {
		stores = append(stores, archive)
	}
	for _, es := range ts.eventShards {
		if es.store != nil {
			stores = append(stores, es.store)
		}
	}
	return stores
}

// persistTemporalIndexDefsLocked writes the current store-level temporal index
// tracking definitions. Caller must hold ts.tempIdxMu.
func (ts *Store) persistTemporalIndexDefsLocked() error {
	if ts.inMemory {
		return nil
	}
	labels := append([]uint16(nil), ts.tempIdxLabels...)
	sort.Slice(labels, func(i, j int) bool { return labels[i] < labels[j] })

	hfDefs := make([]tieredHFIdef, 0, len(ts.hfIdxBuckets))
	for tok, bucket := range ts.hfIdxBuckets {
		hfDefs = append(hfDefs, tieredHFIdef{
			LabelToken:       tok,
			BucketSizeMillis: bucket.Milliseconds(),
		})
	}
	sort.Slice(hfDefs, func(i, j int) bool { return hfDefs[i].LabelToken < hfDefs[j].LabelToken })

	return saveTemporalIndexFile(ts.temporalIdxFile, temporalIndexFileData{
		TemporalLabels: labels,
		HighFrequency:  hfDefs,
	})
}
