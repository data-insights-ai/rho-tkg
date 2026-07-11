package core

import (
	"errors"
	"fmt"
	"time"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// --- Property indexes ---

// CreateProperty creates a property index on the given label and property key.
// Resolves or creates the label token. Returns storepkg.ErrIndexExists if the index already exists.
func (i *IndexOps) CreateProperty(label, propertyKey string) error {
	c := i.c
	if err := c.checkWritable(); err != nil {
		return err
	}
	return c.readUnderRLock(func() error {
		if err := c.validateIndexLabel(label); err != nil {
			return err
		}
		if err := c.validateIndexPropertyKey(propertyKey); err != nil {
			return err
		}
		cap, err := c.propertyIndexCap()
		if err != nil {
			return err
		}
		tok, labelSnapshot, allocatedLabel, err := c.getOrCreateLabelWithSnapshot(label)
		if err != nil {
			return err
		}
		labelFinished := false
		defer func() {
			if !labelFinished {
				_ = c.restoreNewLabelIndexOnError(labelSnapshot, allocatedLabel, label,
					fmt.Errorf("panic during property index create"),
					func() error { return cap.DropPropertyIndex(tok, propertyKey) },
					storepkg.ErrIndexNotFound,
					storepkg.ErrIndexExists,
				)
			}
		}()
		err = c.restoreNewLabelIndexOnError(labelSnapshot, allocatedLabel, label,
			cap.CreatePropertyIndex(tok, propertyKey),
			func() error { return cap.DropPropertyIndex(tok, propertyKey) },
			storepkg.ErrIndexNotFound,
			storepkg.ErrIndexExists,
		)
		labelFinished = true
		return err
	})
}

// DropProperty removes a property index.
// Resolves the label name to a token. Returns storepkg.ErrIndexNotFound if the index does not exist.
func (i *IndexOps) DeleteProperty(label, propertyKey string) error {
	c := i.c
	if err := c.checkWritable(); err != nil {
		return err
	}
	return c.readUnderRLock(func() error {
		if err := c.validateIndexLabel(label); err != nil {
			return err
		}
		if err := c.validateIndexPropertyKey(propertyKey); err != nil {
			return err
		}
		tok, ok := c.labels.Lookup(label)
		if !ok {
			return storepkg.ErrIndexNotFound
		}
		cap, err := c.propertyIndexCap()
		if err != nil {
			return err
		}
		return cap.DropPropertyIndex(tok, propertyKey)
	})
}

// CreateComposite creates a composite property index over the declared,
// ORDER-PRESERVING keys (2..4) under one label — EQUALITY-only in v1, see
// docs/query-planners.md "Composite property indexes" for planner guidance
// on when this beats a single-key index + post-filter. Resolves or creates
// the label token. Returns storepkg.ErrIndexExists if a composite index for
// the exact same (label, ordered key list) already exists — a different key
// ORDER for the same key SET is a distinct definition.
func (i *IndexOps) CreateComposite(label string, keys []string) error {
	c := i.c
	if err := c.checkWritable(); err != nil {
		return err
	}
	return c.readUnderRLock(func() error {
		if err := c.validateIndexLabel(label); err != nil {
			return err
		}
		if err := storepkg.ValidateCompositeIndexKeys(keys); err != nil {
			return err
		}
		for _, k := range keys {
			if err := c.validateIndexPropertyKey(k); err != nil {
				return err
			}
		}
		cap, err := c.compositeIndexCap()
		if err != nil {
			return err
		}
		tok, labelSnapshot, allocatedLabel, err := c.getOrCreateLabelWithSnapshot(label)
		if err != nil {
			return err
		}
		labelFinished := false
		defer func() {
			if !labelFinished {
				_ = c.restoreNewLabelIndexOnError(labelSnapshot, allocatedLabel, label,
					fmt.Errorf("panic during composite index create"),
					func() error { return cap.DropCompositePropertyIndex(tok, keys) },
					storepkg.ErrIndexNotFound,
					storepkg.ErrIndexExists,
				)
			}
		}()
		err = c.restoreNewLabelIndexOnError(labelSnapshot, allocatedLabel, label,
			cap.CreateCompositePropertyIndex(tok, keys),
			func() error { return cap.DropCompositePropertyIndex(tok, keys) },
			storepkg.ErrIndexNotFound,
			storepkg.ErrIndexExists,
		)
		labelFinished = true
		return err
	})
}

// DeleteComposite removes a composite property index declared over the
// exact ordered keys. Returns storepkg.ErrIndexNotFound if no such
// definition exists.
func (i *IndexOps) DeleteComposite(label string, keys []string) error {
	c := i.c
	if err := c.checkWritable(); err != nil {
		return err
	}
	return c.readUnderRLock(func() error {
		if err := c.validateIndexLabel(label); err != nil {
			return err
		}
		if err := storepkg.ValidateCompositeIndexKeys(keys); err != nil {
			return err
		}
		tok, ok := c.labels.Lookup(label)
		if !ok {
			return storepkg.ErrIndexNotFound
		}
		cap, err := c.compositeIndexCap()
		if err != nil {
			return err
		}
		return cap.DropCompositePropertyIndex(tok, keys)
	})
}

// CreateTemporal creates a temporal index on nodes with the given label.
// Accelerates temporal queries (ValidAt/interval filter) for that label.
// Returns storepkg.ErrTemporalIndexExists if the index already exists.
// Resolves or creates the label token so the index applies to future matching nodes.
func (i *IndexOps) CreateTemporal(label string) error {
	c := i.c
	if err := c.checkWritable(); err != nil {
		return err
	}
	return c.readUnderRLock(func() error {
		if err := c.validateIndexLabel(label); err != nil {
			return err
		}
		cap, err := c.temporalIndexCap()
		if err != nil {
			return err
		}
		tok, labelSnapshot, allocatedLabel, err := c.getOrCreateLabelWithSnapshot(label)
		if err != nil {
			return err
		}
		labelFinished := false
		defer func() {
			if !labelFinished {
				_ = c.restoreNewLabelIndexOnError(labelSnapshot, allocatedLabel, label,
					fmt.Errorf("panic during temporal index create"),
					func() error { return cap.DropTemporalIndex(tok) },
					storepkg.ErrTemporalIndexNotFound,
					storepkg.ErrTemporalIndexExists,
				)
			}
		}()
		err = c.restoreNewLabelIndexOnError(labelSnapshot, allocatedLabel, label,
			cap.CreateTemporalIndex(tok),
			func() error { return cap.DropTemporalIndex(tok) },
			storepkg.ErrTemporalIndexNotFound,
			storepkg.ErrTemporalIndexExists,
		)
		labelFinished = true
		return err
	})
}

// DropTemporal removes a temporal index for the given label.
// Returns storepkg.ErrTemporalIndexNotFound if the index does not exist.
func (i *IndexOps) DeleteTemporal(label string) error {
	c := i.c
	if err := c.checkWritable(); err != nil {
		return err
	}
	return c.readUnderRLock(func() error {
		if err := c.validateIndexLabel(label); err != nil {
			return err
		}
		tok, ok := c.labels.Lookup(label)
		if !ok {
			return storepkg.ErrTemporalIndexNotFound
		}
		cap, err := c.temporalIndexCap()
		if err != nil {
			return err
		}
		return cap.DropTemporalIndex(tok)
	})
}

// --- High-frequency indexes ---

// CreateHighFrequency creates a time-bucketed high-frequency temporal index
// on nodes with the given label. The bucketSize parameter controls the time
// width of each bucket (e.g., time.Hour).
// Designed for high-write-rate scenarios (thousands of event writes/sec).
// Only one temporal index type (temporal or high-frequency) can exist per label.
// Resolves or creates the label token so the index applies to future matching nodes.
// Returns storepkg.ErrInvalidTemporalIndexConfig if bucketSize is not a
// positive whole millisecond.
// Returns storepkg.ErrTemporalIndexExists if any temporal index already exists for this label.
// In-memory indexes are rebuilt from current store state when this method runs.
func (i *IndexOps) CreateHighFrequency(label string, bucketSize time.Duration) error {
	c := i.c
	if err := c.checkWritable(); err != nil {
		return err
	}
	return c.readUnderRLock(func() error {
		if err := c.validateIndexLabel(label); err != nil {
			return err
		}
		if err := storepkg.ValidateHighFrequencyBucketSize(bucketSize); err != nil {
			return err
		}
		cap, err := c.highFrequencyIndexCap()
		if err != nil {
			return err
		}
		tok, labelSnapshot, allocatedLabel, err := c.getOrCreateLabelWithSnapshot(label)
		if err != nil {
			return err
		}
		labelFinished := false
		defer func() {
			if !labelFinished {
				_ = c.restoreNewLabelIndexOnError(labelSnapshot, allocatedLabel, label,
					fmt.Errorf("panic during high-frequency index create"),
					func() error { return cap.DropHighFrequencyIndex(tok) },
					storepkg.ErrTemporalIndexNotFound,
					storepkg.ErrTemporalIndexExists,
				)
			}
		}()
		err = c.restoreNewLabelIndexOnError(labelSnapshot, allocatedLabel, label,
			cap.CreateHighFrequencyIndex(tok, bucketSize),
			func() error { return cap.DropHighFrequencyIndex(tok) },
			storepkg.ErrTemporalIndexNotFound,
			storepkg.ErrTemporalIndexExists,
		)
		labelFinished = true
		return err
	})
}

// DropHighFrequency removes the high-frequency temporal index for the given label.
// Returns storepkg.ErrTemporalIndexNotFound if no high-frequency index exists.
func (i *IndexOps) DeleteHighFrequency(label string) error {
	c := i.c
	if err := c.checkWritable(); err != nil {
		return err
	}
	return c.readUnderRLock(func() error {
		if err := c.validateIndexLabel(label); err != nil {
			return err
		}
		tok, ok := c.labels.Lookup(label)
		if !ok {
			return storepkg.ErrTemporalIndexNotFound
		}
		cap, err := c.highFrequencyIndexCap()
		if err != nil {
			return err
		}
		return cap.DropHighFrequencyIndex(tok)
	})
}

// --- Vector indexes ---

// CreateVector creates a vector similarity index on the given label and property key.
// dims is the expected vector dimension. metric selects the distance function.
// The index defaults to the approximate HNSW engine (see
// CLAUDE.md "Vector Indexes"); use CreateVectorWithOptions for the
// brute-force escape hatch or HNSW tuning.
// Resolves or creates the label token so the index applies to future matching nodes.
// Returns ErrVectorIndexExists if the index already exists.
func (i *IndexOps) CreateVector(label, propertyKey string, dims int, metric storepkg.DistanceMetric) error {
	return i.CreateVectorWithOptions(label, propertyKey, dims, metric, storepkg.VectorIndexOptions{})
}

// CreateVectorWithOptions is CreateVector with additional control over the
// search engine (opts.UseBruteForce) and HNSW tuning (opts.M /
// EfConstruction / EfSearch). A zero-value opts is identical to
// CreateVector (documented HNSW defaults). A backend that does not
// implement storepkg.VectorIndexOptionsCapability falls back to
// CreateVectorIndex (opts silently unavailable — the same "optional
// acceleration" contract as FilteredVectorSearchCapability); every in-tree
// backend (memory/badger/tiered) implements it.
func (i *IndexOps) CreateVectorWithOptions(label, propertyKey string, dims int, metric storepkg.DistanceMetric, opts storepkg.VectorIndexOptions) error {
	c := i.c
	if err := c.checkWritable(); err != nil {
		return err
	}
	return c.readUnderRLock(func() error {
		if err := c.validateIndexLabel(label); err != nil {
			return err
		}
		if err := c.validateIndexPropertyKey(propertyKey); err != nil {
			return err
		}
		if err := indexpkg.ValidateVectorIndexConfig(dims, metric); err != nil {
			return err
		}
		if err := indexpkg.ValidateVectorIndexOptions(opts); err != nil {
			return err
		}
		cap, err := c.vectorIndexCap()
		if err != nil {
			return err
		}
		tok, labelSnapshot, allocatedLabel, err := c.getOrCreateLabelWithSnapshot(label)
		if err != nil {
			return err
		}
		labelFinished := false
		defer func() {
			if !labelFinished {
				_ = c.restoreNewLabelIndexOnError(labelSnapshot, allocatedLabel, label,
					fmt.Errorf("panic during vector index create"),
					func() error { return cap.DropVectorIndex(tok, propertyKey) },
					storepkg.ErrVectorIndexNotFound,
					storepkg.ErrVectorIndexExists,
				)
			}
		}()
		var createErr error
		if c.vectorIndexOptions != nil {
			createErr = c.vectorIndexOptions.CreateVectorIndexWithOptions(tok, propertyKey, dims, metric, opts)
		} else {
			createErr = cap.CreateVectorIndex(tok, propertyKey, dims, metric)
		}
		err = c.restoreNewLabelIndexOnError(labelSnapshot, allocatedLabel, label,
			createErr,
			func() error { return cap.DropVectorIndex(tok, propertyKey) },
			storepkg.ErrVectorIndexNotFound,
			storepkg.ErrVectorIndexExists,
		)
		labelFinished = true
		return err
	})
}

// DropVector removes a vector index.
// Returns ErrVectorIndexNotFound if the index does not exist.
func (i *IndexOps) DeleteVector(label, propertyKey string) error {
	c := i.c
	if err := c.checkWritable(); err != nil {
		return err
	}
	return c.readUnderRLock(func() error {
		if err := c.validateIndexLabel(label); err != nil {
			return err
		}
		if err := c.validateIndexPropertyKey(propertyKey); err != nil {
			return err
		}
		tok, ok := c.labels.Lookup(label)
		if !ok {
			return storepkg.ErrVectorIndexNotFound
		}
		cap, err := c.vectorIndexCap()
		if err != nil {
			return err
		}
		return cap.DropVectorIndex(tok, propertyKey)
	})
}

func (c *Core) restoreNewLabelIndexOnError(snapshot []string, allocated bool, label string, err error, cleanup func() error, notFound, exists error) error {
	if err != nil && cleanup != nil {
		if !allocated && exists != nil && errors.Is(err, exists) {
			return c.restoreNewLabelOnError(snapshot, allocated, label, err)
		}
		if cleanupErr := runRollbackCleanup(cleanup); cleanupErr != nil && (notFound == nil || !errors.Is(cleanupErr, notFound)) {
			err = fmt.Errorf("%w; additionally failed to remove partial index for rolled-back label %q: %v", err, label, cleanupErr)
			if allocated {
				if persistErr := c.persistRegistries(); persistErr != nil {
					err = fmt.Errorf("%w; additionally failed to persist retained label registry after partial index cleanup failure: %v", err, persistErr)
				}
				c.registryMu.Unlock()
				return err
			}
		}
	}
	if err == nil && allocated {
		if persistErr := c.persistRegistries(); persistErr != nil {
			err = persistErr
			if cleanup != nil {
				if cleanupErr := runRollbackCleanup(cleanup); cleanupErr != nil && (notFound == nil || !errors.Is(cleanupErr, notFound)) {
					err = fmt.Errorf("%w; additionally failed to remove partial index for rolled-back label %q: %v", err, label, cleanupErr)
					if persistRetainedErr := c.persistRegistries(); persistRetainedErr != nil {
						err = fmt.Errorf("%w; additionally failed to persist retained label registry after partial index cleanup failure: %v", err, persistRetainedErr)
					}
					c.registryMu.Unlock()
					return err
				}
			}
			return c.restoreNewLabelOnError(snapshot, allocated, label, err)
		}
		c.registryMu.Unlock()
		return nil
	}
	return c.restoreNewLabelOnError(snapshot, allocated, label, err)
}

func runRollbackCleanup(cleanup func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
	}()
	return cleanup()
}

func (c *Core) validateIndexLabel(label string) error {
	return c.validateIndexName(label)
}

func (c *Core) validateIndexName(name string) error {
	return c.validateName(name)
}

func (c *Core) validateIndexPropertyKey(propertyKey string) error {
	if err := storepkg.ValidateIndexPropertyKey(propertyKey); err != nil {
		return err
	}
	if len(propertyKey) > c.validation.MaxPropertyKeyLength {
		return fmt.Errorf("%w: %q (%d > %d)", ErrKeyTooLong, propertyKey, len(propertyKey), c.validation.MaxPropertyKeyLength)
	}
	return nil
}
