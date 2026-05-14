package core

import (
	"errors"
	"fmt"
	"time"

	indexpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/internal/index"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/store"
)

// --- Property indexes ---

// CreateProperty creates a property index on the given label and property key.
// Resolves or creates the label token. Returns storepkg.ErrIndexExists if the index already exists.
func (i *IndexOps) CreateProperty(label, propertyKey string) error {
	c := i.c
	if err := c.checkOpen(); err != nil {
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
	if err := c.checkOpen(); err != nil {
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

// CreateTemporal creates a temporal index on nodes with the given label.
// Accelerates temporal queries (ValidAt/interval filter) for that label.
// Returns storepkg.ErrTemporalIndexExists if the index already exists.
// Resolves or creates the label token so the index applies to future matching nodes.
func (i *IndexOps) CreateTemporal(label string) error {
	c := i.c
	if err := c.checkOpen(); err != nil {
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
	if err := c.checkOpen(); err != nil {
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
	if err := c.checkOpen(); err != nil {
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
	if err := c.checkOpen(); err != nil {
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
// Resolves or creates the label token so the index applies to future matching nodes.
// Returns ErrVectorIndexExists if the index already exists.
func (i *IndexOps) CreateVector(label, propertyKey string, dims int, metric storepkg.DistanceMetric) error {
	c := i.c
	if err := c.checkOpen(); err != nil {
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
		err = c.restoreNewLabelIndexOnError(labelSnapshot, allocatedLabel, label,
			cap.CreateVectorIndex(tok, propertyKey, dims, metric),
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
	if err := c.checkOpen(); err != nil {
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
