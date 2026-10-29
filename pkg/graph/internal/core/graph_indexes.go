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
	return i.CreatePropertyWithOptions(label, propertyKey, storepkg.PropertyIndexOptions{})
}

// CreatePropertyWithOptions is CreateProperty with storepkg.PropertyIndexOptions.
// Zero options equal CreateProperty. Non-zero options need
// storepkg.PropertyIndexOptionsCapability, else ErrCapabilityNotSupported before
// anything is created. Elements is refused with ErrElementsIndexUnsupported
// together with RangeCounts or on a key under a unique constraint (docs/api.md
// "Indexes").
func (i *IndexOps) CreatePropertyWithOptions(label, propertyKey string, opts storepkg.PropertyIndexOptions) error {
	c := i.c
	if err := c.checkWritable(); err != nil {
		return err
	}
	// Value statistics at pins are cached per (type, key) and answer
	// only while the index serves them: index DDL drops them (R9-3).
	defer c.asOfColumns.bump()
	if err := storepkg.ValidatePropertyIndexOptions(opts); err != nil {
		return err
	}
	return c.readUnderRLock(func() error {
		if err := c.validateIndexLabel(label); err != nil {
			return err
		}
		if err := c.validateIndexPropertyKey(propertyKey); err != nil {
			return err
		}
		if opts.Elements {
			if err := c.refuseUniqueKey(label, propertyKey); err != nil {
				return err
			}
		}
		cap, err := c.propertyIndexCap()
		if err != nil {
			return err
		}
		create := func(tok uint16) error { return cap.CreatePropertyIndex(tok, propertyKey) }
		if opts != (storepkg.PropertyIndexOptions{}) {
			oc, ok := c.store.(storepkg.PropertyIndexOptionsCapability)
			if !ok {
				return fmt.Errorf("%w: PropertyIndexOptionsCapability", storepkg.ErrCapabilityNotSupported)
			}
			create = func(tok uint16) error { return oc.CreatePropertyIndexWithOptions(tok, propertyKey, opts) }
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
					func() error { return c.indexDDL(cap.DropPropertyIndex(tok, propertyKey)) },
					storepkg.ErrIndexNotFound,
					storepkg.ErrIndexExists,
				)
			}
		}()
		err = c.restoreNewLabelIndexOnError(labelSnapshot, allocatedLabel, label,
			c.indexDDL(create(tok)),
			func() error { return c.indexDDL(cap.DropPropertyIndex(tok, propertyKey)) },
			storepkg.ErrIndexNotFound,
			storepkg.ErrIndexExists,
		)
		labelFinished = true
		return err
	})
}

// DeleteProperty removes a property index.
// Resolves the label name to a token. Returns storepkg.ErrIndexNotFound if the index does not exist.
func (i *IndexOps) DeleteProperty(label, propertyKey string) error {
	c := i.c
	if err := c.checkWritable(); err != nil {
		return err
	}
	// Value statistics at pins are cached per (type, key) and answer
	// only while the index serves them: index DDL drops them (R9-3).
	defer c.asOfColumns.bump()
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
		return c.indexDDL(cap.DropPropertyIndex(tok, propertyKey))
	})
}

// CreateComposite creates a composite property index over 2..4 declared,
// ORDER-PRESERVING keys under one label (EQUALITY-only; docs/query-planners.md
// "Composite property indexes"). Returns storepkg.ErrIndexExists for the exact
// same (label, ordered keys); another order of the same set is a distinct definition.
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
		// Equality on every key would give an Elements-indexed key a second meaning (backlog 63).
		if tok, ok := c.labels.Lookup(label); ok {
			for _, k := range keys {
				if err := c.refuseNodeElementsIndex(tok, k, "composite index"); err != nil {
					return err
				}
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
					func() error { return c.indexDDL(cap.DropCompositePropertyIndex(tok, keys)) },
					storepkg.ErrIndexNotFound,
					storepkg.ErrIndexExists,
				)
			}
		}()
		err = c.restoreNewLabelIndexOnError(labelSnapshot, allocatedLabel, label,
			c.indexDDL(cap.CreateCompositePropertyIndex(tok, keys)),
			func() error { return c.indexDDL(cap.DropCompositePropertyIndex(tok, keys)) },
			storepkg.ErrIndexNotFound,
			storepkg.ErrIndexExists,
		)
		labelFinished = true
		return err
	})
}

// DeleteComposite removes the composite index declared over exactly these ordered
// keys; storepkg.ErrIndexNotFound if none.
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
		return c.indexDDL(cap.DropCompositePropertyIndex(tok, keys))
	})
}

// ListComposites returns the declared, ORDER-PRESERVING key tuple of every
// composite index under label (distinct orderings are listed separately), as
// caller-owned copies; unregistered labels give an empty slice. Cheap per query
// plan; callers that cache revalidate with InventoryEpoch. Backends without
// introspection return storepkg.ErrCapabilityNotSupported.
func (i *IndexOps) ListComposites(label string) ([][]string, error) {
	c := i.c
	var out [][]string
	err := c.readUnderRLock(func() error {
		if err := c.validateIndexLabel(label); err != nil {
			return err
		}
		cap, ok := c.store.(storepkg.CompositeIndexIntrospectionCapability)
		if !ok {
			return fmt.Errorf("%w: CompositeIndexIntrospectionCapability", storepkg.ErrCapabilityNotSupported)
		}
		tok, found := c.labels.Lookup(label)
		if !found {
			out = [][]string{}
			return nil
		}
		defs, err := cap.ListCompositePropertyIndexes(tok)
		if err != nil {
			return err
		}
		out = defs
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// HasComposite reports whether a composite index on label has a key SET equal to
// keys (ORDER-INSENSITIVE, duplicates ignored) - the match rule
// NodesByLabelAndProperties uses for index-vs-label-scan. Unregistered labels
// return false; backends without introspection return
// storepkg.ErrCapabilityNotSupported.
func (i *IndexOps) HasComposite(label string, keys []string) (bool, error) {
	// Laxer than ValidateCompositeIndexKeys: a planner may probe any key set; sizes no
	// definition can have answer false. Individual keys still validate.
	if len(keys) == 0 {
		return false, fmt.Errorf("%w: HasComposite requires at least one key", storepkg.ErrInvalidStoreMutation)
	}
	want := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		if err := storepkg.ValidateIndexPropertyKey(k); err != nil {
			return false, err
		}
		want[k] = struct{}{}
	}
	defs, err := i.ListComposites(label)
	if err != nil {
		return false, err
	}
	for _, def := range defs {
		if len(def) != len(want) {
			continue
		}
		match := true
		for _, k := range def {
			if _, ok := want[k]; !ok {
				match = false
				break
			}
		}
		if match {
			return true, nil
		}
	}
	return false, nil
}

// HasProperty reports whether a property index exists on (label, propertyKey)
// (backlog 21b); the single-key mirror of HasComposite. Unregistered labels
// return false; backends without introspection return
// storepkg.ErrCapabilityNotSupported.
func (i *IndexOps) HasProperty(label, propertyKey string) (bool, error) {
	c := i.c
	var out bool
	err := c.readUnderRLock(func() error {
		if err := c.validateIndexLabel(label); err != nil {
			return err
		}
		if err := c.validateIndexPropertyKey(propertyKey); err != nil {
			return err
		}
		cap, ok := c.store.(storepkg.PropertyIndexIntrospectionCapability)
		if !ok {
			return fmt.Errorf("%w: PropertyIndexIntrospectionCapability", storepkg.ErrCapabilityNotSupported)
		}
		tok, found := c.labels.Lookup(label)
		if !found {
			out = false
			return nil
		}
		has, err := cap.HasPropertyIndex(tok, propertyKey)
		if err != nil {
			return err
		}
		out = has
		return nil
	})
	if err != nil {
		return false, err
	}
	return out, nil
}

// PropertyIndexStats reports what each declared property index holds in RAM
// (IDXSTATS): one row per definition (per open shard on tiered), its label or
// relationship type named; storepkg.PropertyIndexStatsByScope sums them per
// label or type. A diagnostic: the store walks every indexed value under its
// index read lock, so writes wait for it. Backends without the capability
// return storepkg.ErrCapabilityNotSupported.
func (i *IndexOps) PropertyIndexStats() ([]storepkg.PropertyIndexStat, error) {
	c := i.c
	var out []storepkg.PropertyIndexStat
	err := c.readUnderRLock(func() error {
		cap, ok := c.store.(storepkg.PropertyIndexStatsCapability)
		if !ok {
			return fmt.Errorf("%w: PropertyIndexStatsCapability", storepkg.ErrCapabilityNotSupported)
		}
		stats, err := cap.PropertyIndexStats()
		if err != nil {
			return err
		}
		for j := range stats {
			if stats[j].RelTypeToken != 0 {
				stats[j].RelType = c.relTypes.Resolve(stats[j].RelTypeToken)
			} else {
				stats[j].Label = c.labels.Resolve(stats[j].LabelToken)
			}
		}
		out = stats
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// PropertyIndexValidEndStats counts the store's valid-end pruning since it
// opened (VALIDENDPRUNE): entries a QueryOpts.ValidEndAfter lookup left out,
// entries it read without a bound, bounds reads wrote, refines skipped.
// Backends without it return storepkg.ErrCapabilityNotSupported.
func (i *IndexOps) PropertyIndexValidEndStats() (storepkg.PropertyIndexValidEndStats, error) {
	c := i.c
	var out storepkg.PropertyIndexValidEndStats
	err := c.readUnderRLock(func() error {
		if c.validEndQuery == nil {
			return fmt.Errorf("%w: PropertyIndexValidEndCapability", storepkg.ErrCapabilityNotSupported)
		}
		out = c.validEndQuery.PropertyIndexValidEndStats()
		return nil
	})
	return out, err
}

// HasTemporal reports whether a temporal interval index exists on label (backlog
// 21b); a high-frequency index does not count. Unregistered labels return false;
// backends without introspection return storepkg.ErrCapabilityNotSupported.
func (i *IndexOps) HasTemporal(label string) (bool, error) {
	c := i.c
	var out bool
	err := c.readUnderRLock(func() error {
		if err := c.validateIndexLabel(label); err != nil {
			return err
		}
		cap, ok := c.store.(storepkg.TemporalIndexIntrospectionCapability)
		if !ok {
			return fmt.Errorf("%w: TemporalIndexIntrospectionCapability", storepkg.ErrCapabilityNotSupported)
		}
		tok, found := c.labels.Lookup(label)
		if !found {
			out = false
			return nil
		}
		has, err := cap.HasTemporalIndex(tok)
		if err != nil {
			return err
		}
		out = has
		return nil
	})
	if err != nil {
		return false, err
	}
	return out, nil
}

// VectorIndexInfo returns the declared configuration (dims, metric, engine, HNSW
// tuning) of the vector index on (label, propertyKey), or (zero, false) if none
// (backlog 21b), so a planner can validate a query vector before SearchNearest.
// Backends without introspection return storepkg.ErrCapabilityNotSupported.
func (i *IndexOps) VectorIndexInfo(label, propertyKey string) (storepkg.VectorIndexInfo, bool, error) {
	c := i.c
	var (
		out   storepkg.VectorIndexInfo
		found bool
	)
	err := c.readUnderRLock(func() error {
		if err := c.validateIndexLabel(label); err != nil {
			return err
		}
		if err := c.validateIndexPropertyKey(propertyKey); err != nil {
			return err
		}
		cap, ok := c.store.(storepkg.VectorIndexIntrospectionCapability)
		if !ok {
			return fmt.Errorf("%w: VectorIndexIntrospectionCapability", storepkg.ErrCapabilityNotSupported)
		}
		tok, ok := c.labels.Lookup(label)
		if !ok {
			return nil
		}
		info, has, err := cap.VectorIndexInfo(tok, propertyKey)
		if err != nil {
			return err
		}
		out, found = info, has
		return nil
	})
	if err != nil {
		return storepkg.VectorIndexInfo{}, false, err
	}
	return out, found, nil
}

// HasRelProperty reports whether a relationship property index exists on
// (typeName, propertyKey) (backlog 21b); the rel-side mirror of HasProperty.
// Backends without introspection return storepkg.ErrCapabilityNotSupported.
func (i *IndexOps) HasRelProperty(typeName, propertyKey string) (bool, error) {
	c := i.c
	var out bool
	err := c.readUnderRLock(func() error {
		if err := c.validateIndexName(typeName); err != nil {
			return err
		}
		if err := c.validateIndexPropertyKey(propertyKey); err != nil {
			return err
		}
		cap, ok := c.store.(storepkg.RelPropertyIndexIntrospectionCapability)
		if !ok {
			return fmt.Errorf("%w: RelPropertyIndexIntrospectionCapability", storepkg.ErrCapabilityNotSupported)
		}
		tok, found := c.relTypes.Lookup(typeName)
		if !found {
			out = false
			return nil
		}
		has, err := cap.HasRelPropertyIndex(tok, propertyKey)
		if err != nil {
			return err
		}
		out = has
		return nil
	})
	if err != nil {
		return false, err
	}
	return out, nil
}

// CreateTemporal creates a temporal index on nodes with the given label,
// resolving or creating the label token. Returns storepkg.ErrTemporalIndexExists
// if it already exists.
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
					func() error { return c.indexDDL(cap.DropTemporalIndex(tok)) },
					storepkg.ErrTemporalIndexNotFound,
					storepkg.ErrTemporalIndexExists,
				)
			}
		}()
		err = c.restoreNewLabelIndexOnError(labelSnapshot, allocatedLabel, label,
			c.indexDDL(cap.CreateTemporalIndex(tok)),
			func() error { return c.indexDDL(cap.DropTemporalIndex(tok)) },
			storepkg.ErrTemporalIndexNotFound,
			storepkg.ErrTemporalIndexExists,
		)
		labelFinished = true
		return err
	})
}

// DeleteTemporal removes a temporal index for the given label.
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
		return c.indexDDL(cap.DropTemporalIndex(tok))
	})
}

// --- High-frequency indexes ---

// CreateHighFrequency creates a time-bucketed temporal index on label for
// high-write-rate events; bucketSize is the bucket width (e.g. time.Hour). One
// temporal index type (temporal or high-frequency) per label. Resolves or creates
// the label token and rebuilds in-memory indexes from current state. Returns
// storepkg.ErrInvalidTemporalIndexConfig if bucketSize is not a positive whole
// millisecond, storepkg.ErrTemporalIndexExists if any temporal index exists.
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
					func() error { return c.indexDDL(cap.DropHighFrequencyIndex(tok)) },
					storepkg.ErrTemporalIndexNotFound,
					storepkg.ErrTemporalIndexExists,
				)
			}
		}()
		err = c.restoreNewLabelIndexOnError(labelSnapshot, allocatedLabel, label,
			c.indexDDL(cap.CreateHighFrequencyIndex(tok, bucketSize)),
			func() error { return c.indexDDL(cap.DropHighFrequencyIndex(tok)) },
			storepkg.ErrTemporalIndexNotFound,
			storepkg.ErrTemporalIndexExists,
		)
		labelFinished = true
		return err
	})
}

// DeleteHighFrequency removes the high-frequency temporal index for the given label.
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
		return c.indexDDL(cap.DropHighFrequencyIndex(tok))
	})
}

// --- Vector indexes ---

// CreateVector creates a vector index on (label, propertyKey) with dims and
// metric, defaulting to the HNSW engine (docs/architecture.md "Vector
// indexes"); see CreateVectorWithOptions for brute force or tuning. Resolves or
// creates the label token. Returns ErrVectorIndexExists if it exists.
func (i *IndexOps) CreateVector(label, propertyKey string, dims int, metric storepkg.DistanceMetric) error {
	return i.CreateVectorWithOptions(label, propertyKey, dims, metric, storepkg.VectorIndexOptions{})
}

// CreateVectorWithOptions is CreateVector with the engine (opts.UseBruteForce)
// and HNSW tuning (opts.M / EfConstruction / EfSearch); zero opts equal
// CreateVector. A backend without storepkg.VectorIndexOptionsCapability falls
// back to CreateVectorIndex and drops opts; in-tree backends implement it.
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
					func() error { return c.indexDDL(cap.DropVectorIndex(tok, propertyKey)) },
					storepkg.ErrVectorIndexNotFound,
					storepkg.ErrVectorIndexExists,
				)
			}
		}()
		var createErr error
		if c.vectorIndexOptions != nil {
			createErr = c.indexDDL(c.vectorIndexOptions.CreateVectorIndexWithOptions(tok, propertyKey, dims, metric, opts))
		} else {
			createErr = c.indexDDL(cap.CreateVectorIndex(tok, propertyKey, dims, metric))
		}
		err = c.restoreNewLabelIndexOnError(labelSnapshot, allocatedLabel, label,
			createErr,
			func() error { return c.indexDDL(cap.DropVectorIndex(tok, propertyKey)) },
			storepkg.ErrVectorIndexNotFound,
			storepkg.ErrVectorIndexExists,
		)
		labelFinished = true
		return err
	})
}

// DeleteVector removes a vector index.
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
		return c.indexDDL(cap.DropVectorIndex(tok, propertyKey))
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
