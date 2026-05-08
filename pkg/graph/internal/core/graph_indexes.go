package core

import (
	"time"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
)

// --- Property indexes ---

// CreateProperty creates a property index on the given label and property key.
// Resolves the label name to a token. Returns storepkg.ErrIndexExists if the index already exists.
// Returns nil if the label has never been registered (nothing to index).
func (i *IndexOps) CreateProperty(label, propertyKey string) error {
	c := i.c
	tok, ok := c.labels.Lookup(label)
	if !ok {
		return nil
	}
	cap, err := c.propertyIndexCap()
	if err != nil {
		return err
	}
	return cap.CreatePropertyIndex(tok, propertyKey)
}

// DropProperty removes a property index.
// Resolves the label name to a token. Returns storepkg.ErrIndexNotFound if the index does not exist.
// Returns nil if the label has never been registered.
func (i *IndexOps) DropProperty(label, propertyKey string) error {
	c := i.c
	tok, ok := c.labels.Lookup(label)
	if !ok {
		return nil
	}
	cap, err := c.propertyIndexCap()
	if err != nil {
		return err
	}
	return cap.DropPropertyIndex(tok, propertyKey)
}

// CreateTemporal creates a temporal index on nodes with the given label.
// Accelerates temporal queries (ValidAt/interval filter) for that label.
// Returns storepkg.ErrTemporalIndexExists if the index already exists.
// Returns nil if the label has never been registered.
func (i *IndexOps) CreateTemporal(label string) error {
	c := i.c
	tok, ok := c.labels.Lookup(label)
	if !ok {
		return nil
	}
	cap, err := c.temporalIndexCap()
	if err != nil {
		return err
	}
	return cap.CreateTemporalIndex(tok)
}

// DropTemporal removes a temporal index for the given label.
// Returns storepkg.ErrTemporalIndexNotFound if the index does not exist.
// Returns nil if the label has never been registered.
func (i *IndexOps) DropTemporal(label string) error {
	c := i.c
	tok, ok := c.labels.Lookup(label)
	if !ok {
		return nil
	}
	cap, err := c.temporalIndexCap()
	if err != nil {
		return err
	}
	return cap.DropTemporalIndex(tok)
}

// --- High-frequency indexes ---

// CreateHighFrequency creates a time-bucketed high-frequency temporal index
// on nodes with the given label. The bucketSize parameter controls the time
// width of each bucket (e.g., time.Hour).
// Designed for high-write-rate scenarios (thousands of event writes/sec).
// Only one temporal index type (temporal or high-frequency) can exist per label.
// Returns nil if the label has never been registered.
// Returns storepkg.ErrTemporalIndexExists if any temporal index already exists for this label.
// Not persisted: the index must be rebuilt via CreateHighFrequency after restart.
func (i *IndexOps) CreateHighFrequency(label string, bucketSize time.Duration) error {
	c := i.c
	tok, ok := c.labels.Lookup(label)
	if !ok {
		return nil
	}
	cap, err := c.highFrequencyIndexCap()
	if err != nil {
		return err
	}
	return cap.CreateHighFrequencyIndex(tok, bucketSize)
}

// DropHighFrequency removes the high-frequency temporal index for the given label.
// Returns nil if the label has never been registered.
// Returns storepkg.ErrTemporalIndexNotFound if no high-frequency index exists.
func (i *IndexOps) DropHighFrequency(label string) error {
	c := i.c
	tok, ok := c.labels.Lookup(label)
	if !ok {
		return nil
	}
	cap, err := c.highFrequencyIndexCap()
	if err != nil {
		return err
	}
	return cap.DropHighFrequencyIndex(tok)
}

// --- Vector indexes ---

// CreateVector creates a vector similarity index on the given label and property key.
// dims is the expected vector dimension. metric selects the distance function.
// Returns nil if the label has never been registered (no-op).
// Returns ErrVectorIndexExists if the index already exists.
func (i *IndexOps) CreateVector(label, propertyKey string, dims int, metric storepkg.DistanceMetric) error {
	c := i.c
	tok, ok := c.labels.Lookup(label)
	if !ok {
		return nil
	}
	cap, err := c.vectorIndexCap()
	if err != nil {
		return err
	}
	return cap.CreateVectorIndex(tok, propertyKey, dims, metric)
}

// DropVector removes a vector index.
// Returns nil if the label has never been registered.
// Returns ErrVectorIndexNotFound if the index does not exist.
func (i *IndexOps) DropVector(label, propertyKey string) error {
	c := i.c
	tok, ok := c.labels.Lookup(label)
	if !ok {
		return nil
	}
	cap, err := c.vectorIndexCap()
	if err != nil {
		return err
	}
	return cap.DropVectorIndex(tok, propertyKey)
}
