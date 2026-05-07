package graph

import (
	"time"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
)

// --- Property indexes ---

// CreatePropertyIndex creates a property index on the given label and property key.
// Resolves the label name to a token. Returns storepkg.ErrIndexExists if the index already exists.
// Returns nil if the label has never been registered (nothing to index).
func (g *Graph) CreatePropertyIndex(label, propertyKey string) error {
	tok, ok := g.labels.Lookup(label)
	if !ok {
		return nil
	}
	return g.store.CreatePropertyIndex(tok, propertyKey)
}

// DropPropertyIndex removes a property index.
// Resolves the label name to a token. Returns storepkg.ErrIndexNotFound if the index does not exist.
// Returns nil if the label has never been registered.
func (g *Graph) DropPropertyIndex(label, propertyKey string) error {
	tok, ok := g.labels.Lookup(label)
	if !ok {
		return nil
	}
	return g.store.DropPropertyIndex(tok, propertyKey)
}

// CreateTemporalIndex creates a temporal index on nodes with the given label.
// Accelerates temporal queries (ValidAt/interval filter) for that label.
// Returns storepkg.ErrTemporalIndexExists if the index already exists.
// Returns nil if the label has never been registered.
func (g *Graph) CreateTemporalIndex(label string) error {
	tok, ok := g.labels.Lookup(label)
	if !ok {
		return nil
	}
	return g.store.CreateTemporalIndex(tok)
}

// DropTemporalIndex removes a temporal index for the given label.
// Returns storepkg.ErrTemporalIndexNotFound if the index does not exist.
// Returns nil if the label has never been registered.
func (g *Graph) DropTemporalIndex(label string) error {
	tok, ok := g.labels.Lookup(label)
	if !ok {
		return nil
	}
	return g.store.DropTemporalIndex(tok)
}

// --- High-frequency indexes ---

// CreateHighFrequencyIndex creates a time-bucketed high-frequency temporal index
// on nodes with the given label. The bucketSize parameter controls the time
// width of each bucket (e.g., time.Hour).
// Designed for high-write-rate scenarios (thousands of event writes/sec).
// Only one temporal index type (temporal or high-frequency) can exist per label.
// Returns nil if the label has never been registered.
// Returns storepkg.ErrTemporalIndexExists if any temporal index already exists for this label.
// Not persisted: the index must be rebuilt via CreateHighFrequencyIndex after restart.
func (g *Graph) CreateHighFrequencyIndex(label string, bucketSize time.Duration) error {
	tok, ok := g.labels.Lookup(label)
	if !ok {
		return nil
	}
	return g.store.CreateHighFrequencyIndex(tok, bucketSize)
}

// DropHighFrequencyIndex removes the high-frequency temporal index for the given label.
// Returns nil if the label has never been registered.
// Returns storepkg.ErrTemporalIndexNotFound if no high-frequency index exists.
func (g *Graph) DropHighFrequencyIndex(label string) error {
	tok, ok := g.labels.Lookup(label)
	if !ok {
		return nil
	}
	return g.store.DropHighFrequencyIndex(tok)
}

// --- Vector indexes ---

// CreateVectorIndex creates a vector similarity index on the given label and property key.
// dims is the expected vector dimension. metric selects the distance function.
// Returns nil if the label has never been registered (no-op).
// Returns ErrVectorIndexExists if the index already exists.
func (g *Graph) CreateVectorIndex(label, propertyKey string, dims int, metric storepkg.DistanceMetric) error {
	tok, ok := g.labels.Lookup(label)
	if !ok {
		return nil
	}
	return g.store.CreateVectorIndex(tok, propertyKey, dims, metric)
}

// DropVectorIndex removes a vector index.
// Returns nil if the label has never been registered.
// Returns ErrVectorIndexNotFound if the index does not exist.
func (g *Graph) DropVectorIndex(label, propertyKey string) error {
	tok, ok := g.labels.Lookup(label)
	if !ok {
		return nil
	}
	return g.store.DropVectorIndex(tok, propertyKey)
}
