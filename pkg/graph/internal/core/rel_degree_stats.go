package core

import (
	"sync"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// relDegreeCache keeps RelTypeDegreeStats per relationship-type token (0 =
// every type) with the relationship mutation epoch they were counted at.
// The zero value is ready.
type relDegreeCache struct {
	mu      sync.Mutex
	entries map[uint16]relDegreeEntry
}

type relDegreeEntry struct {
	epoch uint64
	stats storepkg.RelTypeDegreeStats
}

func (d *relDegreeCache) get(tok uint16, epoch uint64) (storepkg.RelTypeDegreeStats, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	e, ok := d.entries[tok]
	if !ok || e.epoch != epoch {
		return storepkg.RelTypeDegreeStats{}, false
	}
	return e.stats, true
}

func (d *relDegreeCache) put(tok uint16, epoch uint64, stats storepkg.RelTypeDegreeStats) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.entries == nil {
		d.entries = make(map[uint16]relDegreeEntry)
	}
	d.entries[tok] = relDegreeEntry{epoch: epoch, stats: stats}
}

// relTypeEpochScanner is the per-type freshness stamp (badger stripes it per
// type; memory returns its store-wide relationship epoch).
type relTypeEpochScanner interface {
	RelMutationEpochForType(token uint16) uint64
}

// relDegreeEpoch returns the epoch RelTypeDegreeStats of tok are keyed on and
// whether the store has one: the per-type stamp of a native store, else the
// store-wide relationship epoch.
func (c *Core) relDegreeEpoch(tok uint16) (uint64, bool) {
	if tok != 0 && c.storeRowsTrust {
		if s, ok := c.store.(relTypeEpochScanner); ok {
			return s.RelMutationEpochForType(tok), true
		}
	}
	if s, ok := c.store.(relMutationEpochScanner); ok {
		return s.RelMutationEpoch(), true
	}
	return 0, false
}

// RelTypeDegreeStats returns the largest out- and in-degree of a
// relationship type's current relationships, with the counts of
// relationships and of distinct start and end nodes. typeName "" means every
// relationship. Derived, not maintained: the first call after any write to
// the type scans its relationships once (O(relationships of the type), one
// streaming pass, O(distinct endpoints) memory); the result is kept with the
// relationship mutation epoch (badger: the type's own stripe) and served in
// O(1) until that moves. An unregistered type returns zero stats with Exact
// true. Errors: a malformed type name, ErrGraphClosed.
func (s *StatOps) RelTypeDegreeStats(typeName string) (storepkg.RelTypeDegreeStats, error) {
	c := s.c
	var zero storepkg.RelTypeDegreeStats
	if typeName != "" {
		if err := c.validateRelTypeQueryName(typeName); err != nil {
			return zero, err
		}
	}
	var tok uint16
	known := true
	if err := c.readUnderRLock(func() error {
		if typeName != "" {
			tok, known = c.lookupRelTypeQueryToken(typeName)
		}
		return nil
	}); err != nil {
		return zero, err
	}
	if !known {
		return storepkg.RelTypeDegreeStats{Exact: true}, nil
	}
	const maxAttempts = 3
	var stats storepkg.RelTypeDegreeStats
	for attempt := 0; attempt < maxAttempts; attempt++ {
		before, epochOK := c.relDegreeEpoch(tok)
		if epochOK {
			if cached, ok := c.relDegrees.get(tok, before); ok {
				return cached, nil
			}
		}
		var err error
		stats, err = c.countRelDegrees(typeName, tok)
		if err != nil {
			return zero, err
		}
		if !epochOK {
			return stats, nil
		}
		if after, _ := c.relDegreeEpoch(tok); after == before {
			stats.Exact = true
			c.relDegrees.put(tok, before, stats)
			return stats, nil
		}
	}
	return stats, nil
}

// countRelDegrees asks a native store's adjacency index first, else streams
// the type's relationships (every relationship for "") through the graph's
// own scan doors, which take and release the graph lock themselves.
func (c *Core) countRelDegrees(typeName string, tok uint16) (storepkg.RelTypeDegreeStats, error) {
	if native, ok := c.store.(storepkg.RelTypeDegreeCapability); ok && c.storeRowsTrust {
		var (
			st     storepkg.RelTypeDegreeStats
			served bool
		)
		if err := c.readUnderRLock(func() error {
			var err error
			st, served, err = native.RelTypeDegreeStats(tok)
			return err
		}); err != nil {
			return storepkg.RelTypeDegreeStats{}, err
		}
		if served {
			return st, nil
		}
	}
	out := make(map[types.NodeID]int64)
	in := make(map[types.NodeID]int64)
	var rels int64
	count := func(r *types.Relationship) bool {
		rels++
		out[r.StartNodeID()]++
		in[r.EndNodeID()]++
		return true
	}
	var err error
	if typeName == "" {
		err = c.Rels.ForEach(storepkg.QueryOpts{}, count)
	} else {
		err = c.Rels.ForEachByType(typeName, storepkg.QueryOpts{}, count)
	}
	if err != nil {
		return storepkg.RelTypeDegreeStats{}, err
	}
	return storepkg.DegreeStatsFromCounts(rels, out, in), nil
}
