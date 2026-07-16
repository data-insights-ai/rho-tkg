package sharded

import (
	"errors"
	"sync"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Node property indexes (PropertyIndexCapability) — ADR-0007 S5.
//
// A label's nodes are distributed across slots, so a property index has no
// single home shard (unlike tiered, which routes property indexes to its
// ontology reference shard). Instead the sharded store fans the index DDL out
// to EVERY shard — each shard's badger.Store maintains a property index over
// its own local nodes and auto-updates it on every write it owns — and answers
// a lookup by folding the per-shard matches into one ID-sorted, paginated
// result, exactly like NodesByLabel. The shards move in lockstep for index
// definitions (every create/drop hits all of them; each shard rebuilds its own
// defs from persisted meta at open), so a uniform per-shard sentinel
// (ErrIndexExists / ErrIndexNotFound) is the single logical outcome.

var _ storecontract.PropertyIndexCapability = (*Store)(nil)

// CreatePropertyIndex builds a property index over (labelToken, propertyKey) on
// every shard. Returns ErrIndexExists if the index already exists (uniformly
// across shards).
func (s *Store) CreatePropertyIndex(labelToken uint16, propertyKey string) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return err
	}
	if err := storecontract.ValidateIndexPropertyKey(propertyKey); err != nil {
		return err
	}
	return s.fanOutUniform(func(shard *badgerShard) error {
		return shard.CreatePropertyIndex(labelToken, propertyKey)
	})
}

// DropPropertyIndex removes the (labelToken, propertyKey) index from every
// shard. Returns ErrIndexNotFound if no such index exists.
func (s *Store) DropPropertyIndex(labelToken uint16, propertyKey string) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return err
	}
	if err := storecontract.ValidateIndexPropertyKey(propertyKey); err != nil {
		return err
	}
	return s.fanOutUniform(func(shard *badgerShard) error {
		return shard.DropPropertyIndex(labelToken, propertyKey)
	})
}

// NodesByLabelAndProperty folds each shard's accelerated equality matches into
// one globally ID-sorted, paginated result. Pagination is applied AFTER the
// merge (per-shard folds return every match), mirroring NodesByLabel.
func (s *Store) NodesByLabelAndProperty(labelToken uint16, key string, value any, opts QueryOpts) ([]*types.Node, error) {
	if err := s.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateQueryOpts(opts); err != nil {
		return nil, err
	}
	per := make([][]*types.Node, len(s.shards))
	err := s.forEachShardErr(func(idx int, shard *badgerShard) error {
		nodes, e := shard.NodesByLabelAndProperty(labelToken, key, value, stripPagination(opts))
		per[idx] = nodes
		return e
	})
	if err != nil {
		return nil, err
	}
	return paginateNodes(mergeSortNodes(per), opts), nil
}

// fanOutUniform runs fn on every shard in parallel and coalesces the results.
// Because the shards move in lockstep for index DDL, a uniform sentinel across
// all shards is the ONE logical outcome and is returned once; a non-uniform
// result (genuine cross-shard divergence) returns the joined error so the
// inconsistency is visible rather than masked.
func (s *Store) fanOutUniform(fn func(shard *badgerShard) error) error {
	errs := make([]error, len(s.shards))
	var wg sync.WaitGroup
	for i, shard := range s.shards {
		wg.Add(1)
		go func(i int, shard *badgerShard) {
			defer wg.Done()
			errs[i] = fn(shard)
		}(i, shard)
	}
	wg.Wait()
	return coalesceUniform(errs)
}

// coalesceUniform returns nil if every error is nil, the single common error if
// EVERY shard returned the same sentinel (errors.Is-equal both ways), or the
// joined error otherwise (a mix of success and failure is real divergence).
func coalesceUniform(errs []error) error {
	nonNil := 0
	var first error
	allMatch := true
	for _, e := range errs {
		if e == nil {
			continue
		}
		nonNil++
		if first == nil {
			first = e
		} else if !errorsEquivalent(e, first) {
			allMatch = false
		}
	}
	if nonNil == 0 {
		return nil
	}
	if nonNil == len(errs) && allMatch {
		return first
	}
	return errors.Join(errs...)
}

func errorsEquivalent(a, b error) bool {
	return errors.Is(a, b) || errors.Is(b, a)
}
