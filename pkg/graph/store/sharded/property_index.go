package sharded

import (
	"errors"
	"fmt"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Node property indexes (PropertyIndexCapability) — ADR-0007.
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
	return s.fanOutUniformCreate(
		func(shard *badgerShard) error { return shard.CreatePropertyIndex(labelToken, propertyKey) },
		func(shard *badgerShard) error { return shard.DropPropertyIndex(labelToken, propertyKey) },
	)
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
	runShardPool(len(s.shards), func(i int) {
		errs[i] = fn(s.shards[i])
	})
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

// fanOutUniformCreate is fanOutUniform for CREATE-shaped DDL (BACKLOG 20b): a
// mid-build I/O failure on one shard while others succeed used to leave the
// index built on N-1 shards and absent on one, with no reconciliation path —
// for vector indexes this additionally orphaned per-shard disk state, since
// the store-level def map (vectorDefs) was only updated after the WHOLE
// fan-out succeeded, so the caller had no way to even discover the already-
// built shards existed. do is the create call; rollback is the matching drop,
// called on every shard whose do() succeeded, whenever the overall result is
// non-nil — including the "every shard failed, but not identically" case,
// where rollback is a no-op for shards that never succeeded (do()!=nil on
// them, so they're skipped) and safe/idempotent for ones that did. A rollback
// failure is joined into the returned error rather than swallowed, so a
// stuck rollback stays visible to the caller (and to RunRepair-style manual
// reconciliation) instead of silently leaving orphaned state.
func (s *Store) fanOutUniformCreate(do, rollback func(shard *badgerShard) error) error {
	errs := make([]error, len(s.shards))
	runShardPool(len(s.shards), func(i int) {
		errs[i] = do(s.shards[i])
	})
	result := coalesceUniform(errs)
	if result == nil {
		return nil
	}

	rollbackErrs := make([]error, len(s.shards))
	runShardPool(len(s.shards), func(i int) {
		if errs[i] != nil {
			return // this shard's create never succeeded — nothing to undo
		}
		if rerr := rollback(s.shards[i]); rerr != nil {
			rollbackErrs[i] = fmt.Errorf("shard %d: %w", i, rerr)
		}
	})
	if joined := errors.Join(rollbackErrs...); joined != nil {
		return fmt.Errorf("%w (rollback of succeeded shards failed: %w)", result, joined)
	}
	return result
}
