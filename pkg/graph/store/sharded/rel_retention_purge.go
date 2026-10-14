package sharded

import (
	"sync"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var (
	_ storecontract.RelRetentionPurgeCapability = (*Store)(nil)
	_ storecontract.RelRangePurgeLogCapability  = (*Store)(nil)
)

// PurgeRelsByTypeBefore hard-removes aged-out relationships of a type across
// every shard (backlog 21). A relationship's row, BOTH adjacency legs and its
// history live on the relationship's own shard (ADR-0007), so each shard's
// purge is complete for the relationships it holds and no cross-shard sweep is
// needed (unlike the node purge, whose victims' edges may sit elsewhere).
// Each shard batch is atomic; the whole is crash-recoverable-not-atomic (a
// rerun finishes; the per-type watermark makes any residue unreadable).
func (s *Store) PurgeRelsByTypeBefore(typeToken uint16, before types.Instant, chunk int) (storecontract.RetentionPurgeResult, error) {
	var zero storecontract.RetentionPurgeResult
	if err := s.checkOpen(); err != nil {
		return zero, err
	}
	var mu sync.Mutex
	total := storecontract.RetentionPurgeResult{}
	err := s.forEachShardErr(func(_ int, shard *badger.Store) error {
		res, e := shard.PurgeRelsByTypeBefore(typeToken, before, chunk)
		if e != nil {
			return e
		}
		mu.Lock()
		total.RelsPurged += res.RelsPurged
		total.More = total.More || res.More
		total.PurgedRels = append(total.PurgedRels, res.PurgedRels...)
		mu.Unlock()
		return nil
	})
	return total, err
}

// LogRelRangePurge appends ONE store-global ChangeRelRangePurge record on the
// anchor shard (backlog 21), as LogRangePurge does for the node purge.
func (s *Store) LogRelRangePurge(typeToken uint16, before types.Instant, mode uint8) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	return s.anchor().LogRelRangePurge(typeToken, before, mode)
}
