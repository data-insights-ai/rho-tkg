package sharded

import (
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var _ storecontract.CommitFloorCapability = (*Store)(nil)

// StoredCommitFloor implements store.CommitFloorCapability: the largest slot
// floor. Every slot is a badger store opened at open, each persisting its own
// floor with its own rows.
func (s *Store) StoredCommitFloor() (types.Instant, error) {
	if err := s.checkOpen(); err != nil {
		return 0, err
	}
	var floor types.Instant
	for _, shard := range s.shards {
		f, err := shard.StoredCommitFloor()
		if err != nil {
			return 0, err
		}
		floor = max(floor, f)
	}
	return floor, nil
}
