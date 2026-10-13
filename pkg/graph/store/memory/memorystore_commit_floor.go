package memory

import (
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var _ storecontract.CommitFloorCapability = (*Store)(nil)

// StoredCommitFloor implements store.CommitFloorCapability (backlog 44). The
// only rows a memory store holds across a process are the sealed
// relationship rows of an attached segment directory (OpenRelSegmentDir), and
// its MetaKV does not survive, so not even a clean Close leaves the graph's
// floor watermark behind: the answer is the newest transaction stamp of the
// rows loaded from the directory (folded while they are installed), 0
// without one.
func (ms *Store) StoredCommitFloor() (types.Instant, error) {
	if ms == nil {
		return 0, ErrNilStore
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if err := ms.checkOpenLocked(); err != nil {
		return 0, err
	}
	return ms.segCommitFloor, nil
}
