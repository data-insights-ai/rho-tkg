package badger

import (
	"sync"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var _ storecontract.HistoryCountCapability = (*Store)(nil)

// historyCount is one kind's exact count of IDs with history rows, cached
// against the history epoch it was counted at.
type historyCount struct {
	mu    sync.Mutex
	valid bool
	epoch uint64
	n     int
}

// get returns the count cached at epoch now, or counts (outside the lock) and
// caches the result under the epoch read BEFORE the count: a history key
// enqueued during the count advances the epoch past it, so the next call
// counts again, and a count is never cached under an epoch newer than what it
// saw.
func (h *historyCount) get(epoch uint64, count func() (int, error)) (int, error) {
	h.mu.Lock()
	if h.valid && h.epoch == epoch {
		n := h.n
		h.mu.Unlock()
		return n, nil
	}
	h.mu.Unlock()
	n, err := count()
	if err != nil {
		return 0, err
	}
	h.mu.Lock()
	if !h.valid || h.epoch < epoch {
		h.valid, h.epoch, h.n = true, epoch, n
	}
	h.mu.Unlock()
	return n, nil
}

// noteHistoryKey advances the history epoch of key's kind when key is a node
// or relationship history key. Every history row write and delete enters the
// write buffer through appendOps / appendOpsLoggedRouted, which call this
// under wbMu with the op.
func (bs *Store) noteHistoryKey(key []byte) {
	if len(key) == 0 {
		return
	}
	switch key[0] {
	case storepkg.KeyHistNode:
		bs.histNodeEpoch.Add(1)
	case storepkg.KeyHistRel:
		bs.histRelEpoch.Add(1)
	}
}

// NodeHistoryCount is the number of node IDs with history rows — the IDs
// ForEachNodeHistoryID visits, pending writes included
// (store.HistoryCountCapability). Cached until a node history key is written
// or deleted (or Clear); the call after such a change counts once, a key-only
// walk of the history keyspace.
func (bs *Store) NodeHistoryCount() (int, error) {
	if err := bs.checkOpen(); err != nil {
		return 0, err
	}
	return bs.histNodeCount.get(bs.histNodeEpoch.Load(), func() (int, error) {
		n := 0
		err := bs.ForEachNodeHistoryID(func(types.NodeID) bool {
			n++
			return true
		})
		return n, err
	})
}

// RelHistoryCount is NodeHistoryCount for relationships.
func (bs *Store) RelHistoryCount() (int, error) {
	if err := bs.checkOpen(); err != nil {
		return 0, err
	}
	return bs.histRelCount.get(bs.histRelEpoch.Load(), func() (int, error) {
		n := 0
		err := bs.ForEachRelHistoryID(func(types.RelID) bool {
			n++
			return true
		})
		return n, err
	})
}
