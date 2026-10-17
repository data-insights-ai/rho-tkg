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

// noteHistoryKey advances the history epoch of op's kind when op's key is a
// node or relationship history key, and records the op in that kind's history
// presence set (badgerstore_history_presence.go) and history-stamps sidecar
// (badgerstore_history_stamps.go). Every history row write and
// delete enters the write buffer through appendOps / appendOpsLoggedRouted,
// which call this under wbMu with the op.
//
// The kind's epoch advances LAST, after the presence set has the op: the
// HistoryCounts cache (historyCount.get) and HistoryGeneration readers load the
// epoch without wbMu and then read the presence set, so an epoch that moved
// first let a count that missed the op be cached under the op's own epoch, and
// every later count returned it.
func (bs *Store) noteHistoryKey(op writeOp) {
	key := op.key
	if len(key) == 0 {
		return
	}
	switch key[0] {
	case storepkg.KeyHistNode:
		if len(key) == storepkg.SizeHistKey {
			id, version := storepkg.ParseIDFromKey(key, 1), historyVersionFromKey(key)
			if h := bs.historyNoteTestHook; h != nil {
				h()
			}
			bs.histNodePresence.note(id, version, op.opType == writeOpDelete)
			bs.histNodeStamps.note(id, version, op, true)
			bs.histNodeEpochs.Note(id, -1)
		} else {
			bs.histNodeEpochs.NoteAll()
		}
		bs.histNodeEpoch.Add(1) // last (doc comment)
	case storepkg.KeyHistRel:
		if len(key) == storepkg.SizeHistKey {
			id, version := storepkg.ParseIDFromKey(key, 1), historyVersionFromKey(key)
			if h := bs.historyNoteTestHook; h != nil {
				h()
			}
			bs.histRelPresence.note(id, version, op.opType == writeOpDelete)
			bs.histRelStamps.note(id, version, op, false)
			bs.histRelEpochs.Note(id, -1)
		} else {
			bs.histRelEpochs.NoteAll()
		}
		bs.histRelEpoch.Add(1) // last (doc comment)
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
