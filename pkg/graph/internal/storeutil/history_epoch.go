package storeutil

import (
	"sync"
	"sync/atomic"
)

// History epochs (store.HistoryEpochCapability, backlog 68): a token per
// entity that changes whenever the entity's history rows change.
//
// A store keeps tokens only for the IDs a reader asked about (the watch
// table): Epoch registers an unwatched ID with a fresh token, and every
// history write the store notes for a watched ID replaces its token with a
// fresh one. An unwatched ID's history may change freely: the next Epoch
// call hands out a token never handed out before, so no value derived under
// an older token matches it. Tokens come from one process-wide counter, so a
// reopened store or another shard never reuses one.
//
// A store whose removal paths do not all pass one funnel (the memory store)
// passes the ID's history row count: a token stays valid only while the
// count is the one recorded with it. Rows are only removed by those paths, so
// the count falls; it cannot climb back without a write, and every write is
// noted. A store that notes every op passes -1.

var historyEpochSeq atomic.Uint64

// historyEpochWatchCap bounds a watch table: at the cap it is dropped whole
// (every watched ID gets a fresh token on its next Epoch call).
const historyEpochWatchCap = 1 << 16

type historyEpochEntry struct {
	epoch uint64
	n     int
}

// HistoryEpochs is one kind's watch table (nodes or relationships).
type HistoryEpochs[ID comparable] struct {
	mu sync.Mutex
	m  map[ID]historyEpochEntry
}

// Note records that id's history changed; n is its history row count after
// the change (-1: the store does not track counts).
func (h *HistoryEpochs[ID]) Note(id ID, n int) {
	h.mu.Lock()
	if _, ok := h.m[id]; ok {
		h.m[id] = historyEpochEntry{epoch: historyEpochSeq.Add(1), n: n}
	}
	h.mu.Unlock()
}

// NoteAll records that the history of any ID may have changed (Clear, a
// history key the store cannot attribute to one ID).
func (h *HistoryEpochs[ID]) NoteAll() {
	h.mu.Lock()
	h.m = nil
	h.mu.Unlock()
}

// Epoch returns id's token for a history of n rows (-1: not tracked),
// registering id when it is not watched or its count moved.
func (h *HistoryEpochs[ID]) Epoch(id ID, n int) uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	if e, ok := h.m[id]; ok && e.n == n {
		return e.epoch
	}
	if h.m == nil || len(h.m) >= historyEpochWatchCap {
		h.m = make(map[ID]historyEpochEntry)
	}
	e := historyEpochEntry{epoch: historyEpochSeq.Add(1), n: n}
	h.m[id] = e
	return e.epoch
}
