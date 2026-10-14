package storeutil

import (
	"hash/maphash"
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
// an older token matches it. Tokens come from one process-wide increasing
// counter (NextHistoryEpoch), so a reopened store or another shard never
// reuses one, and a token drawn later is larger than every earlier one.
//
// A store whose removal paths do not all pass one funnel (the memory store)
// passes the ID's history row count: a token stays valid only while the
// count is the one recorded with it. Rows are only removed by those paths, so
// the count falls; it cannot climb back without a write, and every write is
// noted. A store that notes every op passes -1.
//
// The table is striped (historyEpochStripes, by maphash of the ID) under
// read-write locks: a hit (Epoch of a watched ID with its count) and a note of
// an unwatched ID take one stripe's read lock only.

var historyEpochSeq atomic.Uint64

// NextHistoryEpoch draws a fresh token: larger than every token drawn before.
func NextHistoryEpoch() uint64 { return historyEpochSeq.Add(1) }

const (
	historyEpochStripes = 64
	// historyEpochStripeCap bounds one stripe: at the cap it is dropped whole
	// (its IDs get fresh tokens on their next Epoch call). 64 x 1024 IDs.
	historyEpochStripeCap = 1 << 10
)

type historyEpochEntry struct {
	epoch uint64
	n     int
}

type historyEpochStripe[ID comparable] struct {
	mu sync.RWMutex
	m  map[ID]historyEpochEntry
	_  [40]byte // keep stripes on separate cache lines
}

// HistoryEpochs is one kind's watch table (nodes or relationships). The zero
// value is ready.
type HistoryEpochs[ID comparable] struct {
	stripes [historyEpochStripes]historyEpochStripe[ID]
}

var historyEpochSeed = maphash.MakeSeed()

func (h *HistoryEpochs[ID]) stripe(id ID) *historyEpochStripe[ID] {
	return &h.stripes[maphash.Comparable(historyEpochSeed, id)%historyEpochStripes]
}

// Note records that id's history changed; n is its history row count after
// the change (-1: the store does not track counts).
func (h *HistoryEpochs[ID]) Note(id ID, n int) {
	s := h.stripe(id)
	s.mu.RLock()
	_, watched := s.m[id]
	s.mu.RUnlock()
	if !watched {
		return
	}
	s.mu.Lock()
	if _, ok := s.m[id]; ok {
		s.m[id] = historyEpochEntry{epoch: NextHistoryEpoch(), n: n}
	}
	s.mu.Unlock()
}

// NoteAll records that the history of any ID may have changed (Clear, a
// history key the store cannot attribute to one ID).
func (h *HistoryEpochs[ID]) NoteAll() {
	for i := range h.stripes {
		s := &h.stripes[i]
		s.mu.Lock()
		s.m = nil
		s.mu.Unlock()
	}
}

// Epoch returns id's token for a history of n rows (-1: not tracked),
// registering id when it is not watched or its count moved.
func (h *HistoryEpochs[ID]) Epoch(id ID, n int) uint64 {
	s := h.stripe(id)
	s.mu.RLock()
	e, ok := s.m[id]
	s.mu.RUnlock()
	if ok && e.n == n {
		return e.epoch
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.m[id]; ok && e.n == n {
		return e.epoch
	}
	if s.m == nil || len(s.m) >= historyEpochStripeCap {
		s.m = make(map[ID]historyEpochEntry)
	}
	e = historyEpochEntry{epoch: NextHistoryEpoch(), n: n}
	s.m[id] = e
	return e.epoch
}
