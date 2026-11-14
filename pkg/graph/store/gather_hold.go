package store

import (
	"sync"
	"sync/atomic"
)

// GatherHold is the allowance of a label or type gather under a read budget
// (R2 round 3): how many ever-members the stores may examine for one gather,
// over every shard, before the read is refused. The core makes it from the
// candidate room and the gather's kind; a store reserves a set's size before it
// copies the set (Reserve), so a gather past the allowance is refused before the
// copy, in O(1), not after every member has been copied and merged. Members are
// counted as the core counts them (one candidate each when it lists or reads
// them), so the hold refuses and counts nothing within the allowance; past it
// the refusal is the core's (refuse, called once, with the number reserved). A
// nil *GatherHold bounds nothing. It is safe for concurrent shards.
type GatherHold struct {
	limit  int64
	used   atomic.Int64
	refuse func(n int) error
	once   sync.Once
	err    error
}

// NewGatherHold returns a hold of limit members; refuse makes the budget error
// for n members reserved past it. A negative limit is no bound (nil).
func NewGatherHold(limit int64, refuse func(n int) error) *GatherHold {
	if limit < 0 || refuse == nil {
		return nil
	}
	return &GatherHold{limit: limit, refuse: refuse}
}

// Reserve reserves n members of the allowance before they are examined: nil
// within it, else the refusal (the same error for every later Reserve, so
// shards that race stop at their next reserve with one count).
func (h *GatherHold) Reserve(n int) error {
	if h == nil || n <= 0 {
		return h.Err()
	}
	if u := h.used.Add(int64(n)); u > h.limit {
		h.once.Do(func() { h.err = h.refuse(int(u)) })
		return h.err
	}
	return h.Err()
}

// Err is the refusal once a Reserve passed the allowance, else nil.
func (h *GatherHold) Err() error {
	if h == nil {
		return nil
	}
	if h.used.Load() > h.limit {
		h.once.Do(func() { h.err = h.refuse(int(h.used.Load())) })
		return h.err
	}
	return nil
}
