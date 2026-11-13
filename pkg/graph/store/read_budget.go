package store

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
)

// Read budgets (RH list R2, sigma's selective doors): a caller bounds the
// internal work of one selective read, not only the rows it returns, by
// putting a ReadBudget in the call's context. A door that supports budgets
// counts its work in the context's ReadMeter and, at a limit, returns a
// *ReadBudgetError (errors.Is ErrReadBudget) carrying the counters, before it
// hands out any row: an answer is exact or refused, never partial. A context
// without a budget reads as before: general callers keep their contract.

// ErrReadBudget is the sentinel of a selective read stopped at its budget.
var ErrReadBudget = errors.New("graph: read budget exceeded")

// ReadBudget bounds one selective read. A zero field is unbounded.
type ReadBudget struct {
	// Candidates bounds the entries a door examines before filtering: the
	// members an index lookup yields (each read as a timeline afterwards), the
	// IDs a by-ID or history probe looks at.
	Candidates uint64
	// Segments bounds the timeline segments the doors return.
	Segments uint64
	// ShardOpens bounds the closed shards the reads open (tiered).
	ShardOpens uint64
	// DecodedBytes bounds the stored (wire) bytes the doors' reads hand to a
	// row decoder: each current row decoded, each history version decoded (a
	// delta row and its anchor) and each history skeleton decoded, counted by
	// the store at the decode, before it, so a read past the limit stops
	// before it decodes the row that passes it. It counts decode work, not the
	// size of the data a read touches: a row the store's entity cache serves
	// is not decoded and counts nothing, nor does a store that keeps its rows
	// decoded (memory); a history row decoded twice (its skeleton, then as a
	// winner) counts twice; a partial decode counts the whole value handed to
	// it. Index reads count as Candidates, not here. The doors that count:
	// RelEffectiveTimelinesByProperties, LendByIDs, the node property doors'
	// time-free reads and GetByIDsCtx (docs/api.md, Read budgets).
	DecodedBytes uint64
	// HistoryExamined bounds the history versions the doors' reads look at:
	// each history row the store hands to a reader (a skeleton entry, a
	// temporal peek of an as-of walk, a decode), counted before it is looked
	// at, so a long history stops at the version that passes the limit. A
	// version read twice (skeleton, then winner) counts twice. The memory
	// store counts the versions of an entity's chain before it walks them.
	HistoryExamined uint64
	// BufferBytes bounds the bytes a read holds in its temporary buffers
	// (candidate sets and member lists, the rows and segments it collects
	// before it returns them), a logical estimate per entry (8 per ID or row
	// pointer, 24 per set entry, a segment's struct size), charged before each
	// entry is added, so a read stops before the buffer grows past the limit.
	// It is not the Go heap: the stores' own buffers (badger blocks, caches)
	// are not counted.
	BufferBytes uint64
}

// ReadWork is what the reads made with one context did, counted by the doors
// that support budgets (fields a door does not count stay 0).
type ReadWork struct {
	Candidates      uint64
	Segments        uint64
	ShardOpens      uint64
	DecodedBytes    uint64
	HistoryExamined uint64
	BufferBytes     uint64
}

// ReadBudgetError is a read stopped by Door at Limit ("candidates",
// "segments", "shard opens", "decoded bytes", "history examined" or "buffer bytes") of Budget, the budget whose limit it passed;
// Work is that budget's meter, the work past the limit included.
type ReadBudgetError struct {
	Door   string
	Limit  string
	Budget ReadBudget
	Work   ReadWork
}

func (e *ReadBudgetError) Error() string {
	return fmt.Sprintf("%v: %s at %s (budget %+v, work %+v)", ErrReadBudget, e.Door, e.Limit, e.Budget, e.Work)
}

// Is makes errors.Is(err, ErrReadBudget) true.
func (e *ReadBudgetError) Is(target error) bool { return target == ErrReadBudget }

// ReadMeter counts the work of every read made with its context, cumulative
// over the calls; safe for concurrent use. A meter made inside another
// budget's context adds its work to the outer meter too, and each meter checks
// its own limits, so the tighter one stops first.
type ReadMeter struct {
	budget                                         ReadBudget
	parent                                         *ReadMeter
	candidates, segments, shardOpens, decodedBytes atomic.Uint64
	historyExamined, bufferBytes                   atomic.Uint64
	exceeded                                       atomic.Pointer[ReadBudgetError] // the first refusal, for Exceeded
}

type readMeterKey struct{}

// WithReadBudget returns ctx carrying budget and the meter that counts the
// work of the reads made with it. Read the counters with Work, under or over
// the limit.
func WithReadBudget(ctx context.Context, budget ReadBudget) (context.Context, *ReadMeter) {
	m := &ReadMeter{budget: budget, parent: ReadMeterFrom(ctx)}
	return context.WithValue(ctx, readMeterKey{}, m), m
}

// ReadMeterFrom returns ctx's meter, or nil when ctx carries no budget.
func ReadMeterFrom(ctx context.Context) *ReadMeter {
	if ctx == nil {
		return nil
	}
	m, _ := ctx.Value(readMeterKey{}).(*ReadMeter)
	return m
}

// BoundsStoreWork reports whether this meter or an outer one bounds work only
// the store itself can count (DecodedBytes, ShardOpens): a store that cannot
// count it must refuse such a budget rather than read unbounded.
func (m *ReadMeter) BoundsStoreWork() bool {
	for r := m; r != nil; r = r.parent {
		if r.budget.DecodedBytes != 0 || r.budget.ShardOpens != 0 {
			return true
		}
	}
	return false
}

// BoundsShardOpens reports whether this meter or an outer one bounds
// ShardOpens.
func (m *ReadMeter) BoundsShardOpens() bool {
	for r := m; r != nil; r = r.parent {
		if r.budget.ShardOpens != 0 {
			return true
		}
	}
	return false
}

// Work returns the counters so far.
func (m *ReadMeter) Work() ReadWork {
	if m == nil {
		return ReadWork{}
	}
	return ReadWork{Candidates: m.candidates.Load(), Segments: m.segments.Load(), ShardOpens: m.shardOpens.Load(), DecodedBytes: m.decodedBytes.Load(), HistoryExamined: m.historyExamined.Load(), BufferBytes: m.bufferBytes.Load()}
}

// AddCandidates counts n candidates door examined, here and in every outer
// meter, and returns the budget error of the innermost meter whose candidate
// limit the total passes. A nil meter counts nothing.
func (m *ReadMeter) AddCandidates(door string, n int) error {
	return m.add(door, "candidates", n, func(r *ReadMeter) (*atomic.Uint64, uint64) { return &r.candidates, r.budget.Candidates })
}

// AddSegments counts n returned segments, as AddCandidates.
func (m *ReadMeter) AddSegments(door string, n int) error {
	return m.add(door, "segments", n, func(r *ReadMeter) (*atomic.Uint64, uint64) { return &r.segments, r.budget.Segments })
}

// AddShardOpens counts n closed shards opened, as AddCandidates.
func (m *ReadMeter) AddShardOpens(door string, n int) error {
	return m.add(door, "shard opens", n, func(r *ReadMeter) (*atomic.Uint64, uint64) { return &r.shardOpens, r.budget.ShardOpens })
}

// AddDecodedBytes counts n stored bytes door hands to a row decoder, as
// AddCandidates. A store calls it (through a ReadTally) before it decodes the
// bytes, so a read past the limit stops before it decodes the row that passes
// it.
func (m *ReadMeter) AddDecodedBytes(door string, n int) error {
	return m.add(door, "decoded bytes", n, func(r *ReadMeter) (*atomic.Uint64, uint64) { return &r.decodedBytes, r.budget.DecodedBytes })
}

// AddHistoryExamined counts n history versions door looks at, as
// AddCandidates.
func (m *ReadMeter) AddHistoryExamined(door string, n int) error {
	return m.add(door, "history examined", n, func(r *ReadMeter) (*atomic.Uint64, uint64) { return &r.historyExamined, r.budget.HistoryExamined })
}

// AddBufferBytes counts n bytes door is about to add to a temporary buffer,
// as AddCandidates.
func (m *ReadMeter) AddBufferBytes(door string, n int) error {
	return m.add(door, "buffer bytes", n, func(r *ReadMeter) (*atomic.Uint64, uint64) { return &r.bufferBytes, r.budget.BufferBytes })
}

// Exceeded returns the first budget error this meter or an outer one gave
// (nil while every count is within its limits): parallel workers of one read
// check it before their next unit, so a refusal stops them all. A nil meter
// returns nil.
func (m *ReadMeter) Exceeded() error {
	for r := m; r != nil; r = r.parent {
		if e := r.exceeded.Load(); e != nil {
			return e
		}
	}
	return nil
}

// ReadTally is a meter bound to the door that reads: what a store receives
// (store.ReadSnapshotMeterCapability, QueryOpts, the metered lend doors) to
// count the bytes it decodes. A nil tally counts nothing and refuses nothing:
// a read without a budget pays one nil check per decoded row.
type ReadTally struct {
	meter *ReadMeter
	door  string
}

// Tally binds m to door. A nil meter gives a nil tally.
func (m *ReadMeter) Tally(door string) *ReadTally {
	if m == nil {
		return nil
	}
	return &ReadTally{meter: m, door: door}
}

// Decoded counts n bytes about to be decoded, returning the budget error past
// a limit. A nil tally counts nothing.
func (t *ReadTally) Decoded(n int) error {
	if t == nil {
		return nil
	}
	return t.meter.AddDecodedBytes(t.door, n)
}

// History counts n history versions about to be looked at, returning the
// budget error past a limit. A nil tally counts nothing.
func (t *ReadTally) History(n int) error {
	if t == nil {
		return nil
	}
	return t.meter.AddHistoryExamined(t.door, n)
}

// ShardOpen counts one closed shard about to be opened (ShardOpens),
// returning the budget error past a limit, before the open. A nil tally
// counts nothing.
func (t *ReadTally) ShardOpen() error {
	if t == nil {
		return nil
	}
	return t.meter.AddShardOpens(t.door, 1)
}

// Exceeded is the tally's meter's Exceeded. A nil tally returns nil.
func (t *ReadTally) Exceeded() error {
	if t == nil {
		return nil
	}
	return t.meter.Exceeded()
}

// DecodedHistory counts one history version and its n stored bytes about to
// be decoded (History(1), then Decoded(n)).
func (t *ReadTally) DecodedHistory(n int) error {
	if t == nil {
		return nil
	}
	if err := t.History(1); err != nil {
		return err
	}
	return t.Decoded(n)
}

func (m *ReadMeter) add(door, limit string, n int, field func(*ReadMeter) (*atomic.Uint64, uint64)) error {
	var over error
	for r := m; r != nil; r = r.parent {
		c, bound := field(r)
		total := c.Add(uint64(n)) // #nosec G115 -- a count, never negative
		if over == nil && bound != 0 && total > bound {
			e := &ReadBudgetError{Door: door, Limit: limit, Budget: r.budget, Work: r.Work()}
			r.exceeded.CompareAndSwap(nil, e)
			over = e
		}
	}
	return over
}
