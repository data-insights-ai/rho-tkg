package store

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
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
// "segments", "shard opens", "decoded bytes", "history examined" or "buffer
// bytes") of Budget, the budget whose limit it passed. Work is that budget's
// meter's counters as the charge that passed the limit read them, right after
// its own add: Work.<Limit> is above Budget.<Limit>, and every field is at most
// the meter's Work read after the door returned. They are equal when one
// goroutine charges the meter and nothing is charged after the refusal; the
// parallel shards of a tiered fan-out may each make their own refusal, joined
// (errors.As gives the first in shard order, ReadMeter.Exceeded the first in
// time).
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

// BoundsOutput reports whether this meter or an outer one bounds what a read
// keeps (Segments, BufferBytes): a read that decodes ahead of what it keeps
// then reads one entity at a time, so a refusal stops it before the next
// entity's decode.
func (m *ReadMeter) BoundsOutput() bool {
	for r := m; r != nil; r = r.parent {
		if r.budget.Segments != 0 || r.budget.BufferBytes != 0 {
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
// count the work it does: the bytes it decodes, the entries it examines, the
// members it collects, the buffers it grows. A nil tally counts nothing and
// refuses nothing: a read without a budget pays one nil check per decoded row.
// The layer that does a piece of work charges it, once (a store that takes a
// tally counts what it walks and collects; the graph counts only what a store
// without the metered form hands it). No charge is ever given back: every
// counter of a meter only grows, so the room a read sees only shrinks.
type ReadTally struct {
	meter *ReadMeter
	door  string
	hold  *holdScope // set by Held: the entries the read holds ahead of the work that counts them
}

// holdScope is one read's holds (ReadTally.Held): the entries a gather has
// taken into its hands and will count later, when it drops them (Drop) or when
// it settles before it returns (Settle); the door does not count them again.
// pending is not work: it only narrows the room the read's admissions see
// (CandidateRoom). mu orders every admission, Drop and Settle of the read, so
// pending and the meter move together; err is the read's refusal, kept,
// returned by every later Hold, Drop and Settle.
type holdScope struct {
	mu      sync.Mutex
	pending atomic.Uint64
	err     error
}

// Tally binds m to door. A nil meter gives a nil tally.
func (m *ReadMeter) Tally(door string) *ReadTally {
	if m == nil {
		return nil
	}
	return &ReadTally{meter: m, door: door}
}

// Candidates counts n entries a door is about to examine before it filters or
// reads them (an adjacency entry, an index match), returning the budget error
// past a limit. A nil tally counts nothing.
func (t *ReadTally) Candidates(n int) error {
	if t == nil {
		return nil
	}
	return t.meter.AddCandidates(t.door, n)
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

// Buffer counts n bytes about to be added to a temporary buffer the store
// holds for the read (BufferBytes), returning the budget error past a limit,
// before the buffer grows. A nil tally counts nothing.
func (t *ReadTally) Buffer(n int) error {
	if t == nil {
		return nil
	}
	return t.meter.AddBufferBytes(t.door, n)
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

// CandidateRoom returns how many more candidates the reads on t's meter may
// count before a candidate limit of it or of an outer meter refuses (0: the
// next one does), net of the entries t's read holds (Hold), read live; or
// math.MaxUint64 when no limit bounds them. Every reader gets that figure as
// it is now; nobody keeps it across work. A nil tally has no limit.
func (t *ReadTally) CandidateRoom() uint64 {
	if t == nil {
		return math.MaxUint64
	}
	room := uint64(math.MaxUint64)
	for r := t.meter; r != nil; r = r.parent {
		if bound := r.budget.Candidates; bound != 0 {
			left := uint64(0)
			if n := r.candidates.Load(); n < bound {
				left = bound - n
			}
			room = min(room, left)
		}
	}
	if room == math.MaxUint64 || t.hold == nil {
		return room
	}
	return room - min(room, t.hold.pending.Load())
}

// Held returns t with a hold scope, for one read whose gather holds entries
// ahead of the work that counts them (Hold, Drop, Settle): t itself when t is
// nil or has a scope already (one scope a read: the shards and the labels of
// one gather share it). Every metered tally gets a scope, a candidate limit or
// not, so the unlimited control holds and settles as a limited read does and
// counts what that read needs.
func (t *ReadTally) Held() *ReadTally {
	if t == nil || t.hold != nil {
		return t
	}
	return &ReadTally{meter: t.meter, door: t.door, hold: &holdScope{}}
}

// Hold admits n entries a gather is about to take into its hands, to be
// counted later by whoever drops them (Drop) or reads them: within the room
// (CandidateRoom, net of the read's holds) they are held, counted nothing;
// past it the entries held and these n are counted as work done (one add,
// which passes the limit and is the meter's typed error), the error is kept
// for the read, and nothing more is held. A read already refused by another
// limit (ReadMeter.Exceeded) gets that error. On a tally without a scope (Held
// not called: a gather that counts nothing, gatherFree) it holds nothing and
// returns nil.
func (t *ReadTally) Hold(n int) error {
	if t == nil || t.hold == nil {
		return nil
	}
	return t.ahead(n, true)
}

// CandidatesAhead refuses a read about to do work on n entries that it will
// count only as it examines them later, more than CandidateRoom (net of the
// read's holds): the same admission as Hold, with nothing retained. Past the
// room it counts the n entries, and on a tally with a scope the entries held
// too (they are cleared, so nothing counts twice), as work done, and returns
// the budget error of the limit they pass (the count would have refused at the
// entry past it). Within the room it counts nothing and returns nil. A nil
// tally counts nothing.
func (t *ReadTally) CandidatesAhead(n int) error {
	if t == nil {
		return nil
	}
	if t.hold == nil {
		if uint64(max(n, 0)) <= t.CandidateRoom() { // #nosec G115 -- a count
			return nil
		}
		return t.meter.AddCandidates(t.door, n)
	}
	return t.ahead(n, false)
}

// ahead is the one admission of a scoped tally (the owner of the count: the
// meter), under the scope's mutex. n within the room: held when keep, nil.
// Past it: the held entries and n are counted by one add, which passes the
// limit of the meter whose room was the least (the counters only grow), so it
// returns that meter's typed error; it is kept for the read. Never a give-back:
// nothing is subtracted from a counter.
func (t *ReadTally) ahead(n int, keep bool) error {
	s := t.hold
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	if err := t.meter.Exceeded(); err != nil {
		return err
	}
	k := uint64(max(n, 0)) // #nosec G115 -- a count
	if k <= t.CandidateRoom() {
		if keep {
			s.pending.Add(k)
		}
		return nil
	}
	total := s.pending.Swap(0) + k
	s.err = t.meter.AddCandidates(t.door, int(total)) // #nosec G115 -- a count of entries held in RAM
	return s.err
}

// Drop counts n held entries a gather examined and dropped (a member past the
// pin or at or below After, an entity a second shard or label hands on, an ID
// the envelope prune drops): they leave the read's holds and are counted now,
// under the scope's mutex. The add comes before pending is lowered, so a
// reader of CandidateRoom, which takes no lock, may see the room smaller for a
// moment, never larger. After the read's refusal it returns that error and
// counts nothing (the refusal counted them); a refusal of its own add is kept.
// On a tally without a scope (gatherFree, which held nothing) it counts
// nothing.
func (t *ReadTally) Drop(n int) error {
	if t == nil || t.hold == nil {
		return nil
	}
	s := t.hold
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	return t.countHeldLocked(uint64(max(n, 0))) // #nosec G115 -- a count
}

// Settle counts every entry the read still holds, by one add, as work done:
// the gather that held them calls it before it returns, so the read's count
// is every entry the gather took into its hands whether the door then reads
// it or not (a Limit that stops the door early, a stale sidecar entry held
// beside the current member it repeats), and the door does not count them
// again. The room does not move. After the read's refusal it returns that
// error; on a tally without a scope it counts nothing.
func (t *ReadTally) Settle() error {
	if t == nil || t.hold == nil {
		return nil
	}
	s := t.hold
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	return t.countHeldLocked(s.pending.Load())
}

// countHeldLocked counts k of the read's held entries: the add first, then
// pending lowered by k (never below 0). A refusal of the add is kept for the
// read. Caller holds t.hold.mu.
func (t *ReadTally) countHeldLocked(k uint64) error {
	if k == 0 {
		return nil
	}
	s := t.hold
	err := t.meter.AddCandidates(t.door, int(k)) // #nosec G115 -- a count of entries held in RAM
	p := s.pending.Load()                        // written only under mu
	s.pending.Store(p - min(p, k))
	if err != nil {
		s.err = err
	}
	return err
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
