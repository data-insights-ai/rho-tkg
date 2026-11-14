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
// starts the refused work. Slice doors return all or an error; streaming
// callbacks delivered before refusal remain valid. A context without a budget
// reads as before: general callers keep their contract.

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
	// DecodedBytes bounds stored bytes handed to a decoder, charged before
	// decoding: current rows, history skeletons, winners and delta anchors,
	// encoded memory relationship segments, and property-index bounds.
	// Cache hits and already decoded RAM rows add no decode bytes. A partial
	// decode counts its whole input; repeated decodes count again. It measures
	// logical decoder input, not physical I/O, block decompression or live heap.
	// Covered doors and native tests are listed in docs/api.md.
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
// "segments", "shard opens", "decoded bytes", "history examined", "buffer
// bytes" or "unsupported") of Budget. Unsupported refuses work that cannot be
// charged, before it starts; other limits name the bound it passed. Work is
// that budget's meter's counters at refusal. For a numeric limit the charge
// has just been added: Work.<Limit> is above Budget.<Limit>, and every field
// is at most the meter's Work read after the door returned. They are equal when one
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
	candidateMu                                    sync.Mutex // the outermost meter orders candidate admissions for its whole tree
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
	if m == nil {
		return nil
	}
	root := m
	for root.parent != nil {
		root = root.parent
	}
	root.candidateMu.Lock()
	defer root.candidateMu.Unlock()
	if err := m.Exceeded(); err != nil {
		return err
	}
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
	hold  *holdScope // set by Held: this gather admits its entries before copying them
}

// holdScope marks a gather that charges candidates at admission. It keeps no
// private reservation: every read sharing the meter or an outer meter sees
// that charge before another copy starts. Drop and Settle never charge again.
type holdScope struct{}

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

// CandidateRoom returns how many more candidates reads on t's meter may
// admit before a candidate limit of it or an outer meter refuses (0: the
// next one does), including entries other gathers already admitted. The room
// only shrinks; admission must still charge before work starts. A nil tally
// or a meter without a candidate limit returns math.MaxUint64.
func (t *ReadTally) CandidateRoom() uint64 {
	if t == nil {
		return math.MaxUint64
	}
	return t.meter.candidateRoom()
}

// candidateRoom is the least room of m and its outer meters. All accepted
// holds are already counted; atomic loads let a diagnostic read sample the
// room without taking the admission lock. It is never a reservation.
func (m *ReadMeter) candidateRoom() uint64 {
	room := uint64(math.MaxUint64)
	for r := m; r != nil; r = r.parent {
		if bound := r.budget.Candidates; bound != 0 {
			left := uint64(0)
			if n := r.candidates.Load(); n < bound {
				left = bound - n
			}
			room = min(room, left)
		}
	}
	return room
}

// Held marks a gather that pays for its entries through Hold before copying
// them. It returns t itself when nil or already marked. Unlimited controls
// use the same admission and counting path as bounded reads.
func (t *ReadTally) Held() *ReadTally {
	if t == nil || t.hold != nil {
		return t
	}
	return &ReadTally{meter: t.meter, door: t.door, hold: &holdScope{}}
}

// Hold charges n candidates before a gather copies them. The charge is
// immediately visible to every read sharing any enclosing budget. Refused
// admissions do no copy, and a spent context returns its original refusal.
// A tally without Held counts nothing here: its caller owns admission.
func (t *ReadTally) Hold(n int) error {
	if t == nil || t.hold == nil {
		return nil
	}
	return t.meter.AddCandidates(t.door, max(n, 0))
}

// CandidatesAhead refuses a read about to do work on n entries that it will
// count as it examines them later, when n exceeds CandidateRoom. This is a
// preflight, not a reservation: the native walk must still charge its work.
// Past the room it charges the refused demand and returns the typed error;
// within the room it counts nothing. Already admitted holds stay counted.
func (t *ReadTally) CandidatesAhead(n int) error {
	if t == nil {
		return nil
	}
	if err := t.meter.Exceeded(); err != nil {
		return err
	}
	if uint64(max(n, 0)) <= t.CandidateRoom() { // #nosec G115 -- a count
		return nil
	}
	return t.meter.AddCandidates(t.door, max(n, 0))
}

// Drop finishes rejected entries already charged by Hold. It returns a
// shared refusal if the context was spent and never refunds or charges again.
func (t *ReadTally) Drop(_ int) error {
	if t == nil || t.hold == nil {
		return nil
	}
	return t.Exceeded()
}

// Settle checks the gather before it returns its list. Every entry was paid
// for at admission, including entries a caller's result limit will skip.
func (t *ReadTally) Settle() error { return t.Drop(0) }

// Exceeded is the tally's meter's Exceeded. A nil tally returns nil.
func (t *ReadTally) Exceeded() error {
	if t == nil {
		return nil
	}
	return t.meter.Exceeded()
}

// RefuseUnsupported stops before work that cannot be charged to this read.
// Every enclosing meter keeps the refusal, so sibling reads sharing an outer
// allowance stop too. A nil tally is an unmetered read and refuses nothing.
func (t *ReadTally) RefuseUnsupported() error {
	if t == nil || t.meter == nil {
		return nil
	}
	if err := t.Exceeded(); err != nil {
		return err
	}
	var first *ReadBudgetError
	for m := t.meter; m != nil; m = m.parent {
		e := &ReadBudgetError{Door: t.door, Limit: "unsupported", Budget: m.budget, Work: m.Work()}
		m.exceeded.CompareAndSwap(nil, e)
		if first == nil {
			first = m.exceeded.Load()
		}
	}
	return first
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
	if err := m.Exceeded(); err != nil {
		return err
	}
	var over error
	for r := m; r != nil; r = r.parent {
		c, bound := field(r)
		total := c.Add(uint64(n)) // #nosec G115 -- a count, never negative
		if bound != 0 && total > bound {
			e := &ReadBudgetError{Door: door, Limit: limit, Budget: r.budget, Work: r.Work()}
			r.exceeded.CompareAndSwap(nil, e)
			if over == nil {
				over = r.exceeded.Load()
			}
		}
	}
	return over
}
