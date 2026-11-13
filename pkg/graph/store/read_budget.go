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
}

// ReadWork is what the reads made with one context did, counted by the doors
// that support budgets (fields a door does not count stay 0).
type ReadWork struct {
	Candidates uint64
	Segments   uint64
	ShardOpens uint64
}

// ReadBudgetError is a read stopped by Door at Limit ("candidates",
// "segments" or "shard opens") of Budget, the budget whose limit it passed;
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
	budget                           ReadBudget
	parent                           *ReadMeter
	candidates, segments, shardOpens atomic.Uint64
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

// Work returns the counters so far.
func (m *ReadMeter) Work() ReadWork {
	if m == nil {
		return ReadWork{}
	}
	return ReadWork{Candidates: m.candidates.Load(), Segments: m.segments.Load(), ShardOpens: m.shardOpens.Load()}
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

func (m *ReadMeter) add(door, limit string, n int, field func(*ReadMeter) (*atomic.Uint64, uint64)) error {
	var over error
	for r := m; r != nil; r = r.parent {
		c, bound := field(r)
		total := c.Add(uint64(n)) // #nosec G115 -- a count, never negative
		if over == nil && bound != 0 && total > bound {
			over = &ReadBudgetError{Door: door, Limit: limit, Budget: r.budget, Work: r.Work()}
		}
	}
	return over
}
