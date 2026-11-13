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
	// Candidates bounds the entities a door resolves before filtering: the
	// members an index lookup yields, each read as a timeline afterwards.
	Candidates uint64
	// Segments bounds the timeline segments the door returns.
	Segments uint64
}

// ReadWork is what one selective read did, counted by the doors that support
// budgets (fields a door does not count stay 0).
type ReadWork struct {
	Candidates uint64
	Segments   uint64
}

// ReadBudgetError is a selective read stopped at Limit ("candidates" or
// "segments") of Budget; Work is what it had counted, the work that exceeded
// the limit included.
type ReadBudgetError struct {
	Limit  string
	Budget ReadBudget
	Work   ReadWork
}

func (e *ReadBudgetError) Error() string {
	return fmt.Sprintf("%v: %s (budget %+v, work %+v)", ErrReadBudget, e.Limit, e.Budget, e.Work)
}

// Is makes errors.Is(err, ErrReadBudget) true.
func (e *ReadBudgetError) Is(target error) bool { return target == ErrReadBudget }

// ReadMeter counts the work of the reads made with its context; safe for
// concurrent use.
type ReadMeter struct {
	budget               ReadBudget
	candidates, segments atomic.Uint64
}

type readMeterKey struct{}

// WithReadBudget returns ctx carrying budget and the meter that counts the
// work of the reads made with it. Read the counters with Work, under or over
// the limit.
func WithReadBudget(ctx context.Context, budget ReadBudget) (context.Context, *ReadMeter) {
	m := &ReadMeter{budget: budget}
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
	return ReadWork{Candidates: m.candidates.Load(), Segments: m.segments.Load()}
}

// AddCandidates counts n candidates and returns the budget error once their
// total passes the candidate limit. A nil meter counts nothing.
func (m *ReadMeter) AddCandidates(n int) error {
	if m == nil {
		return nil
	}
	total := m.candidates.Add(uint64(n)) // #nosec G115 -- a count, never negative
	return m.over("candidates", m.budget.Candidates, total)
}

// AddSegments counts n returned segments, as AddCandidates.
func (m *ReadMeter) AddSegments(n int) error {
	if m == nil {
		return nil
	}
	total := m.segments.Add(uint64(n)) // #nosec G115 -- a count, never negative
	return m.over("segments", m.budget.Segments, total)
}

func (m *ReadMeter) over(limit string, bound, total uint64) error {
	if bound == 0 || total <= bound {
		return nil
	}
	return &ReadBudgetError{Limit: limit, Budget: m.budget, Work: m.Work()}
}
