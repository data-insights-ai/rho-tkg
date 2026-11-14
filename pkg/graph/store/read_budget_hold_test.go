package store

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

// TestReadTally_Hold (R2 meter, H1; lane A T1): the holds of one read share the
// room the meter has left. Within it Hold counts nothing; the Hold that passes
// it counts every entry held and its own as work done, by one add that passes
// the limit, and the refusal is kept: every later Hold and Drop returns it and
// counts nothing. Drop moves held entries to the meter under the scope's
// mutex, so the room does not move. A nested meter's tighter outer limit
// refuses. 64 goroutines Hold(1) at room 40: exactly 40 succeed (the native
// count of nil returns, which the meter does not keep), one error value, 41
// counted, every round of 200. The control, with no candidate limit, holds as
// well (a scope on every metered tally) and counts what it held when the read
// settles (Settle: one add); a tally without a scope (gatherFree) holds,
// drops and settles nothing.
func TestReadTally_Hold(t *testing.T) {
	var nilTally *ReadTally
	if nilTally.Held() != nil || nilTally.Hold(5) != nil || nilTally.Drop(5) != nil || nilTally.Settle() != nil {
		t.Fatal("a nil tally holds or counts")
	}

	// The control: no candidate limit. Held gives the read a scope all the
	// same; the holds count nothing until a Drop or the read's Settle.
	_, m := WithReadBudget(context.Background(), ReadBudget{Segments: 1})
	free := m.Tally("door")
	h0 := free.Held()
	if h0 == free || h0.Held() != h0 {
		t.Fatal("Held made no scope (or a second one) without a candidate limit")
	}
	for range 3 {
		if err := h0.Hold(4); err != nil {
			t.Fatalf("unlimited Hold: %v", err)
		}
	}
	if m.Work() != (ReadWork{}) {
		t.Fatalf("unlimited holds counted %+v; want nothing", m.Work())
	}
	if err := h0.Drop(2); err != nil || m.Work().Candidates != 2 {
		t.Fatalf("unlimited Drop(2): %v, %+v; want 2 counted at once", err, m.Work())
	}
	if err := h0.Settle(); err != nil || m.Work().Candidates != 12 {
		t.Fatalf("unlimited Settle: %v, %+v; want the 10 still held counted, 12", err, m.Work())
	}
	if err := h0.Settle(); err != nil || m.Work().Candidates != 12 {
		t.Fatalf("a second Settle: %v, %+v; want nothing more counted", err, m.Work())
	}
	// A tally without a scope (a gather that holds nothing, gatherFree).
	if free.Hold(4) != nil || free.Drop(4) != nil || free.Settle() != nil || m.Work().Candidates != 12 {
		t.Fatalf("a tally without a scope counted: %+v; want 12", m.Work())
	}

	// Three holds that each fit and pass together: refused at the third.
	_, m = WithReadBudget(context.Background(), ReadBudget{Candidates: 10})
	h := m.Tally("door").Held()
	if h.Held() != h {
		t.Fatal("Held on a scoped tally made a second scope")
	}
	if err := h.Hold(4); err != nil || h.Hold(4) != nil || m.Work().Candidates != 0 || h.CandidateRoom() != 2 {
		t.Fatalf("two holds of 4 at 10: %v, work %+v, room %d; want nothing counted and a room of 2", err, m.Work(), h.CandidateRoom())
	}
	if m.Tally("other").CandidateRoom() != 10 {
		t.Fatal("holds are per read: another tally's room moved")
	}
	err := h.Hold(4)
	var be *ReadBudgetError
	if !errors.As(err, &be) || be.Limit != "candidates" || be.Work.Candidates != 12 || m.Work().Candidates != 12 {
		t.Fatalf("the third hold: %v, work %+v; want the refusal with 12 counted (the sum)", err, m.Work())
	}
	if h.Hold(1) != err || h.Drop(3) != err || h.CandidatesAhead(1) != err || m.Work().Candidates != 12 {
		t.Fatalf("after the refusal: work %+v; want the same error and nothing more counted", m.Work())
	}

	// Drop moves held entries to the meter and leaves the room as it was.
	_, m = WithReadBudget(context.Background(), ReadBudget{Candidates: 10})
	h = m.Tally("door").Held()
	if err := h.Hold(6); err != nil {
		t.Fatal(err)
	}
	before := h.CandidateRoom()
	if err := h.Drop(2); err != nil || m.Work().Candidates != 2 || h.CandidateRoom() != before || before != 4 {
		t.Fatalf("Drop(2) of 6 held at 10: %v, work %+v, room %d (was %d); want 2 counted, the room 4", err, m.Work(), h.CandidateRoom(), before)
	}
	// CandidatesAhead on a scoped tally: the same admission, nothing kept;
	// past the room it counts the held entries too.
	if err := h.CandidatesAhead(4); err != nil || h.CandidateRoom() != 4 || m.Work().Candidates != 2 {
		t.Fatalf("CandidatesAhead(4) within the room: %v, room %d, work %+v", err, h.CandidateRoom(), m.Work())
	}
	if err := h.CandidatesAhead(5); !errors.Is(err, ErrReadBudget) || m.Work().Candidates != 2+4+5 {
		t.Fatalf("CandidatesAhead(5) past the room: %v, work %+v; want the refusal with the 4 held and the 5 counted (11)", err, m.Work())
	}

	// Settle counts what the read still holds, one add, and leaves the room.
	_, m = WithReadBudget(context.Background(), ReadBudget{Candidates: 10})
	h = m.Tally("door").Held()
	if err := h.Hold(6); err != nil || h.Drop(1) != nil {
		t.Fatal("Hold(6), Drop(1) at 10")
	}
	if err := h.Settle(); err != nil || m.Work().Candidates != 6 || h.CandidateRoom() != 4 {
		t.Fatalf("Settle of 5 held at 10: %v, work %+v, room %d; want 6 counted and a room of 4", err, m.Work(), h.CandidateRoom())
	}
	// After a refusal Settle returns it and counts nothing.
	if err := h.Hold(5); !errors.Is(err, ErrReadBudget) || h.Settle() != err || m.Work().Candidates != 11 {
		t.Fatalf("Settle after the refusal: work %+v; want the same error, 11 counted", m.Work())
	}

	// A read stopped by another limit: Hold returns that error.
	_, m = WithReadBudget(context.Background(), ReadBudget{Candidates: 10, BufferBytes: 1})
	h = m.Tally("door").Held()
	stopped := h.Buffer(2)
	if err := h.Hold(1); err == nil || err != stopped || m.Work().Candidates != 0 {
		t.Fatalf("Hold after a buffer refusal: %v (refusal %v), work %+v; want the refusal, nothing held or counted", err, stopped, m.Work())
	}

	// Nested: the tighter outer limit refuses.
	outerCtx, outer := WithReadBudget(context.Background(), ReadBudget{Candidates: 5})
	_, inner := WithReadBudget(outerCtx, ReadBudget{Candidates: 100})
	h = inner.Tally("door").Held()
	if err := h.Hold(3); err != nil || h.CandidateRoom() != 2 {
		t.Fatalf("nested Hold(3) at outer 5: %v, room %d; want held, room 2", err, h.CandidateRoom())
	}
	err = h.Hold(3)
	if !errors.As(err, &be) || be.Budget.Candidates != 5 || outer.Work().Candidates != 6 || inner.Work().Candidates != 6 {
		t.Fatalf("nested Hold past the outer room: %v, outer %+v, inner %+v; want the outer budget's refusal, 6 counted in both", err, outer.Work(), inner.Work())
	}

	// Concurrent holds on one scope at room 40 (run with -race).
	const goroutines, room, rounds = 64, 40, 200
	var bad []string
	for round := range rounds {
		_, m := WithReadBudget(context.Background(), ReadBudget{Candidates: room})
		h := m.Tally("door").Held()
		var ok atomic.Int64
		errs := make([]error, goroutines)
		var wg sync.WaitGroup
		for g := range goroutines {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if errs[g] = h.Hold(1); errs[g] == nil {
					ok.Add(1)
				}
			}()
		}
		wg.Wait()
		var first error
		distinct := 0
		for _, err := range errs {
			if err != nil && err != first {
				first = err
				distinct++
			}
		}
		if ok.Load() != room || distinct != 1 || m.Work().Candidates != room+1 {
			bad = append(bad, fmt.Sprintf("round %d: %d held, %d error values, %d counted", round, ok.Load(), distinct, m.Work().Candidates))
		}
	}
	if len(bad) > 0 {
		t.Fatalf("%d of %d rounds (want 40 held, one error value, 41 counted): %v", len(bad), rounds, bad[:min(len(bad), 3)])
	}
}

// TestReadTally_NoPhantomRefusal (R2 meter, P1; review findings 1 and 4): a
// meter that is shared live never refuses a demand that is exactly its limit,
// and its counters never fall. Limit 300 candidates and 3200 buffer bytes; 8
// goroutines each charge Candidates(1) and Buffer(16) 25 times on direct
// tallies, while 4 goroutines on one read's scope each Hold(25) and then
// Drop(1) 25 times. Every call answers nil, nothing is Exceeded, Work is
// exactly {300, 3200}, and a sampler sees Work only grow, every round of 200
// (an add that is subtracted again, or a Drop that moves pending and the meter
// apart, refuses a round). One more Candidates(1) is refused at 301. The
// control: the same schedule unlimited, the same Work. Native count: the
// operations that returned nil.
func TestReadTally_NoPhantomRefusal(t *testing.T) {
	const direct, holders, per, rounds = 8, 4, 25, 200
	run := func(b ReadBudget) (m *ReadMeter, done int64, failed []error, fell bool) {
		_, m = WithReadBudget(context.Background(), b)
		h := m.Tally("gather").Held()
		var ok atomic.Int64
		var mu sync.Mutex
		fail := func(err error) {
			mu.Lock()
			failed = append(failed, err)
			mu.Unlock()
		}
		stop := make(chan struct{})
		var fellFlag atomic.Bool
		var sampler sync.WaitGroup
		sampler.Add(1)
		go func() {
			defer sampler.Done()
			var last ReadWork
			for {
				w := m.Work()
				if w.Candidates < last.Candidates || w.BufferBytes < last.BufferBytes {
					fellFlag.Store(true)
				}
				last = w
				select {
				case <-stop:
					return
				default:
				}
			}
		}()
		var wg sync.WaitGroup
		for range direct {
			wg.Add(1)
			go func() {
				defer wg.Done()
				d := m.Tally("direct")
				for range per {
					if err := d.Candidates(1); err != nil {
						fail(err)
						return
					}
					if err := d.Buffer(16); err != nil {
						fail(err)
						return
					}
					ok.Add(2)
				}
			}()
		}
		for range holders {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := h.Hold(per); err != nil {
					fail(err)
					return
				}
				ok.Add(1)
				for range per {
					if err := h.Drop(1); err != nil {
						fail(err)
						return
					}
					ok.Add(1)
				}
			}()
		}
		wg.Wait()
		close(stop)
		sampler.Wait()
		return m, ok.Load(), failed, fellFlag.Load()
	}
	const ops = direct*per*2 + holders*(1+per)
	want := ReadWork{Candidates: 300, BufferBytes: 3200}

	m, done, failed, fell := run(ReadBudget{Segments: 1})
	if done != ops || len(failed) != 0 || fell || m.Work() != want {
		t.Fatalf("unlimited control: %d of %d done, %v, fell %v, work %+v; want every operation and work %+v", done, ops, failed, fell, m.Work(), want)
	}
	var bad []string
	for round := range rounds {
		m, done, failed, fell := run(ReadBudget{Candidates: 300, BufferBytes: 3200})
		if done != ops || len(failed) != 0 || fell || m.Exceeded() != nil || m.Work() != want {
			bad = append(bad, fmt.Sprintf("round %d: %d of %d done, %d refused (%v), fell %v, work %+v", round, done, ops, len(failed), errors.Join(failed...), fell, m.Work()))
			continue
		}
		if round == 0 {
			err := m.Tally("one more").Candidates(1)
			var be *ReadBudgetError
			if !errors.As(err, &be) || be.Work.Candidates != 301 {
				t.Fatalf("one more candidate: %v; want the refusal at 301", err)
			}
		}
	}
	if len(bad) > 0 {
		t.Fatalf("%d of %d rounds refused a demand of exactly the limit or saw Work fall: %v", len(bad), rounds, bad[:min(len(bad), 3)])
	}
}

// TestReadTally_DropKeepsTheRoom (fix round, review defect 6): Drop counts
// the entries before it lowers the read's holds, so a reader of
// CandidateRoom, which takes no lock, never sees the room larger than it is.
// Limit 1000; one scope holds 400 and 4 goroutines Drop(1) 100 times each
// while a sampler reads the room: it is never above 600, every round of 200
// (the native count: the samples taken). The control: no Drop, the room 600.
func TestReadTally_DropKeepsTheRoom(t *testing.T) {
	const limit, held, droppers, per, rounds = 1000, 400, 4, 100, 200
	_, m := WithReadBudget(context.Background(), ReadBudget{Candidates: limit})
	if h := m.Tally("door").Held(); h.Hold(held) != nil || h.CandidateRoom() != limit-held {
		t.Fatalf("control: room %d; want %d", h.CandidateRoom(), limit-held)
	}
	var bad []string
	for round := range rounds {
		_, m := WithReadBudget(context.Background(), ReadBudget{Candidates: limit})
		h := m.Tally("door").Held()
		if err := h.Hold(held); err != nil {
			t.Fatal(err)
		}
		stop := make(chan struct{})
		var samples, wrong atomic.Int64
		var sampler sync.WaitGroup
		sampler.Add(1)
		go func() {
			defer sampler.Done()
			for {
				if h.CandidateRoom() > limit-held {
					wrong.Add(1)
				}
				samples.Add(1)
				select {
				case <-stop:
					return
				default:
				}
			}
		}()
		var wg sync.WaitGroup
		for range droppers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range per {
					if err := h.Drop(1); err != nil {
						t.Error(err)
						return
					}
				}
			}()
		}
		wg.Wait()
		close(stop)
		sampler.Wait()
		if wrong.Load() != 0 || samples.Load() == 0 || m.Work().Candidates != held {
			bad = append(bad, fmt.Sprintf("round %d: %d of %d samples off the room, %d counted", round, wrong.Load(), samples.Load(), m.Work().Candidates))
		}
	}
	if len(bad) > 0 {
		t.Fatalf("%d of %d rounds (want the room at most 600 in every sample, 400 counted): %v", len(bad), rounds, bad[:min(len(bad), 3)])
	}
}
