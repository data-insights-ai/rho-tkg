package store

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
)

// A refused dimension stops every later native operation admitted through a
// different dimension too, including another child of the same outer meter.
func TestReadMeter_RefusalStopsEveryDimension(t *testing.T) {
	ops := []struct {
		name   string
		budget ReadBudget
		add    func(*ReadMeter, string, int) error
	}{
		{"candidates", ReadBudget{Candidates: 1}, (*ReadMeter).AddCandidates},
		{"segments", ReadBudget{Segments: 1}, (*ReadMeter).AddSegments},
		{"shard opens", ReadBudget{ShardOpens: 1}, (*ReadMeter).AddShardOpens},
		{"decoded bytes", ReadBudget{DecodedBytes: 1}, (*ReadMeter).AddDecodedBytes},
		{"history examined", ReadBudget{HistoryExamined: 1}, (*ReadMeter).AddHistoryExamined},
		{"buffer bytes", ReadBudget{BufferBytes: 1}, (*ReadMeter).AddBufferBytes},
	}
	// Exact non-empty unlimited control: all six native operations run once.
	_, control := WithReadBudget(t.Context(), ReadBudget{})
	native := 0
	for _, op := range ops {
		if err := op.add(control, "control", 1); err != nil {
			t.Fatal(err)
		}
		native++
	}
	if native != len(ops) || control.Work() != (ReadWork{Candidates: 1, Segments: 1, ShardOpens: 1, DecodedBytes: 1, HistoryExamined: 1, BufferBytes: 1}) {
		t.Fatalf("unlimited: %d native operations, work %+v", native, control.Work())
	}
	for _, first := range ops {
		t.Run(first.name, func(t *testing.T) {
			ctx, outer := WithReadBudget(t.Context(), first.budget)
			_, inner := WithReadBudget(ctx, ReadBudget{})
			_, sibling := WithReadBudget(ctx, ReadBudget{})
			refusal := first.add(inner, "first", 2)
			if !errors.Is(refusal, ErrReadBudget) {
				t.Fatalf("first charge: %v; want refusal", refusal)
			}
			native = 0
			beforeOuter := outer.Work()
			for _, meter := range []*ReadMeter{inner, sibling, outer} {
				before := meter.Work()
				for _, op := range ops {
					err := op.add(meter, "later", 1)
					if err == nil {
						native++ // the native operation happens only after admission
					}
					if err != refusal || meter.Work() != before || outer.Work() != beforeOuter {
						t.Errorf("%s after %s refusal: %v, work %+v (before %+v), outer %+v; want same refusal and unchanged counters", op.name, first.name, err, meter.Work(), before, outer.Work())
					}
				}
			}
			if native != 0 {
				t.Errorf("%d native operations admitted after refusal", native)
			}
		})
	}
}

func TestReadMeter_RefusalMarksEveryExceededAncestor(t *testing.T) {
	ctx, outer := WithReadBudget(t.Context(), ReadBudget{Candidates: 1})
	_, inner := WithReadBudget(ctx, ReadBudget{Candidates: 1})
	_, sibling := WithReadBudget(ctx, ReadBudget{})
	if err := inner.AddCandidates("first", 2); !errors.Is(err, ErrReadBudget) {
		t.Fatalf("crossing both limits: %v", err)
	}
	native := 0
	if err := sibling.AddDecodedBytes("sibling", 1); err == nil {
		native++
	}
	if native != 0 || outer.Exceeded() == nil || inner.Exceeded() == nil || outer.Work().DecodedBytes != 0 || sibling.Work() != (ReadWork{}) {
		t.Fatalf("sibling admitted %d native decode, outer %+v, sibling %+v; want refusal through the exceeded outer meter", native, outer.Work(), sibling.Work())
	}
}

func TestReadTally_IndependentGathersShareRoom(t *testing.T) {
	for _, nested := range []bool{false, true} {
		for _, limit := range []uint64{0, 6, 8} {
			t.Run(fmt.Sprintf("nested=%t/limit=%d", nested, limit), func(t *testing.T) {
				ctx, outer := WithReadBudget(t.Context(), ReadBudget{Candidates: limit})
				left, right := outer, outer
				if nested {
					_, left = WithReadBudget(ctx, ReadBudget{})
					_, right = WithReadBudget(ctx, ReadBudget{})
				}
				a, b := left.Tally("a").Held(), right.Tally("b").Held()
				var copied []int
				copyAfterAdmission := func(h *ReadTally, values []int) error {
					if err := h.Hold(len(values)); err != nil {
						return err
					}
					copied = append(copied, values...)
					return nil
				}
				if err := copyAfterAdmission(a, []int{1, 2, 3, 4}); err != nil {
					t.Fatal(err)
				}
				err := copyAfterAdmission(b, []int{5, 6, 7, 8})
				if limit == 6 {
					var refusal *ReadBudgetError
					if !errors.As(err, &refusal) || refusal.Limit != "candidates" || !slices.Equal(copied, []int{1, 2, 3, 4}) {
						t.Fatalf("second independent gather: copied %v, err %v; want refusal before copying its four entries", copied, err)
					}
					return
				}
				if err != nil || !slices.Equal(copied, []int{1, 2, 3, 4, 5, 6, 7, 8}) {
					t.Fatalf("control: %v, %v", copied, err)
				}
				if err := a.Settle(); err != nil {
					t.Fatal(err)
				}
				if err := b.Settle(); err != nil || outer.Work().Candidates != 8 {
					t.Fatalf("settle: %v, %+v", err, outer.Work())
				}
			})
		}
	}
}

// Holds charge at admission across every read sharing a meter or its parent.
// Drop and Settle preserve that charge and report any shared refusal. Exactly
// 40 of 64 concurrent one-entry holds succeed under a limit of 40; the first
// refused admission counts 41 and later attempts retain that same error.
// Unlimited reads use the same accounting; an unmarked tally leaves candidate
// admission to its caller.
func TestReadTally_Hold(t *testing.T) {
	var nilTally *ReadTally
	if nilTally.Held() != nil || nilTally.Hold(5) != nil || nilTally.Drop(5) != nil || nilTally.Settle() != nil {
		t.Fatal("a nil tally holds or counts")
	}

	// The unlimited control charges holds immediately too.
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
	if m.Work() != (ReadWork{Candidates: 12}) {
		t.Fatalf("unlimited holds counted %+v; want 12 admitted candidates", m.Work())
	}
	if err := h0.Drop(2); err != nil || m.Work().Candidates != 12 {
		t.Fatalf("unlimited Drop(2): %v, %+v; want the 12 admission charges unchanged", err, m.Work())
	}
	if err := h0.Settle(); err != nil || m.Work().Candidates != 12 {
		t.Fatalf("unlimited Settle: %v, %+v; want the 12 admission charges unchanged", err, m.Work())
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
	if err := h.Hold(4); err != nil || h.Hold(4) != nil || m.Work().Candidates != 8 || h.CandidateRoom() != 2 {
		t.Fatalf("two holds of 4 at 10: %v, work %+v, room %d; want 8 counted and a room of 2", err, m.Work(), h.CandidateRoom())
	}
	if m.Tally("other").CandidateRoom() != 2 {
		t.Fatal("another read did not see the eight admitted candidates")
	}
	err := h.Hold(4)
	var be *ReadBudgetError
	if !errors.As(err, &be) || be.Limit != "candidates" || be.Work.Candidates != 12 || m.Work().Candidates != 12 {
		t.Fatalf("the third hold: %v, work %+v; want the refusal with 12 counted (the sum)", err, m.Work())
	}
	if h.Hold(1) != err || h.Drop(3) != err || h.CandidatesAhead(1) != err || m.Work().Candidates != 12 {
		t.Fatalf("after the refusal: work %+v; want the same error and nothing more counted", m.Work())
	}

	// Rejecting entries neither refunds nor charges their admission again.
	_, m = WithReadBudget(context.Background(), ReadBudget{Candidates: 10})
	h = m.Tally("door").Held()
	if err := h.Hold(6); err != nil {
		t.Fatal(err)
	}
	before := h.CandidateRoom()
	if err := h.Drop(2); err != nil || m.Work().Candidates != 6 || h.CandidateRoom() != before || before != 4 {
		t.Fatalf("Drop(2) of 6 held at 10: %v, work %+v, room %d (was %d); want 6 counted, the room 4", err, m.Work(), h.CandidateRoom(), before)
	}
	// A preflight within the remaining room does not reserve it. A refused
	// demand adds to the six candidates already admitted.
	if err := h.CandidatesAhead(4); err != nil || h.CandidateRoom() != 4 || m.Work().Candidates != 6 {
		t.Fatalf("CandidatesAhead(4) within the room: %v, room %d, work %+v", err, h.CandidateRoom(), m.Work())
	}
	if err := h.CandidatesAhead(5); !errors.Is(err, ErrReadBudget) || m.Work().Candidates != 2+4+5 {
		t.Fatalf("CandidatesAhead(5) past the room: %v, work %+v; want 6 admitted plus 5 refused (11)", err, m.Work())
	}

	// Settle only checks for a shared refusal; it leaves counts and room intact.
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

// Drop preserves the admission charge and remaining room. Limit 1000; one scope holds 400 and
// 4 goroutines Drop(1) 100 times each while a sampler reads the room: it is
// exactly 600 in every sample, every round of 200. Native counts, which the
// meter does not keep: the samples taken and the Drops that returned nil
// (400), beside 400 counted. The control without a Drop also has 400 counted
// and room 600. Nested: an outer meter of 1000 binds an inner one of 2000 whose
// tally holds; the same room, 400 counted in both meters.
func TestReadTally_DropKeepsTheRoom(t *testing.T) {
	const held, droppers, per, rounds = 400, 4, 100, 200
	const room = 600
	cases := []struct {
		name  string
		build func() (*ReadTally, []*ReadMeter)
	}{
		{"one meter", func() (*ReadTally, []*ReadMeter) {
			_, m := WithReadBudget(context.Background(), ReadBudget{Candidates: 1000})
			return m.Tally("door").Held(), []*ReadMeter{m}
		}},
		{"nested, the outer binds", func() (*ReadTally, []*ReadMeter) {
			ctx, outer := WithReadBudget(context.Background(), ReadBudget{Candidates: 1000})
			_, inner := WithReadBudget(ctx, ReadBudget{Candidates: 2000})
			return inner.Tally("door").Held(), []*ReadMeter{inner, outer}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h, meters := tc.build()
			if err := h.Hold(held); err != nil || h.CandidateRoom() != room {
				t.Fatalf("control: %v, room %d; want %d", err, h.CandidateRoom(), room)
			}
			for _, m := range meters {
				if m.Work() != (ReadWork{Candidates: held}) {
					t.Fatalf("control: a hold counted %+v; want the admitted candidates", m.Work())
				}
			}
			var bad []string
			for round := range rounds {
				h, meters := tc.build()
				if err := h.Hold(held); err != nil {
					t.Fatal(err)
				}
				stop := make(chan struct{})
				var samples, wrong, dropped atomic.Int64
				var lo, hi atomic.Uint64
				lo.Store(room)
				hi.Store(room)
				var sampler sync.WaitGroup
				sampler.Add(1)
				go func() {
					defer sampler.Done()
					for {
						if r := h.CandidateRoom(); r != room {
							wrong.Add(1)
							if r < lo.Load() {
								lo.Store(r)
							}
							if r > hi.Load() {
								hi.Store(r)
							}
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
							dropped.Add(1)
						}
					}()
				}
				wg.Wait()
				close(stop)
				sampler.Wait()
				counted := true
				for _, m := range meters {
					counted = counted && m.Work().Candidates == held
				}
				if wrong.Load() != 0 || samples.Load() == 0 || dropped.Load() != held || !counted || h.CandidateRoom() != room {
					bad = append(bad, fmt.Sprintf("round %d: %d of %d samples off the room (seen %d..%d), %d drops returned nil, %d counted", round, wrong.Load(), samples.Load(), lo.Load(), hi.Load(), dropped.Load(), meters[0].Work().Candidates))
				}
			}
			if len(bad) > 0 {
				t.Fatalf("%d of %d rounds (want the room exactly 600 in every sample, 400 drops, 400 counted): %v", len(bad), rounds, bad[:min(len(bad), 3)])
			}
		})
	}
}

// A refused Hold adds its demand to the already admitted entries, so a reader
// of CandidateRoom sees the room before it (600) or after it (0), never more.
// Limit 1000 on the innermost of 32 nested
// meters (the outer ones bound candidates far above it, so every room read
// loads all 32 counters and spans the refusal often); Hold(400), then 4
// samplers read the room while one Hold(700) is refused, every round of 400.
// Afterwards the room is 0 and 1100 are counted, in the meter and in the
// error. Native count: one non-nil Hold return a round. The control: the same
// schedule unlimited, Hold(700) nil, 1100 counted before and after Settle.
func TestReadTally_RefusalKeepsTheRoom(t *testing.T) {
	const depth, samplers, rounds = 32, 4, 400
	chain := func(inner, outer ReadBudget) *ReadMeter {
		ctx := context.Background()
		for range depth - 1 {
			ctx, _ = WithReadBudget(ctx, outer)
		}
		_, m := WithReadBudget(ctx, inner)
		return m
	}
	// sample runs the schedule on h with the samplers reading the room and
	// returns the Hold errors and the rooms seen other than want.
	sample := func(h *ReadTally, want ...uint64) (errs []error, off []uint64, samples int64) {
		if err := h.Hold(400); err != nil {
			errs = append(errs, err)
		}
		stop := make(chan struct{})
		var started, done sync.WaitGroup
		var n atomic.Int64
		var mu sync.Mutex
		for range samplers {
			started.Add(1)
			done.Add(1)
			go func() {
				defer done.Done()
				first := true
				for {
					r := h.CandidateRoom()
					n.Add(1)
					if !slices.Contains(want, r) {
						mu.Lock()
						off = append(off, r)
						mu.Unlock()
					}
					if first {
						started.Done()
						first = false
					}
					select {
					case <-stop:
						return
					default:
					}
				}
			}()
		}
		started.Wait()
		if err := h.Hold(700); err != nil {
			errs = append(errs, err)
		}
		close(stop)
		done.Wait()
		return errs, off, n.Load()
	}

	m := chain(ReadBudget{Segments: 1}, ReadBudget{Segments: 1})
	h := m.Tally("door").Held()
	errs, off, samples := sample(h, math.MaxUint64)
	if len(errs) != 0 || len(off) != 0 || samples == 0 || m.Work() != (ReadWork{Candidates: 1100}) {
		t.Fatalf("unlimited control: %v, rooms %v, %d samples, work %+v; want no refusal, no limit, 1100 admitted", errs, off, samples, m.Work())
	}
	if err := h.Settle(); err != nil || m.Work().Candidates != 1100 {
		t.Fatalf("unlimited control Settle: %v, work %+v; want 1100 counted", err, m.Work())
	}

	var bad []string
	for round := range rounds {
		m := chain(ReadBudget{Candidates: 1000}, ReadBudget{Candidates: 1 << 40})
		h := m.Tally("door").Held()
		errs, off, samples := sample(h, 600, 0)
		var be *ReadBudgetError
		if len(errs) != 1 || !errors.As(errs[0], &be) || be.Budget.Candidates != 1000 || be.Work.Candidates != 1100 ||
			len(off) != 0 || samples == 0 || h.CandidateRoom() != 0 || m.Work().Candidates != 1100 {
			bad = append(bad, fmt.Sprintf("round %d: %d refusals (%v), rooms off %v, %d samples, room %d after, %d counted", round, len(errs), errors.Join(errs...), off[:min(len(off), 4)], samples, h.CandidateRoom(), m.Work().Candidates))
		}
	}
	if len(bad) > 0 {
		t.Fatalf("%d of %d rounds (want one refusal with 1100 counted, every room 600 or 0, then 0): %v", len(bad), rounds, bad[:min(len(bad), 3)])
	}
}
