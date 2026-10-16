package idalloc

import (
	"context"
	"math"
	"testing"
)

func cappedFixture(t *testing.T, maxID uint64) (State, Authority) {
	t.Helper()
	a, err := NewAuthority([16]byte{2}, 1)
	if err != nil {
		t.Fatal(err)
	}
	s, err := NewStateCapped(GraphID{1}, a, 100, maxID)
	if err != nil {
		t.Fatal(err)
	}
	return s, a
}

func TestNewStateCappedValidation(t *testing.T) {
	a, _ := NewAuthority([16]byte{2}, 1)
	for _, maxID := range []uint64{0} {
		if _, err := NewStateCapped(GraphID{1}, a, 100, maxID); err == nil {
			t.Fatalf("maxID %d accepted", maxID)
		}
	}
	s, err := NewStateCapped(GraphID{1}, a, 100, 100)
	if err != nil {
		t.Fatal(err)
	}
	if v, _ := Inspect(&s); v.MaxID != 100 {
		t.Fatalf("Inspect MaxID=%d", v.MaxID)
	}
	d, _ := NewState(GraphID{1}, a, 100)
	if v, _ := Inspect(&d); v.MaxID != math.MaxUint64 {
		t.Fatal("NewState default cap changed")
	}
}

func TestReserveRefusesPastMaxID(t *testing.T) {
	s, a := cappedFixture(t, math.MaxInt64)
	s.high = math.MaxInt64 - 4
	req := Request{Graph: GraphID{1}, Authority: a, Sequence: 1, Count: 4}
	next, blk, replay, err := Reserve(&s, req)
	if err != nil || replay || blk.Last != math.MaxInt64 {
		t.Fatalf("block ending below cap: %+v %v", blk, err)
	}
	// Exactly at the cap: +1 refused, state untouched.
	over := Request{Graph: GraphID{1}, Authority: a, Sequence: 2, Count: 1}
	if _, _, _, err := Reserve(&next, over); err != ErrExhausted {
		t.Fatalf("+1 past maxID: %v", err)
	}
	// Count 0 refused.
	zero := Request{Graph: GraphID{1}, Authority: a, Sequence: 2, Count: 0}
	if _, _, _, err := Reserve(&next, zero); err != ErrInvalid {
		t.Fatalf("count 0: %v", err)
	}
	// Replay of the same request still replays at the cap.
	n2, b2, replay, err := Reserve(&next, req)
	if err != nil || !replay || b2 != blk || n2 != next {
		t.Fatalf("replay at cap: %+v %v %v", b2, replay, err)
	}
	// A block that would straddle the cap is refused whole.
	s2, a2 := cappedFixture(t, 1000)
	s2.high = 998
	if _, _, _, err := Reserve(&s2, Request{Graph: GraphID{1}, Authority: a2, Sequence: 1, Count: 3}); err != ErrExhausted {
		t.Fatalf("straddle: %v", err)
	}
	if _, b, _, err := Reserve(&s2, Request{Graph: GraphID{1}, Authority: a2, Sequence: 1, Count: 2}); err != nil || b.Last != 1000 {
		t.Fatalf("exact fit: %+v %v", b, err)
	}
}

func TestStateAboveMaxIDIsInvalidAndCheckpointCarriesMaxID(t *testing.T) {
	s, a := cappedFixture(t, 500)
	s.high = 501
	if s.valid() {
		t.Fatal("high > maxID valid")
	}
	if _, err := MarshalCheckpoint(&s); err != ErrInvalid {
		t.Fatalf("marshal: %v", err)
	}
	s.high = 0
	next, _, _, err := Reserve(&s, Request{Graph: GraphID{1}, Authority: a, Sequence: 1, Count: 4})
	if err != nil {
		t.Fatal(err)
	}
	img, err := MarshalCheckpoint(&next)
	if err != nil {
		t.Fatal(err)
	}
	back, err := DecodeCheckpoint(img)
	if err != nil || back != next {
		t.Fatalf("roundtrip %+v %v", back, err)
	}
	if v, _ := Inspect(&back); v.MaxID != 500 {
		t.Fatalf("MaxID lost: %d", v.MaxID)
	}
	// Transfer preserves the cap.
	a2, _ := NewAuthority([16]byte{3}, 2)
	moved, err := Transfer(&next, a, a2)
	if err != nil {
		t.Fatal(err)
	}
	if v, _ := Inspect(&moved); v.MaxID != 500 {
		t.Fatal("Transfer dropped cap")
	}
}

func TestCursorNextStopsAtMaxID(t *testing.T) {
	a, _ := NewAuthority([16]byte{2}, 1)
	// Defence in depth: a cursor whose block overruns its cap never mints past it.
	c := &Cursor{block: Reservation{Request: Request{Graph: GraphID{1}, Authority: a, Sequence: 8, Count: 5}, First: 8, Last: 12}, next: 8, maxID: 10}
	for _, want := range []uint64{8, 9, 10} {
		if got, err := c.Next(); err != nil || got != want {
			t.Fatalf("Next=%d,%v want %d", got, err, want)
		}
	}
	for range 2 {
		if got, err := c.Next(); err != ErrExhausted || got != 0 {
			t.Fatalf("past cap: %d,%v", got, err)
		}
	}
	// Issuer with a cap refuses a committer block beyond it; one ending exactly at it is served.
	i, err := NewIssuerCapped(GraphID{1}, a, 10)
	if err != nil {
		t.Fatal(err)
	}
	over := Request{Graph: GraphID{1}, Authority: a, Sequence: 8, Count: 5}
	if _, err := i.Acquire(context.Background(), over, func(context.Context, Request) (Reservation, bool, error) {
		return Reservation{Request: over, First: 8, Last: 12}, false, nil
	}); err == nil {
		t.Fatal("block beyond maxID accepted")
	}
	for _, c := range []struct {
		seq, first, last uint64
		ok               bool
	}{{9, 9, 11, false}, {10, 10, 10, true}} {
		r := Request{Graph: GraphID{1}, Authority: a, Sequence: c.seq, Count: c.last - c.first + 1}
		cur, err := i.Acquire(context.Background(), r, func(context.Context, Request) (Reservation, bool, error) {
			return Reservation{Request: r, First: c.first, Last: c.last}, false, nil
		})
		if (err == nil) != c.ok {
			t.Fatalf("block %d..%d: %v", c.first, c.last, err)
		}
		if c.ok {
			if id, err := cur.Next(); err != nil || id != 10 {
				t.Fatalf("Next=%d,%v", id, err)
			}
			if _, err := cur.Next(); err != ErrExhausted {
				t.Fatal(err)
			}
		}
	}
}
