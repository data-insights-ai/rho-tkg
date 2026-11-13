package store

import (
	"context"
	"errors"
	"math"
	"testing"
)

// TestReadMeter_CumulativeNestedNamed: a meter sums every call made with its
// context; a nested budget applies its own limit and adds its work to the
// outer meter too, so the tighter limit stops first; the error names the door
// and the field.
func TestReadMeter_CumulativeNestedNamed(t *testing.T) {
	outerCtx, outer := WithReadBudget(context.Background(), ReadBudget{Candidates: 10, ShardOpens: 3})
	if err := ReadMeterFrom(outerCtx).AddCandidates("a", 4); err != nil {
		t.Fatal(err)
	}
	innerCtx, inner := WithReadBudget(outerCtx, ReadBudget{Candidates: 5})
	if err := ReadMeterFrom(innerCtx).AddCandidates("b", 5); err != nil {
		t.Fatal(err)
	}
	if outer.Work().Candidates != 9 || inner.Work().Candidates != 5 {
		t.Fatalf("outer %+v inner %+v, want 9 and 5", outer.Work(), inner.Work())
	}
	// The inner limit is the tighter one now: 6 > 5.
	err := ReadMeterFrom(innerCtx).AddCandidates("c", 1)
	var be *ReadBudgetError
	if !errors.Is(err, ErrReadBudget) || !errors.As(err, &be) || be.Door != "c" || be.Limit != "candidates" || be.Work.Candidates != 6 {
		t.Fatalf("inner limit: %v (%+v)", err, be)
	}
	// The outer limit: 11 > 10, though a sibling inner budget has room.
	siblingCtx, _ := WithReadBudget(outerCtx, ReadBudget{Candidates: 100})
	err = ReadMeterFrom(siblingCtx).AddCandidates("d", 1)
	if !errors.As(err, &be) || be.Door != "d" || be.Budget.Candidates != 10 || be.Work.Candidates != 11 {
		t.Fatalf("outer limit through a sibling: %v (%+v)", err, be)
	}
	// Shard opens are a field of their own.
	if err := ReadMeterFrom(outerCtx).AddShardOpens("lend", 4); !errors.As(err, &be) || be.Limit != "shard opens" {
		t.Fatalf("shard opens: %v", err)
	}
	// No budget: nothing counted, nothing refused.
	if m := ReadMeterFrom(context.Background()); m != nil || m.AddCandidates("x", 1<<30) != nil || m.Work() != (ReadWork{}) {
		t.Fatal("a context without a budget metered")
	}
}

// TestReadMeter_DecodedBytes: decoded bytes are a field of their own: counted
// cumulatively, nested like the others, read back under the limit, and a limit
// error names the door and "decoded bytes". A tally binds a meter to a door
// for the stores that decode; a nil tally (no budget) counts nothing.
func TestReadMeter_DecodedBytes(t *testing.T) {
	ctx, meter := WithReadBudget(context.Background(), ReadBudget{DecodedBytes: 100})
	tally := ReadMeterFrom(ctx).Tally("door-a")
	if err := tally.Decoded(60); err != nil {
		t.Fatal(err)
	}
	if err := ReadMeterFrom(ctx).AddDecodedBytes("door-b", 40); err != nil { // exactly the limit
		t.Fatal(err)
	}
	if w := meter.Work(); w.DecodedBytes != 100 || w.Candidates != 0 {
		t.Fatalf("work %+v, want 100 decoded bytes", w)
	}
	err := tally.Decoded(1)
	var be *ReadBudgetError
	if !errors.Is(err, ErrReadBudget) || !errors.As(err, &be) || be.Door != "door-a" || be.Limit != "decoded bytes" || be.Budget.DecodedBytes != 100 || be.Work.DecodedBytes != 101 {
		t.Fatalf("past the limit: %v (%+v)", err, be)
	}
	// Nested: the tighter inner limit stops first, the outer meter adds up.
	inner, innerMeter := WithReadBudget(ctx, ReadBudget{DecodedBytes: 5})
	if err := ReadMeterFrom(inner).AddDecodedBytes("c", 6); !errors.As(err, &be) || be.Budget.DecodedBytes != 5 {
		t.Fatalf("inner limit: %v", err)
	}
	if innerMeter.Work().DecodedBytes != 6 || meter.Work().DecodedBytes != 107 {
		t.Fatalf("inner %+v outer %+v", innerMeter.Work(), meter.Work())
	}
	// No budget: a nil meter makes a nil tally, which counts and refuses nothing.
	var none *ReadMeter
	if tl := none.Tally("x"); tl != nil || tl.Decoded(1<<30) != nil || none.AddDecodedBytes("x", 1<<30) != nil {
		t.Fatal("no meter, yet counted")
	}
	// QueryOpts carries the tally of its meter.
	opts := QueryOpts{}.WithReadMeter(meter, "opts-door")
	if err := opts.ReadTally().Decoded(1); !errors.As(err, &be) || be.Door != "opts-door" {
		t.Fatalf("opts tally: %v", err)
	}
	if (QueryOpts{}).ReadTally() != nil {
		t.Fatal("opts without a meter carry a tally")
	}
}

// TestReadTally_CandidateRoomAndAhead (R2): a walk that holds members ahead of
// the door that counts them reads the room the meter has left (the tightest
// candidate limit of the meter and its outer ones, less what they counted) and
// refuses past it without counting a thing it did not hold; a nil tally and an
// unlimited meter have no room limit.
func TestReadTally_CandidateRoomAndAhead(t *testing.T) {
	var nilTally *ReadTally
	if nilTally.CandidateRoom() != math.MaxUint64 || nilTally.CandidatesAhead(1<<40) != nil {
		t.Fatal("a nil tally has a room limit")
	}
	_, free := WithReadBudget(context.Background(), ReadBudget{ShardOpens: 1})
	if free.Tally("d").CandidateRoom() != math.MaxUint64 {
		t.Fatal("a meter without a candidate limit has a room limit")
	}
	outerCtx, outer := WithReadBudget(context.Background(), ReadBudget{Candidates: 10})
	innerCtx, inner := WithReadBudget(outerCtx, ReadBudget{Candidates: 8})
	tally := inner.Tally("door")
	if err := ReadMeterFrom(innerCtx).AddCandidates("door", 3); err != nil {
		t.Fatal(err)
	}
	if room := tally.CandidateRoom(); room != 5 { // inner 8-3; outer 10-3 is looser
		t.Fatalf("room = %d, want 5", room)
	}
	if err := tally.CandidatesAhead(5); err != nil || inner.Work().Candidates != 3 || outer.Work().Candidates != 3 {
		t.Fatalf("within the room: %v, inner %+v outer %+v; want nil and nothing counted", err, inner.Work(), outer.Work())
	}
	if err := ReadMeterFrom(outerCtx).AddCandidates("o", 6); err != nil { // outer 9 of 10: its room (1) is the tighter now
		t.Fatal(err)
	}
	if room := tally.CandidateRoom(); room != 1 {
		t.Fatalf("room = %d, want 1 (the outer meter's)", room)
	}
	err := tally.CandidatesAhead(2)
	var be *ReadBudgetError
	if !errors.Is(err, ErrReadBudget) || !errors.As(err, &be) || be.Door != "door" || be.Limit != "candidates" || be.Budget.Candidates != 10 || be.Work.Candidates != 11 {
		t.Fatalf("past the room: %v (%+v); want the outer limit at 11", err, be)
	}
	if outer.Exceeded() == nil {
		t.Fatal("the refusal is not kept on the meter")
	}
	if _, fresh := WithReadBudget(context.Background(), ReadBudget{Candidates: 2}); fresh.Tally("d").CandidateRoom() != 2 {
		t.Fatal("a fresh meter's room is its limit")
	}
}
