package store

import (
	"context"
	"errors"
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
