package graph

import (
	"context"
	"testing"
)

// TestCommittedTxMonotonic: CommittedTx is one strictly increasing millisecond
// per commit (the v5 knowledge instant), equal while nothing commits, persisted
// across a reopen and never behind the wall clock stepping back.
func TestCommittedTxMonotonic(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	g, err := New(Config{BadgerDir: dir, AllowTxBackfill: true})
	if err != nil {
		t.Fatal(err)
	}
	tx0, err := g.Temporal().CommittedTx()
	if err != nil {
		t.Fatal(err)
	}
	prev := tx0
	for i := 0; i < 5; i++ {
		if _, err := g.Nodes().Add(ctx, []string{"M"}, nil); err != nil {
			t.Fatal(err)
		}
		cur, err := g.Temporal().CommittedTx()
		if err != nil || cur <= prev {
			t.Fatalf("commit %d: CommittedTx %d -> %d, %v; want a strict increase", i, prev, cur, err)
		}
		again, _ := g.Temporal().CommittedTx()
		if again != cur {
			t.Fatalf("CommittedTx moved without a commit: %d -> %d", cur, again)
		}
		prev = cur
	}
	// The wall clock steps back: the next commit is still +1 at least.
	back := int64(prev) - 10_000
	nowMs = func() int64 { return back }
	defer func() { nowMs = wallMs }()
	if _, err := g.Nodes().Add(ctx, []string{"M"}, nil); err != nil {
		t.Fatal(err)
	}
	cur, _ := g.Temporal().CommittedTx()
	if cur <= prev {
		t.Fatalf("clock stepped back: CommittedTx %d -> %d", prev, cur)
	}
	prev = cur
	// A backfill stamp in the past is evidence, not a commit instant: the commit
	// still advances CommittedTx.
	if _, err := g.Nodes().AddWithTx(ctx, []string{"M"}, nil, 5); err != nil {
		t.Fatal(err)
	}
	cur, _ = g.Temporal().CommittedTx()
	if cur <= prev {
		t.Fatalf("backfill commit: CommittedTx %d -> %d", prev, cur)
	}
	prev = cur
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	// Persisted: a reopen reads the same value, and the next commit goes above it.
	g, err = New(Config{BadgerDir: dir, AllowTxBackfill: true})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	cur, err = g.Temporal().CommittedTx()
	if err != nil || cur != prev {
		t.Fatalf("after reopen CommittedTx = %d, %v; want %d", cur, err, prev)
	}
	if _, err := g.Nodes().Add(ctx, []string{"M"}, nil); err != nil {
		t.Fatal(err)
	}
	next, _ := g.Temporal().CommittedTx()
	if next <= prev {
		t.Fatalf("after reopen the next commit did not advance: %d -> %d", prev, next)
	}
	// A refused write is no commit.
	if _, err := g.Nodes().Add(ctx, nil, nil); err == nil {
		t.Fatalf("Add without labels accepted")
	}
	if after, _ := g.Temporal().CommittedTx(); after != next {
		t.Fatalf("a refused write moved CommittedTx: %d -> %d", next, after)
	}
}
