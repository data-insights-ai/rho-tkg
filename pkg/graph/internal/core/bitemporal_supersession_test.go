package core

import (
	"context"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Lesson 43 regressions: superseded versions are recorded knowledge, not
// retracted knowledge. The flagship 4.3.0 bitemporal scenario — explicit-VT
// update tiling the timeline, then a point query at (old VT, current txAt) —
// must return the old version's content. Under the previous visibility
// predicate (TxFrom <= txAt < TxTo) every supersession hid the prior version
// from all later txAt, so this exact query returned ErrNoVersionValidAt.
func TestBitemporalPointQueryAfterSupersession(t *testing.T) {
	t.Parallel()
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()
	ctx := context.Background()

	n, err := g.Nodes.Add(ctx, []string{"T"}, map[string]any{
		"tkg_valid_from": types.Instant(1000), "state": "v0",
	})
	if err != nil {
		t.Fatalf("add: %v", err)
	}
	time.Sleep(3 * time.Millisecond)
	txBetween := types.Instant(time.Now().UnixMilli())
	time.Sleep(3 * time.Millisecond)

	if _, err := g.Nodes.Update(ctx, n.ID(), map[string]any{
		"tkg_valid_from": types.Instant(2000), "state": "v1",
	}); err != nil {
		t.Fatalf("update: %v", err)
	}
	time.Sleep(3 * time.Millisecond)
	txNow := types.Instant(time.Now().UnixMilli())

	// (VT=1500, txAt=now): tiled answer is v0 — [1000, 2000) as known now.
	at, err := g.Temporal.NodeAtTx(n.ID(), 1500, txNow)
	if err != nil {
		t.Fatalf("NodeAtTx(1500, now): %v", err)
	}
	if v, _ := at.GetProperty("state"); v != "v0" {
		t.Fatalf("NodeAtTx(1500, now) state = %v, want v0", v)
	}

	// (VT=2500, txAt=now): v1.
	at, err = g.Temporal.NodeAtTx(n.ID(), 2500, txNow)
	if err != nil {
		t.Fatalf("NodeAtTx(2500, now): %v", err)
	}
	if v, _ := at.GetProperty("state"); v != "v1" {
		t.Fatalf("NodeAtTx(2500, now) state = %v, want v1", v)
	}

	// (VT=2500, txAt=BETWEEN): as known then, v0 was open-ended — the v1
	// assertion did not exist yet. Belief reconstruction, not current truth.
	at, err = g.Temporal.NodeAtTx(n.ID(), 2500, txBetween)
	if err != nil {
		t.Fatalf("NodeAtTx(2500, between): %v", err)
	}
	if v, _ := at.GetProperty("state"); v != "v0" {
		t.Fatalf("NodeAtTx(2500, between) state = %v, want v0 (as believed then)", v)
	}

	// (VT=1500, txAt before the entity was recorded at all): nothing was
	// known — must NOT leak any version.
	if at, err := g.Temporal.NodeAtTx(n.ID(), 1500, 1); err == nil {
		t.Fatalf("NodeAtTx with txAt before first record returned %v; want no version", at.ID())
	}
}

// Relationship mirror (testing rule 2): the same supersession semantics must
// hold behind the rel door.
func TestBitemporalRelPointQueryAfterSupersession(t *testing.T) {
	t.Parallel()
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()
	ctx := context.Background()

	a, _ := g.Nodes.Add(ctx, []string{"N"}, nil)
	b, _ := g.Nodes.Add(ctx, []string{"N"}, nil)
	if a == nil || b == nil {
		t.Fatalf("setup failed")
	}
	r, err := g.Rels.Add(ctx, "R", a, b, map[string]any{
		"tkg_valid_from": types.Instant(1000), "state": "v0",
	})
	if err != nil {
		t.Fatalf("rel add: %v", err)
	}
	time.Sleep(3 * time.Millisecond)
	if _, err := g.Rels.Update(ctx, r.ID(), map[string]any{
		"tkg_valid_from": types.Instant(2000), "state": "v1",
	}); err != nil {
		t.Fatalf("rel update: %v", err)
	}
	time.Sleep(3 * time.Millisecond)
	txNow := types.Instant(time.Now().UnixMilli())

	at, err := g.Temporal.RelAtTx(r.ID(), 1500, txNow)
	if err != nil {
		t.Fatalf("RelAtTx(1500, now): %v", err)
	}
	if v, _ := at.GetProperty("state"); v != "v0" {
		t.Fatalf("RelAtTx(1500, now) state = %v, want v0", v)
	}
}
