package core

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
)

// TestPropertyIndexOnDiskTxPinWindowKeepsLaterUpdated (RELDISKWIN review
// finding 1): an on-disk definition judges a member's rows one at a time, so
// a windowed enumeration at transaction time 0 hands on the lowest first
// transaction time of the rows that meet the window, which for an entity
// updated after the pin is the update's. A read pinned by TxPin before the
// update, with a valid filter the pre-update row meets, must still answer that
// row, as the memory store does. RelEffectiveTimelinesByProperties is the
// door that pins by TxPin with a valid window (the node doors refuse TxPin
// with a valid filter and prune by the TxAt the window is judged at).
func TestPropertyIndexOnDiskTxPinWindowKeepsLaterUpdated(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	run := func(t *testing.T, g *Core) map[string]string {
		t.Helper()
		t.Cleanup(func() { _ = g.Close() })
		if err := g.Index.CreateProperty("D", "k"); err != nil {
			t.Fatal(err)
		}
		if err := g.Index.CreateRelProperty("R", "k"); err != nil {
			t.Fatal(err)
		}
		a, err := g.Nodes.Add(ctx, []string{"D"}, map[string]any{"k": "x"})
		if err != nil {
			t.Fatal(err)
		}
		b, err := g.Nodes.Add(ctx, []string{"D"}, map[string]any{"k": "y"})
		if err != nil {
			t.Fatal(err)
		}
		r, err := g.Rels.Add(ctx, "R", a, b, map[string]any{"k": "x"})
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Millisecond)
		pin, err := g.Temporal.NowTx()
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Millisecond)
		if _, err := g.Nodes.Update(ctx, a.ID(), map[string]any{"n": int64(1)}); err != nil {
			t.Fatal(err)
		}
		if _, err := g.Rels.Update(ctx, r.ID(), map[string]any{"n": int64(1)}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Millisecond)
		later, err := g.Temporal.NowTx()
		if err != nil {
			t.Fatal(err)
		}
		out := map[string]string{}
		got, err := g.Temporal.RelEffectiveTimelinesByProperties(ctx, "R", []string{"k"}, [][]any{{"x"}},
			temporal.Pin{Tx: pin, ValidStart: later, ValidEnd: later + 10, ValidWindowSet: true})
		if err != nil {
			t.Fatal(err)
		}
		n := 0
		for _, tl := range got {
			n += len(tl)
		}
		out["rel timelines"] = fmt.Sprint(len(got), " ", n)
		return out
	}
	mem, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	want := run(t, mem)
	disk, err := New(Config{BadgerDir: t.TempDir(), RelPropertyIndexOnDiskTypes: []string{"R"}, PropertyIndexOnDiskLabels: []string{"D"}})
	if err != nil {
		t.Fatal(err)
	}
	got := run(t, disk)
	if want["rel timelines"] != "1 1" {
		t.Fatalf("memory answers nothing: %v (fixture wrong)", want)
	}
	for k, w := range want {
		if got[k] != w {
			t.Errorf("%s: on disk %s, memory %s", k, got[k], w)
		}
	}
}
