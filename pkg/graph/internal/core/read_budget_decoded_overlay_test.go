package core

import (
	"context"
	"fmt"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
)

// TestReadBudget_DecodedBytes_PendingOverlay (READBUDGET criterion: pending
// overlays obey the same meter): history rows still in the write buffer, not
// yet flushed to badger, are decoded for the timelines' skeletons and winning
// versions from the buffer's bytes, and those decodes count and refuse like a
// committed row's. The current rows of unflushed writes sit decoded in the
// entity cache and count nothing.
func TestReadBudget_DecodedBytes_PendingOverlay(t *testing.T) {
	t.Parallel()
	bs, err := badger.New(badger.Config{InMemory: true, FlushInterval: 1<<63 - 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	g, err := New(Config{Store: bs, UntimedValidStart: UntimedFromTxFrom})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close() })
	w := newRTBWorld(t, g)
	w.index(t, rtbType, rtbKeys...)
	for i := range 3 {
		id := w.add(t, fmt.Sprintf("p%d", i), rtbType, map[string]any{"estate": "e1", "device": "d1", "pad": fmt.Sprintf("%0*d", 200, i)})
		for v := range 3 {
			if _, err := g.Rels.Update(w.ctx, id, map[string]any{"v": int64(v)}); err != nil {
				t.Fatal(err)
			}
		}
	}
	if bs.PendingWriteCount() == 0 {
		t.Fatal("the writes were flushed: no pending overlay to read")
	}
	pin := temporal.Pin{Tx: w.pin(t)}
	tuples := [][]any{{"e1", "d1"}}
	want := rtbRender(w, rtbCheck(t, w, "no budget", rtbKeys, tuples, pin))

	rbdExact(t, "pending overlay", "RelEffectiveTimelinesByProperties", 1, func() {},
		func(ctx context.Context) (bool, error) {
			got, err := g.Temporal.RelEffectiveTimelinesByProperties(ctx, rtbType, rtbKeys, tuples, pin)
			if err != nil {
				return false, err
			}
			return rtbRender(w, got) == want, nil
		})
	if bs.PendingWriteCount() == 0 {
		t.Fatal("the reads flushed the buffer: the overlay was not what they decoded")
	}
}
