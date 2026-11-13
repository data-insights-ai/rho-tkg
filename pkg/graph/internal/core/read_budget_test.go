package core

import (
	"context"
	"errors"
	"fmt"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestReadBudget_SelectiveDoorRefusesBroadRead (RH list R2, ai-soc's
// native_test case): 40 rows carry the probed value, 2 of them meet the
// window. A RAM sidecar narrows the candidates to the window; the on-disk
// index (RelPropertyIndexOnDiskTypes) cannot and hands every member on. Under
// a read budget the door counts its candidates and output segments; past the
// candidate limit it returns ErrReadBudget with the counters and no rows,
// before it decodes a timeline. Without a budget every answer is unchanged.
func TestReadBudget_SelectiveDoorRefusesBroadRead(t *testing.T) {
	t.Parallel()
	const rows, limit = 40, 10
	for _, disk := range []bool{false, true} {
		t.Run(fmt.Sprintf("disk=%t", disk), func(t *testing.T) {
			cfg := Config{BadgerDir: t.TempDir(), UntimedValidStart: UntimedFromTxFrom}
			if disk {
				cfg.RelPropertyIndexOnDiskTypes = []string{rtbType}
			}
			g, err := New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = g.Close() })
			w := newRTBWorld(t, g)
			w.index(t, rtbType, rtbKeys...)
			for i := range rows {
				from := types.Instant(1000 + i*1000)
				w.add(t, fmt.Sprintf("r%02d", i), rtbType, map[string]any{
					"estate": "e1", "device": "d1", "tkg_valid_from": from, "tkg_valid_to": from + 10,
				})
			}
			pin := temporal.Pin{Tx: w.pin(t), ValidStart: 1000, ValidEnd: 2010} // r00 and r01
			tuples := [][]any{{"e1", "d1"}}
			want := rtbNames(w, rtbCheck(t, w, "no budget", rtbKeys, tuples, pin)[0])
			if want != "r00,r01" {
				t.Fatalf("no budget = %q, want r00,r01", want)
			}

			// Under the limit (RAM narrows): the same answer, counted.
			ctx, meter := storepkg.WithReadBudget(w.ctx, storepkg.ReadBudget{Candidates: limit})
			got, err := g.Temporal.RelEffectiveTimelinesByProperties(ctx, rtbType, rtbKeys, tuples, pin)
			work := meter.Work()
			if !disk {
				if err != nil || rtbNames(w, got[0]) != want {
					t.Fatalf("RAM under budget = %s, %v; want %s", rtbRender(w, got), err, want)
				}
				if work.Candidates != 2 || work.Segments != 2 {
					t.Fatalf("RAM work = %+v, want 2 candidates, 2 segments", work)
				}
				return
			}
			// The disk index's every member: refused, counted, nothing read.
			var be *storepkg.ReadBudgetError
			if !errors.Is(err, storepkg.ErrReadBudget) || !errors.As(err, &be) || got != nil {
				t.Fatalf("disk under budget %d = %s, %v; want ErrReadBudget", limit, rtbRender(w, got), err)
			}
			if be.Limit != "candidates" || be.Work.Candidates != rows || be.Work.Segments != 0 || work != be.Work {
				t.Fatalf("budget error %+v (meter %+v), want candidates limit at %d candidates, 0 segments", be, work, rows)
			}

			// A budget the read fits: answered and counted.
			ctx, meter = storepkg.WithReadBudget(context.Background(), storepkg.ReadBudget{Candidates: rows})
			got, err = g.Temporal.RelEffectiveTimelinesByProperties(ctx, rtbType, rtbKeys, tuples, pin)
			if err != nil || rtbNames(w, got[0]) != want {
				t.Fatalf("disk within budget = %s, %v; want %s", rtbRender(w, got), err, want)
			}
			if work := meter.Work(); work.Candidates != rows || work.Segments != 2 {
				t.Fatalf("disk work = %+v, want %d candidates, 2 segments", work, rows)
			}
		})
	}
}
