package core

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestReadBudget_R2Twins_RelGates: every relationship twin checks its context
// first (cancelled: context.Canceled, nil: ErrNilContext), its IDs and a closed
// graph before a candidate is counted.
func TestReadBudget_R2Twins_RelGates(t *testing.T) {
	t.Parallel()
	g, shut := rbdBackends()[0].open(t, t.TempDir())
	w := sbBuild(t, g, 4)
	pin := w.pin
	ctx := context.Background()
	keep := func(temporal.RelSegment) bool { return true }
	doors := []struct {
		name string
		call func(ctx context.Context, id types.RelID) error
	}{
		{"Get", func(ctx context.Context, id types.RelID) error { _, err := g.Rels.GetCtx(ctx, id); return err }},
		{"RelAtTx", func(ctx context.Context, id types.RelID) error {
			_, err := g.Temporal.RelAtTxCtx(ctx, id, pin, pin)
			return err
		}},
		{"RelAt", func(ctx context.Context, id types.RelID) error {
			_, err := g.Temporal.RelAtCtx(ctx, id, pin)
			return err
		}},
		{"RelsAtTxByIDs", func(ctx context.Context, id types.RelID) error {
			_, err := g.Temporal.RelsAtTxByIDsCtx(ctx, []types.RelID{id}, pin, pin)
			return err
		}},
		{"RelsEffectiveByIDs", func(ctx context.Context, id types.RelID) error {
			return g.Temporal.RelsEffectiveByIDsCtx(ctx, []types.RelID{id}, pin, keep)
		}},
		{"RelLowestAnsweredPin", func(ctx context.Context, id types.RelID) error {
			_, err := g.Temporal.RelLowestAnsweredPinCtx(ctx, id)
			return err
		}},
	}
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	var nilCtx context.Context
	bctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: 100})
	for _, d := range doors {
		if err := d.call(cctx, w.rels[0]); !errors.Is(err, context.Canceled) {
			t.Fatalf("%s: cancelled ctx = %v", d.name, err)
		}
		if err := d.call(nilCtx, w.rels[0]); !errors.Is(err, ErrNilContext) {
			t.Fatalf("%s: nil ctx = %v", d.name, err)
		}
		if err := d.call(bctx, 0); err == nil || isBudget(err, "candidates") || meter.Work() != (storepkg.ReadWork{}) {
			t.Fatalf("%s: an invalid ID = %v, work %+v; want ErrInvalidStoreMutation and nothing counted", d.name, err, meter.Work())
		}
	}
	shut()
	for _, d := range doors {
		if err := d.call(bctx, w.rels[0]); !errors.Is(err, ErrGraphClosed) || meter.Work() != (storepkg.ReadWork{}) {
			t.Fatalf("%s: closed graph = %v, work %+v; want ErrGraphClosed and no candidate counted", d.name, err, meter.Work())
		}
	}
}

// TestReadBudget_R2Twins_RetentionGateReadsAreCounted: under a relationship
// retention watermark a point read below the graph's max first runs the
// retention gate, which reads the relationship's current row for its type
// (db76a07f did this for the node doors). RelAtTxCtx, RelsAtTxByIDsCtx and
// RelsEffectiveByIDsCtx count that read like every other: DecodedBytes 1 refuses
// before the row is decoded and the refused read leaves the row out of the
// entity cache. RelsEffectiveByIDsCtx runs the gate for every ID before the
// first segment, as the plain door does, so it counts one candidate per distinct
// ID there, once (the stream does not count them again), and a refusal in the
// gate hands out no segment: Candidates 1 over two IDs refuses at the second ID
// before its row is read.
func TestReadBudget_R2Twins_RetentionGateReadsAreCounted(t *testing.T) {
	t.Parallel()
	for _, be := range rbdBackends() {
		t.Run(be.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			g, shut := be.open(t, dir)
			ctx := context.Background()
			s, e := txbEndpointsTB(t, g)
			var rels []types.RelID
			for i := range 2 {
				r, err := g.Rels.AddByID(ctx, "GATED", s, e, map[string]any{"i": int64(i), "pad": fmt.Sprintf("%0*d", 60, i)})
				if err != nil {
					t.Fatal(err)
				}
				for v := range 3 {
					if _, err := g.Rels.Update(ctx, r.ID(), map[string]any{"v": int64(v)}); err != nil {
						t.Fatal(err)
					}
				}
				rels = append(rels, r.ID())
			}
			if _, err := g.Rels.AddByID(ctx, "OTHER", s, e, nil); err != nil {
				t.Fatal(err)
			}
			pin, err := g.Temporal.NowTx()
			if err != nil {
				t.Fatal(err)
			}
			// GATED's floor is at the first instant, so the gate passes; OTHER's
			// watermark lifts the relationship max above the pin, so the gate runs.
			gated, ok := g.relTypes.Lookup("GATED")
			if !ok {
				t.Fatal("type GATED not registered")
			}
			other, ok := g.relTypes.Lookup("OTHER")
			if !ok {
				t.Fatal("type OTHER not registered")
			}
			if err := g.advanceRelRetentionWatermark(gated, 1); err != nil {
				t.Fatal(err)
			}
			if err := g.advanceRelRetentionWatermark(other, pin+1000); err != nil {
				t.Fatal(err)
			}
			shut()
			g, shut = be.open(t, dir)
			defer shut()
			cold := func() { rbdEvict(t, g, nil, rels) }
			cached := func(id types.RelID) bool { return r2RelCached(t, g, id) }

			cold()
			plain, err := g.Temporal.RelAtTx(rels[0], pin, pin)
			if err != nil || plain == nil {
				t.Fatalf("setup: plain RelAtTx = %v, %v", plain, err)
			}
			plainRows, err := g.Temporal.RelsAtTxByIDs(rels, pin, pin)
			if err != nil || len(plainRows) != 2 || plainRows[0] == nil {
				t.Fatalf("setup: plain RelsAtTxByIDs = %v, %v", plainRows, err)
			}
			cold()
			wantStream, err := sbCollect(func(fn func(string) bool) error {
				return g.Temporal.RelsEffectiveByIDs(rels, pin, func(s temporal.RelSegment) bool { return fn(sbRelSeg(s)) })
			})
			if err != nil || len(wantStream) == 0 {
				t.Fatalf("setup: plain RelsEffectiveByIDs = %v, %v", wantStream, err)
			}
			stream := func(ctx context.Context) ([]string, error) {
				return sbCollect(func(fn func(string) bool) error {
					return g.Temporal.RelsEffectiveByIDsCtx(ctx, rels, pin, func(s temporal.RelSegment) bool { return fn(sbRelSeg(s)) })
				})
			}

			t.Run("RelAtTx", func(t *testing.T) {
				cold()
				pctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{DecodedBytes: 1})
				r, err := g.Temporal.RelAtTxCtx(pctx, rels[0], pin, pin)
				if !isBudget(err, "decoded bytes") || r != nil || cached(rels[0]) {
					t.Fatalf("DecodedBytes 1 = %v, %v (work %+v), cached %v; want a refusal before the gate's row read, the row not cached", r != nil, err, meter.Work(), cached(rels[0]))
				}
				cold()
				fctx, full := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
				r, err = g.Temporal.RelAtTxCtx(fctx, rels[0], pin, pin)
				if err != nil || r2Row(r) != r2Row(plain) || full.Work().DecodedBytes == 0 {
					t.Fatalf("unlimited = %v, %v, work %+v; want the plain row and the decoded bytes counted", r2Row(r), err, full.Work())
				}
			})
			t.Run("RelsAtTxByIDs", func(t *testing.T) {
				cold()
				pctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{DecodedBytes: 1})
				rows, err := g.Temporal.RelsAtTxByIDsCtx(pctx, rels, pin, pin)
				if !isBudget(err, "decoded bytes") || rows != nil || cached(rels[0]) {
					t.Fatalf("DecodedBytes 1 = %v, %v (work %+v), cached %v; want a refusal before the gate's row read, the row not cached", rows, err, meter.Work(), cached(rels[0]))
				}
				cold()
				fctx, full := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
				rows, err = g.Temporal.RelsAtTxByIDsCtx(fctx, rels, pin, pin)
				if err != nil || r2Rows(rows) != r2Rows(plainRows) || full.Work().DecodedBytes == 0 {
					t.Fatalf("unlimited = %v, %v, work %+v; want the plain rows and the decoded bytes counted", r2Rows(rows), err, full.Work())
				}
			})
			t.Run("RelsEffectiveByIDs", func(t *testing.T) {
				cold()
				fctx, full := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
				got, err := stream(fctx)
				work := full.Work()
				if err != nil || !slices.Equal(got, wantStream) || work.Candidates != 2 || work.DecodedBytes == 0 || work.Segments != uint64(len(wantStream)) { // #nosec G115 -- a count
					t.Fatalf("unlimited = %d segments, %v, work %+v; want the plain stream, 2 candidates (each distinct ID once), the decoded bytes and %d segments", len(got), err, work, len(wantStream))
				}
				cold()
				pctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{DecodedBytes: 1})
				got, err = stream(pctx)
				if !isBudget(err, "decoded bytes") || len(got) != 0 || cached(rels[0]) {
					t.Fatalf("DecodedBytes 1 = %d callbacks, %v (work %+v), cached %v; want no segment and a refusal before the gate's row read", len(got), err, meter.Work(), cached(rels[0]))
				}
				cold()
				pctx, meter = storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: 1})
				got, err = stream(pctx)
				if !isBudget(err, "candidates") || len(got) != 0 || meter.Work().Candidates != 2 || cached(rels[1]) {
					t.Fatalf("Candidates 1 = %d callbacks, %v (work %+v), second row cached %v; want no segment and a refusal at the second ID before its row is read", len(got), err, meter.Work(), cached(rels[1]))
				}
			})
		})
	}
}
