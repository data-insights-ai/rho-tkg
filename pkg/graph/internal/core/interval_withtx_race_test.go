package core

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// 13 — the order rule runs under the entity lock at the moment of the write
// (R11's shape, under -race). Plain updates and plain corrections race the
// caller-instant corrections of every door, each t reserved from NowTx just
// before the call; a plain write landing between the reservation and the
// write makes t stale, and the door must refuse it with ErrTxOrder (errors.Is
// through the Batch / ingest wrapping).
//
// Catches: an order check outside the entity lock (a GraphTx twin checking
// before its lock, a Batch / ingest pre-flight whose apply does not re-check
// in the kernel), and a door stamping anything but t. After the race every
// accepted t is the TxFrom of the rows of exactly one correction, and every
// row below them (by version) was recorded before t.
func TestIntervalWithTx_RaceClock(t *testing.T) {
	t.Parallel()
	const writers, rounds = 3, 10
	ctx := context.Background()
	for _, be := range txbBackends() {
		for _, d := range ivDoors() {
			for _, kind := range ivKinds {
				t.Run(be.name+"/"+d.name+"/"+kind, func(t *testing.T) {
					g := be.open(t, true)
					f := ivFixture(t, g)
					sh := ivShapes(f)[0]
					var (
						wg       sync.WaitGroup
						mu       sync.Mutex
						accepted []types.Instant
						errs     []error
					)
					report := func(err error) {
						mu.Lock()
						errs = append(errs, err)
						mu.Unlock()
					}
					for w := 0; w < writers; w++ {
						wg.Add(2)
						go func(w int) {
							defer wg.Done()
							for i := 0; i < rounds; i++ {
								var err error
								v := map[string]any{"k": int64(w*1000 + i)}
								switch {
								case kind == "node" && i%2 == 0:
									_, err = g.Nodes.Update(ctx, f.nid, v)
								case kind == "node":
									_, err = g.Temporal.SetNodeVersionInterval(ctx, f.nid, sh.vf, sh.vt, v)
								case i%2 == 0:
									_, err = g.Rels.Update(ctx, f.rid, v)
								default:
									_, err = g.Temporal.SetRelVersionInterval(ctx, f.rid, sh.vf, sh.vt, v)
								}
								if err != nil {
									report(fmt.Errorf("plain write: %w", err))
								}
							}
						}(w)
						go func(w int) {
							defer wg.Done()
							for i := 0; i < rounds; i++ {
								at, _ := g.Temporal.NowTx()
								err := ivRun(d, g, kind, f, sh.vf, sh.vt, map[string]any{"p": int64(-(w*1000 + i))}, at)
								switch {
								case err == nil:
									mu.Lock()
									accepted = append(accepted, at)
									mu.Unlock()
								case !errors.Is(err, ErrTxOrder) || !errors.Is(err, ErrInvalidTxFrom):
									report(fmt.Errorf("%s at %d: %w", d.name, at, err))
								}
							}
						}(w)
					}
					wg.Wait()
					for _, err := range errs {
						t.Error(err)
					}
					if t.Failed() {
						return
					}
					if len(accepted) == 0 {
						t.Fatal("no caller-instant correction accepted: the race never let one through")
					}
					rows := ivRowTemporals(t, g, kind, f)
					for _, at := range accepted {
						var lowest uint32 = math.MaxUint32
						n := 0
						for v, tm := range rows {
							if tm.TxFrom == at {
								n++
								lowest = min(lowest, v)
							}
						}
						if n == 0 {
							t.Fatalf("accepted t=%d is the TxFrom of no row", at)
						}
						for v := lowest; v < lowest+uint32(n); v++ { // #nosec G115 -- n is a small row count
							if tm, ok := rows[v]; !ok || tm.TxFrom != at {
								t.Fatalf("the rows stamped t=%d are not the consecutive versions of one correction (v%d: %+v)", at, v, tm)
							}
						}
						// A row below the correction was recorded before t. Its
						// TxTo may be later only when a later write superseded
						// it: then that write's row lies above the correction
						// and starts at that TxTo.
						top := lowest + uint32(n) - 1 // #nosec G115 -- n is a small row count
						supersededLater := func(txTo types.Instant) bool {
							for v, tm := range rows {
								if v > top && tm.TxFrom == txTo {
									return true
								}
							}
							return false
						}
						for v, tm := range rows {
							if v >= lowest {
								continue
							}
							if max(tm.TxFrom, tm.UpdatedAt, tm.DeletedAt) >= at || (tm.TxTo >= at && !supersededLater(tm.TxTo)) {
								t.Fatalf("row v%d below the correction at t=%d (v%d..v%d) carries a stamp at or after t: %+v", v, at, lowest, top, tm)
							}
						}
					}
				})
			}
		}
	}
}
