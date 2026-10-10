package core

import (
	"context"
	"fmt"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Shared fixtures of the caller-instant interval-correction tests
// (SetNodeVersionIntervalWithTx / SetRelVersionIntervalWithTx, backlog 34) and
// the guard that the plain doors keep stamping the commit clock.
//
// The fixture entity (node "Ref", or a LINK between a "Ref" and an "Ev" node:
// cross-shard on tiered) has two versions recorded at backfilled instants:
//
//	v0 [vf0, vf1) k=1  TxFrom = base       TxTo = base+1000
//	v1 [vf1, ∞)   k=2  TxFrom = base+1000  (UpdatedAt = base+1000)
//
// so every t in (base+1000, now] is placeable and every t <= base+1000 is not.

// ivFix is the fixture: one node and one relationship with the chain above.
type ivFix struct {
	base, vf0, vf1 types.Instant
	nid            types.NodeID
	rid            types.RelID
}

// ivFixture builds the fixture on g (AllowTxBackfill must be on).
func ivFixture(t *testing.T, g *Core) ivFix {
	t.Helper()
	ctx := context.Background()
	base := txbWall() - 2*txbHour
	vf0 := base - txbHour
	vf1 := vf0 + 600_000
	f := ivFix{base: base, vf0: vf0, vf1: vf1}
	n, err := g.Nodes.AddWithTx(ctx, []string{"Ref"}, map[string]any{"tkg_valid_from": vf0, "k": int64(1)}, base)
	if err != nil {
		t.Fatalf("fixture Nodes.AddWithTx: %v", err)
	}
	if _, err := g.Nodes.UpdateWithTx(ctx, n.ID(), map[string]any{"tkg_valid_from": vf1, "k": int64(2)}, base+1000); err != nil {
		t.Fatalf("fixture Nodes.UpdateWithTx: %v", err)
	}
	f.nid = n.ID()
	s := txbBackfillNodeAt(t, g, "Ref", base, map[string]any{"tkg_valid_from": vf0 - 1000})
	e := txbBackfillNodeAt(t, g, "Ev", base, map[string]any{"tkg_valid_from": vf0 - 1000})
	r, err := g.Rels.AddWithTx(ctx, "LINK", s, e, map[string]any{"tkg_valid_from": vf0, "k": int64(1)}, base)
	if err != nil {
		t.Fatalf("fixture Rels.AddWithTx: %v", err)
	}
	if _, err := g.Rels.UpdateWithTx(ctx, r.ID(), map[string]any{"tkg_valid_from": vf1, "k": int64(2)}, base+1000); err != nil {
		t.Fatalf("fixture Rels.UpdateWithTx: %v", err)
	}
	f.rid = r.ID()
	return f
}

// ivShape is one cascade target over the fixture and how many rows it appends.
type ivShape struct {
	name     string
	vf, vt   types.Instant
	appended int
}

// ivShapes: "bounded" [a,b) with a inside v0 and b inside v1 appends two
// correction pieces ([a,vf1) on v0, [vf1,b) on v1) and the resumption [b,∞),
// which takes the current slot; "open" [a,∞) appends two pieces, the second
// takes the current slot.
func ivShapes(f ivFix) []ivShape {
	a := f.vf0 + 300_000
	b := f.vf1 + 300_000
	return []ivShape{
		{"bounded", a, b, 3},
		{"open", a, 0, 2},
	}
}

// ivRowTemporals returns version -> temporal stamps of every row of the kind's
// fixture entity (node when kind == "node", else the relationship).
func ivRowTemporals(t *testing.T, g *Core, kind string, f ivFix) map[uint32]types.TemporalMetadata {
	t.Helper()
	out := map[uint32]types.TemporalMetadata{}
	if kind == "node" {
		for _, n := range txbNodeChain(t, g, f.nid) {
			out[n.Version()] = nodeTemporalCopy(n)
		}
		return out
	}
	for _, r := range txbChain(t, g, f.rid) {
		out[r.Version()] = relTemporalCopy(r)
	}
	return out
}

// ivAppended returns the rows of after whose version is not in before.
func ivAppended(before, after map[uint32]types.TemporalMetadata) map[uint32]types.TemporalMetadata {
	out := map[uint32]types.TemporalMetadata{}
	for v, tm := range after {
		if _, ok := before[v]; !ok {
			out[v] = tm
		}
	}
	return out
}

// ivAssertOldRowsUnchanged fails when a row present before the call changed
// (append-only: lesson 46).
func ivAssertOldRowsUnchanged(t *testing.T, phase string, before, after map[uint32]types.TemporalMetadata) {
	t.Helper()
	for v, b := range before {
		a, ok := after[v]
		if !ok {
			t.Fatalf("[%s] row v%d vanished", phase, v)
		}
		if a != b {
			t.Fatalf("[%s] stored row v%d changed:\n before %+v\n after  %+v", phase, v, b, a)
		}
	}
}

// ivPlainDoor is one plain (clock-stamped) cascade door.
type ivPlainDoor struct {
	name string
	node func(g *Core, id types.NodeID, vf, vt types.Instant, props map[string]any) error
	rel  func(g *Core, id types.RelID, vf, vt types.Instant, props map[string]any) error
}

// ivPlainQueue is the plain cascade surface BatchBuilder and Session share.
type ivPlainQueue interface {
	SetNodeVersionInterval(id types.NodeID, validFrom, validTo types.Instant, props map[string]any) error
	SetRelVersionInterval(id types.RelID, validFrom, validTo types.Instant, props map[string]any) error
}

// ivApplyBatch queues on a fresh BatchBuilder and executes it. The queue error
// comes first; the unit is always executed. A failed op's own error is returned
// (not only ErrBatchFailed), so sentinels stay checkable.
func ivApplyBatch[Q any](g *Core, wrap func(*BatchBuilder) Q, queue func(q Q) error) error {
	b, err := NewBatchBuilder(g)
	if err != nil {
		return fmt.Errorf("%w: NewBatchBuilder: %w", errTxbSetup, err)
	}
	qErr := queue(wrap(b))
	res, execErr := b.Execute()
	if qErr != nil {
		return qErr
	}
	if execErr != nil && res != nil && len(res.Errors) > 0 {
		return res.Errors[0].Err
	}
	return execErr
}

// ivApplySession queues on a fresh synchronous ingest session (strong or
// concurrent mode) and submits; the queue error comes first, the group is
// always submitted.
func ivApplySession[Q any](g *Core, concurrent bool, wrap func(*Session) Q, queue func(q Q) error) error {
	s, err := g.Ingest.NewSession(IngestOptions{Sync: true, Concurrent: concurrent})
	if err != nil {
		return fmt.Errorf("%w: NewSession: %w", errTxbSetup, err)
	}
	qErr := queue(wrap(s))
	_, subErr := s.Submit()
	if err := s.Close(); err != nil {
		return fmt.Errorf("%w: Session.Close: %w", errTxbSetup, err)
	}
	if qErr != nil {
		return qErr
	}
	return subErr
}

func ivPlainDoors() []ivPlainDoor {
	ctx := context.Background()
	asBatch := func(b *BatchBuilder) ivPlainQueue { return b }
	asSession := func(s *Session) ivPlainQueue { return s }
	doors := []ivPlainDoor{
		{"standalone",
			func(g *Core, id types.NodeID, vf, vt types.Instant, p map[string]any) error {
				_, err := g.Temporal.SetNodeVersionInterval(ctx, id, vf, vt, p)
				return err
			},
			func(g *Core, id types.RelID, vf, vt types.Instant, p map[string]any) error {
				_, err := g.Temporal.SetRelVersionInterval(ctx, id, vf, vt, p)
				return err
			}},
		{"GraphTx",
			func(g *Core, id types.NodeID, vf, vt types.Instant, p map[string]any) error {
				return txbTxCommitDo(g, func(tx *GraphTx) error { _, err := tx.SetNodeVersionInterval(id, vf, vt, p); return err })
			},
			func(g *Core, id types.RelID, vf, vt types.Instant, p map[string]any) error {
				return txbTxCommitDo(g, func(tx *GraphTx) error { _, err := tx.SetRelVersionInterval(id, vf, vt, p); return err })
			}},
		{"Batch",
			func(g *Core, id types.NodeID, vf, vt types.Instant, p map[string]any) error {
				return ivApplyBatch(g, asBatch, func(q ivPlainQueue) error { return q.SetNodeVersionInterval(id, vf, vt, p) })
			},
			func(g *Core, id types.RelID, vf, vt types.Instant, p map[string]any) error {
				return ivApplyBatch(g, asBatch, func(q ivPlainQueue) error { return q.SetRelVersionInterval(id, vf, vt, p) })
			}},
	}
	for _, concurrent := range []bool{false, true} {
		name := "IngestStrong"
		if concurrent {
			name = "IngestConcurrent"
		}
		doors = append(doors, ivPlainDoor{name,
			func(g *Core, id types.NodeID, vf, vt types.Instant, p map[string]any) error {
				return ivApplySession(g, concurrent, asSession, func(q ivPlainQueue) error { return q.SetNodeVersionInterval(id, vf, vt, p) })
			},
			func(g *Core, id types.RelID, vf, vt types.Instant, p map[string]any) error {
				return ivApplySession(g, concurrent, asSession, func(q ivPlainQueue) error { return q.SetRelVersionInterval(id, vf, vt, p) })
			}})
	}
	return doors
}

// GUARD (green before the WithTx doors existed; kept as the plain-door pin):
// the plain SetXVersionInterval doors stamp the commit clock on every row they
// append, one instant per call, above every stamp of the chain.
//
// Catches, after the seam took a caller instant: a kernel that stamps its
// instant argument even when it is 0 (the plain door's "use the clock"), a
// plain door that forwards a non-zero instant, and a refactor that stamps the
// resumption or the slot row differently from the pieces. The clock is frozen
// ahead of the wall, so the stamp must be that reading (plus the instants the
// door's own wrapper burns), never 0, never a fixture instant.
func TestIntervalWithTx_PlainDoorsUnchanged(t *testing.T) {
	t.Parallel()
	for _, be := range txbBackends() {
		for _, d := range ivPlainDoors() {
			for _, kind := range []string{"node", "rel"} {
				t.Run(be.name+"/"+d.name+"/"+kind, func(t *testing.T) {
					g := be.open(t, true)
					f := ivFixture(t, g)
					sh := ivShapes(f)[0]
					clk := &switchableClock{}
					g.SetClockForTest(t, clk.Now)
					x := txbWall() + 10_000
					clk.at.Store(int64(x))
					before := ivRowTemporals(t, g, kind, f)
					var err error
					if kind == "node" {
						err = d.node(g, f.nid, sh.vf, sh.vt, map[string]any{"p": "x"})
					} else {
						err = d.rel(g, f.rid, sh.vf, sh.vt, map[string]any{"p": "x"})
					}
					if err != nil {
						t.Fatalf("plain %s cascade: %v", d.name, err)
					}
					peek, _ := g.Temporal.PeekTx()
					after := ivRowTemporals(t, g, kind, f)
					ivAssertOldRowsUnchanged(t, "plain door", before, after)
					app := ivAppended(before, after)
					if len(app) != sh.appended {
						t.Fatalf("plain door appended %d rows; want %d", len(app), sh.appended)
					}
					var stamp types.Instant
					for v, tm := range app {
						if stamp == 0 {
							stamp = tm.TxFrom
						}
						if tm.TxFrom != stamp || tm.UpdatedAt != stamp || tm.TxTo != 0 || tm.DeletedAt != 0 {
							t.Fatalf("appended v%d %+v; want TxFrom = UpdatedAt = %d (one instant per call), TxTo = DeletedAt = 0", v, tm, stamp)
						}
					}
					if stamp < x || stamp > peek {
						t.Fatalf("plain door stamped %d; want the frozen clock reading in [%d, %d]", stamp, x, peek)
					}
				})
			}
		}
	}
}
