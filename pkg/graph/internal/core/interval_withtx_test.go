package core

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"testing"
	"time"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Break-the-code tests for the caller-instant interval corrections
// (Temporal().SetNodeVersionIntervalWithTx / SetRelVersionIntervalWithTx and
// their GraphTx, BatchBuilder and ingest Session twins; backlog 34, ledger in
// tasks/evidence/interval-withtx/notes.md). Every test names the faulty
// implementation it catches and runs on memory, badger (in-memory), sharded and
// tiered, node and relationship, through every door. An accepted call appears
// only as the counterpart inside a test that breaks a faulty implementation.

// ivQueue is the queue surface BatchBuilder and the ingest Session share.
type ivQueue interface {
	ivPlainQueue
	SetNodeVersionIntervalWithTx(id types.NodeID, validFrom, validTo types.Instant, props map[string]any, txFrom types.Instant) error
	SetRelVersionIntervalWithTx(id types.RelID, validFrom, validTo types.Instant, props map[string]any, txFrom types.Instant) error
	UpdateNode(id types.NodeID, updates map[string]any) error
	UpdateRelationship(id types.RelID, updates map[string]any) error
	DeleteNode(id types.NodeID) error
}

// ivQueueMode applies one queued unit: Batch, ingest strong, ingest concurrent.
type ivQueueMode struct {
	name  string
	apply func(g *Core, queue func(q ivQueue) error) error
}

func ivQueueModes() []ivQueueMode {
	asBatch := func(b *BatchBuilder) ivQueue { return b }
	asSession := func(s *Session) ivQueue { return s }
	return []ivQueueMode{
		{"Batch", func(g *Core, queue func(q ivQueue) error) error { return ivApplyBatch(g, asBatch, queue) }},
		{"IngestStrong", func(g *Core, queue func(q ivQueue) error) error { return ivApplySession(g, false, asSession, queue) }},
		{"IngestConcurrent", func(g *Core, queue func(q ivQueue) error) error { return ivApplySession(g, true, asSession, queue) }},
	}
}

// ivDoor is one caller-instant interval door, node and relationship.
type ivDoor struct {
	name string
	node func(g *Core, id types.NodeID, vf, vt types.Instant, p map[string]any, at types.Instant) error
	rel  func(g *Core, id types.RelID, vf, vt types.Instant, p map[string]any, at types.Instant) error
}

// ivDoors: the standalone door, the GraphTx twin (committed even when the call
// fails, so a door that writes before it refuses leaves its write behind) and
// the queue twins.
func ivDoors() []ivDoor {
	ctx := context.Background()
	doors := []ivDoor{
		{"standalone",
			func(g *Core, id types.NodeID, vf, vt types.Instant, p map[string]any, at types.Instant) error {
				_, err := g.Temporal.SetNodeVersionIntervalWithTx(ctx, id, vf, vt, p, at)
				return err
			},
			func(g *Core, id types.RelID, vf, vt types.Instant, p map[string]any, at types.Instant) error {
				_, err := g.Temporal.SetRelVersionIntervalWithTx(ctx, id, vf, vt, p, at)
				return err
			}},
		{"GraphTx",
			func(g *Core, id types.NodeID, vf, vt types.Instant, p map[string]any, at types.Instant) error {
				return txbTxCommitDo(g, func(tx *GraphTx) error {
					_, err := tx.SetNodeVersionIntervalWithTx(id, vf, vt, p, at)
					return err
				})
			},
			func(g *Core, id types.RelID, vf, vt types.Instant, p map[string]any, at types.Instant) error {
				return txbTxCommitDo(g, func(tx *GraphTx) error {
					_, err := tx.SetRelVersionIntervalWithTx(id, vf, vt, p, at)
					return err
				})
			}},
	}
	for _, m := range ivQueueModes() {
		doors = append(doors, ivDoor{m.name,
			func(g *Core, id types.NodeID, vf, vt types.Instant, p map[string]any, at types.Instant) error {
				return m.apply(g, func(q ivQueue) error { return q.SetNodeVersionIntervalWithTx(id, vf, vt, p, at) })
			},
			func(g *Core, id types.RelID, vf, vt types.Instant, p map[string]any, at types.Instant) error {
				return m.apply(g, func(q ivQueue) error { return q.SetRelVersionIntervalWithTx(id, vf, vt, p, at) })
			}})
	}
	return doors
}

// ivRun runs door d on the kind's fixture entity.
func ivRun(d ivDoor, g *Core, kind string, f ivFix, vf, vt types.Instant, p map[string]any, at types.Instant) error {
	if kind == "node" {
		return d.node(g, f.nid, vf, vt, p, at)
	}
	return d.rel(g, f.rid, vf, vt, p, at)
}

var ivKinds = []string{"node", "rel"}

// ivAssertStampedAt fails unless exactly want rows were appended, every one of
// them stamped TxFrom = UpdatedAt = at with no retraction, and no stored row
// changed.
func ivAssertStampedAt(t *testing.T, phase string, before, after map[uint32]types.TemporalMetadata, want int, at types.Instant) {
	t.Helper()
	ivAssertOldRowsUnchanged(t, phase, before, after)
	app := ivAppended(before, after)
	if len(app) != want {
		t.Fatalf("[%s] %d rows appended; want %d", phase, len(app), want)
	}
	for v, tm := range app {
		if tm.TxFrom != at || tm.UpdatedAt != at || tm.TxTo != 0 || tm.DeletedAt != 0 {
			t.Fatalf("[%s] appended v%d stamps TxFrom=%d UpdatedAt=%d TxTo=%d DeletedAt=%d; want TxFrom = UpdatedAt = t = %d, TxTo = DeletedAt = 0",
				phase, v, tm.TxFrom, tm.UpdatedAt, tm.TxTo, tm.DeletedAt, at)
		}
	}
}

// ivAssertRefused fails unless err is an order refusal (ErrTxOrder wrapping
// ErrInvalidTxFrom, never a store invariant error) naming stamp, and nothing
// changed.
func ivAssertRefused(t *testing.T, phase string, err error, stamp types.Instant, before, after map[uint32]types.TemporalMetadata) {
	t.Helper()
	if !errors.Is(err, ErrTxOrder) || !errors.Is(err, ErrInvalidTxFrom) {
		t.Fatalf("[%s] err = %v; want ErrTxOrder wrapping ErrInvalidTxFrom", phase, err)
	}
	if errors.Is(err, storepkg.ErrInvalidStoreMutation) {
		t.Fatalf("[%s] err = %v; a store invariant error leaked instead of the order refusal", phase, err)
	}
	if stamp != 0 && !strings.Contains(err.Error(), fmt.Sprint(stamp)) {
		t.Fatalf("[%s] err = %q; want it to name the binding stamp %d", phase, err, stamp)
	}
	if len(after) != len(before) {
		t.Fatalf("[%s] refused but %d rows -> %d", phase, len(before), len(after))
	}
	ivAssertOldRowsUnchanged(t, phase, before, after)
}

// ivWaitPast sleeps until the wall clock is past x, so x+1 is not a future
// instant for the caller-instant gate.
func ivWaitPast(x types.Instant) {
	for txbWall() <= x+1 {
		time.Sleep(time.Millisecond)
	}
}

// ivMaxStamp is the largest TxFrom/TxTo/UpdatedAt/DeletedAt over rows.
func ivMaxStamp(rows map[uint32]types.TemporalMetadata) types.Instant {
	var m types.Instant
	for _, tm := range rows {
		m = max(m, tm.TxFrom, tm.TxTo, tm.UpdatedAt, tm.DeletedAt)
	}
	return m
}

// 1 — every appended row carries the caller instant.
//
// Catches: a door that ignores t and stamps the clock (every appended row at
// now, not at t); a door that moves t (t+1, or the clock floor above t); a
// kernel that stamps only some appended rows with t — the resumption row, the
// row that takes the current slot, or the second correction piece with the
// clock. The bounded shape appends two correction pieces and a resumption
// that takes the current slot; the open shape two pieces, the last taking the
// slot; the future shape (validFrom after t and after now — decision: allowed)
// also catches a door that applies the update doors' version-start rule to
// the cascade (it would refuse or move t).
func TestIntervalWithTx_StampsEveryAppendedRow(t *testing.T) {
	t.Parallel()
	for _, be := range txbBackends() {
		for _, d := range ivDoors() {
			for _, kind := range ivKinds {
				t.Run(be.name+"/"+d.name+"/"+kind, func(t *testing.T) {
					g := be.open(t, true)
					for i := range ivShapes(ivFix{}) {
						f := ivFixture(t, g) // a fresh chain per shape
						sh := ivShapes(f)[i]
						at := f.base + 2000
						before := ivRowTemporals(t, g, kind, f)
						if err := ivRun(d, g, kind, f, sh.vf, sh.vt, map[string]any{"p": sh.name}, at); err != nil {
							t.Fatalf("%s %s at t=%d: %v", d.name, sh.name, at, err)
						}
						after := ivRowTemporals(t, g, kind, f)
						ivAssertStampedAt(t, sh.name, before, after, sh.appended, at)
						var cur types.TemporalMetadata
						var curV uint32
						if kind == "node" {
							n, err := g.Nodes.Get(context.Background(), f.nid)
							if err != nil {
								t.Fatalf("Get: %v", err)
							}
							cur, curV = nodeTemporalCopy(n), n.Version()
						} else {
							r, err := g.Rels.Get(context.Background(), f.rid)
							if err != nil {
								t.Fatalf("Get: %v", err)
							}
							cur, curV = relTemporalCopy(r), r.Version()
						}
						if _, old := before[curV]; old || cur.TxFrom != at {
							t.Fatalf("[%s] current row v%d TxFrom=%d; want an appended row stamped t = %d in the current slot", sh.name, curV, cur.TxFrom, at)
						}
						if sh.name == "future" { // decision: validFrom after t is allowed
							if p := ivPropAt(t, g, kind, f, sh.vf+1, at, "p"); p != sh.name {
								t.Fatalf("pin t at vf+1: p = %v; want %s", p, sh.name)
							}
							if p := ivPropAt(t, g, kind, f, sh.vf+1, at-1, "p"); p != nil {
								t.Fatalf("pin t-1 at vf+1: p = %v; want none", p)
							}
						}
					}
				})
			}
		}
	}
}

// 2 — the order rule runs against the entity's whole chain.
//
// Catches: a door that skips the order check (t at or below a recorded stamp
// accepted; the appended rows would sit at or below their predecessors); a
// check with >= instead of > (t equal to the newest TxFrom accepted); a check
// that reads only the current row (the binding stamp is on history rows a
// correction appended above the current row — lesson 62 — and t between the
// current row's TxFrom and that stamp is accepted); a check limited to the
// rows overlapping the target interval (decision: the rule is about the
// entity's chain, so a binding row outside [validFrom, validTo) still
// refuses); a check that reads only the newest life of a re-imported ID.
// Every refusal changes nothing and names the binding stamp; the counterpart
// newest+1 is stamped exactly.
func TestIntervalWithTx_Order(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for _, be := range txbBackends() {
		for _, d := range ivDoors() {
			for _, kind := range ivKinds {
				t.Run(be.name+"/"+d.name+"/"+kind+"/current", func(t *testing.T) {
					g := be.open(t, true)
					f := ivFixture(t, g)
					sh := ivShapes(f)[0]
					newest := f.base + 1000
					for _, at := range []types.Instant{newest, newest - 1, f.base, f.base - 1} {
						before := ivRowTemporals(t, g, kind, f)
						err := ivRun(d, g, kind, f, sh.vf, sh.vt, map[string]any{"p": "x"}, at)
						ivAssertRefused(t, fmt.Sprintf("t=%d newest=%d", at, newest), err, newest, before, ivRowTemporals(t, g, kind, f))
					}
					before := ivRowTemporals(t, g, kind, f)
					if err := ivRun(d, g, kind, f, sh.vf, sh.vt, map[string]any{"p": "x"}, newest+1); err != nil {
						t.Fatalf("counterpart t = newest+1: %v", err)
					}
					ivAssertStampedAt(t, "newest+1", before, ivRowTemporals(t, g, kind, f), sh.appended, newest+1)
				})
				t.Run(be.name+"/"+d.name+"/"+kind+"/history_above_current_outside_target", func(t *testing.T) {
					g := be.open(t, true)
					f := ivFixture(t, g)
					// A plain correction inside v0: its rows (a piece and a
					// bounded resumption ending at vf1) take versions above
					// the current row, which keeps the slot, and carry the
					// newest stamps of the chain, at the clock.
					var err error
					if kind == "node" {
						_, err = g.Temporal.SetNodeVersionInterval(ctx, f.nid, f.vf0+100_000, f.vf0+200_000, map[string]any{"p": "plain"})
					} else {
						_, err = g.Temporal.SetRelVersionInterval(ctx, f.rid, f.vf0+100_000, f.vf0+200_000, map[string]any{"p": "plain"})
					}
					if err != nil {
						t.Fatalf("plain cascade: %v", err)
					}
					rows := ivRowTemporals(t, g, kind, f)
					binding := ivMaxStamp(rows)
					var curV uint32
					if kind == "node" {
						n, _ := g.Nodes.Get(ctx, f.nid)
						curV = n.Version()
					} else {
						r, _ := g.Rels.Get(ctx, f.rid)
						curV = r.Version()
					}
					if cur := rows[curV]; cur.TxFrom >= binding || cur.TxFrom != f.base+1000 {
						t.Fatalf("fixture: current v%d TxFrom %d; want the fixture's %d below the history stamp %d", curV, cur.TxFrom, f.base+1000, binding)
					}
					ivWaitPast(binding)
					// The target overlaps only v1: none of the binding rows.
					vf, vt := f.vf1+100_000, types.Instant(0)
					for _, at := range []types.Instant{binding, binding - 1, f.base + 1001} {
						before := ivRowTemporals(t, g, kind, f)
						err := ivRun(d, g, kind, f, vf, vt, map[string]any{"p": "x"}, at)
						ivAssertRefused(t, fmt.Sprintf("t=%d binding=%d", at, binding), err, binding, before, ivRowTemporals(t, g, kind, f))
					}
					before := ivRowTemporals(t, g, kind, f)
					if err := ivRun(d, g, kind, f, vf, vt, map[string]any{"p": "x"}, binding+1); err != nil {
						t.Fatalf("counterpart t = binding+1: %v", err)
					}
					ivAssertStampedAt(t, "binding+1", before, ivRowTemporals(t, g, kind, f), 1, binding+1)
				})
				t.Run(be.name+"/"+d.name+"/"+kind+"/reimported_life", func(t *testing.T) {
					g := be.open(t, true)
					f := ivFixture(t, g)
					life2 := f.vf1 + 10*txbHour
					var err error
					if kind == "node" {
						if err = g.Nodes.DeleteWithTx(ctx, f.nid, f.base+2000); err != nil {
							t.Fatalf("DeleteWithTx: %v", err)
						}
						_, err = g.Nodes.Import(ctx, f.nid, []string{"Ref"}, map[string]any{"tkg_valid_from": life2, "k": int64(3)})
					} else {
						old := txbChain(t, g, f.rid)[0]
						if err = g.Rels.DeleteWithTx(ctx, f.rid, f.base+2000); err != nil {
							t.Fatalf("DeleteWithTx: %v", err)
						}
						s, serr := g.Nodes.Get(ctx, old.StartNodeID())
						e, eerr := g.Nodes.Get(ctx, old.EndNodeID())
						if serr != nil || eerr != nil {
							t.Fatalf("endpoints: %v %v", serr, eerr)
						}
						_, err = g.Rels.Import(ctx, f.rid, "LINK", s, e, map[string]any{"tkg_valid_from": life2, "k": int64(3)})
					}
					if err != nil {
						t.Fatalf("re-import: %v", err)
					}
					rows := ivRowTemporals(t, g, kind, f)
					binding := ivMaxStamp(rows)
					ivWaitPast(binding)
					vf := life2 + 1000
					for _, at := range []types.Instant{binding, binding - 1, f.base + 2001} {
						before := ivRowTemporals(t, g, kind, f)
						err := ivRun(d, g, kind, f, vf, 0, map[string]any{"p": "x"}, at)
						ivAssertRefused(t, fmt.Sprintf("t=%d binding=%d", at, binding), err, binding, before, ivRowTemporals(t, g, kind, f))
					}
					before := ivRowTemporals(t, g, kind, f)
					if err := ivRun(d, g, kind, f, vf, 0, map[string]any{"p": "x"}, binding+1); err != nil {
						t.Fatalf("counterpart t = binding+1: %v", err)
					}
					ivAssertStampedAt(t, "binding+1", before, ivRowTemporals(t, g, kind, f), 1, binding+1)
				})
			}
		}
	}
}

// 3 — gate and value, in that order of precedence: value first
// (ErrInvalidTxFrom), then privilege (ErrTxBackfillDisabled), then the order
// rule.
//
// Catches: a door without the AllowTxBackfill gate (the correction lands at t
// on a graph that never opted in); the gate checked after the write; the
// order check running before the privilege check (an order-violating t must
// still answer ErrTxBackfillDisabled); 0 read as "use the clock" (the plain
// door runs); no upper bound (a future t or MaxInt64 accepted); the gate
// checked before the value. Every refusal writes nothing.
func TestIntervalWithTx_GateAndValue(t *testing.T) {
	t.Parallel()
	for _, be := range txbBackends() {
		for _, d := range ivDoors() {
			for _, kind := range ivKinds {
				for _, gate := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/%s/%s/gate=%v", be.name, d.name, kind, gate), func(t *testing.T) {
						g := be.open(t, gate)
						f := ivPlainFixture(t, g)
						sh := ivShapes(f)[0]
						peek, _ := g.Temporal.PeekTx()
						type tc struct {
							name string
							at   types.Instant
							want error
						}
						cases := []tc{
							{"zero", 0, ErrInvalidTxFrom},
							{"negative", -1, ErrInvalidTxFrom},
							{"future", peek + 60_000, ErrInvalidTxFrom},
							{"MaxInt64", math.MaxInt64, ErrInvalidTxFrom},
						}
						if !gate {
							cases = append(cases,
								tc{"valid but gate off", peek, ErrTxBackfillDisabled},
								tc{"order-violating, gate off", 1, ErrTxBackfillDisabled})
						}
						for _, c := range cases {
							before := ivRowTemporals(t, g, kind, f)
							err := ivRun(d, g, kind, f, sh.vf, sh.vt, map[string]any{"p": "x"}, c.at)
							if !errors.Is(err, c.want) {
								t.Fatalf("[%s t=%d] err = %v; want %v", c.name, c.at, err, c.want)
							}
							if c.want == ErrInvalidTxFrom && errors.Is(err, ErrTxBackfillDisabled) {
								t.Fatalf("[%s] err = %v; the value error must win over the gate", c.name, err)
							}
							if c.want == ErrTxBackfillDisabled && errors.Is(err, ErrTxOrder) {
								t.Fatalf("[%s] err = %v; the gate must win over the order rule", c.name, err)
							}
							after := ivRowTemporals(t, g, kind, f)
							if len(after) != len(before) {
								t.Fatalf("[%s] refused but %d rows -> %d", c.name, len(before), len(after))
							}
							ivAssertOldRowsUnchanged(t, c.name, before, after)
						}
					})
				}
			}
		}
	}
}

// ivPlainFixture is ivFixture through the plain doors (a graph without the
// backfill gate): the same chain shape, stamped with the clock.
func ivPlainFixture(t *testing.T, g *Core) ivFix {
	t.Helper()
	ctx := context.Background()
	base := txbWall() - 2*txbHour
	vf0 := base - txbHour
	vf1 := vf0 + 600_000
	f := ivFix{base: base, vf0: vf0, vf1: vf1}
	n, err := g.Nodes.Add(ctx, []string{"Ref"}, map[string]any{"tkg_valid_from": vf0, "k": int64(1)})
	if err != nil {
		t.Fatalf("fixture Nodes.Add: %v", err)
	}
	if _, err := g.Nodes.Update(ctx, n.ID(), map[string]any{"tkg_valid_from": vf1, "k": int64(2)}); err != nil {
		t.Fatalf("fixture Nodes.Update: %v", err)
	}
	f.nid = n.ID()
	s, err := g.Nodes.Add(ctx, []string{"Ref"}, map[string]any{"tkg_valid_from": vf0 - 1000})
	if err != nil {
		t.Fatalf("fixture Nodes.Add: %v", err)
	}
	e, err := g.Nodes.Add(ctx, []string{"Ev"}, map[string]any{"tkg_valid_from": vf0 - 1000})
	if err != nil {
		t.Fatalf("fixture Nodes.Add: %v", err)
	}
	r, err := g.Rels.Add(ctx, "LINK", s, e, map[string]any{"tkg_valid_from": vf0, "k": int64(1)})
	if err != nil {
		t.Fatalf("fixture Rels.Add: %v", err)
	}
	if _, err := g.Rels.Update(ctx, r.ID(), map[string]any{"tkg_valid_from": vf1, "k": int64(2)}); err != nil {
		t.Fatalf("fixture Rels.Update: %v", err)
	}
	f.rid = r.ID()
	return f
}

// 4 — the commit clock is not advanced by the door.
//
// With the clock frozen ahead of the wall, every instant a door reserves moves
// the floor by one. The caller-instant door reserves one instant at its gate
// (the "not in the future" check) instead of the one the plain kernel stamps
// with, so over equal fixtures the WithTx door must move the floor exactly as
// far as its plain twin.
//
// Catches: a kernel that still reads the clock when a caller instant is given
// (stamps t but burns an instant: the commit clock advances on every replayed
// record), and a door that raises the floor for t.
func TestIntervalWithTx_CommitClockNotAdvanced(t *testing.T) {
	t.Parallel()
	plain := ivPlainDoors()
	for _, be := range txbBackends() {
		for i, d := range ivDoors() {
			p := plain[i]
			if p.name != d.name {
				t.Fatalf("door tables out of step: %s vs %s", p.name, d.name)
			}
			for _, kind := range ivKinds {
				t.Run(be.name+"/"+d.name+"/"+kind, func(t *testing.T) {
					g := be.open(t, true)
					f1 := ivFixture(t, g)
					f2 := ivFixture(t, g)
					sh := ivShapes(f1)[0]
					clk := &switchableClock{}
					g.SetClockForTest(t, clk.Now)
					clk.at.Store(int64(txbWall() + 10_000))
					// One reservation lifts the floor to the frozen reading, so
					// from here every reserved instant moves PeekTx by exactly one.
					if _, err := g.Temporal.NowTx(); err != nil {
						t.Fatalf("NowTx: %v", err)
					}
					p0, _ := g.Temporal.PeekTx()
					var err error
					if kind == "node" {
						err = p.node(g, f1.nid, sh.vf, sh.vt, map[string]any{"p": "x"})
					} else {
						err = p.rel(g, f1.rid, sh.vf, sh.vt, map[string]any{"p": "x"})
					}
					if err != nil {
						t.Fatalf("plain door: %v", err)
					}
					p1, _ := g.Temporal.PeekTx()
					if err := ivRun(d, g, kind, f2, sh.vf, sh.vt, map[string]any{"p": "x"}, f2.base+2000); err != nil {
						t.Fatalf("WithTx door: %v", err)
					}
					p2, _ := g.Temporal.PeekTx()
					if p2-p1 != p1-p0 {
						t.Fatalf("the WithTx door moved the commit clock by %d, its plain twin by %d; want equal (the gate's read replaces the stamp's)", p2-p1, p1-p0)
					}
				})
			}
		}
	}
}

// 5 — a hard-deleted entity is still refused.
//
// Catches: a caller-instant door (or the Batch/ingest pre-flight) that skips
// the deleted check and appends a correction after the tombstone, and a
// pre-flight that reports a deleted entity as plainly not found (errors.Is
// ErrEntityDeleted must hold on every door, as for the plain twins). An ID
// that never existed answers the not-found sentinel only. Nothing is written.
func TestIntervalWithTx_DeletedEntity(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for _, be := range txbBackends() {
		for _, d := range ivDoors() {
			for _, kind := range ivKinds {
				t.Run(be.name+"/"+d.name+"/"+kind, func(t *testing.T) {
					g := be.open(t, true)
					f := ivFixture(t, g)
					sh := ivShapes(f)[0]
					notFound := storepkg.ErrNodeNotFound
					var err error
					if kind == "node" {
						err = g.Nodes.Delete(ctx, f.nid)
					} else {
						notFound = storepkg.ErrRelNotFound
						err = g.Rels.Delete(ctx, f.rid)
					}
					if err != nil {
						t.Fatalf("Delete: %v", err)
					}
					before := ivRowTemporals(t, g, kind, f)
					at, _ := g.Temporal.NowTx()
					err = ivRun(d, g, kind, f, sh.vf, sh.vt, map[string]any{"p": "x"}, at)
					if !errors.Is(err, ErrEntityDeleted) || !errors.Is(err, notFound) {
						t.Fatalf("deleted entity err = %v; want ErrEntityDeleted and %v", err, notFound)
					}
					after := ivRowTemporals(t, g, kind, f)
					if len(after) != len(before) {
						t.Fatalf("refused but %d rows -> %d", len(before), len(after))
					}
					ivAssertOldRowsUnchanged(t, "deleted", before, after)
					never := ivFix{nid: 1, rid: 1}
					err = ivRun(d, g, kind, never, sh.vf, sh.vt, nil, at)
					if !errors.Is(err, notFound) || errors.Is(err, ErrEntityDeleted) {
						t.Fatalf("never-existed id err = %v; want %v only", err, notFound)
					}
				})
			}
		}
	}
}

// ivRender renders a row for the two-phase views.
func ivRender(v uint32, props map[string]any) string {
	keys := make([]string, 0, len(props))
	for k := range props {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	fmt.Fprintf(&b, "v%d{", v)
	for _, k := range keys {
		fmt.Fprintf(&b, "%s=%v,", k, props[k])
	}
	b.WriteString("}")
	return b.String()
}

// ivView renders everything the state and record doors answer about the
// fixture entity at pin: NodeAtTx / RelAtTx and the generic ByLabel / ByType
// with {ValidAt, TxAt} on a valid-time grid, the as-of door, the {TxPin}
// door, and the effective timeline (whose segment at every grid instant must
// hold the row the point door answers — checked here).
func ivView(t *testing.T, g *Core, kind string, f ivFix, pin types.Instant, grid []types.Instant) string {
	t.Helper()
	var b strings.Builder
	pointAt := map[types.Instant]string{}
	for _, v := range grid {
		var s string
		if kind == "node" {
			n, err := g.Temporal.NodeAtTx(f.nid, v, pin)
			switch {
			case errors.Is(err, storepkg.ErrNoVersionValidAt):
				s = "none"
			case err != nil:
				t.Fatalf("NodeAtTx(%d,%d): %v", v, pin, err)
			default:
				s = ivRender(n.Version(), n.PropertiesMap())
			}
			ns, err := g.Nodes.ByLabel("Ref", storepkg.QueryOpts{ValidAt: v, TxAt: pin})
			if err != nil {
				t.Fatalf("ByLabel: %v", err)
			}
			gen := "none"
			for _, n := range ns {
				if n.ID() == f.nid {
					gen = ivRender(n.Version(), n.PropertiesMap())
				}
			}
			if gen != s {
				t.Fatalf("pin %d valid %d: NodeAtTx %s, ByLabel{ValidAt,TxAt} %s (rule 17)", pin, v, s, gen)
			}
		} else {
			r, err := g.Temporal.RelAtTx(f.rid, v, pin)
			switch {
			case errors.Is(err, storepkg.ErrNoVersionValidAt):
				s = "none"
			case err != nil:
				t.Fatalf("RelAtTx(%d,%d): %v", v, pin, err)
			default:
				s = ivRender(r.Version(), r.PropertiesMap())
			}
			rs, err := g.Rels.ByType("LINK", storepkg.QueryOpts{ValidAt: v, TxAt: pin})
			if err != nil {
				t.Fatalf("ByType: %v", err)
			}
			gen := "none"
			for _, r := range rs {
				if r.ID() == f.rid {
					gen = ivRender(r.Version(), r.PropertiesMap())
				}
			}
			if gen != s {
				t.Fatalf("pin %d valid %d: RelAtTx %s, ByType{ValidAt,TxAt} %s (rule 17)", pin, v, s, gen)
			}
		}
		pointAt[v] = s
		fmt.Fprintf(&b, "at(%d)=%s ", v-f.vf0, s)
	}
	if kind == "node" {
		if n, err := g.Temporal.NodeAsOf(f.nid, pin); err == nil {
			fmt.Fprintf(&b, "asof=%s ", ivRender(n.Version(), n.PropertiesMap()))
		} else if !errors.Is(err, ErrNoVersionAsOf) {
			t.Fatalf("NodeAsOf: %v", err)
		}
		ns, err := g.Nodes.ByLabel("Ref", storepkg.QueryOpts{TxPin: pin})
		if err != nil {
			t.Fatalf("ByLabel{TxPin}: %v", err)
		}
		for _, n := range ns {
			if n.ID() == f.nid {
				fmt.Fprintf(&b, "pin=v%d ", n.Version())
			}
		}
		segs, err := g.Temporal.NodeEffectiveTimeline(f.nid, pin)
		if err != nil {
			t.Fatalf("NodeEffectiveTimeline: %v", err)
		}
		for _, s := range segs {
			fmt.Fprintf(&b, "seg[%d,%d)=%s ", s.ValidFrom-f.vf0, s.ValidTo-f.vf0, ivRender(s.Node.Version(), s.Node.PropertiesMap()))
		}
		for v, want := range pointAt {
			got := "none"
			for _, s := range segs {
				if s.ValidFrom <= v && (s.ValidTo == 0 || v < s.ValidTo) {
					got = ivRender(s.Node.Version(), s.Node.PropertiesMap())
				}
			}
			if got != want {
				t.Fatalf("pin %d valid %d: effective timeline %s, NodeAtTx %s", pin, v, got, want)
			}
		}
		return b.String()
	}
	if r, err := g.Temporal.RelAsOf(f.rid, pin); err == nil {
		fmt.Fprintf(&b, "asof=%s ", ivRender(r.Version(), r.PropertiesMap()))
	} else if !errors.Is(err, ErrNoVersionAsOf) {
		t.Fatalf("RelAsOf: %v", err)
	}
	rs, err := g.Rels.ByType("LINK", storepkg.QueryOpts{TxPin: pin})
	if err != nil {
		t.Fatalf("ByType{TxPin}: %v", err)
	}
	for _, r := range rs {
		if r.ID() == f.rid {
			fmt.Fprintf(&b, "pin=v%d ", r.Version())
		}
	}
	segs, err := g.Temporal.RelEffectiveTimeline(f.rid, pin)
	if err != nil {
		t.Fatalf("RelEffectiveTimeline: %v", err)
	}
	for _, s := range segs {
		fmt.Fprintf(&b, "seg[%d,%d)=%s ", s.ValidFrom-f.vf0, s.ValidTo-f.vf0, ivRender(s.Rel.Version(), s.Rel.PropertiesMap()))
	}
	for v, want := range pointAt {
		got := "none"
		for _, s := range segs {
			if s.ValidFrom <= v && (s.ValidTo == 0 || v < s.ValidTo) {
				got = ivRender(s.Rel.Version(), s.Rel.PropertiesMap())
			}
		}
		if got != want {
			t.Fatalf("pin %d valid %d: effective timeline %s, RelAtTx %s", pin, v, got, want)
		}
	}
	return b.String()
}

// ivPropAt reads property key of the kind's entity valid at v as believed at pin.
func ivPropAt(t *testing.T, g *Core, kind string, f ivFix, v, pin types.Instant, key string) any {
	t.Helper()
	var props map[string]any
	if kind == "node" {
		n, err := g.Temporal.NodeAtTx(f.nid, v, pin)
		if err != nil {
			t.Fatalf("NodeAtTx(%d,%d): %v", v, pin, err)
		}
		props = n.PropertiesMap()
	} else {
		r, err := g.Temporal.RelAtTx(f.rid, v, pin)
		if err != nil {
			t.Fatalf("RelAtTx(%d,%d): %v", v, pin, err)
		}
		props = r.PropertiesMap()
	}
	return props[key]
}

// 7 — the sigma-tkgd shape: a burst fact grows [vs,ve) -> [vs,ve') -> [vs,ve”)
// at the record instants t1 < t2 of a replayed stream.
//
// Two-phase (rules 15, 17): before each growth the views at pin t_i - 1 are
// recorded (ivView: AtTx and the generic ByLabel/ByType{ValidAt,TxAt} on a grid,
// the as-of and {TxPin} doors, the effective timeline, which must agree with
// AtTx); after it they are unchanged, pin t2 answers what the newest pin
// answers, and every pin keeps its answer after a later plain correction.
//
// Catches: a door that stamps the growth at the clock (at pin t1 the fact
// still ends at ve; the replayed past differs from the live past), a door that
// stamps t2's growth at t1 or the reverse, an off-by-one at the pin (t_i - 1
// sees the growth), a door that rewrites stored rows, doors that disagree on
// the corrected rows, and a correction that breaks the hash chain
// (Verify*Chain). The queue doors run each growth in a unit of its own (one op
// per entity per unit).
func TestIntervalWithTx_BurstGrowthReplay(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for _, be := range txbBackends() {
		for _, d := range ivDoors() {
			for _, kind := range ivKinds {
				t.Run(be.name+"/"+d.name+"/"+kind, func(t *testing.T) {
					g := be.open(t, true)
					t0 := txbWall() - 2*txbHour
					vs := t0 - 30*60_000
					ends := []types.Instant{vs + 60_000, vs + 120_000, vs + 180_000}
					props := map[string]any{"tkg_valid_from": vs, "tkg_valid_to": ends[0], "burst": "b1"}
					f := ivFix{vf0: vs}
					grid := []types.Instant{vs + 10, ends[0] - 1, ends[0], ends[1] - 1, ends[1], ends[2] - 1, ends[2]}
					if kind == "node" {
						n, err := g.Nodes.AddWithTx(ctx, []string{"Ref"}, props, t0)
						if err != nil {
							t.Fatalf("AddWithTx: %v", err)
						}
						f.nid = n.ID()
					} else {
						s := txbBackfillNodeAt(t, g, "Ref", t0, map[string]any{"tkg_valid_from": vs - 1000})
						e := txbBackfillNodeAt(t, g, "Ev", t0, map[string]any{"tkg_valid_from": vs - 1000})
						r, err := g.Rels.AddWithTx(ctx, "LINK", s, e, props, t0)
						if err != nil {
							t.Fatalf("AddWithTx: %v", err)
						}
						f.rid = r.ID()
					}
					present := func(v, pin types.Instant) bool {
						t.Helper()
						var err error
						if kind == "node" {
							_, err = g.Temporal.NodeAtTx(f.nid, v, pin)
						} else {
							_, err = g.Temporal.RelAtTx(f.rid, v, pin)
						}
						if err != nil && !errors.Is(err, storepkg.ErrNoVersionValidAt) {
							t.Fatalf("AtTx(%d,%d): %v", v, pin, err)
						}
						return err == nil
					}
					lastEnd := func(pin types.Instant) (types.Instant, types.Instant) {
						t.Helper()
						var first, last types.Instant
						if kind == "node" {
							segs, err := g.Temporal.NodeEffectiveTimeline(f.nid, pin)
							if err != nil || len(segs) == 0 {
								t.Fatalf("NodeEffectiveTimeline(%d) = %v, %v", pin, segs, err)
							}
							for i := 1; i < len(segs); i++ {
								if segs[i].ValidFrom != segs[i-1].ValidTo {
									t.Fatalf("pin %d: timeline has a gap: %+v", pin, segs)
								}
							}
							first, last = segs[0].ValidFrom, segs[len(segs)-1].ValidTo
						} else {
							segs, err := g.Temporal.RelEffectiveTimeline(f.rid, pin)
							if err != nil || len(segs) == 0 {
								t.Fatalf("RelEffectiveTimeline(%d) = %v, %v", pin, segs, err)
							}
							for i := 1; i < len(segs); i++ {
								if segs[i].ValidFrom != segs[i-1].ValidTo {
									t.Fatalf("pin %d: timeline has a gap: %+v", pin, segs)
								}
							}
							first, last = segs[0].ValidFrom, segs[len(segs)-1].ValidTo
						}
						return first, last
					}
					ts := []types.Instant{t0, t0 + 1000, t0 + 2000}
					views := make([]string, len(ts))
					for i := 1; i < len(ts); i++ {
						before := ivRowTemporals(t, g, kind, f)
						prev := ivView(t, g, kind, f, ts[i]-1, grid)
						if err := ivRun(d, g, kind, f, vs, ends[i], nil, ts[i]); err != nil {
							t.Fatalf("growth %d at t%d: %v", i, i, err)
						}
						if got := ivView(t, g, kind, f, ts[i]-1, grid); got != prev {
							t.Fatalf("pin t%d-1 changed by the growth at t%d:\n before %s\n after  %s", i, i, prev, got)
						}
						app := ivAppended(before, ivRowTemporals(t, g, kind, f))
						if len(app) == 0 {
							t.Fatalf("growth %d appended nothing", i)
						}
						for v, tm := range app {
							if tm.TxFrom != ts[i] || tm.UpdatedAt != ts[i] {
								t.Fatalf("growth %d row v%d TxFrom=%d UpdatedAt=%d; want the record instant t%d = %d", i, v, tm.TxFrom, tm.UpdatedAt, i, ts[i])
							}
						}
					}
					for i, pin := range ts {
						views[i] = ivView(t, g, kind, f, pin, grid)
					}
					nowPin, _ := g.Temporal.NowTx()
					if now := ivView(t, g, kind, f, nowPin, grid); views[2] != now {
						t.Fatalf("pin t2 does not answer the growth recorded at t2:\n pin t2  %s\n pin now %s", views[2], now)
					}
					var err error
					if kind == "node" { // a later plain correction, at the clock
						_, err = g.Temporal.SetNodeVersionInterval(ctx, f.nid, vs, ends[2]+60_000, nil)
					} else {
						_, err = g.Temporal.SetRelVersionInterval(ctx, f.rid, vs, ends[2]+60_000, nil)
					}
					if err != nil {
						t.Fatalf("later plain correction: %v", err)
					}
					for i, pin := range ts {
						if got := ivView(t, g, kind, f, pin, grid); got != views[i] {
							t.Fatalf("pin t%d forgotten after a later write:\n then %s\n now  %s", i, views[i], got)
						}
						for _, p := range []types.Instant{pin, pin + 999} {
							if !present(vs+10, p) || !present(ends[i]-1, p) {
								t.Fatalf("pin %d (t%d): the fact is not valid inside [vs, ve%d)", p, i, i)
							}
							if present(ends[i], p) {
								t.Fatalf("pin %d (t%d): the fact is valid at ve%d = %d; the growth recorded at a later instant leaked into this pin", p, i, i, ends[i])
							}
							if first, last := lastEnd(p); first != vs || last != ends[i] {
								t.Fatalf("pin %d (t%d): effective timeline [%d,%d); want [%d,%d)", p, i, first, last, vs, ends[i])
							}
						}
					}
					var ok bool
					if kind == "node" {
						ok, err = g.Hash.VerifyNodeChain(f.nid)
					} else {
						ok, err = g.Hash.VerifyRelChain(f.rid)
					}
					if err != nil || !ok {
						t.Fatalf("Verify%sChain = %v, %v; want a valid chain", kind, ok, err)
					}
				})
			}
		}
	}
}
