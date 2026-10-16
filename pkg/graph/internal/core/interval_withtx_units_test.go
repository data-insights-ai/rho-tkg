package core

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Units, rollback, the as-of cache and replicas for the caller-instant interval
// corrections (see interval_withtx_test.go for the door table and fixture).

// 8 — the GraphTx twins against rollback and against the standalone door.
//
// Catches: a twin that skips the pre-mutation snapshot (Rollback leaves the
// appended rows, or restores the current row without its history), a twin
// that drops t inside the tx (stamps the clock), and a twin that diverges from
// the standalone door (the committed bytes differ from the standalone door's
// on the same fixture: another instant, another version, another hash).
func TestIntervalWithTx_TxRollbackEquiv(t *testing.T) {
	if testing.Short() && !isRaceEnabled() {
		t.Skip("long tier: release gate")
	}
	t.Parallel()
	ctx := context.Background()
	for _, be := range txbBackends() {
		for _, kind := range ivKinds {
			t.Run(be.name+"/"+kind+"/rollback", func(t *testing.T) {
				g := be.open(t, true)
				f := ivFixture(t, g)
				other := txbAddNode(t, g, "Ref", map[string]any{"k": int64(1)})
				sh := ivShapes(f)[0]
				at := f.base + 2000
				grid := []types.Instant{f.vf0, sh.vf, f.vf1, sh.vt, f.vf1 + 10*txbHour}
				body0 := exportBody(t, g)
				views0 := ivView(t, g, kind, f, at-1, grid) + ivView(t, g, kind, f, at, grid)
				before := ivRowTemporals(t, g, kind, f)
				tx, err := g.BeginTx()
				if err != nil {
					t.Fatalf("BeginTx: %v", err)
				}
				if kind == "node" {
					_, err = tx.SetNodeVersionIntervalWithTx(f.nid, sh.vf, sh.vt, map[string]any{"p": "x"}, at)
				} else {
					_, err = tx.SetRelVersionIntervalWithTx(f.rid, sh.vf, sh.vt, map[string]any{"p": "x"}, at)
				}
				if err != nil {
					_ = tx.Rollback()
					t.Fatalf("twin: %v", err)
				}
				if _, err := tx.UpdateNode(other.ID(), map[string]any{"k": int64(2)}); err != nil {
					_ = tx.Rollback()
					t.Fatalf("UpdateNode in the tx: %v", err)
				}
				// Inside the tx the rows landed at t (write-through).
				inside := ivRowTemporals(t, g, kind, f)
				for v, tm := range ivAppended(before, inside) {
					if tm.TxFrom != at {
						_ = tx.Rollback()
						t.Fatalf("inside the tx appended v%d TxFrom = %d; want t = %d", v, tm.TxFrom, at)
					}
				}
				if len(inside) != len(before)+sh.appended {
					_ = tx.Rollback()
					t.Fatalf("inside the tx %d rows; want %d", len(inside), len(before)+sh.appended)
				}
				if err := tx.Rollback(); err != nil {
					t.Fatalf("Rollback: %v", err)
				}
				if body := exportBody(t, g); string(body) != string(body0) {
					t.Fatalf("rollback left the export changed (%d -> %d body bytes)", len(body0), len(body))
				}
				if views := ivView(t, g, kind, f, at-1, grid) + ivView(t, g, kind, f, at, grid); views != views0 {
					t.Fatalf("rollback changed the pinned reads:\n before %s\n after  %s", views0, views)
				}
				if n, err := g.Nodes.Get(ctx, other.ID()); err != nil || n.PropertiesMap()["k"] != int64(1) {
					t.Fatalf("rollback left the other op: %v, %v", n, err)
				}
			})
			t.Run(be.name+"/"+kind+"/commit_equals_standalone", func(t *testing.T) {
				a := be.open(t, true)
				f := ivFixture(t, a)
				b := be.open(t, true)
				fullExportInto(t, a, b)
				assertEquivalent(t, a, b, "fixture copy")
				sh := ivShapes(f)[0]
				at := f.base + 2000
				if err := ivRun(ivDoors()[0], a, kind, f, sh.vf, sh.vt, map[string]any{"p": "x"}, at); err != nil {
					t.Fatalf("standalone: %v", err)
				}
				if err := txbTxDo(t, b, func(tx *GraphTx) error {
					var err error
					if kind == "node" {
						_, err = tx.SetNodeVersionIntervalWithTx(f.nid, sh.vf, sh.vt, map[string]any{"p": "x"}, at)
					} else {
						_, err = tx.SetRelVersionIntervalWithTx(f.rid, sh.vf, sh.vt, map[string]any{"p": "x"}, at)
					}
					return err
				}); err != nil {
					t.Fatalf("twin: %v", err)
				}
				assertEquivalent(t, a, b, "committed twin vs standalone door")
			})
		}
	}
}

// 9 — a Batch or ingest unit with a caller-instant interval correction is
// pre-flighted as a whole and refused as a whole, and the correction must be
// the only op of the unit on its entity.
//
// Catches: a unit applied op by op (the plain op on another entity commits
// while the correction is refused: a partial past no later write at an
// earlier t can repair); a pre-flight that does not count the corrections
// (two ops on one entity: the plain op is stamped at the clock above t, or the
// second correction lands below the first); a node delete in the unit whose
// cascade reaches the corrected relationship (or the corrected node itself);
// a pre-flight that lets a correction on a deleted entity through to fail
// alone. Every refused unit writes nothing; the counterpart unit (a plain op
// on another entity plus the correction) applies both, the correction at t.
func TestIntervalWithTx_BatchIngestWholeUnit(t *testing.T) {
	if testing.Short() && !isRaceEnabled() {
		t.Skip("long tier: release gate")
	}
	t.Parallel()
	ctx := context.Background()
	for _, be := range txbBackends() {
		for _, m := range ivQueueModes() {
			for _, kind := range ivKinds {
				t.Run(be.name+"/"+m.name+"/"+kind, func(t *testing.T) {
					g := be.open(t, true)
					f := ivFixture(t, g)
					sh := ivShapes(f)[0]
					x := txbAddNode(t, g, "Ref", map[string]any{"k": int64(1)})
					p := map[string]any{"p": "x"}
					correct := func(q ivQueue, at types.Instant) error {
						if kind == "node" {
							return q.SetNodeVersionIntervalWithTx(f.nid, sh.vf, sh.vt, p, at)
						}
						return q.SetRelVersionIntervalWithTx(f.rid, sh.vf, sh.vt, p, at)
					}
					var deleted ivFix
					{
						d := ivFixture(t, g)
						if err := g.Nodes.Delete(ctx, d.nid); err != nil {
							t.Fatalf("Delete: %v", err)
						}
						if err := g.Rels.Delete(ctx, d.rid); err != nil {
							t.Fatalf("Delete: %v", err)
						}
						deleted = d
					}
					cascadedBy := f.nid // a node delete that reaches the corrected entity
					if kind == "rel" {
						cascadedBy = txbChain(t, g, f.rid)[0].StartNodeID()
					}
					good := f.base + 2000
					cases := []struct {
						name  string
						queue func(q ivQueue) error
						want  error
					}{
						{"order-refused correction + plain op elsewhere", func(q ivQueue) error {
							if err := q.UpdateNode(x.ID(), map[string]any{"k": int64(9)}); err != nil {
								return err
							}
							return correct(q, f.base+1000)
						}, ErrTxOrder},
						{"correction + plain update of the same entity", func(q ivQueue) error {
							if err := correct(q, good); err != nil {
								return err
							}
							if kind == "node" {
								return q.UpdateNode(f.nid, map[string]any{"k": int64(5)})
							}
							return q.UpdateRelationship(f.rid, map[string]any{"k": int64(5)})
						}, ErrTxOrder},
						{"two corrections of the same entity", func(q ivQueue) error {
							if err := correct(q, good); err != nil {
								return err
							}
							return correct(q, good+1000)
						}, ErrTxOrder},
						{"correction + plain correction of the same entity", func(q ivQueue) error {
							if err := correct(q, good); err != nil {
								return err
							}
							if kind == "node" {
								return q.SetNodeVersionInterval(f.nid, sh.vf, sh.vt, p)
							}
							return q.SetRelVersionInterval(f.rid, sh.vf, sh.vt, p)
						}, ErrTxOrder},
						{"correction + a node delete that cascades into it", func(q ivQueue) error {
							if err := correct(q, good); err != nil {
								return err
							}
							return q.DeleteNode(cascadedBy)
						}, ErrTxOrder},
						{"correction of a deleted entity + plain op elsewhere", func(q ivQueue) error {
							if err := q.UpdateNode(x.ID(), map[string]any{"k": int64(9)}); err != nil {
								return err
							}
							if kind == "node" {
								return q.SetNodeVersionIntervalWithTx(deleted.nid, sh.vf, sh.vt, p, good)
							}
							return q.SetRelVersionIntervalWithTx(deleted.rid, sh.vf, sh.vt, p, good)
						}, ErrEntityDeleted},
					}
					for _, c := range cases {
						before := ivRowTemporals(t, g, kind, f)
						bx := snapNode(t, g, x.ID())
						bd := snapNode(t, g, cascadedBy)
						err := m.apply(g, c.queue)
						if !errors.Is(err, c.want) {
							t.Fatalf("[%s] err = %v; want %v", c.name, err, c.want)
						}
						after := ivRowTemporals(t, g, kind, f)
						if len(after) != len(before) {
							t.Fatalf("[%s] refused unit but the entity went %d rows -> %d", c.name, len(before), len(after))
						}
						ivAssertOldRowsUnchanged(t, c.name, before, after)
						assertNodeUnchanged(t, c.name+" (other entity)", bx, snapNode(t, g, x.ID()))
						assertNodeUnchanged(t, c.name+" (delete target)", bd, snapNode(t, g, cascadedBy))
					}
					// Counterpart: a unit with a plain op elsewhere and the
					// correction applies both.
					before := ivRowTemporals(t, g, kind, f)
					if err := m.apply(g, func(q ivQueue) error {
						if err := q.UpdateNode(x.ID(), map[string]any{"k": int64(9)}); err != nil {
							return err
						}
						return correct(q, good)
					}); err != nil {
						t.Fatalf("counterpart unit: %v", err)
					}
					ivAssertStampedAt(t, "counterpart", before, ivRowTemporals(t, g, kind, f), sh.appended, good)
					if n, err := g.Nodes.Get(ctx, x.ID()); err != nil || n.PropertiesMap()["k"] != int64(9) {
						t.Fatalf("counterpart: the plain op did not apply: %v, %v", n, err)
					}
				})
			}
		}
	}
}

// 10 — strong-mode ingest: a group carrying a caller-instant correction is
// applied in a commit unit of its own.
//
// Catches: an applier that coalesces such a group with sibling groups (its
// whole-unit refusal then fails the siblings, or a later sibling's correction
// of the same entity meets the refused one in one unit).
func TestIntervalWithTx_IngestStrongGroupAlone(t *testing.T) {
	if testing.Short() && !isRaceEnabled() {
		t.Skip("long tier: release gate")
	}
	t.Parallel()
	for _, be := range txbBackends() {
		for _, kind := range ivKinds {
			t.Run(be.name+"/"+kind, func(t *testing.T) {
				g := be.open(t, true)
				f := ivFixture(t, g)
				sh := ivShapes(f)[0]
				x := txbAddNode(t, g, "Ref", map[string]any{"k": int64(1)})
				ap, err := g.ensureIngestApplier(0)
				if err != nil {
					t.Fatalf("ensureIngestApplier: %v", err)
				}
				group := func(queue func(q ivQueue) error) *ingestGroup {
					b, err := NewBatchBuilder(g)
					if err != nil {
						t.Fatalf("NewBatchBuilder: %v", err)
					}
					if err := queue(b); err != nil {
						t.Fatalf("queue: %v", err)
					}
					gr := b.takeIngestGroup()
					gr.result = make(chan error, 1)
					return gr
				}
				correct := func(at types.Instant) func(q ivQueue) error {
					return func(q ivQueue) error {
						if kind == "node" {
							return q.SetNodeVersionIntervalWithTx(f.nid, sh.vf, sh.vt, map[string]any{"p": int64(at)}, at)
						}
						return q.SetRelVersionIntervalWithTx(f.rid, sh.vf, sh.vt, map[string]any{"p": int64(at)}, at)
					}
				}
				good := group(func(q ivQueue) error { return q.UpdateNode(x.ID(), map[string]any{"k": int64(9)}) })
				bad := group(correct(f.base + 1000))
				good2 := group(correct(f.base + 2000))
				before := ivRowTemporals(t, g, kind, f)
				ap.applyCommitGroup([]*ingestGroup{good, bad, good2})
				if err := <-bad.result; !errors.Is(err, ErrTxOrder) {
					t.Fatalf("refused group err = %v; want ErrTxOrder", err)
				}
				if err := <-good.result; err != nil {
					t.Fatalf("sibling group failed with the refused group: %v", err)
				}
				if err := <-good2.result; err != nil {
					t.Fatalf("later sibling group failed with the refused group: %v", err)
				}
				ivAssertStampedAt(t, "good2", before, ivRowTemporals(t, g, kind, f), sh.appended, f.base+2000)
				if n, err := g.Nodes.Get(context.Background(), x.ID()); err != nil || n.PropertiesMap()["k"] != int64(9) {
					t.Fatalf("sibling group not applied: %v, %v", n, err)
				}
			})
		}
	}
}

// 11 — the as-of column cache on the primary (the W2 pattern of
// TestTxBackfillNode_PrimaryCacheStale): a node corrected at t <= P must show
// the correction at a pin P whose column a reader built while the door ran.
//
// Catches: a door that never reports its past-dated write (the warm column
// keeps the old k at P); a report placed BEFORE the store write (the build the
// hook interleaves after the gate is cached under the bumped epoch with the
// pre-write belief); a Batch/ingest applier whose report folds the create and
// update instants but not the corrections'. (The relationship doors share the
// node doors' report code: a rel-only report test would be a subset.)
func TestIntervalWithTx_PrimaryCacheStale(t *testing.T) {
	if testing.Short() && !isRaceEnabled() {
		t.Skip("long tier: release gate")
	}
	ctx := context.Background()
	for _, be := range pastDatedBackends() {
		for _, d := range ivDoors() {
			t.Run(be.name+"/"+d.name, func(t *testing.T) {
				g := be.open(t, Config{AllowTxBackfill: true}, false)
				base := txbWall() - 2*txbHour
				vf := base - txbHour
				a, err := g.Nodes.AddWithTx(ctx, []string{pastDatedLabel}, map[string]any{"k": int64(1), "tkg_valid_from": vf}, base)
				if err != nil {
					t.Fatalf("seed a: %v", err)
				}
				b, err := g.Nodes.Add(ctx, []string{pastDatedLabel}, map[string]any{"k": int64(2)})
				if err != nil {
					t.Fatalf("seed b: %v", err)
				}
				pin, err := g.Temporal.NowTx()
				if err != nil {
					t.Fatalf("NowTx: %v", err)
				}
				at := base + 1000
				warmAsOf(t, g, pin)
				assertMembers(t, "warm pin", asOfMembers(t, g, pin), a.ID(), b.ID())
				builds := 0
				g.backfillGateHook = func() {
					done := make(chan error, 1)
					go func() {
						_, err := g.buildAsOfColumns(pastDatedLabel, []string{"k"}, pin)
						done <- err
					}()
					if err := <-done; err != nil {
						t.Errorf("interleaved build: %v", err)
					}
					builds++
				}
				err = d.node(g, a.ID(), vf+1000, 0, map[string]any{"k": int64(7)}, at)
				g.backfillGateHook = nil
				if err != nil {
					t.Fatalf("door: %v", err)
				}
				if builds == 0 {
					t.Fatal("the door never reached the backfill gate (hook did not run)")
				}
				if v := txbAsOfValues(t, g, pin)[a.ID()]; v != int64(7) {
					t.Fatalf("k at pin after the correction at t = %v; want 7", v)
				}
				if v := txbAsOfValues(t, g, at-1)[a.ID()]; v != int64(1) {
					t.Fatalf("k at t-1 = %v; want the uncorrected 1", v)
				}
			})
		}
	}
}

// 12 — a replica reproduces the stamps.
//
// Catches: a primary door that stamps something else than what its change
// records carry, and a replica apply path that re-stamps the appended rows or
// does not feed their TxFrom to its as-of cache (a warm replica column at
// P >= t keeps the uncorrected k).
func TestIntervalWithTx_ReplicaReproducesStamps(t *testing.T) {
	if testing.Short() && !isRaceEnabled() {
		t.Skip("long tier: release gate")
	}
	ctx := context.Background()
	for _, be := range pastDatedBackends() {
		for _, d := range ivDoors() {
			for _, kind := range ivKinds {
				t.Run(be.name+"/"+d.name+"/"+kind, func(t *testing.T) {
					primary := be.open(t, Config{SnowflakeNodeID: 0, AllowTxBackfill: true}, true)
					replica := be.open(t, Config{SnowflakeNodeID: 1, ReadOnlyReplica: true, ReplicationSource: primary.Repl}, false)
					f := ivFixture(t, primary)
					probe, err := primary.Nodes.Add(ctx, []string{"Ref"}, map[string]any{"k": int64(0)})
					if err != nil {
						t.Fatalf("probe: %v", err)
					}
					pin := txFromStamp(t, probe.Temporal())
					feed := changeFeed(t, primary)
					applyAll(t, replica, feed)
					kAt := func(g *Core, p types.Instant) any {
						t.Helper()
						var got any
						_, ok, err := g.Nodes.ForEachDocValuesAsOf("Ref", []string{"k"}, p, func(id types.NodeID, vals []any, present []bool) bool {
							if id == f.nid && present[0] {
								got = vals[0]
							}
							return true
						})
						if err != nil || !ok {
							t.Fatalf("ForEachDocValuesAsOf(%d): ok=%v err=%v", p, ok, err)
						}
						return got
					}
					if k := kAt(replica, pin); k != int64(2) {
						t.Fatalf("replica warm k at pin = %v; want 2", k)
					}
					at := f.base + 2000
					sh := ivShapes(f)[1] // open: the slot row carries the patch
					if err := ivRun(d, primary, kind, f, sh.vf, sh.vt, map[string]any{"k": int64(7)}, at); err != nil {
						t.Fatalf("primary door: %v", err)
					}
					applyAll(t, replica, changeFeed(t, primary)[len(feed):])
					pRows, rRows := ivRowTemporals(t, primary, kind, f), ivRowTemporals(t, replica, kind, f)
					if !reflect.DeepEqual(pRows, rRows) {
						t.Fatalf("replica rows differ from the primary's:\n primary %+v\n replica %+v", pRows, rRows)
					}
					grid := []types.Instant{f.vf0, sh.vf, f.vf1, f.vf1 + 10*txbHour}
					for _, p := range []types.Instant{at - 1, at} {
						if pv, rv := ivView(t, primary, kind, f, p, grid), ivView(t, replica, kind, f, p, grid); pv != rv {
							t.Fatalf("pin %d: replica answers differ:\n primary %s\n replica %s", p, pv, rv)
						}
					}
					if kind == "node" {
						if k := kAt(replica, pin); k != int64(7) {
							t.Fatalf("replica k at pin after the correction at t <= pin = %v; want 7", k)
						}
						if k := kAt(replica, at-1); k != int64(2) {
							t.Fatalf("replica k at t-1 = %v; want 2", k)
						}
					}
				})
			}
		}
	}
}
