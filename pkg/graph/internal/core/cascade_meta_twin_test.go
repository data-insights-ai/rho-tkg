package core

import (
	"context"
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// newMetaTwin opens twin graphs on be: A plans a correction on a head miss
// on skeletons, B on the whole history (cascadeMetaOff, the code before
// CASCADEMETA). Neither plans from a head entry or the current row
// (headWriteOff), so every correction is a miss. The clocks are one fixed
// instant that only the writes step, so the twins stamp alike.
func newMetaTwin(t *testing.T, be r10Backend) *headTwin {
	tw := &headTwin{t: t, rng: rand.New(rand.NewPCG(0xca5c, 0x3e7a)), ops: map[string]int{}}
	fixed := time.Now().Add(time.Hour)
	open := func() *Core {
		cfg := be.cfg()
		cfg.AllowTxBackfill = true
		g, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = g.Close() })
		g.SetClockForTest(t, func() time.Time { return fixed })
		g.headWriteOff = true
		return g
	}
	tw.a, tw.b = open(), open()
	tw.b.cascadeMetaOff = true
	return tw
}

// metaDo runs fn on both twins (the errors must match; wantErr says whether
// there is one), then checks that A planned exactly meta corrections on
// skeletons (0 where the store has no skeleton door), B none, and that the
// twins hold byte-identical rows and timelines.
func (tw *headTwin) metaDo(name string, meta uint64, wantErr bool, fn func(g *Core) error) {
	tw.t.Helper()
	tw.last = name
	before := tw.a.metaWrites.Load()
	err := tw.both(fn)
	if (err != nil) != wantErr {
		tw.t.Fatalf("%s: error %v, want error %v", name, err, wantErr)
	}
	if tw.a.temporalMetaHistory == nil {
		meta = 0
	}
	if got := tw.a.metaWrites.Load() - before; got != meta {
		tw.t.Fatalf("%s: A planned %d corrections on skeletons, want %d", name, got, meta)
	}
	if n := tw.b.metaWrites.Load(); n != 0 {
		tw.t.Fatalf("%s: B (skeletons off) planned %d corrections on skeletons", name, n)
	}
	tw.check(name)
}

// metaEntities imports a node and a relationship (between two further
// nodes) with the same IDs on both twins, valid from 1000, each corrected
// three times; end ("delete", "retract" or "") then ends both and imports
// them again from 3000, corrected three times more.
func (tw *headTwin) metaEntities(end string) (types.NodeID, types.RelID) {
	ctx := context.Background()
	first := len(tw.nodes)
	for i := range 3 {
		id := tw.a.nextNodeID()
		vf := int64(10)
		if i == 2 {
			vf = 1000
		}
		tw.metaDo("import node", 0, false, func(g *Core) error {
			_, err := g.Nodes.Import(ctx, id, []string{"H"}, map[string]any{"tkg_valid_from": vf, "k": int64(0), "keep": "first"})
			return err
		})
		tw.nodes = append(tw.nodes, id)
	}
	nid := tw.nodes[first+2]
	rid := tw.a.nextRelID()
	tw.metaDo("import rel", 0, false, func(g *Core) error {
		return tw.importRel(g, rid, first, map[string]any{"tkg_valid_from": int64(1000), "k": int64(0), "keep": "first"})
	})
	tw.rels = append(tw.rels, rid)
	correct := func(base types.Instant, k int64) {
		for i := range 3 {
			vf := base + types.Instant(100*i)
			p := map[string]any{"k": k + int64(i)}
			tw.metaDo(fmt.Sprintf("rel [%d,%d)", vf, vf+500), 1, false, func(g *Core) error {
				_, err := g.Temporal.SetRelVersionInterval(ctx, rid, vf, vf+500, p)
				return err
			})
			tw.metaDo(fmt.Sprintf("node [%d,%d)", vf, vf+500), 1, false, func(g *Core) error {
				_, err := g.Temporal.SetNodeVersionInterval(ctx, nid, vf, vf+500, p)
				return err
			})
		}
	}
	correct(1000, 1)
	if end == "" {
		return nid, rid
	}
	tw.metaDo(end, 0, false, func(g *Core) error {
		if end == "retract" {
			if err := g.Rels.Retract(ctx, rid); err != nil {
				return err
			}
			return g.Nodes.Retract(ctx, nid)
		}
		if err := g.Rels.Delete(ctx, rid); err != nil {
			return err
		}
		return g.Nodes.Delete(ctx, nid)
	})
	tw.metaDo("re-import", 0, false, func(g *Core) error {
		if _, err := g.Nodes.Import(ctx, nid, []string{"H"}, map[string]any{"tkg_valid_from": int64(3000), "k": int64(10), "keep": "second"}); err != nil {
			return err
		}
		return tw.importRel(g, rid, first, map[string]any{"tkg_valid_from": int64(3000), "k": int64(10), "keep": "second"})
	})
	correct(3000, 11)
	return nid, rid
}

// metaBoth runs one cascade door on the node, then on the relationship.
func (tw *headTwin) metaBoth(name string, meta uint64, wantErr bool, node, rel func(g *Core) error) {
	tw.metaDo(name+" (node)", meta, wantErr, node)
	tw.metaDo(name+" (rel)", meta, wantErr, rel)
}

func (tw *headTwin) metaCorrect(name string, meta uint64, nid types.NodeID, rid types.RelID, vf, vt types.Instant) {
	p := map[string]any{"k": int64(100 + tw.rng.IntN(100)), "c": "x"}
	ctx := context.Background()
	tw.metaBoth(fmt.Sprintf("%s [%d,%d)", name, vf, vt), meta, false,
		func(g *Core) error { _, err := g.Temporal.SetNodeVersionInterval(ctx, nid, vf, vt, p); return err },
		func(g *Core) error { _, err := g.Temporal.SetRelVersionInterval(ctx, rid, vf, vt, p); return err })
}

// TestCascadeMetaTwin: the correction planned on skeletons writes
// byte-for-byte the rows the whole-history plan writes (every appended row,
// the current row, and the timelines read after it), on memory, badger,
// sharded and tiered, both valid axes, nodes and relationships: bounded and
// open corrections, excises (one refused over a gap) and corrections over
// their void rows, a whole-axis untimed update and label change
// (cascadeExisting), chains holding a tombstone or a retraction and a
// re-import, a caller instant refused by the order rule (same error) and one
// accepted; it falls back to the whole history, with the same rows, on a
// chain stored before backlog 38 (a history row at the current version) and
// on a history compacted between the skeleton read and the point reads; it
// plans on skeletons on a chain read as stored before the bitemporal
// migration. Mutation: a hydration that keeps a skeleton or reads the wrong
// version, a gate that admits the shared version, a plan step skipped on
// skeletons, all differ here.
func TestCascadeMetaTwin(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for _, be := range append(r10AllBackends(t), r10WholeAxisBackends(t)...) {
		t.Run(be.name, func(t *testing.T) {
			t.Parallel()
			tw := newMetaTwin(t, be)
			wholeAxis := tw.a.untimedWholeAxis.Load()
			open := tw.a.axis().OpenEnd()

			// A tombstone inside the chain (delete and re-import).
			nid, rid := tw.metaEntities("delete")
			tw.metaCorrect("bounded", 1, nid, rid, 3050, 3450)
			tw.metaCorrect("bounded from the first life", 1, nid, rid, 1050, 3250)
			tw.metaCorrect("open", 1, nid, rid, 3250, open)
			tw.metaBoth("excise [3100,3300)", 1, false,
				func(g *Core) error { return g.Temporal.ExciseNodeValidity(ctx, nid, 3100, 3300) },
				func(g *Core) error { return g.Temporal.ExciseRelValidity(ctx, rid, 3100, 3300) })
			tw.metaCorrect("over the void rows", 1, nid, rid, 3000, 3500)
			tw.metaBoth("excise of a gap", 1, true,
				func(g *Core) error { return g.Temporal.ExciseNodeValidity(ctx, nid, 100, 200) },
				func(g *Core) error { return g.Temporal.ExciseRelValidity(ctx, rid, 100, 200) })

			// The order rule: refused at the newest stamp (the error names
			// it), accepted just above.
			stamp := func(g *Core, rel bool) types.Instant {
				var f, to types.Instant
				if rel {
					f, to, _, _ = g.Rels.LatestStamps(rid)
				} else {
					f, to, _, _ = g.Nodes.LatestStamps(nid)
				}
				return max(f, to)
			}
			p := map[string]any{"k": int64(7)}
			tw.metaBoth("caller instant at the newest stamp", 1, true,
				func(g *Core) error {
					_, err := g.Temporal.SetNodeVersionIntervalWithTx(ctx, nid, 3050, 3150, p, stamp(g, false))
					return err
				},
				func(g *Core) error {
					_, err := g.Temporal.SetRelVersionIntervalWithTx(ctx, rid, 3050, 3150, p, stamp(g, true))
					return err
				})
			tw.metaBoth("caller instant above it", 1, false,
				func(g *Core) error {
					_, err := g.Temporal.SetNodeVersionIntervalWithTx(ctx, nid, 3050, 3150, p, stamp(g, false)+1)
					return err
				},
				func(g *Core) error {
					_, err := g.Temporal.SetRelVersionIntervalWithTx(ctx, rid, 3050, 3150, p, stamp(g, true)+1)
					return err
				})

			// A retraction and a re-import.
			rn, rr := tw.metaEntities("retract")
			tw.metaCorrect("bounded after a retraction", 1, rn, rr, 3050, 3450)
			tw.metaCorrect("open after a retraction", 1, rn, rr, 3150, open)

			// A whole-axis untimed update patches every valid segment
			// (cascadeExisting), a label change likewise on the node; on the
			// legacy axis both are plain one-version writes.
			var whole uint64
			if wholeAxis {
				whole = 1
			}
			up := map[string]any{"k": int64(55), "u": "all"}
			tw.metaBoth("untimed update", whole, false,
				func(g *Core) error { _, err := g.Nodes.Update(ctx, nid, up); return err },
				func(g *Core) error { _, err := g.Rels.Update(ctx, rid, up); return err })
			tw.metaDo("untimed label change (node)", whole, false, func(g *Core) error { return g.Nodes.AddLabel(ctx, rn, "Extra") })

			// A history compacted between the skeleton read and the point
			// reads: A falls back to the whole history, as B reads it.
			for _, rel := range []bool{false, true} {
				fired := false
				tw.metaDo(fmt.Sprintf("compaction between the reads (rel=%v)", rel), 0, false, func(g *Core) error {
					trim := func() {
						var err error
						if rel {
							err = g.store.TruncateRelHistory(rid, 1)
						} else {
							err = g.store.TruncateNodeHistory(nid, 1)
						}
						if err != nil {
							t.Errorf("truncate: %v", err)
						}
					}
					if g == tw.b || g.temporalMetaHistory == nil {
						trim() // no skeleton read: compacted before the history read
					} else {
						g.cascadeMetaHook = func() { fired = true; g.cascadeMetaHook = nil; trim() }
						defer func() { g.cascadeMetaHook = nil }()
					}
					if rel {
						_, err := g.Temporal.SetRelVersionInterval(ctx, rid, 3050, 3450, map[string]any{"k": int64(77)})
						return err
					}
					_, err := g.Temporal.SetNodeVersionInterval(ctx, nid, 3050, 3450, map[string]any{"k": int64(77)})
					return err
				})
				if tw.a.temporalMetaHistory != nil && !fired {
					t.Fatalf("compaction (rel=%v): the hook never ran", rel)
				}
			}

			// A chain stored before backlog 38: a history row at the current
			// row's version. The skeletons cannot tell it from the current
			// row: the whole history is read.
			ln, lr := tw.metaEntities("")
			tw.metaDo("legacy shared version", 0, false, func(g *Core) error {
				n, err := g.store.GetNode(ln)
				if err != nil {
					return err
				}
				if err := g.store.PutNodeVersion(ln, n.Version(), n.DeepCopy()); err != nil {
					return err
				}
				r, err := g.store.GetRelationship(lr)
				if err != nil {
					return err
				}
				return g.store.PutRelVersion(lr, r.Version(), r.DeepCopy())
			})
			tw.metaCorrect("bounded on a shared version", 0, ln, lr, 1050, 1450)

			// A chain read as stored before the bitemporal migration (the
			// legacy start rule of the positional bounds).
			tw.a.bitemporalMigrated, tw.b.bitemporalMigrated = false, false
			tw.metaCorrect("pre-migration bounded", 1, nid, rid, 3020, 3420)
			tw.metaCorrect("pre-migration open", 1, rn, rr, 3120, open)
			tw.a.bitemporalMigrated, tw.b.bitemporalMigrated = true, true
		})
	}
}
