package core

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"reflect"
	"strings"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// RELHIST P2 (tasks/evidence/RELHIST/design.txt): sigma's cut writes a fact
// (a plain Update) and reads it back through the plural door. The head entry
// answers the plural door when every pin is at or above the chain's newest
// TxFrom, the plural door's fold fills it, and a plain Update, ReplaceVersion
// or CloseVersion extends a valid entry by the appended row instead of
// leaving it stale (headSupersede), so the read-back after the write is a
// hit, not a skeleton walk of every version.

// relhistP2Backends is every store with a history token: memory, badger,
// tiered (sigma's) and sharded (Fable review: it admits the token too).
func relhistP2Backends(tb testing.TB) []r10Backend {
	return r10AllBackends(tb)
}

func relhistP2Graph(tb testing.TB, be r10Backend) *Core {
	tb.Helper()
	cfg := be.cfg()
	cfg.ReplaceVersionLabels = []string{"P"}
	g, err := New(cfg)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = g.Close() })
	return g
}

// relhistP2Entities adds a node (label P) and a relationship, each with
// versions rows written by plain Updates.
func relhistP2Entities(tb testing.TB, g *Core, versions int) (types.NodeID, types.RelID) {
	tb.Helper()
	ctx := context.Background()
	n, err := g.Nodes.Add(ctx, []string{"P"}, map[string]any{"v": int64(0)})
	if err != nil {
		tb.Fatal(err)
	}
	m, err := g.Nodes.Add(ctx, []string{"P"}, map[string]any{"v": int64(0)})
	if err != nil {
		tb.Fatal(err)
	}
	r, err := g.Rels.AddByID(ctx, "PR", n.ID(), m.ID(), map[string]any{"v": int64(0)})
	if err != nil {
		tb.Fatal(err)
	}
	for i := 1; i < versions; i++ {
		if _, err := g.Nodes.Update(ctx, n.ID(), map[string]any{"v": int64(i)}); err != nil {
			tb.Fatal(err)
		}
		if _, err := g.Rels.Update(ctx, r.ID(), map[string]any{"v": int64(i)}); err != nil {
			tb.Fatal(err)
		}
	}
	return n.ID(), r.ID()
}

// relhistP2Plural reads the plural doors and the singular fold (cache off)
// at pins and fails on any difference.
func relhistP2Plural(t *testing.T, g *Core, nid types.NodeID, rid types.RelID, pins []types.Instant, when string) {
	t.Helper()
	gotN, errN := g.Temporal.NodeEffectiveTimelines(nid, pins)
	gotR, errR := g.Temporal.RelEffectiveTimelines(rid, pins)
	g.headCacheOff = true
	defer func() { g.headCacheOff = false }()
	for i, pin := range pins {
		wantN, werr := g.Temporal.NodeEffectiveTimeline(nid, pin)
		if (werr != nil) != (errN != nil) {
			t.Fatalf("%s: node pin %d: plural err %v, fold err %v", when, pin, errN, werr)
		}
		if werr == nil && etSegsString(etNodeSegs(gotN[i])) != etSegsString(etNodeSegs(wantN)) {
			t.Fatalf("%s: node pin %d (head %d):\n plural %s\n fold   %s", when, pin, g.peekNow(),
				etSegsString(etNodeSegs(gotN[i])), etSegsString(etNodeSegs(wantN)))
		}
		wantR, werr := g.Temporal.RelEffectiveTimeline(rid, pin)
		if (werr != nil) != (errR != nil) {
			t.Fatalf("%s: rel pin %d: plural err %v, fold err %v", when, pin, errR, werr)
		}
		if werr == nil && etSegsString(etRelSegs(gotR[i])) != etSegsString(etRelSegs(wantR)) {
			t.Fatalf("%s: rel pin %d (head %d):\n plural %s\n fold   %s", when, pin, g.peekNow(),
				etSegsString(etRelSegs(gotR[i])), etSegsString(etRelSegs(wantR)))
		}
	}
}

// The plural door fills the entry on its first call (from its fold at the
// highest pin, though another pin lies below the chain's newest TxFrom) and
// answers the second from it; a pin below the newest TxFrom folds.
func TestRELHISTP2PluralDoorHeadHit(t *testing.T) {
	for _, be := range relhistP2Backends(t) {
		t.Run(be.name, func(t *testing.T) {
			g := relhistP2Graph(t, be)
			nid, rid := relhistP2Entities(t, g, 8)
			old := g.peekNow()
			if _, err := g.Nodes.Update(context.Background(), nid, map[string]any{"v": int64(99)}); err != nil {
				t.Fatal(err)
			}
			if _, err := g.Rels.Update(context.Background(), rid, map[string]any{"v": int64(99)}); err != nil {
				t.Fatal(err)
			}
			head := g.peekNow()
			for _, door := range []string{"node", "rel"} {
				read := func(pins []types.Instant) {
					var err error
					if door == "node" {
						_, err = g.Temporal.NodeEffectiveTimelines(nid, pins)
					} else {
						_, err = g.Temporal.RelEffectiveTimelines(rid, pins)
					}
					if err != nil {
						t.Fatal(err)
					}
				}
				h0 := g.headHits.Load()
				read([]types.Instant{old, head}) // a pin below maxTx: the fold, which fills at the highest pin
				h1 := g.headHits.Load()
				read([]types.Instant{head, head})
				h2 := g.headHits.Load()
				read([]types.Instant{head, old})
				h3 := g.headHits.Load()
				if h1 != h0 || h2 != h1+1 || h3 != h2 {
					t.Fatalf("%s: head hits %d -> %d (fill) -> %d (second call) -> %d (a pin below maxTx); want +0, +1, +0", door, h0, h1, h2, h3)
				}
			}
			relhistP2Plural(t, g, nid, rid, []types.Instant{head, old, head - 1}, "after the reads")
		})
	}
}

// An excise appends a void row that wins nothing, so a read pinned before the
// excise must not see the post-excise gap although every winner of the entry
// was recorded by its pin: the pin rule (the lowest pin against maxTx) keeps
// it from the newest answer, and the generation the excise kept (RELHIST P5
// option C) answers it with the pre-excise one. Then a CloseVersion in the
// own-bounds arm keeps the entry.
func TestRELHISTP2PluralDoorExciseAndClose(t *testing.T) {
	ctx := context.Background()
	for _, be := range relhistP2Backends(t) {
		t.Run(be.name, func(t *testing.T) {
			g := relhistP2Graph(t, be)
			id := r10Node(t, g, 4) // four rows of validity [1000, 2000)
			rid := r10Rel(t, g, 4)
			relhistP2Plural(t, g, id, rid, []types.Instant{g.peekNow()}, "fill")
			o0 := g.headOverlays.Load()
			if err := g.Temporal.ExciseNodeValidity(ctx, id, 1500, 2000); err != nil {
				t.Fatal(err)
			}
			if err := g.Temporal.ExciseRelValidity(ctx, rid, 1500, 2000); err != nil {
				t.Fatal(err)
			}
			if o := g.headOverlays.Load(); o != o0+2 {
				t.Fatalf("head overlays %d -> %d after two excises, want +2", o0, o)
			}
			nf, _, _, _ := g.Nodes.LatestStamps(id)
			rf, _, _, _ := g.Rels.LatestStamps(rid)
			below := min(nf, rf) - 1 // after every row but the void ones
			h0, gh0 := g.headHits.Load(), g.headStats.genHits[headDoorMany].Load()
			relhistP2Plural(t, g, id, rid, []types.Instant{below, g.peekNow()}, "a pin before the excises")
			if h, gh := g.headHits.Load(), g.headStats.genHits[headDoorMany].Load(); h != h0+2 || gh != gh0+1 {
				t.Fatalf("head hits %d -> %d, node generation hits %d -> %d for a call with a pin before the excises, want +2, +1", h0, h, gh0, gh)
			}
			// CloseVersion of an own-bounds chain (an open row from 1000, a
			// correction [1200, 1400) and its open resumption, then one at
			// [1100, 1150) below the newest start): no row starts after the
			// closed current row, so the entry is extended (the instants
			// after the close are a gap) and the read-back hits.
			n, err := g.Nodes.Add(ctx, []string{"P"}, map[string]any{"v": int64(0), "tkg_valid_from": int64(1000)})
			if err != nil {
				t.Fatal(err)
			}
			m, err := g.Nodes.Add(ctx, []string{"P"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			r, err := g.Rels.AddByID(ctx, "PR", n.ID(), m.ID(), map[string]any{"v": int64(0), "tkg_valid_from": int64(1000)})
			if err != nil {
				t.Fatal(err)
			}
			id, rid = n.ID(), r.ID()
			for _, iv := range [][2]types.Instant{{1200, 1400}, {1100, 1150}} {
				if _, err := g.Temporal.SetNodeVersionInterval(ctx, id, iv[0], iv[1], map[string]any{"v": int64(iv[0])}); err != nil {
					t.Fatal(err)
				}
				if _, err := g.Temporal.SetRelVersionInterval(ctx, rid, iv[0], iv[1], map[string]any{"v": int64(iv[0])}); err != nil {
					t.Fatal(err)
				}
			}
			relhistP2Plural(t, g, id, rid, []types.Instant{g.peekNow()}, "fill")
			o0 = g.headOverlays.Load()
			if err := g.Nodes.CloseVersion(ctx, id, 1500); err != nil {
				t.Fatal(err)
			}
			if err := g.Rels.CloseVersion(ctx, rid, 1500); err != nil {
				t.Fatal(err)
			}
			if o := g.headOverlays.Load(); o != o0+2 {
				t.Fatalf("head overlays %d -> %d after two CloseVersion, want +2", o0, o)
			}
			h0 = g.headHits.Load()
			head := g.peekNow()
			relhistP2Plural(t, g, id, rid, []types.Instant{head, head}, "after CloseVersion")
			if h := g.headHits.Load(); h != h0+2 {
				t.Fatalf("head hits %d -> %d on the read-back after CloseVersion, want +2", h0, h)
			}
		})
	}
}

// After a plain Update (with and without a valid start), a ReplaceVersion
// and a relationship Update, the entry filled before the write is extended,
// not dropped: the read-back is a hit at both doors.
func TestRELHISTP2UpdateKeepsHeadEntry(t *testing.T) {
	ctx := context.Background()
	for _, be := range relhistP2Backends(t) {
		t.Run(be.name, func(t *testing.T) {
			g := relhistP2Graph(t, be)
			nid, rid := relhistP2Entities(t, g, 8)
			writes := []struct {
				name  string
				write func() error
			}{
				{"node Update", func() error { _, err := g.Nodes.Update(ctx, nid, map[string]any{"v": int64(100)}); return err }},
				{"rel Update", func() error { _, err := g.Rels.Update(ctx, rid, map[string]any{"v": int64(100)}); return err }},
				{"node Update with a valid start", func() error {
					n, err := g.Nodes.Get(ctx, nid)
					if err != nil {
						return err
					}
					_, err = g.Nodes.Update(ctx, nid, map[string]any{"v": int64(101), "tkg_valid_from": int64(g.nodeValidFrom(n) + 7)})
					return err
				}},
				{"node ReplaceVersion", func() error {
					n, err := g.Nodes.Get(ctx, nid)
					if err != nil {
						return err
					}
					_, err = g.Nodes.ReplaceVersion(ctx, nid, map[string]any{"v": int64(102)}, g.nodeValidFrom(n)+5, g.axis().OpenEnd())
					return err
				}},
			}
			for _, w := range writes {
				head := g.peekNow()
				relhistP2Plural(t, g, nid, rid, []types.Instant{head}, w.name+": fill") // fills both entries
				o0 := g.headOverlays.Load()
				if err := w.write(); err != nil {
					t.Fatalf("%s: %v", w.name, err)
				}
				if o := g.headOverlays.Load(); o != o0+1 {
					t.Fatalf("%s: head overlays %d -> %d, want +1 (the entry was dropped)", w.name, o0, o)
				}
				head = g.peekNow()
				h0 := g.headHits.Load()
				if _, err := g.Temporal.NodeEffectiveTimelines(nid, []types.Instant{head}); err != nil {
					t.Fatal(err)
				}
				if _, err := g.Temporal.RelEffectiveTimelines(rid, []types.Instant{head}); err != nil {
					t.Fatal(err)
				}
				if _, err := g.Temporal.NodeEffectiveTimeline(nid, head); err != nil {
					t.Fatal(err)
				}
				if _, err := g.Temporal.RelEffectiveTimeline(rid, head); err != nil {
					t.Fatal(err)
				}
				if h := g.headHits.Load(); h != h0+4 {
					t.Fatalf("%s: head hits %d -> %d on four head reads after the write, want +4", w.name, h0, h)
				}
				relhistP2Plural(t, g, nid, rid, []types.Instant{head, head - 1, head}, w.name+": read-back")
			}
		})
	}
}

// The oracle: a random sequence of plain Updates (open, with a valid start,
// with a valid end), ReplaceVersion, CloseVersion and corrections
// (SetVersionInterval: the own-bounds arm), each followed by a plural read
// at the head pin, at the pin just before the write and at earlier pins,
// compared with the singular fold (cache off) pin by pin; and every entry
// valid at the head equals the one the fold builds (bounds, stamps, arm,
// pieces), so a later correction planned from it plans as from the history.
func TestRELHISTP2UpdateThenPluralOracle(t *testing.T) {
	ctx := context.Background()
	var overlays, hits uint64
	for _, be := range relhistP2Backends(t) {
		for s := range 2 {
			t.Run(fmt.Sprintf("%s/seed=%d", be.name, s), func(t *testing.T) {
				g := relhistP2Graph(t, be)
				rng := rand.New(rand.NewPCG(uint64(0x9202+s), 2))
				nid, rid := relhistP2Entities(t, g, 3)
				pins := []types.Instant{g.peekNow()}
				for step := range 60 {
					before := g.peekNow()
					// Fill both entries, so the write finds one to extend.
					relhistP2Plural(t, g, nid, rid, []types.Instant{before}, "fill")
					rel := rng.IntN(3) == 0
					var cur types.Instant
					if rel {
						if r, err := g.Rels.Get(ctx, rid); err == nil {
							cur = g.relValidFrom(r)
						}
					} else if n, err := g.Nodes.Get(ctx, nid); err == nil {
						cur = g.nodeValidFrom(n)
					}
					p := map[string]any{"v": int64(rng.IntN(1000))}
					var what string
					var err error
					switch op := rng.IntN(100); {
					case op < 35:
						what = "Update"
					case op < 45:
						what = "Update valid_from"
						p["tkg_valid_from"] = int64(cur + 1 + types.Instant(rng.IntN(50)))
					case op < 50:
						what = "Update valid_from+valid_to"
						f := cur + 1 + types.Instant(rng.IntN(50))
						p["tkg_valid_from"], p["tkg_valid_to"] = int64(f), int64(f+1+types.Instant(rng.IntN(50)))
					case op < 65 && !rel:
						f := cur + types.Instant(rng.IntN(60)) - 30
						to := g.axis().OpenEnd()
						if rng.IntN(2) == 0 {
							to = f + 1 + types.Instant(rng.IntN(60))
						}
						what = fmt.Sprintf("ReplaceVersion [%d,%d)", f, to)
						_, err = g.Nodes.ReplaceVersion(ctx, nid, p, f, to)
					case op < 72:
						what = "CloseVersion"
						at := cur + 1 + types.Instant(rng.IntN(100))
						if rel {
							err = g.Rels.CloseVersion(ctx, rid, at)
						} else {
							err = g.Nodes.CloseVersion(ctx, nid, at)
						}
					default:
						f := cur + types.Instant(rng.IntN(80)) - 40
						to := types.Instant(0)
						if rng.IntN(3) > 0 {
							to = f + 1 + types.Instant(rng.IntN(60))
						}
						what = fmt.Sprintf("SetVersionInterval [%d,%d)", f, to)
						if rel {
							_, err = g.Temporal.SetRelVersionInterval(ctx, rid, f, to, p)
						} else {
							_, err = g.Temporal.SetNodeVersionInterval(ctx, nid, f, to, p)
						}
					}
					if strings.HasPrefix(what, "Update") {
						if rel {
							_, err = g.Rels.Update(ctx, rid, p)
						} else {
							_, err = g.Nodes.Update(ctx, nid, p)
						}
					}
					when := fmt.Sprintf("step %d: %s rel=%v (err %v)", step, what, rel, err)
					head := g.peekNow()
					// First the entry the write left against the fold's
					// (before any read can refill it), then the pins it may
					// answer: the written row's TxFrom and the instant before
					// it (at or above the old entry's newest TxFrom, below
					// the new one's: a generation's, option C), then a mixed
					// read (a pin below maxTx: a generation's, or the fold,
					// which refills).
					relhistP2EntryAsFold(t, g, nid, rid, head, when)
					wrote, _, _, _ := g.Nodes.LatestStamps(nid)
					if rel {
						wrote, _, _, _ = g.Rels.LatestStamps(rid)
					}
					relhistP2Plural(t, g, nid, rid, []types.Instant{max(wrote-1, 1), max(wrote, 1), head}, when)
					pins = append(pins, before, head)
					read := []types.Instant{head, before, head}
					for range 3 {
						read = append(read, pins[rng.IntN(len(pins))])
					}
					relhistP2Plural(t, g, nid, rid, read, when)
				}
				overlays += g.headOverlays.Load()
				hits += g.headHits.Load()
			})
		}
	}
	t.Logf("%d overlays, %d head hits", overlays, hits)
	if overlays == 0 || hits == 0 {
		t.Fatalf("the sequence never exercised the cache: %d overlays, %d hits", overlays, hits)
	}
}

// BenchmarkRELHISTP2UpdateReadBack is sigma's cut on one fact: a plain Update,
// then the plural read-back at the head pin. 64 keys of 100 versions each,
// written round-robin, so run it with -benchtime 640x (each key ends at 110
// versions); before RELHIST P2 every read-back walked every version's
// skeleton (badger, tiered) or the whole history (memory).
func BenchmarkRELHISTP2UpdateReadBack(b *testing.B) {
	const keys, versions = 64, 100
	ctx := context.Background()
	for _, be := range relhistP2Backends(b) {
		for _, rel := range []bool{false, true} {
			b.Run(fmt.Sprintf("%s/rel=%v", be.name, rel), func(b *testing.B) {
				g := relhistP2Graph(b, be)
				nids := make([]types.NodeID, keys)
				rids := make([]types.RelID, keys)
				for i := range keys {
					nids[i], rids[i] = relhistP2Entities(b, g, versions)
				}
				pins := []types.Instant{0}
				b.ReportAllocs()
				b.ResetTimer()
				for i := range b.N {
					k := i % keys
					p := map[string]any{"v": int64(i)}
					var err error
					if rel {
						if _, err = g.Rels.Update(ctx, rids[k], p); err == nil {
							pins[0] = g.peekNow()
							_, err = g.Temporal.RelEffectiveTimelines(rids[k], pins)
						}
					} else if _, err = g.Nodes.Update(ctx, nids[k], p); err == nil {
						pins[0] = g.peekNow()
						_, err = g.Temporal.NodeEffectiveTimelines(nids[k], pins)
					}
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

// relhistP2EntryAsFold checks that each entity's entry, when valid at the
// head, equals the entry a fold of the chain builds, its generations aside.
func relhistP2EntryAsFold(t *testing.T, g *Core, nid types.NodeID, rid types.RelID, head types.Instant, when string) {
	t.Helper()
	check := func(k headKey, valid func() *headEntry, refold func()) {
		e := valid()
		if e == nil {
			return
		}
		g.heads.put(k, nil)
		refold()
		f := g.heads.get(k)
		if f == nil {
			t.Fatalf("%s: %+v: the entry is valid but the fold keeps none", when, k)
		}
		// Generations (RELHIST P5 option C) are kept by extensions only, never
		// by a fold; their answers are checked against the fold by the reads.
		kept, fold := *e, *f
		kept.gens, fold.gens = nil, nil
		if !reflect.DeepEqual(kept, fold) {
			t.Fatalf("%s: %+v: entry differs from the fold's\n kept %+v\n fold %+v", when, k, kept, fold)
		}
	}
	ctx := context.Background()
	if n, err := g.Nodes.Get(ctx, nid); err == nil {
		check(headKey{id: nid.SnowflakeID()}, func() *headEntry {
			return g.headValid(headKey{id: nid.SnowflakeID()}, g.nodeHistoryEpoch(nid), n.Version(), temporalOf(n), math.MaxInt64)
		}, func() { _, _ = g.Temporal.NodeEffectiveTimeline(nid, head) })
	}
	if r, err := g.Rels.Get(ctx, rid); err == nil {
		check(headKey{rel: true, id: rid.SnowflakeID()}, func() *headEntry {
			return g.headValid(headKey{rel: true, id: rid.SnowflakeID()}, g.relHistoryEpoch(rid), r.Version(), temporalOf(r), math.MaxInt64)
		}, func() { _, _ = g.Temporal.RelEffectiveTimeline(rid, head) })
	}
}

// A GraphTx Update supersedes the entry like a plain one (the scoped branches
// of node_update.go and relationship_update.go); its rollback restores the
// old current row, so the entry it left must not answer: every read after the
// rollback, and after a committed Update, equals the fold.
func TestRELHISTP2RolledBackTxUpdate(t *testing.T) {
	for _, be := range relhistP2Backends(t) {
		t.Run(be.name, func(t *testing.T) {
			g := relhistP2Graph(t, be)
			nid, rid := relhistP2Entities(t, g, 8)
			before := g.peekNow()
			relhistP2Plural(t, g, nid, rid, []types.Instant{before}, "fill")
			tx, err := g.BeginTx()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := tx.UpdateNode(nid, map[string]any{"v": int64(77)}); err != nil {
				_ = tx.Rollback()
				t.Fatal(err)
			}
			if _, err := tx.UpdateRelationship(rid, map[string]any{"v": int64(77)}); err != nil {
				_ = tx.Rollback()
				t.Fatal(err)
			}
			relhistP2Plural(t, g, nid, rid, []types.Instant{g.peekNow(), before}, "inside the tx")
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			// Head pins only first: a pin below maxTx would fold and refill, hiding a stale entry.
			relhistP2Plural(t, g, nid, rid, []types.Instant{g.peekNow(), g.peekNow()}, "after the rollback")
			relhistP2Plural(t, g, nid, rid, []types.Instant{g.peekNow(), before}, "after the rollback, an older pin")
			if _, err := g.Nodes.Update(context.Background(), nid, map[string]any{"v": int64(78)}); err != nil {
				t.Fatal(err)
			}
			if _, err := g.Rels.Update(context.Background(), rid, map[string]any{"v": int64(78)}); err != nil {
				t.Fatal(err)
			}
			relhistP2Plural(t, g, nid, rid, []types.Instant{g.peekNow(), before}, "after a committed Update")
		})
	}
}
