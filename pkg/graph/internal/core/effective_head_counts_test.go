package core

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	shardedpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// RELHIST P5 Phase 0: the head read counts (effective_head.go, headReadStats)
// on sigma's shape, and each reason on its own. The counts only measure; the
// differential oracle (effective_head_test.go) owns the answers.

// headReasonNames labels the reasons in logs, in headReason order.
var headReasonNames = [headReasons]string{"hit", "current-only", "no-entry", "token-moved", "current-changed",
	"pin-below-maxTx", "not-whole", "not-qualifying", "scan", "row-gone"}

// headCounts is a copy of Core.headStats.
type headCounts struct {
	reads                                  [headDoors][headReasons]uint64
	genHits                                [headDoors]uint64 // hits answered from a generation (also in reads)
	nodeAt, missPlans, metaLists, metaRows uint64
	gens                                   uint64 // Core.headGens: generations pushed
}

func headCountsOf(c *Core) headCounts {
	var h headCounts
	for d := range headDoors {
		for r := range headReasons {
			h.reads[d][r] = c.headStats.reads[d][r].Load()
		}
		h.genHits[d] = c.headStats.genHits[d].Load()
	}
	h.gens = c.headGens.Load()
	h.nodeAt, h.missPlans = c.headStats.nodeAt.Load(), c.headStats.missPlans.Load()
	h.metaLists, h.metaRows = c.headStats.metaLists.Load(), c.headStats.metaRows.Load()
	return h
}

func (h headCounts) sub(b headCounts) headCounts {
	for d := range headDoors {
		for r := range headReasons {
			h.reads[d][r] -= b.reads[d][r]
		}
		h.genHits[d] -= b.genHits[d]
	}
	h.gens -= b.gens
	h.nodeAt -= b.nodeAt
	h.missPlans -= b.missPlans
	h.metaLists -= b.metaLists
	h.metaRows -= b.metaRows
	return h
}

func (h headCounts) add(b headCounts) headCounts {
	for d := range headDoors {
		for r := range headReasons {
			h.reads[d][r] += b.reads[d][r]
		}
		h.genHits[d] += b.genHits[d]
	}
	h.gens += b.gens
	h.nodeAt += b.nodeAt
	h.missPlans += b.missPlans
	h.metaLists += b.metaLists
	h.metaRows += b.metaRows
	return h
}

func (h headCounts) door(d headDoor) (n uint64) {
	for _, v := range h.reads[d] {
		n += v
	}
	return n
}

// only reports the one reason door d counted in h (ok false: none or more).
func (h headCounts) only(d headDoor) (headReason, bool) {
	found, n := headReason(-1), 0
	for r, v := range h.reads[d] {
		if v != 0 {
			found, n = headReason(r), n+int(v) // #nosec G115 -- test counts
		}
	}
	return found, n == 1
}

func (h headCounts) String() string {
	var b strings.Builder
	for d, name := range []string{"singular", "plural"} {
		fmt.Fprintf(&b, "%s %d [", name, h.door(headDoor(d)))
		first := true
		for r, v := range h.reads[d] {
			if v == 0 {
				continue
			}
			if !first {
				b.WriteString(" ")
			}
			first = false
			fmt.Fprintf(&b, "%s=%d", headReasonNames[r], v)
		}
		if h.genHits[d] != 0 {
			fmt.Fprintf(&b, " (gen-hits=%d)", h.genHits[d])
		}
		b.WriteString("] ")
	}
	fmt.Fprintf(&b, "node-at=%d miss-plans=%d skeleton-lists=%d skeletons=%d gens-pushed=%d", h.nodeAt, h.missPlans, h.metaLists, h.metaRows, h.gens)
	return b.String()
}

// p5Backends are r10AllBackends with sigma's placement on tiered: Derived and
// Session on the reference shard (ai-soc graphmgr RefLabels).
func p5Backends(tb testing.TB) []r10Backend {
	return []r10Backend{
		{"memory", func() Config { return Config{} }},
		{"badger", func() Config { return Config{BadgerInMemory: true} }},
		{"sharded", func() Config {
			st, err := shardedpkg.New(shardedpkg.Config{InMemory: true, BaseSlot: 0, SlotCount: 2})
			if err != nil {
				tb.Fatalf("sharded.New: %v", err)
			}
			return Config{Store: st}
		}},
		{"tiered", func() Config {
			ts, err := tiered.New(tiered.Config{InMemory: true, RefLabels: []string{"Derived", "Session"},
				ShardWindow: 7 * 24 * time.Hour, FlushInterval: 1<<63 - 1})
			if err != nil {
				tb.Fatalf("tiered.New: %v", err)
			}
			tb.Cleanup(func() { _ = ts.Close() })
			return Config{Store: ts}
		}},
	}
}

// p5Shape is sigma's cut loop, per sigma's and ai-soc's code answers of
// 4 Nov (tasks/evidence/RELHIST/design.txt, RELHIST P5): one process through
// every cut; each cut one GraphTx — every open Session's current row patched
// over its own interval (tx.SetNodeVersionInterval(id, start, open, ...),
// sigma overview_graph.go:815), a Session's life ended by one close
// (tx.UpdateNode setting tkg_valid_to, :793) and reopened the next cut as a
// new open segment (SetNodeVersionInterval(id, the cut's end, open, ...),
// :850); a share of the Derived nodes rewritten by tx.UpdateNode, some closed
// in place (tkg_valid_to at the cut's end) and replaced by a new one; then
// the cut's reads, routed as sigma's derived_read.go:350-360: one pin at or
// after the current row's TxFrom through the singular door, several pins or
// one older pin through the plural door.
type p5Shape struct {
	sessions    int // Session nodes, never replaced: closed and reopened in turn
	derived     int // Derived nodes at the start
	cuts        int
	sessionLife int // a Session is closed every sessionLife cuts (staggered by slot), reopened the next cut
	rewrites    int // live Derived nodes rewritten per cut
	closes      int // live Derived nodes closed per cut, each replaced by a new one
	reads       int // Derived nodes (live or closed) read per cut besides the Sessions
}

// p5Result is what one run counted.
type p5Result struct {
	read, write   headCounts // during the read phases and the write phases
	histRead      uint64     // history rows read by the read phases
	histWrite     uint64     // by the write phases (GraphTx snapshots, the writes' plans)
	singular      int        // reads through the singular door
	plural        int        // reads through the plural door
	sessionOneHit int        // see sessionOne: hits
	sessionOne    int        // Session reads at the cut's pin, patched in the cut, entry filled at an earlier cut
	r1Hit         int        // see r1: plural hits with one pin from a generation
	r1            int        // R1: Session reads at the previous and the cut's pin, patched in the cut, entry filled at an earlier cut
	r2Hit, r2     [6]int     // R2 by k: reads at one pin k cuts back of a node whose entry was filled at or before that cut
	headWrites    uint64     // Core.headWrites, metaWrites, headOverlays at the end
	metaWrites    uint64
	overlays      uint64
	copied        uint64 // rows the GraphTx rollback snapshots copied
	byClass       [p5PinClasses]headCounts
	noEntryFirst  int // no-entry reads of a node no earlier read left an entry for
	noEntryAfter  int // no-entry reads of a node an earlier read left an entry for (dropped since)
}

// p5PinClass is the pins a read asks.
type p5PinClass int

const (
	p5PinCut p5PinClass = iota // the cut's pin
	p5PinTwo                   // the previous cut's pin and this one's
	p5PinOld                   // one pin two to five cuts back
	p5PinClasses
)

var p5PinClassNames = [p5PinClasses]string{"cut pin", "previous+cut pins", "one older pin"}

func runP5Shape(tb testing.TB, g *Core, sh p5Shape, seed uint64) (res p5Result) {
	tb.Helper()
	ctx := context.Background()
	rng := rand.New(rand.NewPCG(seed, 5))
	var rows atomic.Uint64
	g.historyRead = func(n int) { rows.Add(uint64(n)) } // #nosec G115 -- a length
	var copied atomic.Uint64
	g.historyCopy = func(n int) { copied.Add(uint64(n)) } // #nosec G115 -- a length
	defer func() { g.historyRead, g.historyCopy = nil, nil; res.copied = copied.Load() }()

	add := func(add func([]string, map[string]any) (*types.Node, error), label string, cut int, from int64) types.NodeID {
		key := "v"
		if label == "Session" {
			key = "n"
		}
		n, err := add([]string{label}, map[string]any{key: int64(cut), "tkg_valid_from": from})
		if err != nil {
			tb.Fatal(err)
		}
		return n.ID()
	}
	plainAdd := func(labels []string, p map[string]any) (*types.Node, error) { return g.Nodes.Add(ctx, labels, p) }
	var sessions, live, pool []types.NodeID // Sessions; live Derived; every Derived
	start := map[types.NodeID]int64{}       // a Session's open segment's start
	for range sh.sessions {
		id := add(plainAdd, "Session", 0, 500)
		sessions, start[id] = append(sessions, id), 500
	}
	for range sh.derived {
		id := add(plainAdd, "Derived", 0, 500)
		live, pool = append(live, id), append(pool, id)
	}
	pins := []types.Instant{g.now()}
	open := g.axis().OpenEnd()
	reopen := map[types.NodeID]bool{}     // a Session closed last cut
	patchedAt := map[types.NodeID]int{}   // the cut whose write patched the Session's open row
	filledAt := map[types.NodeID]int{}    // the cut whose read at its pin left an entry
	filledSince := map[types.NodeID]int{} // the cut whose read filled the entry kept since (a fold at the cut's pin); a close forgets it

	var validEnd int64
	for cut := 1; cut <= sh.cuts; cut++ {
		// The cut's end in valid time: now, as sigma closes at the pin (an
		// update without tkg_valid_from starts its row at its TxFrom).
		validEnd = max(int64(g.readNow())+1, validEnd+1)
		// The write phase: one GraphTx.
		w0, r0 := headCountsOf(g), rows.Load()
		tx, err := g.BeginTx()
		if err != nil {
			tb.Fatal(err)
		}
		for slot, id := range sessions {
			switch {
			case reopen[id]: // a new open segment from the cut's end (sigma :850)
				_, err = tx.SetNodeVersionInterval(id, types.Instant(validEnd), open, map[string]any{"n": int64(cut)})
				start[id], reopen[id] = validEnd, false
			case (cut+slot)%sh.sessionLife == 0: // the close (sigma :793)
				_, err = tx.UpdateNode(id, map[string]any{"n": int64(cut), "tkg_valid_to": validEnd})
				reopen[id] = true
				delete(filledSince, id)
			default: // the every-cut patch of the open row over its own interval (sigma :815)
				_, err = tx.SetNodeVersionInterval(id, types.Instant(start[id]), open, map[string]any{"n": int64(cut)})
				patchedAt[id] = cut
			}
			if err != nil {
				tb.Fatalf("cut %d: session %v: %v", cut, id, err)
			}
		}
		perm := rng.Perm(len(live))
		for _, i := range perm[:sh.rewrites] {
			if _, err := tx.UpdateNode(live[i], map[string]any{"v": int64(cut)}); err != nil {
				tb.Fatalf("cut %d: derived %v: %v", cut, live[i], err)
			}
		}
		for _, i := range perm[sh.rewrites : sh.rewrites+sh.closes] {
			if _, err := tx.UpdateNode(live[i], map[string]any{"v": int64(cut), "tkg_valid_to": validEnd}); err != nil {
				tb.Fatalf("cut %d: derived close %v: %v", cut, live[i], err)
			}
			delete(filledSince, live[i])
			id := add(tx.AddNode, "Derived", cut, validEnd)
			live[i], pool = id, append(pool, id)
		}
		if err := tx.Commit(); err != nil {
			tb.Fatal(err)
		}
		res.write = res.write.add(headCountsOf(g).sub(w0))
		res.histWrite += rows.Load() - r0
		res.headWrites, res.metaWrites, res.overlays = g.headWrites.Load(), g.metaWrites.Load(), g.headOverlays.Load()
		pins = append(pins, g.now()) // reserved: every later write stamps above it

		// The read phase: every Session node and a sample of the Derived.
		r0, rc0 := rows.Load(), headCountsOf(g)
		read := slices.Clone(sessions)
		for _, i := range rng.Perm(len(pool))[:min(sh.reads, len(pool))] {
			read = append(read, pool[i])
		}
		for _, id := range read {
			var ask []types.Instant
			var class p5PinClass
			k := 0 // one older pin: cuts back
			kind := rng.IntN(100)
			switch {
			case kind < 60 || cut < 2:
				ask, class = []types.Instant{pins[cut]}, p5PinCut
			case kind < 85:
				ask, class = []types.Instant{pins[cut-1], pins[cut]}, p5PinTwo
			default:
				back := max(0, cut-2-rng.IntN(4))
				ask, class, k = []types.Instant{pins[back]}, p5PinOld, cut-back
			}
			cur, err := g.getCurrentNode(id)
			if err != nil {
				tb.Fatal(err)
			}
			before := headCountsOf(g)
			door := headDoorMany
			if len(ask) == 1 && ask[0] >= cur.Temporal().TxFrom {
				if _, err := g.Temporal.NodeEffectiveTimeline(id, ask[0]); err != nil {
					tb.Fatal(err)
				}
				res.singular++
				door = headDoorOne
			} else {
				if _, err := g.Temporal.NodeEffectiveTimelines(id, ask); err != nil {
					tb.Fatal(err)
				}
				res.plural++
			}
			got := headCountsOf(g).sub(before)
			res.byClass[class] = res.byClass[class].add(got)
			r, one := got.only(door)
			hit := one && r == headHit
			if got.reads[headDoorOne][headMissNoEntry]+got.reads[headDoorMany][headMissNoEntry] != 0 {
				if _, ok := filledAt[id]; ok {
					res.noEntryAfter++
				} else {
					res.noEntryFirst++
				}
			}
			f, filled := filledSince[id]
			switch {
			case class == p5PinCut && patchedAt[id] == cut && filled && f <= cut-1:
				res.sessionOne++
				if hit && door == headDoorOne {
					res.sessionOneHit++
				}
			case class == p5PinTwo && patchedAt[id] == cut && filled && f <= cut-1:
				// R1: the previous cut's pin lies in the generation the patch kept.
				res.r1++
				if hit && door == headDoorMany && got.genHits[headDoorMany] == 1 {
					res.r1Hit++
				}
			case class == p5PinOld && filled && f <= cut-k:
				// R2: the entry was filled at or before the pin's cut and kept
				// by every write since (at most one a cut, k <= headMaxGens).
				res.r2[k]++
				if hit && (door == headDoorOne || got.genHits[headDoorMany] == 1) {
					res.r2Hit[k]++
				}
			}
			// A read at the cut's pin of a node with history leaves a valid
			// entry (a hit, or the fold's fill); a node without history is
			// answered from its current row and leaves none.
			if slices.Max(ask) == pins[cut] && got.reads[headDoorOne][headCurrentOnly]+got.reads[headDoorMany][headCurrentOnly] == 0 {
				filledAt[id] = cut
				if !hit {
					filledSince[id] = cut // the fold's fill: no generation below it
				}
			}
		}
		res.read = res.read.add(headCountsOf(g).sub(rc0))
		res.histRead += rows.Load() - r0
	}
	return res
}

func p5DefaultShape() p5Shape {
	sh := p5Shape{sessions: 40, derived: 260, cuts: 30, sessionLife: 10, rewrites: 26, closes: 5, reads: 80}
	if isRaceEnabled() {
		sh = p5Shape{sessions: 10, derived: 60, cuts: 20, sessionLife: 8, rewrites: 6, closes: 2, reads: 20}
	}
	return sh
}

// p5Table is the RELHIST P5 counts table of res: reads by door and pins, by
// how they were answered.
func p5Table(res p5Result) string {
	var b strings.Builder
	fmt.Fprintf(&b, "  %-30s %6s %6s %9s %13s %9s %16s\n", "door / pins", "reads", "hit", "gen-hits", "current-only", "no-entry", "pin-below-maxTx")
	var tot [6]uint64
	for _, row := range []struct {
		name  string
		door  headDoor
		class p5PinClass
	}{
		{"singular, cut pin", headDoorOne, p5PinCut},
		{"singular, one older pin", headDoorOne, p5PinOld},
		{"plural, previous + cut pins", headDoorMany, p5PinTwo},
		{"plural, one older pin", headDoorMany, p5PinOld},
	} {
		h := res.byClass[row.class]
		v := [6]uint64{h.door(row.door), h.reads[row.door][headHit], h.genHits[row.door], h.reads[row.door][headCurrentOnly],
			h.reads[row.door][headMissNoEntry], h.reads[row.door][headMissPinBelow]}
		for i := range v {
			tot[i] += v[i]
		}
		fmt.Fprintf(&b, "  %-30s %6d %6d %9d %13d %9d %16d\n", row.name, v[0], v[1], v[2], v[3], v[4], v[5])
	}
	fmt.Fprintf(&b, "  %-30s %6d %6d %9d %13d %9d %16d\n", "total", tot[0], tot[1], tot[2], tot[3], tot[4], tot[5])
	fmt.Fprintf(&b, "  skeletons read: %d lists, %d skeletons; history rows read %d; generations pushed %d",
		res.read.metaLists, res.read.metaRows, res.histRead, res.write.gens)
	return b.String()
}

// TestHeadCountsSigmaShape runs sigma's cut loop on every backend and logs
// how its node history reads were answered: hits and misses by reason per
// door, skeleton lists read, history rows read (the RELHIST P5 table). It
// asserts the deterministic facts only: every read is counted once, at its
// door; no read meets a chain that cannot qualify or a scan; a Session
// patched in the cut whose entry was filled at an earlier cut hits at the
// cut's pin (singular door) and (R1, option C) at the previous and the cut's
// pin (plural door, the previous pin from a generation); (R2) a read at one
// pin k (2..5) cuts back of a node whose entry was filled at or before that
// cut and kept since hits; (R3) no read at the previous and the cut's pin
// misses as pin below maxTx.
func TestHeadCountsSigmaShape(t *testing.T) {
	sh := p5DefaultShape()
	for _, be := range p5Backends(t) {
		t.Run(be.name, func(t *testing.T) {
			g, err := New(be.cfg())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = g.Close() })
			res := runP5Shape(t, g, sh, 0x5005)
			name := be.name
			t.Logf("%s reads:  %v", name, res.read)
			t.Logf("%s writes: %v; corrections planned from an entry %d, on skeletons %d; entries extended %d; tx snapshot rows copied %d",
				name, res.write, res.headWrites, res.metaWrites, res.overlays, res.copied)
			for cl, h := range res.byClass {
				t.Logf("%s   %s: %v", name, p5PinClassNames[cl], h)
			}
			t.Logf("%s no-entry reads: %d of nodes never left an entry, %d of nodes whose entry was dropped",
				name, res.noEntryFirst, res.noEntryAfter)
			t.Logf("%s history rows read: by reads %d, by writes %d; skeletons per read-phase list %.1f",
				name, res.histRead, res.histWrite, float64(res.read.metaRows)/float64(max(1, res.read.metaLists)))
			t.Logf("%s counts table:\n%s", name, p5Table(res))
			if got := res.read.door(headDoorOne); got != uint64(res.singular) { // #nosec G115 -- test counts
				t.Errorf("singular door counted %d reads, %d made", got, res.singular)
			}
			if got := res.read.door(headDoorMany); got != uint64(res.plural) { // #nosec G115 -- test counts
				t.Errorf("plural door counted %d reads, %d made", got, res.plural)
			}
			for d := range headDoors {
				if n := res.read.reads[d][headMissNotQualifying] + res.read.reads[d][headMissScan]; n != 0 {
					t.Errorf("door %d: %d reads counted not-qualifying or scan in a shape with neither", d, n)
				}
			}
			if res.sessionOne == 0 || res.sessionOneHit != res.sessionOne {
				t.Errorf("Session reads at the cut's pin: %d of %d hit", res.sessionOneHit, res.sessionOne)
			}
			if res.r1 == 0 || res.r1Hit != res.r1 {
				t.Errorf("R1: Session reads at the previous and the cut's pin: %d of %d hit with one generation pin", res.r1Hit, res.r1)
			}
			for k := 2; k <= 5; k++ {
				if res.r2Hit[k] != res.r2[k] {
					t.Errorf("R2: reads at one pin %d cuts back of an entry filled by then: %d of %d hit", k, res.r2Hit[k], res.r2[k])
				}
			}
			if res.r2[5] == 0 {
				t.Errorf("R2: no read at the K boundary (5 cuts back): %v", res.r2)
			}
			if n := res.byClass[p5PinTwo].reads[headDoorMany][headMissPinBelow]; n != 0 {
				t.Errorf("R3: %d reads at the previous and the cut's pin missed as pin below maxTx", n)
			}
		})
	}
}

// BenchmarkHeadCountsSigmaShape is the test's cut loop as a benchmark: per
// backend, one fresh graph and the whole loop per iteration, with the counts
// per read and per cut as metrics.
func BenchmarkHeadCountsSigmaShape(b *testing.B) {
	sh := p5DefaultShape()
	for _, be := range p5Backends(b) {
		b.Run(be.name, func(b *testing.B) {
			var res p5Result
			for range b.N {
				g, err := New(be.cfg())
				if err != nil {
					b.Fatal(err)
				}
				res = runP5Shape(b, g, sh, 0x5005)
				_ = g.Close()
			}
			reads := float64(res.singular + res.plural)
			hits := float64(res.read.reads[headDoorOne][headHit] + res.read.reads[headDoorMany][headHit])
			b.ReportMetric(hits/reads, "hits/read")
			b.ReportMetric(float64(res.read.reads[headDoorMany][headMissPinBelow])/reads, "pin-below/read")
			b.ReportMetric(float64(res.read.metaRows)/float64(sh.cuts), "skeletons/cut")
			b.ReportMetric(float64(res.histRead)/float64(sh.cuts), "hist-rows-read/cut")
			b.ReportMetric(float64(res.histWrite)/float64(sh.cuts), "hist-rows-write/cut")
		})
	}
}

// TestHeadCountsReasons drives each reason once on every backend and checks
// the one count it moves: a first read (no entry), a second (hit), a plural
// read below the newest TxFrom, a node without history, a chain with a
// delete (not qualifying, whatever entry the lookup found), a scan, a
// node-at read of history and a correction planned without an entry.
func TestHeadCountsReasons(t *testing.T) {
	ctx := context.Background()
	for _, be := range p5Backends(t) {
		t.Run(be.name, func(t *testing.T) {
			g, err := New(be.cfg())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = g.Close() })
			expect := func(what string, door headDoor, want headReason, fn func() error) {
				t.Helper()
				before := headCountsOf(g)
				if err := fn(); err != nil {
					t.Fatalf("%s: %v", what, err)
				}
				got := headCountsOf(g).sub(before)
				if r, ok := got.only(door); !ok || r != want || got.door(1-door) != 0 {
					t.Errorf("%s: want one %s read at door %d, got %v", what, headReasonNames[want], door, got)
				}
			}
			n, err := g.Nodes.Add(ctx, []string{"Derived"}, map[string]any{"v": int64(0), "tkg_valid_from": int64(100)})
			if err != nil {
				t.Fatal(err)
			}
			id := n.ID()
			one := func() error { _, err := g.Temporal.NodeEffectiveTimeline(id, g.peekNow()); return err }
			if _, known, _ := (entityReads{c: g}).hasNodeHistory(id); known {
				expect("no history", headDoorOne, headCurrentOnly, one)
			}
			pin0 := g.now() // reserved: the Update stamps above it
			if _, err := g.Nodes.Update(ctx, id, map[string]any{"v": int64(1)}); err != nil {
				t.Fatal(err)
			}
			expect("first read", headDoorOne, headMissNoEntry, one)
			expect("second read", headDoorOne, headHit, one)
			expect("plural at the head", headDoorMany, headHit, func() error {
				_, err := g.Temporal.NodeEffectiveTimelines(id, []types.Instant{g.peekNow()})
				return err
			})
			expect("plural below the newest TxFrom", headDoorMany, headMissPinBelow, func() error {
				_, err := g.Temporal.NodeEffectiveTimelines(id, []types.Instant{pin0, g.peekNow()})
				return err
			})
			// Option C: an Update that extends the entry keeps the entry it
			// supersedes as a generation for the pins between the two newest
			// TxFroms; below the lowest generation stays pin below.
			pin1 := g.now() // reserved: the Update stamps above it
			gens := g.headGens.Load()
			if _, err := g.Nodes.Update(ctx, id, map[string]any{"v": int64(5)}); err != nil {
				t.Fatal(err)
			}
			if got := g.headGens.Load() - gens; got != 1 {
				t.Errorf("an Update extending a valid entry pushed %d generations, want 1", got)
			}
			genHit := func(what string, door headDoor, fn func() error) {
				t.Helper()
				before := headCountsOf(g)
				expect(what, door, headHit, fn)
				if got := headCountsOf(g).sub(before); got.genHits[door] != 1 || got.genHits[1-door] != 0 {
					t.Errorf("%s: want one generation hit at door %d, got %v", what, door, got)
				}
			}
			genHit("plural between the two newest TxFroms", headDoorMany, func() error {
				_, err := g.Temporal.NodeEffectiveTimelines(id, []types.Instant{pin1, g.peekNow()})
				return err
			})
			genHit("singular between the two newest TxFroms", headDoorOne, func() error {
				_, err := g.Temporal.NodeEffectiveTimeline(id, pin1)
				return err
			})
			expect("plural below the lowest generation", headDoorMany, headMissPinBelow, func() error {
				_, err := g.Temporal.NodeEffectiveTimelines(id, []types.Instant{pin0, g.peekNow()})
				return err
			})
			expect("scan", headDoorOne, headMissScan, func() error {
				return g.Temporal.NodesEffectiveByIDs([]types.NodeID{id}, g.peekNow(), func(_ temporal.NodeSegment) bool { return true })
			})
			// node-at: a read below the current row's TxFrom takes the history.
			before := headCountsOf(g)
			if _, err := g.Temporal.NodeAtTx(id, 200, pin0); err != nil && !errors.Is(err, storepkg.ErrNoVersionValidAt) {
				t.Fatal(err)
			}
			if got := headCountsOf(g).sub(before); got.nodeAt != 1 || got.door(headDoorOne)+got.door(headDoorMany) != 0 {
				t.Errorf("node-at read: %v", got)
			}
			// A correction with no valid entry plans on the history.
			if _, err := g.Nodes.Update(ctx, id, map[string]any{"v": int64(2)}); err != nil {
				t.Fatal(err)
			}
			g.heads.put(headKey{id: id.SnowflakeID()}, nil)
			before = headCountsOf(g)
			if _, err := g.Temporal.SetNodeVersionInterval(ctx, id, 300, 400, map[string]any{"v": int64(3)}); err != nil {
				t.Fatal(err)
			}
			if got := headCountsOf(g).sub(before); got.missPlans != 1 {
				t.Errorf("correction without an entry: %v", got)
			}
			// A delete and a re-import: the stale entry's lookup says the
			// token moved, the fold says the chain cannot qualify.
			expect("read before the delete", headDoorOne, headMissNoEntry, one)
			expect("hit before the delete", headDoorOne, headHit, one)
			if err := g.Nodes.Delete(ctx, id); err != nil {
				t.Fatal(err)
			}
			if _, err := g.Nodes.Import(ctx, id, []string{"Derived"}, map[string]any{"v": int64(4), "tkg_valid_from": int64(1000)}); err != nil {
				t.Fatal(err)
			}
			expect("read after delete and re-import", headDoorOne, headMissNotQualifying, one)
			expect("again", headDoorOne, headMissNotQualifying, one)
		})
	}
}
