package core

import (
	"context"
	"errors"

	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// The head-answer cache (effective_head.go, backlog 68) against the fold: a
// random sequence of corrections (SetVersionInterval bounded and open, gaps,
// at the clock and at a caller instant equal to the newest stamp), excises
// (void rows), Updates (replacing rows: TxTo), CloseVersion, GraphTx commits
// and rollbacks, deletes and history compaction, on nodes and relationships,
// on memory, badger, sharded and tiered. After every step each entity's
// timeline is read twice at the head pin (a miss that keeps the answer, then
// a hit) and at random earlier pins, and every answer must equal the fold
// (headCacheOff) segment for segment — same bounds, same row (version,
// stamps, content) — or fail with the same error.
//
// Catches: an entry kept across a write the token missed (a rollback trim, a
// compaction, an Update's TxTo), an overlay that ignores a supersession, a
// life end, an arm change or an open row's positional end, a winner hydrated
// from the wrong row, a cached answer served below the chain's newest TxFrom.

func TestEffectiveHead_DifferentialOracle(t *testing.T) {
	t.Parallel()
	seeds, steps := 3, 60
	if !testing.Short() {
		seeds, steps = 8, 120
	}
	if isRaceEnabled() {
		seeds, steps = 1, 50
	}
	if n, err := strconv.Atoi(os.Getenv("HEAD_ORACLE_SEEDS")); err == nil && n > 0 {
		seeds = n
	}
	var hits, overlays, checks uint64
	for _, be := range r10AllBackends(t) {
		for s := range seeds {
			seed := uint64(0x68_00 + s)
			t.Run(fmt.Sprintf("%s/seed=%d", be.name, seed), func(t *testing.T) {
				cfg := be.cfg()
				cfg.AllowTxBackfill = true
				g, err := New(cfg)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = g.Close() })
				h := &headOracle{t: t, g: g, rng: rand.New(rand.NewPCG(seed, 68))}
				h.setup()
				for step := range steps {
					h.step(step)
					h.checkAll(fmt.Sprintf("step %d (%s)", step, h.last))
				}
				hits += g.headHits.Load()
				overlays += g.headOverlays.Load()
				checks += h.checks
			})
		}
	}
	t.Logf("%d comparisons, %d head reads answered from an entry, %d corrections extended one", checks, hits, overlays)
	if hits == 0 || overlays == 0 {
		t.Fatalf("the generator never exercised the cache: %d hits, %d overlays", hits, overlays)
	}
}

type headOracle struct {
	t      *testing.T
	g      *Core
	rng    *rand.Rand
	nodes  []types.NodeID
	rels   []types.RelID
	pins   []types.Instant
	last   string
	checks uint64
}

func (h *headOracle) props() map[string]any {
	return map[string]any{"c": int64(h.rng.IntN(3)), "w": int64(h.rng.IntN(1000))}
}

func (h *headOracle) setup() {
	ctx := context.Background()
	for i := range 3 {
		p := h.props()
		if i > 0 {
			p["tkg_valid_from"] = int64(100 * i)
		}
		if i == 2 {
			p["tkg_valid_to"] = int64(900)
		}
		n, err := h.g.Nodes.Add(ctx, []string{"H"}, p)
		if err != nil {
			h.t.Fatal(err)
		}
		h.nodes = append(h.nodes, n.ID())
	}
	for i := range 2 {
		p := h.props()
		p["tkg_valid_from"] = int64(50 + 100*i)
		r, err := h.g.Rels.AddByID(ctx, "HR", h.nodes[0], h.nodes[1], p)
		if err != nil {
			h.t.Fatal(err)
		}
		h.rels = append(h.rels, r.ID())
	}
}

// interval draws [a, b) on a small grid (b 0: open, one draw in four).
func (h *headOracle) interval() (types.Instant, types.Instant) {
	a := types.Instant(50 * (1 + h.rng.IntN(20)))
	if h.rng.IntN(4) == 0 {
		return a, 0
	}
	return a, a + types.Instant(50*(1+h.rng.IntN(6)))
}

func (h *headOracle) step(step int) {
	ctx := context.Background()
	g := h.g
	rel := h.rng.IntN(3) == 0
	nid := h.nodes[h.rng.IntN(len(h.nodes))]
	rid := h.rels[h.rng.IntN(len(h.rels))]
	a, b := h.interval()
	var err error
	op := h.rng.IntN(100)
	switch {
	case op < 40: // the R10 shape: a correction, often of one validity
		if h.rng.IntN(2) == 0 {
			a, b = 1000, 2000
		}
		h.last = fmt.Sprintf("SetVersionInterval [%d,%d) rel=%v", a, b, rel)
		if rel {
			_, err = g.Temporal.SetRelVersionInterval(ctx, rid, a, b, h.props())
		} else {
			_, err = g.Temporal.SetNodeVersionInterval(ctx, nid, a, b, h.props())
		}
	case op < 48: // at a caller instant just above the entity's newest stamp
		var f, to types.Instant
		if rel {
			f, to, _, _ = g.Rels.LatestStamps(rid)
		} else {
			f, to, _, _ = g.Nodes.LatestStamps(nid)
		}
		at := max(f, to) + 1
		h.last = fmt.Sprintf("SetVersionIntervalWithTx [%d,%d) at %d rel=%v", a, b, at, rel)
		if rel {
			_, err = g.Temporal.SetRelVersionIntervalWithTx(ctx, rid, a, b, h.props(), at)
		} else {
			_, err = g.Temporal.SetNodeVersionIntervalWithTx(ctx, nid, a, b, h.props(), at)
		}
	case op < 60: // GraphTx: commit or roll back
		commit := h.rng.IntN(2) == 0
		h.last = fmt.Sprintf("GraphTx SetVersionInterval [%d,%d) commit=%v rel=%v", a, b, commit, rel)
		tx, terr := g.BeginTx()
		if terr != nil {
			h.t.Fatal(terr)
		}
		if rel {
			_, err = tx.SetRelVersionInterval(rid, a, b, h.props())
		} else {
			_, err = tx.SetNodeVersionInterval(nid, a, b, h.props())
		}
		if commit && err == nil {
			err = tx.Commit()
		} else {
			_ = tx.Rollback()
			// The rolled-back versions are reused by the next write: a kept
			// answer naming them must not survive the rollback.
			a2, b2 := h.interval()
			h.last += fmt.Sprintf(", then SetVersionInterval [%d,%d)", a2, b2)
			if rel {
				_, err = g.Temporal.SetRelVersionInterval(ctx, rid, a2, b2, h.props())
			} else {
				_, err = g.Temporal.SetNodeVersionInterval(ctx, nid, a2, b2, h.props())
			}
		}
	case op < 70: // excise: void rows
		if b == 0 {
			b = a + 100
		}
		h.last = fmt.Sprintf("Excise [%d,%d) rel=%v", a, b, rel)
		if rel {
			err = g.Temporal.ExciseRelValidity(ctx, rid, a, b)
		} else {
			err = g.Temporal.ExciseNodeValidity(ctx, nid, a, b)
		}
	case op < 78: // a replacing write
		h.last = fmt.Sprintf("Update rel=%v", rel)
		if rel {
			_, err = g.Rels.Update(ctx, rid, h.props())
		} else {
			_, err = g.Nodes.Update(ctx, nid, h.props())
		}
	case op < 82: // the current row's validity rewritten in place: no history write
		p := h.props()
		p["tkg_valid_to"] = int64(a + 2000)
		h.last = fmt.Sprintf("UpdateInPlace valid_to %d rel=%v", a+2000, rel)
		if rel {
			_, err = g.Rels.UpdateInPlace(ctx, rid, p)
		} else {
			_, err = g.Nodes.UpdateInPlace(ctx, nid, p)
		}
	case op < 90:
		h.last = fmt.Sprintf("CloseVersion %d rel=%v", a+1000, rel)
		if rel {
			err = g.Rels.CloseVersion(ctx, rid, a+1000)
		} else {
			err = g.Nodes.CloseVersion(ctx, nid, a+1000)
		}
	case op < 93 && step > 10: // delete, or re-import a deleted relationship (a second life)
		if _, gerr := g.Rels.Get(ctx, rid); errors.Is(gerr, storepkg.ErrRelNotFound) {
			h.last = "Rels.Import (second life)"
			s, err1 := g.Nodes.Get(ctx, h.nodes[0])
			e, err2 := g.Nodes.Get(ctx, h.nodes[1])
			if err1 == nil && err2 == nil {
				_, err = g.Rels.Import(ctx, rid, "HR", s, e, map[string]any{"tkg_valid_from": int64(a), "life": int64(2)})
			}
			break
		}
		h.last = fmt.Sprintf("Delete rel %v", rid)
		err = g.Rels.Delete(ctx, rid)
	case op < 96:
		keep := 1 + h.rng.IntN(4)
		h.last = fmt.Sprintf("Compact KeepVersions %d", keep)
		_, err = g.Admin.CompactHistoryNodes(ctx, RetentionPolicy{KeepVersions: keep})
		if err == nil {
			_, err = g.Admin.CompactHistoryRels(ctx, RetentionPolicy{KeepVersions: keep})
		}
	default:
		h.last = "read only"
	}
	_ = err // refusals (no version valid, deleted, ...) are part of the sequence
	h.pins = append(h.pins, g.peekNow())
}

func (h *headOracle) pickPins() []types.Instant {
	head := h.g.peekNow()
	out := []types.Instant{head, head}
	for range 3 {
		if len(h.pins) == 0 {
			break
		}
		p := h.pins[h.rng.IntN(len(h.pins))] + types.Instant(h.rng.IntN(3)-1)
		if p > 0 && p <= head {
			out = append(out, p)
		}
	}
	return out
}

func (h *headOracle) checkAll(when string) {
	g := h.g
	for _, id := range h.nodes {
		for _, pin := range h.pickPins() {
			got, gerr := g.Temporal.NodeEffectiveTimeline(id, pin)
			g.headCacheOff = true
			want, werr := g.Temporal.NodeEffectiveTimeline(id, pin)
			g.headCacheOff = false
			h.checks++
			if fmt.Sprint(gerr) != fmt.Sprint(werr) || etSegsString(etNodeSegs(got)) != etSegsString(etNodeSegs(want)) {
				h.t.Fatalf("%s: node %v pin %d (head %d):\n cached %v%s\n fold   %v%s", when, id, pin, g.peekNow(),
					gerr, etSegsString(etNodeSegs(got)), werr, etSegsString(etNodeSegs(want)))
			}
		}
	}
	for _, id := range h.rels {
		for _, pin := range h.pickPins() {
			got, gerr := g.Temporal.RelEffectiveTimeline(id, pin)
			g.headCacheOff = true
			want, werr := g.Temporal.RelEffectiveTimeline(id, pin)
			g.headCacheOff = false
			h.checks++
			if fmt.Sprint(gerr) != fmt.Sprint(werr) || etSegsString(etRelSegs(got)) != etSegsString(etRelSegs(want)) {
				h.t.Fatalf("%s: rel %v pin %d (head %d):\n cached %v%s\n fold   %v%s", when, id, pin, g.peekNow(),
					gerr, etSegsString(etRelSegs(got)), werr, etSegsString(etRelSegs(want)))
			}
		}
	}
}
