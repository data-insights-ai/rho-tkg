package core

import (
	"context"
	"errors"
	"testing"
	"time"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Row 104 (sigma O1-3 P1): NodeLowestAnsweredPin / RelLowestAnsweredPin and
// the plural forms return, per id, the lowest transaction pin at which the
// singular reads answer without ErrHistoryCompacted / ErrRetentionExpired:
// the max of the compaction stub's LastTrimmedTxTo (under the graph gate),
// the node's label retention watermarks, and for a relationship its type
// watermark (checkRelTypePointRetention) and the node-family max
// (checkRelPointRetention). sigma replaces ~50 binary-search calls per
// expired entity with it.
//
// The oracle is that binary search, done here over the singular door
// (NodeEffectiveTimeline / RelEffectiveTimeline) from pin 1 to the commit
// clock, plus the as-of door at the boundary. Break cases: never compacted
// (0), a compacted stub, a label watermark above the stub and one below it,
// several labels, the rel-type watermark, a relationship minted after every
// type watermark, the node family lifting relationships, a purged entity,
// an unknown id, a deleted entity, id 0, empty and repeated plural ids,
// a closed graph.

// lapRefused reports whether the singular door refuses pin with a
// compaction or retention sentinel; other errors (not found) are answers.
func lapRefused(t *testing.T, g *Core, rel bool, id int64, pin types.Instant) bool {
	t.Helper()
	var err error
	if rel {
		_, err = g.Temporal.RelEffectiveTimeline(types.RelID(id), pin)
	} else {
		_, err = g.Temporal.NodeEffectiveTimeline(types.NodeID(id), pin)
	}
	switch {
	case errors.Is(err, ErrHistoryCompacted), errors.Is(err, ErrRetentionExpired):
		return true
	case err == nil, errors.Is(err, storepkg.ErrNodeNotFound), errors.Is(err, storepkg.ErrRelNotFound):
		return false
	}
	t.Fatalf("singular door at %d: %v", pin, err)
	return false
}

// lapAsOfRefused is lapRefused over the as-of door (positive pins only).
func lapAsOfRefused(t *testing.T, g *Core, rel bool, id int64, pin types.Instant) bool {
	t.Helper()
	var err error
	if rel {
		_, err = g.Temporal.RelAsOf(types.RelID(id), pin)
	} else {
		_, err = g.Temporal.NodeAsOf(types.NodeID(id), pin)
	}
	return errors.Is(err, ErrHistoryCompacted) || errors.Is(err, ErrRetentionExpired)
}

// lapOracle binary-searches the lowest positive pin in [1, PeekTx] the
// singular door answers, 0 when pin 1 answers (the door's "no floor"); above
// is true when every pin up to the clock is refused (the floor lies above
// the clock). Pins at or below 0 are outside the door's contract: they read
// "nothing recorded yet", and an active retention gate refuses them even for
// a type never purged (checkRelTypePointRetention compares with a zero
// watermark).
func lapOracle(t *testing.T, g *Core, rel bool, id int64) (want types.Instant, above bool) {
	t.Helper()
	peek, err := g.Temporal.PeekTx()
	if err != nil {
		t.Fatal(err)
	}
	if !lapRefused(t, g, rel, id, 1) {
		return 0, false
	}
	if lapRefused(t, g, rel, id, peek) {
		return peek, true
	}
	lo, hi := types.Instant(1), peek // lo refused, hi answered
	for hi-lo > 1 {
		mid := lo + (hi-lo)/2
		if lapRefused(t, g, rel, id, mid) {
			lo = mid
		} else {
			hi = mid
		}
	}
	return hi, false
}

// lapGot calls the singular door and the plural door with the one id; both
// must agree.
func lapGot(t *testing.T, g *Core, rel bool, id int64) types.Instant {
	t.Helper()
	var one types.Instant
	var many []types.Instant
	var err1, err2 error
	if rel {
		one, err1 = g.Temporal.RelLowestAnsweredPin(types.RelID(id))
		many, err2 = g.Temporal.RelsLowestAnsweredPins([]types.RelID{types.RelID(id)})
	} else {
		one, err1 = g.Temporal.NodeLowestAnsweredPin(types.NodeID(id))
		many, err2 = g.Temporal.NodesLowestAnsweredPins([]types.NodeID{types.NodeID(id)})
	}
	if err1 != nil || err2 != nil {
		t.Fatalf("LowestAnsweredPin(%d): %v / plural %v", id, err1, err2)
	}
	if len(many) != 1 || many[0] != one {
		t.Fatalf("LowestAnsweredPin(%d) = %d, plural %v", id, one, many)
	}
	return one
}

// lapCheck asserts the door equals the oracle for id and returns the floor.
func lapCheck(t *testing.T, g *Core, rel bool, what string, id int64) types.Instant {
	t.Helper()
	got := lapGot(t, g, rel, id)
	want, above := lapOracle(t, g, rel, id)
	if above {
		if got <= want {
			t.Errorf("%s: LowestAnsweredPin = %d; every pin up to the clock %d is refused", what, got, want)
		}
		return got
	}
	if got != want && (want != 0 || got > 1) { // pin 1 answers: 0 or 1
		t.Errorf("%s: LowestAnsweredPin = %d, want %d (binary search over the singular door)", what, got, want)
	}
	if want >= 2 && (!lapAsOfRefused(t, g, rel, id, want-1) || lapAsOfRefused(t, g, rel, id, want)) {
		t.Errorf("%s: the as-of door disagrees at the floor %d", what, want)
	}
	return got
}

func lapNode(t *testing.T, g *Core, labels ...string) int64 {
	t.Helper()
	n, err := g.Nodes.Add(context.Background(), labels, map[string]any{"x": int64(0)})
	if err != nil {
		t.Fatalf("Add(%v): %v", labels, err)
	}
	return int64(n.ID())
}

func lapRel(t *testing.T, g *Core, typ string, s, e types.NodeID) int64 {
	t.Helper()
	r, err := g.Rels.AddByID(context.Background(), typ, s, e, map[string]any{"x": int64(0)})
	if err != nil {
		t.Fatalf("AddByID(%s): %v", typ, err)
	}
	return int64(r.ID())
}

func lapUpdate(t *testing.T, g *Core, rel bool, id int64, n int) {
	t.Helper()
	for i := 1; i <= n; i++ {
		var err error
		if rel {
			_, err = g.Rels.Update(context.Background(), types.RelID(id), map[string]any{"x": int64(i)})
		} else {
			_, err = g.Nodes.Update(context.Background(), types.NodeID(id), map[string]any{"x": int64(i)})
		}
		if err != nil {
			t.Fatalf("update %d: %v", id, err)
		}
	}
}

// lapCompact compacts every chain to its newest version; false when the
// backend has no history compaction (sharded, tiered): the stub cases then
// hold a floor of 0, still checked against the oracle.
func lapCompact(t *testing.T, g *Core, rel bool) bool {
	t.Helper()
	var err error
	if rel {
		_, err = g.Admin.CompactHistoryRels(context.Background(), RetentionPolicy{KeepVersions: 1})
	} else {
		_, err = g.Admin.CompactHistoryNodes(context.Background(), RetentionPolicy{KeepVersions: 1})
	}
	if errors.Is(err, storepkg.ErrCapabilityNotSupported) {
		return false
	}
	if err != nil {
		t.Fatalf("compact: %v", err)
	}
	return true
}

func lapPin(t *testing.T, g *Core) types.Instant {
	t.Helper()
	p, err := g.Temporal.NowTx()
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLowestAnsweredPin(t *testing.T) {
	ctx := context.Background()
	for _, be := range txbBackends(func(c *Config) { c.AllowRetentionPurge = true }) {
		t.Run(be.name, func(t *testing.T) {
			t.Run("node", func(t *testing.T) {
				g := be.open(t, false)
				fresh := lapNode(t, g, "F")
				if got := lapCheck(t, g, false, "never compacted", fresh); got != 0 {
					t.Errorf("never compacted: %d, want 0", got)
				}
				a, b := lapNode(t, g, "A"), lapNode(t, g, "B")
				both := lapNode(t, g, "A", "B")
				gone := lapNode(t, g, "B")
				for _, id := range []int64{a, b, both, gone} {
					lapUpdate(t, g, false, id, 2)
				}
				compacted := lapCompact(t, g, false)
				stubA := lapCheck(t, g, false, "compacted stub", a)
				if compacted && stubA <= 0 {
					t.Fatalf("compacted stub: floor %d, want > 0", stubA)
				}
				// A label watermark below the stub: the stub decides. One above
				// it (B): the watermark decides, for every node carrying B.
				if compacted {
					if err := g.advanceRetentionWatermark(nodeLabelToken(t, g, "A"), stubA-1); err != nil {
						t.Fatal(err)
					}
				}
				wB := lapPin(t, g)
				if err := g.advanceRetentionWatermark(nodeLabelToken(t, g, "B"), wB); err != nil {
					t.Fatal(err)
				}
				if got := lapCheck(t, g, false, "label watermark below the stub", a); got != stubA {
					t.Errorf("label watermark below the stub: %d, want the stub %d", got, stubA)
				}
				for what, id := range map[string]int64{"label watermark above the stub": b, "two labels": both} {
					if got := lapCheck(t, g, false, what, id); got != wB {
						t.Errorf("%s: %d, want the B watermark %d", what, got, wB)
					}
				}
				if got := lapCheck(t, g, false, "label without a watermark", fresh); got != 0 {
					t.Errorf("label without a watermark: %d, want 0", got)
				}
				if err := g.Nodes.Delete(ctx, types.NodeID(gone)); err != nil {
					t.Fatal(err)
				}
				if got := lapCheck(t, g, false, "deleted", gone); got != wB {
					t.Errorf("deleted: %d, want the B watermark %d (labels from history)", got, wB)
				}
				lapCheck(t, g, false, "unknown id", fresh+1)

				// Plural: aligned with the ids, repeated and unsorted.
				ids := []types.NodeID{types.NodeID(both), types.NodeID(fresh), types.NodeID(a), types.NodeID(both), types.NodeID(gone)}
				got, err := g.Temporal.NodesLowestAnsweredPins(ids)
				if err != nil || len(got) != len(ids) {
					t.Fatalf("plural: %v, %v", got, err)
				}
				for i, id := range ids {
					if one := lapGot(t, g, false, int64(id)); got[i] != one {
						t.Errorf("plural[%d] (id %d) = %d, singular %d", i, id, got[i], one)
					}
				}
			})

			t.Run("rel", func(t *testing.T) {
				g := be.open(t, false)
				s, e := txbEndpoints(t, g)
				fresh := lapRel(t, g, "TF", s, e)
				if got := lapCheck(t, g, true, "never compacted", fresh); got != 0 {
					t.Errorf("never compacted: %d, want 0", got)
				}
				comp := lapRel(t, g, "TA", s, e)
				typed := lapRel(t, g, "TB", s, e)
				gone := lapRel(t, g, "TB", s, e)
				lapUpdate(t, g, true, comp, 2)
				compacted := lapCompact(t, g, true)
				stub := lapCheck(t, g, true, "compacted stub", comp)
				if compacted && stub <= 0 {
					t.Fatalf("compacted stub: floor %d, want > 0", stub)
				}
				tokA, _ := g.relTypes.Lookup("TA")
				tokB, _ := g.relTypes.Lookup("TB")
				if compacted {
					if err := g.advanceRelRetentionWatermark(tokA, stub-1); err != nil {
						t.Fatal(err)
					}
				}
				wB := lapPin(t, g)
				if err := g.advanceRelRetentionWatermark(tokB, wB); err != nil {
					t.Fatal(err)
				}
				if got := lapCheck(t, g, true, "type watermark below the stub", comp); got != stub {
					t.Errorf("type watermark below the stub: %d, want the stub %d", got, stub)
				}
				if got := lapCheck(t, g, true, "type watermark", typed); got != wB {
					t.Errorf("type watermark: %d, want %d", got, wB)
				}
				if got := lapCheck(t, g, true, "type without a watermark", fresh); got != 0 {
					t.Errorf("type without a watermark: %d, want 0", got)
				}
				if err := g.Rels.Delete(ctx, types.RelID(gone)); err != nil {
					t.Fatal(err)
				}
				if got := lapCheck(t, g, true, "deleted", gone); got != wB {
					t.Errorf("deleted: %d, want %d (type from history)", got, wB)
				}
				if got := lapCheck(t, g, true, "unknown id minted below the type max", typed+1); got != wB {
					t.Errorf("unknown id: %d, want the type max %d", got, wB)
				}
				// Minted after every type watermark: never a purge candidate.
				time.Sleep(time.Duration(30) * time.Millisecond)
				late := lapRel(t, g, "TB", s, e)
				if types.RelID(late).MintInstant() < wB {
					t.Fatalf("fixture: late rel minted at %d, below %d", types.RelID(late).MintInstant(), wB)
				}
				if got := lapCheck(t, g, true, "minted after the type max", late); got != 0 {
					t.Errorf("minted after the type max: %d, want 0", got)
				}
				// The node family lifts every relationship (checkRelPointRetention).
				wN := lapPin(t, g)
				if err := g.advanceRetentionWatermark(nodeLabelToken(t, g, "Zed"), wN); err != nil {
					t.Fatal(err)
				}
				for what, id := range map[string]int64{"node family, fresh": fresh, "node family, compacted": comp, "node family, typed": typed, "node family, late": late} {
					if got := lapCheck(t, g, true, what, id); got != wN {
						t.Errorf("%s: %d, want the node max %d", what, got, wN)
					}
				}
				ids := []types.RelID{types.RelID(late), types.RelID(comp), types.RelID(late), types.RelID(gone)}
				got, err := g.Temporal.RelsLowestAnsweredPins(ids)
				if err != nil || len(got) != len(ids) {
					t.Fatalf("plural: %v, %v", got, err)
				}
				for i, id := range ids {
					if one := lapGot(t, g, true, int64(id)); got[i] != one {
						t.Errorf("plural[%d] (id %d) = %d, singular %d", i, id, got[i], one)
					}
				}
			})

			t.Run("purged", func(t *testing.T) {
				g := be.open(t, false)
				s, e := txbEndpoints(t, g)
				old := lapNode(t, g, "Old")
				lapUpdate(t, g, false, old, 1)
				link := lapRel(t, g, "LINK", s, types.NodeID(old))
				ev := lapRel(t, g, "EVENT", s, e)
				time.Sleep(time.Duration(30) * time.Millisecond)
				kept := lapNode(t, g, "Old")
				evKept := lapRel(t, g, "EVENT", s, e)
				nodeB, relB := types.NodeID(kept).MintInstant(), types.RelID(evKept).MintInstant()
				if _, err := g.Admin.PurgeExpiredNodes(ctx, PurgePolicy{Label: "Old", Before: nodeB}); err != nil {
					if errors.Is(err, storepkg.ErrCapabilityNotSupported) {
						t.Skip(err)
					}
					t.Fatal(err)
				}
				if _, err := g.Admin.PurgeExpiredRels(ctx, RelPurgePolicy{Type: "EVENT", Before: relB}); err != nil {
					t.Fatal(err)
				}
				// Part A: a purged node is not found at every pin; nothing refuses.
				if got := lapCheck(t, g, false, "purged node", old); got != 0 {
					t.Errorf("purged node: %d, want 0 (no gate refuses; every pin answers not found)", got)
				}
				// Gone, so its type is unknown: the relationship max counts too.
				if got := lapCheck(t, g, true, "rel purged with its node", link); got != max(relB, nodeB) {
					t.Errorf("rel purged with its node: %d, want max(node watermark %d, rel max %d)", got, nodeB, relB)
				}
				if got := lapCheck(t, g, true, "rel purged by type", ev); got != max(relB, nodeB) {
					t.Errorf("rel purged by type: %d, want %d", got, max(relB, nodeB))
				}
				lapCheck(t, g, false, "kept node", kept)
				lapCheck(t, g, true, "kept rel", evKept)
			})

			t.Run("break", func(t *testing.T) {
				g := be.open(t, false)
				s, e := txbEndpoints(t, g)
				n := lapNode(t, g, "F")
				r := lapRel(t, g, "TF", s, e)
				if _, err := g.Temporal.NodeLowestAnsweredPin(0); !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
					t.Errorf("node id 0: %v", err)
				}
				if _, err := g.Temporal.RelLowestAnsweredPin(0); !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
					t.Errorf("rel id 0: %v", err)
				}
				if got, err := g.Temporal.NodesLowestAnsweredPins([]types.NodeID{types.NodeID(n), 0}); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || got != nil {
					t.Errorf("plural with id 0: %v, %v; want nil, ErrInvalidStoreMutation", got, err)
				}
				if got, err := g.Temporal.RelsLowestAnsweredPins([]types.RelID{0, types.RelID(r)}); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || got != nil {
					t.Errorf("plural with id 0: %v, %v; want nil, ErrInvalidStoreMutation", got, err)
				}
				for _, ids := range [][]types.NodeID{nil, {}} {
					if got, err := g.Temporal.NodesLowestAnsweredPins(ids); err != nil || got == nil || len(got) != 0 {
						t.Errorf("empty node ids %v: %v, %v; want empty, nil", ids, got, err)
					}
				}
				if got, err := g.Temporal.RelsLowestAnsweredPins(nil); err != nil || got == nil || len(got) != 0 {
					t.Errorf("empty rel ids: %v, %v; want empty, nil", got, err)
				}
				if err := g.Close(); err != nil {
					t.Fatal(err)
				}
				if _, err := g.Temporal.NodeLowestAnsweredPin(types.NodeID(n)); !errors.Is(err, ErrGraphClosed) {
					t.Errorf("closed node: %v", err)
				}
				if _, err := g.Temporal.RelLowestAnsweredPin(types.RelID(r)); !errors.Is(err, ErrGraphClosed) {
					t.Errorf("closed rel: %v", err)
				}
				if got, err := g.Temporal.NodesLowestAnsweredPins([]types.NodeID{types.NodeID(n)}); !errors.Is(err, ErrGraphClosed) || got != nil {
					t.Errorf("closed plural nodes: %v, %v", got, err)
				}
				if got, err := g.Temporal.RelsLowestAnsweredPins([]types.RelID{types.RelID(r)}); !errors.Is(err, ErrGraphClosed) || got != nil {
					t.Errorf("closed plural rels: %v, %v", got, err)
				}
			})
		})
	}
}
