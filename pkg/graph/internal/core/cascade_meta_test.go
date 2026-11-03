package core

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	shardedpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// CASCADEMETA (sigma's RELHIST profile, 3 Nov): a correction on a head miss
// (no valid head entry: a chain with a delete or retraction never has one)
// plans on the history's temporal skeletons and reads full rows only for
// the rows it copies — each segment base and the resumption source — instead
// of the whole history.

// cascadeCountStore counts a cascade's history reads: whole-chain reads and
// point reads by version. Being a wrapper, it hides the native head-entry
// and history-stamp capabilities, so every correction on it is a head miss.
type cascadeCountStore struct {
	storepkg.MandatoryStore
	nodeHistory, relHistory, nodeVersion, relVersion atomic.Int64
}

func (s *cascadeCountStore) GetNodeHistory(id types.NodeID) ([]*types.Node, error) {
	s.nodeHistory.Add(1)
	return s.MandatoryStore.GetNodeHistory(id)
}

func (s *cascadeCountStore) GetRelHistory(id types.RelID) ([]*types.Relationship, error) {
	s.relHistory.Add(1)
	return s.MandatoryStore.GetRelHistory(id)
}

func (s *cascadeCountStore) GetNodeVersion(id types.NodeID, v uint32) (*types.Node, error) {
	s.nodeVersion.Add(1)
	return s.MandatoryStore.GetNodeVersion(id, v)
}

func (s *cascadeCountStore) GetRelVersion(id types.RelID, v uint32) (*types.Relationship, error) {
	s.relVersion.Add(1)
	return s.MandatoryStore.GetRelVersion(id, v)
}

func (s *cascadeCountStore) reset() {
	s.nodeHistory.Store(0)
	s.relHistory.Store(0)
	s.nodeVersion.Store(0)
	s.relVersion.Store(0)
}

// cascadeCountMetaStore is cascadeCountStore declaring the skeleton reads
// of the store it wraps (memory, badger, tiered).
type cascadeCountMetaStore struct{ *cascadeCountStore }

func (s *cascadeCountMetaStore) NodeHistoryTemporalMeta(id types.NodeID) ([]storepkg.VersionTemporalMeta, error) {
	return s.MandatoryStore.(storepkg.TemporalMetaHistoryCapability).NodeHistoryTemporalMeta(id)
}

func (s *cascadeCountMetaStore) RelHistoryTemporalMeta(id types.RelID) ([]storepkg.VersionTemporalMeta, error) {
	return s.MandatoryStore.(storepkg.TemporalMetaHistoryCapability).RelHistoryTemporalMeta(id)
}

type cascadeCountBackend struct {
	name string
	meta bool // the wrapped store offers the skeleton reads
	open func(tb testing.TB) storepkg.MandatoryStore
}

func cascadeCountBackends() []cascadeCountBackend {
	return []cascadeCountBackend{
		{"memory", true, func(testing.TB) storepkg.MandatoryStore { return memory.New() }},
		{"badger", true, func(tb testing.TB) storepkg.MandatoryStore {
			bs, err := badger.New(badger.Config{InMemory: true})
			if err != nil {
				tb.Fatalf("badger.New: %v", err)
			}
			tb.Cleanup(func() { _ = bs.Close() })
			return bs
		}},
		{"tiered", true, func(tb testing.TB) storepkg.MandatoryStore {
			ts, err := tiered.New(tiered.Config{InMemory: true, RefLabels: []string{"Ref"},
				ShardWindow: 7 * 24 * time.Hour, FlushInterval: 1<<63 - 1})
			if err != nil {
				tb.Fatalf("tiered.New: %v", err)
			}
			tb.Cleanup(func() { _ = ts.Close() })
			return ts
		}},
		{"sharded", false, func(tb testing.TB) storepkg.MandatoryStore {
			st, err := shardedpkg.New(shardedpkg.Config{InMemory: true, BaseSlot: 0, SlotCount: 2})
			if err != nil {
				tb.Fatalf("sharded.New: %v", err)
			}
			return st
		}},
	}
}

// openCountedGraph opens a graph on be's store behind the counting wrapper.
func openCountedGraph(t *testing.T, be cascadeCountBackend) (*Core, *cascadeCountStore) {
	t.Helper()
	cs := &cascadeCountStore{MandatoryStore: be.open(t)}
	var st storepkg.MandatoryStore = cs
	if be.meta {
		st = &cascadeCountMetaStore{cs}
	}
	g, err := New(Config{Store: st, AllowTxBackfill: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close() })
	if (g.temporalMetaHistory != nil) != be.meta {
		t.Fatalf("%s: skeleton reads offered = %v, want %v", be.name, g.temporalMetaHistory != nil, be.meta)
	}
	return g, cs
}

// cascadeMetaRelChain builds a relationship whose chain holds an ended life
// (end: "delete" or "retract") and a second life, each corrected three
// times; it returns the relationship's ID.
func cascadeMetaRelChain(t *testing.T, g *Core, end string) types.RelID {
	t.Helper()
	ctx := context.Background()
	s, err := g.Nodes.Add(ctx, []string{"H"}, map[string]any{"tkg_valid_from": int64(10)})
	if err != nil {
		t.Fatal(err)
	}
	e, err := g.Nodes.Add(ctx, []string{"H"}, map[string]any{"tkg_valid_from": int64(10)})
	if err != nil {
		t.Fatal(err)
	}
	r, err := g.Rels.Add(ctx, "HR", s, e, map[string]any{"tkg_valid_from": int64(1000), "k": int64(0), "keep": "first"})
	if err != nil {
		t.Fatal(err)
	}
	id := r.ID()
	for i := range 3 {
		vf := types.Instant(1000 + 100*i)
		if _, err := g.Temporal.SetRelVersionInterval(ctx, id, vf, vf+500, map[string]any{"k": int64(1 + i)}); err != nil {
			t.Fatal(err)
		}
	}
	switch end {
	case "delete":
		err = g.Rels.Delete(ctx, id)
	case "retract":
		err = g.Rels.Retract(ctx, id)
	}
	if err != nil {
		t.Fatalf("%s: %v", end, err)
	}
	if _, err := g.Rels.Import(ctx, id, "HR", s, e, map[string]any{"tkg_valid_from": int64(3000), "k": int64(10), "keep": "second"}); err != nil {
		t.Fatalf("re-import: %v", err)
	}
	for i := range 3 {
		vf := types.Instant(3000 + 100*i)
		if _, err := g.Temporal.SetRelVersionInterval(ctx, id, vf, vf+500, map[string]any{"k": int64(11 + i)}); err != nil {
			t.Fatal(err)
		}
	}
	return id
}

// cascadeMetaNodeChain mirrors cascadeMetaRelChain for a node.
func cascadeMetaNodeChain(t *testing.T, g *Core, end string) types.NodeID {
	t.Helper()
	ctx := context.Background()
	n, err := g.Nodes.Add(ctx, []string{"H"}, map[string]any{"tkg_valid_from": int64(1000), "k": int64(0), "keep": "first"})
	if err != nil {
		t.Fatal(err)
	}
	id := n.ID()
	for i := range 3 {
		vf := types.Instant(1000 + 100*i)
		if _, err := g.Temporal.SetNodeVersionInterval(ctx, id, vf, vf+500, map[string]any{"k": int64(1 + i)}); err != nil {
			t.Fatal(err)
		}
	}
	switch end {
	case "delete":
		err = g.Nodes.Delete(ctx, id)
	case "retract":
		err = g.Nodes.Retract(ctx, id)
	}
	if err != nil {
		t.Fatalf("%s: %v", end, err)
	}
	if _, err := g.Nodes.Import(ctx, id, []string{"H"}, map[string]any{"tkg_valid_from": int64(3000), "k": int64(10), "keep": "second"}); err != nil {
		t.Fatalf("re-import: %v", err)
	}
	for i := range 3 {
		vf := types.Instant(3000 + 100*i)
		if _, err := g.Temporal.SetNodeVersionInterval(ctx, id, vf, vf+500, map[string]any{"k": int64(11 + i)}); err != nil {
			t.Fatal(err)
		}
	}
	return id
}

// planWinners is the number of distinct history rows (not the current row)
// a correction of [vf, vt) copies — every non-gap segment base and the
// resumption source unless it starts at vt (no resumption is written) —
// planned on the whole chain (the reference), and whether the winner at vt
// starts there.
func planWinners[T chainRow](c *Core, k *chainKernel[T], history []T, current T, vf, vt types.Instant, splitGaps bool) (int, bool) {
	prep := prepareCorrection(c, k, preCorrectionChain(history, current))
	seen := map[T]bool{}
	resumeAt := false
	if !c.axis().Open(vt) {
		if src, ok := prep.winnerAt(vt); ok {
			if start, _ := k.ownBounds(c, src); start == vt {
				resumeAt = true
			} else {
				seen[src] = true
			}
		}
	}
	for _, seg := range prep.segments(current, vf, vt, splitGaps) {
		if !seg.gap {
			seen[seg.base] = true
		}
	}
	delete(seen, current)
	return len(seen), resumeAt
}

// TestCascadeMetaMissReadsNoHistory: a correction on a chain holding a
// delete or a retraction (a head miss every time) reads no whole history on
// memory, badger and tiered, and exactly one full row per distinct history
// row it copies (at most the distinct winners + 1): a bounded correction
// with a resumption, and one whose resumption source starts at its end (no
// resumption: that row is not read); sharded (no skeleton door on the
// store) keeps the whole-history read. Mutation: the skeleton path off
// (cascadeMetaOff) or a gate that always falls back reads the history;
// hydrating every skeleton, the source when no resumption is written, or
// without the memo reads more rows.
func TestCascadeMetaMissReadsNoHistory(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	// [3050, 3450): several winners and a resumption; [3050, 3200): the row
	// valid at 3200 starts there (no resumption).
	intervals := [][2]types.Instant{{3050, 3450}, {3050, 3200}}
	for _, be := range cascadeCountBackends() {
		for _, end := range []string{"delete", "retract"} {
			t.Run(be.name+"/"+end+"/rel", func(t *testing.T) {
				t.Parallel()
				g, cs := openCountedGraph(t, be)
				id := cascadeMetaRelChain(t, g, end)
				resumeAt := 0
				for _, iv := range intervals {
					cur, err := g.getCurrentRelationship(id)
					if err != nil {
						t.Fatal(err)
					}
					hist, err := g.getRelHistory(id)
					if err != nil {
						t.Fatal(err)
					}
					winners, at := planWinners(g, relKernel, hist, cur, iv[0], iv[1], false)
					if at {
						resumeAt++
					}
					cs.reset()
					if _, err := g.Temporal.SetRelVersionInterval(ctx, id, iv[0], iv[1], map[string]any{"k": int64(99)}); err != nil {
						t.Fatal(err)
					}
					checkCascadeReads(t, be, iv, len(hist), winners, cs.relHistory.Load(), cs.relVersion.Load())
				}
				if resumeAt != 1 {
					t.Fatalf("fixture: %d corrections end where their source starts, want 1", resumeAt)
				}
			})
			t.Run(be.name+"/"+end+"/node", func(t *testing.T) {
				t.Parallel()
				g, cs := openCountedGraph(t, be)
				id := cascadeMetaNodeChain(t, g, end)
				resumeAt := 0
				for _, iv := range intervals {
					cur, err := g.getCurrentNode(id)
					if err != nil {
						t.Fatal(err)
					}
					hist, err := g.getNodeHistory(id)
					if err != nil {
						t.Fatal(err)
					}
					winners, at := planWinners(g, nodeKernel, hist, cur, iv[0], iv[1], false)
					if at {
						resumeAt++
					}
					cs.reset()
					if _, err := g.Temporal.SetNodeVersionInterval(ctx, id, iv[0], iv[1], map[string]any{"k": int64(99)}); err != nil {
						t.Fatal(err)
					}
					checkCascadeReads(t, be, iv, len(hist), winners, cs.nodeHistory.Load(), cs.nodeVersion.Load())
				}
				if resumeAt != 1 {
					t.Fatalf("fixture: %d corrections end where their source starts, want 1", resumeAt)
				}
			})
		}
	}
}

func checkCascadeReads(t *testing.T, be cascadeCountBackend, iv [2]types.Instant, versions, winners int, histories, points int64) {
	t.Helper()
	t.Logf("[%d,%d): %d history rows, %d rows copied: %d whole-history reads, %d point reads", iv[0], iv[1], versions, winners, histories, points)
	if winners < 2 {
		t.Fatalf("fixture: %d rows copied over [%d,%d), want a multi-row plan", winners, iv[0], iv[1])
	}
	if !be.meta {
		if histories != 1 {
			t.Fatalf("%s: %d whole-history reads, want 1 (no skeleton door: the fallback)", be.name, histories)
		}
		return
	}
	if histories != 0 {
		t.Fatalf("%s: %d whole-history reads on a head miss, want 0", be.name, histories)
	}
	if points != int64(winners) {
		t.Fatalf("%s: %d full-row reads, want %d (one per distinct row copied) of %d history rows", be.name, points, winners, versions)
	}
}
