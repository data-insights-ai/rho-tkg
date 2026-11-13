package core

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Sigma's group B doors under a read budget (RH list R2, "complete Sigma
// propagation"): the effective scans, the per-entity timelines and the by-ID
// doors. Tests count work (rows decoded, history looked at, shard opens,
// callbacks), never time or the heap. Notes and red output:
// tasks/evidence/READBUDGET/sigma-doors-b.txt.

const (
	sbLabel = "D"
	sbType  = "NET"
)

// sbWorld is one backend's graph of the group B scenario: n nodes of label D
// and n relationships of type NET, each with two updates (three versions);
// every fourth one is deleted after them, so it has history and no current row.
type sbWorld struct {
	g     *Core
	nodes []types.NodeID // ascending
	rels  []types.RelID  // ascending
	// deletedNodes and deletedRels are the ones with no current row.
	deletedNodes map[types.NodeID]bool
	deletedRels  map[types.RelID]bool
	pin          types.Instant
}

func sbBuild(t testing.TB, g *Core, n int) *sbWorld {
	t.Helper()
	ctx := context.Background()
	w := &sbWorld{g: g, deletedNodes: map[types.NodeID]bool{}, deletedRels: map[types.RelID]bool{}}
	s, e := func() (types.NodeID, types.NodeID) {
		a, err := g.Nodes.Add(ctx, []string{"Ref"}, map[string]any{"k": "s"})
		if err != nil {
			t.Fatal(err)
		}
		b, err := g.Nodes.Add(ctx, []string{"Ev"}, map[string]any{"k": "e"})
		if err != nil {
			t.Fatal(err)
		}
		return a.ID(), b.ID()
	}()
	for i := range n {
		nd, err := g.Nodes.Add(ctx, []string{sbLabel}, map[string]any{"i": int64(i), "pad": fmt.Sprintf("%0*d", 60, i)})
		if err != nil {
			t.Fatal(err)
		}
		r, err := g.Rels.AddByID(ctx, sbType, s, e, map[string]any{"i": int64(i), "pad": fmt.Sprintf("%0*d", 60, i)})
		if err != nil {
			t.Fatal(err)
		}
		for v := range 2 {
			if _, err := g.Nodes.Update(ctx, nd.ID(), map[string]any{"v": int64(v)}); err != nil {
				t.Fatal(err)
			}
			if _, err := g.Rels.Update(ctx, r.ID(), map[string]any{"v": int64(v)}); err != nil {
				t.Fatal(err)
			}
		}
		if i%4 == 3 {
			if err := g.Nodes.Delete(ctx, nd.ID()); err != nil {
				t.Fatal(err)
			}
			if err := g.Rels.Delete(ctx, r.ID()); err != nil {
				t.Fatal(err)
			}
			w.deletedNodes[nd.ID()], w.deletedRels[r.ID()] = true, true
		}
		w.nodes = append(w.nodes, nd.ID())
		w.rels = append(w.rels, r.ID())
	}
	pin, err := g.Temporal.NowTx()
	if err != nil {
		t.Fatal(err)
	}
	w.pin = pin
	return w
}

// sbNodeSeg and sbRelSeg render a segment: entity, valid span, version.
func sbNodeSeg(s temporal.NodeSegment) string {
	return fmt.Sprintf("n%d[%d,%d)v%d", s.Node.ID(), s.ValidFrom, s.ValidTo, s.Node.Version())
}

func sbRelSeg(s temporal.RelSegment) string {
	return fmt.Sprintf("r%d[%d,%d)v%d", s.Rel.ID(), s.ValidFrom, s.ValidTo, s.Rel.Version())
}

func sbSegs(segs []temporal.NodeSegment) string {
	var out []string
	for _, s := range segs {
		out = append(out, sbNodeSeg(s))
	}
	return strings.Join(out, " ")
}

func sbRelSegs(segs []temporal.RelSegment) string {
	var out []string
	for _, s := range segs {
		out = append(out, sbRelSeg(s))
	}
	return strings.Join(out, " ")
}

// sbEvict drops the entity-cache entries of every node and relationship of w
// (after a reopen the rows are clean), so the next read decodes them again.
func (w *sbWorld) evict(t testing.TB) {
	t.Helper()
	rbdEvict(t, w.g, w.nodes, w.rels)
}

// sbTieredWorld is a tiered graph over three event shards (two nodes and two
// relationships each, the second of each pair deleted), all demoted to cold: closeCold closes them all before each read, so every read
// reopens them and ColdOpens counts the opens.
type sbTieredWorld struct {
	g     *Core
	ts    *tiered.Store
	nodes []types.NodeID // label D, one more per shard
	rels  []types.RelID  // type NET, one more per shard
	pin   types.Instant
	// preDelete is a pin after the first shard's second node and relationship
	// were written and before they were deleted: they exist at it.
	preDelete types.Instant
	cold      []*tiered.EventShard
}

func sbBuildTiered(t *testing.T) *sbTieredWorld {
	t.Helper()
	ts, err := tiered.New(tiered.Config{
		DataDir:           t.TempDir(),
		RefLabels:         []string{"Ref"},
		ShardWindow:       7 * 24 * time.Hour,
		FlushInterval:     1<<63 - 1,
		MaxOpenColdShards: 8,
	})
	if err != nil {
		t.Fatalf("tiered.New: %v", err)
	}
	g, err := New(Config{Store: ts, UntimedValidStart: UntimedFromTxFrom})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close(); _ = ts.Close() })
	ctx := context.Background()
	w := &sbTieredWorld{g: g, ts: ts}
	s, e := txbEndpoints(t, g)
	const shards = 3
	for i := range shards {
		for j := range 2 {
			n, err := g.Nodes.Add(ctx, []string{sbLabel}, map[string]any{"i": int64(i*2 + j)})
			if err != nil {
				t.Fatal(err)
			}
			r, err := g.Rels.AddByID(ctx, sbType, s, e, map[string]any{"i": int64(i*2 + j)})
			if err != nil {
				t.Fatal(err)
			}
			for v := range 2 {
				if _, err := g.Nodes.Update(ctx, n.ID(), map[string]any{"v": int64(v)}); err != nil {
					t.Fatal(err)
				}
				if _, err := g.Rels.Update(ctx, r.ID(), map[string]any{"v": int64(v)}); err != nil {
					t.Fatal(err)
				}
			}
			if j == 1 { // history only: the membership sidecars list it, the current members do not
				if i == 0 {
					if w.preDelete, err = g.Temporal.NowTx(); err != nil {
						t.Fatal(err)
					}
				}
				if err := g.Nodes.Delete(ctx, n.ID()); err != nil {
					t.Fatal(err)
				}
				if err := g.Rels.Delete(ctx, r.ID()); err != nil {
					t.Fatal(err)
				}
			}
			w.nodes = append(w.nodes, n.ID())
			w.rels = append(w.rels, r.ID())
		}
		time.Sleep(2 * time.Millisecond)
		if err := ts.RotateHotShard(); err != nil {
			t.Fatal(err)
		}
	}
	if w.pin, err = g.Temporal.NowTx(); err != nil {
		t.Fatal(err)
	}
	ts.MuForTest().RLock()
	hot := ts.HotShardForTest()
	for _, es := range ts.EventShardsForTest() {
		if es != hot {
			w.cold = append(w.cold, es)
		}
	}
	ts.MuForTest().RUnlock()
	if len(w.cold) < shards {
		t.Fatalf("%d cold shards, want %d", len(w.cold), shards)
	}
	for _, es := range w.cold {
		demoteToCold(ts, es.Name())
	}
	return w
}

// closeCold closes every cold shard, so the next read opens them again.
func (w *sbTieredWorld) closeCold(t *testing.T) {
	t.Helper()
	for _, es := range w.cold {
		closeEventShardStore(t, es)
	}
}

func (w *sbTieredWorld) opens() uint64 { return w.ts.ShardAccessStats().ColdOpens }

// sbPrefix reports whether got is a prefix of want.
func sbPrefix(got, want []string) bool {
	if len(got) > len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

// sbBudget is ReadBudget{} with one counter set to 1 (or to n).
func sbBudget(limit string, n uint64) storepkg.ReadBudget {
	var b storepkg.ReadBudget
	switch limit {
	case "candidates":
		b.Candidates = n
	case "segments":
		b.Segments = n
	case "shard opens":
		b.ShardOpens = n
	case "decoded bytes":
		b.DecodedBytes = n
	case "history examined":
		b.HistoryExamined = n
	case "buffer bytes":
		b.BufferBytes = n
	}
	return b
}

func sbWorkOf(w storepkg.ReadWork, limit string) uint64 {
	switch limit {
	case "candidates":
		return w.Candidates
	case "segments":
		return w.Segments
	case "shard opens":
		return w.ShardOpens
	case "decoded bytes":
		return w.DecodedBytes
	case "history examined":
		return w.HistoryExamined
	case "buffer bytes":
		return w.BufferBytes
	}
	return 0
}
