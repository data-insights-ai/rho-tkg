package core

import (
	"errors"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestBudgetedAsOfOwnerWalkAvoidsWholeHistory(t *testing.T) {
	for _, backend := range []string{"sharded", "tiered-reference", "tiered-event"} {
		t.Run(backend, func(t *testing.T) {
			var st storepkg.MandatoryStore
			var owner func(types.NodeID) *badger.Store
			label := "Case"
			if backend == "sharded" {
				s, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 2})
				if err != nil {
					t.Fatal(err)
				}
				st = s
				owner = func(id types.NodeID) *badger.Store {
					bs, err := s.ShardForIDForTest(id.SnowflakeID())
					if err != nil {
						t.Fatal(err)
					}
					return bs
				}
			} else {
				ts := newTestTieredStore(t)
				st = ts
				owner = func(types.NodeID) *badger.Store { return ts.RefShardForTest() }
				if backend == "tiered-event" {
					label = "Event"
					owner = func(types.NodeID) *badger.Store { return ts.HotShardForTest().Store() }
				}
			}
			g, err := New(Config{Store: st, SnowflakeNodeID: 0, UntimedValidStart: UntimedFromTxFrom})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = g.Close() })
			n, err := g.Nodes.Add(t.Context(), []string{label}, map[string]any{"x": int64(0)})
			if err != nil {
				t.Fatal(err)
			}
			end, err := g.Nodes.Add(t.Context(), []string{label}, nil)
			if err != nil {
				t.Fatal(err)
			}
			r, err := g.Rels.AddByID(t.Context(), "R", n.ID(), end.ID(), map[string]any{"x": int64(0)})
			if err != nil {
				t.Fatal(err)
			}
			oldPin, err := g.Temporal.NowTx()
			if err != nil {
				t.Fatal(err)
			}
			for i := range 8 {
				if _, err := g.Nodes.Update(t.Context(), n.ID(), map[string]any{"x": int64(i + 1)}); err != nil {
					t.Fatal(err)
				}
				if _, err := g.Rels.Update(t.Context(), r.ID(), map[string]any{"x": int64(i + 1)}); err != nil {
					t.Fatal(err)
				}
			}
			pin, err := g.Temporal.NowTx()
			if err != nil {
				t.Fatal(err)
			}
			wantNode := asOfRender(g.Temporal.NodeAsOf(n.ID(), pin))
			wantRel, err := g.Temporal.RelAsOf(r.ID(), pin)
			if err != nil || wantRel == nil || wantRel.Version() != 8 {
				t.Fatalf("plain relationship: %v, %v", wantRel, err)
			}
			nodeBS, relBS := owner(n.ID()), owner(types.NodeID(r.ID()))
			decodes := func() int64 {
				n := nodeBS.HistoryRowDecodesForTest()
				if relBS != nodeBS {
					n += relBS.HistoryRowDecodesForTest()
				}
				return n
			}
			before := decodes()
			ctx, meter := storepkg.WithReadBudget(t.Context(), storepkg.ReadBudget{})
			gotNode := asOfRender(g.Temporal.NodeAsOfCtx(ctx, n.ID(), pin))
			gotRel, err := g.Temporal.RelAsOfCtx(ctx, r.ID(), pin)
			if gotNode != wantNode || err != nil || gotRel == nil || gotRel.Version() != 8 {
				t.Fatalf("metered latest: node %s, rel %v, err %v; want node %s and relationship v8", gotNode, gotRel, err, wantNode)
			}
			if x, _ := gotRel.GetProperty("x"); x != int64(8) {
				t.Fatalf("relationship x=%v; want 8", x)
			}
			if read := decodes() - before; read != 0 {
				t.Fatalf("latest head decoded %d history rows, work %+v; want owner walks without a whole-chain decode", read, meter.Work())
			}
			// The new route still resolves an old pin, rather than returning the
			// current head just because it lives on one owner.
			ctx, _ = storepkg.WithReadBudget(t.Context(), storepkg.ReadBudget{})
			n0, err := g.Temporal.NodeAsOfCtx(ctx, n.ID(), oldPin)
			if err != nil || n0 == nil || n0.Version() != 0 {
				t.Fatalf("old node pin: %v, %v; want v0", n0, err)
			}
			r0, err := g.Temporal.RelAsOfCtx(ctx, r.ID(), oldPin)
			if err != nil || r0 == nil || r0.Version() != 0 {
				t.Fatalf("old rel pin: %v, %v; want v0", r0, err)
			}
		})
	}
}

func TestBudgetedAsOfOwnerWalkArchiveGap(t *testing.T) {
	for _, rel := range []bool{false, true} {
		t.Run(map[bool]string{false: "node", true: "rel"}[rel], func(t *testing.T) {
			g, ts := newTestTieredGraph(t)
			n, err := g.Nodes.Add(t.Context(), []string{"Case"}, map[string]any{"x": int64(0)})
			if err != nil {
				t.Fatal(err)
			}
			end, err := g.Nodes.Add(t.Context(), []string{"Case"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			r, err := g.Rels.AddByID(t.Context(), "R", n.ID(), end.ID(), map[string]any{"x": int64(0)})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := g.Nodes.Update(t.Context(), n.ID(), map[string]any{"x": int64(1)}); err != nil {
				t.Fatal(err)
			}
			if _, err := g.Rels.Update(t.Context(), r.ID(), map[string]any{"x": int64(1)}); err != nil {
				t.Fatal(err)
			}
			pin, err := g.Temporal.NowTx()
			if err != nil {
				t.Fatal(err)
			}
			fired := false
			ts.SetAsOfWalkGapHookForTest(func() {
				if !fired {
					fired = true
					if err := ts.ArchiveNode(n.ID()); err != nil {
						t.Error(err)
					}
				}
			})
			defer ts.SetAsOfWalkGapHookForTest(nil)
			ctx, _ := storepkg.WithReadBudget(t.Context(), storepkg.ReadBudget{})
			if rel {
				got, err := g.Temporal.RelAsOfCtx(ctx, r.ID(), pin)
				if !fired || err != nil || got == nil || got.Version() != 1 {
					t.Fatalf("rel archive gap: fired %v, row %v, err %v; want archived v1", fired, got, err)
				}
			} else {
				got, err := g.Temporal.NodeAsOfCtx(ctx, n.ID(), pin)
				if !fired || err != nil || got == nil || got.Version() != 1 {
					t.Fatalf("node archive gap: fired %v, row %v, err %v; want archived v1", fired, got, err)
				}
			}
		})
	}
}

type asOfWalkSnapshotFixture struct {
	storepkg.ReadSnapshot
	tally           *storepkg.ReadTally
	node            *types.Node
	rel             *types.Relationship
	handled, refuse bool
	err             error
	calls, fallback int
}

func (s *asOfWalkSnapshotFixture) MeterReads(t *storepkg.ReadTally) { s.tally = t }
func (s *asOfWalkSnapshotFixture) Close()                           {}
func (s *asOfWalkSnapshotFixture) Release()                         {}
func (s *asOfWalkSnapshotFixture) NodeAsOfWalk(types.NodeID, types.Instant) (*types.Node, bool, error) {
	s.calls++
	if s.refuse {
		return nil, false, s.tally.Candidates(2)
	}
	return s.node, s.handled, s.err
}
func (s *asOfWalkSnapshotFixture) RelAsOfWalk(types.RelID, types.Instant) (*types.Relationship, bool, error) {
	s.calls++
	if s.refuse {
		return nil, false, s.tally.Candidates(2)
	}
	return s.rel, s.handled, s.err
}
func (s *asOfWalkSnapshotFixture) GetNode(types.NodeID) (*types.Node, error) {
	s.fallback++
	return nil, storepkg.ErrNodeNotFound
}
func (s *asOfWalkSnapshotFixture) GetRelationship(types.RelID) (*types.Relationship, error) {
	s.fallback++
	return nil, storepkg.ErrRelNotFound
}
func (s *asOfWalkSnapshotFixture) GetNodeHistory(types.NodeID) ([]*types.Node, error) {
	s.fallback++
	return nil, nil
}
func (s *asOfWalkSnapshotFixture) GetRelHistory(types.RelID) ([]*types.Relationship, error) {
	s.fallback++
	return nil, nil
}

type asOfWalkFixtureStore struct {
	storepkg.MandatoryStore
	snap *asOfWalkSnapshotFixture
}

func (s *asOfWalkFixtureStore) NewReadSnapshot() storepkg.ReadSnapshot { return s.snap }

func TestBudgetedAsOfSnapshotContract(t *testing.T) {
	for _, rel := range []bool{false, true} {
		for _, kind := range []string{"normalize", "wrong-id", "nil", "absent", "refusal", "decline"} {
			t.Run(map[bool]string{false: "node/", true: "rel/"}[rel]+kind, func(t *testing.T) {
				n := types.NewNode(types.NodeID(1), 1, nil)
				r := types.NewRelationship(types.RelID(1), 1, types.NodeID(1), types.NodeID(2))
				tm := types.TemporalMetadata{TxFrom: 1, TxTo: 20, ValidTo: 30, DeletedAt: 30}
				wantSource := tm // an independent value: normalization must not mutate it
				n.SetTemporal(&tm)
				r.SetTemporal(&tm)
				snap := &asOfWalkSnapshotFixture{node: n, rel: r, handled: true}
				switch kind {
				case "wrong-id":
					snap.node = types.NewNode(types.NodeID(2), 1, nil)
					snap.rel = types.NewRelationship(types.RelID(2), 1, types.NodeID(1), types.NodeID(2))
				case "nil":
					snap.node, snap.rel = nil, nil
				case "absent":
					snap.err = storepkg.ErrVersionNotFound
				case "refusal":
					snap.refuse = true
				case "decline":
					snap.handled = false
				}
				g, err := New(Config{Store: &asOfWalkFixtureStore{MandatoryStore: memory.New(), snap: snap}})
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = g.Close() }()
				ctx, _ := storepkg.WithReadBudget(t.Context(), storepkg.ReadBudget{Candidates: 1})
				var got *types.TemporalMetadata
				if rel {
					row, e := g.Temporal.RelAsOfCtx(ctx, types.RelID(1), 10)
					err = e
					if row != nil {
						got = row.Temporal()
					}
				} else {
					row, e := g.Temporal.NodeAsOfCtx(ctx, types.NodeID(1), 10)
					err = e
					if row != nil {
						got = row.Temporal()
					}
				}
				wantErr := error(nil)
				switch kind {
				case "wrong-id", "nil":
					wantErr = storepkg.ErrInvalidStoreMutation
				case "absent", "decline":
					wantErr = ErrNoVersionAsOf
				case "refusal":
					wantErr = storepkg.ErrReadBudget
				}
				if !errors.Is(err, wantErr) || snap.calls != 1 {
					t.Fatalf("err %v (want %v), calls %d", err, wantErr, snap.calls)
				}
				wantFallback := 0
				if kind == "decline" {
					wantFallback = 2
				}
				if snap.fallback != wantFallback {
					t.Fatalf("fallback reads %d, want %d", snap.fallback, wantFallback)
				}
				if kind == "normalize" {
					if got == nil || got.TxTo != 0 || got.ValidTo != 0 || got.DeletedAt != 0 {
						t.Fatalf("visible row metadata %+v", got)
					}
					if *n.Temporal() != wantSource || *r.Temporal() != wantSource {
						t.Fatalf("borrowed snapshot rows mutated: node %+v, rel %+v", n.Temporal(), r.Temporal())
					}
				} else if got != nil {
					t.Fatalf("unexpected row metadata %+v", got)
				}
			})
		}
	}
}

type hidingAsOfSnapshotStore struct{ *tiered.Store }

func (s *hidingAsOfSnapshotStore) GetNode(types.NodeID) (*types.Node, error) {
	return nil, storepkg.ErrNodeNotFound
}

func TestBudgetedAsOfSnapshotDoesNotBypassWrapper(t *testing.T) {
	ts := newTestTieredStore(t)
	wrapped := &hidingAsOfSnapshotStore{ts}
	g, err := New(Config{Store: wrapped})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = g.Close() }()
	if g.readSnapshots != nil {
		t.Fatal("wrapper inherited a native snapshot that bypasses its reads")
	}
	n, err := g.Nodes.Add(t.Context(), []string{"Case"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	pin, err := g.Temporal.NowTx()
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	ts.RefShardForTest().SetAsOfAfterCurrentTestHookForTest(func() { calls++ })
	defer ts.RefShardForTest().SetAsOfAfterCurrentTestHookForTest(nil)
	ctx, _ := storepkg.WithReadBudget(t.Context(), storepkg.ReadBudget{})
	got, err := g.Temporal.NodeAsOfCtx(ctx, n.ID(), pin)
	var rb *storepkg.ReadBudgetError
	if got != nil || !errors.As(err, &rb) || rb.Limit != "unsupported" || calls != 0 {
		t.Fatalf("wrapper: row %v, err %v, native owner calls %d", got, err, calls)
	}
}

// Restore moves the live rows back, while older snapshots stay in the
// archive. Those rows must keep the owner shortcut disabled for both kinds.
func TestBudgetedAsOfOwnerWalkArchiveHistory(t *testing.T) {
	g, ts := newTestTieredGraph(t)
	n, err := g.Nodes.Add(t.Context(), []string{"Case"}, map[string]any{"x": int64(0)})
	if err != nil {
		t.Fatal(err)
	}
	end, err := g.Nodes.Add(t.Context(), []string{"Case"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	r, err := g.Rels.AddByID(t.Context(), "R", n.ID(), end.ID(), map[string]any{"x": int64(0)})
	if err != nil {
		t.Fatal(err)
	}
	if err := ts.ArchiveNode(n.ID()); err != nil {
		t.Fatal(err)
	}
	pin, err := g.Temporal.NowTx()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.Nodes.Update(t.Context(), n.ID(), map[string]any{"x": int64(1)}); err != nil {
		t.Fatal(err)
	}
	if _, err := g.Rels.Update(t.Context(), r.ID(), map[string]any{"x": int64(1)}); err != nil {
		t.Fatal(err)
	}
	if err := ts.RestoreNode(n.ID()); err != nil {
		t.Fatal(err)
	}
	calls := 0
	ts.RefShardForTest().SetAsOfAfterCurrentTestHookForTest(func() { calls++ })
	defer ts.RefShardForTest().SetAsOfAfterCurrentTestHookForTest(nil)
	ctx, _ := storepkg.WithReadBudget(t.Context(), storepkg.ReadBudget{})
	n0, err := g.Temporal.NodeAsOfCtx(ctx, n.ID(), pin)
	if err != nil || n0 == nil || n0.Version() != 0 {
		t.Fatalf("restored node old pin: %v, %v; want archived v0", n0, err)
	}
	r0, err := g.Temporal.RelAsOfCtx(ctx, r.ID(), pin)
	if err != nil || r0 == nil || r0.Version() != 0 {
		t.Fatalf("restored relationship old pin: %v, %v; want archived v0", r0, err)
	}
	if x, _ := n0.GetProperty("x"); x != int64(0) {
		t.Fatalf("archived node x=%v; want 0", x)
	}
	if x, _ := r0.GetProperty("x"); x != int64(0) {
		t.Fatalf("archived relationship x=%v; want 0", x)
	}
	if calls != 0 {
		t.Fatalf("native owner reads %d; archived history requires the admitted fallback", calls)
	}
}
