package core

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"

	eventspkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/events"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

type storeContractBackend struct {
	name              string
	snowflakeNodeID   int
	newGraph          func(t *testing.T, snowflakeNodeID int) *Core
	supportsLRUStats  bool
	expectZeroLRUStat bool
}

func storeContractBackends() []storeContractBackend {
	return []storeContractBackend{
		{
			name:            "MemoryStore",
			snowflakeNodeID: 12,
			newGraph: func(t *testing.T, snowflakeNodeID int) *Core {
				t.Helper()
				g, err := New(Config{SnowflakeNodeID: int64(snowflakeNodeID), Store: memory.New()})
				if err != nil {
					t.Fatalf("New memory graph: %v", err)
				}
				t.Cleanup(func() { _ = g.Close() })
				return g
			},
			expectZeroLRUStat: true,
		},
		{
			name:             "badger.Store",
			snowflakeNodeID:  13,
			supportsLRUStats: true,
			newGraph: func(t *testing.T, snowflakeNodeID int) *Core {
				t.Helper()
				bs, err := badger.New(badger.Config{
					InMemory:      true,
					FlushInterval: 1<<63 - 1,
				})
				if err != nil {
					t.Fatalf("badger.New: %v", err)
				}
				g, err := New(Config{SnowflakeNodeID: int64(snowflakeNodeID), Store: bs})
				if err != nil {
					_ = bs.Close()
					t.Fatalf("New badger graph: %v", err)
				}
				t.Cleanup(func() { _ = g.Close() })
				return g
			},
		},
		{
			name:            "tiered.Store",
			snowflakeNodeID: 14,
			newGraph: func(t *testing.T, snowflakeNodeID int) *Core {
				t.Helper()
				ts, err := tiered.New(tiered.Config{
					InMemory:      true,
					RefLabels:     []string{"Case", "User"},
					ShardWindow:   7 * 24 * time.Hour,
					FlushInterval: 1<<63 - 1,
				})
				if err != nil {
					t.Fatalf("tiered.New: %v", err)
				}
				g, err := New(Config{SnowflakeNodeID: int64(snowflakeNodeID), Store: ts})
				if err != nil {
					_ = ts.Close()
					t.Fatalf("New tiered graph: %v", err)
				}
				t.Cleanup(func() { _ = g.Close() })
				return g
			},
			expectZeroLRUStat: true,
		},
	}
}

func runStoreContract(t *testing.T, name string, fn func(t *testing.T, backend storeContractBackend)) {
	t.Helper()
	for _, backend := range storeContractBackends() {
		backend := backend
		t.Run(backend.name+"/"+name, func(t *testing.T) {
			fn(t, backend)
		})
	}
}

func newContractGraph(t *testing.T, backend storeContractBackend) *Core {
	t.Helper()
	return backend.newGraph(t, backend.snowflakeNodeID)
}

func TestStoreContract_CurrentVisibility(t *testing.T) {
	runStoreContract(t, "current visibility", func(t *testing.T, backend storeContractBackend) {
		g := newContractGraph(t, backend)

		caseNode, signalNode, observed := addContractCaseSignalRel(t, g)

		assertGraphCount(t, "NodeCount", g.Nodes.Count, 2)
		assertGraphCount(t, "RelationshipCount", g.Rels.Count, 1)
		assertGraphCount(t, "NodeCountByLabel(Case)", func() (int, error) {
			return g.Nodes.CountByLabel("Case")
		}, 1)
		assertGraphCount(t, "NodeCountByLabel(Signal)", func() (int, error) {
			return g.Nodes.CountByLabel("Signal")
		}, 1)
		assertGraphCount(t, "RelCountByType(OBSERVED)", func() (int, error) {
			return g.Rels.CountByType("OBSERVED")
		}, 1)

		assertNodeIDs(t, "AllNodes", mustAllNodes(t, g, storepkg.QueryOpts{}),
			[]types.NodeID{caseNode.ID(), signalNode.ID()})
		assertNodeIDs(t, "NodesByLabel(Case)", mustNodesByLabel(t, g, "Case", storepkg.QueryOpts{}),
			[]types.NodeID{caseNode.ID()})
		assertNodeIDs(t, "NodesByLabel(Signal)", mustNodesByLabel(t, g, "Signal", storepkg.QueryOpts{}),
			[]types.NodeID{signalNode.ID()})

		assertRelIDs(t, "AllRelationships", mustAllRelationships(t, g, storepkg.QueryOpts{}),
			[]types.RelID{observed.ID()})
		assertRelIDs(t, "RelationshipsByType(OBSERVED)", mustRelationshipsByType(t, g, "OBSERVED", storepkg.QueryOpts{}),
			[]types.RelID{observed.ID()})
		assertRelIDs(t, "OutgoingRelationships(Case)", mustOutgoingRelationships(t, g, caseNode.ID(), ""),
			[]types.RelID{observed.ID()})
		assertRelIDs(t, "IncomingRelationships(Signal)", mustIncomingRelationships(t, g, signalNode.ID(), ""),
			[]types.RelID{observed.ID()})
	})
}

// TestStoreContract_TransactionTimeAsOf is the memory↔badger↔tiered parity gate
// for the transaction-time query capability (Pattern 34). Memory and badger both
// run their NATIVE NodeAsOf/RelAsOf/NodesAsOf/RelsAsOf here; tiered runs the
// mandatory fallback — so a divergence in the badger reverse-scan implementation
// (wrong version selected, an expired/not-yet version mis-classified, a deleted
// entity wrongly visible, or the bulk union miscounting) fails this test against
// the memory oracle. Exercises every arm: current-match fast path, history-scan,
// not-yet-created, deleted-before/after, and the bulk two-pass union.
func TestStoreContract_TransactionTimeAsOf(t *testing.T) {
	runStoreContract(t, "transaction-time AsOf", func(t *testing.T, backend storeContractBackend) {
		g := newContractGraph(t, backend)
		clk := useTestClock(t, g)
		ctx := context.Background()

		assertNodeVer := func(label string, id types.NodeID, at types.Instant, wantVer uint32) {
			t.Helper()
			got, err := g.Temporal.NodeAsOf(id, at)
			if err != nil {
				t.Fatalf("%s: NodeAsOf(%d, %d) = %v, want version %d", label, id, at, err, wantVer)
			}
			if got.Version() != wantVer {
				t.Fatalf("%s: NodeAsOf(%d, %d) version = %d, want %d", label, id, at, got.Version(), wantVer)
			}
		}
		assertNoNodeVer := func(label string, id types.NodeID, at types.Instant) {
			t.Helper()
			if _, err := g.Temporal.NodeAsOf(id, at); !errors.Is(err, ErrNoVersionAsOf) {
				t.Fatalf("%s: NodeAsOf(%d, %d) = %v, want ErrNoVersionAsOf", label, id, at, err)
			}
		}

		// Live node L: three versions (v0,v1,v2), never deleted.
		l0, err := g.Nodes.Add(ctx, []string{"Doc"}, map[string]any{"v": int64(0)})
		if err != nil {
			t.Fatalf("Add L: %v", err)
		}
		lid := l0.ID()
		ltx0 := l0.Temporal().TxFrom
		clk.Advance(4 * time.Millisecond)
		l1, err := g.Nodes.Update(ctx, lid, map[string]any{"v": int64(1)})
		if err != nil {
			t.Fatalf("Update L v1: %v", err)
		}
		ltx1 := l1.Temporal().TxFrom
		clk.Advance(4 * time.Millisecond)
		l2, err := g.Nodes.Update(ctx, lid, map[string]any{"v": int64(2)})
		if err != nil {
			t.Fatalf("Update L v2: %v", err)
		}
		ltx2 := l2.Temporal().TxFrom

		// Deleted node D: one version then deleted.
		clk.Advance(4 * time.Millisecond)
		d0, err := g.Nodes.Add(ctx, []string{"Doc"}, map[string]any{"v": int64(0)})
		if err != nil {
			t.Fatalf("Add D: %v", err)
		}
		did := d0.ID()
		dtx0 := d0.Temporal().TxFrom
		clk.Advance(4 * time.Millisecond)
		delTime := clk.PeekInstant()
		clk.Advance(1 * time.Millisecond)
		if err := g.Nodes.Delete(ctx, did); err != nil {
			t.Fatalf("Delete D: %v", err)
		}

		// L: before-create, history versions, and the current-match fast path.
		assertNoNodeVer("L before create", lid, ltx0-1)
		assertNodeVer("L at v0", lid, ltx0+(ltx1-ltx0)/2, 0)  // history scan
		assertNodeVer("L at v1", lid, ltx1+(ltx2-ltx1)/2, 1)  // history scan
		assertNodeVer("L far future", lid, ltx2+1_000_000, 2) // current-match arm

		// D: visible before its delete, gone after.
		assertNodeVer("D before delete", did, dtx0+1, 0) // history scan (current is gone)
		assertNoNodeVer("D after delete", did, delTime+10)

		assertOnlyNode := func(label string, nodes []*types.Node, want types.NodeID) {
			t.Helper()
			if len(nodes) != 1 || nodes[0].ID() != want {
				got := make([]types.NodeID, len(nodes))
				for i, n := range nodes {
					got[i] = n.ID()
				}
				t.Fatalf("%s = %v, want only %d", label, got, want)
			}
		}
		// Bulk NodesAsOf when only L's v0 existed (before D was created): {L}.
		atV0 := ltx0 + (ltx1-ltx0)/2
		bulk, err := g.Temporal.NodesAsOf(atV0)
		if err != nil {
			t.Fatalf("NodesAsOf(atV0): %v", err)
		}
		assertOnlyNode("NodesAsOf(atV0)", bulk, lid)
		// Bulk far future: L live (v2), D deleted -> {L}.
		bulkNow, err := g.Temporal.NodesAsOf(ltx2 + 1_000_000)
		if err != nil {
			t.Fatalf("NodesAsOf(future): %v", err)
		}
		assertOnlyNode("NodesAsOf(future)", bulkNow, lid)

		// Relationship AsOf: current-match arm + before-create miss.
		clk.Advance(4 * time.Millisecond)
		m0, err := g.Nodes.Add(ctx, []string{"Doc"}, map[string]any{"v": int64(0)})
		if err != nil {
			t.Fatalf("Add M: %v", err)
		}
		rel, err := g.Rels.Add(ctx, "LINKS", l2, m0, nil)
		if err != nil {
			t.Fatalf("Add rel: %v", err)
		}
		rid := rel.ID()
		rtx := rel.Temporal().TxFrom
		got, err := g.Temporal.RelAsOf(rid, rtx+1_000_000)
		if err != nil {
			t.Fatalf("RelAsOf future: %v", err)
		}
		if got.ID() != rid {
			t.Fatalf("RelAsOf future = %d, want %d", got.ID(), rid)
		}
		if _, err := g.Temporal.RelAsOf(rid, rtx-1); !errors.Is(err, ErrNoVersionAsOf) {
			t.Fatalf("RelAsOf before create = %v, want ErrNoVersionAsOf", err)
		}
	})
}

func TestStoreContract_HistoryVisibility(t *testing.T) {
	runStoreContract(t, "history visibility", func(t *testing.T, backend storeContractBackend) {
		g := newContractGraph(t, backend)
		caseNode, signalNode, rel := addContractCaseSignalRel(t, g)

		updatedNode, err := g.Nodes.Update(context.Background(), caseNode.ID(), map[string]any{"status": "investigating"})
		if err != nil {
			t.Fatalf("UpdateNode: %v", err)
		}
		if updatedNode.Version() != 1 {
			t.Fatalf("updated node version = %d, want 1", updatedNode.Version())
		}
		updatedRel, err := g.Rels.Update(context.Background(), rel.ID(), map[string]any{"weight": int64(2)})
		if err != nil {
			t.Fatalf("UpdateRelationship: %v", err)
		}
		if updatedRel.Version() != 1 {
			t.Fatalf("updated rel version = %d, want 1", updatedRel.Version())
		}

		nodeHistory, err := g.Nodes.History(caseNode.ID())
		if err != nil {
			t.Fatalf("GetNodeHistory: %v", err)
		}
		if len(nodeHistory) != 1 {
			t.Fatalf("node history len = %d, want 1", len(nodeHistory))
		}
		if nodeHistory[0].Version() != 0 {
			t.Fatalf("node history version = %d, want 0", nodeHistory[0].Version())
		}
		if v, ok := nodeHistory[0].GetProperty("status"); !ok || v != "open" {
			t.Fatalf("node history status = %v (ok=%v), want open", v, ok)
		}

		relHistory, err := g.Rels.History(rel.ID())
		if err != nil {
			t.Fatalf("GetRelHistory: %v", err)
		}
		if len(relHistory) != 1 {
			t.Fatalf("rel history len = %d, want 1", len(relHistory))
		}
		if relHistory[0].Version() != 0 {
			t.Fatalf("rel history version = %d, want 0", relHistory[0].Version())
		}
		if v, ok := relHistory[0].GetProperty("weight"); !ok || v != int64(1) {
			t.Fatalf("rel history weight = %v (ok=%v), want 1", v, ok)
		}

		assertNodeIDs(t, "AllNodeHistoryIDs", idsToNodes(t, g.store, mustStoreNodeHistoryIDs(t, g.store)),
			[]types.NodeID{caseNode.ID()})
		assertRelIDs(t, "AllRelHistoryIDs", idsToRels(t, g.store, mustStoreRelHistoryIDs(t, g.store)),
			[]types.RelID{rel.ID()})

		gotCurrent, err := g.Nodes.Get(context.Background(), caseNode.ID())
		if err != nil {
			t.Fatalf("GetNode current: %v", err)
		}
		if v, ok := gotCurrent.GetProperty("status"); !ok || v != "investigating" {
			t.Fatalf("current node status = %v (ok=%v), want investigating", v, ok)
		}
		gotSignal, err := g.Nodes.Get(context.Background(), signalNode.ID())
		if err != nil {
			t.Fatalf("GetNode signal: %v", err)
		}
		if gotSignal.Version() != 0 {
			t.Fatalf("untouched signal version = %d, want 0", gotSignal.Version())
		}
	})
}

func TestStoreContract_DeleteTombstonesAndHistory(t *testing.T) {
	runStoreContract(t, "delete tombstones and history", func(t *testing.T, backend storeContractBackend) {
		g := newContractGraph(t, backend)
		caseNode, signalNode, rel := addContractCaseSignalRel(t, g)

		if err := g.Rels.Delete(context.Background(), rel.ID()); err != nil {
			t.Fatalf("DeleteRelationship: %v", err)
		}
		if _, err := g.Rels.Get(context.Background(), rel.ID()); !errors.Is(err, storepkg.ErrRelNotFound) {
			t.Fatalf("GetRelationship after delete err = %v, want storepkg.ErrRelNotFound", err)
		}
		assertRelIDs(t, "RelationshipsByType after rel delete", mustRelationshipsByType(t, g, "OBSERVED", storepkg.QueryOpts{}), nil)
		assertRelTombstone(t, g, rel.ID())

		if err := g.Nodes.Delete(context.Background(), signalNode.ID()); err != nil {
			t.Fatalf("DeleteNode(signal): %v", err)
		}
		if _, err := g.Nodes.Get(context.Background(), signalNode.ID()); !errors.Is(err, storepkg.ErrNodeNotFound) {
			t.Fatalf("GetNode after delete err = %v, want storepkg.ErrNodeNotFound", err)
		}
		assertNodeIDs(t, "NodesByLabel(Signal) after delete", mustNodesByLabel(t, g, "Signal", storepkg.QueryOpts{}), nil)
		assertNodeTombstone(t, g, signalNode.ID())

		cascadeTarget, cascadeRel := addContractSignalRel(t, g, caseNode)
		if err := g.Nodes.Delete(context.Background(), caseNode.ID()); err != nil {
			t.Fatalf("DeleteNode(case cascade): %v", err)
		}
		if _, err := g.Rels.Get(context.Background(), cascadeRel.ID()); !errors.Is(err, storepkg.ErrRelNotFound) {
			t.Fatalf("GetRelationship cascade rel err = %v, want storepkg.ErrRelNotFound", err)
		}
		assertNodeTombstone(t, g, caseNode.ID())
		assertRelTombstone(t, g, cascadeRel.ID())

		nodeHistoryIDs := mustStoreNodeHistoryIDs(t, g.store)
		assertIDSet(t, "AllNodeHistoryIDs after deletes", nodeHistoryIDs, []types.NodeID{
			caseNode.ID(),
			signalNode.ID(),
		})
		relHistoryIDs := mustStoreRelHistoryIDs(t, g.store)
		assertIDSet(t, "AllRelHistoryIDs after deletes", relHistoryIDs, []types.RelID{
			rel.ID(),
			cascadeRel.ID(),
		})

		var forEachNodeIDs []types.NodeID
		if err := g.store.ForEachNodeHistoryID(func(id types.NodeID) bool {
			forEachNodeIDs = append(forEachNodeIDs, id)
			return true
		}); err != nil {
			t.Fatalf("ForEachNodeHistoryID: %v", err)
		}
		assertIDSet(t, "ForEachNodeHistoryID", forEachNodeIDs, nodeHistoryIDs)

		var forEachRelIDs []types.RelID
		if err := g.store.ForEachRelHistoryID(func(id types.RelID) bool {
			forEachRelIDs = append(forEachRelIDs, id)
			return true
		}); err != nil {
			t.Fatalf("ForEachRelHistoryID: %v", err)
		}
		assertIDSet(t, "ForEachRelHistoryID", forEachRelIDs, relHistoryIDs)

		if _, err := g.Nodes.Get(context.Background(), cascadeTarget.ID()); err != nil {
			t.Fatalf("cascade target should remain current: %v", err)
		}
	})
}

func TestStoreContract_Pagination(t *testing.T) {
	runStoreContract(t, "pagination", func(t *testing.T, backend storeContractBackend) {
		g := newContractGraph(t, backend)

		nodes := make([]*types.Node, 0, 5)
		for i := range 5 {
			n, err := g.Nodes.Add(context.Background(), []string{"Case"}, map[string]any{"idx": int64(i)})
			if err != nil {
				t.Fatalf("AddNode %d: %v", i, err)
			}
			nodes = append(nodes, n)
		}
		wantNodeIDs := nodeIDs(nodes)

		firstNodes := mustNodesByLabel(t, g, "Case", storepkg.QueryOpts{Limit: 2})
		assertNodeIDs(t, "NodesByLabel first page", firstNodes, wantNodeIDs[:2])
		secondNodes := mustNodesByLabel(t, g, "Case", storepkg.QueryOpts{
			Limit: 2,
			After: types.EntityID(firstNodes[len(firstNodes)-1].ID()),
		})
		assertNodeIDs(t, "NodesByLabel second page", secondNodes, wantNodeIDs[2:4])

		allNodeIDs, err := g.store.AllNodeIDs(storepkg.QueryOpts{Limit: 3})
		if err != nil {
			t.Fatalf("AllNodeIDs first page: %v", err)
		}
		assertIDSetPreserveOrder(t, "AllNodeIDs first page", allNodeIDs, wantNodeIDs[:3])
		allNodeIDs, err = g.store.AllNodeIDs(storepkg.QueryOpts{After: types.EntityID(allNodeIDs[len(allNodeIDs)-1])})
		if err != nil {
			t.Fatalf("AllNodeIDs second page: %v", err)
		}
		assertIDSetPreserveOrder(t, "AllNodeIDs second page", allNodeIDs, wantNodeIDs[3:])

		rels := make([]*types.Relationship, 0, 4)
		for i := range 4 {
			r, err := g.Rels.Add(context.Background(), "LINK", nodes[0], nodes[1], map[string]any{"idx": int64(i)})
			if err != nil {
				t.Fatalf("AddRelationship %d: %v", i, err)
			}
			rels = append(rels, r)
		}
		wantRelIDs := relIDs(rels)

		firstRels := mustRelationshipsByType(t, g, "LINK", storepkg.QueryOpts{Limit: 2})
		assertRelIDs(t, "RelationshipsByType first page", firstRels, wantRelIDs[:2])
		secondRels := mustRelationshipsByType(t, g, "LINK", storepkg.QueryOpts{
			Limit: 2,
			After: types.EntityID(firstRels[len(firstRels)-1].ID()),
		})
		assertRelIDs(t, "RelationshipsByType second page", secondRels, wantRelIDs[2:4])

		allRelIDs, err := g.store.AllRelIDs(storepkg.QueryOpts{Limit: 3})
		if err != nil {
			t.Fatalf("AllRelIDs first page: %v", err)
		}
		assertIDSetPreserveOrder(t, "AllRelIDs first page", allRelIDs, wantRelIDs[:3])
		allRelIDs, err = g.store.AllRelIDs(storepkg.QueryOpts{After: types.EntityID(allRelIDs[len(allRelIDs)-1])})
		if err != nil {
			t.Fatalf("AllRelIDs second page: %v", err)
		}
		assertIDSetPreserveOrder(t, "AllRelIDs second page", allRelIDs, wantRelIDs[3:])
	})
}

func TestStoreContract_TemporalFilters(t *testing.T) {
	runStoreContract(t, "temporal filters", func(t *testing.T, backend storeContractBackend) {
		g := newContractGraph(t, backend)

		caseA := addContractTemporalNode(t, g, "Case", "red", 100, 300)
		caseB := addContractTemporalNode(t, g, "Case", "red", 200, 0)
		caseC := addContractTemporalNode(t, g, "Case", "blue", 500, 0)
		signal := addContractTemporalNode(t, g, "Signal", "event", 200, 400)

		assertNodeIDs(t, "NodesByLabel Case ValidAt=250", mustNodesByLabel(t, g, "Case", storepkg.QueryOpts{ValidAt: 250}),
			[]types.NodeID{caseA.ID(), caseB.ID()})
		assertNodeIDs(t, "NodesByLabel Case ValidAt=350", mustNodesByLabel(t, g, "Case", storepkg.QueryOpts{ValidAt: 350}),
			[]types.NodeID{caseB.ID()})
		assertNodeIDs(t, "NodesByLabel Case interval", mustNodesByLabel(t, g, "Case", storepkg.QueryOpts{ValidStart: 250, ValidEnd: 550}),
			[]types.NodeID{caseA.ID(), caseB.ID(), caseC.ID()})
		assertNodeIDs(t, "AllNodes ValidAt=250", mustAllNodes(t, g, storepkg.QueryOpts{ValidAt: 250}),
			[]types.NodeID{caseA.ID(), caseB.ID(), signal.ID()})

		if err := g.Index.CreateProperty("Case", "color"); err != nil {
			t.Fatalf("CreatePropertyIndex: %v", err)
		}
		assertNodeIDs(t, "NodesByLabelAndProperty indexed ValidAt=350",
			mustNodesByLabelAndProperty(t, g, "Case", "color", "red", storepkg.QueryOpts{ValidAt: 350}),
			[]types.NodeID{caseB.ID()})

		relA := addContractTemporalRel(t, g, caseA, caseB, 100, 300)
		relB := addContractTemporalRel(t, g, caseB, caseC, 400, 0)
		assertRelIDs(t, "RelationshipsByType ValidAt=250", mustRelationshipsByType(t, g, "TEMPORAL_LINK", storepkg.QueryOpts{ValidAt: 250}),
			[]types.RelID{relA.ID()})
		assertRelIDs(t, "RelationshipsByType interval", mustRelationshipsByType(t, g, "TEMPORAL_LINK", storepkg.QueryOpts{ValidStart: 250, ValidEnd: 450}),
			[]types.RelID{relA.ID(), relB.ID()})
		assertRelIDs(t, "AllRelationships ValidAt=450", mustAllRelationships(t, g, storepkg.QueryOpts{ValidAt: 450}),
			[]types.RelID{relB.ID()})
	})
}

func TestStoreContract_Events(t *testing.T) {
	runStoreContract(t, "events", func(t *testing.T, backend storeContractBackend) {
		g := newContractGraph(t, backend)
		bus := eventspkg.NewEventBus()
		var events []eventspkg.Event
		bus.Subscribe(func(e eventspkg.Event) {
			events = append(events, e)
		})
		_ = g.Events.SetSync(bus)

		caseNode, signalNode, rel := addContractCaseSignalRel(t, g)
		if _, err := g.Nodes.Update(context.Background(), caseNode.ID(), map[string]any{"status": "closed"}); err != nil {
			t.Fatalf("UpdateNode: %v", err)
		}
		if _, err := g.Rels.Update(context.Background(), rel.ID(), map[string]any{"weight": int64(2)}); err != nil {
			t.Fatalf("UpdateRelationship: %v", err)
		}
		if err := g.Rels.Delete(context.Background(), rel.ID()); err != nil {
			t.Fatalf("DeleteRelationship: %v", err)
		}
		if err := g.Nodes.Delete(context.Background(), signalNode.ID()); err != nil {
			t.Fatalf("DeleteNode: %v", err)
		}

		want := []eventspkg.EventType{
			eventspkg.EventNodeCreate,
			eventspkg.EventNodeCreate,
			eventspkg.EventRelCreate,
			eventspkg.EventNodeUpdate,
			eventspkg.EventRelUpdate,
			eventspkg.EventRelDelete,
			eventspkg.EventNodeDelete,
		}
		if len(events) != len(want) {
			t.Fatalf("event count = %d, want %d: %+v", len(events), len(want), events)
		}
		for i, typ := range want {
			if events[i].Type != typ {
				t.Fatalf("event[%d].Type = %v, want %v", i, events[i].Type, typ)
			}
			if events[i].EntityID == 0 {
				t.Fatalf("event[%d].EntityID is zero", i)
			}
			if events[i].Timestamp == 0 {
				t.Fatalf("event[%d].Timestamp is zero", i)
			}
		}
		if events[0].Priority != eventspkg.PriorityHigh || events[2].Priority != eventspkg.PriorityHigh {
			t.Fatalf("create event priorities = %v/%v, want eventspkg.PriorityHigh", events[0].Priority, events[2].Priority)
		}
		if events[3].Priority != eventspkg.PriorityNormal || events[4].Priority != eventspkg.PriorityNormal {
			t.Fatalf("update event priorities = %v/%v, want eventspkg.PriorityNormal", events[3].Priority, events[4].Priority)
		}
		if events[5].Priority != eventspkg.PriorityCritical || events[6].Priority != eventspkg.PriorityCritical {
			t.Fatalf("delete event priorities = %v/%v, want eventspkg.PriorityCritical", events[5].Priority, events[6].Priority)
		}
	})
}

func TestStoreContract_Stats(t *testing.T) {
	runStoreContract(t, "stats", func(t *testing.T, backend storeContractBackend) {
		g := newContractGraph(t, backend)

		caseNode, signalNode, rel := addContractCaseSignalRel(t, g)
		if _, err := g.Nodes.Get(context.Background(), caseNode.ID()); err != nil {
			t.Fatalf("GetNode: %v", err)
		}
		if _, err := g.Rels.Get(context.Background(), rel.ID()); err != nil {
			t.Fatalf("GetRelationship: %v", err)
		}
		if _, err := g.Nodes.Update(context.Background(), caseNode.ID(), map[string]any{"status": "closed"}); err != nil {
			t.Fatalf("UpdateNode: %v", err)
		}
		if _, err := g.Rels.Update(context.Background(), rel.ID(), map[string]any{"weight": int64(2)}); err != nil {
			t.Fatalf("UpdateRelationship: %v", err)
		}
		if err := g.Rels.Delete(context.Background(), rel.ID()); err != nil {
			t.Fatalf("DeleteRelationship: %v", err)
		}
		if err := g.Nodes.Delete(context.Background(), signalNode.ID()); err != nil {
			t.Fatalf("DeleteNode: %v", err)
		}

		stats, _ := g.Stats.Get()
		if stats.NodesAdded != 2 || stats.RelsAdded != 1 ||
			stats.NodesRead != 1 || stats.RelsRead != 1 ||
			stats.NodesUpdated != 1 || stats.RelsUpdated != 1 ||
			stats.NodesDeleted != 1 || stats.RelsDeleted != 1 {
			t.Fatalf("operation stats mismatch: %+v", stats)
		}

		if backend.supportsLRUStats {
			if stats.NodeCacheHits+stats.NodeCacheMisses == 0 {
				t.Fatalf("Badger-backed stats had no node cache activity: %+v", stats)
			}
			if stats.RelCacheHits+stats.RelCacheMisses == 0 {
				t.Fatalf("Badger-backed stats had no rel cache activity: %+v", stats)
			}
		}
		if backend.expectZeroLRUStat {
			if stats.NodeCacheHits != 0 || stats.NodeCacheMisses != 0 ||
				stats.RelCacheHits != 0 || stats.RelCacheMisses != 0 {
				t.Fatalf("cache stats should be zero for %s: %+v", backend.name, stats)
			}
		}
	})
}

func addContractCaseSignalRel(t *testing.T, g *Core) (*types.Node, *types.Node, *types.Relationship) {
	t.Helper()
	caseNode, err := g.Nodes.Add(context.Background(), []string{"Case"}, map[string]any{"status": "open"})
	if err != nil {
		t.Fatalf("AddNode(Case): %v", err)
	}
	signalNode, err := g.Nodes.Add(context.Background(), []string{"Signal"}, map[string]any{"severity": "high"})
	if err != nil {
		t.Fatalf("AddNode(Signal): %v", err)
	}
	rel, err := g.Rels.Add(context.Background(), "OBSERVED", caseNode, signalNode, map[string]any{"weight": int64(1)})
	if err != nil {
		t.Fatalf("AddRelationship(OBSERVED): %v", err)
	}
	return caseNode, signalNode, rel
}

func addContractSignalRel(t *testing.T, g *Core, caseNode *types.Node) (*types.Node, *types.Relationship) {
	t.Helper()
	signalNode, err := g.Nodes.Add(context.Background(), []string{"Signal"}, map[string]any{"severity": "medium"})
	if err != nil {
		t.Fatalf("AddNode(cascade Signal): %v", err)
	}
	rel, err := g.Rels.Add(context.Background(), "OBSERVED", caseNode, signalNode, map[string]any{"weight": int64(3)})
	if err != nil {
		t.Fatalf("AddRelationship(cascade OBSERVED): %v", err)
	}
	return signalNode, rel
}

func addContractTemporalNode(t *testing.T, g *Core, label, color string, validFrom, validTo types.Instant) *types.Node {
	t.Helper()
	n, err := g.Nodes.Add(context.Background(), []string{label}, map[string]any{
		"color":          color,
		"tkg_valid_from": validFrom,
		"tkg_valid_to":   validTo,
	})
	if err != nil {
		t.Fatalf("AddNode(%s temporal): %v", label, err)
	}
	return n
}

func addContractTemporalRel(t *testing.T, g *Core, start, end *types.Node, validFrom, validTo types.Instant) *types.Relationship {
	t.Helper()
	r, err := g.Rels.Add(context.Background(), "TEMPORAL_LINK", start, end, map[string]any{
		"tkg_valid_from": validFrom,
		"tkg_valid_to":   validTo,
	})
	if err != nil {
		t.Fatalf("AddRelationship(TEMPORAL_LINK): %v", err)
	}
	return r
}

func assertNodeTombstone(t *testing.T, g *Core, id types.NodeID) {
	t.Helper()
	history, err := g.Nodes.History(id)
	if err != nil {
		t.Fatalf("GetNodeHistory(%d): %v", id, err)
	}
	if len(history) == 0 {
		t.Fatalf("GetNodeHistory(%d) returned no tombstone", id)
	}
	tm := history[len(history)-1].Temporal()
	if tm == nil {
		t.Fatalf("node tombstone %d has nil temporal metadata", id)
	}
	if tm.DeletedAt == 0 || tm.ValidTo == 0 || tm.TxFrom == 0 || tm.TxTo == 0 {
		t.Fatalf("node tombstone %d missing delete temporal fields: %+v", id, tm)
	}
}

func assertRelTombstone(t *testing.T, g *Core, id types.RelID) {
	t.Helper()
	history, err := g.Rels.History(id)
	if err != nil {
		t.Fatalf("GetRelHistory(%d): %v", id, err)
	}
	if len(history) == 0 {
		t.Fatalf("GetRelHistory(%d) returned no tombstone", id)
	}
	tm := history[len(history)-1].Temporal()
	if tm == nil {
		t.Fatalf("rel tombstone %d has nil temporal metadata", id)
	}
	if tm.DeletedAt == 0 || tm.ValidTo == 0 || tm.TxFrom == 0 || tm.TxTo == 0 {
		t.Fatalf("rel tombstone %d missing delete temporal fields: %+v", id, tm)
	}
}

func assertGraphCount(t *testing.T, name string, fn func() (int, error), want int) {
	t.Helper()
	got, err := fn()
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	if got != want {
		t.Fatalf("%s = %d, want %d", name, got, want)
	}
}

func mustAllNodes(t *testing.T, g *Core, opts storepkg.QueryOpts) []*types.Node {
	t.Helper()
	nodes, err := g.Nodes.All(opts)
	if err != nil {
		t.Fatalf("AllNodes: %v", err)
	}
	return nodes
}

func mustNodesByLabel(t *testing.T, g *Core, label string, opts storepkg.QueryOpts) []*types.Node {
	t.Helper()
	nodes, err := g.Nodes.ByLabel(label, opts)
	if err != nil {
		t.Fatalf("NodesByLabel(%s): %v", label, err)
	}
	return nodes
}

func mustNodesByLabelAndProperty(t *testing.T, g *Core, label, key string, value any, opts storepkg.QueryOpts) []*types.Node {
	t.Helper()
	nodes, err := g.Nodes.ByLabelAndProperty(label, key, value, opts)
	if err != nil {
		t.Fatalf("NodesByLabelAndProperty(%s, %s): %v", label, key, err)
	}
	return nodes
}

func mustAllRelationships(t *testing.T, g *Core, opts storepkg.QueryOpts) []*types.Relationship {
	t.Helper()
	rels, err := g.Rels.All(opts)
	if err != nil {
		t.Fatalf("AllRelationships: %v", err)
	}
	return rels
}

func mustRelationshipsByType(t *testing.T, g *Core, typeName string, opts storepkg.QueryOpts) []*types.Relationship {
	t.Helper()
	rels, err := g.Rels.ByType(typeName, opts)
	if err != nil {
		t.Fatalf("RelationshipsByType(%s): %v", typeName, err)
	}
	return rels
}

func mustOutgoingRelationships(t *testing.T, g *Core, id types.NodeID, typeName string) []*types.Relationship {
	t.Helper()
	rels, err := g.Rels.Outgoing(id, typeName)
	if err != nil {
		t.Fatalf("OutgoingRelationships(%d, %s): %v", id, typeName, err)
	}
	return rels
}

func mustIncomingRelationships(t *testing.T, g *Core, id types.NodeID, typeName string) []*types.Relationship {
	t.Helper()
	rels, err := g.Rels.Incoming(id, typeName)
	if err != nil {
		t.Fatalf("IncomingRelationships(%d, %s): %v", id, typeName, err)
	}
	return rels
}

func mustStoreNodeHistoryIDs(t *testing.T, store storepkg.MandatoryStore) []types.NodeID {
	t.Helper()
	ids, err := store.AllNodeHistoryIDs()
	if err != nil {
		t.Fatalf("AllNodeHistoryIDs: %v", err)
	}
	return ids
}

func mustStoreRelHistoryIDs(t *testing.T, store storepkg.MandatoryStore) []types.RelID {
	t.Helper()
	ids, err := store.AllRelHistoryIDs()
	if err != nil {
		t.Fatalf("AllRelHistoryIDs: %v", err)
	}
	return ids
}

func idsToNodes(t *testing.T, store storepkg.MandatoryStore, ids []types.NodeID) []*types.Node {
	t.Helper()
	nodes := make([]*types.Node, 0, len(ids))
	for _, id := range ids {
		n, err := store.GetNode(id)
		if errors.Is(err, storepkg.ErrNodeNotFound) {
			continue
		}
		if err != nil {
			t.Fatalf("GetNode(%d): %v", id, err)
		}
		nodes = append(nodes, n)
	}
	return nodes
}

func idsToRels(t *testing.T, store storepkg.MandatoryStore, ids []types.RelID) []*types.Relationship {
	t.Helper()
	rels := make([]*types.Relationship, 0, len(ids))
	for _, id := range ids {
		r, err := store.GetRelationship(id)
		if errors.Is(err, storepkg.ErrRelNotFound) {
			continue
		}
		if err != nil {
			t.Fatalf("GetRelationship(%d): %v", id, err)
		}
		rels = append(rels, r)
	}
	return rels
}

// orderedID covers every typed-ID wrapper plus raw snowflake.ID. All have
// underlying type int64, so `<` sorts and `==` compares correctly across all
// of them. Keeps the assertion helpers polymorphic without an explicit
// raw/typed bridge.
type orderedID interface {
	~int64
}

func nodeIDs(nodes []*types.Node) []types.NodeID {
	ids := make([]types.NodeID, 0, len(nodes))
	for _, n := range nodes {
		ids = append(ids, n.ID())
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func relIDs(rels []*types.Relationship) []types.RelID {
	ids := make([]types.RelID, 0, len(rels))
	for _, r := range rels {
		ids = append(ids, r.ID())
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func assertNodeIDs(t *testing.T, name string, nodes []*types.Node, want []types.NodeID) {
	t.Helper()
	assertIDSetPreserveOrder(t, name, nodeIDs(nodes), want)
}

func assertRelIDs(t *testing.T, name string, rels []*types.Relationship, want []types.RelID) {
	t.Helper()
	assertIDSetPreserveOrder(t, name, relIDs(rels), want)
}

func assertIDSet[T orderedID](t *testing.T, name string, got, want []T) {
	t.Helper()
	got = append([]T(nil), got...)
	want = append([]T(nil), want...)
	sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
	sort.Slice(want, func(i, j int) bool { return want[i] < want[j] })
	assertIDSetPreserveOrder(t, name, got, want)
}

func assertIDSetPreserveOrder[T orderedID](t *testing.T, name string, got, want []T) {
	t.Helper()
	if len(got) == 0 && len(want) == 0 {
		return
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s ids = %v, want %v", name, got, want)
	}
}
