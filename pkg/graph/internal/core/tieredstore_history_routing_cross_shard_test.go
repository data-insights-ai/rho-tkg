package core

import (
	"testing"
	"time"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/badger"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/tiered"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// All / AllRelationships / NodeCount / RelationshipCount must include
// archived entities. Pre-fix, archived nodes were GetNode-addressable but
// silently absent from public bulk scans.
func TestTieredStore_BulkQueries_IncludeArchive(t *testing.T) {
	g, ts := newTestTieredGraph(t)

	caseNode, err := g.Nodes.Add([]string{"Case"}, map[string]any{"v": 1})
	if err != nil {
		t.Fatal(err)
	}
	id := caseNode.ID()

	if err := ts.ArchiveNode(id); err != nil {
		t.Fatalf("ArchiveNode: %v", err)
	}

	t.Run("AllNodes includes archived", func(t *testing.T) {
		all, err := ts.AllNodes(storepkg.QueryOpts{})
		if err != nil {
			t.Fatalf("AllNodes: %v", err)
		}
		found := false
		for _, n := range all {
			if n.ID() == id {
				found = true
				break
			}
		}
		if !found {
			t.Fatal("AllNodes missed archived node")
		}
	})

	t.Run("NodeCount includes archived", func(t *testing.T) {
		count, err := ts.NodeCount()
		if err != nil {
			t.Fatalf("NodeCount: %v", err)
		}
		if count < 1 {
			t.Fatalf("NodeCount = %d, want >= 1 (archived node missed)", count)
		}
	})

	t.Run("ForEachNodeID enumerates archived", func(t *testing.T) {
		seen := make(map[types.NodeID]struct{})
		if err := ts.ForEachNodeID(func(nid types.NodeID) bool {
			seen[nid] = struct{}{}
			return true
		}); err != nil {
			t.Fatalf("ForEachNodeID: %v", err)
		}
		if _, ok := seen[id]; !ok {
			t.Fatal("ForEachNodeID missed archived node")
		}
	})
}

// Cold-start archive: after a process restart where the catalog records
// an archive shard but the in-memory ts.RefArchiveForTest() pointer is nil, the
// slice and ForEach history APIs must lazy-open the archive instead of
// silently skipping it. This requires disk-backed persistence — the
// in-memory mode loses archive state on instance teardown.
//
// We exercise a real restart: write archive data, Close the tiered.Store,
// reopen against the same DataDir, and assert the public APIs see the
// archived entity without an explicit point lookup having triggered
// ensureRefArchive first.
func TestTieredStore_HistoryAndBulkAPIs_ColdStartLazyOpenArchive(t *testing.T) {
	dir := t.TempDir()
	mkStore := func() *tiered.Store {
		ts, err := tiered.New(tiered.Config{
			DataDir:       dir,
			RefLabels:     []string{"Case", "User"},
			ShardWindow:   7 * 24 * time.Hour,
			FlushInterval: 1<<63 - 1,
		})
		if err != nil {
			t.Fatalf("tiered.New: %v", err)
		}
		return ts
	}

	// Phase 1: write + archive + post-archive update, then Close.
	ts := mkStore()
	g, err := New(Config{Store: ts})
	if err != nil {
		t.Fatalf("New graph: %v", err)
	}

	n, err := g.Nodes.Add([]string{"Case"}, map[string]any{"v": 1})
	if err != nil {
		t.Fatal(err)
	}
	id := n.ID()
	if _, err := g.Nodes.Update(id, map[string]any{"v": 2}); err != nil {
		t.Fatal(err)
	}
	if err := ts.ArchiveNode(id); err != nil {
		t.Fatalf("ArchiveNode: %v", err)
	}
	if _, err := g.Nodes.Update(id, map[string]any{"v": 3}); err != nil {
		t.Fatalf("post-archive UpdateNode: %v", err)
	}
	if err := g.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}

	// Phase 2: reopen against the same DataDir. ts.RefArchiveForTest() starts
	// nil; the catalog has the archive entry. checkoutArchive must
	// lazy-open via ensureRefArchive.
	ts = mkStore()
	t.Cleanup(func() { _ = ts.Close() })

	if ts.RefArchiveForTest().Load() != nil {
		t.Fatal("setup: refArchive should be nil immediately after fresh open")
	}
	if !ts.HasArchiveShardForTest() {
		t.Fatal("setup: catalog should still have archive entry after restart")
	}

	t.Run("AllNodeHistoryIDs lazy-opens archive", func(t *testing.T) {
		ids, err := ts.AllNodeHistoryIDs()
		if err != nil {
			t.Fatalf("AllNodeHistoryIDs: %v", err)
		}
		found := false
		for _, x := range ids {
			if x == id {
				found = true
				break
			}
		}
		if !found {
			t.Fatal("AllNodeHistoryIDs missed archived node after restart (lazy-open did not run)")
		}
	})

	t.Run("AllNodes lazy-opens archive", func(t *testing.T) {
		nodes, err := ts.AllNodes(storepkg.QueryOpts{})
		if err != nil {
			t.Fatalf("AllNodes: %v", err)
		}
		found := false
		for _, nn := range nodes {
			if nn.ID() == id {
				found = true
				break
			}
		}
		if !found {
			t.Fatal("AllNodes missed archived node after restart")
		}
	})

	t.Run("NodeCount counts archived after restart", func(t *testing.T) {
		count, err := ts.NodeCount()
		if err != nil {
			t.Fatalf("NodeCount: %v", err)
		}
		if count < 1 {
			t.Fatalf("NodeCount = %d post-restart, want >= 1", count)
		}
	})
}

// Indexed public queries (NodesByLabel / NodesByLabelAndProperty /
// NodeCountByLabel / RelationshipsByType / RelCountByType) must include
// refArchive entries. Pre-fix, archived reference nodes were
// GetNode/AllNodes-visible but disappeared from indexed reads — the
// label/property/type indexes on the archive store carry the same
// metadata refShard does.
func TestTieredStore_IndexedPublicQueries_IncludeArchive(t *testing.T) {
	// The self-loop makes the relationship fully archive-resident, so the
	// rel-side assertions exercise archive indexes directly. Cross-shard
	// archive-placement migration is covered by the node routing tests.
	ts := newTestTieredStore(t)
	g, err := New(Config{
		SnowflakeNodeID: 0,
		Store:           ts,
		Validation:      ValidationLimits{AllowSelfLoops: true},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	caseNode, err := g.Nodes.Add([]string{"Case"}, map[string]any{"status": "open"})
	if err != nil {
		t.Fatal(err)
	}
	r, err := g.Rels.Add("KNOWS", caseNode, caseNode, nil)
	if err != nil {
		t.Fatal(err)
	}
	caseID := caseNode.InternalID()
	relID := r.InternalID()

	if err := ts.ArchiveNode(caseID); err != nil {
		t.Fatalf("ArchiveNode: %v", err)
	}

	caseTok, ok := g.Resolve.LookupLabel("Case")
	if !ok {
		t.Fatal("Case label not registered")
	}
	knowsTok, ok := g.Resolve.LookupRelType("KNOWS")
	if !ok {
		t.Fatal("KNOWS reltype not registered")
	}

	t.Run("NodesByLabel surfaces archived", func(t *testing.T) {
		nodes, err := ts.NodesByLabel(caseTok, storepkg.QueryOpts{})
		if err != nil {
			t.Fatalf("NodesByLabel: %v", err)
		}
		found := false
		for _, n := range nodes {
			if n.InternalID() == caseID {
				found = true
				break
			}
		}
		if !found {
			t.Fatal("NodesByLabel missed archived Case")
		}
	})

	t.Run("NodesByLabelAndProperty surfaces archived", func(t *testing.T) {
		nodes, err := ts.NodesByLabelAndProperty(caseTok, "status", "open", storepkg.QueryOpts{})
		if err != nil {
			t.Fatalf("NodesByLabelAndProperty: %v", err)
		}
		found := false
		for _, n := range nodes {
			if n.InternalID() == caseID {
				found = true
				break
			}
		}
		if !found {
			t.Fatal("NodesByLabelAndProperty missed archived Case")
		}
	})

	t.Run("NodeCountByLabel counts archived", func(t *testing.T) {
		count, err := ts.NodeCountByLabel(caseTok)
		if err != nil {
			t.Fatalf("NodeCountByLabel: %v", err)
		}
		if count < 1 {
			t.Fatalf("NodeCountByLabel(Case) = %d, want >= 1 (archived Case missed)", count)
		}
	})

	if archive := ts.RefArchiveForTest().Load(); archive != nil && archive.HasRelID(relID.SnowflakeID()) {
		t.Run("RelationshipsByType surfaces archived", func(t *testing.T) {
			rels, err := ts.RelationshipsByType(knowsTok, storepkg.QueryOpts{})
			if err != nil {
				t.Fatalf("RelationshipsByType: %v", err)
			}
			found := false
			for _, rr := range rels {
				if rr.InternalID() == relID {
					found = true
					break
				}
			}
			if !found {
				t.Fatal("RelationshipsByType missed archived rel")
			}
		})
		t.Run("RelCountByType counts archived", func(t *testing.T) {
			count, err := ts.RelCountByType(knowsTok)
			if err != nil {
				t.Fatalf("RelCountByType: %v", err)
			}
			if count < 1 {
				t.Fatalf("RelCountByType(KNOWS) = %d, want >= 1", count)
			}
		})
	}
}

// All / AllRelationships gate archive enumeration on Depth ==
// storepkg.DepthAll. Archive is the coldest tier of reference data; including
// it in storepkg.DepthHot or storepkg.DepthWarm would surface entities the caller asked
// to exclude. refShard is queried for all Depth values per existing
// semantics — reference data is not Depth-tiered.
func TestTieredStore_BulkQueries_DepthGatesArchive(t *testing.T) {
	g, ts := newTestTieredGraph(t)

	caseNode, err := g.Nodes.Add([]string{"Case"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	caseID := caseNode.InternalID()
	if err := ts.ArchiveNode(caseID); err != nil {
		t.Fatalf("ArchiveNode: %v", err)
	}

	containsNodeID := func(nodes []*types.Node, want types.NodeID) bool {
		for _, n := range nodes {
			if n.InternalID() == want {
				return true
			}
		}
		return false
	}

	t.Run("storepkg.DepthHot excludes archive", func(t *testing.T) {
		nodes, err := ts.AllNodes(storepkg.QueryOpts{Depth: storepkg.DepthHot})
		if err != nil {
			t.Fatalf("AllNodes(storepkg.DepthHot): %v", err)
		}
		if containsNodeID(nodes, caseID) {
			t.Fatal("AllNodes(storepkg.DepthHot) returned archived node — Depth gate did not exclude archive")
		}
	})

	t.Run("storepkg.DepthWarm excludes archive", func(t *testing.T) {
		nodes, err := ts.AllNodes(storepkg.QueryOpts{Depth: storepkg.DepthWarm})
		if err != nil {
			t.Fatalf("AllNodes(storepkg.DepthWarm): %v", err)
		}
		if containsNodeID(nodes, caseID) {
			t.Fatal("AllNodes(storepkg.DepthWarm) returned archived node — Depth gate did not exclude archive")
		}
	})

	t.Run("storepkg.DepthAll includes archive", func(t *testing.T) {
		nodes, err := ts.AllNodes(storepkg.QueryOpts{Depth: storepkg.DepthAll})
		if err != nil {
			t.Fatalf("AllNodes(storepkg.DepthAll): %v", err)
		}
		if !containsNodeID(nodes, caseID) {
			t.Fatal("AllNodes(storepkg.DepthAll) missed archived node")
		}
	})

	t.Run("default opts (Depth=0=storepkg.DepthAll) includes archive", func(t *testing.T) {
		nodes, err := ts.AllNodes(storepkg.QueryOpts{})
		if err != nil {
			t.Fatalf("AllNodes(zero): %v", err)
		}
		if !containsNodeID(nodes, caseID) {
			t.Fatal("AllNodes(zero opts) missed archived node — storepkg.DepthAll is the zero value")
		}
	})
}

func TestTieredStore_AllCurrentIDAPIs_IncludeArchiveAtDepthAll(t *testing.T) {
	g, ts := newTestTieredGraph(t)

	caseNode, err := g.Nodes.Add([]string{"Case"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	caseID := caseNode.InternalID()
	if err := ts.ArchiveNode(caseID); err != nil {
		t.Fatalf("ArchiveNode: %v", err)
	}

	t.Run("AllNodeIDs default includes archive", func(t *testing.T) {
		ids, err := ts.AllNodeIDs(storepkg.QueryOpts{})
		if err != nil {
			t.Fatalf("AllNodeIDs(default): %v", err)
		}
		if !containsNodeIDValue(ids, caseID) {
			t.Fatal("AllNodeIDs(default) missed archived node; GetNode/AllNodes see it")
		}
	})

	t.Run("AllNodeIDs storepkg.DepthAll includes archive", func(t *testing.T) {
		ids, err := ts.AllNodeIDs(storepkg.QueryOpts{Depth: storepkg.DepthAll})
		if err != nil {
			t.Fatalf("AllNodeIDs(storepkg.DepthAll): %v", err)
		}
		if !containsNodeIDValue(ids, caseID) {
			t.Fatal("AllNodeIDs(storepkg.DepthAll) missed archived node")
		}
	})

	t.Run("AllNodeIDs storepkg.DepthHot excludes archive", func(t *testing.T) {
		ids, err := ts.AllNodeIDs(storepkg.QueryOpts{Depth: storepkg.DepthHot})
		if err != nil {
			t.Fatalf("AllNodeIDs(storepkg.DepthHot): %v", err)
		}
		if containsNodeIDValue(ids, caseID) {
			t.Fatal("AllNodeIDs(storepkg.DepthHot) returned archived node")
		}
	})

	t.Run("AllNodeIDs storepkg.DepthWarm excludes archive", func(t *testing.T) {
		ids, err := ts.AllNodeIDs(storepkg.QueryOpts{Depth: storepkg.DepthWarm})
		if err != nil {
			t.Fatalf("AllNodeIDs(storepkg.DepthWarm): %v", err)
		}
		if containsNodeIDValue(ids, caseID) {
			t.Fatal("AllNodeIDs(storepkg.DepthWarm) returned archived node")
		}
	})

	relTS, relID, _ := mustArchivedRelationshipFixture(t)

	t.Run("AllRelIDs default includes archive", func(t *testing.T) {
		ids, err := relTS.AllRelIDs(storepkg.QueryOpts{})
		if err != nil {
			t.Fatalf("AllRelIDs(default): %v", err)
		}
		if !containsRelIDValue(ids, relID) {
			t.Fatal("AllRelIDs(default) missed archived relationship; GetRelationship can still resolve it")
		}
	})

	t.Run("AllRelIDs storepkg.DepthAll includes archive", func(t *testing.T) {
		ids, err := relTS.AllRelIDs(storepkg.QueryOpts{Depth: storepkg.DepthAll})
		if err != nil {
			t.Fatalf("AllRelIDs(storepkg.DepthAll): %v", err)
		}
		if !containsRelIDValue(ids, relID) {
			t.Fatal("AllRelIDs(storepkg.DepthAll) missed archived relationship")
		}
	})

	t.Run("AllRelIDs storepkg.DepthHot excludes archive", func(t *testing.T) {
		ids, err := relTS.AllRelIDs(storepkg.QueryOpts{Depth: storepkg.DepthHot})
		if err != nil {
			t.Fatalf("AllRelIDs(storepkg.DepthHot): %v", err)
		}
		if containsRelIDValue(ids, relID) {
			t.Fatal("AllRelIDs(storepkg.DepthHot) returned archived relationship")
		}
	})
}

func TestTieredStore_IndexedQueries_DepthGatesArchive(t *testing.T) {
	g, ts := newTestTieredGraph(t)

	caseNode, err := g.Nodes.Add([]string{"Case"}, map[string]any{"status": "open"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	caseID := caseNode.InternalID()
	if err := ts.ArchiveNode(caseID); err != nil {
		t.Fatalf("ArchiveNode: %v", err)
	}
	caseTok, ok := g.Resolve.LookupLabel("Case")
	if !ok {
		t.Fatal("Case label not registered")
	}

	t.Run("NodesByLabel default includes archive", func(t *testing.T) {
		nodes, err := ts.NodesByLabel(caseTok, storepkg.QueryOpts{})
		if err != nil {
			t.Fatalf("NodesByLabel(default): %v", err)
		}
		if !containsNodeEntity(nodes, caseID) {
			t.Fatal("NodesByLabel(default) missed archived node")
		}
	})

	t.Run("NodesByLabel storepkg.DepthHot excludes archive", func(t *testing.T) {
		nodes, err := ts.NodesByLabel(caseTok, storepkg.QueryOpts{Depth: storepkg.DepthHot})
		if err != nil {
			t.Fatalf("NodesByLabel(storepkg.DepthHot): %v", err)
		}
		if containsNodeEntity(nodes, caseID) {
			t.Fatal("NodesByLabel(storepkg.DepthHot) returned archived node")
		}
	})

	t.Run("NodesByLabelAndProperty default includes archive", func(t *testing.T) {
		nodes, err := ts.NodesByLabelAndProperty(caseTok, "status", "open", storepkg.QueryOpts{})
		if err != nil {
			t.Fatalf("NodesByLabelAndProperty(default): %v", err)
		}
		if !containsNodeEntity(nodes, caseID) {
			t.Fatal("NodesByLabelAndProperty(default) missed archived node")
		}
	})

	t.Run("NodesByLabelAndProperty storepkg.DepthWarm excludes archive", func(t *testing.T) {
		nodes, err := ts.NodesByLabelAndProperty(caseTok, "status", "open", storepkg.QueryOpts{Depth: storepkg.DepthWarm})
		if err != nil {
			t.Fatalf("NodesByLabelAndProperty(storepkg.DepthWarm): %v", err)
		}
		if containsNodeEntity(nodes, caseID) {
			t.Fatal("NodesByLabelAndProperty(storepkg.DepthWarm) returned archived node")
		}
	})

	relTS, relID, knowsTok := mustArchivedRelationshipFixture(t)

	t.Run("RelationshipsByType default includes archive", func(t *testing.T) {
		rels, err := relTS.RelationshipsByType(knowsTok, storepkg.QueryOpts{})
		if err != nil {
			t.Fatalf("RelationshipsByType(default): %v", err)
		}
		if !containsRelEntity(rels, relID) {
			t.Fatal("RelationshipsByType(default) missed archived relationship")
		}
	})

	t.Run("RelationshipsByType storepkg.DepthHot excludes archive", func(t *testing.T) {
		rels, err := relTS.RelationshipsByType(knowsTok, storepkg.QueryOpts{Depth: storepkg.DepthHot})
		if err != nil {
			t.Fatalf("RelationshipsByType(storepkg.DepthHot): %v", err)
		}
		if containsRelEntity(rels, relID) {
			t.Fatal("RelationshipsByType(storepkg.DepthHot) returned archived relationship")
		}
	})
}

func TestTieredStore_ForEachHistoryShard_PinsArchive(t *testing.T) {
	g, ts := newTestTieredGraph(t)

	caseNode, err := g.Nodes.Add([]string{"Case"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	if err := ts.ArchiveNode(caseNode.InternalID()); err != nil {
		t.Fatalf("ArchiveNode: %v", err)
	}

	archive := ts.RefArchiveForTest().Load()
	if archive == nil {
		t.Fatal("ArchiveNode left refArchive nil")
	}

	sawArchive := false
	err = ts.ForEachHistoryShardForTest(ts.RefShardForTest(), func(store *badger.Store) (bool, error) {
		if store != archive {
			return false, nil
		}
		sawArchive = true
		if got := ts.ArchiveActiveReqsForTest().Load(); got == 0 {
			t.Fatalf("archiveActiveReqs during history fallback = %d, want archive pinned", got)
		}
		return true, nil
	})
	if err != nil {
		t.Fatalf("forEachHistoryShard: %v", err)
	}
	if !sawArchive {
		t.Fatal("forEachHistoryShard did not visit refArchive")
	}
}

// TestTieredStore_TemporalIndexCreate_CoversArchive verifies
// CreateTemporalIndex installs the index on refArchive too — otherwise an
// archived reference node is silently absent from the temporal index even
// though it remains GetNode-addressable. Also covers DropTemporalIndex
// reaching the archive (orphan index files would otherwise persist).
func TestTieredStore_TemporalIndexCreate_CoversArchive(t *testing.T) {
	g, ts := newTestTieredGraph(t)

	caseNode, err := g.Nodes.Add([]string{"Case"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	if err := ts.ArchiveNode(caseNode.InternalID()); err != nil {
		t.Fatalf("ArchiveNode: %v", err)
	}
	caseTok, ok := g.Resolve.LookupLabel("Case")
	if !ok {
		t.Fatal("Case label not registered")
	}

	if err := ts.CreateTemporalIndex(caseTok); err != nil {
		t.Fatalf("CreateTemporalIndex: %v", err)
	}

	archive := ts.RefArchiveForTest().Load()
	if archive == nil {
		t.Fatal("ArchiveNode left refArchive nil")
	}
	if !archive.HasTemporalIndexForTest(caseTok) {
		t.Fatal("CreateTemporalIndex did not install index on refArchive; archived reference nodes will silently fall out of the temporal index")
	}

	if err := ts.DropTemporalIndex(caseTok); err != nil {
		t.Fatalf("DropTemporalIndex: %v", err)
	}
	if archive.HasTemporalIndexForTest(caseTok) {
		t.Fatal("DropTemporalIndex left orphan temporal index on refArchive")
	}
}

// TestTieredStore_ResolveShardStore_PinsArchive verifies that
// resolveShardStore("archive") increments archiveActiveReqs and returns
// a real (non-noop) checkin. Without the pin, a long-running admin
// operation like VerifyShard("archive") races a concurrent Close: Close
// drains archiveActiveReqs (sees zero), proceeds to archive.Close(),
// and the verifier hits Badger v4's Flush-on-closed-DB hang.
func TestTieredStore_ResolveShardStore_PinsArchive(t *testing.T) {
	g, ts := newTestTieredGraph(t)

	caseNode, err := g.Nodes.Add([]string{"Case"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	if err := ts.ArchiveNode(caseNode.InternalID()); err != nil {
		t.Fatalf("ArchiveNode: %v", err)
	}

	before := ts.ArchiveActiveReqsForTest().Load()
	store, release, err := ts.ResolveShardStoreForTest("archive")
	if err != nil {
		t.Fatalf("resolveShardStore(archive): %v", err)
	}
	if store == nil {
		t.Fatal("resolveShardStore(archive) returned nil store")
	}
	during := ts.ArchiveActiveReqsForTest().Load()
	if during != before+1 {
		t.Fatalf("archiveActiveReqs = %d during resolve, want %d (archive not pinned — Close race window)", during, before+1)
	}
	release()
	after := ts.ArchiveActiveReqsForTest().Load()
	if after != before {
		t.Fatalf("archiveActiveReqs = %d after release, want %d (release didn't drop the pin)", after, before)
	}
}

// TestTieredStore_FindRelInAnyShardStore_ProbesArchive verifies that
// the helper used by RunRepair Phase 1 to determine "does this rel
// exist anywhere?" probes refArchive. Pre-fix, an archived rel looked
// "missing everywhere" so a real in/ entry on another shard pointing
// to it was treated as orphaned and DELETED — silent data loss.
func TestTieredStore_FindRelInAnyShardStore_ProbesArchive(t *testing.T) {
	ts, relID, _ := mustArchivedRelationshipFixture(t)

	stores, release, err := ts.AllShardStoresWithLazyOpenForTest()
	if err != nil {
		t.Fatalf("allShardStoresWithLazyOpen: %v", err)
	}
	defer release()

	owner := ts.FindRelInAnyShardStoreForTest(relID.SnowflakeID(), stores)
	if owner == nil {
		t.Fatal("findRelInAnyShardStore returned nil for archived rel; Phase 1 of RunRepair will treat its in/ entries as orphaned and delete them (data loss)")
	}
	if owner != ts.RefArchiveForTest().Load() {
		t.Fatalf("findRelInAnyShardStore returned wrong store; want refArchive, got %p (refShard=%p)", owner, ts.RefShardForTest())
	}
}

// TestTieredStore_AllShardStoresWithLazyOpen_IncludesArchive verifies
// the admin-side store enumeration used by RunRepair / repair scans
// includes refArchive. Otherwise the rel-IDs and in/ entries on the
// archive are entirely invisible to the repair scanner.
func TestTieredStore_AllShardStoresWithLazyOpen_IncludesArchive(t *testing.T) {
	g, ts := newTestTieredGraph(t)

	caseNode, err := g.Nodes.Add([]string{"Case"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	if err := ts.ArchiveNode(caseNode.InternalID()); err != nil {
		t.Fatalf("ArchiveNode: %v", err)
	}
	archive := ts.RefArchiveForTest().Load()
	if archive == nil {
		t.Fatal("ArchiveNode left refArchive nil")
	}

	stores, release, err := ts.AllShardStoresWithLazyOpenForTest()
	if err != nil {
		t.Fatalf("allShardStoresWithLazyOpen: %v", err)
	}
	defer release()

	saw := false
	for _, ns := range stores {
		if ns.StoreForTest() == archive {
			saw = true
			break
		}
	}
	if !saw {
		t.Fatal("allShardStoresWithLazyOpen did not include refArchive; RunRepair / repair scanners cannot inspect archived entities")
	}
}

// TestTieredStore_HighFrequencyIndexCreate_CoversArchive — same gap as
// the temporal index test above, for high-frequency time-bucketed indexes.
func TestTieredStore_HighFrequencyIndexCreate_CoversArchive(t *testing.T) {
	g, ts := newTestTieredGraph(t)

	caseNode, err := g.Nodes.Add([]string{"Case"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	if err := ts.ArchiveNode(caseNode.InternalID()); err != nil {
		t.Fatalf("ArchiveNode: %v", err)
	}
	caseTok, ok := g.Resolve.LookupLabel("Case")
	if !ok {
		t.Fatal("Case label not registered")
	}

	if err := ts.CreateHighFrequencyIndex(caseTok, time.Hour); err != nil {
		t.Fatalf("CreateHighFrequencyIndex: %v", err)
	}

	archive := ts.RefArchiveForTest().Load()
	if archive == nil {
		t.Fatal("ArchiveNode left refArchive nil")
	}
	if !archive.HasHFIndexForTest(caseTok) {
		t.Fatal("CreateHighFrequencyIndex did not install HFI on refArchive")
	}

	if err := ts.DropHighFrequencyIndex(caseTok); err != nil {
		t.Fatalf("DropHighFrequencyIndex: %v", err)
	}
	if archive.HasHFIndexForTest(caseTok) {
		t.Fatal("DropHighFrequencyIndex left orphan HFI on refArchive")
	}
}
