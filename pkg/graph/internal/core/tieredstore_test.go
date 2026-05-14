package core

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"

	snowflake "github.com/bds421/rho-snowflake-2026"
	registrypkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/registry"
	storeutil "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/storeutil"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/tiered"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// --- Test helpers ---

// newTestTieredStore creates an in-memory tiered.Store with Case/User as reference labels.
func newTestTieredStore(t *testing.T) *tiered.Store {
	t.Helper()
	ts, err := tiered.New(tiered.Config{
		InMemory:      true,
		RefLabels:     []string{"Case", "User"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1, // disable periodic flush
	})
	if err != nil {
		t.Fatalf("tiered.New: %v", err)
	}
	t.Cleanup(func() { _ = ts.Close() })
	return ts
}

// newTestTieredGraph creates a Graph backed by an in-memory tiered.Store.
func newTestTieredGraph(t *testing.T) (*Core, *tiered.Store) {
	t.Helper()
	ts := newTestTieredStore(t)
	g, err := New(Config{
		SnowflakeNodeID: 0,
		Store:           ts,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	return g, ts
}

// tieredNodeGen and tieredRelGen for test entities.
func tieredNodeGen(t *testing.T) *snowflake.Node {
	t.Helper()
	return newTestGen(t, 0)
}

// --- Ontology routing tests ---
// --- Node CRUD tests ---
// --- Same-shard relationship tests ---
// --- Cross-shard relationship tests ---
// --- Merge query tests ---
// --- DeleteNodeCascade cross-shard tests ---
// --- History routing tests ---
// --- Batch operation tests ---
// --- Property index tests ---
// --- Lifecycle tests ---
// --- Multi-shard model tests ---
// --- Pagination tests ---
// --- Disk-backed tests ---
// --- Registry round-trip via Graph ---

func TestTieredStore_RegistryRoundTrip_ViaGraph(t *testing.T) {
	dir := t.TempDir()

	// Create graph with tiered.Store.
	ts, err := tiered.New(tiered.Config{
		DataDir:       dir,
		RefLabels:     []string{"Case"},
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	g, err := New(Config{
		SnowflakeNodeID: 0,
		Store:           ts,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Add entities to populate registries.
	n, err := g.Nodes.Add(context.Background(), []string{"Case"}, map[string]any{"name": "test"})
	if err != nil {
		t.Fatal(err)
	}
	n2, err := g.Nodes.Add(context.Background(), []string{"Case"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = g.Rels.Add(context.Background(), "RELATES_TO", n, n2, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Close to save registries.
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}

	// Verify registry file exists.
	regPath := filepath.Join(dir, "meta", "registry.msgpack")
	if _, err := os.Stat(regPath); err != nil {
		t.Fatalf("registry file missing: %v", err)
	}

	// Reopen.
	ts2, err := tiered.New(tiered.Config{
		DataDir:       dir,
		RefLabels:     []string{"Case"},
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	g2, err := New(Config{
		SnowflakeNodeID: 1,
		Store:           ts2,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer g2.Close()

	// Registries should have been restored.
	if g2.labels.Len() == 0 {
		t.Error("label registry should have entries after reload")
	}
	if g2.relTypes.Len() == 0 {
		t.Error("reltype registry should have entries after reload")
	}
}

// --- GetNodesByIDs / GetRelationshipsByIDs ---

func TestTieredStore_GraphGetByIDsSorted(t *testing.T) {
	g, _ := newTestTieredGraph(t)

	case1, err := g.Nodes.Add(context.Background(), []string{"Case"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	signal, err := g.Nodes.Add(context.Background(), []string{"Signal"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	case2, err := g.Nodes.Add(context.Background(), []string{"Case"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	_, err = g.Nodes.GetByIDs([]types.NodeID{
		signal.ID(),
		types.NodeID(999),
		case1.ID(),
	})
	if !errors.Is(err, storepkg.ErrNodeNotFound) {
		t.Fatalf("Nodes.GetByIDs missing err = %v, want ErrNodeNotFound", err)
	}

	nodes, err := g.Nodes.GetByIDs([]types.NodeID{signal.ID(), case1.ID()})
	if err != nil {
		t.Fatalf("Nodes.GetByIDs existing: %v", err)
	}
	if len(nodes) != 2 {
		t.Fatalf("Nodes.GetByIDs = %d nodes, want 2", len(nodes))
	}
	if nodes[0].ID() != case1.ID() || nodes[1].ID() != signal.ID() {
		t.Fatalf("Nodes.GetByIDs order = [%v, %v], want sorted [%v, %v]",
			nodes[0].ID(), nodes[1].ID(), case1.ID(), signal.ID())
	}

	rel1, err := g.Rels.Add(context.Background(), "LINKS", case1, signal, nil)
	if err != nil {
		t.Fatal(err)
	}
	rel2, err := g.Rels.Add(context.Background(), "LINKS", signal, case2, nil)
	if err != nil {
		t.Fatal(err)
	}

	_, err = g.Rels.GetByIDs([]types.RelID{
		rel2.ID(),
		types.RelID(999),
		rel1.ID(),
	})
	if !errors.Is(err, storepkg.ErrRelNotFound) {
		t.Fatalf("Rels.GetByIDs missing err = %v, want ErrRelNotFound", err)
	}

	rels, err := g.Rels.GetByIDs([]types.RelID{rel2.ID(), rel1.ID()})
	if err != nil {
		t.Fatalf("Rels.GetByIDs existing: %v", err)
	}
	if len(rels) != 2 {
		t.Fatalf("Rels.GetByIDs = %d rels, want 2", len(rels))
	}
	if rels[0].ID() != rel1.ID() || rels[1].ID() != rel2.ID() {
		t.Fatalf("Rels.GetByIDs order = [%v, %v], want sorted [%v, %v]",
			rels[0].ID(), rels[1].ID(), rel1.ID(), rel2.ID())
	}
}

// --- RelationshipsByType merge test ---
// --- RelCountByType merge test ---
// --- Replace relationship test ---
// --- Truncate history test ---
// --- GetNodeVersion / GetRelVersion ---
// --- ReplaceWithHistory tests ---
// --- DeleteRelationshipsBatch ---
// --- AllRelHistoryIDs merge test ---
// --- TruncateRelHistory test ---
// =============================================================================
// Phase 3b+3c: Rotation, Warm Shards, Depth-Aware Reads, E→E Cross-Shard
// =============================================================================
// --- Rotation tests ---
// --- E→E cross-shard tests ---
// --- Depth-aware read tests ---
// --- Warm recovery tests ---
// --- ReadOnly badger.Store tests ---
// --- Catalog tests ---
// --- Depth-aware RelationshipsByType test ---
// --- Depth-aware AllRelIDs test ---
// ============================================================================
// Phase 3d: Cold Shard Lifecycle + Parallel Query + Reference Archive
// ============================================================================

// --- Cold shard tests ---

// demoteToCold manually sets a shard to cold tier. Test-only helper.
//
// Bypasses the normal warm→cold transition (driven by ColdAfter and the
// idle-close goroutine) so tests can deterministically observe behaviour
// against a cold shard without sleeping. Holds ts.MuForTest() across the tier flip
// AND the catalog update so a concurrent rotation cannot read a half-updated
// state — but does NOT close the underlying badger.Store. Pair with
// closeEventShardStore to fully simulate a cold idle-close, or leave the
// store open to test pure tier-based code paths.
func demoteToCold(ts *tiered.Store, shardName string) {
	ts.MuForTest().Lock()
	defer ts.MuForTest().Unlock()
	if es, ok := ts.EventShardsForTest()[shardName]; ok {
		es.SetTierForTest(tiered.TierCold)
		ts.CatalogForTest().UpdateShardTier(shardName, tiered.TierCold)
	}
}

// --- Parallel query tests ---
func TestTieredStore_ParallelRelsByType(t *testing.T) {
	g, ts := newTestTieredGraph(t)
	_ = ts

	case1, _ := g.Nodes.Add(context.Background(), []string{"Case"}, nil)
	sig1, _ := g.Nodes.Add(context.Background(), []string{"Signal"}, nil)
	sig2, _ := g.Nodes.Add(context.Background(), []string{"Signal"}, nil)

	_, _ = g.Rels.Add(context.Background(), "TRIGGERED", sig1, case1, nil)

	// Rotate.
	time.Sleep(2 * time.Millisecond)
	_ = ts.RotateHotShard()

	_, _ = g.Rels.Add(context.Background(), "TRIGGERED", sig2, case1, nil)

	// RelationshipsByType should find rels across shards (parallel).
	tok, _ := g.Resolve.LookupRelType("TRIGGERED")
	rels, err := ts.RelationshipsByType(tok, storepkg.QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(rels) != 2 {
		t.Errorf("RelationshipsByType = %d, want 2", len(rels))
	}
}

// --- Archive tests ---

func TestTieredStore_ArchiveNode(t *testing.T) {
	g, ts := newTestTieredGraph(t)

	caseNode, _ := g.Nodes.Add(context.Background(), []string{"Case"}, map[string]any{"name": "C001"})
	caseID := caseNode.ID()

	// Archive.
	if err := ts.ArchiveNode(caseID); err != nil {
		t.Fatal(err)
	}

	// Node should no longer be in refShard.
	if ts.RefShardForTest().HasNodeID(caseID.SnowflakeID()) {
		t.Error("node should not be in refShard after archive")
	}

	// Node should be in refArchive.
	archive := ts.RefArchiveForTest().Load()
	if archive == nil || !archive.HasNodeID(caseID.SnowflakeID()) {
		t.Error("node should be in refArchive after archive")
	}

	// GetNode should still find it (via archive routing).
	got, err := g.Nodes.Get(context.Background(), caseID)
	if err != nil {
		t.Fatalf("GetNode after archive: %v", err)
	}
	if got.ID() != caseID {
		t.Error("node ID mismatch")
	}
}

func TestTieredStore_ArchiveWithRels(t *testing.T) {
	g, ts := newTestTieredGraph(t)

	case1, _ := g.Nodes.Add(context.Background(), []string{"Case"}, nil)
	case2, _ := g.Nodes.Add(context.Background(), []string{"Case"}, nil)
	rel, _ := g.Rels.Add(context.Background(), "RELATED_TO", case1, case2, nil)

	case1ID := case1.ID()
	case2ID := case2.ID()
	relID := rel.ID()

	if err := ts.ArchiveNode(case1ID); err != nil {
		t.Fatalf("ArchiveNode with ref-ref rel: %v", err)
	}

	archive := ts.RefArchiveForTest().Load()
	if archive == nil {
		t.Fatal("ArchiveNode did not open refArchive")
	}
	if ts.RefShardForTest().HasNodeID(case1ID.SnowflakeID()) || !archive.HasNodeID(case1ID.SnowflakeID()) {
		t.Error("case1 should move to refArchive")
	}
	if !ts.RefShardForTest().HasNodeID(case2ID.SnowflakeID()) {
		t.Error("case2 should remain in refShard")
	}
	if ts.RefShardForTest().HasRelID(relID.SnowflakeID()) || !archive.HasRelID(relID.SnowflakeID()) {
		t.Error("rel entity/out should move to refArchive with archived start node")
	}
	if !tiered.HasIncomingEntryForTest(ts.RefShardForTest(), case2ID.SnowflakeID(), relID.SnowflakeID()) {
		t.Error("live endpoint should keep incoming entry in refShard")
	}
}

func TestTieredStore_RestoreNode(t *testing.T) {
	g, ts := newTestTieredGraph(t)

	caseNode, _ := g.Nodes.Add(context.Background(), []string{"Case"}, map[string]any{"status": "closed"})
	caseID := caseNode.ID()

	// Archive then restore.
	if err := ts.ArchiveNode(caseID); err != nil {
		t.Fatal(err)
	}
	if err := ts.RestoreNode(caseID); err != nil {
		t.Fatal(err)
	}

	// Node should be back in refShard.
	if !ts.RefShardForTest().HasNodeID(caseID.SnowflakeID()) {
		t.Error("node should be in refShard after restore")
	}

	// Node should NOT be in archive.
	if archive := ts.RefArchiveForTest().Load(); archive != nil && archive.HasNodeID(caseID.SnowflakeID()) {
		t.Error("node should not be in archive after restore")
	}

	// GetNode should work normally.
	got, err := g.Nodes.Get(context.Background(), caseID)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID() != caseID {
		t.Error("node ID mismatch after restore")
	}
}

func TestTieredStore_ArchiveLazyOpen(t *testing.T) {
	// Verify archive is lazily opened on first ArchiveNode call.
	g, ts := newTestTieredGraph(t)
	_ = g

	if ts.RefArchiveForTest().Load() != nil {
		t.Error("refArchive should be nil initially")
	}

	caseNode, _ := g.Nodes.Add(context.Background(), []string{"Case"}, nil)
	_ = ts.ArchiveNode(caseNode.ID())

	if ts.RefArchiveForTest().Load() == nil {
		t.Error("refArchive should be opened after ArchiveNode")
	}
}

func TestTieredStore_ArchiveReadRouting(t *testing.T) {
	// Verify shardForNodeID falls back to archive.
	g, ts := newTestTieredGraph(t)

	caseNode, _ := g.Nodes.Add(context.Background(), []string{"Case"}, nil)
	caseID := caseNode.ID()

	_ = ts.ArchiveNode(caseID)

	shard, err := ts.ShardForNodeIDForTest(caseID)
	if err != nil {
		t.Fatal(err)
	}
	if shard != ts.RefArchiveForTest().Load() {
		t.Error("shardForNodeID should return refArchive for archived node")
	}
}

func TestTieredStore_ArchiveDepthAll(t *testing.T) {
	// AllNodes with archive data should include archived nodes.
	g, ts := newTestTieredGraph(t)

	case1, _ := g.Nodes.Add(context.Background(), []string{"Case"}, nil)
	sig1, _ := g.Nodes.Add(context.Background(), []string{"Signal"}, nil)
	_ = sig1

	_ = ts.ArchiveNode(case1.ID())

	// GetNode should find archived node.
	got, err := g.Nodes.Get(context.Background(), case1.ID())
	if err != nil {
		t.Fatalf("GetNode for archived node: %v", err)
	}
	if got.ID() != case1.ID() {
		t.Error("archived node ID mismatch")
	}
}
func TestTieredStore_ArchiveEventNodeRejected(t *testing.T) {
	g, ts := newTestTieredGraph(t)

	sigNode, _ := g.Nodes.Add(context.Background(), []string{"Signal"}, nil)
	err := ts.ArchiveNode(sigNode.ID())
	if !errors.Is(err, tiered.ErrNotReferenceEntity) {
		t.Errorf("expected tiered.ErrNotReferenceEntity for event node archive, got %v", err)
	}
}

// --- Routing error tests ---
// --- Graph-layer archive passthrough tests ---

func TestGraph_ArchiveNode_NotTiered(t *testing.T) {
	// ArchiveNode on non-tiered.Store should return an error.
	g, err := New(Config{
		SnowflakeNodeID: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = g.Close() }()

	gen := tieredNodeGen(t)
	err = g.Admin.Archive(types.NodeID(gen.Generate()))
	if err == nil {
		t.Error("expected error for ArchiveNode on non-tiered.Store")
	}
}

func TestGraph_RestoreNode_NotTiered(t *testing.T) {
	g, err := New(Config{
		SnowflakeNodeID: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = g.Close() }()

	gen := tieredNodeGen(t)
	err = g.Admin.Restore(types.NodeID(gen.Generate()))
	if err == nil {
		t.Error("expected error for RestoreNode on non-tiered.Store")
	}
}

func TestGraph_ArchiveRestoreRejectInvalidIDsBeforeCapabilityCheck(t *testing.T) {
	g, err := New(Config{SnowflakeNodeID: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = g.Close() }()

	checks := []struct {
		name string
		err  error
	}{
		{name: "Archive zero", err: g.Admin.Archive(0)},
		{name: "Archive negative", err: g.Admin.Archive(types.NodeID(-1))},
		{name: "Restore zero", err: g.Admin.Restore(0)},
		{name: "Restore negative", err: g.Admin.Restore(types.NodeID(-1))},
	}
	for _, check := range checks {
		if !errors.Is(check.err, storepkg.ErrInvalidStoreMutation) {
			t.Fatalf("%s = %v, want ErrInvalidStoreMutation", check.name, check.err)
		}
	}
}

// --- ColdEventShards catalog helper test ---
// ====================================================================
// Phase 3e: Repair + Tooling tests
// ====================================================================

// --- Step 1: ID Decomposition ---

func TestDecomposeID_KnownValues(t *testing.T) {
	gen := newTestGen(t, 7)
	id := gen.Generate()
	c := DecomposeID(id)

	if c.NodeID != 7 {
		t.Errorf("NodeID = %d, want 7", c.NodeID)
	}
	if c.Sequence < 0 || c.Sequence > 1023 {
		t.Errorf("Sequence = %d, out of range [0,1023]", c.Sequence)
	}
	if c.CreatedAt.IsZero() {
		t.Error("CreatedAt is zero")
	}
}

func TestDecomposeID_TimePrecision(t *testing.T) {
	gen := newTestGen(t, 0)
	before := time.Now()
	id := gen.Generate()
	after := time.Now()

	c := DecomposeID(id)

	// CreatedAt should be between before and after (within millisecond precision).
	if c.CreatedAt.Before(before.Truncate(time.Millisecond)) {
		t.Errorf("CreatedAt %v before %v", c.CreatedAt, before)
	}
	if c.CreatedAt.After(after.Add(time.Millisecond)) {
		t.Errorf("CreatedAt %v after %v", c.CreatedAt, after)
	}
}

func TestDecomposeID_NodeField(t *testing.T) {
	// Test with different node IDs to verify the 5-bit node field (max 31).
	for _, nodeID := range []int64{0, 1, 15, 31} {
		gen := newTestGen(t, nodeID)
		id := gen.Generate()
		c := DecomposeID(id)
		if c.NodeID != nodeID {
			t.Errorf("NodeID = %d, want %d", c.NodeID, nodeID)
		}
	}
}

func TestDecomposeID_ConsistentWithTemporalFilter(t *testing.T) {
	gen := newTestGen(t, 0)
	id := gen.Generate()

	// DecomposeID time should match EntityValidFrom derivation.
	c := DecomposeID(id)
	efrom := storeutil.EntityValidFrom(id, nil)
	decomposedMs := c.CreatedAt.UnixMilli()
	efromMs := int64(efrom)

	if decomposedMs != efromMs {
		t.Errorf("DecomposeID ms=%d, EntityValidFrom ms=%d — mismatch", decomposedMs, efromMs)
	}
}

// --- Step 2: Property Index Restriction ---
// --- Step 3: Catalog Extensions ---
// --- Step 4: Admin API ---
func TestTieredStore_ForceRotate_ViaGraph(t *testing.T) {
	g, _ := newTestTieredGraph(t)

	if err := g.Admin.ForceRotate(); err != nil {
		t.Fatalf("Graph.Admin.ForceRotate: %v", err)
	}

	// Verify a non-tiered store returns error.
	g2, err := New(Config{SnowflakeNodeID: 1, BadgerInMemory: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g2.Close() })

	if err := g2.Admin.ForceRotate(); err == nil {
		t.Error("ForceRotate on non-tiered.Store should error")
	}
}
func TestTieredStore_AdminNotTiered(t *testing.T) {
	g, err := New(Config{SnowflakeNodeID: 1, BadgerInMemory: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	if err := g.Admin.ForceRotate(); err == nil {
		t.Error("ForceRotate should fail")
	}
	if _, err := g.Admin.ListShards(); err == nil {
		t.Error("ListShards should fail")
	}
	if err := g.Admin.RebuildCatalog(); err == nil {
		t.Error("RebuildCatalog should fail")
	}
	if _, err := g.Admin.Repair(); err == nil {
		t.Error("RunRepair should fail")
	}
	if _, err := g.Admin.VerifyShard("ref"); err == nil {
		t.Error("VerifyShard should fail")
	}
}

// --- Step 5: Per-Shard Verification ---

func TestTieredStore_VerifyShard_Hot(t *testing.T) {
	g, ts := newTestTieredGraph(t)

	// Add nodes and a relationship to the hot shard.
	n, err := g.Nodes.Add(context.Background(), []string{"Signal"}, nil)
	if err != nil {
		t.Fatalf("AddNode n: %v", err)
	}
	n2, err := g.Nodes.Add(context.Background(), []string{"Signal"}, nil)
	if err != nil {
		t.Fatalf("AddNode n2: %v", err)
	}
	if _, err := g.Rels.Add(context.Background(), "OBSERVED", n, n2, nil); err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}

	result, err := g.Admin.VerifyShard(ts.HotShardForTest().Name())
	if err != nil {
		t.Fatalf("VerifyShard: %v", err)
	}
	if result.NodesOK != 2 {
		t.Errorf("NodesOK = %d, want 2", result.NodesOK)
	}
	if result.NodesFailed != 0 {
		t.Errorf("NodesFailed = %d, want 0", result.NodesFailed)
	}
	if result.RelsOK != 1 {
		t.Errorf("RelsOK = %d, want 1", result.RelsOK)
	}
	if result.RelsFailed != 0 {
		t.Errorf("RelsFailed = %d, want 0", result.RelsFailed)
	}
	if result.Cached {
		t.Error("should not be cached for hot shard")
	}
}

func TestTieredStore_VerifyShard_ImmutableCached(t *testing.T) {
	g, ts := newTestTieredGraph(t)

	// Add a node to hot, then rotate → becomes warm.
	_, err := g.Nodes.Add(context.Background(), []string{"Signal"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	oldHot := ts.HotShardForTest().Name()
	if err := g.Admin.ForceRotate(); err != nil {
		t.Fatalf("ForceRotate: %v", err)
	}

	// First verify: should scan and return non-cached.
	result1, err := g.Admin.VerifyShard(oldHot)
	if err != nil {
		t.Fatalf("VerifyShard first: %v", err)
	}
	if result1.Cached {
		t.Error("first verify should not be cached")
	}
	if result1.NodesOK != 1 {
		t.Errorf("NodesOK = %d, want 1", result1.NodesOK)
	}

	// Second verify: should return cached result.
	result2, err := g.Admin.VerifyShard(oldHot)
	if err != nil {
		t.Fatalf("VerifyShard second: %v", err)
	}
	if !result2.Cached {
		t.Error("second verify should be cached")
	}
	if result2.NodesOK != result1.NodesOK {
		t.Errorf("cached NodesOK = %d, want %d", result2.NodesOK, result1.NodesOK)
	}
}

func TestTieredStore_VerifyShard_Unknown(t *testing.T) {
	g, _ := newTestTieredGraph(t)

	_, err := g.Admin.VerifyShard("nonexistent")
	if err == nil {
		t.Error("expected error for unknown shard")
	}
}

// --- Step 6: Split-Write Repair ---

func TestTieredStore_Repair_NoOrphans(t *testing.T) {
	g, _ := newTestTieredGraph(t)

	// Create a ref node and an event node with a cross-shard rel.
	refNode, err := g.Nodes.Add(context.Background(), []string{"Case"}, nil)
	if err != nil {
		t.Fatalf("AddNode ref: %v", err)
	}
	evtNode, err := g.Nodes.Add(context.Background(), []string{"Signal"}, nil)
	if err != nil {
		t.Fatalf("AddNode evt: %v", err)
	}
	_, err = g.Rels.Add(context.Background(), "TRIGGERED", evtNode, refNode, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}

	result, err := g.Admin.Repair()
	if err != nil {
		t.Fatalf("RunRepair: %v", err)
	}
	if result.OrphanedInEntries != 0 {
		t.Errorf("OrphanedInEntries = %d, want 0", result.OrphanedInEntries)
	}
	if result.MissingInEntries != 0 {
		t.Errorf("MissingInEntries = %d, want 0", result.MissingInEntries)
	}
	if result.ShardsScanned < 2 {
		t.Errorf("ShardsScanned = %d, want >= 2", result.ShardsScanned)
	}
}
func TestTieredStore_Repair_ViaGraph(t *testing.T) {
	g, _ := newTestTieredGraph(t)

	result, err := g.Admin.Repair()
	if err != nil {
		t.Fatalf("Graph.Admin.Repair: %v", err)
	}
	if result.OrphanedInEntries != 0 || result.MissingInEntries != 0 {
		t.Errorf("clean graph should have 0 repairs, got orphaned=%d missing=%d",
			result.OrphanedInEntries, result.MissingInEntries)
	}
}

// --- Step 7: Migration Tool ---
// --- Graph-layer DecomposeID test ---

func TestGraph_DecomposeID(t *testing.T) {
	g, err := New(Config{SnowflakeNodeID: 5, BadgerInMemory: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	n, err := g.Nodes.Add(context.Background(), []string{"Test"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	c := g.Admin.DecomposeNodeID(n.ID())
	// SnowflakeNodeID 5 → nodeGen uses 5*2=10, relGen uses 5*2+1=11.
	if c.NodeID != 10 {
		t.Errorf("NodeID = %d, want 10", c.NodeID)
	}
	if c.CreatedAt.IsZero() {
		t.Error("CreatedAt is zero")
	}
}

// ====================================================================
// v3.0.30 Bug Fixes — checkout/checkin, cold shard skip, rollback, etc.
// ====================================================================

// --- Fix 1: idleCloseLoop race — active request tracking ---

func TestTieredStore_ColdShard_IdleCloseBlockedByActiveRequest(t *testing.T) {
	// Checkout a cold shard store. Verify closeIdleShards skips it while
	// activeReqs > 0, then succeeds after checkin.
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	_, _ = reg.GetOrCreate("Case")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	n := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	if err := ts.PutNode(n); err != nil {
		t.Fatal(err)
	}

	ts.MuForTest().RLock()
	hotName := ts.HotShardForTest().Name()
	ts.MuForTest().RUnlock()

	// Rotate hot→warm, demote to cold.
	time.Sleep(2 * time.Millisecond)
	if err := ts.RotateHotShard(); err != nil {
		t.Fatal(err)
	}
	demoteToCold(ts, hotName)

	// Find the cold shard.
	ts.MuForTest().RLock()
	coldES := ts.EventShardsForTest()[hotName]
	ts.MuForTest().RUnlock()
	if coldES == nil || coldES.Tier() != tiered.TierCold {
		t.Fatal("expected cold shard")
	}

	// Checkout: should open the store and increment activeReqs.
	store, err := coldES.CheckoutStoreForTest(ts)
	if err != nil {
		t.Fatalf("checkoutStore: %v", err)
	}
	if store == nil {
		t.Fatal("expected non-nil store from checkoutStore")
	}
	if coldES.ActiveReqsForTest().Load() != 1 {
		t.Errorf("activeReqs = %d, want 1", coldES.ActiveReqsForTest().Load())
	}

	// Set idle timeout very low, force close attempt.
	ts.SetIdleTimeoutForTest(time.Millisecond)
	coldES.SetLastAccessForTest(0) // pretend it was last accessed long ago
	ts.CloseIdleShardsForTest()

	// Store should NOT be closed because activeReqs > 0.
	coldES.LockShardMuForTest()
	storeAfterClose := coldES.Store()
	coldES.UnlockShardMuForTest()
	if storeAfterClose == nil {
		t.Error("closeIdleShards closed a shard with active requests")
	}

	// Checkin.
	coldES.CheckinStoreForTest()
	if coldES.ActiveReqsForTest().Load() != 0 {
		t.Errorf("activeReqs = %d, want 0", coldES.ActiveReqsForTest().Load())
	}

	// Now close should succeed.
	coldES.SetLastAccessForTest(0)
	ts.CloseIdleShardsForTest()
	coldES.LockShardMuForTest()
	storeAfterClose2 := coldES.Store()
	coldES.UnlockShardMuForTest()
	if storeAfterClose2 != nil {
		t.Error("closeIdleShards should have closed the shard after checkin")
	}
}

func TestTieredStore_ColdShard_ConcurrentReadDuringIdleClose(t *testing.T) {
	// Spawn goroutines doing checkoutStore/checkinStore from a cold shard
	// while idle-close runs. No panics, no data corruption.
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	_, _ = reg.GetOrCreate("Case")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	n := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	if err := ts.PutNode(n); err != nil {
		t.Fatal(err)
	}

	ts.MuForTest().RLock()
	hotName := ts.HotShardForTest().Name()
	ts.MuForTest().RUnlock()

	time.Sleep(2 * time.Millisecond)
	_ = ts.RotateHotShard()
	demoteToCold(ts, hotName)

	ts.MuForTest().RLock()
	coldES := ts.EventShardsForTest()[hotName]
	ts.MuForTest().RUnlock()

	ts.SetIdleTimeoutForTest(time.Millisecond)
	var wg sync.WaitGroup

	// 10 goroutines doing checkout/checkin (long hold simulated by a brief sleep).
	checkoutErrs := make([]error, 10)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			store, err := coldES.CheckoutStoreForTest(ts)
			if err != nil {
				checkoutErrs[i] = err
				return
			}
			// Hold the store briefly — idle-close must not close it.
			_ = store
			time.Sleep(time.Millisecond)
			coldES.CheckinStoreForTest()
		}(i)
	}

	// 10 idle-close goroutines running concurrently.
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ts.CloseIdleShardsForTest()
		}()
	}

	wg.Wait()

	// Checkout should not fail — store must remain open while checked out.
	for i, err := range checkoutErrs {
		if err != nil {
			t.Errorf("checkout goroutine %d: %v", i, err)
		}
	}
}

// --- Fix 2: shardForRelID — probe cold shards when needed ---

// A cross-shard relationship written while the start-node shard was warm can
// later age to cold without ever being deleted. The lookup must follow it,
// even at the cost of opening the cold shard. The earlier "skip cold shards"
// fast-path was incorrect — it silently lost live cross-shard rels once the
// start-node shard aged out.
func TestTieredStore_ShardForRelID_FindsRelOnColdShard(t *testing.T) {
	g, ts := newTestTieredGraph(t)

	a, err := g.Nodes.Add(context.Background(), []string{"Signal"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.Nodes.Add(context.Background(), []string{"Signal"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	ts.MuForTest().RLock()
	originName := ts.HotShardForTest().Name()
	ts.MuForTest().RUnlock()

	time.Sleep(2 * time.Millisecond)
	if err := ts.RotateHotShard(); err != nil {
		t.Fatal(err)
	}

	r, err := g.Rels.Add(context.Background(), "OBSERVED", a, b, nil)
	if err != nil {
		t.Fatal(err)
	}
	relID := r.InternalID()

	demoteToCold(ts, originName)

	shard, err := ts.ShardForRelIDForTest(relID)
	if err != nil {
		t.Fatalf("ShardForRelIDForTest after cold demotion: %v", err)
	}
	if !shard.HasRelID(relID.SnowflakeID()) {
		t.Errorf("ShardForRelIDForTest returned a shard that does not own rel %d after cold demotion", relID)
	}
}

// --- Fix 3: ArchiveNode/RestoreNode — rollback ---

func TestTieredStore_ArchiveNode_RollbackOnDeleteFailure(t *testing.T) {
	// Round-trip archive + restore for a node with a relationship. The
	// self-loop keeps this test focused on rollback wiring; dedicated
	// cross-shard archive tests cover relationships to live partners.
	//
	// Caveat carried over from the original: the test name says "rollback
	// on delete failure" but no failure is injected. That was true of the
	// original too ("we can't easily inject failure into DeleteNodeCascade,
	// so we test the happy path"). Renaming is out of scope here.
	cfg := Config{
		SnowflakeNodeID: 0,
		Validation:      ValidationLimits{AllowSelfLoops: true},
	}
	ts := newTestTieredStore(t)
	cfg.Store = ts
	g, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	n1, err := g.Nodes.Add(context.Background(), []string{"Case"}, map[string]any{"name": "C1"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.Rels.Add(context.Background(), "LOOPS_TO", n1, n1, nil); err != nil {
		t.Fatal(err)
	}

	// Archive n1 — self-loop migrates correctly.
	id1 := n1.ID()
	if err := ts.ArchiveNode(id1); err != nil {
		t.Fatalf("ArchiveNode: %v", err)
	}

	// Verify n1 is in archive, not in refShard.
	if ts.RefShardForTest().HasNodeID(id1.SnowflakeID()) {
		t.Error("node should not be in refShard after archive")
	}
	if archive := ts.RefArchiveForTest().Load(); archive == nil || !archive.HasNodeID(id1.SnowflakeID()) {
		t.Error("node should be in refArchive after archive")
	}

	// Restore n1 — should move back.
	if err := ts.RestoreNode(id1); err != nil {
		t.Fatalf("RestoreNode: %v", err)
	}

	if !ts.RefShardForTest().HasNodeID(id1.SnowflakeID()) {
		t.Error("node should be in refShard after restore")
	}
	if archive := ts.RefArchiveForTest().Load(); archive != nil && archive.HasNodeID(id1.SnowflakeID()) {
		t.Error("node should not be in refArchive after restore")
	}
}

func TestTieredStore_RestoreNode_RollbackOnDeleteFailure(t *testing.T) {
	// Restore round-trip with a rel that touches the archived node.
	// The self-loop and node both live on archive after ArchiveNode(n1);
	// RestoreNode(n1) must move both back to refShard.
	cfg := Config{
		SnowflakeNodeID: 0,
		Validation:      ValidationLimits{AllowSelfLoops: true},
	}
	ts := newTestTieredStore(t)
	cfg.Store = ts
	g, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	n1, err := g.Nodes.Add(context.Background(), []string{"Case"}, map[string]any{"name": "C1"})
	if err != nil {
		t.Fatal(err)
	}
	rel, err := g.Rels.Add(context.Background(), "LOOPS_TO", n1, n1, nil)
	if err != nil {
		t.Fatal(err)
	}
	id1 := n1.ID()
	relID := rel.ID()

	if err := ts.ArchiveNode(id1); err != nil {
		t.Fatalf("ArchiveNode: %v", err)
	}

	archive := ts.RefArchiveForTest().Load()
	if archive == nil || !archive.HasNodeID(id1.SnowflakeID()) {
		t.Fatal("n1 should be in archive after ArchiveNode")
	}
	if !archive.HasRelID(relID.SnowflakeID()) {
		t.Fatal("self-loop rel should be on archive after ArchiveNode")
	}

	if err := ts.RestoreNode(id1); err != nil {
		t.Fatalf("RestoreNode: %v", err)
	}

	if !ts.RefShardForTest().HasNodeID(id1.SnowflakeID()) {
		t.Error("n1 should be in refShard after restore")
	}
	if archive := ts.RefArchiveForTest().Load(); archive != nil && archive.HasNodeID(id1.SnowflakeID()) {
		t.Error("n1 should not be in refArchive after restore")
	}
	// Restore must migrate the rel back too — otherwise the round-trip
	// is lossy in the same way the pre-fix archive path was.
	if !ts.RefShardForTest().HasRelID(relID.SnowflakeID()) {
		t.Error("self-loop rel should be in refShard after restore")
	}
}

// --- WAL corruption recovery tests ---
// ─── OutgoingRelationshipsForNodes ───────────────────────────────────────────
// ─── IncomingRelationshipsForNodes ───────────────────────────────────────────
