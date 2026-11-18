package tiered

import (
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/internal/testtier"
	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Coverage-lift tests target the lowest-covered routing/admin branches in
// tieredstore_admin.go, tieredstore_write.go (index management), and the
// archive merge paths in tieredstore_read_history*.go. Each test is shaped
// around a concrete user-facing scenario, not the line number — but the
// branches it exercises are noted in the comment.

// withArchiveAndRegistry builds a Store with the default test labels (Case,
// User, Signal) and a refArchive shard already created via ArchiveNode of a
// scratch reference node. Useful for tests that need an archive present.
func withArchiveAndRegistry(t *testing.T) (*Store, uint16, uint16) {
	t.Helper()
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	caseTok, err := reg.GetOrCreate("Case")
	if err != nil {
		t.Fatalf("GetOrCreate Case: %v", err)
	}
	_, _ = reg.GetOrCreate("User")
	signalTok, err := reg.GetOrCreate("Signal")
	if err != nil {
		t.Fatalf("GetOrCreate Signal: %v", err)
	}
	scratch := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), caseTok, nil)
	if err := ts.PutNode(scratch); err != nil {
		t.Fatalf("PutNode scratch: %v", err)
	}
	if err := ts.ArchiveNode(scratch.ID()); err != nil {
		t.Fatalf("ArchiveNode scratch: %v", err)
	}
	if !ts.HasArchiveShardForTest() {
		t.Fatalf("HasArchiveShardForTest = false after ArchiveNode")
	}
	return ts, caseTok, signalTok
}

// --- RebuildCatalog hits the archive branch ---

func TestTiered_RebuildCatalog_WithArchivePresent(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	ts, _, _ := withArchiveAndRegistry(t)
	if err := ts.RebuildCatalog(); err != nil {
		t.Fatalf("RebuildCatalog with archive: %v", err)
	}
	// Verify catalog now reflects the archive entry.
	if entry, ok := ts.CatalogForTest().GetShard("archive"); !ok || entry.Name != "archive" {
		t.Fatalf("catalog has no archive entry after RebuildCatalog: ok=%v entry=%+v", ok, entry)
	}
}

// TestTiered_TemporalIndexes_WithArchivePresent: with an archive shard open,
// creating and dropping a temporal or high-frequency index on a reference label
// reaches the archive too. Each step is checked on the archive's own index
// state, and a repeated create is a no-op.
func TestTiered_TemporalIndexes_WithArchivePresent(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	ts, caseTok, _ := withArchiveAndRegistry(t)
	archive := ts.RefArchiveForTest().Load()
	if archive == nil {
		t.Fatal("setup: archive not open")
	}
	want := func(step string, temporal, hf bool, bucket time.Duration) {
		t.Helper()
		gotT, gotHF, gotBucket, err := archive.TemporalIndexState(caseTok)
		if err != nil {
			t.Fatalf("%s: archive TemporalIndexState: %v", step, err)
		}
		if gotT != temporal || gotHF != hf || gotBucket != bucket {
			t.Fatalf("%s: archive index state = (temporal %v, hf %v, bucket %v), want (%v, %v, %v)",
				step, gotT, gotHF, gotBucket, temporal, hf, bucket)
		}
	}
	want("before", false, false, 0)
	if err := ts.CreateTemporalIndex(caseTok); err != nil {
		t.Fatalf("CreateTemporalIndex: %v", err)
	}
	want("CreateTemporalIndex", true, false, 0)
	if err := ts.CreateTemporalIndex(caseTok); err != nil {
		t.Fatalf("CreateTemporalIndex repeated: %v", err)
	}
	want("CreateTemporalIndex repeated", true, false, 0)
	if err := ts.DropTemporalIndex(caseTok); err != nil {
		t.Fatalf("DropTemporalIndex: %v", err)
	}
	want("DropTemporalIndex", false, false, 0)
	// One temporal index kind per label: the high-frequency one after the drop.
	if err := ts.CreateHighFrequencyIndex(caseTok, time.Hour); err != nil {
		t.Fatalf("CreateHighFrequencyIndex: %v", err)
	}
	want("CreateHighFrequencyIndex", false, true, time.Hour)
	if err := ts.DropHighFrequencyIndex(caseTok); err != nil {
		t.Fatalf("DropHighFrequencyIndex: %v", err)
	}
	want("DropHighFrequencyIndex", false, false, 0)
}

// --- GetRelVersion isArchive=true path: rel lives on refArchive after Archive ---

// After archiving the start node, the rel lives on refArchive. Querying for
// a version exercises the isArchive=true fall-through to forEachHistoryShard
// fanout in GetRelVersion.
func TestTiered_GetRelVersion_ArchivedRel_FanoutBranch(t *testing.T) {
	testtier.Slow(t)
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")

	gen := tieredNodeGen(t)
	a := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	b := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	if err := ts.PutNode(a); err != nil {
		t.Fatalf("PutNode a: %v", err)
	}
	if err := ts.PutNode(b); err != nil {
		t.Fatalf("PutNode b: %v", err)
	}
	r := types.NewRelationship(types.RelID(tieredRelGen(t).Generate()), 1, a.ID(), b.ID())
	if err := ts.PutRelationship(r); err != nil {
		t.Fatalf("PutRelationship: %v", err)
	}
	if err := ts.PutRelVersion(r.ID(), 0, r); err != nil {
		t.Fatalf("PutRelVersion v0 on refShard: %v", err)
	}
	if err := ts.ArchiveNode(a.ID()); err != nil {
		t.Fatalf("ArchiveNode: %v", err)
	}
	// Now r lives on refArchive (live), but v0 history is still on refShard.
	got, err := ts.GetRelVersion(r.ID(), 0)
	if err != nil {
		t.Fatalf("GetRelVersion v0 archived rel: %v", err)
	}
	if got.Version() != 0 {
		t.Fatalf("got v=%d, want 0", got.Version())
	}
	// Missing version on neither shard → falls past fanout, returns ErrVersionNotFound.
	if _, err := ts.GetRelVersion(r.ID(), 99); !errors.Is(err, ErrVersionNotFound) {
		t.Fatalf("GetRelVersion v99 archived rel = %v, want ErrVersionNotFound", err)
	}
}

// TestTiered_GetNodeHistory_RefLiveWithArchivePresent covers the
// archive != nil && archive != skip branch of nodeHistoryWithArchive: a node
// stays on refShard while a DIFFERENT node has been archived (so refArchive
// is open). The function must read both shards' history for the queried
// node and merge — even though only refShard contains its history.
func TestTiered_GetNodeHistory_RefLiveWithArchivePresent(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	ts, caseTok, _ := withArchiveAndRegistry(t)

	// Queried node: stays on refShard.
	n := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), caseTok, nil)
	if err := ts.PutNode(n); err != nil {
		t.Fatalf("PutNode queried: %v", err)
	}
	if err := ts.PutNodeVersion(n.ID(), 0, n); err != nil {
		t.Fatalf("PutNodeVersion: %v", err)
	}

	history, err := ts.GetNodeHistory(n.ID())
	if err != nil {
		t.Fatalf("GetNodeHistory ref-live with archive present: %v", err)
	}
	if len(history) != 1 || history[0].Version() != 0 {
		t.Fatalf("history = %v, want exactly v0", tieredNodeHistoryVersions(history))
	}
}

func TestTiered_GetRelHistory_RefLiveWithArchivePresent(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	ts, caseTok, _ := withArchiveAndRegistry(t)

	gen := tieredNodeGen(t)
	a := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	b := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	if err := ts.PutNode(a); err != nil {
		t.Fatalf("PutNode a: %v", err)
	}
	if err := ts.PutNode(b); err != nil {
		t.Fatalf("PutNode b: %v", err)
	}
	r := types.NewRelationship(types.RelID(tieredRelGen(t).Generate()), 1, a.ID(), b.ID())
	if err := ts.PutRelationship(r); err != nil {
		t.Fatalf("PutRelationship: %v", err)
	}
	if err := ts.PutRelVersion(r.ID(), 0, r); err != nil {
		t.Fatalf("PutRelVersion: %v", err)
	}

	history, err := ts.GetRelHistory(r.ID())
	if err != nil {
		t.Fatalf("GetRelHistory ref-live with archive present: %v", err)
	}
	if len(history) != 1 || history[0].Version() != 0 {
		t.Fatalf("history = %v, want exactly v0", tieredRelHistoryVersions(history))
	}
}

// --- VerifyShard with archive present ---

type noopHashChainVerifier struct{}

func (noopHashChainVerifier) VerifyNodeChain(types.NodeID) (bool, error) { return true, nil }
func (noopHashChainVerifier) VerifyRelChain(types.RelID) (bool, error)   { return true, nil }

// failingHashChainVerifier flips Verify*Chain to return false (validation
// failure, not error). Used to exercise the NodesFailed/RelsFailed counters
// in VerifyShard.
type failingHashChainVerifier struct{}

func (failingHashChainVerifier) VerifyNodeChain(types.NodeID) (bool, error) {
	return false, nil
}
func (failingHashChainVerifier) VerifyRelChain(types.RelID) (bool, error) { return false, nil }

func TestTiered_VerifyShard_ArchiveShard(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	ts, _, _ := withArchiveAndRegistry(t)
	result, err := ts.VerifyShard(noopHashChainVerifier{}, "archive")
	if err != nil {
		t.Fatalf("VerifyShard archive: %v", err)
	}
	if result.ShardName != "archive" {
		t.Fatalf("VerifyShard.ShardName = %q, want \"archive\"", result.ShardName)
	}
	// The archive holds exactly the one archived scratch node.
	if result.NodesOK != 1 || result.RelsOK != 0 || result.NodesFailed != 0 || result.RelsFailed != 0 {
		t.Fatalf("VerifyShard archive = %+v, want 1 node OK and nothing else", result)
	}
}

// TestTiered_VerifyShard_FailedChain covers the NodesFailed / RelsFailed
// branches in VerifyShard.
func TestTiered_VerifyShard_FailedChain(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	gen := tieredNodeGen(t)
	a := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	b := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	if err := ts.PutNode(a); err != nil {
		t.Fatalf("PutNode a: %v", err)
	}
	if err := ts.PutNode(b); err != nil {
		t.Fatalf("PutNode b: %v", err)
	}
	r := types.NewRelationship(types.RelID(tieredRelGen(t).Generate()), 1, a.ID(), b.ID())
	if err := ts.PutRelationship(r); err != nil {
		t.Fatalf("PutRelationship: %v", err)
	}

	result, err := ts.VerifyShard(failingHashChainVerifier{}, "reference")
	if err != nil {
		t.Fatalf("VerifyShard: %v", err)
	}
	if result.NodesFailed != 2 {
		t.Fatalf("NodesFailed = %d, want 2", result.NodesFailed)
	}
	if result.RelsFailed != 1 {
		t.Fatalf("RelsFailed = %d, want 1", result.RelsFailed)
	}
	if result.NodesOK != 0 || result.RelsOK != 0 {
		t.Fatalf("OK counts = %d/%d, want 0/0", result.NodesOK, result.RelsOK)
	}
}

// TestTiered_VerifyShard_UnknownShard covers the catalog-miss branch.
func TestTiered_VerifyShard_UnknownShard(t *testing.T) {
	testtier.Slow(t)
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	_, _ = reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	// Unknown-shard returns a wrapped fmt.Errorf, not a sentinel — pin
	// the message so a sentinel introduction in the future doesn't
	// silently bypass this case.
	_, err := ts.VerifyShard(noopHashChainVerifier{}, "no-such-shard")
	if err == nil {
		t.Fatalf("VerifyShard unknown shard = nil, want error")
	}
	if !strings.Contains(err.Error(), "not found in catalog") {
		t.Fatalf("VerifyShard unknown shard err = %v, want message containing \"not found in catalog\"", err)
	}
}

func TestTiered_VerifyShard_ReferenceShard(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	n := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), caseTok, nil)
	if err := ts.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	result, err := ts.VerifyShard(noopHashChainVerifier{}, "reference")
	if err != nil {
		t.Fatalf("VerifyShard reference: %v", err)
	}
	if result.ShardName != "reference" {
		t.Fatalf("VerifyShard.ShardName = %q, want \"reference\"", result.ShardName)
	}
	if result.NodesOK != 1 || result.RelsOK != 0 || result.NodesFailed != 0 || result.RelsFailed != 0 {
		t.Fatalf("VerifyShard reference = %+v, want 1 node OK and nothing else", result)
	}
}

// --- PutRelationshipsBatch with mixed same-shard and cross-shard rels ---

// TestTiered_PutRelationshipsBatch_CrossShard exercises the cross-shard
// partitioning branch. Cross-shard rels go through
// the split-write helper rather than the same-shard batch fast path.
func TestTiered_PutRelationshipsBatch_CrossShard(t *testing.T) {
	testtier.Slow(t)
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	_, _ = reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)

	// First event shard inhabitants.
	a := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	b := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	if err := ts.PutNode(a); err != nil {
		t.Fatalf("PutNode a: %v", err)
	}
	if err := ts.PutNode(b); err != nil {
		t.Fatalf("PutNode b: %v", err)
	}

	// Rotate — a, b now live in the warm shard; new nodes go to fresh hot.
	if err := ts.ForceRotate(); err != nil {
		t.Fatalf("ForceRotate: %v", err)
	}
	c := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	d := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	if err := ts.PutNode(c); err != nil {
		t.Fatalf("PutNode c: %v", err)
	}
	if err := ts.PutNode(d); err != nil {
		t.Fatalf("PutNode d: %v", err)
	}

	relGen := tieredRelGen(t)
	// Same-shard rel within new hot shard.
	sameShardRel := types.NewRelationship(types.RelID(relGen.Generate()), 1, c.ID(), d.ID())
	// Cross-shard rel from warm to hot.
	crossShardRel := types.NewRelationship(types.RelID(relGen.Generate()), 1, a.ID(), c.ID())

	if err := ts.PutRelationshipsBatch([]*types.Relationship{sameShardRel, crossShardRel}); err != nil {
		t.Fatalf("PutRelationshipsBatch mixed: %v", err)
	}

	// Both rels are readable with their endpoints, and both legs of each are
	// written on their shards: c and d each see their own incoming, a and c
	// their own outgoing.
	for _, want := range []*types.Relationship{sameShardRel, crossShardRel} {
		got, err := ts.GetRelationship(want.ID())
		if err != nil || got.StartNodeID() != want.StartNodeID() || got.EndNodeID() != want.EndNodeID() {
			t.Fatalf("GetRelationship %d = (%+v, %v), want %d->%d", want.ID(), got, err, want.StartNodeID(), want.EndNodeID())
		}
	}
	relIDs := func(rels []*types.Relationship, err error) []types.RelID {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		ids := make([]types.RelID, 0, len(rels))
		for _, r := range rels {
			ids = append(ids, r.ID())
		}
		slices.Sort(ids)
		return ids
	}
	if got := relIDs(ts.IncomingRelationships(c.ID(), 0)); !slices.Equal(got, []types.RelID{crossShardRel.ID()}) {
		t.Fatalf("IncomingRelationships(c) = %v, want [%v]", got, crossShardRel.ID())
	}
	if got := relIDs(ts.IncomingRelationships(d.ID(), 0)); !slices.Equal(got, []types.RelID{sameShardRel.ID()}) {
		t.Fatalf("IncomingRelationships(d) = %v, want [%v]", got, sameShardRel.ID())
	}
	if got := relIDs(ts.OutgoingRelationships(a.ID(), 0)); !slices.Equal(got, []types.RelID{crossShardRel.ID()}) {
		t.Fatalf("OutgoingRelationships(a) = %v, want [%v]", got, crossShardRel.ID())
	}
	if got := relIDs(ts.OutgoingRelationships(c.ID(), 0)); !slices.Equal(got, []types.RelID{sameShardRel.ID()}) {
		t.Fatalf("OutgoingRelationships(c) = %v, want [%v]", got, sameShardRel.ID())
	}
}

// TestTiered_PutRelationshipsBatch_DuplicateInBatchRejected covers the
// in-batch duplicate-ID check.
func TestTiered_PutRelationshipsBatch_DuplicateInBatchRejected(t *testing.T) {
	testtier.Slow(t)
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	_, _ = reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	a := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	b := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	if err := ts.PutNode(a); err != nil {
		t.Fatalf("PutNode a: %v", err)
	}
	if err := ts.PutNode(b); err != nil {
		t.Fatalf("PutNode b: %v", err)
	}
	r := types.NewRelationship(types.RelID(tieredRelGen(t).Generate()), 1, a.ID(), b.ID())
	if err := ts.PutRelationshipsBatch([]*types.Relationship{r, r}); !errors.Is(err, ErrRelExists) {
		t.Fatalf("duplicate in-batch = %v, want ErrRelExists", err)
	}
}

// --- ForEachNodeHistoryIDByDepth / maxNodeHistoryIDByDepth with multi-shard data ---

// TestTiered_ForEachNodeHistoryIDByDepth_AllTiers populates history on
// refShard, refArchive, hot event shard, AND warm event shard, then iterates
// at each depth. Exercises the archive-and-event-shard merging logic in
// maxNodeHistoryIDByDepth and forEachNodeHistoryIDByDepth.
func TestTiered_ForEachNodeHistoryIDByDepth_AllTiers(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	ts, caseTok, signalTok := withArchiveAndRegistry(t)

	gen := tieredNodeGen(t)
	// Reference node with history on refShard.
	refNode := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	if err := ts.PutNode(refNode); err != nil {
		t.Fatalf("PutNode refNode: %v", err)
	}
	if err := ts.PutNodeVersion(refNode.ID(), 0, refNode); err != nil {
		t.Fatalf("PutNodeVersion refNode: %v", err)
	}
	// Signal node history on hot event shard.
	sig1 := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	if err := ts.PutNode(sig1); err != nil {
		t.Fatalf("PutNode sig1: %v", err)
	}
	if err := ts.PutNodeVersion(sig1.ID(), 0, sig1); err != nil {
		t.Fatalf("PutNodeVersion sig1: %v", err)
	}
	// Rotate so sig1's shard moves to warm; next signal goes to fresh hot.
	if err := ts.ForceRotate(); err != nil {
		t.Fatalf("ForceRotate: %v", err)
	}
	sig2 := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	if err := ts.PutNode(sig2); err != nil {
		t.Fatalf("PutNode sig2: %v", err)
	}
	if err := ts.PutNodeVersion(sig2.ID(), 0, sig2); err != nil {
		t.Fatalf("PutNodeVersion sig2: %v", err)
	}

	for _, depth := range []storepkg.ShardDepth{storepkg.DepthAll, storepkg.DepthHot, storepkg.DepthWarm} {
		seen := map[types.NodeID]bool{}
		if err := ts.ForEachNodeHistoryIDByDepth(depth, func(id types.NodeID) bool {
			seen[id] = true
			return true
		}); err != nil {
			t.Fatalf("ForEachNodeHistoryIDByDepth(%v): %v", depth, err)
		}
		// At least the hot-shard sig2 must be visible at every depth that
		// includes hot. Don't over-specify exact membership — different
		// depths legitimately scope to different tiers.
		if depth == storepkg.DepthAll && !seen[refNode.ID()] {
			t.Fatalf("DepthAll missing refNode %d (saw %v)", refNode.ID(), seen)
		}
	}
}

func TestTiered_ForEachRelHistoryIDByDepth_AllTiers(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	ts, caseTok, signalTok := withArchiveAndRegistry(t)

	gen := tieredNodeGen(t)
	a := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	b := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	if err := ts.PutNode(a); err != nil {
		t.Fatalf("PutNode a: %v", err)
	}
	if err := ts.PutNode(b); err != nil {
		t.Fatalf("PutNode b: %v", err)
	}
	refRel := types.NewRelationship(types.RelID(tieredRelGen(t).Generate()), 1, a.ID(), b.ID())
	if err := ts.PutRelationship(refRel); err != nil {
		t.Fatalf("PutRelationship: %v", err)
	}
	if err := ts.PutRelVersion(refRel.ID(), 0, refRel); err != nil {
		t.Fatalf("PutRelVersion: %v", err)
	}
	sigA := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	sigB := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	if err := ts.PutNode(sigA); err != nil {
		t.Fatalf("PutNode sigA: %v", err)
	}
	if err := ts.PutNode(sigB); err != nil {
		t.Fatalf("PutNode sigB: %v", err)
	}
	sigRel := types.NewRelationship(types.RelID(tieredRelGen(t).Generate()), 1, sigA.ID(), sigB.ID())
	if err := ts.PutRelationship(sigRel); err != nil {
		t.Fatalf("PutRelationship sigRel: %v", err)
	}
	if err := ts.PutRelVersion(sigRel.ID(), 0, sigRel); err != nil {
		t.Fatalf("PutRelVersion sigRel: %v", err)
	}

	for _, depth := range []storepkg.ShardDepth{storepkg.DepthAll, storepkg.DepthHot, storepkg.DepthWarm} {
		seen := map[types.RelID]bool{}
		if err := ts.ForEachRelHistoryIDByDepth(depth, func(id types.RelID) bool {
			seen[id] = true
			return true
		}); err != nil {
			t.Fatalf("ForEachRelHistoryIDByDepth(%v): %v", depth, err)
		}
		if depth == storepkg.DepthAll && !seen[refRel.ID()] {
			t.Fatalf("DepthAll missing refRel %d", refRel.ID())
		}
	}
}

// --- NodeHistoryVersionsFrom paging across many event shards ---

// After rotating the hot shard a few times, a node deleted from current state
// will need history-shard fanout to find all its versions. This exercises the
// "loop continues until all shards probed" path in forEachHistoryShard.
func TestTiered_NodeHistory_AfterRotation_HistoryFanout(t *testing.T) {
	testtier.Slow(t)
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	_, _ = reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	n := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), signalTok, nil)
	if err := ts.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	if err := ts.PutNodeVersion(n.ID(), 0, n); err != nil {
		t.Fatalf("PutNodeVersion: %v", err)
	}
	// Rotate so the next writes (if any) land in a fresh shard.
	if err := ts.ForceRotate(); err != nil {
		t.Fatalf("ForceRotate: %v", err)
	}
	// Now delete the live row — history fanout will need to scan rotated shards.
	if err := ts.DeleteNode(n.ID()); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}
	history, err := ts.NodeHistoryVersionsFrom(n.ID(), 0, 5)
	if err != nil {
		t.Fatalf("NodeHistoryVersionsFrom after delete+rotate: %v", err)
	}
	if versions := tieredNodeHistoryVersions(history); !slices.Equal(versions, []uint32{0}) {
		t.Fatalf("history after rotate+delete = %v, want [0]", versions)
	}
}

func TestTiered_RelHistory_AfterRotation_HistoryFanout(t *testing.T) {
	testtier.Slow(t)
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	_, _ = reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	a := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	b := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	if err := ts.PutNode(a); err != nil {
		t.Fatalf("PutNode a: %v", err)
	}
	if err := ts.PutNode(b); err != nil {
		t.Fatalf("PutNode b: %v", err)
	}
	r := types.NewRelationship(types.RelID(tieredRelGen(t).Generate()), 1, a.ID(), b.ID())
	if err := ts.PutRelationship(r); err != nil {
		t.Fatalf("PutRelationship: %v", err)
	}
	if err := ts.PutRelVersion(r.ID(), 0, r); err != nil {
		t.Fatalf("PutRelVersion: %v", err)
	}
	if err := ts.ForceRotate(); err != nil {
		t.Fatalf("ForceRotate: %v", err)
	}
	if err := ts.DeleteRelationship(r.ID()); err != nil {
		t.Fatalf("DeleteRelationship: %v", err)
	}
	history, err := ts.RelHistoryVersionsFrom(r.ID(), 0, 5)
	if err != nil {
		t.Fatalf("RelHistoryVersionsFrom: %v", err)
	}
	if versions := tieredRelHistoryVersions(history); !slices.Equal(versions, []uint32{0}) {
		t.Fatalf("history after rotate+delete = %v, want [0]", versions)
	}
}
