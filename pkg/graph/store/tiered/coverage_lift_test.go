package tiered

import (
	"errors"
	"reflect"
	"sort"
	"testing"
	"time"

	registrypkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/registry"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
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

// --- RebuildCatalog hits the archive branch (lines ~240-253) ---

func TestTiered_RebuildCatalog_WithArchivePresent(t *testing.T) {
	ts, _, _ := withArchiveAndRegistry(t)
	if err := ts.RebuildCatalog(); err != nil {
		t.Fatalf("RebuildCatalog with archive: %v", err)
	}
	// Verify catalog now reflects the archive entry.
	if entry, ok := ts.CatalogForTest().GetShard("archive"); !ok || entry.Name != "archive" {
		t.Fatalf("catalog has no archive entry after RebuildCatalog: ok=%v entry=%+v", ok, entry)
	}
}

// --- CreateTemporalIndex with archive present exercises ensureTemporalIndexArchiveOpenLocked ---

func TestTiered_CreateTemporalIndex_WithArchivePresent(t *testing.T) {
	ts, caseTok, _ := withArchiveAndRegistry(t)
	if err := ts.CreateTemporalIndex(caseTok); err != nil {
		t.Fatalf("CreateTemporalIndex with archive: %v", err)
	}
	// Repeated create returns nil (already exists).
	if err := ts.CreateTemporalIndex(caseTok); err != nil {
		t.Fatalf("CreateTemporalIndex repeated with archive: %v", err)
	}
}

func TestTiered_CreateHighFrequencyIndex_WithArchivePresent(t *testing.T) {
	ts, caseTok, _ := withArchiveAndRegistry(t)
	if err := ts.CreateHighFrequencyIndex(caseTok, time.Hour); err != nil {
		t.Fatalf("CreateHighFrequencyIndex with archive: %v", err)
	}
}

func TestTiered_DropTemporalIndex_WithArchivePresent(t *testing.T) {
	ts, caseTok, _ := withArchiveAndRegistry(t)
	if err := ts.CreateTemporalIndex(caseTok); err != nil {
		t.Fatalf("CreateTemporalIndex: %v", err)
	}
	if err := ts.DropTemporalIndex(caseTok); err != nil {
		t.Fatalf("DropTemporalIndex with archive: %v", err)
	}
}

func TestTiered_DropHighFrequencyIndex_WithArchivePresent(t *testing.T) {
	ts, caseTok, _ := withArchiveAndRegistry(t)
	if err := ts.CreateHighFrequencyIndex(caseTok, time.Hour); err != nil {
		t.Fatalf("CreateHighFrequencyIndex: %v", err)
	}
	if err := ts.DropHighFrequencyIndex(caseTok); err != nil {
		t.Fatalf("DropHighFrequencyIndex with archive: %v", err)
	}
}

// --- GetRelVersion archive-after-ref-miss (lines ~41-53) ---

// After Archive+Restore of a Case node with a rel touching it, history
// for the rel exists on refArchive (from the archived phase) while the
// live row is back on refShard. GetRelVersion for the archive-era
// version must fall through to the archive probe.
func TestTiered_GetRelVersion_RefLiveArchiveMissBranch(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")

	gen := tieredNodeGen(t)
	relGen := tieredRelGen(t)

	a := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	b := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	if err := ts.PutNode(a); err != nil {
		t.Fatalf("PutNode a: %v", err)
	}
	if err := ts.PutNode(b); err != nil {
		t.Fatalf("PutNode b: %v", err)
	}
	r := types.NewRelationship(types.RelID(relGen.Generate()), 1, a.ID(), b.ID())
	if err := ts.PutRelationship(r); err != nil {
		t.Fatalf("PutRelationship: %v", err)
	}
	if err := ts.PutRelVersion(r.ID(), 0, r); err != nil {
		t.Fatalf("PutRelVersion v0 on refShard: %v", err)
	}

	// Archive a — rel moves to refArchive.
	if err := ts.ArchiveNode(a.ID()); err != nil {
		t.Fatalf("ArchiveNode a: %v", err)
	}
	v1 := r.DeepCopy()
	v1.SetVersion(1)
	if err := ts.PutRelVersion(r.ID(), 1, v1); err != nil {
		t.Fatalf("PutRelVersion v1 on refArchive: %v", err)
	}

	// Restore a — rel migrates back to refShard, but history v1 remains on refArchive.
	if err := ts.RestoreNode(a.ID()); err != nil {
		t.Fatalf("RestoreNode a: %v", err)
	}

	// GetRelVersion(0) should still work — v0 is on refShard.
	got, err := ts.GetRelVersion(r.ID(), 0)
	if err != nil {
		t.Fatalf("GetRelVersion v0 after restore: %v", err)
	}
	if got.Version() != 0 {
		t.Fatalf("v0 version = %d, want 0", got.Version())
	}

	// GetRelVersion(1) — v1 lives on refArchive. Live rel is on refShard
	// after restore, so the lookup goes: shard=refShard, !isArchive,
	// liveHere, shard==refShard → refShard miss → checkoutArchive →
	// GetRelVersion v1 from refArchive. Hits lines 41-53.
	got, err = ts.GetRelVersion(r.ID(), 1)
	if err != nil {
		t.Fatalf("GetRelVersion v1 from archive after restore: %v", err)
	}
	if got.Version() != 1 {
		t.Fatalf("v1 version = %d, want 1", got.Version())
	}

	// Negative case: a version that's nowhere should ErrVersionNotFound,
	// which exercises the same archive-miss branch returning at line 55.
	if _, err := ts.GetRelVersion(r.ID(), 99); !errors.Is(err, ErrVersionNotFound) {
		t.Fatalf("GetRelVersion v99 after restore = %v, want ErrVersionNotFound", err)
	}
}

// --- GetRelVersion isArchive=true path: rel lives on refArchive after Archive ---

// After archiving the start node, the rel lives on refArchive. Querying for
// a version exercises the isArchive=true fall-through to forEachHistoryShard
// fanout (lines 58-76 in GetRelVersion).
func TestTiered_GetRelVersion_ArchivedRel_FanoutBranch(t *testing.T) {
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

// --- Same shape for nodes (already covered by existing tests but pins parity) ---

func TestTiered_GetNodeVersion_RefLiveArchiveMissBranch(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")

	n := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), caseTok, nil)
	if err := ts.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	if err := ts.PutNodeVersion(n.ID(), 0, n); err != nil {
		t.Fatalf("PutNodeVersion v0: %v", err)
	}
	if err := ts.ArchiveNode(n.ID()); err != nil {
		t.Fatalf("ArchiveNode: %v", err)
	}
	v1 := n.DeepCopy()
	v1.SetVersion(1)
	if err := ts.PutNodeVersion(n.ID(), 1, v1); err != nil {
		t.Fatalf("PutNodeVersion v1 on archive: %v", err)
	}
	if err := ts.RestoreNode(n.ID()); err != nil {
		t.Fatalf("RestoreNode: %v", err)
	}

	if got, err := ts.GetNodeVersion(n.ID(), 0); err != nil || got.Version() != 0 {
		t.Fatalf("GetNodeVersion v0 = (%+v, %v), want v0 nil err", got, err)
	}
	if got, err := ts.GetNodeVersion(n.ID(), 1); err != nil || got.Version() != 1 {
		t.Fatalf("GetNodeVersion v1 = (%+v, %v), want v1 nil err", got, err)
	}
	if _, err := ts.GetNodeVersion(n.ID(), 99); !errors.Is(err, ErrVersionNotFound) {
		t.Fatalf("GetNodeVersion missing v99 = %v, want ErrVersionNotFound", err)
	}
}

// --- GetNodeHistory and GetRelHistory after Restore — verify merge ---

func TestTiered_GetRelHistory_AfterRestore_MergesRefAndArchive(t *testing.T) {
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
		t.Fatalf("PutRelVersion v0: %v", err)
	}
	if err := ts.ArchiveNode(a.ID()); err != nil {
		t.Fatalf("ArchiveNode a: %v", err)
	}
	v1 := r.DeepCopy()
	v1.SetVersion(1)
	if err := ts.PutRelVersion(r.ID(), 1, v1); err != nil {
		t.Fatalf("PutRelVersion v1 on archive: %v", err)
	}
	if err := ts.RestoreNode(a.ID()); err != nil {
		t.Fatalf("RestoreNode: %v", err)
	}

	history, err := ts.GetRelHistory(r.ID())
	if err != nil {
		t.Fatalf("GetRelHistory after restore: %v", err)
	}
	if versions := tieredRelHistoryVersions(history); !reflect.DeepEqual(versions, []uint32{0, 1}) {
		t.Fatalf("history merged across ref+archive = %v, want [0 1]", versions)
	}
}

// TestTiered_GetNodeHistory_RefLiveWithArchivePresent covers the
// archive != nil && archive != skip branch of nodeHistoryWithArchive: a node
// stays on refShard while a DIFFERENT node has been archived (so refArchive
// is open). The function must read both shards' history for the queried
// node and merge — even though only refShard contains its history.
func TestTiered_GetNodeHistory_RefLiveWithArchivePresent(t *testing.T) {
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

func (noopHashChainVerifier) VerifyNodeChain(types.NodeID) (bool, error)   { return true, nil }
func (noopHashChainVerifier) VerifyRelChain(types.RelID) (bool, error)     { return true, nil }

func TestTiered_VerifyShard_ArchiveShard(t *testing.T) {
	ts, _, _ := withArchiveAndRegistry(t)
	result, err := ts.VerifyShard(noopHashChainVerifier{}, "archive")
	if err != nil {
		t.Fatalf("VerifyShard archive: %v", err)
	}
	if result.ShardName != "archive" {
		t.Fatalf("VerifyShard.ShardName = %q, want \"archive\"", result.ShardName)
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
}

// --- TruncateNodeHistory error path: invalid keep ---

func TestTiered_TruncateNodeHistory_NegativeKeep(t *testing.T) {
	ts := newTestTieredStore(t)
	_ = installDefaultTestLabelRegistryDelegate(t, ts)
	id := types.NodeID(tieredNodeGen(t).Generate())
	if err := ts.TruncateNodeHistory(id, -1); err == nil {
		t.Fatalf("TruncateNodeHistory keep=-1 = nil, want error")
	}
}

func TestTiered_TruncateRelHistory_NegativeKeep(t *testing.T) {
	ts := newTestTieredStore(t)
	_ = installDefaultTestLabelRegistryDelegate(t, ts)
	id := types.RelID(tieredRelGen(t).Generate())
	if err := ts.TruncateRelHistory(id, -1); err == nil {
		t.Fatalf("TruncateRelHistory keep=-1 = nil, want error")
	}
}

func installDefaultTestLabelRegistryDelegate(t *testing.T, ts *Store) uint16 {
	t.Helper()
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	tok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	return tok
}

// --- NodeHistoryVersionsFrom paging across many event shards ---

// After rotating the hot shard a few times, a node deleted from current state
// will need history-shard fanout to find all its versions. This exercises the
// "loop continues until all shards probed" path in forEachHistoryShard.
func TestTiered_NodeHistory_AfterRotation_HistoryFanout(t *testing.T) {
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
	if len(history) == 0 {
		t.Fatalf("history-only after rotate+delete returned empty; want v0")
	}
	versions := tieredNodeHistoryVersions(history)
	sort.Slice(versions, func(i, j int) bool { return versions[i] < versions[j] })
	if versions[0] != 0 {
		t.Fatalf("first version after rotate+delete = %d, want 0", versions[0])
	}
}

func TestTiered_RelHistory_AfterRotation_HistoryFanout(t *testing.T) {
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
	if len(history) == 0 {
		t.Fatalf("history-only after rotate+delete returned empty; want v0")
	}
}
