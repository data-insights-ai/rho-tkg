package raftlog

import (
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

// Fixed diagnostic ownership, never a source/claim factory or process proof.
// Native operations below supply every actual cut/import/plan and entry.
type stableStateFS struct {
	vfs.FS
	selection             StableStateSelection
	deny, wrong, overflow bool
	spent, reserves       uint64
	events                [64]StableStateObservation
	count                 int
}

var _ StableStateObserver = (*stableStateFS)(nil)

func (f *stableStateFS) StableStateSelection(_ StableStateKind, identity StableStateIdentity) (StableStateSelection, bool) {
	f.selection = StableStateSelection{Cookie: [16]byte{1}, Sequence: f.selection.Sequence + 1, Identity: identity}
	if f.wrong {
		f.selection.Identity.ManifestID[0] ^= 1
	}
	return f.selection, true
}
func (f *stableStateFS) ReserveStableStateObservation(selection StableStateSelection, work StableStorageWork) bool {
	f.reserves++
	if f.deny || selection != f.selection || work.Entries != 3 || work.OutputBytes != 2048 || work.Bytes > (1<<32)-f.spent {
		return false
	}
	f.spent += work.Bytes
	return true
}
func (f *stableStateFS) ObserveStableState(event StableStateObservation) {
	if f.count == len(f.events) {
		f.overflow = true
		return
	}
	f.events[f.count], f.count = event, f.count+1
	// Every field is by value: mutation cannot replace a native manifest,
	// prospective metadata or the later observation of the same batch.
	event.Selection.Identity.ManifestID[0] ^= 1
	event.MetadataSHA256[0] ^= 1
}

func stableStatePair(t *testing.T, f *stableStateFS, kind StableStateKind) StableStateObservation {
	t.Helper()
	var pair [2]StableStateObservation
	found := false
	for i := 1; i < f.count; i++ {
		if f.events[i].Kind == kind && f.events[i].Stage == 2 {
			pair, found = [2]StableStateObservation{f.events[i-1], f.events[i]}, true
		}
	}
	first, last := pair[0], pair[1]
	if !found || f.overflow || first.Kind != kind || first.Stage != 1 || first.Synced || last.Stage != 2 || !last.Synced || first.Selection.Cookie == ([16]byte{}) || first.Selection.Sequence == 0 || first.BeforeMetadataBytes == 0 || first.MetadataBytes == 0 || first.BeforeMetadataSHA256 == ([32]byte{}) || first.MetadataSHA256 == ([32]byte{}) || first.BatchBytes < 12 || first.BatchPayloadSHA256 == ([32]byte{}) || first.Work.Entries != 3 || first.Work.OutputBytes != 2048 || first.Work.Bytes < first.BatchBytes+first.BeforeMetadataBytes+first.MetadataBytes {
		t.Fatal("missing prepaid actual native-state Sync pair", kind, first, last)
	}
	last.Stage, last.Synced = 1, false
	if first != last {
		t.Fatal("same operation substituted batch/metadata/native identity")
	}
	return first
}

func TestStableStateObservationsBindActualImportActivationPublicationAndOldRoots(t *testing.T) {
	fs := &stableStateFS{FS: vfs.NewMem()}
	s := receiverStore(t, fs, 1)
	initial := stableStatePair(t, fs, StableStateInitialize)
	if initial.Selection.Identity.Applied != 1 || initial.Selection.Identity.ManifestID != ([32]byte{}) {
		t.Fatal("initial atomic genesis fabricated transfer authority")
	}
	old := viewApplication(t, s, 1)
	if err := s.SaveCheckpoint(1, s.meta.Conf, []byte("initial")); err != nil {
		t.Fatal(err)
	}
	checkpoint := stableStatePair(t, fs, StableStateCheckpoint)
	if checkpoint.Selection.Identity.SourceIndex != 1 || checkpoint.Selection.Identity.ImageSHA256 != sha256.Sum256([]byte("initial")) {
		t.Fatal("checkpoint identity substituted current source")
	}
	i, prepared, snapshot := preparedFixture(t, s)
	manifestID := i.id
	begin := stableStatePair(t, fs, StableStateImportBegin)
	appendPage := stableStatePair(t, fs, StableStateImportAppend)
	seal := stableStatePair(t, fs, StableStateImportSeal)
	if begin.Selection.Identity.ManifestID != manifestID || begin.Selection.Identity.TargetGeneration != i.generation || begin.Selection.Identity.SourceIndex != 2 || begin.Selection.Identity.Sequence != 0 || begin.Selection.Identity.Final || begin.Selection.Identity.Verified || appendPage.Selection.Identity.ManifestID != manifestID || appendPage.Selection.Identity.Sequence != i.sequence || !appendPage.Selection.Identity.Final || appendPage.Selection.Identity.Verified || seal.Selection.Identity.ManifestID != manifestID || seal.Selection.Identity.Sequence != i.sequence || !seal.Selection.Identity.Final || !seal.Selection.Identity.Verified || seal.Selection.Identity.ProgressSHA256 != i.state.digest {
		t.Fatal("native staging/page/seal coordinates were conflated")
	}
	claim, err := prepared.Claim(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	priorActivation, err := encodeMeta(s.meta)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.PersistApplicationReady(raft.Ready{Snapshot: snapshot, HardState: &pb.HardState{Term: new(uint64(5)), Commit: new(uint64(2))}}, claim); err != nil {
		t.Fatal(err)
	}
	activation := stableStatePair(t, fs, StableStateActivation)
	cut, err := s.PublishedApplicationCut()
	if err != nil || activation.Selection.Identity.ManifestID != manifestID || activation.Selection.Identity.CurrentPublicationID != cut.ID || activation.Selection.Identity.SourceCutID != cut.ID || activation.Selection.Identity.ActiveGeneration != 2 || activation.Selection.Identity.TargetGeneration != 2 || activation.Selection.Identity.Applied != 2 || activation.Selection.Identity.ImageSHA256 != sha256.Sum256([]byte("incoming")) || activation.Selection.Identity.SourceImageSHA256 != sha256.Sum256([]byte("incoming")) {
		t.Fatal("activation did not bind the actual complete claim/cut", err)
	}
	encoded, err := encodeMeta(s.meta)
	if err != nil || activation.BeforeMetadataSHA256 != sha256.Sum256(priorActivation) || activation.MetadataSHA256 != sha256.Sum256(encoded) || activation.BeforeMetadataSHA256 == activation.MetadataSHA256 {
		t.Fatal("activation metadata commitment differs from actual stored envelope", err)
	}
	getApplication(t, old, "a", "", false, false)
	if root, err := old.Root(); err != nil || root.Index != 1 || string(root.Image) != "initial" {
		t.Fatal("activation replaced held old state", root, err)
	}
	if err := claim.Close(); err != nil {
		t.Fatal(err)
	}
	if err := prepared.Close(); err != nil {
		t.Fatal(err)
	}
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	cleared := stableStatePair(t, fs, StableStateGenerationClear)
	if cleared.Selection.Identity.TargetBank != 0 || cleared.Selection.Identity.TargetGeneration != 1 || cleared.Selection.Identity.ManifestID != ([32]byte{}) {
		t.Fatal("retired-bank cleanup borrowed a nonexistent manifest")
	}
	held := viewApplication(t, s, 2)
	generationApplyTerm(t, s, 6, "later")
	if err := s.PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	publication := stableStatePair(t, fs, StableStatePublication)
	latest, err := s.PublishedApplicationCut()
	if err != nil || publication.Selection.Identity.CurrentPublicationID != latest.ID || publication.Selection.Identity.ManifestID != ([32]byte{}) || publication.Selection.Identity.SourceIndex != latest.Index || latest.ID == cut.ID {
		t.Fatal("publication fabricated an activation or reused an old cut", err)
	}
	if root, err := held.Root(); err != nil || root.Index != 2 || string(root.Image) != "incoming" {
		t.Fatal("publication advanced held native source", root, err)
	}
	// Identity mismatch and grant denial are diagnostic refusals, never native
	// write errors or synthetic successful transition receipts.
	count, reserves := fs.count, fs.reserves
	fs.wrong = true
	if err := s.SaveCheckpoint(3, s.meta.Conf, []byte("later")); err != nil || fs.count != count || fs.reserves != reserves {
		t.Fatal("foreign selection changed native checkpoint", err)
	}
	fs.wrong, fs.deny = false, true
	if err := s.SaveCheckpoint(3, s.meta.Conf, []byte("later")); err != nil || fs.count != count || fs.reserves != reserves+1 {
		t.Fatal("denied grant changed native persistence", err)
	}
}

func TestStableStateRetentionPagesBindInstalledPlanAndKeepProtectedOldData(t *testing.T) {
	fs := &stableStateFS{FS: vfs.NewMem()}
	p := DefaultApplicationPolicy(1)
	controls, recovery, retention := ApplicationControlConfig{Version: 1}, ApplicationRecoveryConfig{Version: 2}, ApplicationRetentionConfig{Version: 1}
	contract, err := ApplicationContractForPolicyWithRetention(p, controls, recovery, retention)
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{Dir: "db", FS: fs, Create: true, Application: p, Controls: controls, Recovery: recovery, Retention: retention, SemanticContractID: ApplicationSemanticContractID{9}, Replication: ApplicationReplicationConfig{Voters: [3]uint64{1, 2, 3}}, Transfer: ApplicationTransferConfig{Identity: ApplicationIdentity{Graph: [16]byte{1}, Partition: 1, Group: [16]byte{2}}, Contract: contract, Limits: DefaultApplicationTransferLimits()}, Generations: ApplicationGenerationLimits{MaxBytes: 2 << 30, MaxRecords: 2_000_000}, PublishedCuts: ApplicationPublishedCutLimits{MaxTransferChunks: 4096}}
	s, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Initialize([]uint64{1, 2, 3}, []byte("initial")); err != nil {
		t.Fatal(err)
	}
	generationApply(t, s, "old", KV{Key: []byte("key"), Value: []byte("old")})
	generationApply(t, s, "new", KV{Key: []byte("key"), Value: []byte("new")})
	view := viewApplication(t, s, 3)
	cut, err := s.PublishedApplicationCut()
	if err != nil {
		t.Fatal(err)
	}
	plan := ApplicationRetentionState{ID: [32]byte{7}, SourceIndex: 3, InstalledIndex: 4, SourceHash: sha256.Sum256([]byte("new")), RetiredCutID: cut.ID, RootFloor: 2, DataFloor: 3, FeedFloor: 3, OutcomeFloor: 3, RecoveryFloor: 3, Roots: []ApplicationRetentionRoot{{Index: 2, ImageHash: sha256.Sum256([]byte("old")), Data: true}}}
	command := []byte("actual-floor-command")
	persist(t, s, 4, 4, ent(4, 4, string(command)))
	admission, _, err := view.PrepareApplicationRetention(t.Context(), plan, command, ReadBudget{Rows: 8, Bytes: p.MaxPageBytes})
	if err != nil {
		t.Fatal(err)
	}
	if err := view.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.InstallApplication(4, ApplicationBatch{RetentionAdmission: admission, BaseGeneration: s.ApplicationGeneration(), BaseIndex: 3, BaseImageHash: plan.SourceHash, Image: []byte("new")}); err != nil {
		t.Fatal(err)
	}
	complete := false
	for page := 0; page < 8; page++ {
		beforePage, err := encodeMeta(s.meta)
		if err != nil {
			t.Fatal(err)
		}
		progress, _, err := s.CollectApplicationRetention(t.Context(), plan.ID, ReadBudget{Rows: p.MaxPageRows, Bytes: p.MaxInstallBytes})
		if err != nil {
			t.Fatal(err)
		}
		event := stableStatePair(t, fs, StableStateRetentionPage)
		afterPage, err := encodeMeta(s.meta)
		if err != nil || event.BeforeMetadataSHA256 != sha256.Sum256(beforePage) || event.MetadataSHA256 != sha256.Sum256(afterPage) {
			t.Fatal("collector substituted prospective metadata for its actual prior envelope", err)
		}
		identity := event.Selection.Identity
		if identity.PlanID != plan.ID || identity.SourceIndex != 3 || identity.Applied != 4 || identity.Section != progress.Section || identity.ProgressSHA256 != s.meta.Retention.Cursor.Hash || identity.Final != progress.Complete {
			t.Fatal("page/cursor/deletion metadata did not share the actual atomic batch")
		}
		if progress.Complete {
			complete = true
			break
		}
	}
	if !complete || fs.overflow {
		t.Fatal("finite genuine collection did not reach sealed complete cursor")
	}
	old := viewApplication(t, s, 2)
	current := viewApplication(t, s, 4)
	getApplication(t, old, "key", "old", true, false)
	getApplication(t, current, "key", "new", true, false)
	if _, err := s.ApplicationView(1); !errors.Is(err, ErrApplicationExpired) {
		t.Fatal("deleted unprotected history returned a substituted source", err)
	}
}

func TestStableStateImportRetireAndLegacyDormantClearPreserveActiveSource(t *testing.T) {
	fs := &stableStateFS{FS: vfs.NewMem()}
	s := receiverStore(t, fs, 1)
	old := viewApplication(t, s, 1)
	i, prepared, _ := preparedFixture(t, s)
	manifest, generation := i.id, i.generation
	if err := prepared.Close(); err != nil {
		t.Fatal(err)
	}
	retired := stableStatePair(t, fs, StableStateImportRetire)
	cleared := stableStatePair(t, fs, StableStateGenerationClear)
	if retired.Selection.Identity.ManifestID != manifest || retired.Selection.Identity.TargetGeneration != generation || retired.Selection.Identity.ActiveGeneration != 1 || cleared.Selection.Identity.TargetGeneration != generation || cleared.Selection.Identity.TargetBank != 1 || cleared.Selection.Identity.ManifestID != ([32]byte{}) || !i.closed {
		t.Fatal("dormant retirement/physical clearing invented activation")
	}
	getApplication(t, old, "a", "", false, false)
	if root, err := old.Root(); err != nil || root.Index != 1 || string(root.Image) != "initial" {
		t.Fatal("dormant retirement replaced active history", root, err)
	}
	// The applicable legacy AS1 path has no generation/publication authority.
	// Real Begin/Append/Verify/Abort may delete ONLY its actual dormant rows.
	donor := transferStore(t, "donor", vfs.NewMem(), 1)
	applyApplication(t, donor, "incoming", KV{Key: []byte("a"), Value: []byte("incoming")})
	export, err := donor.BeginApplicationExport(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer export.Close()
	manifestValue, err := export.Manifest()
	if err != nil {
		t.Fatal(err)
	}
	legacyFS := &stableStateFS{FS: vfs.NewMem()}
	legacy := transferStore(t, "legacy", legacyFS, 3)
	held := viewApplication(t, legacy, 1)
	imported, err := legacy.BeginApplicationImport(t.Context(), manifestValue)
	if err != nil {
		t.Fatal(err)
	}
	for _, chunk := range exportChunks(t, export) {
		if err := imported.Append(t.Context(), chunk); err != nil {
			t.Fatal(err)
		}
	}
	if err := imported.Verify(t.Context()); err != nil {
		t.Fatal(err)
	}
	beforeClear, err := encodeMeta(legacy.meta)
	if err != nil {
		t.Fatal(err)
	}
	if err := imported.Abort(); err != nil {
		t.Fatal(err)
	}
	clear := stableStatePair(t, legacyFS, StableStateDormantClear)
	if clear.Selection.Identity.ManifestID != ([32]byte{}) || clear.Selection.Identity.SourceCutID != ([32]byte{}) || clear.Selection.Identity.TargetBank != 1 || clear.Selection.Identity.TargetGeneration != 0 || clear.Selection.Identity.ActiveGeneration != 0 || clear.BeforeMetadataSHA256 != sha256.Sum256(beforeClear) || clear.MetadataSHA256 != sha256.Sum256(beforeClear) {
		t.Fatal("legacy dormant cleanup fabricated a native generation/cut")
	}
	getApplication(t, held, "a", "", false, false)
	if root, err := held.Root(); err != nil || root.Index != 1 || string(root.Image) != "initial" {
		t.Fatal("legacy dormant cleanup changed captured active source", root, err)
	}
}
