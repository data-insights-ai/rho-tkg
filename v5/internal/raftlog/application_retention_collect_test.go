package raftlog

import (
	"crypto/sha256"
	"errors"
	"path/filepath"
	"testing"
)

// Authored native floor/opaque Stage guard. Store fixture persistence does not
// claim distributed graph GC, durable live-pin or fault acceptance.
func TestApplicationRetentionFloorsAuthenticateCommandAndCapturedOwners(t *testing.T) {
	policy := DefaultApplicationPolicy(1)
	// Two ordinary installation allowances + the actual four-index census18
	// + one valid eight-record completion exactly fit WITHOUT the cursor row.
	policy.RetainedApplicationRecords = 2*unsignedLimit(policy.MaxInstallWrites+4) + 26
	controls, recovery, retention := ApplicationControlConfig{Version: 1}, ApplicationRecoveryConfig{Version: 2}, ApplicationRetentionConfig{Version: 1}
	contract, err := ApplicationContractForPolicyWithRetention(policy, controls, recovery, retention)
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{Dir: filepath.Join(t.TempDir(), "floor"), Create: true, Application: policy, Controls: controls, Recovery: recovery, Retention: retention, SemanticContractID: ApplicationSemanticContractID{9},
		Replication: ApplicationReplicationConfig{Voters: [3]uint64{1, 2, 3}}, Transfer: ApplicationTransferConfig{Identity: ApplicationIdentity{Graph: [16]byte{1}, Partition: 1, Group: [16]byte{2}}, Contract: contract, Limits: DefaultApplicationTransferLimits()},
		Generations: ApplicationGenerationLimits{MaxBytes: 2 << 30, MaxRecords: 2_000_000}, PublishedCuts: ApplicationPublishedCutLimits{MaxTransferChunks: 4096},
	}
	s, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Initialize([]uint64{1, 2, 3}, []byte("genesis")); err != nil {
		t.Fatal(err)
	}
	generationApply(t, s, "old", KV{Key: []byte("key"), Value: []byte("old")})
	old, err := s.ApplicationView(2)
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	generationApply(t, s, "current", KV{Key: []byte("key"), Value: []byte("new")})
	getApplication(t, old, "key", "old", true, false)
	past, err := old.RootAt(t.Context(), 1, policy.MaxImageBytes)
	if err != nil || string(past.Image) != "genesis" {
		t.Fatal("old view lost earlier history", past, err)
	}
	view, err := s.ApplicationView(3)
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	cut, err := s.PublishedApplicationCut()
	if err != nil {
		t.Fatal(err)
	}
	next := ApplicationRetentionState{ID: [32]byte{7}, SourceIndex: 3, InstalledIndex: 4, SourceHash: sha256.Sum256([]byte("current")), RetiredCutID: cut.ID, RootFloor: 2, DataFloor: 2, FeedFloor: 4, OutcomeFloor: 3, RecoveryFloor: 3,
		Roots: []ApplicationRetentionRoot{{Index: 1, ImageHash: sha256.Sum256([]byte("genesis"))}, {Index: 2, ImageHash: sha256.Sum256([]byte("old")), Data: true}},
	}
	command := []byte("install-floor")
	persist(t, s, 4, 4, ent(4, 4, string(command)))
	var nilView *ApplicationView
	if _, _, err := nilView.PrepareApplicationRetention(t.Context(), next, command, ReadBudget{Rows: 8, Bytes: policy.MaxPageBytes}); !errors.Is(err, ErrInvalid) {
		t.Fatal("nil admission", err)
	}
	if _, err := nilView.RetentionAnchor(t.Context(), next.ID, 4, 1, 32); !errors.Is(err, ErrInvalid) {
		t.Fatal("nil anchor", err)
	}
	if a, _, err := view.PrepareApplicationRetention(t.Context(), next, command, ReadBudget{Rows: 8, Bytes: 1}); a != nil || !errors.Is(err, ErrLimit) || s.poison != nil {
		t.Fatal("budget refusal minted authority or poisoned", a, err)
	}
	if a, _, err := view.PrepareApplicationRetention(t.Context(), next, []byte("different"), ReadBudget{Rows: 8, Bytes: policy.MaxPageBytes}); a != nil || !errors.Is(err, ErrInvalid) {
		t.Fatal("caller digest replaced actual committed command", a, err)
	}
	wrong := cloneRetentionState(next)
	wrong.Roots[0].ImageHash[0]++
	if a, _, err := view.PrepareApplicationRetention(t.Context(), wrong, command, ReadBudget{Rows: 8, Bytes: policy.MaxPageBytes}); a != nil || !errors.Is(err, ErrInvalid) || s.poison != nil {
		t.Fatal("inert wrong anchor poisoned valid durable state", a, err)
	}
	admission, _, err := view.PrepareApplicationRetention(t.Context(), next, command, ReadBudget{Rows: 8, Bytes: policy.MaxPageBytes})
	if err != nil {
		t.Fatal(err)
	}
	batch := ApplicationBatch{RetentionAdmission: admission, BaseGeneration: s.ApplicationGeneration(), BaseIndex: 3, BaseImageHash: next.SourceHash, Image: []byte("current")}
	before := readyMetadataFingerprint(t, s)
	if err := s.InstallApplication(4, batch); !errors.Is(err, ErrLimit) || readyMetadataFingerprint(t, s) != before {
		t.Fatal("unrelated held owner was excluded", err)
	}
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	copied := *admission
	batch.RetentionAdmission = &copied
	if err := s.InstallApplication(4, batch); !errors.Is(err, ErrInvalid) {
		t.Fatal("copied native grant was accepted", err)
	}
	batch.RetentionAdmission = admission
	// The original Stage closes its exact reader before Driver installation.
	// Neither its closed registration nor a numerical ref count exempts another.
	if err := view.Close(); err != nil {
		t.Fatal(err)
	}
	next.Roots[0].ImageHash[0]++
	if err := s.InstallApplication(4, batch); err != nil {
		t.Fatal("closed original source could not install exact grant", err)
	}
	if s.meta.Retention.State.Roots[0].ImageHash != sha256.Sum256([]byte("genesis")) || s.meta.Retention.Cursor != (applicationRetentionCursor{}) {
		t.Fatal("admission borrowed caller plan or manufactured cursor")
	}
	if _, err := s.ApplicationView(1); !errors.Is(err, ErrApplicationExpired) || s.poison != nil {
		t.Fatal("new old cut was retroactively admitted", err)
	}
	recoveryOld, err := s.ApplicationView(2)
	if err != nil {
		t.Fatal("root/data floor incorrectly equated with recovery floor", err)
	}
	if _, _, err := recoveryOld.RecoveryOwner(t.Context(), ReadBudget{Rows: 1, Bytes: 4096}); !errors.Is(err, ErrApplicationExpired) {
		t.Fatal("recovery-owner floor", err)
	}
	if _, _, err := recoveryOld.CheckTerminalBank(t.Context(), nil, ReadBudget{Rows: 1, Bytes: 4096}); !errors.Is(err, ErrApplicationExpired) {
		t.Fatal("terminal-bank recovery floor", err)
	}
	if err := recoveryOld.Close(); err != nil {
		t.Fatal(err)
	}
	current, err := s.ApplicationView(4)
	if err != nil {
		t.Fatal(err)
	}
	defer current.Close()
	getApplication(t, current, "key", "new", true, false)
	if _, err := current.RootAt(t.Context(), 1, 32); !errors.Is(err, ErrApplicationExpired) {
		t.Fatal("ordinary root used semantic/data anchor as old cut", err)
	}
	if _, err := current.ApplicationRecord(t.Context(), 2, false, 32); !errors.Is(err, ErrApplicationExpired) {
		t.Fatal("view feed floor", err)
	}
	if _, err := s.ApplicationRecord(t.Context(), 2, true, 32); !errors.Is(err, ErrApplicationExpired) {
		t.Fatal("store outcome floor", err)
	}
	if _, _, err := current.CheckRetainedRootFeed(t.Context(), 1, sha256.Sum256([]byte("genesis")), 4, ReadBudget{Rows: 2, Bytes: 4096}); !errors.Is(err, ErrApplicationExpired) {
		t.Fatal("retired source was blessed as pin evidence", err)
	}
	anchor, err := current.RetentionAnchor(t.Context(), admission.state.ID, 4, 1, 32)
	if err != nil || string(anchor.Image) != "genesis" {
		t.Fatal("exact installed semantic anchor", anchor, err)
	}
	if _, err := current.RetentionAnchor(t.Context(), [32]byte{8}, 4, 1, 32); !errors.Is(err, ErrInvalid) {
		t.Fatal("caller plan hash minted anchor authority", err)
	}
	if err := s.PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	export, err := s.BeginPublishedApplicationExport(t.Context(), s.meta.Gen.Publication.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer export.Close()
	manifest := buildPublished(t, export, ReadBudget{Rows: 8, Bytes: 8192})
	if manifest.Retention.ID != admission.state.ID || manifest.RecoveryRecords != 4 || manifest.NamespaceRecords[1] != 4 {
		t.Fatal("floor-only AS7 falsely inferred physical deletion", manifest)
	}
	if _, records := applicationTotals(s.meta); records != 18 {
		t.Fatal("adjacent quota fixture lost actual retained census", records)
	}
	reserve := ApplicationRecoveryReservation{ID: [32]byte{9}, Digest: [32]byte{10}, Credits: []ApplicationRecoveryCredit{{Phase: 1, CommandBytes: 128, RetainedBytes: 8192, RetainedRecords: 8}}}
	before = readyMetadataFingerprint(t, s)
	if err := s.admitRecoveryCreationLocked(128, reserve); !errors.Is(err, ErrLimit) {
		t.Fatal("explicit recovery creation spent cursor backing", err)
	}
	if err := s.admitTerminalCreationLocked(128, reserve); !errors.Is(err, ErrLimit) {
		t.Fatal("explicit terminal creation spent cursor backing", err)
	}
	if before != readyMetadataFingerprint(t, s) || s.poison != nil {
		t.Fatal("cursor quota refusal changed native state")
	}
	if growth := retentionCursorGrowth(s.meta); growth.records != 1 || growth.bytes != unsignedLimit(1+appFrameBytes+policy.MaxKeyBytes+32) {
		t.Fatal("cursor obligation was not separately prepaid", growth)
	}
}
