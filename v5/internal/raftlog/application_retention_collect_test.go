package raftlog

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"go.etcd.io/raft/v3"
)

// Authored native floor/opaque Stage guard. Store fixture persistence does not
// claim distributed graph GC, durable live-pin or fault acceptance.
func TestApplicationRetentionFloorsAuthenticateCommandAndCapturedOwners(t *testing.T) {
	policy := DefaultApplicationPolicy(1)
	// Two ordinary installation allowances + the actual six-index census29
	// + one valid eight-record completion exactly fit WITHOUT the cursor row.
	policy.RetainedApplicationRecords = 2*unsignedLimit(policy.MaxInstallWrites+4) + 37
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
	persist(t, s, 2, 2, ent(2, 2, "old"))
	if err := s.InstallApplication(2, ApplicationBatch{BaseGeneration: s.ApplicationGeneration(), BaseIndex: 1, BaseImageHash: sha256.Sum256([]byte("genesis")), Image: []byte("old"), Writes: []KV{{Key: []byte("key"), Value: []byte("old")}}, Changes: []byte("change:old"), Outcome: []byte("outcome:old"), ControlPuts: []KV{{Key: []byte("original"), Value: []byte("immutable-decision")}}}); err != nil {
		t.Fatal(err)
	}
	old, err := s.ApplicationView(2)
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	generationApply(t, s, "obsolete", KV{Key: []byte("key"), Value: []byte("obsolete")})
	generationApply(t, s, "middle", KV{Key: []byte("key"), Value: []byte("middle")})
	generationApply(t, s, "current", KV{Key: []byte("key"), Value: []byte("new")})
	getApplication(t, old, "key", "old", true, false)
	past, err := old.RootAt(t.Context(), 1, policy.MaxImageBytes)
	if err != nil || string(past.Image) != "genesis" {
		t.Fatal("old view lost earlier history", past, err)
	}
	view, err := s.ApplicationView(5)
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	cut, err := s.PublishedApplicationCut()
	if err != nil {
		t.Fatal(err)
	}
	next := ApplicationRetentionState{ID: [32]byte{7}, SourceIndex: 5, InstalledIndex: 6, SourceHash: sha256.Sum256([]byte("current")), RetiredCutID: cut.ID, RootFloor: 2, DataFloor: 4, FeedFloor: 6, OutcomeFloor: 5, RecoveryFloor: 5,
		Roots: []ApplicationRetentionRoot{{Index: 1, ImageHash: sha256.Sum256([]byte("genesis"))}, {Index: 2, ImageHash: sha256.Sum256([]byte("old")), Data: true}},
	}
	command := []byte("install-floor")
	persist(t, s, 6, 6, ent(6, 6, string(command)))
	var nilView *ApplicationView
	if _, _, err := nilView.PrepareApplicationRetention(t.Context(), next, command, ReadBudget{Rows: 8, Bytes: policy.MaxPageBytes}); !errors.Is(err, ErrInvalid) {
		t.Fatal("nil admission", err)
	}
	if _, err := nilView.RetentionAnchor(t.Context(), next.ID, 6, 1, 32); !errors.Is(err, ErrInvalid) {
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
	batch := ApplicationBatch{RetentionAdmission: admission, BaseGeneration: s.ApplicationGeneration(), BaseIndex: 5, BaseImageHash: next.SourceHash, Image: []byte("current")}
	before := readyMetadataFingerprint(t, s)
	if err := s.InstallApplication(6, batch); !errors.Is(err, ErrLimit) || readyMetadataFingerprint(t, s) != before {
		t.Fatal("unrelated held owner was excluded", err)
	}
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	copied := *admission
	batch.RetentionAdmission = &copied
	if err := s.InstallApplication(6, batch); !errors.Is(err, ErrInvalid) {
		t.Fatal("copied native grant was accepted", err)
	}
	batch.RetentionAdmission = admission
	// The original Stage closes its exact reader before Driver installation.
	// Neither its closed registration nor a numerical ref count exempts another.
	if err := view.Close(); err != nil {
		t.Fatal(err)
	}
	next.Roots[0].ImageHash[0]++
	if err := s.InstallApplication(6, batch); err != nil {
		t.Fatal("closed original source could not install exact grant", err)
	}
	if s.meta.Retention.State.Roots[0].ImageHash != sha256.Sum256([]byte("genesis")) || s.meta.Retention.Cursor != (applicationRetentionCursor{}) {
		t.Fatal("admission borrowed caller plan or manufactured cursor")
	}
	if _, err := s.ApplicationView(1); !errors.Is(err, ErrApplicationExpired) || s.poison != nil {
		t.Fatal("new old cut was retroactively admitted", err)
	}
	recoveryOld, err := s.ApplicationView(4)
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
	current, err := s.ApplicationView(6)
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
	anchor, err := current.RetentionAnchor(t.Context(), admission.state.ID, 6, 1, 32)
	if err != nil || string(anchor.Image) != "genesis" {
		t.Fatal("exact installed semantic anchor", anchor, err)
	}
	if _, err := current.RetentionAnchor(t.Context(), [32]byte{8}, 6, 1, 32); !errors.Is(err, ErrInvalid) {
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
	if manifest.Retention.ID != admission.state.ID || manifest.RecoveryRecords != 6 || manifest.NamespaceRecords[1] != 6 {
		t.Fatal("floor-only AS7 falsely inferred physical deletion", manifest)
	}
	if _, records := applicationTotals(s.meta); records != 29 {
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

	if err := current.Close(); err != nil {
		t.Fatal(err)
	}
	if err := export.Close(); err != nil {
		t.Fatal(err)
	}
	beforeCut, err := s.PublishedApplicationCut()
	if err != nil {
		t.Fatal(err)
	}
	var nilStore *Store
	if _, _, err := nilStore.CollectApplicationRetention(t.Context(), admission.state.ID, ReadBudget{Rows: 4, Bytes: 32768}); !errors.Is(err, ErrInvalid) {
		t.Fatal("nil collector", err)
	}
	if _, _, err := s.CollectApplicationRetention(nil, admission.state.ID, ReadBudget{Rows: 4, Bytes: 32768}); !errors.Is(err, ErrInvalid) {
		t.Fatal("nil context", err)
	}
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, _, err := s.CollectApplicationRetention(cancelled, admission.state.ID, ReadBudget{Rows: 4, Bytes: 32768}); !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled collector", err)
	}
	if _, _, err := s.CollectApplicationRetention(t.Context(), [32]byte{99}, ReadBudget{Rows: 4, Bytes: 32768}); !errors.Is(err, ErrInvalid) {
		t.Fatal("unknown installed plan", err)
	}
	before = readyMetadataFingerprint(t, s)
	if _, _, err := s.CollectApplicationRetention(t.Context(), admission.state.ID, ReadBudget{Rows: 1, Bytes: 1}); !errors.Is(err, ErrLimit) || readyMetadataFingerprint(t, s) != before || s.poison != nil {
		t.Fatal("budget refusal made progress", err)
	}
	// Cursor lookup plus the two kept newest versions exhausts four rows before
	// an authenticated predecessor probe can decide the third. This page scans
	// only: retiring its publication would resurrect the same ID on republish.
	progress, work, err := s.CollectApplicationRetention(t.Context(), admission.state.ID, ReadBudget{Rows: 4, Bytes: 32768})
	if err != nil || progress.Section != 1 || progress.CursorRecords != 1 || work.Records > 4 {
		t.Fatal("scan-only bounded page", progress, work, err)
	}
	still, err := s.PublishedApplicationCut()
	if err != nil || still.ID != beforeCut.ID {
		t.Fatal("scan-only page retired unchanged publication", still, err)
	}
	usage, err := s.ApplicationUsage()
	if err != nil || usage.RetainedRecords != 30 {
		t.Fatal("local cursor absent from usage", usage, err)
	}
	if _, rows := applicationTotals(s.meta); rows != 29 {
		t.Fatal("cursor entered portable census", rows)
	}
	replacement := cloneRetentionState(admission.state)
	replacement.PreviousID, replacement.ID = replacement.ID, [32]byte{11}
	replacement.SourceIndex, replacement.InstalledIndex = 6, 7
	replacement.RetiredCutID = beforeCut.ID
	pendingView, err := s.ApplicationView(6)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := pendingView.PrepareApplicationRetention(t.Context(), replacement, []byte("next-plan"), ReadBudget{Rows: 8, Bytes: policy.MaxPageBytes}); !errors.Is(err, ErrLimit) {
		t.Fatal("unfinished original plan replaced", err)
	}
	if err := pendingView.Close(); err != nil {
		t.Fatal(err)
	}
	// Resume from the actual synchronized cursor after a physical close/reopen.
	cursorBefore := s.meta.Retention.Cursor
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	cfg.Create = false
	s, err = Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if s.meta.Retention.Cursor != cursorBefore {
		t.Fatal("reopen discarded actual cursor")
	}
	progress, _, err = s.CollectApplicationRetention(t.Context(), admission.state.ID, ReadBudget{Rows: 4, Bytes: 32768})
	if err != nil || progress.Section != 1 {
		t.Fatal("first actual obsolete version delete", progress, err)
	}
	if _, err := s.PublishedApplicationCut(); !errors.Is(err, ErrApplicationExpired) {
		t.Fatal("deleted publication remained live", err)
	}
	if _, err := s.BeginPublishedApplicationExport(t.Context(), beforeCut.ID); !errors.Is(err, ErrApplicationExpired) {
		t.Fatal("retired exact publication exported", err)
	}
	for page := 0; !progress.Complete && page < 32; page++ {
		progress, work, err = s.CollectApplicationRetention(t.Context(), admission.state.ID, ReadBudget{Rows: 16, Bytes: 32768})
		if err != nil || work.Records > 16 || work.Bytes > 32768 {
			t.Fatal("finite resume", progress, work, err)
		}
	}
	if !progress.Complete || progress.Section != 6 || progress.CursorBytes != 0 || progress.CursorRecords != 0 {
		t.Fatal("cursor did not retire", progress)
	}
	if err := s.ScrubApplication(t.Context()); err != nil {
		t.Fatal("actual sparse scrub", err)
	}
	if _, rows := applicationTotals(s.meta); rows != 15 {
		t.Fatal("sparse census", rows)
	}
	if err := s.PublishSnapshot(); err != nil {
		t.Fatal("genuine same-index sparse publication", err)
	}
	afterCut, err := s.PublishedApplicationCut()
	if err != nil || afterCut.ID == beforeCut.ID || afterCut.Index != beforeCut.Index || afterCut.ImageHash != beforeCut.ImageHash || afterCut.RetainedRecords != 15 {
		t.Fatal("collection invented business progress or revived old ID", afterCut, err)
	}
	exact, err := s.BeginPublishedApplicationExport(t.Context(), afterCut.ID)
	if err != nil {
		t.Fatal(err)
	}
	collected := buildPublished(t, exact, ReadBudget{Rows: 8, Bytes: 8192})
	if collected.ControlRecords != 1 || collected.RecoveryRecords != 2 || collected.NamespaceRecords != [4]uint64{3, 6, 1, 2} {
		t.Fatal("sparse namespace census", collected)
	}
	if err := exact.Close(); err != nil {
		t.Fatal(err)
	}
	// Verify exact retained physical versions and suffixes independently of the
	// keep predicate: latest floor predecessor4 and explicit Data anchor2 survive;
	// obsolete3 is absent, current5 survives, controls/originals were untouched.
	want := map[string]bool{"control:original:2": true, "data:key:2": true, "data:key:4": true, "data:key:5": true, "root:1": true, "root:2": true, "root:3": true, "root:4": true, "root:5": true, "root:6": true, "feed:6": true, "outcome:5": true, "outcome:6": true, "recovery:5": true, "recovery:6": true}
	it, err := newApplicationIterator(s.db.NewIter, s.activeBank(), true, true)
	if err != nil {
		t.Fatal(err)
	}
	for valid := it.First(); valid; valid = it.Next() {
		key := it.Key()
		var label string
		switch key[0] {
		case controlBankTag(s.activeBank()):
			logical, index, err := decodeControlKey(s.activeBank(), key, policy.MaxKeyBytes)
			if err != nil {
				t.Fatal(err)
			}
			raw, err := it.ValueAndErr()
			if err != nil {
				t.Fatal(err)
			}
			data, deleted, err := inspectAppFrame(key, raw, policy.MaxValueBytes)
			if err != nil || deleted || string(data) != "immutable-decision" {
				t.Fatal("immutable original changed", err)
			}
			label = fmt.Sprintf("control:%s:%d", logical, index)
		case bankTag(s.activeBank(), appDataTag):
			logical, index, err := decodeBankAppKey(s.activeBank(), key, policy.MaxKeyBytes)
			if err != nil {
				t.Fatal(err)
			}
			label = fmt.Sprintf("data:%s:%d", logical, index)
		default:
			if len(key) != 9 {
				t.Fatal("unexpected native key", key)
			}
			name := map[byte]string{bankTag(s.activeBank(), appRootTag): "root", bankTag(s.activeBank(), appChangeTag): "feed", bankTag(s.activeBank(), appOutcomeTag): "outcome", recoveryRootTag + s.activeBank(): "recovery"}[key[0]]
			label = fmt.Sprintf("%s:%d", name, binary.BigEndian.Uint64(key[1:]))
		}
		if !want[label] {
			t.Fatal("unexpected or duplicated retained row", label)
		}
		delete(want, label)
	}
	iterErr := it.Error()
	closeErr := it.Close()
	if err := errors.Join(iterErr, closeErr); err != nil || len(want) != 0 {
		t.Fatal("missing exact retained rows", want, err)
	}
	if _, _, err := s.CollectApplicationRetention(t.Context(), admission.state.ID, ReadBudget{Rows: 4, Bytes: 32768}); err != nil {
		t.Fatal("completed no-op", err)
	}
	afterNoop, err := s.PublishedApplicationCut()
	if err != nil || afterNoop.ID != afterCut.ID {
		t.Fatal("no-deletion page retired current ID", err)
	}
	if err := s.PublishSnapshot(); !errors.Is(err, raft.ErrSnapOutOfDate) {
		t.Fatal("no-deletion same-ID republish", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(cfg)
	if err != nil {
		t.Fatal("sparse reopen", err)
	}
	defer s.Close()
	if err := s.ScrubApplication(t.Context()); err != nil {
		t.Fatal("reopened sparse scrub", err)
	}
}
