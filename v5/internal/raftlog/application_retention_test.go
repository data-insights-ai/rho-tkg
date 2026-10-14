package raftlog

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	pb "go.etcd.io/raft/v3/raftpb"
	"path/filepath"
	"testing"
)

// Authored contract/codec guards only; sparse publication/collection and native
// graph-pin histories require the later operative AS7 packages and qualification.
func TestApplicationRetentionContractMetadataAndLegacyRefusal(t *testing.T) {
	p := DefaultApplicationPolicy(1)
	controls, recovery := ApplicationControlConfig{Version: 1}, ApplicationRecoveryConfig{Version: 2}
	disabled, err := ApplicationContractForPolicyWithRetention(p, controls, recovery, ApplicationRetentionConfig{})
	old, oldErr := ApplicationContractForPolicyWithRecovery(p, controls, recovery)
	if err != nil || oldErr != nil || disabled != old || disabled.Version != 4 {
		t.Fatal("disabled retention changed old contract", disabled, err, oldErr)
	}
	retention := ApplicationRetentionConfig{Version: 1}
	fresh, err := ApplicationContractForPolicyWithRetention(p, controls, recovery, retention)
	want := old
	want.Version = 5
	if err != nil || fresh != want {
		t.Fatal("fresh profile changed numerical quotas", fresh, err)
	}
	if _, err := ApplicationContractForPolicyWithRetention(p, controls, recovery, ApplicationRetentionConfig{Version: 2}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := ApplicationContractForPolicyWithRetention(p, controls, ApplicationRecoveryConfig{Version: 1}, retention); !errors.Is(err, ErrInvalid) {
		t.Fatal("retention borrowed RS1 profile", err)
	}
	var nilStore *Store
	if nilStore.ApplicationRetention() != (ApplicationRetentionConfig{}) {
		t.Fatal("nil retention inspection")
	}
	r := applicationRetentionMetadata{Config: retention, State: ApplicationRetentionState{
		ID: [32]byte{1}, PreviousID: [32]byte{2}, SourceHash: [32]byte{3}, RetiredCutID: [32]byte{4},
		SourceIndex: 20, InstalledIndex: 21, RootFloor: 20, FeedFloor: 21, OutcomeFloor: 20, RecoveryFloor: 20, DataFloor: 20,
		Roots: []ApplicationRetentionRoot{{Index: 1, ImageHash: [32]byte{5}}, {Index: 15, ImageHash: [32]byte{6}, Data: true}},
	}, Cursor: applicationRetentionCursor{Section: 1, Bytes: 30, Hash: [32]byte{7}}}
	wire := appendRetentionMetadata(nil, r)
	got, tail, err := decodeRetentionMetadata(wire, 21)
	if err != nil || len(tail) != 0 || !bytes.Equal(appendRetentionMetadata(nil, got), wire) || len(wire) != retentionMetadataBytes(r) {
		t.Fatal("complete bounded plan/cursor codec", err)
	}
	wire[8+128+56+4+8]++
	if got.State.Roots[0].ImageHash != ([32]byte{5}) || got.State.Roots[1].ImageHash != ([32]byte{6}) || !got.State.Roots[1].Data {
		t.Fatal("decoded anchor borrowed wire or lost data protection")
	}
	oversized := appendRetentionMetadata(nil, r)
	binary.BigEndian.PutUint32(oversized[8+128+56:8+128+56+4], 65)
	if _, _, err := decodeRetentionMetadata(oversized, 21); !errors.Is(err, ErrCorrupt) {
		t.Fatal("oversized anchor allocation", err)
	}
	changed := r
	changed.State.InstalledIndex = 22
	if err := changed.syntax(22); !errors.Is(err, ErrInvalid) {
		t.Fatal("plan skipped its actual installation", err)
	}
	changed = r
	changed.State.SourceIndex, changed.State.InstalledIndex = ^uint64(0), 0
	if err := changed.syntax(0); !errors.Is(err, ErrInvalid) {
		t.Fatal("overflowing installation index", err)
	}
	changed = r
	changed.State.FeedFloor = 22
	if err := changed.syntax(21); !errors.Is(err, ErrInvalid) {
		t.Fatal("future feed floor", err)
	}
	changed = r
	changed.Cursor.Hash = [32]byte{}
	if err := changed.syntax(21); !errors.Is(err, ErrInvalid) {
		t.Fatal("unbound local cursor", err)
	}
	// Inert independently assembled sparse frames exercise the fresh wire
	// contract only. They are not a native installed plan or GC acceptance.
	manifest := ApplicationSnapshotManifest{Version: 7, Identity: ApplicationIdentity{Graph: [16]byte{1}, Partition: 1, Group: [16]byte{2}}, Contract: fresh, SemanticContractID: ApplicationSemanticContractID{9}, Index: 12, Term: 2, ConfState: &pb.ConfState{Voters: []uint64{1, 2, 3}}, Image: []byte("r12")}
	manifest.ImageHash = sha256.Sum256(manifest.Image)
	manifest.Retention = ApplicationRetentionState{ID: [32]byte{11}, SourceIndex: 10, InstalledIndex: 11, SourceHash: sha256.Sum256([]byte("r10")), RootFloor: 10, FeedFloor: 10, OutcomeFloor: 10, RecoveryFloor: 10, DataFloor: 10, Roots: []ApplicationRetentionRoot{{Index: 1, ImageHash: sha256.Sum256([]byte("genesis"))}}}
	type inertFrame struct{ key, raw []byte }
	var frames []inertFrame
	add := func(tag byte, index uint64, value []byte) {
		key := appIndexKey(tag, index)
		raw := appFrame(key, value, false)
		frames = append(frames, inertFrame{key, raw})
		cost := uint64(len(key) + len(raw))
		if tag == recoveryRootTag {
			manifest.RecoveryRecords++
			manifest.RecoveryBytes += cost
		} else {
			slot := int(tag - appDataTag)
			manifest.NamespaceRecords[slot]++
			manifest.NamespaceBytes[slot] += cost
		}
	}
	for _, root := range []struct {
		index uint64
		value string
	}{{1, "genesis"}, {10, "r10"}, {11, "r11"}, {12, "r12"}} {
		add(appRootTag, root.index, []byte(root.value))
	}
	for _, tag := range []byte{appChangeTag, appOutcomeTag} {
		for index := uint64(10); index <= 12; index++ {
			add(tag, index, []byte("native envelope"))
		}
	}
	rs := appendNativeRecoveryRoot(applicationRecoveryMetadata{Config: recovery})
	for index := uint64(10); index <= 12; index++ {
		add(recoveryRootTag, index, rs)
	}
	total, records, err := snapshotTotals(manifest)
	if err != nil {
		t.Fatal(err)
	}
	ref := ApplicationCutReference{Retention: manifestRetention(manifest), Identity: manifest.Identity, Contract: fresh, SemanticContractID: manifest.SemanticContractID, Index: 12, Term: 2, ConfState: manifest.ConfState, ImageBytes: uint64(len(manifest.Image)), ImageHash: manifest.ImageHash, RetainedBytes: total, RetainedRecords: records, RecoveryBytes: manifest.RecoveryBytes, RecoveryRecords: manifest.RecoveryRecords, RecoveryHash: snapshotRecoveryHash(manifest)}
	manifest.CutID, err = cutID(ref)
	if err != nil {
		t.Fatal("sparse cut was treated as continuous history", err)
	}
	state := snapshotVerifier{digest: snapshotSeed(manifest)}
	for _, frame := range frames {
		state, err = state.accept(manifest, frame.key, frame.raw)
		if err != nil {
			t.Fatal(err)
		}
	}
	manifest.RecordsHash = state.digest
	if err := state.complete(manifest); err != nil {
		t.Fatal("complete sparse census", err)
	}
	encodedManifest, err := EncodeApplicationSnapshotManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	decodedManifest, err := DecodeApplicationSnapshotManifestForBinding(encodedManifest, p.MaxImageBytes, ApplicationBinding{Identity: manifest.Identity, SemanticContractID: manifest.SemanticContractID})
	if err != nil || !bytes.Equal(appendPortableRetention(nil, decodedManifest.Retention), appendPortableRetention(nil, manifest.Retention)) || decodedManifest.RecoveryRecords != 3 || decodedManifest.NamespaceRecords[1] != 4 {
		t.Fatal("sparse AS7 header lost its actual census/anchors", err)
	}
	changedState := cloneRetentionState(manifest.Retention)
	changedState.ID[0]++
	changedRef := ref
	changedRef.Retention = &changedState
	changedID, err := cutID(changedRef)
	if err != nil || changedID == manifest.CutID {
		t.Fatal("cut identity ignored portable plan", err)
	}
	missing := snapshotVerifier{digest: snapshotSeed(manifest)}
	for _, frame := range frames {
		if frame.key[0] == appRootTag && binary.BigEndian.Uint64(frame.key[1:]) == 1 {
			continue
		}
		missing, err = missing.accept(manifest, frame.key, frame.raw)
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := missing.completeRetentionRuns(manifest); !errors.Is(err, ErrCorrupt) {
		t.Fatal("required semantic anchor was omitted", err)
	}
	badSource := appFrame(frames[1].key, []byte("different"), false)
	badVerifier := snapshotVerifier{digest: snapshotSeed(manifest)}
	if _, err := badVerifier.accept(manifest, frames[1].key, badSource); !errors.Is(err, ErrCorrupt) {
		t.Fatal("plan borrowed an unbound source image", err)
	}
	// Fresh configuration is genuinely persisted; the capability cannot be
	// retrofitted on reopen. No store initialization or portable token is asserted.
	cfg := Config{Dir: filepath.Join(t.TempDir(), "retention"), Create: true, Application: p, Controls: controls, Recovery: recovery, Retention: retention,
		SemanticContractID: ApplicationSemanticContractID{9}, Replication: ApplicationReplicationConfig{Voters: [3]uint64{1, 2, 3}},
		Transfer:    ApplicationTransferConfig{Identity: ApplicationIdentity{Graph: [16]byte{1}, Partition: 1, Group: [16]byte{2}}, Contract: fresh, Limits: DefaultApplicationTransferLimits()},
		Generations: ApplicationGenerationLimits{MaxBytes: 2 << 30, MaxRecords: 2_000_000}, PublishedCuts: ApplicationPublishedCutLimits{MaxTransferChunks: 4096},
	}
	s, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if s.ApplicationRetention() != retention {
		t.Fatal("actual configured retention")
	}
	encoded, err := encodeMeta(s.meta)
	if err != nil || !bytes.Equal(encoded[:4], []byte("RLM9")) {
		t.Fatal("fresh metadata profile", err)
	}
	decoded, err := decodeMeta(encoded, s.limits)
	if err != nil || decoded.Retention.Config != retention || len(encoded) != int(metadataBytes(s.meta)) {
		t.Fatal("fresh metadata accounting", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	cfg.Create = false
	reopened, err := Open(cfg)
	if err != nil {
		t.Fatal("fresh exact reopen", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	cfg.Retention = ApplicationRetentionConfig{}
	if reopened, err := Open(cfg); reopened != nil || !errors.Is(err, ErrInvalid) {
		t.Fatal("legacy reopen acquired retention", reopened, err)
	}
	cfg.Retention = retention
	cfg.Transfer.Contract = old
	if reopened, err := Open(cfg); reopened != nil || !errors.Is(err, ErrInvalid) {
		t.Fatal("old agreement admitted sparse profile", reopened, err)
	}
}
