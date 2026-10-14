package raftlog

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"path/filepath"
	"testing"

	pb "go.etcd.io/raft/v3/raftpb"
)

// Authored native lifecycle guard. Its independently assembled sparse stream is
// not a native collector installation, graph publication or GC acceptance trace.
func TestApplicationRetentionSnapshotOwnsSparseStateAndReadyBanks(t *testing.T) {
	policy := DefaultApplicationPolicy(1)
	controls, recovery, retention := ApplicationControlConfig{Version: 1}, ApplicationRecoveryConfig{Version: 2}, ApplicationRetentionConfig{Version: 1}
	contract, err := ApplicationContractForPolicyWithRetention(policy, controls, recovery, retention)
	if err != nil {
		t.Fatal(err)
	}
	credit := ApplicationRecoveryCredit{Phase: 1, CommandBytes: 128, RetainedBytes: 8192, RetainedRecords: 8}
	transient := ApplicationRecoveryReservation{ID: [32]byte{1}, Digest: [32]byte{2}, Credits: []ApplicationRecoveryCredit{credit}}
	terminal := ApplicationRecoveryReservation{ID: [32]byte{3}, Digest: [32]byte{4}, Credits: []ApplicationRecoveryCredit{credit}}
	m := ApplicationSnapshotManifest{Version: 7, Identity: ApplicationIdentity{Graph: [16]byte{1}, Partition: 1, Group: [16]byte{2}}, Contract: contract, SemanticContractID: ApplicationSemanticContractID{9}, Index: 5, Term: 2,
		ConfState: &pb.ConfState{Voters: []uint64{1, 2, 3}}, Image: []byte("current"), RecoveryReservation: transient, TerminalReservations: []ApplicationRecoveryReservation{terminal},
		Retention: ApplicationRetentionState{ID: [32]byte{5}, SourceIndex: 3, InstalledIndex: 4, SourceHash: sha256.Sum256([]byte("source")), RootFloor: 3, FeedFloor: 3, OutcomeFloor: 3, RecoveryFloor: 3, DataFloor: 3,
			Roots: []ApplicationRetentionRoot{{Index: 1, ImageHash: sha256.Sum256([]byte("genesis"))}}},
	}
	m.ImageHash = sha256.Sum256(m.Image)
	type frame struct{ key, raw []byte }
	var frames []frame
	add := func(tag byte, index uint64, value []byte) {
		key := appIndexKey(tag, index)
		raw := appFrame(key, value, false)
		frames = append(frames, frame{key, raw})
		cost := uint64(len(key) + len(raw))
		if tag == recoveryRootTag {
			m.RecoveryBytes += cost
			m.RecoveryRecords++
		} else {
			slot := int(tag - appDataTag)
			m.NamespaceBytes[slot] += cost
			m.NamespaceRecords[slot]++
		}
	}
	for _, root := range []struct {
		index uint64
		image string
	}{{1, "genesis"}, {3, "source"}, {4, "installed"}, {5, "current"}} {
		add(appRootTag, root.index, []byte(root.image))
	}
	for _, tag := range []byte{appChangeTag, appOutcomeTag} {
		for index := uint64(3); index <= 5; index++ {
			add(tag, index, []byte("native envelope"))
		}
	}
	for index := uint64(3); index <= 5; index++ {
		add(recoveryRootTag, index, appendNativeRecoveryRoot(snapshotRecoveryState(m)))
	}
	size, rows, err := snapshotTotals(m)
	if err != nil {
		t.Fatal(err)
	}
	m.CutID, err = cutID(ApplicationCutReference{Retention: manifestRetention(m), Identity: m.Identity, Contract: m.Contract, SemanticContractID: m.SemanticContractID, Index: m.Index, Term: m.Term, ConfState: m.ConfState,
		ImageBytes: uint64(len(m.Image)), ImageHash: m.ImageHash, RetainedBytes: size, RetainedRecords: rows, RecoveryBytes: m.RecoveryBytes, RecoveryRecords: m.RecoveryRecords, RecoveryHash: snapshotRecoveryHash(m)})
	if err != nil {
		t.Fatal(err)
	}
	state := snapshotVerifier{digest: snapshotSeed(m)}
	var data []byte
	for _, f := range frames {
		state, err = state.accept(m, f.key, f.raw)
		if err != nil {
			t.Fatal(err)
		}
		data = appendSnapshotFrame(data, f.key, f.raw)
	}
	m.RecordsHash = state.digest
	if err := state.complete(m); err != nil {
		t.Fatal(err)
	}
	descriptor, err := EncodeApplicationSnapshotDescriptor(m)
	if err != nil || !bytes.Equal(descriptor[:4], []byte{'A', 'D', 6, 0}) {
		t.Fatal("AD6 binding", err)
	}
	owned := cloneSnapshotManifest(m)
	owned.Retention.Roots[0].ImageHash[0]++
	if owned.Retention.Roots[0] == m.Retention.Roots[0] {
		t.Fatal("manifest anchor alias")
	}
	cfg := Config{Dir: filepath.Join(t.TempDir(), "receiver"), Create: true, Application: policy, Controls: controls, Recovery: recovery, Retention: retention, SemanticContractID: m.SemanticContractID,
		Replication: ApplicationReplicationConfig{Voters: [3]uint64{1, 2, 3}}, Transfer: ApplicationTransferConfig{Identity: m.Identity, Contract: contract, Limits: DefaultApplicationTransferLimits()},
		Generations: ApplicationGenerationLimits{MaxBytes: 2 << 30, MaxRecords: 2_000_000}, PublishedCuts: ApplicationPublishedCutLimits{MaxTransferChunks: 4096},
	}
	s, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Initialize([]uint64{1, 2, 3}, []byte("initial")); err != nil {
		t.Fatal(err)
	}
	before := readyMetadataFingerprint(t, s)
	pins := s.pinnedApplicationBytes
	// This adjacent quota admits either future pool alone but not both. The
	// import must refuse before copying or installing any staging ownership.
	originalLimit := s.meta.Gen.Limits.MaxRecords
	s.meta.Gen.Limits.MaxRecords = s.meta.Gen.Banks[s.activeBank()].Records + rows + 2*credit.RetainedRecords - 1
	if imp, err := s.BeginApplicationImport(t.Context(), m); imp != nil || !errors.Is(err, ErrLimit) || s.applicationImport != nil || s.pinnedApplicationBytes != pins {
		t.Fatal("incoming terminal pool was not jointly prepaid", imp, err)
	}
	s.meta.Gen.Limits.MaxRecords = originalLimit
	if before != readyMetadataFingerprint(t, s) {
		t.Fatal("refused import changed durable state")
	}
	imp, err := s.BeginApplicationImport(t.Context(), m)
	if err != nil {
		t.Fatal(err)
	}
	defer imp.Abort()
	if imp.retentionReservation == 0 || s.pinnedApplicationBytes != pins+imp.retentionReservation {
		t.Fatal("import anchors uncharged")
	}
	chunk := ApplicationSnapshotChunk{Version: 7, CutID: m.CutID, ManifestID: publishedManifestID(m), Data: data, After: frames[len(frames)-1].key, Visited: uint64(len(frames)), VisitedBytes: uint64(len(data)), Final: true}
	if err := imp.Append(t.Context(), chunk); err != nil {
		t.Fatal(err)
	}
	if err := imp.Verify(t.Context()); err != nil {
		t.Fatal(err)
	}
	prepared, err := imp.Prepare()
	if err != nil {
		t.Fatal(err)
	}
	view, err := prepared.OpenReadView(t.Context(), m.Index, policy.MaxImageBytes)
	if err != nil {
		t.Fatal(err)
	}
	owner, _, err := view.RecoveryOwner(t.Context(), ReadBudget{Rows: 1, Bytes: 4096})
	if err != nil || owner != (ApplicationRecoveryOwner{ID: transient.ID, Digest: transient.Digest}) {
		t.Fatal("prepared AS7 fell back to RS1 digest", owner, err)
	}
	equal, _, err := view.CheckTerminalBank(t.Context(), m.TerminalReservations, ReadBudget{Rows: 1, Bytes: 4096})
	if err != nil || !equal {
		t.Fatal("prepared AS7 lost terminal roster", equal, err)
	}
	if err := view.Close(); err != nil {
		t.Fatal(err)
	}
	snapshot := &pb.Snapshot{Metadata: &pb.SnapshotMetadata{Index: new(m.Index), Term: new(m.Term), ConfState: m.ConfState}, Data: descriptor}
	claim, err := prepared.Claim(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	defer claim.Close()
	raw, ready := readyMatchRaw(t, s, snapshot)
	root, err := s.PersistApplicationReady(ready, claim)
	if err != nil || root.Index != m.Index || !bytes.Equal(root.Image, m.Image) {
		t.Fatal("actual Ready activation", root, err)
	}
	raw.Advance(ready)
	if imp.retentionReservation != 0 || !sameRecoveryReservation(s.meta.Recovery.Reservation, transient) || !sameTerminalBank(s.meta.Recovery.Terminal, applicationTerminalBank(m.TerminalReservations)) || !bytes.Equal(appendPortableRetention(nil, s.meta.Retention.State), appendPortableRetention(nil, m.Retention)) || s.meta.Retention.Cursor != (applicationRetentionCursor{}) {
		t.Fatal("activation lost sparse state or bank obligations")
	}
	cut, err := s.PublishedApplicationCut()
	if err != nil || cut.ID != m.CutID || cut.Retention == nil {
		t.Fatal("sparse publication identity", cut, err)
	}
	cut.Retention.Roots[0].ImageHash[0]++
	if s.meta.Retention.State.Roots[0] != m.Retention.Roots[0] || s.meta.Gen.Publication.Retention.Roots[0] != m.Retention.Roots[0] {
		t.Fatal("returned cut aliases active metadata")
	}
	if err := claim.Close(); err != nil {
		t.Fatal(err)
	}
	if s.pinnedApplicationBytes != pins {
		t.Fatal("activated import/claim ownership leaked")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	cfg.Create = false
	reopened, err := Open(cfg)
	if err != nil {
		t.Fatal("sparse metadata reopen", err)
	}
	defer reopened.Close()
	if !sameTerminalBank(reopened.meta.Recovery.Terminal, applicationTerminalBank(m.TerminalReservations)) || reopened.meta.Retention.State.ID != m.Retention.ID || reopened.meta.Recovery.Records != 3 {
		t.Fatal("reopen lost imported native identity/census")
	}
}
