package raftlog

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"testing"

	pb "go.etcd.io/raft/v3/raftpb"
)

// This inert AS6 guard is not a snapshot-activation or durable-pin history.
func TestApplicationTerminalSnapshotExactBanksBindingAndOwnedCopies(t *testing.T) {
	p := DefaultApplicationPolicy(1)
	contract := ApplicationContractForPolicy(p)
	contract.Version = 4
	owner := ApplicationRecoveryReservation{ID: [32]byte{2}, Digest: [32]byte{3}, Credits: []ApplicationRecoveryCredit{
		{Phase: 1, CommandBytes: 128, RetainedBytes: 8192, RetainedRecords: 8},
		{Phase: 2, CommandBytes: 128, RetainedBytes: 8192, RetainedRecords: 8},
	}}
	m := ApplicationSnapshotManifest{Version: 6, Identity: ApplicationIdentity{Graph: [16]byte{1}, Partition: 1, Group: [16]byte{2}}, SemanticContractID: ApplicationSemanticContractID{3}, Contract: contract, Index: 1, Term: 1, ConfState: &pb.ConfState{Voters: []uint64{1, 4, 7}}, Image: []byte{7}, NamespaceBytes: [4]uint64{0, 46, 45, 45}, NamespaceRecords: [4]uint64{0, 1, 1, 1}, TerminalReservations: []ApplicationRecoveryReservation{owner}, RecoveryRecords: 1}
	m.ImageHash = sha256.Sum256(m.Image)
	m.RecoveryBytes = uint64(9 + appFrameBytes + nativeRecoveryRootBytes(snapshotRecoveryState(m)))
	seal := func(m *ApplicationSnapshotManifest) {
		b, r, err := snapshotTotals(*m)
		if err != nil {
			t.Fatal(err)
		}
		m.CutID, err = cutID(ApplicationCutReference{Identity: m.Identity, SemanticContractID: m.SemanticContractID, Contract: m.Contract, Index: m.Index, Term: m.Term, ConfState: m.ConfState, ImageBytes: uint64(len(m.Image)), ImageHash: m.ImageHash, RetainedBytes: b, RetainedRecords: r, RecoveryBytes: m.RecoveryBytes, RecoveryRecords: m.RecoveryRecords, RecoveryHash: snapshotRecoveryHash(*m)})
		if err != nil {
			t.Fatal(err)
		}
	}
	seal(&m)
	wire, err := EncodeApplicationSnapshotManifest(m)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(wire[:4], []byte{'A', 'S', 6, 0}) {
		t.Fatal("fresh snapshot envelope")
	}
	decoded, err := DecodeApplicationSnapshotManifestForBinding(wire, p.MaxImageBytes, ApplicationBinding{Identity: m.Identity, SemanticContractID: m.SemanticContractID})
	if err != nil || decoded.CutID != m.CutID || !sameTerminalBank(applicationTerminalBank(decoded.TerminalReservations), applicationTerminalBank(m.TerminalReservations)) {
		t.Fatalf("exact AS6 binding: %v", err)
	}
	descriptor, err := EncodeApplicationSnapshotDescriptor(m)
	if err != nil || !bytes.Equal(descriptor[:4], []byte{'A', 'D', 5, 0}) {
		t.Fatalf("AD5 descriptor: %v", err)
	}
	owned := cloneSnapshotManifest(m)
	owned.TerminalReservations[0].Credits[0].RetainedBytes++
	if m.TerminalReservations[0].Credits[0].RetainedBytes != 8192 {
		t.Fatal("manifest clone borrowed owner credits")
	}
	if err := validateManifest(owned); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("bank not bound into cut ID: %v", err)
	}
	seal(&owned)
	changedDescriptor, err := EncodeApplicationSnapshotDescriptor(owned)
	if err != nil || bytes.Equal(changedDescriptor, descriptor) || publishedManifestID(owned) == publishedManifestID(m) {
		t.Fatalf("terminal bank missing descriptor/manifest identity: %v", err)
	}
	wrong := ApplicationBinding{Identity: m.Identity, SemanticContractID: ApplicationSemanticContractID{4}}
	if _, err := DecodeApplicationSnapshotManifestForBinding(wire, p.MaxImageBytes, wrong); !errors.Is(err, ErrInvalid) {
		t.Fatalf("foreign agreement: %v", err)
	}
	legacy := m
	legacy.Version = 5
	legacy.Contract.Version = 3
	if err := validateManifest(legacy); !errors.Is(err, ErrInvalid) {
		t.Fatalf("AS5 admitted terminal bank: %v", err)
	}
	legacy.TerminalReservations = nil
	legacy.RecoveryBytes = uint64(9 + appFrameBytes + recoveryRootFixedBytes)
	seal(&legacy)
	old, err := EncodeApplicationSnapshotManifest(legacy)
	if err != nil || !bytes.Equal(old[:4], []byte{'A', 'S', 5, 0}) || snapshotRecoveryHash(legacy) != recoveryRootDigest(legacy.RecoveryReservation) {
		t.Fatalf("legacy AS5 agreement: %v", err)
	}
	configured, err := ApplicationContractForPolicyWithRecovery(p, ApplicationControlConfig{Version: 1}, ApplicationRecoveryConfig{Version: 2})
	if err != nil || configured != contract {
		t.Fatalf("AS6 configured terminal contract: %v", err)
	}
}
