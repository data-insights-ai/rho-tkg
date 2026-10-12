package raftlog_test

import (
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

type receivingDriverMachine struct{ restores int }

func (*receivingDriverMachine) SemanticContractID() raftlog.ApplicationSemanticContractID {
	return raftlog.ApplicationSemanticContractID{41}
}
func (m *receivingDriverMachine) Restore(uint64, []byte) error { m.restores++; return nil }
func (*receivingDriverMachine) Stage(replica.Entry, raftlog.ApplicationBudget) (raftlog.ApplicationBatch, error) {
	return raftlog.ApplicationBatch{}, raftlog.ErrInvalid
}

func receivingDriverStore(t *testing.T) *raftlog.Store {
	t.Helper()
	p := raftlog.DefaultApplicationPolicy(1)
	c := raftlog.Config{Dir: "db", FS: vfs.NewMem(), Create: true, Application: p,
		Transfer:    raftlog.ApplicationTransferConfig{Identity: raftlog.ApplicationIdentity{Graph: [16]byte{1}, Partition: 7, Group: [16]byte{3}}, Contract: raftlog.ApplicationContractForPolicy(p), Limits: raftlog.DefaultApplicationTransferLimits()},
		Generations: raftlog.ApplicationGenerationLimits{MaxBytes: 2 << 30, MaxRecords: 2_000_000}, PublishedCuts: raftlog.ApplicationPublishedCutLimits{MaxTransferChunks: 10000},
		Replication: raftlog.ApplicationReplicationConfig{Voters: [3]uint64{1, 2, 3}}, SemanticContractID: raftlog.ApplicationSemanticContractID{41}}
	s, err := raftlog.Open(c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return s
}

func TestApplicationReceivingNormalDriverAndBootstrapStayClosedUntilActualActivation(t *testing.T) {
	s := receivingDriverStore(t)
	if err := s.PersistApplicationReceiving(raft.Ready{HardState: &pb.HardState{Term: new(uint64(2))}}); err != nil {
		t.Fatal(err)
	}
	machine := &receivingDriverMachine{}
	driverConfig := replica.Config{ID: 1, Store: s, ApplicationMachine: machine,
		ApplicationSnapshotSends: replica.ApplicationSnapshotSendLimits{MaxOffers: 2, MaxOwnedBytes: 4 << 20}}
	if d, err := replica.Open(driverConfig); d != nil || !errors.Is(err, replica.ErrInvalid) || machine.restores != 0 {
		t.Fatal("normal Driver admitted receiving store or restored before membership", d, err, machine.restores)
	}
	declaration, err := graphstore.NewOwnershipDeclaration(graphstate.GraphID{1}, 1,
		[]graphstore.PartitionOwnership{{Partition: 7, OwnershipEpoch: 1, Group: [16]byte{3}}}, graphstore.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if err := graphstore.BootstrapBoundOwnership(s, s.ApplicationBinding(), declaration, [3]uint64{1, 2, 3}); !errors.Is(err, graphstore.ErrInvalid) || !errors.Is(err, raftlog.ErrInvalid) {
		t.Fatal("old receiving identity was fresh-bootstrapped", err)
	}
	state, err := s.ApplicationReceivingState()
	if err != nil || state.Term != 2 {
		t.Fatal(state, err)
	}

	// Public Store fixture APIs build a valid retained donor. Only the actual
	// RawNode snapshot Ready below activates it; this is a unit-contract control,
	// not a replacement/quorum/retirement authorization demonstration.
	donor := receivingDriverStore(t)
	if err := donor.Initialize([]uint64{1, 2, 3}, []byte("seed")); err != nil {
		t.Fatal(err)
	}
	if err := donor.Persist(raft.Ready{HardState: &pb.HardState{Term: new(uint64(2)), Commit: new(uint64(2))},
		Entries: []*pb.Entry{{Index: new(uint64(2)), Term: new(uint64(2)), Type: pb.EntryNormal.Enum(), Data: []byte("snapshot")}}}); err != nil {
		t.Fatal(err)
	}
	if err := donor.InstallApplication(2, raftlog.ApplicationBatch{BaseGeneration: donor.ApplicationGeneration(), BaseIndex: 1, BaseImageHash: sha256.Sum256([]byte("seed")), Image: []byte("snapshot")}); err != nil {
		t.Fatal(err)
	}
	if err := donor.PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	cut, err := donor.PublishedApplicationCut()
	if err != nil {
		t.Fatal(err)
	}
	export, err := donor.BeginPublishedApplicationExport(t.Context(), cut.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer export.Close()
	done := false
	for range 100 {
		done, err = export.BuildManifest(t.Context(), raftlog.ReadBudget{Rows: 1, Bytes: 4096})
		if err != nil {
			t.Fatal(err)
		}
		if done {
			break
		}
	}
	if !done {
		t.Fatal("bounded donor manifest build")
	}
	manifest, err := export.Manifest()
	if err != nil {
		t.Fatal(err)
	}
	imported, err := s.BeginApplicationImport(t.Context(), manifest)
	if err != nil {
		t.Fatal(err)
	}
	defer imported.Abort()
	done = false
	for range 100 {
		chunk, err := export.Next(t.Context(), raftlog.ReadBudget{Rows: 1, Bytes: 4096})
		if err != nil {
			t.Fatal(err)
		}
		if err := imported.Append(t.Context(), chunk); err != nil {
			t.Fatal(err)
		}
		if chunk.Final {
			done = true
			break
		}
	}
	if !done {
		t.Fatal("bounded actual import pages")
	}
	if err := imported.Verify(t.Context()); err != nil {
		t.Fatal(err)
	}
	prepared, err := imported.Prepare()
	if err != nil {
		t.Fatal(err)
	}
	descriptor, err := raftlog.EncodeApplicationSnapshotDescriptor(manifest)
	if err != nil {
		t.Fatal(err)
	}
	snap := &pb.Snapshot{Data: descriptor, Metadata: &pb.SnapshotMetadata{Index: new(manifest.Index), Term: new(manifest.Term), ConfState: manifest.ConfState}}
	claim, err := prepared.Claim(snap)
	if err != nil {
		t.Fatal(err)
	}
	defer claim.Close()
	raw, err := raft.NewRawNode(&raft.Config{ID: 1, Storage: s, ElectionTick: 10, HeartbeatTick: 1, MaxInflightMsgs: 2, MaxSizePerMsg: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if err := raw.Step(&pb.Message{Type: pb.MsgSnap.Enum(), From: new(uint64(2)), To: new(uint64(1)), Term: new(uint64(2)), Snapshot: snap}); err != nil {
		t.Fatal(err)
	}
	rd := raw.Ready()
	if raft.IsEmptySnap(rd.Snapshot) {
		t.Fatal("actual snapshot Ready absent")
	}
	if _, err := s.PersistApplicationReady(rd, claim); err != nil {
		t.Fatal(err)
	}
	raw.Advance(rd)
	if err := claim.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ApplicationReceivingState(); !errors.Is(err, raftlog.ErrInvalid) {
		t.Fatal("activation retained receiving eligibility", err)
	}
	d, err := replica.Open(driverConfig)
	if err != nil || d == nil || machine.restores != 1 || d.Applied() != manifest.Index {
		t.Fatal("normal Driver did not use activated membership/checkpoint", d, err, machine.restores)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
}
