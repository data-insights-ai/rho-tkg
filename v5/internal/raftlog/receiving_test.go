package raftlog

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func receivingTestConfig(t *testing.T, fs vfs.FS, recovery bool) Config {
	t.Helper()
	p, transfer := transferConfig(1)
	c := Config{Dir: "db", FS: fs, Create: true, Application: p, Transfer: transfer,
		Generations: ApplicationGenerationLimits{2 << 30, 2_000_000}, PublishedCuts: ApplicationPublishedCutLimits{10000},
		Replication: ApplicationReplicationConfig{Voters: [3]uint64{1, 2, 3}}, SemanticContractID: ApplicationSemanticContractID{41}}
	if recovery {
		c.Controls, c.Recovery = ApplicationControlConfig{Version: 1}, ApplicationRecoveryConfig{Version: 1}
		var err error
		c.Transfer.Contract, err = ApplicationContractForPolicyWithRecovery(p, c.Controls, c.Recovery)
		if err != nil {
			t.Fatal(err)
		}
	}
	return c
}

func receivingTestOpen(t *testing.T, c Config) *Store {
	t.Helper()
	s, err := Open(c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil && s.poison == nil {
			t.Error(err)
		}
	})
	return s
}

func receivingTerm(term uint64) raft.Ready {
	return raft.Ready{HardState: &pb.HardState{Term: new(term)}}
}

func receivingRawNode(t *testing.T, s *Store) *raft.RawNode {
	t.Helper()
	raw, err := raft.NewRawNode(&raft.Config{ID: 1, Storage: s, ElectionTick: 10, HeartbeatTick: 1,
		MaxSizePerMsg: 1 << 20, MaxInflightMsgs: 2, CheckQuorum: true, PreVote: true})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestApplicationReceivingActualReadyDurableTermAndReopen(t *testing.T) {
	fs := vfs.NewCrashableMem()
	c := receivingTestConfig(t, fs, false)
	s := receivingTestOpen(t, c)
	state, err := s.ApplicationReceivingState()
	if err != nil || state.Binding != s.ApplicationBinding() || state.Voters != ([3]uint64{1, 2, 3}) || state.LocalVoter != 1 || state.Term != 0 {
		t.Fatal(state, err)
	}
	state.Binding.Identity.Graph[0]++
	state.Voters[0] = 99
	again, err := s.ApplicationReceivingState()
	if err != nil || again.Binding != s.ApplicationBinding() || again.Voters[0] != 1 {
		t.Fatal("receiving output aliases configuration", again, err)
	}
	raw := receivingRawNode(t, s)
	// An actual empty-membership RawNode rejects this ordinary missing-prefix
	// append. Its Ready is genuine, but this unit input is not a quorum proof.
	if err := raw.Step(&pb.Message{Type: pb.MsgApp.Enum(), From: new(uint64(2)), To: new(uint64(1)), Term: new(uint64(7)), Index: new(uint64(4)), LogTerm: new(uint64(7))}); err != nil {
		t.Fatal(err)
	}
	rd := raw.Ready()
	if rd.GetTerm() != 7 || rd.GetVote() != 0 || rd.GetCommit() != 0 || len(rd.Entries) != 0 || len(rd.CommittedEntries) != 0 || !raft.IsEmptySnap(rd.Snapshot) {
		t.Fatal("unexpected actual receiving Ready", rd)
	}
	if len(rd.Messages) != 1 || rd.Messages[0].GetType() != pb.MsgAppResp || !rd.Messages[0].GetReject() {
		t.Fatal("genuine reject response absent", rd.Messages)
	}
	owned := proto.Clone(rd.HardState)
	if err := s.Persist(rd); !errors.Is(err, ErrInvalid) {
		t.Fatal("ordinary Persist app0 gate weakened", err)
	}
	if err := s.PersistApplicationReceiving(rd); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(rd.HardState, owned) {
		t.Fatal("actual HardState mutated")
	}
	raw.Advance(rd)
	if err := raw.Campaign(); err != nil {
		t.Fatal(err)
	}
	if status := raw.BasicStatus(); status.GetTerm() != 7 || status.GetVote() != 0 || status.GetCommit() != 0 {
		t.Fatal("empty membership campaigned", status)
	}
	if err := s.Initialize([]uint64{1, 2, 3}, nil); !errors.Is(err, ErrInvalid) {
		t.Fatal("term-only state bootstrapped old identity", err)
	}
	if err := s.PersistApplicationReceiving(receivingTerm(7)); err != nil {
		t.Fatal("same term refused", err)
	}
	if err := s.PersistApplicationReceiving(receivingTerm(9)); err != nil {
		t.Fatal(err)
	}
	c.Create, c.FS = false, fs.CrashClone(vfs.CrashCloneCfg{})
	reopened := receivingTestOpen(t, c)
	state, err = reopened.ApplicationReceivingState()
	hard, conf, stateErr := reopened.InitialState()
	if err != nil || stateErr != nil || state.Term != 9 || hard.GetTerm() != 9 || hard.GetVote() != 0 || hard.GetCommit() != 0 || !receivingConfEmpty(conf) {
		t.Fatal("term not durably retained", state, hard, conf, err, stateErr)
	}
	if index, image, err := reopened.Checkpoint(); err != nil || index != 0 || len(image) != 0 {
		t.Fatal(index, image, err)
	}
	if err := reopened.Initialize([]uint64{1, 2, 3}, nil); !errors.Is(err, ErrInvalid) {
		t.Fatal("reopen enabled Initialize", err)
	}
}

func TestApplicationReceivingNilLifetimeAndProfileRefusals(t *testing.T) {
	var nilStore *Store
	if state, err := nilStore.ApplicationReceivingState(); state != (ApplicationReceivingState{}) || !errors.Is(err, ErrInvalid) {
		t.Fatal(state, err)
	}
	if err := nilStore.PersistApplicationReceiving(receivingTerm(1)); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	for _, name := range []string{"scalar", "unbound", "singleton", "initialized", "closed", "poisoned"} {
		t.Run(name, func(t *testing.T) {
			c := receivingTestConfig(t, vfs.NewMem(), false)
			sentinel := errors.New("existing uncertain cause")
			switch name {
			case "scalar":
				c = Config{Dir: "scalar", FS: vfs.NewMem(), Create: true}
			case "unbound":
				c.SemanticContractID = ApplicationSemanticContractID{}
			case "singleton":
				c.Replication, c.SemanticContractID = ApplicationReplicationConfig{}, ApplicationSemanticContractID{}
			}
			s := receivingTestOpen(t, c)
			want := ErrInvalid
			switch name {
			case "initialized":
				if err := s.Initialize([]uint64{1, 2, 3}, nil); err != nil {
					t.Fatal(err)
				}
			case "closed":
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
				want = ErrClosed
			case "poisoned":
				s.poison = sentinel
				want = ErrPoisoned
			}
			if state, err := s.ApplicationReceivingState(); state != (ApplicationReceivingState{}) || !errors.Is(err, want) || name == "poisoned" && !errors.Is(err, sentinel) {
				t.Fatal(state, err)
			}
			if err := s.PersistApplicationReceiving(receivingTerm(2)); !errors.Is(err, want) || name == "poisoned" && !errors.Is(err, sentinel) {
				t.Fatal(err)
			}
		})
	}
}

func TestApplicationReceivingReadyRefusalsDoNotMutate(t *testing.T) {
	// Privileged malformed Ready fixtures exercise input validation, not Raft
	// authority or a delivered authenticated packet.
	for _, name := range []string{"rollback", "zero-rollback", "vote", "commit", "entry", "nil-entry", "committed", "snapshot", "index0-data", "snapshot-term", "snapshot-conf", "hard-unknown", "snapshot-unknown", "metadata-unknown"} {
		t.Run(name, func(t *testing.T) {
			s := receivingTestOpen(t, receivingTestConfig(t, vfs.NewMem(), false))
			if err := s.PersistApplicationReceiving(receivingTerm(5)); err != nil {
				t.Fatal(err)
			}
			rd := receivingTerm(6)
			switch name {
			case "rollback":
				rd.Term = new(uint64(4))
			case "zero-rollback":
				rd.Term = new(uint64(0))
			case "vote":
				rd.Vote = new(uint64(2))
			case "commit":
				rd.Commit = new(uint64(1))
			case "entry":
				rd.Entries = []*pb.Entry{ent(1, 6, "unexpected")}
			case "nil-entry":
				rd.Entries = []*pb.Entry{nil}
			case "committed":
				rd.CommittedEntries = []*pb.Entry{ent(1, 6, "unexpected")}
			case "snapshot":
				rd.Snapshot = &pb.Snapshot{Metadata: &pb.SnapshotMetadata{Index: new(uint64(1)), Term: new(uint64(1))}}
			case "index0-data":
				rd.Snapshot = &pb.Snapshot{Data: []byte("not empty")}
			case "snapshot-term":
				rd.Snapshot = &pb.Snapshot{Metadata: &pb.SnapshotMetadata{Term: new(uint64(1))}}
			case "snapshot-conf":
				rd.Snapshot = &pb.Snapshot{Metadata: &pb.SnapshotMetadata{ConfState: &pb.ConfState{Voters: []uint64{1}}}}
			case "hard-unknown":
				rd.HardState.ProtoReflect().SetUnknown([]byte{0x80, 0x01, 0x01})
			case "snapshot-unknown":
				rd.Snapshot = &pb.Snapshot{}
				rd.Snapshot.ProtoReflect().SetUnknown([]byte{0x80, 0x01, 0x01})
			case "metadata-unknown":
				rd.Snapshot = &pb.Snapshot{Metadata: &pb.SnapshotMetadata{}}
				rd.Snapshot.Metadata.ProtoReflect().SetUnknown([]byte{0x80, 0x01, 0x01})
			}
			before := readyMetadataFingerprint(t, s)
			if err := s.PersistApplicationReceiving(rd); !errors.Is(err, ErrInvalid) {
				t.Fatal(err)
			}
			if before != readyMetadataFingerprint(t, s) || s.poison != nil {
				t.Fatal("ordinary refusal mutated or poisoned")
			}
			if state, err := s.ApplicationReceivingState(); err != nil || state.Term != 5 {
				t.Fatal(state, err)
			}
		})
	}
	s := receivingTestOpen(t, receivingTestConfig(t, vfs.NewMem(), false))
	if err := s.PersistApplicationReceiving(receivingTerm(5)); err != nil {
		t.Fatal(err)
	}
	before := readyMetadataFingerprint(t, s)
	for _, rd := range []raft.Ready{{}, {HardState: &pb.HardState{}}} {
		if err := s.PersistApplicationReceiving(rd); err != nil {
			t.Fatal("checked no-op", err)
		}
	}
	if before != readyMetadataFingerprint(t, s) {
		t.Fatal("absent HardState reset term")
	}
}

func TestApplicationReceivingOnlyTermMetadataChangesAndProgressFence(t *testing.T) {
	s := receivingTestOpen(t, receivingTestConfig(t, vfs.NewMem(), true))
	before := s.meta
	rd := receivingTerm(7)
	if err := s.PersistApplicationReceiving(rd); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before.Gen, s.meta.Gen) || !reflect.DeepEqual(before.App, s.meta.App) || !reflect.DeepEqual(before.Recovery, s.meta.Recovery) || !reflect.DeepEqual(before.Controls, s.meta.Controls) || !reflect.DeepEqual(before.Rep, s.meta.Rep) || !proto.Equal(before.Conf, s.meta.Conf) || !proto.Equal(before.Snap, s.meta.Snap) {
		t.Fatal("term-only persistence changed active placement/ledgers")
	}
	rd.Term = new(uint64(99))
	if state, err := s.ApplicationReceivingState(); err != nil || state.Term != 7 {
		t.Fatal("caller pointer retained", state, err)
	}
	p, err := s.BeginApplicationReadyProgress(s.ApplicationBinding(), raft.Ready{}, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.ApplicationReceivingState(); !errors.Is(err, ErrInvalid) {
		t.Fatal("receiving inspection crossed progress owner", err)
	}
	if err := s.PersistApplicationReceiving(receivingTerm(8)); !errors.Is(err, ErrInvalid) {
		t.Fatal("receiving write crossed progress owner", err)
	}
	if ok, err := p.PersistFull(); err != nil || !ok {
		t.Fatal(ok, err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.PersistApplicationReceiving(receivingTerm(8)); err != nil {
		t.Fatal(err)
	}
}

func TestApplicationReceivingRejectsActiveLedgersAndInvalidGeneration(t *testing.T) {
	// Privileged metadata mutations test the eligibility predicate; they supply
	// no actual consensus, old-identity retirement or replacement authorization.
	for _, name := range []string{"app-ledger", "controls", "recovery-owner", "queued", "publication", "audit", "generation", "local-voter", "hard-unknown"} {
		t.Run(name, func(t *testing.T) {
			s := receivingTestOpen(t, receivingTestConfig(t, vfs.NewMem(), true))
			switch name {
			case "app-ledger":
				s.meta.App.Records = 1
			case "controls":
				s.meta.Controls.Bytes = 1
			case "recovery-owner":
				s.meta.Recovery.Reservation.ID = [32]byte{1}
			case "queued":
				s.meta.Recovery.Queued = []applicationRecoveryQueued{{Index: 1}}
			case "publication":
				s.meta.Gen.Publication.Valid = true
			case "audit":
				s.meta.Rep.LastActivatedManifestID = [32]byte{1}
			case "generation":
				s.meta.Gen.HighWater = 0
			case "local-voter":
				s.meta.App.Policy.LocalVoter = 4
			case "hard-unknown":
				s.meta.Hard.ProtoReflect().SetUnknown([]byte{0x80, 0x01, 0x01})
			}
			before := readyMetadataFingerprint(t, s)
			if _, err := s.ApplicationReceivingState(); !errors.Is(err, ErrInvalid) {
				t.Fatal("invalid active metadata admitted", name, err)
			}
			if err := s.PersistApplicationReceiving(receivingTerm(5)); !errors.Is(err, ErrInvalid) {
				t.Fatal("invalid active metadata persisted", name, err)
			}
			if before != readyMetadataFingerprint(t, s) || s.poison != nil {
				t.Fatal("metadata eligibility refusal mutated or poisoned")
			}
		})
	}
}

func TestApplicationReceivingRejectsOrphanActiveBytes(t *testing.T) {
	// These raw writes are deliberate corruption fixtures. They never create
	// accepted consensus state or a receiving/activation capability.
	for _, name := range []string{"log", "KV", "root", "changes", "outcome", "control-disabled", "recovery-disabled", "image", "snapshot-image"} {
		t.Run(name, func(t *testing.T) {
			s := receivingTestOpen(t, receivingTestConfig(t, vfs.NewMem(), false))
			key := imageKey
			switch name {
			case "log":
				key = entryKey(1)
			case "KV":
				key = bankVersionKey(s.activeBank(), []byte("hidden"), 2)
			case "root":
				key = bankIndexKey(s.activeBank(), appRootTag, 1)
			case "changes":
				key = bankIndexKey(s.activeBank(), appChangeTag, 1)
			case "outcome":
				key = bankIndexKey(s.activeBank(), appOutcomeTag, 1)
			case "control-disabled":
				key = controlVersionKey(s.activeBank(), []byte("hidden"), 2)
			case "recovery-disabled":
				key = appIndexKey(recoveryRootTag+s.activeBank(), 1)
			case "snapshot-image":
				key = snapshotKey
			}
			if err := s.db.Set(key, []byte("orphan"), pebble.Sync); err != nil {
				t.Fatal(err)
			}
			before := readyMetadataFingerprint(t, s)
			if _, err := s.ApplicationReceivingState(); !errors.Is(err, ErrCorrupt) {
				t.Fatal("orphan accepted", err)
			}
			if err := s.PersistApplicationReceiving(receivingTerm(5)); !errors.Is(err, ErrPoisoned) || !errors.Is(err, ErrCorrupt) {
				t.Fatal("verified corruption was retried", err)
			}
			if before != readyMetadataFingerprint(t, s) {
				t.Fatal("corruption refusal wrote")
			}
		})
	}
}

func receivingTestImport(t *testing.T, target *Store, recovery bool) (*ApplicationImport, *PreparedApplicationSnapshot, *pb.Snapshot) {
	t.Helper()
	donor := receivingTestOpen(t, receivingTestConfig(t, vfs.NewMem(), recovery))
	if err := donor.Initialize([]uint64{1, 2, 3}, []byte("seed")); err != nil {
		t.Fatal(err)
	}
	persist(t, donor, 2, 2, ent(2, 2, "snapshot"))
	b := ApplicationBatch{BaseGeneration: donor.ApplicationGeneration(), BaseIndex: 1, BaseImageHash: sha256.Sum256([]byte("seed")), Image: []byte("snapshot")}
	if recovery {
		b.RecoveryReserve = &ApplicationRecoveryReservation{ID: [32]byte{1}, Digest: [32]byte{2}, Credits: []ApplicationRecoveryCredit{{Phase: 1, CommandBytes: 6, RetainedBytes: 1000, RetainedRecords: 4}}}
	}
	if err := donor.InstallApplication(2, b); err != nil {
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
	manifest := buildPublished(t, export, ReadBudget{1, 4096})
	i, err := target.BeginApplicationImport(t.Context(), manifest)
	if err != nil {
		t.Fatal(err)
	}
	checkTermOnly := func() {
		status, err := i.Status()
		if err != nil {
			t.Fatal(err)
		}
		gen, staged := target.meta.Gen, target.meta.Recovery.Staged
		if err := target.PersistApplicationReceiving(receivingTerm(2)); err != nil {
			t.Fatal("live staging cursor refused", err)
		}
		if after, err := i.Status(); err != nil || after != status || target.meta.Gen != gen || target.meta.Recovery.Staged != staged {
			t.Fatal("term persistence changed staging cursor/reservation", after, status, err)
		}
	}
	checkTermOnly()
	for _, chunk := range collectPublished(t, export, ReadBudget{1, 4096}) {
		if err := i.Append(t.Context(), chunk); err != nil {
			t.Fatal(err)
		}
		checkTermOnly()
	}
	if err := i.Verify(t.Context()); err != nil {
		t.Fatal(err)
	}
	checkTermOnly()
	p, err := i.Prepare()
	if err != nil {
		t.Fatal(err)
	}
	descriptor, err := EncodeApplicationSnapshotDescriptor(manifest)
	if err != nil {
		t.Fatal(err)
	}
	return i, p, &pb.Snapshot{Data: descriptor, Metadata: &pb.SnapshotMetadata{Index: new(manifest.Index), Term: new(manifest.Term), ConfState: manifest.ConfState}}
}

func TestApplicationReceivingPreservesInactiveAS3AS5Lifetimes(t *testing.T) {
	for _, recovery := range []bool{false, true} {
		t.Run(fmt.Sprint(recovery), func(t *testing.T) {
			fs := vfs.NewCrashableMem()
			cfg := receivingTestConfig(t, fs, recovery)
			s := receivingTestOpen(t, cfg)
			i, p, snap := receivingTestImport(t, s, recovery)
			status, err := i.Status()
			if err != nil {
				t.Fatal(err)
			}
			view, err := p.OpenReadView(t.Context(), 2, 64)
			if err != nil {
				t.Fatal(err)
			}
			beforeGen, beforeStaged, pins, views := s.meta.Gen, s.meta.Recovery.Staged, s.pinnedApplicationBytes, s.views
			if recovery && beforeStaged.bytes == 0 {
				t.Fatal("AS5 protected staging allowance absent")
			}
			if err := s.PersistApplicationReceiving(receivingTerm(2)); err != nil {
				t.Fatal("staging view wrongly treated as active state", err)
			}
			if s.meta.Gen != beforeGen || s.meta.Recovery.Staged != beforeStaged || s.pinnedApplicationBytes != pins || s.views != views {
				t.Fatal("term write changed inactive ownership")
			}
			if root, err := view.Root(); err != nil || string(root.Image) != "snapshot" {
				t.Fatal(root, err)
			}
			if err := view.Close(); err != nil {
				t.Fatal(err)
			}
			claim, err := p.Claim(snap)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.PersistApplicationReceiving(receivingTerm(2)); err != nil || claim.consumed {
				t.Fatal("claim changed by receiving term", err)
			}
			if after, err := i.Status(); err != nil || after != status {
				t.Fatal("import cursor changed", after, status, err)
			}
			if err := i.Abort(); !errors.Is(err, ErrLimit) {
				t.Fatal("term persistence revoked actual claim", err)
			}
			if err := claim.Close(); err != nil {
				t.Fatal(err)
			}
			cfg.Create, cfg.FS = false, fs.CrashClone(vfs.CrashCloneCfg{})
			reopened := receivingTestOpen(t, cfg)
			if state, err := reopened.ApplicationReceivingState(); err != nil || state.Term != 2 || reopened.applicationImport != nil || reopened.pinnedApplicationBytes != 0 || reopened.meta.Recovery.Staged != (applicationRecoveryGrowth{}) {
				t.Fatal("reopen resurrected staging or lost term", state, err)
			}
			if err := p.Close(); err != nil {
				t.Fatal(err)
			}
			if state, err := s.ApplicationReceivingState(); err != nil || state.Term != 2 {
				t.Fatal("Abort lost receiving state", state, err)
			}
		})
	}
}

func TestApplicationReceivingEndsOnlyAfterGenuineSnapshotReadyActivation(t *testing.T) {
	s := receivingTestOpen(t, receivingTestConfig(t, vfs.NewMem(), false))
	i, p, snap := receivingTestImport(t, s, false)
	defer i.Abort()
	if err := s.PersistApplicationReceiving(receivingTerm(2)); err != nil {
		t.Fatal(err)
	}
	claim, err := p.Claim(snap)
	if err != nil {
		t.Fatal(err)
	}
	defer claim.Close()
	raw := receivingRawNode(t, s)
	if err := raw.Step(&pb.Message{Type: pb.MsgSnap.Enum(), From: new(uint64(2)), To: new(uint64(1)), Term: new(uint64(2)), Snapshot: snap}); err != nil {
		t.Fatal(err)
	}
	rd := raw.Ready()
	if raft.IsEmptySnap(rd.Snapshot) {
		t.Fatal("actual native snapshot Ready absent")
	}
	if err := s.PersistApplicationReceiving(rd); !errors.Is(err, ErrInvalid) {
		t.Fatal("receiving primitive activated snapshot", err)
	}
	root, err := s.PersistApplicationReady(rd, claim)
	if err != nil || root.Index != 2 || !bytes.Equal(root.Image, []byte("snapshot")) {
		t.Fatal(root, err)
	}
	raw.Advance(rd)
	if _, err := s.ApplicationReceivingState(); !errors.Is(err, ErrInvalid) {
		t.Fatal("active snapshot remained receiving", err)
	}
	if err := s.PersistApplicationReceiving(receivingTerm(3)); !errors.Is(err, ErrInvalid) {
		t.Fatal("receiving write after activation", err)
	}
}
