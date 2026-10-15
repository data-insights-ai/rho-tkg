package raftlog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

type spanRecoveryVerifier struct {
	reserve, terminal ApplicationRecoveryReservation
}

func (v spanRecoveryVerifier) VerifyApplicationRecovery(_ context.Context, data []byte, _ *ApplicationView) (ApplicationRecoveryPurpose, error) {
	if string(data) == "register" {
		return ApplicationRecoveryPurpose{Reserve: &v.reserve}, nil
	}
	if string(data) == "ticket" {
		return ApplicationRecoveryPurpose{TerminalReserve: &v.terminal}, nil
	}
	phase := uint32(100)
	if string(data) == "next" {
		phase = 101
	} else if string(data) == "done" {
		phase = 300
	} else if string(data) != "step" {
		return ApplicationRecoveryPurpose{}, ErrInvalid
	}
	use := ApplicationRecoveryUse{ID: v.reserve.ID, Digest: v.reserve.Digest, CommandDigest: sha256.Sum256(data), Phase: phase}
	if phase == 300 {
		use.RetireBefore = phase
	}
	return ApplicationRecoveryPurpose{Use: &use}, nil
}
func spanRecoveryStore(t *testing.T) (*Store, Config, spanRecoveryVerifier) {
	t.Helper()
	p, tc := transferConfig(1)
	c, r, ret := ApplicationControlConfig{1}, ApplicationRecoveryConfig{3}, ApplicationRetentionConfig{1}
	var err error
	tc.Contract, err = ApplicationContractForPolicyWithRetention(p, c, r, ret)
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{Dir: "span", FS: vfs.NewMem(), Create: true, Application: p, Controls: c, Recovery: r, Retention: ret, Transfer: tc, Generations: ApplicationGenerationLimits{2 << 30, 2_000_000}, PublishedCuts: ApplicationPublishedCutLimits{10000}, Replication: ApplicationReplicationConfig{[3]uint64{1, 2, 3}}, SemanticContractID: ApplicationSemanticContractID{61}}
	s, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := s.Initialize([]uint64{1, 2, 3}, []byte("seed")); err != nil {
		t.Fatal(err)
	}
	reservation := recoverySpanFixture()
	for i := range reservation.Spans {
		reservation.Spans[i].CommandBytes = 128
		reservation.Spans[i].RetainedBytes = 8192
		reservation.Spans[i].RetainedRecords = 16
	}
	verifier := spanRecoveryVerifier{reserve: reservation, terminal: ApplicationRecoveryReservation{ID: [32]byte{8}, Digest: [32]byte{9}, Credits: []ApplicationRecoveryCredit{{1, 128, 8192, 16}}}}
	if err := s.BindApplicationRecoveryVerifier(s.ApplicationBinding(), verifier); err != nil {
		t.Fatal(err)
	}
	return s, cfg, verifier
}
func spanRecoveryInstall(t *testing.T, s *Store, v spanRecoveryVerifier, data string) {
	t.Helper()
	base, image, err := s.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	claim, err := s.AdmitApplicationRecovery(t.Context(), []byte(data))
	if err != nil {
		t.Fatal(err)
	}
	purpose, err := v.VerifyApplicationRecovery(t.Context(), []byte(data), nil)
	if err != nil {
		t.Fatal(err)
	}
	persist(t, s, 2, base+1, ent(base+1, 2, data))
	if err := s.InstallApplication(base+1, ApplicationBatch{BaseGeneration: s.ApplicationGeneration(), BaseIndex: base, BaseImageHash: sha256.Sum256(image), Image: image, RecoveryReserve: purpose.Reserve, RecoveryUse: purpose.Use, TerminalReserve: purpose.TerminalReserve, RecoveryAdmission: claim}); err != nil {
		t.Fatal(err)
	}
}

// Authored actual native Store/RawNode lifecycle; execution/qualification is
// deliberately deferred. No helper receipt claims graph handoff acceptance.
func TestApplicationRecoverySpansReopenHistoryPortableAndSnapshotReady(t *testing.T) {
	donor, cfg, v := spanRecoveryStore(t)
	// Reopen a persisted but unapplied reservation: recovery cannot infer an
	// already-installed owner or manufacture completion capacity after restart.
	persist(t, donor, 2, 2, ent(2, 2, "register"))
	if err := donor.Close(); err != nil {
		t.Fatal(err)
	}
	cfg.Create = false
	var err error
	donor, err = Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer donor.Close()
	if donor.meta.Applied != 1 || len(donor.meta.Recovery.Queued) != 1 || donor.ApplicationRecovery().Version != 3 {
		t.Fatal("queued fresh identity lost")
	}
	if err := donor.BindApplicationRecoveryVerifier(donor.ApplicationBinding(), v); err != nil {
		t.Fatal(err)
	}
	if err := donor.InstallApplication(2, ApplicationBatch{BaseGeneration: donor.ApplicationGeneration(), BaseIndex: 1, BaseImageHash: sha256.Sum256([]byte("seed")), Image: []byte("seed"), RecoveryReserve: &v.reserve}); err != nil {
		t.Fatal(err)
	}
	old, err := donor.ApplicationView(2)
	if err != nil {
		t.Fatal(err)
	}
	spanRecoveryInstall(t, donor, v, "ticket")
	spanRecoveryInstall(t, donor, v, "step")
	before, err := donor.loadRecoveryRootAt(donor.activeBank(), 2)
	if err != nil || before.Spans[0].Next != 100 {
		t.Fatal("old owner substituted", before, err)
	}
	owner, _, err := old.RecoveryOwner(t.Context(), ReadBudget{1, 4096})
	if err != nil || owner != (ApplicationRecoveryOwner{v.reserve.ID, v.reserve.Digest}) {
		t.Fatal(owner, err)
	}
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	if donor.meta.Recovery.Reservation.Spans[0].Next != 101 || len(donor.meta.Recovery.Terminal) != 1 {
		t.Fatal("spent step or terminal bank lost")
	}
	if err := donor.PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	cut, err := donor.PublishedApplicationCut()
	if err != nil {
		t.Fatal(err)
	}
	spanRecoveryInstall(t, donor, v, "next") // Later installed state MUST NOT enter C.
	export, err := donor.BeginPublishedApplicationExport(t.Context(), cut.ID)
	if err != nil {
		t.Fatal(err)
	}
	manifest := buildPublished(t, export, ReadBudget{1, 4096})
	chunks := collectPublished(t, export, ReadBudget{1, 4096})
	if err := export.Close(); err != nil {
		t.Fatal(err)
	}
	if manifest.Version != 8 || manifest.Contract.Version != 6 || manifest.RecoveryReservation.Spans[0].Next != 101 || !sameTerminalBank(applicationTerminalBank(manifest.TerminalReservations), applicationTerminalBank{v.terminal}) {
		t.Fatal("published recovery changed with live root")
	}
	wire, err := EncodeApplicationSnapshotManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeApplicationSnapshotManifestForBinding(wire, cfg.Application.MaxImageBytes, donor.ApplicationBinding())
	if err != nil || !sameRecoveryReservation(decoded.RecoveryReservation, manifest.RecoveryReservation) {
		t.Fatal(decoded, err)
	}
	owned := cloneSnapshotManifest(decoded)
	owned.RecoveryReservation.Spans[0].Next++
	if decoded.RecoveryReservation.Spans[0].Next != 101 {
		t.Fatal("portable manifest borrowed spans")
	}
	if err := validateManifest(owned); !errors.Is(err, ErrCorrupt) {
		t.Fatal("span not bound to cut", err)
	}
	mixed := bytes.Clone(wire)
	mixed[2] = 7
	if _, err := DecodeApplicationSnapshotManifest(mixed, cfg.Application.MaxImageBytes); err == nil {
		t.Fatal("AS7 accepted RS3")
	}
	receiver, rcfg, rv := spanRecoveryStore(t)
	prepared, snapshot := semanticPrepared(t, receiver, manifest, chunks)
	if !bytes.Equal(snapshot.GetData()[:4], []byte{'A', 'D', 7, 0}) {
		t.Fatal("fresh descriptor")
	}
	view, err := prepared.OpenReadView(t.Context(), manifest.Index, cfg.Application.MaxImageBytes)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := view.RecoveryOwner(t.Context(), ReadBudget{1, 4096}); err != nil {
		t.Fatal(err)
	}
	if equal, _, err := view.CheckTerminalBank(t.Context(), manifest.TerminalReservations, ReadBudget{1, 4096}); err != nil || !equal {
		t.Fatal(equal, err)
	}
	if err := view.Close(); err != nil {
		t.Fatal(err)
	}
	claim, err := prepared.Claim(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := raft.NewRawNode(&raft.Config{ID: 1, Storage: receiver, Applied: receiver.meta.Applied, ElectionTick: 10, HeartbeatTick: 1, MaxSizePerMsg: 1 << 20, MaxCommittedSizePerReady: 1 << 20, MaxInflightMsgs: 16})
	if err != nil {
		t.Fatal(err)
	}
	if err := raw.Step(&pb.Message{Type: pb.MsgSnap.Enum(), From: new(uint64(2)), To: new(uint64(1)), Term: new(uint64(5)), Snapshot: proto.Clone(snapshot).(*pb.Snapshot)}); err != nil {
		t.Fatal(err)
	}
	if err := raw.Step(&pb.Message{Type: pb.MsgApp.Enum(), From: new(uint64(2)), To: new(uint64(1)), Term: new(uint64(5)), Index: new(manifest.Index), LogTerm: new(manifest.Term), Commit: new(manifest.Index + 1), Entries: []*pb.Entry{ent(manifest.Index+1, manifest.Term, "next")}}); err != nil {
		t.Fatal(err)
	}
	ready := raw.Ready()
	if raft.IsEmptySnap(ready.Snapshot) || len(ready.Entries) != 1 {
		t.Fatal("missing actual incoming snapshot suffix")
	}
	root, err := receiver.PersistApplicationReady(ready, claim)
	if err != nil {
		t.Fatal(err)
	}
	raw.Advance(ready)
	if err := claim.Close(); err != nil {
		t.Fatal(err)
	}
	if root.Index != manifest.Index || receiver.meta.Recovery.Reservation.Spans[0].Next != 101 {
		t.Fatal("activation spent future recipe")
	}
	pending, err := receiver.RecoveryAdmissionForCommittedEntry(t.Context(), manifest.Index+1)
	if err != nil || pending == nil {
		t.Fatal(pending, err)
	}
	purpose, err := rv.VerifyApplicationRecovery(t.Context(), []byte("next"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := receiver.InstallApplication(manifest.Index+1, ApplicationBatch{BaseGeneration: root.Generation, BaseIndex: root.Index, BaseImageHash: root.ImageHash, Image: root.Image, RecoveryUse: purpose.Use, RecoveryAdmission: pending}); err != nil {
		t.Fatal(err)
	}
	if receiver.meta.Recovery.Reservation.Spans[0].Next != 102 || len(receiver.meta.Recovery.Terminal) != 1 {
		t.Fatal("suffix double spent or terminal bank lost")
	}
	if err := receiver.ScrubApplication(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := receiver.Close(); err != nil {
		t.Fatal(err)
	}
	rcfg.Create = false
	reopened, err := Open(rcfg)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if reopened.meta.Recovery.Reservation.Spans[0].Next != 102 {
		t.Fatal("reopen reset remaining span")
	}
	if err := reopened.BindApplicationRecoveryVerifier(reopened.ApplicationBinding(), rv); err != nil {
		t.Fatal(err)
	}
	spanRecoveryInstall(t, reopened, rv, "done")
	if recoveryEntries(reopened.meta.Recovery.Reservation) != 0 || len(reopened.meta.Recovery.Terminal) != 1 {
		t.Fatal("terminal failed exact suffix retirement or consumed independent bank")
	}
	wrong := rcfg
	wrong.Recovery.Version = 2
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	if store, err := Open(wrong); !errors.Is(err, ErrInvalid) || store != nil {
		if store != nil {
			_ = store.Close()
		}
		t.Fatal("silent store profile downgrade", err)
	}
}
