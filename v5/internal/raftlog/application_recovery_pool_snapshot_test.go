package raftlog

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func recoveryPoolStore(t *testing.T) (*Store, Config, spanRecoveryVerifier) {
	t.Helper()
	p, tc := transferConfig(1)
	p.MaxTailEntries = 8 // explicit small fixture window; no default quota change.
	c, r, retention := ApplicationControlConfig{1}, ApplicationRecoveryConfig{4}, ApplicationRetentionConfig{1}
	var err error
	tc.Contract, err = ApplicationContractForPolicyWithRetention(p, c, r, retention)
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{Dir: "pool", FS: vfs.NewMem(), Create: true, Application: p, Controls: c, Recovery: r, Retention: retention, Transfer: tc, Generations: ApplicationGenerationLimits{2 << 30, 2_000_000}, PublishedCuts: ApplicationPublishedCutLimits{10000}, Replication: ApplicationReplicationConfig{[3]uint64{1, 2, 3}}, SemanticContractID: ApplicationSemanticContractID{62}}
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
	rsv := ApplicationRecoveryReservation{ID: [32]byte{1}, Digest: [32]byte{2}, Spans: []ApplicationRecoverySpan{
		{First: 100, Next: 100, Last: 102, CommandBytes: 128, RetainedBytes: 8192, RetainedRecords: 16, BytePool: ApplicationRecoveryBytePool{20_000, 20_000, 633, 633}},
		{First: 300, Next: 300, Last: 300, CommandBytes: 128, RetainedBytes: 8192, RetainedRecords: 16},
	}}
	v := spanRecoveryVerifier{reserve: rsv, terminal: ApplicationRecoveryReservation{ID: [32]byte{8}, Digest: [32]byte{9}, Credits: []ApplicationRecoveryCredit{{1, 128, 8192, 16}}}}
	if err := s.BindApplicationRecoveryVerifier(s.ApplicationBinding(), v); err != nil {
		t.Fatal(err)
	}
	return s, cfg, v
}

// Authored native Store/RawNode lifecycle only; no execution or capacity claim.
func TestApplicationRecoveryBytePoolNativeReopenPublishedAS9AndReady(t *testing.T) {
	donor, cfg, v := recoveryPoolStore(t)
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
	if donor.meta.Applied != 1 || len(donor.meta.Recovery.Queued) != 1 || donor.meta.Recovery.Queued[0].ActualLogBytes != uint64(len("register")+frameOverhead+9) {
		t.Fatal("RQ4 actual entry association lost on reopen")
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
	// Exercise the actual creation preflights at a per-active cap which admits
	// ordinary traffic but cannot fund a NEW terminal owner in addition to W.
	// The metadata copy authorizes no IO/mutation and leaves the real store's
	// immutable profile and all subsequent native lifecycle steps untouched.
	probe := &Store{meta: donor.meta, limits: donor.limits}
	current, err := recoveryOutstanding(probe.meta, probe.limits)
	if err != nil {
		t.Fatal(err)
	}
	window, err := recoveryPoolPendingWindow(probe.meta.Recovery.Reservation, probe.meta.App.Policy, 0)
	if err != nil {
		t.Fatal(err)
	}
	newTerminal := ApplicationRecoveryReservation{ID: [32]byte{12}, Digest: [32]byte{13}, Credits: []ApplicationRecoveryCredit{{1, 128, 8192, 16}}}
	extra, err := terminalGrowth(applicationTerminalBank{newTerminal}, probe.meta.App.Policy, probe.limits)
	if err != nil {
		t.Fatal(err)
	}
	retained, _ := applicationTotals(probe.meta)
	ordinary := retained + current.bytes + retentionCursorGrowth(probe.meta).bytes + window + 2*unsignedLimit(probe.meta.App.Policy.MaxInstallBytes)
	probe.meta.App.Policy.RetainedApplicationBytes = ordinary + extra.bytes - 1
	if err := probe.admitApplicationLocked(1); err != nil {
		t.Fatal("ordinary admission should fit before new owner", err)
	}
	if err := probe.admitTerminalCreationLocked(1, newTerminal); !errors.Is(err, ErrLimit) {
		t.Fatal("terminal reserve spent protected window", err)
	}
	probe.meta.App.Policy.RetainedApplicationBytes = ordinary - 1
	if err := probe.admitApplicationLocked(1); !errors.Is(err, ErrLimit) {
		t.Fatal("ordinary admission spent protected window", err)
	}
	beforeBytes, _ := applicationTotals(donor.meta)
	spanRecoveryInstall(t, donor, v, "step")
	afterBytes, _ := applicationTotals(donor.meta)
	spent := donor.meta.Recovery.Reservation.Spans[0].BytePool
	if spent.RetainedRemaining != 20_000-(afterBytes-beforeBytes) || spent.LogRemaining != 633-uint64(len("step")+frameOverhead+9) {
		t.Fatal("actual native growth or actual entry charged incorrectly", spent)
	}
	prior, err := donor.loadRecoveryRootAt(donor.activeBank(), 2)
	if err != nil || prior.Spans[0].BytePool.RetainedRemaining != 20_000 || prior.Spans[0].Next != 100 {
		t.Fatal("old native pool changed", err)
	}
	if owner, _, err := old.RecoveryOwner(t.Context(), ReadBudget{1, 8192}); err != nil || owner != (ApplicationRecoveryOwner{v.reserve.ID, v.reserve.Digest}) {
		t.Fatal(owner, err)
	}
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	if err := donor.PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	cut, err := donor.PublishedApplicationCut()
	if err != nil {
		t.Fatal(err)
	}
	spanRecoveryInstall(t, donor, v, "next") // later spent totals must not enter C.
	export, err := donor.BeginPublishedApplicationExport(t.Context(), cut.ID)
	if err != nil {
		t.Fatal(err)
	}
	manifest := buildPublished(t, export, ReadBudget{1, 8192})
	chunks := collectPublished(t, export, ReadBudget{1, 8192})
	if err := export.Close(); err != nil {
		t.Fatal(err)
	}
	if manifest.Version != 9 || manifest.Contract.Version != 7 || manifest.RecoveryReservation.Spans[0].BytePool != spent || manifest.RecoveryReservation.Spans[0].Next != 101 || !sameTerminalBank(applicationTerminalBank(manifest.TerminalReservations), applicationTerminalBank{v.terminal}) {
		t.Fatal("AS9 substituted current totals/terminal bank")
	}
	wire, err := EncodeApplicationSnapshotManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeApplicationSnapshotManifestForBinding(wire, cfg.Application.MaxImageBytes, donor.ApplicationBinding())
	if err != nil || !sameRecoveryReservation(decoded.RecoveryReservation, manifest.RecoveryReservation) {
		t.Fatal("AS9 pool roundtrip", err)
	}
	forged := cloneSnapshotManifest(decoded)
	forged.RecoveryReservation.Spans[0].BytePool.RetainedRemaining++
	if err := validateManifest(forged); !errors.Is(err, ErrCorrupt) {
		t.Fatal("unbound pool accepted", err)
	}
	mixed := bytes.Clone(wire)
	mixed[2] = 8
	if _, err := DecodeApplicationSnapshotManifest(mixed, cfg.Application.MaxImageBytes); !errors.Is(err, ErrCorrupt) {
		t.Fatal("AS8 accepted RS4 pool", err)
	}
	receiver, rcfg, rv := recoveryPoolStore(t)
	prepared, snapshot := semanticPrepared(t, receiver, manifest, chunks)
	if !bytes.Equal(snapshot.GetData()[:4], []byte{'A', 'D', 8, 0}) {
		t.Fatal("missing AD8")
	}
	view, err := prepared.OpenReadView(t.Context(), manifest.Index, cfg.Application.MaxImageBytes)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := view.RecoveryOwner(t.Context(), ReadBudget{1, 8192}); err != nil {
		t.Fatal(err)
	}
	if equal, _, err := view.CheckTerminalBank(t.Context(), manifest.TerminalReservations, ReadBudget{1, 8192}); err != nil || !equal {
		t.Fatal("TB1 changed", equal, err)
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
		t.Fatal("no authentic snapshot+tail Ready")
	}
	root, err := receiver.PersistApplicationReady(ready, claim)
	if err != nil {
		t.Fatal(err)
	}
	raw.Advance(ready)
	if err := claim.Close(); err != nil {
		t.Fatal(err)
	}
	if receiver.meta.Recovery.Reservation.Spans[0].BytePool != spent {
		t.Fatal("activation spent uninstalled actuals")
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
	after := cloneRecoveryReservation(receiver.meta.Recovery.Reservation)
	if after.Spans[0].Next != 102 || after.Spans[0].BytePool.RetainedRemaining >= spent.RetainedRemaining {
		t.Fatal("suffix failed actual debit")
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
	if !sameRecoveryReservation(after, reopened.meta.Recovery.Reservation) {
		t.Fatal("reopen reset pooled credit")
	}
	if err := reopened.BindApplicationRecoveryVerifier(reopened.ApplicationBinding(), rv); err != nil {
		t.Fatal(err)
	}
	spanRecoveryInstall(t, reopened, rv, "done")
	if recoveryEntries(reopened.meta.Recovery.Reservation) != 0 || len(reopened.meta.Recovery.Terminal) != 1 {
		t.Fatal("terminal failed exact unused-suffix retirement or changed TB1")
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	wrong := rcfg
	wrong.Recovery.Version = 3
	if s, err := Open(wrong); !errors.Is(err, ErrInvalid) || s != nil {
		if s != nil {
			_ = s.Close()
		}
		t.Fatal("implicit downgrade", err)
	}
	var nilStore *Store
	if _, err := nilStore.AdmitApplicationRecovery(t.Context(), []byte("step")); !errors.Is(err, ErrInvalid) {
		t.Fatal("nil admission", err)
	}
}
