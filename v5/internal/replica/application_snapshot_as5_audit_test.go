package replica

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

// This bound storage fixture validates a real incoming recovery root. It grants
// no graph, registry, factory or Host readiness authority.
type recoveryAuditApplication struct {
	*recoveryDriverFixture
	begins   int
	verifies int
	context  ApplicationSnapshotContext
}

func (m *recoveryAuditApplication) VerifyApplicationRecovery(ctx context.Context, data []byte, v *raftlog.ApplicationView) (raftlog.ApplicationRecoveryPurpose, error) {
	m.verifies++
	return m.recoveryDriverFixture.VerifyApplicationRecovery(ctx, data, v)
}

func (m *recoveryAuditApplication) ApplicationCallbackLimits() ApplicationCallbackLimits {
	return ApplicationCallbackLimits{StageWork: 1 << 20, StageOwned: 1 << 20, RestoreWork: 1 << 20, RestoreOwned: 1 << 20, CleanupWork: 1 << 20, CleanupOwned: 1 << 20}
}
func (m *recoveryAuditApplication) BeginApplicationSnapshotAudit(p *raftlog.PreparedApplicationSnapshot, c ApplicationSnapshotContext, b ApplicationAuditBudget) (ApplicationSnapshotAudit, error) {
	if b.WorkBytes < 1<<20 || b.OwnedBytes < 1<<20 {
		return nil, ErrLimit
	}
	v, err := p.OpenReadView(context.Background(), c.Index, m.store.ApplicationLimits().MaxImageBytes)
	if err != nil {
		return nil, err
	}
	owner, _, err := v.RecoveryOwner(context.Background(), raftlog.ReadBudget{Rows: 2, Bytes: 4096})
	if err != nil || owner.ID != m.reservation.ID || owner.Digest != m.reservation.Digest {
		return nil, errors.Join(ErrInvalid, err, v.Close())
	}
	m.begins++
	m.context = c
	return &recoverySnapshotAudit{view: v, expected: owner}, nil
}

type recoverySnapshotAudit struct {
	view     *raftlog.ApplicationView
	expected raftlog.ApplicationRecoveryOwner
	complete bool
}

func (a *recoverySnapshotAudit) Advance(ctx context.Context, b ApplicationAuditBudget) (bool, error) {
	if b.WorkBytes < 1<<20 || b.OwnedBytes < 1<<20 {
		return false, ErrLimit
	}
	owner, _, err := a.view.RecoveryOwner(ctx, raftlog.ReadBudget{Rows: 2, Bytes: 4096})
	if err != nil || owner != a.expected {
		return false, errors.Join(ErrInvalid, err)
	}
	r, found, _, err := a.view.GetControl(ctx, []byte("r"), raftlog.ReadBudget{Rows: 2, Bytes: 4096})
	if err != nil || !found || !bytes.Equal(r.Value, a.expected.Digest[:]) {
		return false, errors.Join(ErrInvalid, err)
	}
	a.complete = true
	return true, nil
}
func (a *recoverySnapshotAudit) Finish(ctx context.Context, b ApplicationAuditBudget) error {
	if !a.complete || b.WorkBytes < 1<<20 || b.OwnedBytes < 1<<20 {
		return ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return a.Close()
}
func (a *recoverySnapshotAudit) Close() error {
	v := a.view
	a.view = nil
	if v != nil {
		return v.Close()
	}
	return nil
}

type recoveryAuditFixture struct {
	target, source *Driver
	machine        *recoveryAuditApplication
	prepared       *raftlog.PreparedApplicationSnapshot
	packet         Packet
	manifest       raftlog.ApplicationSnapshotManifest
	manifestID     [32]byte
	nodes          map[uint64]*Driver
}

func actualRecoveryAuditFixture(t *testing.T) recoveryAuditFixture {
	t.Helper()
	return actualRecoveryAuditSchedule(t, false)
}

func actualRecoveryAuditSchedule(t *testing.T, persistRegisterBeforeSnapshot bool) recoveryAuditFixture {
	t.Helper()
	nodes := make(map[uint64]*Driver)
	var receiverMachine *recoveryAuditApplication
	for id := uint64(1); id <= 3; id++ {
		p := raftlog.DefaultApplicationPolicy(id)
		p.RetainedApplicationBytes = 18_875_000 // The existing recovery Driver fixture's exact admission profile.
		ctrl := raftlog.ApplicationControlConfig{Version: 1}
		recovery := raftlog.ApplicationRecoveryConfig{Version: 1}
		contract, err := raftlog.ApplicationContractForPolicyWithRecovery(p, ctrl, recovery)
		if err != nil {
			t.Fatal(err)
		}
		cfg := raftlog.Config{Dir: fmt.Sprintf("recovery-audit-%d", id), FS: vfs.NewMem(), Create: true, Application: p, Controls: ctrl, Recovery: recovery, Transfer: raftlog.ApplicationTransferConfig{Identity: raftlog.ApplicationIdentity{Graph: [16]byte{1}, Partition: 1, Group: [16]byte{2}}, Contract: contract, Limits: raftlog.DefaultApplicationTransferLimits()}, Generations: raftlog.ApplicationGenerationLimits{MaxBytes: 2 << 30, MaxRecords: 2_000_000}, PublishedCuts: raftlog.ApplicationPublishedCutLimits{MaxTransferChunks: 10_000}, Replication: raftlog.ApplicationReplicationConfig{Voters: [3]uint64{1, 2, 3}}, SemanticContractID: raftlog.ApplicationSemanticContractID{42}}
		s, err := raftlog.Open(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Initialize([]uint64{1, 2, 3}, nil); err != nil {
			t.Fatal(errors.Join(err, s.Close()))
		}
		base := &recoveryDriverFixture{store: s, reservation: raftlog.ApplicationRecoveryReservation{ID: [32]byte{1}, Digest: [32]byte{2}, Credits: []raftlog.ApplicationRecoveryCredit{{Phase: 1, CommandBytes: 6, RetainedBytes: 5 << 20, RetainedRecords: 5}, {Phase: 2, CommandBytes: 6, RetainedBytes: 5 << 20, RetainedRecords: 5}}}}
		var machine ApplicationMachine = base
		if id == 3 {
			receiverMachine = &recoveryAuditApplication{recoveryDriverFixture: base}
			machine = receiverMachine
		}
		d, err := Open(Config{ID: id, Store: s, ApplicationMachine: machine, ApplicationSnapshotSends: ApplicationSnapshotSendLimits{MaxOffers: 2, MaxOwnedBytes: 4 << 20}})
		if err != nil {
			t.Fatal(errors.Join(err, s.Close()))
		}
		nodes[id] = d
		t.Cleanup(func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		})
	}
	receiverDriver := nodes[3]
	var packet, withheldAppend Packet
	var send *SnapshotSend
	delivered := 0
	pump := func(out Output, err error) {
		if err != nil {
			t.Fatal(err)
		}
		var queue []Packet
		enqueue := func(out Output) {
			snapshots := 0
			for _, p := range out.Packets {
				if p.Snapshot {
					if p.From != 1 || p.To != 3 || send != nil || snapshots >= len(out.SnapshotSends) {
						t.Fatal("actual snapshot association", p)
					}
					packet, send = p, out.SnapshotSends[snapshots]
					snapshots++
				} else {
					if p.From == 1 && p.To == 3 && nodes[3] == nil {
						m, err := receiverDriver.decodeBoundPacket(p, false)
						if err != nil {
							t.Fatal(err)
						}
						if len(m.Entries) == 1 && m.Entries[0].GetIndex() == 3 && bytes.Equal(m.Entries[0].GetData(), []byte("register")) && m.GetCommit() == 2 {
							withheldAppend = p
						}
					}
					queue = append(queue, p)
				}
			}
			if snapshots != len(out.SnapshotSends) || len(queue) > 10_000 {
				t.Fatal("bounded actual FIFO", out)
			}
		}
		enqueue(out)
		for send == nil && len(queue) != 0 {
			if delivered >= 10_000 {
				t.Fatal("bounded actual delivery")
			}
			delivered++
			p := queue[0]
			queue = queue[1:]
			if target := nodes[p.To]; target != nil {
				next, err := target.Step(p)
				if err != nil {
					t.Fatal(err)
				}
				enqueue(next)
			}
		}
	}
	pump(nodes[1].Campaign())
	target := nodes[3]
	if target.Applied() != 2 || target.store.ApplicationGeneration() != 1 {
		t.Fatal("actual election baseline")
	}
	delete(nodes, 3)
	pump(nodes[1].Propose([]byte("register")))
	if err := nodes[1].store.PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	nodes[3] = target
	for range 24 {
		pump(nodes[1].Tick())
		if send != nil {
			break
		}
		offers, err := nodes[1].SnapshotSends()
		if err != nil {
			t.Fatal(err)
		}
		for _, offer := range offers {
			buildSender(t, offer)
			if err := nodes[1].SealSnapshotSend(offer); err != nil {
				t.Fatal(err)
			}
		}
	}
	if send == nil {
		t.Fatal("actual AS5 MsgSnap unavailable")
	}
	if persistRegisterBeforeSnapshot {
		if len(withheldAppend.Payload) == 0 {
			t.Fatal("actual uncommitted register append unavailable")
		}
		before := receiverMachine.verifies
		out, err := target.Step(withheldAppend)
		if err != nil || out.Applied != 2 || target.Applied() != 2 || receiverMachine.verifies <= before {
			t.Fatal("actual append did not classify pending recovery recipe", out, err, before, receiverMachine.verifies)
		}
		last, err := target.store.LastIndex()
		if err != nil || last != 3 {
			t.Fatal("actual register was not persisted", last, err)
		}
		// Its genuine ACK is deliberately withheld, preserving the leader's lagging progress.
	}
	manifest, err := send.Manifest()
	if err != nil || manifest.Version != 5 || manifest.Contract.Version != 3 {
		t.Fatal("actual WithRecovery producer", manifest.Version, manifest.Contract, err)
	}
	imported, err := target.store.BeginApplicationImport(t.Context(), manifest)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := imported.Abort(); err != nil {
			t.Error(err)
		}
	})
	var manifestID [32]byte
	final := false
	for range 100 {
		chunk, err := send.Next(t.Context(), raftlog.ReadBudget{Rows: 1, Bytes: 4096})
		if err != nil {
			t.Fatal(err)
		}
		if chunk.Version != 5 || chunk.CutID != manifest.CutID || chunk.ManifestID == ([32]byte{}) {
			t.Fatal("actual AS5 chunk identity", chunk)
		}
		if manifestID != ([32]byte{}) && manifestID != chunk.ManifestID {
			t.Fatal("manifest changed across native chunks")
		}
		manifestID = chunk.ManifestID
		if err := imported.Append(t.Context(), chunk); err != nil {
			t.Fatal(err)
		}
		if chunk.Final {
			final = true
			break
		}
	}
	if !final {
		t.Fatal("bounded native AS5 stream incomplete")
	}
	if err := imported.Verify(t.Context()); err != nil {
		t.Fatal(err)
	}
	prepared, err := imported.Prepare()
	if err != nil {
		t.Fatal(err)
	}
	return recoveryAuditFixture{target, nodes[1], receiverMachine, prepared, packet, manifest, manifestID, nodes}
}

func TestApplicationSnapshotAD123ContextRejectsActualAS5AD4BeforeBoundAudit(t *testing.T) {
	f := actualRecoveryAuditFixture(t)
	actual, err := f.target.decodeBoundPacket(f.packet, true)
	if err != nil {
		t.Fatal(err)
	}
	descriptor, err := raftlog.EncodeApplicationSnapshotDescriptor(f.manifest)
	if err != nil || !bytes.Equal(actual.GetSnapshot().Data, descriptor) || !bytes.Equal(descriptor[:4], []byte{'A', 'D', 4, 0}) {
		t.Fatal("actual producer descriptor", err)
	}
	if err := f.target.store.CheckPreparedApplicationSnapshot(f.prepared, actual.GetSnapshot()); err != nil {
		t.Fatal("same-store AS5 check", err)
	}
	t.Logf("actual AS5/AD4 same-store check passes: CutID producer position=%d; incoming credits=%d", bytes.Index(descriptor, f.manifest.CutID[:]), len(f.manifest.RecoveryReservation.Credits))
	pending, err := f.target.BeginApplicationSnapshot(t.Context(), f.packet, f.prepared, ApplicationAuditBudget{WorkBytes: 32 << 20, OwnedBytes: 32 << 20})
	if err != nil || pending == nil {
		t.Fatalf("AD1-3-only context rejected genuine AS5 AD4 before bound audit: begins=%d err=%v", f.machine.begins, err)
	}
	defer pending.Close()
	want := ApplicationSnapshotContext{Index: f.manifest.Index, Term: f.manifest.Term, Voters: [3]uint64{1, 2, 3}, DescriptorHash: sha256.Sum256(descriptor), CutID: f.manifest.CutID, ManifestID: f.manifestID}
	if f.machine.begins != 1 || f.machine.context != want {
		t.Fatal("context lost actual producer identities", f.machine.context, want)
	}
	if f.target.Applied() != 2 || f.target.store.ApplicationGeneration() != 1 {
		t.Fatal("Begin activated AS5")
	}
}

func currentRecoveryManifest(t *testing.T, s *raftlog.Store) raftlog.ApplicationSnapshotManifest {
	t.Helper()
	// Activation already published this exact incoming Base; republishing the
	// same index correctly refuses as stale. Export that actual active cut.
	cut, err := s.PublishedApplicationCut()
	if err != nil {
		t.Fatal(err)
	}
	at, _, err := s.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	if at > cut.Index {
		if err := s.PublishSnapshot(); err != nil {
			t.Fatal(err)
		}
		cut, err = s.PublishedApplicationCut()
		if err != nil {
			t.Fatal(err)
		}
	}
	export, err := s.BeginPublishedApplicationExport(t.Context(), cut.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer export.Close()
	for range 100 {
		done, err := export.BuildManifest(t.Context(), raftlog.ReadBudget{Rows: 1, Bytes: 4096})
		if err != nil {
			t.Fatal(err)
		}
		if done {
			manifest, err := export.Manifest()
			if err != nil {
				t.Fatal(err)
			}
			return manifest
		}
	}
	t.Fatal("bounded current recovery manifest incomplete")
	return raftlog.ApplicationSnapshotManifest{}
}

func TestApplicationSnapshotAS5AuditCannotActivateBeforeFinishAndActualReady(t *testing.T) {
	f := actualRecoveryAuditFixture(t)
	old, err := f.target.store.ApplicationView(2)
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	oldRoot, err := old.RootBounded(t.Context(), 0)
	if err != nil {
		t.Fatal(err)
	}
	checkDormant := func() {
		t.Helper()
		if f.target.Applied() != 2 || f.target.store.ApplicationGeneration() != 1 {
			t.Fatal("audit activated before claim+actual Ready")
		}
		owner, _, err := old.RecoveryOwner(t.Context(), raftlog.ReadBudget{Rows: 2, Bytes: 4096})
		if err != nil || owner != (raftlog.ApplicationRecoveryOwner{}) {
			t.Fatal("old generation gained incoming owner", owner, err)
		}
		row, found, _, err := old.GetControl(t.Context(), []byte("r"), raftlog.ReadBudget{Rows: 2, Bytes: 4096})
		if err != nil || found || len(row.Key)+len(row.Value) != 0 {
			t.Fatal("old generation gained incoming registration", row, found, err)
		}
		if usage, err := f.target.store.ApplicationTransferUsage(); err != nil || usage.Claims != 0 {
			t.Fatal("audit created claim", usage, err)
		}
	}
	checkDormant()
	b := ApplicationAuditBudget{WorkBytes: 32 << 20, OwnedBytes: 32 << 20}
	pending, err := f.target.BeginApplicationSnapshot(t.Context(), f.packet, f.prepared, b)
	if err != nil {
		t.Fatal(err)
	}
	defer pending.Close()
	checkDormant()
	if out, err := pending.Commit(t.Context(), fundedPendingCommitBudget()); !errors.Is(err, ErrUnavailable) || !zeroPendingOutput(out) {
		t.Fatal("Begin granted activation", out, err)
	}
	if done, err := pending.Advance(t.Context(), b); done || err != nil {
		t.Fatal("EOF was mistaken for Finish", done, err)
	}
	checkDormant()
	if usage, err := f.target.store.ApplicationUsage(); err != nil || usage.Views != 2 {
		t.Fatal("reader before Finish", usage, err)
	}
	if out, err := pending.Commit(t.Context(), fundedPendingCommitBudget()); !errors.Is(err, ErrUnavailable) || !zeroPendingOutput(out) {
		t.Fatal("EOF granted activation", out, err)
	}
	if done, err := pending.Advance(t.Context(), b); !done || err != nil {
		t.Fatal("Finish", done, err)
	}
	checkDormant()
	if usage, err := f.target.store.ApplicationUsage(); err != nil || usage.Views != 1 {
		t.Fatal("Finish failed to free prepared reader", usage, err)
	}
	verifiesBeforeCommit := f.machine.verifies
	out, err := pending.Commit(t.Context(), fundedPendingCommitBudget())
	if f.machine.verifies != verifiesBeforeCommit {
		t.Fatal("snapshot-only activation invoked verifier", verifiesBeforeCommit, f.machine.verifies)
	}
	if err != nil || out.Applied != f.manifest.Index || f.target.store.ApplicationGeneration() != 2 {
		t.Fatal("actual claim+Ready activation", out, err)
	}
	activation, err := f.target.store.ApplicationActivationAudit()
	if err != nil || activation.Index != f.manifest.Index || activation.Term != f.manifest.Term || activation.CutID != f.manifest.CutID || activation.ManifestID != f.manifestID {
		t.Fatal("activation identity", activation, err)
	}
	retained, err := old.RootBounded(t.Context(), 0)
	if err != nil || retained.Generation != oldRoot.Generation || retained.Index != oldRoot.Index || retained.ImageHash != oldRoot.ImageHash {
		t.Fatal("old generation invalidated", retained, err)
	}
	owner, _, err := old.RecoveryOwner(t.Context(), raftlog.ReadBudget{Rows: 2, Bytes: 4096})
	if err != nil || owner != (raftlog.ApplicationRecoveryOwner{}) {
		t.Fatal("old view substituted incoming owner", owner, err)
	}
	current, err := f.target.store.ApplicationView(f.target.Applied())
	if err != nil {
		t.Fatal(err)
	}
	incoming, _, err := current.RecoveryOwner(t.Context(), raftlog.ReadBudget{Rows: 2, Bytes: 4096})
	if err != nil || incoming.ID != f.manifest.RecoveryReservation.ID || incoming.Digest != f.manifest.RecoveryReservation.Digest || incoming == owner {
		t.Fatal("incoming owner lost", incoming, owner, err)
	}
	if err := current.Close(); err != nil {
		t.Fatal(err)
	}
	captured := currentRecoveryManifest(t, f.target.store)
	if captured.RecoveryReservation.ID != f.manifest.RecoveryReservation.ID || captured.RecoveryReservation.Digest != f.manifest.RecoveryReservation.Digest || len(captured.RecoveryReservation.Credits) != 2 {
		t.Fatal("incoming credits lost", captured.RecoveryReservation)
	}
	for i, credit := range f.manifest.RecoveryReservation.Credits {
		if captured.RecoveryReservation.Credits[i] != credit {
			t.Fatal("incoming credit changed", i, captured.RecoveryReservation.Credits)
		}
	}
	deliverApplication(t, f.nodes, out.Packets)
	// Actual forwarded completion exercises the preserved bound verifier and
	// RecoveryAdmissionForCommittedEntry → bind → Install path on the incoming
	// owner. Ordinary admission is at the existing recovery profile's cliff.
	if err := f.target.store.AdmitApplication(1); !errors.Is(err, raftlog.ErrLimit) {
		t.Fatal("ordinary backpressure after incoming activation", err)
	}
	completion, err := f.target.Propose([]byte("finish"))
	if err != nil {
		t.Fatal("forwarded incoming-credit completion", err)
	}
	deliverApplication(t, f.nodes, completion.Packets)
	finished := currentRecoveryManifest(t, f.target.store)
	wantCredit := f.manifest.RecoveryReservation.Credits[1]
	if finished.RecoveryReservation.ID != f.manifest.RecoveryReservation.ID || finished.RecoveryReservation.Digest != f.manifest.RecoveryReservation.Digest || len(finished.RecoveryReservation.Credits) != 1 || finished.RecoveryReservation.Credits[0] != wantCredit {
		t.Fatal("completion lost incoming owner/unused credit", finished.RecoveryReservation)
	}
	if row, found, _, err := old.GetControl(t.Context(), []byte("f"), raftlog.ReadBudget{Rows: 2, Bytes: 4096}); err != nil || found || len(row.Key)+len(row.Value) != 0 {
		t.Fatal("old retained generation gained later completion", row, found, err)
	}
	if out, err := pending.Commit(t.Context(), fundedPendingCommitBudget()); !errors.Is(err, raftlog.ErrClosed) || !zeroPendingOutput(out) {
		t.Fatal("repeat activation", out, err)
	}
}

func TestApplicationSnapshotAS5RecoveryCommitIndependentExactAndOneShortLedger(t *testing.T) {
	f := actualRecoveryAuditFixture(t)
	b := ApplicationAuditBudget{WorkBytes: 32 << 20, OwnedBytes: 32 << 20}
	pending, err := f.target.BeginApplicationSnapshot(t.Context(), f.packet, f.prepared, b)
	if err != nil {
		t.Fatal(err)
	}
	defer pending.Close()
	for range 2 {
		if _, err := pending.Advance(t.Context(), b); err != nil {
			t.Fatal(err)
		}
	}
	contract, err := f.prepared.ApplicationContract(t.Context())
	p := raftlog.DefaultApplicationPolicy(3)
	p.RetainedApplicationBytes = 18_875_000
	wantContract, contractErr := raftlog.ApplicationContractForPolicyWithRecovery(p, raftlog.ApplicationControlConfig{Version: 1}, raftlog.ApplicationRecoveryConfig{Version: 1})
	if err != nil || contractErr != nil || contract != wantContract || contract.Version != 3 || f.target.config.Limits != raftlog.DefaultLimits() || f.target.config.MaxPacketBytes != 16<<20+64<<10 || f.target.config.MaxOutputBytes != 2*(16<<20+64<<10) {
		t.Fatal("independent recovery-enabled ledger prerequisites", contract, wantContract, err, contractErr)
	}
	m := pending.message
	cs := m.GetSnapshot().GetMetadata().GetConfState()
	if len(m.Entries) != 0 || len(m.Context) != 0 || len(cs.Voters) != 3 || len(cs.Learners)+len(cs.LearnersNext)+len(cs.VotersOutgoing) != 0 {
		t.Fatal("native message shape")
	}
	// Independent pinned representation: four1024-byte proto objects plus owned
	// descriptor/ConfState capacities; no borrowed packet remains after Begin.
	held := uint64(4096 + 4*1024 + cap(m.Context) + cap(m.GetSnapshot().Data) + 8*(cap(cs.Voters)+cap(cs.Learners)+cap(cs.LearnersNext)+cap(cs.VotersOutgoing)))
	if pending.owned != held {
		t.Fatal("retained native message undercharged", pending.owned, held)
	}
	// Concrete defaults:66Ready envelopes,64tail callbacks,421069output slots;
	// Store staging peak16263814 covers activation vs install/persist/reclaim.
	// Entry-pass funding adds2 predecessor+6 recovery passes per64 tail slots.
	// One retained-prefix reservation adds256MiB+1024bytes*1000000records.
	// This is the actual Version3 WithRecovery contract, not the no-recovery
	// ApplicationContractForPolicy snapshot approximation.
	const expectedWork uint64 = 5_700_322_208
	const expectedOwnedWithoutHeld uint64 = 153_806_950
	exact := ApplicationAuditBudget{WorkBytes: expectedWork, OwnedBytes: held + expectedOwnedWithoutHeld}
	work, owned, err := f.target.applicationCommitRequirements(held, 4096, contract)
	if err != nil || work != exact.WorkBytes || owned != exact.OwnedBytes {
		t.Fatal("recovery-aware component ledger drift", work, owned, exact, err)
	}
	for _, short := range []ApplicationAuditBudget{{WorkBytes: exact.WorkBytes - 1, OwnedBytes: exact.OwnedBytes}, {WorkBytes: exact.WorkBytes, OwnedBytes: exact.OwnedBytes - 1}} {
		out, err := pending.Commit(t.Context(), short)
		if !errors.Is(err, ErrLimit) || !zeroPendingOutput(out) || f.target.Applied() != 2 || f.target.store.ApplicationGeneration() != 1 {
			t.Fatal("one-short budget activated or emitted output", out, err)
		}
		usage, err := f.target.store.ApplicationTransferUsage()
		if err != nil || usage.Claims != 0 || usage.Prepared != 1 {
			t.Fatal("one-short admission claimed owner", usage, err)
		}
	}
	out, err := pending.Commit(t.Context(), exact)
	if err != nil || out.Applied != f.manifest.Index || f.target.store.ApplicationGeneration() != 2 {
		t.Fatal("exact recovery-aware budget did not activate", out, exact, err)
	}
}

func TestApplicationSnapshotAS5StableRecipeFastForwardAndIgnoredCommitNeverReverify(t *testing.T) {
	f := actualRecoveryAuditSchedule(t, true)
	old, err := f.target.store.ApplicationView(2)
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	b := ApplicationAuditBudget{WorkBytes: 32 << 20, OwnedBytes: 32 << 20}
	for _, phase := range []string{"term-match-fast-forward", "already-applied-ignored"} {
		t.Run(phase, func(t *testing.T) {
			pending, err := f.target.BeginApplicationSnapshot(t.Context(), f.packet, f.prepared, b)
			if err != nil {
				t.Fatal(err)
			}
			defer pending.Close()
			for range 2 {
				if _, err := pending.Advance(t.Context(), b); err != nil {
					t.Fatal(err)
				}
			}
			before := f.machine.verifies
			out, err := pending.Commit(t.Context(), fundedPendingCommitBudget())
			if err != nil || out.Applied != 3 || f.target.Applied() != 3 || f.target.store.ApplicationGeneration() != 1 {
				t.Fatal("actual stable/ignored snapshot result", out, err)
			}
			if before == 0 || f.machine.verifies != before {
				t.Fatal("snapshot-only Commit reverified persisted recipe", before, f.machine.verifies)
			}
			usage, err := f.target.store.ApplicationTransferUsage()
			if err != nil || usage.Claims != 0 || usage.Prepared != 1 {
				t.Fatal("unused prepared claim not released", usage, err)
			}
			current, err := f.target.store.ApplicationView(3)
			if err != nil {
				t.Fatal(err)
			}
			owner, _, err := current.RecoveryOwner(t.Context(), raftlog.ReadBudget{Rows: 2, Bytes: 4096})
			if err != nil || owner.ID != f.manifest.RecoveryReservation.ID || owner.Digest != f.manifest.RecoveryReservation.Digest {
				t.Fatal("persisted registration recipe lost", owner, err)
			}
			if err := current.Close(); err != nil {
				t.Fatal(err)
			}
			previous, _, err := old.RecoveryOwner(t.Context(), raftlog.ReadBudget{Rows: 2, Bytes: 4096})
			if err != nil || previous != (raftlog.ApplicationRecoveryOwner{}) {
				t.Fatal("old stable view gained recovery owner", previous, err)
			}
		})
	}
}
