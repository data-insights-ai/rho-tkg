package replica

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// These unit hooks validate the fixture's real prepared eight-byte sum image.
// They grant no graph/authentication/quorum authority; graphapply's native test
// exercises the actual declared semantic auditor and live association handoff.
type receivingActivationAudit struct {
	view                               *raftlog.ApplicationView
	phase                              string
	begins, advances, finishes, closes int
	cancel                             context.CancelFunc
}

type receivingNilAuditor struct{}

func (*receivingNilAuditor) BeginApplicationSnapshotAudit(*raftlog.PreparedApplicationSnapshot, ApplicationSnapshotContext, ApplicationAuditBudget) (ApplicationSnapshotAudit, error) {
	return nil, nil
}

func (a *receivingActivationAudit) BeginApplicationSnapshotAudit(p *raftlog.PreparedApplicationSnapshot, c ApplicationSnapshotContext, _ ApplicationAuditBudget) (ApplicationSnapshotAudit, error) {
	a.begins++
	v, err := p.OpenReadView(context.Background(), c.Index, 8)
	if err != nil {
		return nil, err
	}
	a.view = v
	if a.phase == "cancel-begin" {
		a.cancel()
	}
	if a.phase == "begin" {
		return a, errFixture
	}
	return a, nil
}
func (a *receivingActivationAudit) Advance(ctx context.Context, _ ApplicationAuditBudget) (bool, error) {
	a.advances++
	if a.phase == "advance" {
		return false, errFixture
	}
	r, err := a.view.RootBounded(ctx, 8)
	if err != nil {
		return false, err
	}
	if len(r.Image) != 8 || binary.BigEndian.Uint64(r.Image) != 10 {
		return false, ErrInvalid
	}
	return true, nil
}
func (a *receivingActivationAudit) Finish(context.Context, ApplicationAuditBudget) error {
	a.finishes++
	if a.phase == "finish" {
		return errFixture
	}
	return nil
}
func (a *receivingActivationAudit) Close() error {
	a.closes++
	v := a.view
	a.view = nil
	var err error
	if v != nil {
		err = v.Close()
	}
	if a.phase == "close" {
		err = errors.Join(err, errFixture)
	}
	return err
}

type receivingActivationMachine struct {
	capacityRegressionMachine
	restores, cleanups         int
	restoreError, cleanupError error
	badProfile                 bool
}

func (m *receivingActivationMachine) Restore(index uint64, image []byte) error {
	m.restores++
	if m.restoreError != nil {
		return m.restoreError
	}
	return m.applicationFixture.Restore(index, image)
}
func (m *receivingActivationMachine) TakeApplicationCleanup() ApplicationCleanup { return m }
func (m *receivingActivationMachine) Close() error                               { m.cleanups++; return m.cleanupError }
func (m *receivingActivationMachine) ApplicationCallbackLimits() ApplicationCallbackLimits {
	if m.badProfile {
		return ApplicationCallbackLimits{}
	}
	return ApplicationCallbackLimits{StageWork: 1 << 20, StageOwned: 1 << 20, RestoreWork: 1 << 20, RestoreOwned: 1 << 20, CleanupWork: 1 << 20, CleanupOwned: 1 << 20}
}

type receivingActivationFixture struct {
	r        *ApplicationReceiver
	s        *raftlog.Store
	c        raftlog.Config
	fs       *vfs.MemFS
	packet   Packet
	manifest raftlog.ApplicationSnapshotManifest
	audit    *receivingActivationAudit
	machine  *receivingActivationMachine
	options  ApplicationReceivingActivation
	nodes    map[uint64]*Driver
}

func receivingActivationNativeFixture(t *testing.T) receivingActivationFixture {
	t.Helper()
	fs := vfs.NewCrashableMem()
	f := receivingActivationNativeFixtureAt(t, fs, "receiver")
	f.fs = fs
	return f
}

func receivingActivationNativeFixtureAt(t *testing.T, fs vfs.FS, dir string) receivingActivationFixture {
	t.Helper()
	n, appendPacket, _, cut := receiverNativeAppendFixture(t)
	source := n.nodes[1]
	s, c := receiverTestStoreAt(t, fs, dir)
	r := receiverTestOpen(t, receiverTestConfig(s, cut.Term))
	replies, err := r.StepReceiving(appendPacket)
	if err != nil || len(replies) != 1 {
		t.Fatal(replies, err)
	}
	out, err := source.Step(replies[0])
	if err != nil {
		t.Fatal(err)
	}
	if err := n.pump(out.Packets, func(p Packet) bool { return p.To != 3 }); err != nil {
		t.Fatal(err)
	}
	sends, err := source.SnapshotSends()
	if err != nil || len(sends) != 1 {
		t.Fatal(sends, err)
	}
	send := sends[0]
	buildSender(t, send)
	manifest, err := send.Manifest()
	if err != nil {
		t.Fatal(err)
	}
	if err := source.SealSnapshotSend(send); err != nil {
		t.Fatal(err)
	}
	out, err = source.Propose(command(5))
	if err != nil {
		t.Fatal(err)
	}
	var packet Packet
	for _, p := range out.Packets {
		if p.To == 3 && p.Snapshot {
			packet = p
		}
	}
	if len(packet.Payload) == 0 || len(out.SnapshotSends) != 1 || out.SnapshotSends[0] != send {
		t.Fatal("actual source snapshot/owner missing")
	}
	i, err := s.BeginApplicationImport(t.Context(), manifest)
	if err != nil {
		t.Fatal(err)
	}
	for page := 0; ; page++ {
		if page >= 4096 {
			t.Fatal("bounded import")
		}
		chunk, err := send.Next(t.Context(), raftlog.ReadBudget{Rows: 64, Bytes: 256 << 10})
		if err != nil {
			t.Fatal(err)
		}
		if err := i.Append(t.Context(), chunk); err != nil {
			t.Fatal(err)
		}
		if chunk.Final {
			break
		}
	}
	if err := i.Verify(t.Context()); err != nil {
		t.Fatal(err)
	}
	p, err := i.Prepare()
	if err != nil {
		t.Fatal(err)
	}
	audit := new(receivingActivationAudit)
	machine := &receivingActivationMachine{capacityRegressionMachine: capacityRegressionMachine{t: t, store: s}}
	a := ApplicationReceivingActivation{Prepared: p, Auditor: audit, Budget: ApplicationAuditBudget{WorkBytes: 1 << 20, OwnedBytes: 1 << 20}, MinimumIndex: cut.Index, Check: func() error { return nil }, NewDriver: func() (Config, error) {
		return Config{ID: 3, Store: s, ApplicationMachine: machine, ApplicationSnapshotSends: ApplicationSnapshotSendLimits{MaxOffers: 2, MaxOwnedBytes: 4 << 20}}, nil
	}}
	return receivingActivationFixture{r, s, c, nil, packet, manifest, audit, machine, a, n.nodes}
}

func TestApplicationReceiverActivationNilAndPreflightRefusals(t *testing.T) {
	for _, r := range []*ApplicationReceiver{nil, {}} {
		d, out, err := r.ActivateApplicationSnapshot(t.Context(), Packet{}, ApplicationReceivingActivation{})
		if d != nil || !zeroPendingOutput(out) || !errors.Is(err, ErrInvalid) {
			t.Fatal(d, out, err)
		}
	}
	f := receivingActivationNativeFixture(t)
	for _, name := range []string{"nil-context", "prepared", "auditor", "check", "factory", "work", "owned", "minimum", "source", "target", "snapshot-flag", "RP-scope", "cut", "canceled", "copied"} {
		t.Run(name, func(t *testing.T) {
			a, p, ctx, r := f.options, f.packet, t.Context(), f.r
			errWant := ErrInvalid
			switch name {
			case "nil-context":
				ctx = nil
			case "prepared":
				a.Prepared = nil
			case "auditor":
				a.Auditor = nil
			case "check":
				a.Check = nil
			case "factory":
				a.NewDriver = nil
			case "work":
				a.Budget.WorkBytes = 0
			case "owned":
				a.Budget.OwnedBytes = 0
			case "minimum":
				a.MinimumIndex = 0
			case "source":
				p.From = 2
			case "target":
				p.To = 2
			case "snapshot-flag":
				p.Snapshot = false
			case "RP-scope":
				p.Payload = slices.Clone(p.Payload)
				p.Payload[4]++
			case "cut":
				a.MinimumIndex = f.manifest.Index + 1
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
				errWant = context.Canceled
			case "copied":
				r = &ApplicationReceiver{self: f.r}
			}
			d, out, err := r.ActivateApplicationSnapshot(ctx, p, a)
			if d != nil || !zeroPendingOutput(out) || !errors.Is(err, errWant) {
				t.Fatal(d, out, err)
			}
			state, err := f.s.ApplicationReceivingState()
			if err != nil || state.Term != f.manifest.Term || f.r.raw.HasReady() || f.audit.begins != 0 {
				t.Fatal("preflight changed receiving/audit state", state, err)
			}
		})
	}
}

func TestApplicationReceiverActivationAuditFailuresReleaseBeforeMutation(t *testing.T) {
	for _, phase := range []string{"begin", "advance", "finish", "close", "cancel-begin"} {
		t.Run(phase, func(t *testing.T) {
			f := receivingActivationNativeFixture(t)
			f.audit.phase = phase
			ctx := t.Context()
			want := errFixture
			if phase == "cancel-begin" {
				ctx, f.audit.cancel = context.WithCancel(ctx)
				defer f.audit.cancel()
				want = context.Canceled
			}
			d, out, err := f.r.ActivateApplicationSnapshot(ctx, f.packet, f.options)
			if d != nil || !zeroPendingOutput(out) || !errors.Is(err, want) {
				t.Fatal(d, out, err)
			}
			state, err := f.s.ApplicationReceivingState()
			usage, usageErr := f.s.ApplicationTransferUsage()
			if err != nil || usageErr != nil || state.Term != f.manifest.Term || usage.Claims != 0 || f.audit.closes != 1 || f.audit.view != nil || f.r.raw.HasReady() || f.machine.restores != 0 {
				t.Fatal("audit leaked owner/mutated Raft", state, usage, err, usageErr)
			}
			f.audit.phase = ""
			d, out, err = f.r.ActivateApplicationSnapshot(t.Context(), f.packet, f.options)
			if err != nil || d == nil || out.Applied != f.manifest.Index {
				t.Fatal("clean audit refusal poisoned valid retry", out, err)
			}
			if err := d.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestApplicationReceiverActivationAuthorityRevocationAtEveryFrontierWithholdsACK(t *testing.T) {
	for revoke := 1; revoke <= 5; revoke++ {
		t.Run(fmt.Sprintf("check-%d", revoke), func(t *testing.T) {
			f := receivingActivationNativeFixture(t)
			calls := 0
			f.options.Check = func() error {
				calls++
				if calls == revoke {
					return errFixture
				}
				return nil
			}
			d, out, err := f.r.ActivateApplicationSnapshot(t.Context(), f.packet, f.options)
			if d != nil || !zeroPendingOutput(out) || !errors.Is(err, errFixture) || calls != revoke {
				t.Fatal(d, out, err, calls)
			}
			index, _, stateErr := f.s.Checkpoint()
			want := uint64(0)
			if revoke >= 4 {
				want = f.manifest.Index
			}
			if stateErr != nil || index != want || f.r.raw.BasicStatus().Applied != 0 {
				t.Fatal("revocation advanced RawNode or lost whole durable frontier", index, want, stateErr)
			}
			if revoke >= 3 && !errors.Is(err, ErrStopped) {
				t.Fatal("post-Step revocation failed to stop receiver", err)
			}
			if revoke == 5 && (f.machine.restores != 1 || f.machine.cleanups != 1) {
				t.Fatal("revocation after Restore leaked callback owner")
			}
			usage, usageErr := f.s.ApplicationTransferUsage()
			if usageErr != nil || usage.Claims != 0 || f.audit.view != nil {
				t.Fatal("revocation leaked audit/claim ownership", usage, usageErr)
			}
		})
	}
}

func TestApplicationReceiverActivationClosedStoreAtPersistenceWithholdsACK(t *testing.T) {
	f := receivingActivationNativeFixture(t)
	calls := 0
	f.options.Check = func() error {
		calls++
		if calls == 3 {
			return f.s.Close()
		}
		return nil
	}
	d, out, err := f.r.ActivateApplicationSnapshot(t.Context(), f.packet, f.options)
	if d != nil || !zeroPendingOutput(out) || !errors.Is(err, raftlog.ErrClosed) || !errors.Is(err, ErrStopped) || f.machine.restores != 0 || f.r.raw.BasicStatus().Applied != 0 {
		t.Fatal("failed persistence advanced/adopted RawNode or emitted ACK", d, out, err)
	}
	c := f.c
	c.Create, c.FS = false, f.fs.CrashClone(vfs.CrashCloneCfg{})
	s, err := raftlog.Open(c)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if state, err := s.ApplicationReceivingState(); err != nil || state.Term != f.manifest.Term {
		t.Fatal("failed persistence changed acknowledged receiving state", state, err)
	}
}

func TestApplicationReceiverActivationMalformedWireForeignPreparedAndEmptyAuditNeverMutate(t *testing.T) {
	for _, phase := range []string{"unknown-wire", "wrong-type", "foreign-prepared", "nil-audit", "closed"} {
		t.Run(phase, func(t *testing.T) {
			f := receivingActivationNativeFixture(t)
			want := ErrInvalid
			switch phase {
			case "unknown-wire", "wrong-type":
				m := receiverTestDecode(t, f.s.ApplicationBinding(), f.packet)
				if phase == "unknown-wire" {
					m.ProtoReflect().SetUnknown([]byte{0x80, 0x01, 0x01})
				} else {
					m.Type = pb.MsgVote.Enum()
				}
				wire, err := proto.Marshal(m)
				if err != nil {
					t.Fatal(err)
				}
				f.packet, err = EncodeApplicationPacket(f.s.ApplicationBinding(), Packet{From: 1, To: 3, Snapshot: true, Payload: wire}, f.r.config.MaxPacketBytes)
				if err != nil {
					t.Fatal(err)
				}
			case "foreign-prepared":
				other := receivingActivationNativeFixture(t)
				f.options.Prepared = other.options.Prepared
				want = raftlog.ErrInvalid
			case "nil-audit":
				f.options.Auditor = new(receivingNilAuditor)
			case "closed":
				if err := f.r.Close(); err != nil {
					t.Fatal(err)
				}
				want = ErrStopped
			}
			d, out, err := f.r.ActivateApplicationSnapshot(t.Context(), f.packet, f.options)
			if d != nil || !zeroPendingOutput(out) || !errors.Is(err, want) {
				t.Fatal(d, out, err)
			}
			state, err := f.s.ApplicationReceivingState()
			if err != nil || state.Term != f.manifest.Term || f.machine.restores != 0 {
				t.Fatal("untrusted activation changed durable receiving state", state, err)
			}
		})
	}
}

func TestApplicationReceiverActivationRestoreAndFactoryFailuresReopenDurableRoot(t *testing.T) {
	for _, phase := range []string{"factory", "wrong-config", "unbound-machine", "bad-profile", "restore", "cleanup", "after-adopt"} {
		t.Run(phase, func(t *testing.T) {
			f := receivingActivationNativeFixture(t)
			want := errFixture
			switch phase {
			case "factory":
				f.options.NewDriver = func() (Config, error) { return Config{}, errFixture }
			case "wrong-config":
				f.options.NewDriver = func() (Config, error) { return Config{ID: 2, Store: f.s, ApplicationMachine: f.machine}, nil }
				want = ErrInvalid
			case "unbound-machine":
				f.options.NewDriver = func() (Config, error) {
					return Config{ID: 3, Store: f.s, ApplicationMachine: new(applicationFixture)}, nil
				}
				want = ErrInvalid
			case "bad-profile":
				f.machine.badProfile = true
				want = ErrInvalid
			case "restore":
				f.machine.restoreError = errFixture
			case "cleanup":
				f.machine.cleanupError = errFixture
			case "after-adopt":
				calls := 0
				f.options.Check = func() error {
					calls++
					if calls == 6 {
						return errFixture
					}
					return nil
				}
			}
			d, out, err := f.r.ActivateApplicationSnapshot(t.Context(), f.packet, f.options)
			if d != nil || !zeroPendingOutput(out) || !errors.Is(err, want) || f.r.stopped == nil {
				t.Fatal("post-Step failure escaped/stopped cause", d, out, err)
			}
			if phase == "restore" || phase == "cleanup" {
				if f.machine.restores != 1 || f.machine.cleanups != 1 {
					t.Fatal("Restore failure orphaned cleanup", f.machine.restores, f.machine.cleanups)
				}
			}
			if phase == "bad-profile" && f.machine.restores != 0 {
				t.Fatal("invalid callback profile ran Restore")
			}
			if phase == "after-adopt" {
				if _, _, err := f.s.InitialState(); !errors.Is(err, raftlog.ErrClosed) {
					t.Fatal("adopted Driver orphaned Store", err)
				}
			}
			if err := f.r.Close(); err != nil {
				t.Fatal(err)
			}
			c := f.c
			c.Create = false
			c.FS = f.fs.CrashClone(vfs.CrashCloneCfg{})
			s, err := raftlog.Open(c)
			if err != nil {
				t.Fatal(err)
			}
			m := &capacityRegressionMachine{t: t, store: s}
			reopened, err := Open(Config{ID: 3, Store: s, ApplicationMachine: m, ApplicationSnapshotSends: ApplicationSnapshotSendLimits{MaxOffers: 2, MaxOwnedBytes: 4 << 20}})
			if err != nil {
				t.Fatal("must reopen retained activated state", err)
			}
			defer reopened.Close()
			if reopened.Applied() != f.manifest.Index || m.value != 10 {
				t.Fatal("snapshot lost/double applied on reopen", reopened.Applied(), m.value)
			}
			if err := s.Initialize([]uint64{1, 2, 3}, nil); !errors.Is(err, raftlog.ErrInvalid) {
				t.Fatal("activated identity bootstrapped", err)
			}
		})
	}
}

func TestApplicationReceiverActivationTransfersSameRawNodeAndOutgoingSnapshotStorage(t *testing.T) {
	f := receivingActivationNativeFixture(t)
	raw, storage := f.r.raw, f.r.storage
	d, out, err := f.r.ActivateApplicationSnapshot(t.Context(), f.packet, f.options)
	if err != nil || d == nil || len(out.Packets) != 1 || out.Applied != f.manifest.Index {
		t.Fatal(d, out, err)
	}
	defer d.Close()
	if d.raw != raw || storage.driver != d || f.r.raw != nil || f.r.storage != nil || f.machine.restores != 1 || f.machine.cleanups != 1 {
		t.Fatal("receiver replaced RawNode/storage or repeated Restore")
	}
	if err := f.r.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := f.s.InitialState(); err != nil {
		t.Fatal("former receiver closed adopted Store", err)
	}
	// Elect the adopted receiver through original native packets. Snapshot
	// callbacks correctly refuse followers; no privileged role assignment occurs.
	f.nodes[3] = d
	// Honest packet loss expires voter2's previous leader lease. Campaigning a
	// just-adopted receiver while that lease is live correctly fails PreVote.
	for range 12 {
		if _, err := f.nodes[2].Tick(); err != nil {
			t.Fatal(err)
		}
	}
	election, err := d.Campaign()
	if err != nil {
		t.Fatal(err)
	}
	deliverApplication(t, f.nodes, election.Packets)
	if d.raw.BasicStatus().RaftState != raft.StateLeader {
		t.Fatal("genuine replacement election failed")
	}
	network := &capacityRegressionNetwork{t: t, nodes: f.nodes}
	keep := func(p Packet) bool { return p.To != 1 && p.From != 1 }
	for _, value := range []uint64{6, 7, 8} {
		next, err := d.Propose(command(value))
		if err != nil {
			t.Fatal(err)
		}
		if err := network.pump(next.Packets, keep); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.s.PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	cut, err := f.s.PublishedApplicationCut()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.ReportUnreachable(1); err != nil {
		t.Fatal(err)
	}
	next, err := d.Propose(command(9))
	if err != nil {
		t.Fatal(err)
	}
	if err := network.pump(next.Packets, keep); err != nil {
		t.Fatal(err)
	}
	// Its original RawNode now invokes the adopted Storage itself because the
	// genuine old follower is behind the actual newly published recovery base.
	// A plain Store would bypass SnapshotSend's build/seal ownership path.
	for _, p := range next.Packets {
		if p.To == 1 && p.Snapshot {
			t.Fatal("unsealed snapshot escaped native sender")
		}
	}
	sends, err := d.SnapshotSends()
	if err != nil || len(sends) != 1 {
		t.Fatal("future snapshot storage cannot create normal owner", sends, err)
	}
	buildSender(t, sends[0])
	if err := d.SealSnapshotSend(sends[0]); err != nil {
		t.Fatal(err)
	}
	next, err = d.Propose(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := network.pump(next.Packets, keep); err != nil {
		t.Fatal(err)
	}
	var packet Packet
	for _, p := range next.Packets {
		if p.To == 1 && p.Snapshot {
			packet = p
		}
	}
	if len(packet.Payload) == 0 || len(next.SnapshotSends) != 1 || next.SnapshotSends[0] != sends[0] {
		t.Fatal("adopted native RawNode did not bind original future snapshot", next)
	}
	snapshot := receiverTestDecode(t, d.binding, packet)
	if snapshot.GetTerm() != d.raw.BasicStatus().GetTerm() || snapshot.GetSnapshot().GetMetadata().GetIndex() != cut.Index || snapshot.GetSnapshot().GetMetadata().GetTerm() != cut.Term {
		t.Fatal("actual future snapshot coordinates changed", snapshot, cut)
	}
	if _, err := d.ReportSnapshotSend(sends[0], false); err != nil {
		t.Fatal(err)
	}
}

func TestApplicationReceiverActivationFinalClaimCleanupClosesAdoptedDriver(t *testing.T) {
	f := receivingActivationNativeFixture(t)
	d, out, err := f.r.ActivateApplicationSnapshot(t.Context(), f.packet, f.options)
	if err != nil || d == nil {
		t.Fatal(d, out, err)
	}
	if replacement, err := openDriver(d.config, f.r); replacement != nil || !errors.Is(err, ErrInvalid) {
		t.Fatal("missing receiving RawNode constructed a replacement", replacement, err)
	}
	// A direct unit fault at the SAME deferred cleanup seam, with a genuinely
	// activated/adopted Driver. This synthetic cleanup error is not Raft evidence.
	closes := 0
	f.r.mu.Lock()
	d, out, err = f.r.finishActivationLocked(d, out, nil, func() error { closes++; return errFixture })
	f.r.mu.Unlock()
	if d != nil || !zeroPendingOutput(out) || !errors.Is(err, errFixture) || !errors.Is(err, ErrStopped) || closes != 1 {
		t.Fatal("claim cleanup error orphaned adopted Driver/output", d, out, err, closes)
	}
	if _, _, err := f.s.InitialState(); !errors.Is(err, raftlog.ErrClosed) {
		t.Fatal("orphaned adopted Store", err)
	}
}
