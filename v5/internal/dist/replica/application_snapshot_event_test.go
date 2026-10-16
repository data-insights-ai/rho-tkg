package replica

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"sync"
	"testing"
	"unsafe"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

// This replica/storage fixture has no Registered or graph semantic authority.
type firstPendingSnapshotApplication struct{ *replicatedApplication }

func (m *firstPendingSnapshotApplication) BeginApplicationSnapshotAudit(p *raftlog.PreparedApplicationSnapshot, c ApplicationSnapshotContext, b ApplicationAuditBudget) (ApplicationSnapshotAudit, error) {
	if p == nil || c.Index < 3 || c.Term == 0 || c.Voters != [3]uint64{1, 2, 3} {
		return nil, ErrInvalid
	}
	contract, err := p.ApplicationContract(context.Background())
	if err != nil {
		return nil, err
	}
	work := uint64(1024 + 1082 + 4*contract.MaxImageBytes + contract.MaxPageBytes)
	owned := uint64(1024 + contract.MaxImageBytes + contract.MaxPageBytes)
	if b.WorkBytes < work || b.OwnedBytes < owned {
		return nil, ErrLimit
	}
	v, err := p.OpenReadView(context.Background(), c.Index, 8)
	if err != nil {
		return nil, err
	}
	root, err := v.RootBounded(context.Background(), 8)
	if err != nil || !bytes.Equal(root.Image, []byte{0, 0, 0, 0, 0, 0, 0, 18}) {
		return nil, errors.Join(ErrInvalid, err, v.Close())
	}
	return &firstRealSnapshotAudit{view: v, work: work, owned: owned}, nil
}

var _ ApplicationSnapshotAuditor = (*firstPendingSnapshotApplication)(nil)

func TestBeginApplicationSnapshotValidPreparedPendingFirstContract(t *testing.T) {
	identity := raftlog.ApplicationIdentity{Graph: [16]byte{223}, Partition: 1, Group: [16]byte{224}}
	nodes := make(map[uint64]*Driver)
	for id := uint64(1); id <= 3; id++ {
		d, m, cfg := replicatedDriver(t, id, identity, vfs.NewMem())
		if id == 3 {
			if err := d.Close(); err != nil {
				t.Fatal(err)
			}
			cfg.Create = false
			s, err := raftlog.Open(cfg)
			if err != nil {
				t.Fatal(err)
			}
			wrapper := &firstPendingSnapshotApplication{replicatedApplication: m}
			d, err = Open(Config{ID: id, Store: s, ApplicationMachine: wrapper, ApplicationSnapshotSends: ApplicationSnapshotSendLimits{MaxOffers: 2, MaxOwnedBytes: 4 << 20}})
			if err != nil {
				t.Fatal(errors.Join(err, s.Close()))
			}
			t.Cleanup(func() {
				if err := d.Close(); err != nil {
					t.Error(err)
				}
			})
		}
		nodes[id] = d
	}
	var packet Packet
	var send *SnapshotSend
	delivered := 0
	// Inspect EVERY public output before ordinary FIFO delivery. Never call the
	// recursive stock helper that imports/activates a discovered snapshot.
	pump := func(out Output, eventErr error) {
		if eventErr != nil {
			t.Fatal(eventErr)
		}
		var queue []Packet
		enqueue := func(next Output) {
			snapshot := 0
			for _, p := range next.Packets {
				if p.Snapshot {
					if p.From != 1 || p.To != 3 || snapshot >= len(next.SnapshotSends) || send != nil {
						t.Fatal("actual send association", p, next)
					}
					packet, send = p, next.SnapshotSends[snapshot]
					snapshot++
				} else {
					queue = append(queue, p)
				}
			}
			if snapshot != len(next.SnapshotSends) || len(queue) > 10000 {
				t.Fatal("output association or bounded FIFO", next)
			}
		}
		enqueue(out)
		for send == nil && len(queue) != 0 {
			if delivered >= 10000 {
				t.Fatal("bounded public transport exhausted")
			}
			delivered++
			p := queue[0]
			queue = queue[1:]
			if target := nodes[p.To]; target != nil {
				next, err := target.Step(p)
				if err != nil {
					t.Fatal("ordinary public Step", p, err)
				}
				enqueue(next)
			}
		}
	}
	pump(nodes[1].Campaign())
	receiver := nodes[3]
	if receiver.Applied() != 2 || receiver.store.ApplicationGeneration() != 1 {
		t.Fatal("actual election/noop baseline", receiver.Applied())
	}
	delete(nodes, 3)
	for _, value := range []uint64{7, 11} {
		pump(nodes[1].Propose(command(value)))
	}
	if err := nodes[1].store.PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	nodes[3] = receiver
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
		t.Fatal("bounded actual owned MsgSnap unavailable")
	}
	manifest, err := send.Manifest()
	if err != nil {
		t.Fatal(err)
	}
	imported, err := receiver.store.BeginApplicationImport(t.Context(), manifest)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := imported.Abort(); err != nil {
			t.Error(err)
		}
	})
	final := false
	for range 100 {
		chunk, err := send.Next(t.Context(), raftlog.ReadBudget{Rows: 1, Bytes: 4096})
		if err != nil {
			t.Fatal(err)
		}
		if err := imported.Append(t.Context(), chunk); err != nil {
			t.Fatal(err)
		}
		if chunk.Final {
			final = true
			break
		}
	}
	if !final {
		t.Fatal("bounded stream incomplete")
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
	actual, err := receiver.decodeBoundPacket(packet, true)
	if err != nil || !bytes.Equal(actual.GetSnapshot().Data, descriptor) {
		t.Fatal("actual decoded packet", err)
	}
	if err := receiver.store.CheckPreparedApplicationSnapshot(prepared, actual.GetSnapshot()); err != nil {
		t.Fatal("actual same-Store prepared match", err)
	}
	t.Log("PENDING_VALID_OWNED_PACKET_PREPARED_MATCH_BASELINE_PASS")
	pending, err := receiver.BeginApplicationSnapshot(t.Context(), packet, prepared, ApplicationAuditBudget{WorkBytes: 32 << 20, OwnedBytes: 32 << 20})
	if err != nil || pending == nil {
		t.Fatalf("PENDING_VALID_BEGIN_UNAVAILABLE: %v", err)
	}
	out, eventErr := receiver.Tick()
	if !errors.Is(eventErr, ErrUnavailable) || out.Applied != 0 || out.Packets != nil || out.Reads != nil || out.SnapshotSends != nil || receiver.Applied() != 2 || receiver.store.ApplicationGeneration() != 1 {
		t.Fatal("pending fence or premature activation", out, eventErr)
	}
	if err := pending.Close(); err != nil {
		t.Fatal(err)
	}
}

// This verifier reads real prepared data; its result is storage-fixture evidence,
// not Registered inventory, a certificate, or permission to fabricate a Ready.
type firstRealSnapshotAudit struct {
	view               *raftlog.ApplicationView
	work, owned        uint64
	after              []byte
	complete, finished bool
}

func (a *firstRealSnapshotAudit) Advance(ctx context.Context, b ApplicationAuditBudget) (bool, error) {
	if a == nil || a.view == nil {
		return false, ErrInvalid
	}
	if b.WorkBytes < a.work || b.OwnedBytes < a.owned {
		return false, ErrLimit
	}
	page, err := a.view.Scan(ctx, nil, nil, a.after, raftlog.ReadBudget{Rows: 1, Bytes: 4096})
	if err != nil {
		return false, err
	}
	for _, row := range page.Rows {
		if !bytes.Equal(row.Key, []byte("a")) || !bytes.Equal(row.Value, []byte{0, 0, 0, 0, 0, 0, 0, 18}) || row.Deleted {
			return false, ErrInvalid
		}
	}
	a.after, a.complete = page.Next, page.Complete
	return a.complete, nil
}
func (a *firstRealSnapshotAudit) Finish(ctx context.Context, b ApplicationAuditBudget) error {
	if a == nil || a.view == nil || !a.complete {
		return ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if b.WorkBytes < a.work || b.OwnedBytes < a.owned {
		return ErrLimit
	}
	err := a.view.Close()
	a.view, a.after = nil, nil
	a.finished = err == nil
	return err
}
func (a *firstRealSnapshotAudit) Close() error {
	if a == nil {
		return ErrInvalid
	}
	v := a.view
	a.view, a.after = nil, nil
	if v != nil {
		return v.Close()
	}
	return nil
}

type pendingFixture struct {
	target, source *Driver
	prepared       *raftlog.PreparedApplicationSnapshot
	packet         Packet
}

func newPendingFixture(t *testing.T) pendingFixture {
	t.Helper()
	nodes := make(map[uint64]*Driver)
	for id := uint64(1); id <= 3; id++ {
		d, m, cfg := replicatedDriver(t, id, raftlog.ApplicationIdentity{Graph: [16]byte{223}, Partition: 1, Group: [16]byte{224}}, vfs.NewMem())
		if id == 3 {
			if err := d.Close(); err != nil {
				t.Fatal(err)
			}
			cfg.Create = false
			store, err := raftlog.Open(cfg)
			if err != nil {
				t.Fatal(err)
			}
			wrapper := &firstPendingSnapshotApplication{replicatedApplication: m}
			d, err = Open(Config{ID: 3, Store: store, ApplicationMachine: wrapper, ApplicationSnapshotSends: ApplicationSnapshotSendLimits{MaxOffers: 2, MaxOwnedBytes: 4 << 20}})
			if err != nil {
				t.Fatal(errors.Join(err, store.Close()))
			}
			t.Cleanup(func() {
				if err := d.Close(); err != nil {
					t.Error(err)
				}
			})
		}
		nodes[id] = d
	}
	target := nodes[3]
	var packet Packet
	var send *SnapshotSend
	delivered := 0
	pump := func(out Output, eventErr error) {
		if eventErr != nil {
			t.Fatal(eventErr)
		}
		var queue []Packet
		enqueue := func(next Output) {
			snapshot := 0
			for _, p := range next.Packets {
				if p.Snapshot {
					if p.From != 1 || p.To != 3 || snapshot >= len(next.SnapshotSends) || send != nil {
						t.Fatal("send pairing", p)
					}
					packet, send = p, next.SnapshotSends[snapshot]
					snapshot++
				} else {
					queue = append(queue, p)
				}
			}
			if snapshot != len(next.SnapshotSends) || len(queue) > 10000 {
				t.Fatal("bounded FIFO pairing")
			}
		}
		enqueue(out)
		for send == nil && len(queue) > 0 {
			if delivered >= 10000 {
				t.Fatal("bounded transport")
			}
			delivered++
			p := queue[0]
			queue = queue[1:]
			if d := nodes[p.To]; d != nil {
				next, err := d.Step(p)
				if err != nil {
					t.Fatal(err)
				}
				enqueue(next)
			}
		}
	}
	pump(nodes[1].Campaign())
	delete(nodes, 3)
	pump(nodes[1].Propose(command(7)))
	pump(nodes[1].Propose(command(11)))
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
		t.Fatal("actual snapshot missing")
	}
	manifest, err := send.Manifest()
	if err != nil {
		t.Fatal(err)
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
	final := false
	for range 100 {
		chunk, err := send.Next(t.Context(), raftlog.ReadBudget{Rows: 1, Bytes: 4096})
		if err != nil {
			t.Fatal(err)
		}
		if err := imported.Append(t.Context(), chunk); err != nil {
			t.Fatal(err)
		}
		if chunk.Final {
			final = true
			break
		}
	}
	if !final {
		t.Fatal("incomplete real stream")
	}
	if err := imported.Verify(t.Context()); err != nil {
		t.Fatal(err)
	}
	prepared, err := imported.Prepare()
	if err != nil {
		t.Fatal(err)
	}
	return pendingFixture{target, nodes[1], prepared, packet}
}
func TestApplicationSnapshotPendingPhasesAndActualActivation(t *testing.T) {
	f := newPendingFixture(t)
	b := ApplicationAuditBudget{WorkBytes: 32 << 20, OwnedBytes: 32 << 20}
	p, err := f.target.BeginApplicationSnapshot(t.Context(), f.packet, f.prepared, b)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := p.Close(); err != nil {
			t.Error(err)
		}
	}()
	if out, err := p.Commit(t.Context(), b); !errors.Is(err, ErrUnavailable) || !zeroPendingOutput(out) {
		t.Fatal("premature Commit", out, err)
	}
	done, err := p.Advance(t.Context(), b)
	if err != nil || done {
		t.Fatal("EOF is not Finish", done, err)
	}
	if usage, err := f.target.store.ApplicationUsage(); err != nil || usage.Views != 1 {
		t.Fatal("reader before Finish", usage, err)
	}
	done, err = p.Advance(t.Context(), b)
	if err != nil || !done {
		t.Fatal("distinct Finish", done, err)
	}
	if usage, err := f.target.store.ApplicationUsage(); err != nil || usage.Views != 0 {
		t.Fatal("Finish reader release", usage, err)
	}
	out, err := p.Commit(t.Context(), fundedPendingCommitBudget())
	if err != nil || out.Applied != 4 || f.target.store.ApplicationGeneration() != 2 {
		t.Fatal("actual Ready activation", out, err)
	}
	audit, err := f.target.store.ApplicationActivationAudit()
	if err != nil || audit.Index != 4 || audit.Term != 2 || audit.CutID != p.context.CutID || audit.ManifestID != p.context.ManifestID {
		t.Fatal("actual activation identity", audit, err)
	}
	fixtureRead(t, f.target.store, 4, 18, false)
	if out, err := p.Commit(t.Context(), b); !errors.Is(err, raftlog.ErrClosed) || !zeroPendingOutput(out) {
		t.Fatal("repeat Commit", out, err)
	}
}
func zeroPendingOutput(out Output) bool {
	return out.Applied == 0 && out.Packets == nil && out.Reads == nil && out.SnapshotSends == nil
}
func TestApplicationSnapshotPendingLifecycleAndBudgets(t *testing.T) {
	f := newPendingFixture(t)
	b := ApplicationAuditBudget{WorkBytes: 32 << 20, OwnedBytes: 32 << 20}
	for _, bad := range []ApplicationAuditBudget{{}, {WorkBytes: 1, OwnedBytes: 1}} {
		p, err := f.target.BeginApplicationSnapshot(t.Context(), f.packet, f.prepared, bad)
		want := ErrLimit
		if bad == (ApplicationAuditBudget{}) {
			want = ErrInvalid
		}
		if p != nil || !errors.Is(err, want) {
			t.Fatal("before owner admission", p, err)
		}
	}
	p, err := f.target.BeginApplicationSnapshot(t.Context(), f.packet, f.prepared, b)
	if err != nil {
		t.Fatal(err)
	}
	copied := *p
	if done, err := copied.Advance(t.Context(), b); done || !errors.Is(err, ErrInvalid) {
		t.Fatal("copied event", done, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if done, err := p.Advance(ctx, b); done || !errors.Is(err, context.Canceled) {
		t.Fatal("canceled input", done, err)
	}
	if done, err := p.Advance(t.Context(), ApplicationAuditBudget{WorkBytes: 1, OwnedBytes: 1}); done || !errors.Is(err, ErrLimit) {
		t.Fatal("short phase", done, err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal("repeat close", err)
	}
	if usage, err := f.target.store.ApplicationUsage(); err != nil || usage.Views != 0 {
		t.Fatal("closed reader", usage, err)
	}
	if f.target.Applied() != 2 || f.target.store.ApplicationGeneration() != 1 {
		t.Fatal("Close activated")
	}
	if _, err := f.target.Tick(); err != nil {
		t.Fatal("fence released", err)
	}
}
func TestApplicationSnapshotPendingIdleDriverClose(t *testing.T) {
	f := newPendingFixture(t)
	b := ApplicationAuditBudget{WorkBytes: 32 << 20, OwnedBytes: 32 << 20}
	p, err := f.target.BeginApplicationSnapshot(t.Context(), f.packet, f.prepared, b)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.target.Close(); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.target.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Advance(t.Context(), b); !errors.Is(err, raftlog.ErrClosed) {
		t.Fatal("post close", err)
	}
}
func TestApplicationSnapshotContextGoldenAndLayouts(t *testing.T) {
	for _, version := range []byte{1, 2, 3} {
		cut := 196
		if version >= 2 {
			cut += 32
		}
		if version == 3 {
			cut += 16
		}
		wire := make([]byte, cut+68)
		copy(wire[:4], []byte{'A', 'D', version, 0})
		copy(wire[cut:cut+32], bytes.Repeat([]byte{7}, 32))
		copy(wire[cut+32:cut+64], bytes.Repeat([]byte{9}, 32))
		md := newSnapshotMetadataForContext(4, 2)
		c, err := applicationSnapshotContext(wire, md)
		if err != nil || c.Index != 4 || c.Term != 2 || c.Voters != [3]uint64{1, 2, 3} || c.DescriptorHash != sha256.Sum256(wire) || c.CutID != ([32]byte(bytes.Repeat([]byte{7}, 32))) || c.ManifestID != ([32]byte(bytes.Repeat([]byte{9}, 32))) {
			t.Fatal(version, c, err)
		}
		wire[3] = 1
		if c, err := applicationSnapshotContext(wire, md); c != (ApplicationSnapshotContext{}) || !errors.Is(err, ErrInvalid) {
			t.Fatal("bad descriptor", c, err)
		}
	}
	if unsafe.Sizeof(PendingApplicationSnapshot{})+unsafe.Sizeof(applicationEvent{})+unsafe.Sizeof(applicationClose{})+3*128+2*unsafe.Sizeof(ApplicationSnapshotContext{})+256 > uintptr(applicationPendingFixed) {
		t.Fatal("fixed owner too small")
	}
	if binary.Size(ApplicationSnapshotContext{}) != 136 {
		t.Fatal("value context width")
	}
}

func newSnapshotMetadataForContext(index, term uint64) *pb.SnapshotMetadata {
	return &pb.SnapshotMetadata{Index: new(index), Term: new(term), ConfState: &pb.ConfState{Voters: []uint64{1, 2, 3}}}
}
func TestApplicationSnapshotPendingDirectNilAndOldDoor(t *testing.T) {
	var d *Driver
	if p, err := d.BeginApplicationSnapshot(t.Context(), Packet{}, nil, ApplicationAuditBudget{}); p != nil || !errors.Is(err, ErrInvalid) {
		t.Fatal(p, err)
	}
	var p *PendingApplicationSnapshot
	if done, err := p.Advance(t.Context(), ApplicationAuditBudget{}); done || !errors.Is(err, ErrInvalid) {
		t.Fatal(done, err)
	}
	if out, err := p.Commit(t.Context(), ApplicationAuditBudget{}); !zeroPendingOutput(out) || !errors.Is(err, ErrInvalid) {
		t.Fatal(out, err)
	}
	if err := p.Close(); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	f := newPendingFixture(t)
	if out, err := f.target.StepApplicationSnapshot(f.packet, f.prepared); !zeroPendingOutput(out) || !errors.Is(err, ErrUnavailable) {
		t.Fatal("mandatory audit bypass", out, err)
	}
	if f.target.Applied() != 2 || f.target.store.ApplicationGeneration() != 1 {
		t.Fatal("old door mutated")
	}
	if err := f.prepared.Close(); err != nil {
		t.Fatal(err)
	}
	if p, err := f.target.BeginApplicationSnapshot(t.Context(), f.packet, f.prepared, ApplicationAuditBudget{WorkBytes: 32 << 20, OwnedBytes: 32 << 20}); p != nil || !errors.Is(err, raftlog.ErrClosed) {
		t.Fatal(p, err)
	}
}

// Supplemental protocol guard: freeze before the exact membership check. This
// is not a new runtime RED or a fabricated successful owner.
func TestApplicationSnapshotUnregisteredPendingCannotStealEvent(t *testing.T) {
	f := newPendingFixture(t)
	b := ApplicationAuditBudget{WorkBytes: 32 << 20, OwnedBytes: 32 << 20}
	p, err := f.target.BeginApplicationSnapshot(t.Context(), f.packet, f.prepared, b)
	if err != nil {
		t.Fatal(err)
	}
	forged := &PendingApplicationSnapshot{driver: f.target, event: p.event, done: make(chan struct{})}
	forged.self = forged
	if done, err := forged.Advance(t.Context(), b); done || !errors.Is(err, ErrInvalid) {
		t.Fatal("unregistered Advance", done, err)
	}
	if out, err := forged.Commit(t.Context(), b); !zeroPendingOutput(out) || !errors.Is(err, ErrInvalid) {
		t.Fatal("unregistered Commit", out, err)
	}
	if err := forged.Close(); !errors.Is(err, ErrInvalid) {
		t.Fatal("unregistered Close", err)
	}
	if done, err := p.Advance(t.Context(), b); done || err != nil {
		t.Fatal("real owner changed", done, err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if usage, err := f.target.store.ApplicationUsage(); err != nil || usage.Views != 0 {
		t.Fatal("real owner accounting", usage, err)
	}
}

// These compiled fixture limits cover actual fixed primitive callbacks, not
// Registered parsing or a live factory. No persistent diagnostic slice grows.
func (m *firstPendingSnapshotApplication) ApplicationCallbackLimits() ApplicationCallbackLimits {
	return ApplicationCallbackLimits{StageWork: 1 << 20, StageOwned: 1 << 20, RestoreWork: 1 << 20, RestoreOwned: 1 << 20, CleanupWork: 1 << 20, CleanupOwned: 1 << 20}
}
func (m *firstPendingSnapshotApplication) Restore(index uint64, image []byte) error {
	return m.applicationFixture.Restore(index, image)
}
func (m *firstPendingSnapshotApplication) Stage(e Entry, b raftlog.ApplicationBudget) (raftlog.ApplicationBatch, error) {
	return m.applicationFixture.Stage(e, b)
}
func fundedPendingCommitBudget() ApplicationAuditBudget {
	return ApplicationAuditBudget{WorkBytes: 1 << 34, OwnedBytes: 1 << 30}
}
func TestApplicationSnapshotCommitFundsConfiguredDrainBeforeClaim(t *testing.T) {
	f := newPendingFixture(t)
	b := ApplicationAuditBudget{WorkBytes: 32 << 20, OwnedBytes: 32 << 20}
	p, err := f.target.BeginApplicationSnapshot(t.Context(), f.packet, f.prepared, b)
	if err != nil {
		t.Fatal(err)
	}
	if done, err := p.Advance(t.Context(), b); done || err != nil {
		t.Fatal(done, err)
	}
	if done, err := p.Advance(t.Context(), b); !done || err != nil {
		t.Fatal(done, err)
	}
	// This budgets message/candidate/claim but cannot fund configured Ready,
	// callbacks, clones and output. Refuse before any Claim or RawNode action.
	if out, err := p.Commit(t.Context(), b); !errors.Is(err, ErrLimit) || !zeroPendingOutput(out) {
		t.Fatal("unfunded drain", out, err)
	}
	if f.target.Applied() != 2 || f.target.store.ApplicationGeneration() != 1 {
		t.Fatal("short budget mutated")
	}
	if out, err := p.Commit(t.Context(), fundedPendingCommitBudget()); err != nil || out.Applied != 4 {
		t.Fatal("funded actual drain", out, err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
}
func TestApplicationSnapshotCommitClosePublicRace(t *testing.T) {
	f := newPendingFixture(t)
	b := ApplicationAuditBudget{WorkBytes: 32 << 20, OwnedBytes: 32 << 20}
	p, err := f.target.BeginApplicationSnapshot(t.Context(), f.packet, f.prepared, b)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err := p.Advance(t.Context(), b); err != nil {
			t.Fatal(err)
		}
	}
	var out Output
	var commitErr, closeErr error
	var wg sync.WaitGroup
	start := make(chan struct{})
	wg.Go(func() { <-start; out, commitErr = p.Commit(t.Context(), fundedPendingCommitBudget()) })
	wg.Go(func() { <-start; closeErr = p.Close() })
	close(start)
	wg.Wait()
	if closeErr != nil {
		t.Fatal(closeErr)
	}
	if commitErr != nil && !errors.Is(commitErr, raftlog.ErrClosed) && !errors.Is(commitErr, ErrStopped) && !errors.Is(commitErr, ErrUnavailable) {
		t.Fatal(commitErr)
	}
	if commitErr != nil && !zeroPendingOutput(out) {
		t.Fatal("canceled success escaped", out, commitErr)
	}
	generation := f.target.store.ApplicationGeneration()
	if generation != 1 && generation != 2 {
		t.Fatal("invalid activation", generation)
	}
	if usage, err := f.target.store.ApplicationUsage(); err != nil || usage.Views != 0 {
		t.Fatal("race cleanup", usage, err)
	}
}

// These are typed logical representation bounds, not heap/RSS measurements.
func TestApplicationSnapshotConcreteTypedResourceModels(t *testing.T) {
	for _, n := range []uintptr{unsafe.Sizeof(pb.Message{}), unsafe.Sizeof(pb.Snapshot{}), unsafe.Sizeof(pb.SnapshotMetadata{}), unsafe.Sizeof(pb.ConfState{})} {
		if n+32*unsafe.Sizeof(uint64(0)) > uintptr(applicationProtoObjectBytes) {
			t.Fatal("protobuf object/scalar-pointer allowance", n)
		}
	}
	entry := unsafe.Sizeof(pb.Entry{}) + 3*unsafe.Sizeof(uint64(0))
	if entry > uintptr(applicationProtoEntryBytes) {
		t.Fatal("entry/scalar allocation allowance", entry)
	}
	fixed := unsafe.Sizeof(PendingApplicationSnapshot{}) + unsafe.Sizeof(applicationEvent{}) + unsafe.Sizeof(applicationClose{}) +
		3*128 + 2*unsafe.Sizeof(ApplicationSnapshotContext{}) + 6*unsafe.Sizeof(ApplicationAuditBudget{}) +
		2*unsafe.Sizeof(ApplicationCallbackLimits{}) + unsafe.Sizeof(raft.Ready{}) + unsafe.Sizeof(Entry{}) +
		unsafe.Sizeof(Output{}) + unsafe.Sizeof(raftlog.ApplicationBatch{}) + 16*unsafe.Sizeof(uint64(0)) +
		8*unsafe.Sizeof(error(nil)) + 8*unsafe.Sizeof(ApplicationCleanup(nil)) + 256 + 32 + 512
	if fixed > uintptr(applicationPendingFixed) {
		t.Fatal("fixed caller/return/context/hash schedule", fixed)
	}
	if unsafe.Sizeof(Packet{}) > 64 || unsafe.Sizeof(ReadResult{}) > 64 || unsafe.Sizeof(raft.Ready{}) > 512 {
		t.Fatal("public output/Ready fixed representation")
	}
	fixture := unsafe.Sizeof(firstPendingSnapshotApplication{}) + unsafe.Sizeof(firstRealSnapshotAudit{}) +
		unsafe.Sizeof(cleanupApplication{}) + 16*unsafe.Sizeof(applicationViewCleanup{}) + unsafe.Sizeof(raftlog.ApplicationView{})
	if fixture > 16*1024 {
		t.Fatal("fixture owned diagnostic/view structures", fixture)
	}
	t.Logf("typed model fixed=%d entry=%d fixture=%d Ready=%d", fixed, entry, fixture, unsafe.Sizeof(raft.Ready{}))
}

func TestApplicationPendingBeforeActionInputAndPhaseGuards(t *testing.T) {
	f := newPendingFixture(t)
	b := ApplicationAuditBudget{WorkBytes: 32 << 20, OwnedBytes: 32 << 20}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if p, err := f.target.BeginApplicationSnapshot(ctx, f.packet, f.prepared, b); p != nil || !errors.Is(err, context.Canceled) {
		t.Fatal(p, err)
	}
	for _, tc := range []struct {
		packet Packet
		want   error
	}{
		{packet: Packet{}, want: ErrInvalid},
		{packet: Packet{Payload: make([]byte, f.target.config.MaxPacketBytes+1)}, want: ErrLimit},
		{packet: Packet{From: f.packet.From, To: f.packet.To, Payload: make([]byte, 80), Snapshot: true}, want: ErrInvalid},
	} {
		if p, err := f.target.BeginApplicationSnapshot(t.Context(), tc.packet, f.prepared, b); p != nil || !errors.Is(err, tc.want) {
			t.Fatal(p, err)
		}
	}
	if f.target.config.Limits.MaxReadEntries != 128 {
		t.Fatal("independent default wire ledger assumes128")
	}
	// Independent source-law arithmetic: q128 byte-capacity and Conf growth credit.
	const capacityWork = 8192*128 + 2*8192 + 9*(16*128+8192) + 4*(24*(256*257/2)+8192*256)
	work := uint64(4096 + 80 + 10*(len(f.packet.Payload)-80) + 4096 + 128*256 + 4*256*8 + 512 + capacityWork + 2048 + 2*(1<<20))
	for _, limit := range []uint64{work, work + 1} {
		bad := b
		bad.WorkBytes = limit
		if p, err := f.target.BeginApplicationSnapshot(t.Context(), f.packet, f.prepared, bad); p != nil || !errors.Is(err, ErrLimit) {
			t.Fatal("no constructor/audit work loan", limit, p, err)
		}
	}
	p, err := f.target.BeginApplicationSnapshot(t.Context(), f.packet, f.prepared, b)
	if err != nil {
		t.Fatal(err)
	}
	for _, bad := range []ApplicationAuditBudget{{}, {WorkBytes: 4096 + 2048 + 2*(1<<20), OwnedBytes: 32 << 20}, {WorkBytes: 4096 + 2048 + 2*(1<<20) + 1, OwnedBytes: 32 << 20}} {
		want := ErrLimit
		if bad == (ApplicationAuditBudget{}) {
			want = ErrInvalid
		}
		if done, err := p.Advance(t.Context(), bad); done || !errors.Is(err, want) {
			t.Fatal("phase loan refusal", done, err)
		}
	}
	if done, err := p.Advance(t.Context(), b); done || err != nil {
		t.Fatal(done, err)
	}
	if done, err := p.Advance(t.Context(), b); !done || err != nil {
		t.Fatal(done, err)
	}
	if done, err := p.Advance(t.Context(), b); !done || err != nil {
		t.Fatal("finished audit is fixed", done, err)
	}
	if out, err := p.Commit(ctx, fundedPendingCommitBudget()); !zeroPendingOutput(out) || !errors.Is(err, context.Canceled) {
		t.Fatal(out, err)
	}
	if out, err := p.Commit(t.Context(), ApplicationAuditBudget{}); !zeroPendingOutput(out) || !errors.Is(err, ErrInvalid) {
		t.Fatal(out, err)
	}
	v, err := f.prepared.OpenReadView(t.Context(), 4, 8)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := v.Close(); err != nil {
			t.Error(err)
		}
	})
	if out, err := p.Commit(t.Context(), fundedPendingCommitBudget()); !zeroPendingOutput(out) || !errors.Is(err, raftlog.ErrLimit) {
		t.Fatal("reader must prevent Claim", out, err)
	}
	if err := v.Close(); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if f.target.Applied() != 2 || f.target.store.ApplicationGeneration() != 1 {
		t.Fatal("refusal activated")
	}
}

func TestApplicationPendingPreparedLifetimeRefusesBeforeRaw(t *testing.T) {
	f := newPendingFixture(t)
	b := ApplicationAuditBudget{WorkBytes: 32 << 20, OwnedBytes: 32 << 20}
	p, err := f.target.BeginApplicationSnapshot(t.Context(), f.packet, f.prepared, b)
	if err != nil {
		t.Fatal(err)
	}
	if done, err := p.Advance(t.Context(), b); done || err != nil {
		t.Fatal(done, err)
	}
	if done, err := p.Advance(t.Context(), b); !done || err != nil {
		t.Fatal(done, err)
	}
	if err := f.prepared.Close(); err != nil {
		t.Fatal(err)
	}
	if out, err := p.Commit(t.Context(), fundedPendingCommitBudget()); !zeroPendingOutput(out) || !errors.Is(err, raftlog.ErrClosed) {
		t.Fatal("actual prepared lifetime", out, err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if f.target.Applied() != 2 || f.target.store.ApplicationGeneration() != 1 {
		t.Fatal("closed import activated")
	}
}

func TestApplicationCommitMissingStoreStagingReservation(t *testing.T) {
	f := newPendingFixture(t)
	if f.target.store.ApplicationLimits() != raftlog.DefaultApplicationPolicy(3) || f.target.config.Limits != raftlog.DefaultLimits() {
		t.Fatal("independent default Store staging ledger prerequisite")
	}
	b := ApplicationAuditBudget{WorkBytes: 32 << 20, OwnedBytes: 32 << 20}
	p, err := f.target.BeginApplicationSnapshot(t.Context(), f.packet, f.prepared, b)
	if err != nil {
		t.Fatal(err)
	}
	if done, err := p.Advance(t.Context(), b); done || err != nil {
		t.Fatal(done, err)
	}
	if done, err := p.Advance(t.Context(), b); !done || err != nil {
		t.Fatal(done, err)
	}
	if usage, err := f.target.store.ApplicationUsage(); err != nil || usage.Views != 0 || f.target.Applied() != 2 || f.target.store.ApplicationGeneration() != 1 {
		t.Fatal("completed audit/readers baseline", usage, err)
	}
	t.Log("STORE_STAGING_FINISHED_READERS_BASELINE_PASS")
	//132MiB exceeds the old137551680-byte framework estimate, but omits
	//distinct configured Store batch/appFrame owners (install4MiB/image64KiB).
	short := ApplicationAuditBudget{WorkBytes: 1 << 34, OwnedBytes: 132 << 20}
	out, err := p.Commit(t.Context(), short)
	if !errors.Is(err, ErrLimit) || !zeroPendingOutput(out) || f.target.Applied() != 2 || f.target.store.ApplicationGeneration() != 1 {
		t.Fatal("STORE_STAGING_ADMISSION_DEFICIENCY", out.Applied, err, f.target.Applied(), f.target.store.ApplicationGeneration())
	}
	//A genuine pre-Claim refusal leaves this exact healthy event retryable.
	out, err = p.Commit(t.Context(), fundedPendingCommitBudget())
	if err != nil || out.Applied != 4 || f.target.store.ApplicationGeneration() != 2 {
		t.Fatal("funded retry", out.Applied, err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestApplicationPendingDistinctEventRefusesWithoutLiveMutation(t *testing.T) {
	f := newPendingFixture(t)
	b := ApplicationAuditBudget{WorkBytes: 32 << 20, OwnedBytes: 32 << 20}
	p, err := f.target.BeginApplicationSnapshot(t.Context(), f.packet, f.prepared, b)
	if err != nil {
		t.Fatal(err)
	}
	e := &applicationEvent{done: make(chan struct{})}
	forged := &PendingApplicationSnapshot{driver: f.target, event: e, done: make(chan struct{})}
	forged.self, e.pending = forged, forged
	if done, err := forged.Advance(t.Context(), b); done || !errors.Is(err, ErrInvalid) {
		t.Fatal(done, err)
	}
	if out, err := forged.Commit(t.Context(), b); !zeroPendingOutput(out) || !errors.Is(err, ErrInvalid) {
		t.Fatal(out, err)
	}
	if err := forged.Close(); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if done, err := p.Advance(t.Context(), b); done || err != nil {
		t.Fatal("genuine event remains usable", done, err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
}
