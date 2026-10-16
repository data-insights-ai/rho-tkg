package replica

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	pb "go.etcd.io/raft/v3/raftpb"
)

// Failures are selected at genuine callback boundaries after the real cluster
// creates and commits commands. No Ready, packet or commit field is fabricated.
type readyFailureMachine struct {
	*capacityRegressionMachine
	phase                    string
	cause, sentinel          error
	base                     uint64
	armed                    bool
	stageCalls, restoreCalls []uint64
	verifyCalls              int
	closedCheckpoint         *readyFailureCheckpoint
}

type readyFailureCheckpoint struct {
	index, commit, last uint64
	image               []byte
}

func (m *readyFailureMachine) VerifyApplicationRecovery(ctx context.Context, data []byte, v *raftlog.ApplicationView) (raftlog.ApplicationRecoveryPurpose, error) {
	root, err := v.RootBounded(ctx, 8)
	if err != nil {
		return raftlog.ApplicationRecoveryPurpose{}, err
	}
	if ctx == nil || len(data) != 0 && len(data) != 8 {
		return raftlog.ApplicationRecoveryPurpose{}, raftlog.ErrInvalid
	}
	m.verifyCalls++
	if m.armed && (m.phase == "full-verifier" || m.phase == "joined-verifier" || m.phase == "prefix-verifier" && root.Index > m.base) {
		return raftlog.ApplicationRecoveryPurpose{}, m.cause
	}
	return raftlog.ApplicationRecoveryPurpose{}, nil
}
func (m *readyFailureMachine) Stage(e Entry, b raftlog.ApplicationBudget) (raftlog.ApplicationBatch, error) {
	m.stageCalls = append(m.stageCalls, e.Index)
	if m.armed && (m.phase == "stage" && e.Index == m.base+1 || m.phase == "predecessor" && e.Index == m.base+1) {
		return raftlog.ApplicationBatch{}, m.cause
	}
	batch, err := m.capacityRegressionMachine.Stage(e, b)
	if err == nil && m.armed && m.phase == "install" && e.Index == m.base+2 {
		// Preserve the genuine staged bytes/bounds but deliberately violate the
		// captured checkpoint base. The actual Store must refuse installation.
		batch.BaseImageHash[0] ^= 1
	}
	if err == nil && m.armed && m.phase == "store-close" && e.Index == m.base+2 {
		// Real Close interleaves after a successful bounded Stage and before
		// committed recovery binding. Capture durable state while still open;
		// no Store mutex or publication owner may block this callback.
		index, image, checkpointErr := m.store.Checkpoint()
		hard, _, hardErr := m.store.InitialState()
		last, lastErr := m.store.LastIndex()
		if err := errors.Join(checkpointErr, hardErr, lastErr); err != nil {
			return raftlog.ApplicationBatch{}, err
		}
		m.closedCheckpoint = &readyFailureCheckpoint{index: index, commit: hard.GetCommit(), last: last, image: image}
		if err := m.store.Close(); err != nil {
			return raftlog.ApplicationBatch{}, err
		}
	}
	return batch, err
}
func (m *readyFailureMachine) Restore(index uint64, image []byte) error {
	m.restoreCalls = append(m.restoreCalls, index)
	if m.armed && m.phase == "restore" && index == m.base+2 {
		return m.cause
	}
	return m.capacityRegressionMachine.Restore(index, image)
}

type readyFailureCleanup struct {
	image  []byte
	cause  error
	closed bool
}

func (c *readyFailureCleanup) Close() error {
	if c.closed {
		return nil
	}
	c.closed = true
	clear(c.image)
	c.image = nil
	return c.cause
}

type readyCleanupFailureMachine struct {
	*readyFailureMachine
	pending      ApplicationCleanup
	cleanupCalls int
}

func (m *readyCleanupFailureMachine) Stage(e Entry, b raftlog.ApplicationBudget) (raftlog.ApplicationBatch, error) {
	result, err := m.readyFailureMachine.Stage(e, b)
	if err == nil && m.armed && e.Index == m.base+2 {
		m.pending = &readyFailureCleanup{image: bytes.Clone(result.Image), cause: m.cause}
	}
	return result, err
}
func (m *readyCleanupFailureMachine) TakeApplicationCleanup() ApplicationCleanup {
	p := m.pending
	m.pending = nil
	if p != nil {
		m.cleanupCalls++
	}
	return p
}
func (*readyCleanupFailureMachine) ApplicationCallbackLimits() ApplicationCallbackLimits {
	return ApplicationCallbackLimits{StageWork: 1 << 20, StageOwned: 1 << 20, RestoreWork: 1 << 20, RestoreOwned: 1 << 20, CleanupWork: 1 << 20, CleanupOwned: 1 << 20}
}

type readyFailureNetwork struct {
	*capacityRegressionNetwork
	cfg     map[uint64]raftlog.Config
	fault   map[uint64]*readyFailureMachine
	cleanup *readyCleanupFailureMachine
}

func newReadyFailureNetwork(t *testing.T, phase string, cause, sentinel error) *readyFailureNetwork {
	t.Helper()
	n := &readyFailureNetwork{capacityRegressionNetwork: &capacityRegressionNetwork{t: t, nodes: map[uint64]*Driver{}, machines: map[uint64]*capacityRegressionMachine{}}, cfg: map[uint64]raftlog.Config{}, fault: map[uint64]*readyFailureMachine{}}
	for id := uint64(1); id <= 3; id++ {
		p := raftlog.DefaultApplicationPolicy(id)
		p.MaxInstallWrites, p.RetainedApplicationRecords = 256, 8192
		p.RetainedApplicationBytes = 64 << 20
		l := raftlog.DefaultLimits()
		if id == 3 {
			p.RetainedApplicationBytes = 16 << 20
			if phase == "predecessor" {
				l.MaxReadEntries = 2
				p.ReclaimEntries = 2
			}
		}
		controls, recovery := raftlog.ApplicationControlConfig{Version: 1}, raftlog.ApplicationRecoveryConfig{Version: 1}
		contract, err := raftlog.ApplicationContractForPolicyWithRecovery(p, controls, recovery)
		if err != nil {
			t.Fatal(err)
		}
		cfg := raftlog.Config{Dir: fmt.Sprintf("ready-failure-%d", id), FS: vfs.NewMem(), Create: true, Limits: l, Application: p, Controls: controls, Recovery: recovery,
			Transfer:    raftlog.ApplicationTransferConfig{Identity: raftlog.ApplicationIdentity{Graph: [16]byte{101}, Partition: 1, Group: [16]byte{102}}, Contract: contract, Limits: raftlog.DefaultApplicationTransferLimits()},
			Generations: raftlog.ApplicationGenerationLimits{MaxBytes: 256 << 20, MaxRecords: 32768}, PublishedCuts: raftlog.ApplicationPublishedCutLimits{MaxTransferChunks: 100000}, Replication: raftlog.ApplicationReplicationConfig{Voters: [3]uint64{1, 2, 3}}, SemanticContractID: raftlog.ApplicationSemanticContractID{17}}
		s, err := raftlog.Open(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Initialize([]uint64{1, 2, 3}, nil); err != nil {
			t.Fatal(err)
		}
		base := &capacityRegressionMachine{t: t, store: s}
		m := &readyFailureMachine{capacityRegressionMachine: base, phase: phase, cause: cause, sentinel: sentinel}
		var machine ApplicationMachine = m
		if id == 3 && phase == "cleanup" {
			n.cleanup = &readyCleanupFailureMachine{readyFailureMachine: m}
			machine = n.cleanup
		}
		d, err := Open(Config{ID: id, Store: s, ApplicationMachine: machine, ApplicationSnapshotSends: ApplicationSnapshotSendLimits{MaxOffers: 2, MaxOwnedBytes: 4 << 20}})
		if err != nil {
			t.Fatal(err)
		}
		n.nodes[id], n.machines[id], n.fault[id], n.cfg[id] = d, base, m, cfg
		t.Cleanup(func() {
			if err := d.Close(); err != nil && !errors.Is(err, cause) {
				t.Error(err)
			}
		})
	}
	n.campaign(1, nil)
	m := n.fault[3]
	m.base = n.nodes[3].Applied()
	m.stageCalls = nil
	m.restoreCalls = nil
	m.verifyCalls = 0
	return n
}

func readyFailureZeroOutput(out Output) bool {
	return out.Packets == nil && out.SnapshotSends == nil && out.Reads == nil && out.Applied == 0
}

// Deliver unchanged original packets until the receiver refuses. A failed
// public event must return no ACK, read proof, snapshot capability or position.
func (n *readyFailureNetwork) expectFailure(queue []Packet) error {
	n.t.Helper()
	for count := 0; len(queue) > 0; count++ {
		if count >= 10000 {
			n.t.Fatal("bounded failure transport exhausted")
		}
		p := queue[0]
		queue = queue[1:]
		message := n.decode(p)
		if message.GetType() == pb.MsgApp && len(message.Entries) > 0 {
			n.seen = append(n.seen, capacityRegressionAppend{p.From, p.To, message.GetIndex(), message.GetCommit(), message.Entries[len(message.Entries)-1].GetIndex(), len(message.Entries)})
		}
		out, err := n.nodes[p.To].Step(p)
		if err != nil {
			if p.To != 3 || !errors.Is(err, n.fault[3].cause) || !errors.Is(err, n.fault[3].sentinel) || !errors.Is(err, ErrStopped) || !readyFailureZeroOutput(out) {
				n.t.Fatalf("failure lost cause/stopped/no-output contract: target%d out%+v err%v", p.To, out, err)
			}
			return err
		}
		queue = append(queue, out.Packets...)
	}
	return nil
}
func (n *readyFailureNetwork) failCatchup(original []Packet) {
	n.t.Helper()
	if len(original) > 0 && n.expectFailure(original) != nil {
		return
	}
	for range 12 {
		out, err := n.nodes[1].Tick()
		if err != nil {
			n.t.Fatal(err)
		}
		if n.expectFailure(out.Packets) != nil {
			return
		}
	}
	n.t.Fatal("real callback failure did not occur")
}

func (n *readyFailureNetwork) assertPrefixAndReopen(wantStored, wantCommit, wantLast, wantDriver, wantValue, wantReplayedValue uint64) {
	n.t.Helper()
	d := n.nodes[3]
	m := n.fault[3]
	index, image, err := d.store.Checkpoint()
	hard, _, hardErr := d.store.InitialState()
	last, lastErr := d.store.LastIndex()
	commit := hard.GetCommit()
	if m.phase == "store-close" {
		if !errors.Is(err, raftlog.ErrClosed) || !errors.Is(hardErr, raftlog.ErrClosed) || !errors.Is(lastErr, raftlog.ErrClosed) || m.closedCheckpoint == nil {
			n.t.Fatal("closed Store returned live state", err, hardErr, lastErr, m.closedCheckpoint)
		}
		index, image = m.closedCheckpoint.index, m.closedCheckpoint.image
		commit, last = m.closedCheckpoint.commit, m.closedCheckpoint.last
	} else if err := errors.Join(err, hardErr, lastErr); err != nil {
		n.t.Fatal(err)
	}
	value := uint64(0)
	if len(image) == 8 {
		value = binary.BigEndian.Uint64(image)
	}
	if index != wantStored || commit != wantCommit || last != wantLast || d.Applied() != wantDriver || d.raw.BasicStatus().Applied != m.base || value != wantValue {
		n.t.Fatalf("exact failure prefix: stored%d commit%d last%d driver%d raw%d value%d", index, commit, last, d.Applied(), d.raw.BasicStatus().Applied, value)
	}
	if out, err := d.Tick(); !errors.Is(err, ErrStopped) || !errors.Is(err, m.cause) || !errors.Is(err, m.sentinel) || !readyFailureZeroOutput(out) {
		n.t.Fatal("stopped driver emitted/retried", out, err)
	}
	wantPublication := raftlog.ErrInvalid
	if m.phase == "store-close" {
		wantPublication = raftlog.ErrClosed
	}
	if err := d.store.PublishSnapshot(); !errors.Is(err, wantPublication) {
		n.t.Fatal("failed in-flight fence released", err)
	}
	if err := d.Close(); err != nil && !errors.Is(err, m.cause) {
		n.t.Fatal(err)
	}
	cfg := n.cfg[3]
	cfg.Create = false
	s, err := raftlog.Open(cfg)
	if err != nil {
		n.t.Fatal(err)
	}
	fresh := &readyFailureMachine{capacityRegressionMachine: &capacityRegressionMachine{t: n.t, store: s}}
	r, err := Open(Config{ID: 3, Store: s, ApplicationMachine: fresh, ApplicationSnapshotSends: ApplicationSnapshotSendLimits{MaxOffers: 2, MaxOwnedBytes: 4 << 20}})
	if err != nil {
		n.t.Fatal(err)
	}
	defer func() {
		if err := r.Close(); err != nil {
			n.t.Error("reopened driver cleanup", err)
		}
	}()
	// Open restores the durable checkpoint, not the old in-flight RawNode
	// bookkeeping. Only the next actual Tick may replay committed pending log.
	if r.Applied() != wantStored || r.raw.BasicStatus().Applied != wantStored || fresh.value != wantValue || len(fresh.stageCalls) != 0 || !slices.Equal(fresh.restoreCalls, []uint64{wantStored}) {
		n.t.Fatal("reopen did not restore exact checkpoint", r.Applied(), r.raw.BasicStatus().Applied, fresh.value, fresh.stageCalls, fresh.restoreCalls)
	}
	out, err := r.Tick()
	if err != nil {
		n.t.Fatal(err)
	}
	_ = out // genuine peer packets are not delivered to the stopped old incarnation.
	var expected []uint64
	for i := wantStored + 1; i <= wantCommit; i++ {
		expected = append(expected, i)
	}
	if !slices.Equal(fresh.stageCalls, expected) || r.Applied() != wantCommit || r.raw.BasicStatus().Applied != wantCommit {
		n.t.Fatal("missing/repeated committed pending replay", fresh.stageCalls, expected, r.Applied(), r.raw.BasicStatus().Applied)
	}
	restoredIndex, restoredImage, err := s.Checkpoint()
	if err != nil || restoredIndex != wantCommit || len(restoredImage) != 8 || binary.BigEndian.Uint64(restoredImage) != wantReplayedValue || fresh.value != wantReplayedValue {
		n.t.Fatal("reopen replay changed canonical checkpoint/value", restoredIndex, restoredImage, fresh.value, err)
	}
	if err := s.Scrub(); err != nil {
		n.t.Fatal(err)
	}
}

func TestReadyCapacityFailureActualPacketsAndExactReopen(t *testing.T) {
	for _, phase := range []string{"full-verifier", "joined-verifier", "prefix-verifier", "stage", "restore", "cleanup", "predecessor", "install", "store-close"} {
		t.Run(phase, func(t *testing.T) {
			sentinel := errors.New("actual ready callback failure")
			switch phase {
			case "install":
				sentinel = raftlog.ErrInvalid
			case "store-close":
				sentinel = raftlog.ErrClosed
			}
			cause := sentinel
			if phase == "joined-verifier" {
				cause = errors.Join(raftlog.ErrLimit, sentinel)
			}
			n := newReadyFailureNetwork(t, phase, cause, sentinel)
			m := n.fault[3]
			base := m.base
			var original []Packet
			if phase == "predecessor" {
				originalResponse := n.heartbeatResponse(1, 3)
				var pending, replies []Packet
				for _, v := range []uint64{10, 20, 30} {
					pending = append(pending, n.propose(1, v, nil)...)
				}
				for _, p := range pending {
					if p.To == 3 && n.decode(p).GetType() == pb.MsgApp {
						out, err := n.nodes[3].Step(p)
						if err != nil {
							t.Fatal(err)
						}
						replies = append(replies, out.Packets...)
					}
				}
				if n.nodes[3].Applied() != base {
					t.Fatal("old stable rows already applied")
				}
				if err := n.pump(replies, func(p Packet) bool { return p.To != 3 }); err != nil {
					t.Fatal(err)
				}
				if err := n.pump(pending, func(p Packet) bool { return p.To != 3 }); err != nil {
					t.Fatal(err)
				}
				for _, v := range []uint64{7, 8} {
					n.propose(1, v, func(p Packet) bool { return p.To != 3 })
				}
				original = []Packet{originalResponse}
			} else {
				count := 4
				if phase == "full-verifier" || phase == "joined-verifier" {
					count = 1
				}
				for i := range count {
					n.propose(1, uint64((i+1)*10), func(p Packet) bool { return p.To != 3 })
				}
			}
			m.armed = true
			n.failCatchup(original)
			wantEntries := 4
			if phase == "full-verifier" || phase == "joined-verifier" {
				wantEntries = 1
			}
			if phase == "predecessor" {
				wantEntries = 2
			}
			if !slices.ContainsFunc(n.seen, func(a capacityRegressionAppend) bool {
				return a.to == 3 && a.entries == wantEntries && a.commit >= a.last
			}) {
				t.Fatal("failure did not use original committed bulk Append", n.seen)
			}
			switch phase {
			case "full-verifier", "joined-verifier":
				if m.verifyCalls != 1 || len(m.stageCalls) != 0 || len(m.restoreCalls) != 0 {
					t.Fatal("terminal full verifier retried or applied", m.verifyCalls, m.stageCalls, m.restoreCalls)
				}
				n.assertPrefixAndReopen(base, base, base, base, 0, 0)
			case "prefix-verifier":
				if !slices.Equal(m.stageCalls, []uint64{base + 1}) || !slices.Equal(m.restoreCalls, []uint64{base + 1}) {
					t.Fatal("wrong preceding effect", m.stageCalls, m.restoreCalls)
				}
				n.assertPrefixAndReopen(base+1, base+1, base+1, base+1, 10, 10)
			case "stage":
				if !slices.Equal(m.stageCalls, []uint64{base + 1}) || len(m.restoreCalls) != 0 {
					t.Fatal(m.stageCalls, m.restoreCalls)
				}
				n.assertPrefixAndReopen(base, base+1, base+1, base, 0, 10)
			case "restore":
				if !slices.Equal(m.stageCalls, []uint64{base + 1, base + 2}) || !slices.Equal(m.restoreCalls, []uint64{base + 1, base + 2}) {
					t.Fatal(m.stageCalls, m.restoreCalls)
				}
				n.assertPrefixAndReopen(base+2, base+2, base+2, base+1, 30, 30)
			case "cleanup", "install", "store-close":
				if phase == "cleanup" && n.cleanup.cleanupCalls != 1 || !slices.Equal(m.stageCalls, []uint64{base + 1, base + 2}) || !slices.Equal(m.restoreCalls, []uint64{base + 1}) {
					t.Fatal("second staged prefix escaped before rejected install/cleanup", m.stageCalls, m.restoreCalls)
				}
				n.assertPrefixAndReopen(base+1, base+2, base+2, base+1, 10, 30)
			case "predecessor":
				if !slices.Equal(m.stageCalls, []uint64{base + 1}) || len(m.restoreCalls) != 0 {
					t.Fatal("new payload applied before unchanged predecessor", m.stageCalls, m.restoreCalls)
				}
				n.assertPrefixAndReopen(base, base+3, base+3, base, 0, 60)
			}
		})
	}
}
