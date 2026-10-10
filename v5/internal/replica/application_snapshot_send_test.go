package replica

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"os/exec"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/cockroachdb/pebble/v2/vfs/errorfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/tracker"
	"google.golang.org/protobuf/proto"
)

func TestSnapshotSendConstructorlessRefuses(t *testing.T) {
	for _, s := range []*SnapshotSend{nil, new(SnapshotSend)} {
		if _, err := s.Build(t.Context(), raftlog.ReadBudget{Rows: 1, Bytes: 4096}); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
		if _, err := s.Manifest(); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
		if _, err := s.Next(t.Context(), raftlog.ReadBudget{Rows: 1, Bytes: 4096}); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
		if err := s.Close(); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
	}
}

// senderFixture deliberately bypasses Open's closed fixed-three-voter mode.
// It exercises only outgoing sender ownership with a real RawNode. No receiver
// activation, regular packet admission or replicated materializer is enabled.
func senderFixture(t *testing.T, fs vfs.FS, limit ApplicationSnapshotSendLimits) (*Driver, *applicationSnapshotStorage) {
	return senderFixtureStorage(t, fs, limit, false)
}
func senderFixtureStorage(t *testing.T, fs vfs.FS, limit ApplicationSnapshotSendLimits, flush bool, images ...[]byte) (*Driver, *applicationSnapshotStorage) {
	t.Helper()
	p := raftlog.DefaultApplicationPolicy(1)
	tc := raftlog.ApplicationTransferConfig{Identity: raftlog.ApplicationIdentity{Graph: [16]byte{1}, Partition: 1, Group: [16]byte{2}}, Contract: raftlog.ApplicationContractForPolicy(p), Limits: raftlog.DefaultApplicationTransferLimits()}
	storeConfig := raftlog.Config{Dir: "sender", FS: fs, Create: true, Application: p, Transfer: tc, Generations: raftlog.ApplicationGenerationLimits{MaxBytes: 2 << 30, MaxRecords: 2_000_000}, PublishedCuts: raftlog.ApplicationPublishedCutLimits{MaxTransferChunks: 10000}, Replication: raftlog.ApplicationReplicationConfig{Voters: [3]uint64{1, 2, 3}}, SemanticContractID: raftlog.ApplicationSemanticContractID{7}}
	storeConfig.Limits = raftlog.DefaultLimits()
	storeConfig.Limits.CacheBytes = 1
	store, err := raftlog.Open(storeConfig)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.Initialize([]uint64{1, 2, 3}, nil); err != nil {
		t.Fatal(err)
	}
	machine := &applicationFixture{index: 1}
	if err = store.Persist(raft.Ready{Entries: []*pb.Entry{{Index: new(uint64(2)), Term: new(uint64(1)), Data: command(7)}}, HardState: &pb.HardState{Term: new(uint64(1)), Commit: new(uint64(2))}}); err != nil {
		t.Fatal(err)
	}
	batch, err := machine.Stage(Entry{Generation: store.ApplicationGeneration(), Index: 2, Term: 1, Data: command(7)}, store.ApplicationBudget())
	if err != nil {
		t.Fatal(err)
	}
	if len(images) > 0 {
		batch.Image = bytes.Clone(images[0])
	}
	if err = store.InstallApplication(2, batch); err != nil {
		t.Fatal(err)
	}
	if len(images) > 0 {
		machine.image = bytes.Clone(batch.Image)
		machine.index = 2
		machine.value = 7
	} else if err = machine.Restore(2, batch.Image); err != nil {
		t.Fatal(err)
	}
	if err = store.PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	if flush {
		if err = store.Close(); err != nil {
			t.Fatal(err)
		}
		db, err := pebble.Open("sender", &pebble.Options{FS: fs})
		if err != nil {
			t.Fatal(err)
		}
		if err = db.Flush(); err != nil {
			t.Fatal(err)
		}
		if err = db.Close(); err != nil {
			t.Fatal(err)
		}
		storeConfig.Create = false
		store, err = raftlog.Open(storeConfig)
		if err != nil {
			t.Fatal(err)
		}
	}
	cfg := Config{ID: 1, Store: store, ApplicationMachine: machine, Limits: store.Limits(), MaxPacketBytes: 3 << 20, MaxOutputBytes: 8 << 20, ApplicationSnapshotSends: limit}
	if err = limit.validate(cfg); err != nil {
		t.Fatal(err)
	}
	d := &Driver{store: store, config: cfg, applicationMachine: machine, applied: 2, readNonce: [16]byte{9}, reads: make(map[string]*pendingRead), lastLeader: 1}
	_, d.conf, err = store.InitialState()
	if err != nil {
		t.Fatal(err)
	}
	d.snapshotSender = newApplicationSnapshotSender(d)
	storage := &applicationSnapshotStorage{Store: store, d: d}
	d.raw, err = raft.NewRawNode(&raft.Config{ID: 1, Storage: storage, Applied: 2, ElectionTick: 10, HeartbeatTick: 1, MaxInflightMsgs: 16, MaxSizePerMsg: 1 << 20, ReadOnlyOption: raft.ReadOnlySafe})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, probe := d.store.ApplicationTransferUsage()
		if err := d.Close(); err != nil && !errors.Is(probe, raftlog.ErrPoisoned) && !errors.Is(probe, raftlog.ErrClosed) {
			t.Error(err)
		}
	})
	return d, storage
}
func senderEvent(t *testing.T, d *Driver, fn func() error) ([]*pb.Message, []*SnapshotSend) {
	t.Helper()
	d.mu.Lock()
	defer d.unlock()
	if err := fn(); err != nil {
		t.Fatal(err)
	}
	if d.snapshotSender.fatal != nil {
		return nil, nil
	}
	var messages []*pb.Message
	var sends []*SnapshotSend
	for d.raw.HasReady() {
		rd := d.raw.Ready()
		if err := d.store.Persist(rd); err != nil {
			t.Fatal(err)
		}
		for _, entry := range rd.CommittedEntries {
			if entry.GetIndex() <= d.applied {
				continue
			}
			b, err := d.applicationMachine.Stage(Entry{Generation: d.store.ApplicationGeneration(), Index: entry.GetIndex(), Term: entry.GetTerm(), Data: bytes.Clone(entry.Data)}, d.store.ApplicationBudget())
			if err != nil {
				t.Fatal(err)
			}
			if err = d.store.InstallApplication(entry.GetIndex(), b); err != nil {
				t.Fatal(err)
			}
			if err = d.applicationMachine.Restore(entry.GetIndex(), b.Image); err != nil {
				t.Fatal(err)
			}
			d.applied = entry.GetIndex()
		}
		for _, m := range rd.Messages {
			messages = append(messages, m)
			if m.GetType() == pb.MsgSnap {
				e, err := d.bindSnapshotSend(m)
				if err != nil {
					t.Fatal(err)
				}
				if e != nil {
					sends = append(sends, e)
				}
			}
		}
		d.raw.Advance(rd)
	}
	return messages, sends
}
func senderLeader(t *testing.T, d *Driver) {
	t.Helper()
	senderEvent(t, d, d.raw.Campaign)
	senderEvent(t, d, func() error {
		return d.raw.Step(&pb.Message{Type: pb.MsgVoteResp.Enum(), From: new(uint64(2)), To: new(uint64(1)), Term: new(uint64(2))})
	})
	senderEvent(t, d, func() error {
		return d.raw.Step(&pb.Message{Type: pb.MsgAppResp.Enum(), From: new(uint64(2)), To: new(uint64(1)), Term: new(uint64(2)), Index: new(uint64(3))})
	})
	if d.raw.BasicStatus().RaftState != raft.StateLeader || d.applied != 3 {
		t.Fatal(d.raw.BasicStatus(), d.applied)
	}
}
func senderRequest(t *testing.T, d *Driver, to uint64) ([]*pb.Message, []*SnapshotSend) {
	t.Helper()
	return senderEvent(t, d, func() error {
		var previous uint64
		d.raw.WithProgress(func(id uint64, _ raft.ProgressType, p tracker.Progress) {
			if id == to {
				previous = p.Next - 1
			}
		})
		return d.raw.Step(&pb.Message{Type: pb.MsgAppResp.Enum(), From: new(to), To: new(uint64(1)), Term: new(uint64(2)), Reject: new(true), Index: new(previous), RejectHint: new(uint64(1)), LogTerm: new(uint64(1))})
	})
}
func senderOffer(t *testing.T, d *Driver) *SnapshotSend {
	t.Helper()
	sends, err := d.SnapshotSends()
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range sends {
		if !e.bound && !e.queued {
			return e
		}
	}
	t.Fatal("missing unbound offer")
	return nil
}
func buildSender(t *testing.T, e *SnapshotSend) raftlog.ApplicationSnapshotManifest {
	t.Helper()
	for range 100 {
		ready, err := e.Build(t.Context(), raftlog.ReadBudget{Rows: 1, Bytes: 4096})
		if err != nil {
			t.Fatal(err)
		}
		if ready {
			m, err := e.Manifest()
			if err != nil {
				t.Fatal(err)
			}
			return m
		}
	}
	t.Fatal("unbounded build")
	return raftlog.ApplicationSnapshotManifest{}
}
func TestSnapshotSendRealRawNodeReservationRetryAndExactSource(t *testing.T) {
	d, _ := senderFixture(t, vfs.NewMem(), ApplicationSnapshotSendLimits{MaxOffers: 2, MaxOwnedBytes: 1 << 20})
	senderLeader(t, d)
	messages, sends := senderRequest(t, d, 3)
	if len(sends) != 0 {
		t.Fatal("unbuilt snapshot emitted", sends)
	}
	for _, m := range messages {
		if m.GetType() == pb.MsgSnap {
			t.Fatal("unbuilt snapshot")
		}
	}
	e := senderOffer(t, d)
	if _, err := e.Manifest(); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := e.Next(t.Context(), raftlog.ReadBudget{Rows: 1, Bytes: 4096}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if err := d.SealSnapshotSend(e); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	m := buildSender(t, e)
	if m.Index != 2 || m.Version != 3 || binary.BigEndian.Uint64(m.Image) != 7 {
		t.Fatal(m)
	}
	senderEvent(t, d, func() error { return d.raw.Propose(command(11)) })
	senderEvent(t, d, func() error {
		return d.raw.Step(&pb.Message{Type: pb.MsgAppResp.Enum(), From: new(uint64(2)), To: new(uint64(1)), Term: new(uint64(2)), Index: new(uint64(4))})
	})
	if d.applied != 4 {
		t.Fatal("later application not committed")
	}
	if err := d.store.PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	newer, _ := d.store.PublishedApplicationCut()
	if newer.ID == m.CutID {
		t.Fatal("fixture did not advance publication")
	}
	if err := d.SealSnapshotSend(e); err != nil {
		t.Fatal(err)
	}
	_, sends = senderRequest(t, d, 3)
	if len(sends) != 1 || sends[0] != e || e.to != 3 || e.term != 2 || e.manifest.CutID != m.CutID {
		t.Fatal("exact offer not consumed", sends, e)
	}
	if _, err := d.ReportSnapshotSend(e, true); !errors.Is(err, ErrInvalid) {
		t.Fatal("success before final", err)
	}
	var data []byte
	for range 100 {
		c, err := e.Next(t.Context(), raftlog.ReadBudget{Rows: 1, Bytes: 4096})
		if err != nil {
			t.Fatal(err)
		}
		data = append(data, c.Data...)
		if c.Final {
			break
		}
	}
	if !e.final {
		t.Fatal("wrong source", e.final, data)
	}
	assertSenderOldReplay(t, data, m)
	if _, err := d.ReportSnapshotSend(e, true); err != nil {
		t.Fatal(err)
	}
	if _, err := d.ReportSnapshotSend(e, false); !errors.Is(err, ErrInvalid) {
		t.Fatal("stale feedback admitted", err)
	}
	u, err := d.store.ApplicationTransferUsage()
	if err != nil || u.Exports != 0 || u.PinnedLogicalBytes != 0 || d.snapshotSender.ownedBytes != snapshotSenderBaseBytes {
		t.Fatal(u, err, d.snapshotSender.ownedBytes)
	}
	// The public mode fences remain closed even after this sender-only fixture.
	if _, err := d.Step(Packet{}); !errors.Is(err, ErrInvalid) {
		t.Fatal("Step fence", err)
	}
	cfg := d.config
	fenceMachine := &semanticFenceMachine{}
	cfg.ApplicationMachine = fenceMachine
	if _, err := Open(cfg); !errors.Is(err, ErrInvalid) || fenceMachine.restored {
		t.Fatal("Open fence", err, fenceMachine.restored)
	}
}

func TestSnapshotSendLimitsFixedWidthsAndBeforeCopyAdmission(t *testing.T) {
	d, w := senderFixture(t, vfs.NewMem(), ApplicationSnapshotSendLimits{MaxOffers: 2, MaxOwnedBytes: 1 << 20})
	senderLeader(t, d)
	// Fixed allowances count Go representation separately from wire widths, and
	// include two bounded descriptor/protobuf copies plus channels and slot arrays.
	base := unsafe.Sizeof(applicationSnapshotSender{}) + 2*unsafe.Sizeof([16]*SnapshotSend{})
	fixed := unsafe.Sizeof(SnapshotSend{}) + 2*(unsafe.Sizeof(pb.Snapshot{})+unsafe.Sizeof(pb.SnapshotMetadata{})+unsafe.Sizeof(pb.ConfState{})) + 2*512 + 3*8 + 256
	if base > uintptr(snapshotSenderBaseBytes) || fixed > uintptr(snapshotSendFixedBytes) {
		t.Fatal("fixed allowance", base, fixed)
	}
	worst := &pb.Message{Type: pb.MsgSnap.Enum(), From: new(uint64(math.MaxUint64 - 8)), To: new(uint64(math.MaxUint64 - 9)), Term: new(uint64(math.MaxUint64 - 2)), Snapshot: &pb.Snapshot{Data: make([]byte, 512), Metadata: &pb.SnapshotMetadata{Index: new(uint64(math.MaxUint64 - 2)), Term: new(uint64(math.MaxUint64 - 2)), ConfState: &pb.ConfState{Voters: []uint64{math.MaxUint64 - 10, math.MaxUint64 - 9, math.MaxUint64 - 8}, AutoLeave: new(false)}}}}
	if n := proto.Size(worst) + applicationPacketHeaderBytes + int(unsafe.Sizeof(Packet{})) + int(unsafe.Sizeof((*SnapshotSend)(nil))); n > snapshotSendOutputBytes {
		t.Fatal("wire/output reservation", n)
	}
	cfg := d.config
	needOutput := cfg.MaxPacketBytes + cfg.ApplicationSnapshotSends.MaxOffers*snapshotSendOutputBytes
	cfg.MaxOutputBytes = needOutput
	if err := cfg.ApplicationSnapshotSends.validate(cfg); err != nil {
		t.Fatal(err)
	}
	cfg.MaxOutputBytes--
	if err := cfg.ApplicationSnapshotSends.validate(cfg); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	for _, l := range []ApplicationSnapshotSendLimits{{MaxOffers: -1, MaxOwnedBytes: 1 << 20}, {MaxOffers: 17, MaxOwnedBytes: 1 << 20}, {MaxOffers: 1}, {MaxOwnedBytes: 1 << 20}, {MaxOffers: 1, MaxOwnedBytes: 256<<20 + 1}} {
		if err := l.validate(d.config); !errors.Is(err, ErrInvalid) {
			t.Fatal(l, err)
		}
	}
	if err := (ApplicationSnapshotSendLimits{}).validate(Config{}); err != nil {
		t.Fatal(err)
	}
	cut, err := d.store.PublishedApplicationCut()
	if err != nil {
		t.Fatal(err)
	}
	need := snapshotSenderBaseBytes + snapshotSendFixedBytes + cut.ImageBytes
	before, err := d.store.ApplicationTransferUsage()
	if err != nil {
		t.Fatal(err)
	}
	d.mu.Lock()
	d.snapshotSender.policy.MaxOwnedBytes = need - 1
	snap, err := w.Snapshot()
	d.unlock()
	if err != raft.ErrSnapshotTemporarilyUnavailable || snap != nil {
		t.Fatal("exact sentinel", snap, err)
	}
	after, _ := d.store.ApplicationTransferUsage()
	if !reflect.DeepEqual(before, after) || d.snapshotSender.ownedBytes != snapshotSenderBaseBytes || d.snapshotSender.offerCount() != 0 {
		t.Fatal("quota mutated source", before, after)
	}
	d.mu.Lock()
	d.snapshotSender.policy.MaxOwnedBytes = need
	snap, err = w.Snapshot()
	d.unlock()
	if err != raft.ErrSnapshotTemporarilyUnavailable || snap != nil {
		t.Fatal(snap, err)
	}
	e := senderOffer(t, d)
	buildSender(t, e)
	if cap(e.manifest.Image) != len(e.manifest.Image) || e.ownedBytes != snapshotSendFixedBytes+uint64(cap(e.manifest.Image)) || d.snapshotSender.ownedBytes != need {
		t.Fatal("capacity ledger", cap(e.manifest.Image), e.ownedBytes, d.snapshotSender.ownedBytes)
	}
	copyOwner := *e
	if err := copyOwner.Close(); !errors.Is(err, ErrInvalid) {
		t.Fatal("copied capability admitted", err)
	}
	if err := d.SealSnapshotSend(&copyOwner); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	d.mu.Lock()
	d.snapshotSender.nextID = math.MaxUint64
	snap, err = w.Snapshot()
	d.unlock()
	if err != raft.ErrSnapshotTemporarilyUnavailable || snap != nil || d.snapshotSender.offerCount() != 0 {
		t.Fatal("offer ID wrapped", err)
	}
}

func TestSnapshotSendFailureCancelRetryAndStaleFeedback(t *testing.T) {
	d, _ := senderFixture(t, vfs.NewMem(), ApplicationSnapshotSendLimits{MaxOffers: 2, MaxOwnedBytes: 1 << 20})
	senderLeader(t, d)
	senderRequest(t, d, 3)
	first := senderOffer(t, d)
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	// No callback succeeded, so cancellation has no pending peer to clear.
	senderRequest(t, d, 3)
	e := senderOffer(t, d)
	if e.id == first.id {
		t.Fatal("offer reused")
	}
	buildSender(t, e)
	if err := d.SealSnapshotSend(e); err != nil {
		t.Fatal(err)
	}
	_, sends := senderRequest(t, d, 3)
	if len(sends) != 1 {
		t.Fatal(sends)
	}
	if err := e.Close(); !errors.Is(err, ErrInvalid) {
		t.Fatal("bound cancel bypassed feedback", err)
	}
	if _, err := d.ReportSnapshot(3, true); !errors.Is(err, ErrInvalid) {
		t.Fatal("legacy feedback bypass", err)
	}
	if _, err := d.ReportSnapshotSend(e, false); err != nil {
		t.Fatal(err)
	}
	// Failed feedback moves Progress back to Probe; its normal heartbeat retry
	// permits another snapshot request at the same peer and term.
	senderEvent(t, d, func() error {
		return d.raw.Step(&pb.Message{Type: pb.MsgHeartbeatResp.Enum(), From: new(uint64(3)), To: new(uint64(1)), Term: new(uint64(2))})
	})
	replacement := senderOffer(t, d)
	buildSender(t, replacement)
	if err := d.SealSnapshotSend(replacement); err != nil {
		t.Fatal(err)
	}
	_, sends = senderEvent(t, d, func() error {
		return d.raw.Step(&pb.Message{Type: pb.MsgHeartbeatResp.Enum(), From: new(uint64(3)), To: new(uint64(1)), Term: new(uint64(2))})
	})
	if len(sends) != 1 || sends[0] != replacement || replacement.to != 3 || replacement.term != e.term || replacement.id == e.id {
		t.Fatal("replacement", sends)
	}
	for _, old := range []*SnapshotSend{first, e} {
		if _, err := d.ReportSnapshotSend(old, false); !errors.Is(err, ErrInvalid) {
			t.Fatal("stale feedback", err)
		}
	}
	if err := e.Close(); err != nil {
		t.Fatal("released Close", err)
	}
	if _, err := d.ReportSnapshotSend(replacement, false); err != nil {
		t.Fatal(err)
	}
}

func TestSnapshotSendPreparingAndSealedTermRevocation(t *testing.T) {
	for _, sealed := range []bool{false, true} {
		t.Run(fmt.Sprint(sealed), func(t *testing.T) {
			d, _ := senderFixture(t, vfs.NewMem(), ApplicationSnapshotSendLimits{MaxOffers: 2, MaxOwnedBytes: 1 << 20})
			senderLeader(t, d)
			senderRequest(t, d, 3)
			e := senderOffer(t, d)
			if sealed {
				buildSender(t, e)
				if err := d.SealSnapshotSend(e); err != nil {
					t.Fatal(err)
				}
			}
			d.mu.Lock()
			err := d.raw.Step(&pb.Message{Type: pb.MsgHeartbeat.Enum(), From: new(uint64(2)), To: new(uint64(1)), Term: new(uint64(3))})
			d.reconcileSnapshotSends()
			d.unlock()
			if err != nil {
				t.Fatal(err)
			}
			if !e.closed || d.snapshotSender.offerCount() != 0 || d.snapshotSender.ownedBytes != snapshotSenderBaseBytes {
				t.Fatal("old unbound term retained", e.closed)
			}
			if err := d.SealSnapshotSend(e); !errors.Is(err, ErrInvalid) {
				t.Fatal(err)
			}
			if err := e.Close(); err != nil {
				t.Fatal(err)
			}
			u, err := d.store.ApplicationTransferUsage()
			if err != nil || u.Exports != 0 || u.PinnedLogicalBytes != 0 {
				t.Fatal(u, err)
			}
		})
	}
}

func TestSnapshotSendConcurrentIdempotentBuildNeverChangesQueuedPointer(t *testing.T) {
	d, _ := senderFixture(t, vfs.NewMem(), ApplicationSnapshotSendLimits{MaxOffers: 2, MaxOwnedBytes: 1 << 20})
	senderLeader(t, d)
	senderRequest(t, d, 3)
	e := senderOffer(t, d)
	buildSender(t, e)
	original := e.snapshot
	var wg sync.WaitGroup
	failures := make(chan error, 40)
	for range 20 {
		wg.Go(func() {
			ready, err := e.Build(t.Context(), raftlog.ReadBudget{Rows: 1, Bytes: 4096})
			if err != nil {
				failures <- err
			}
			if !ready {
				failures <- errors.New("idempotent build not ready")
			}
		})
	}
	if err := d.SealSnapshotSend(e); err != nil {
		t.Fatal(err)
	}
	_, sends := senderRequest(t, d, 3)
	if len(sends) != 1 {
		t.Fatal(sends)
	}
	wg.Wait()
	close(failures)
	for err := range failures {
		t.Fatal(err)
	}
	if e.snapshot != original {
		t.Fatal("queued callback pointer replaced")
	}
	if _, err := d.ReportSnapshotSend(e, false); err != nil {
		t.Fatal(err)
	}
}

func TestSnapshotSendBlockedPhysicalReadCancelKeepsChargeAndFatalStops(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	var releaseOnce, startedOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	cause := errors.New("sender actual SST read failure")
	var armed atomic.Bool
	var reads atomic.Int32
	fs := errorfs.Wrap(vfs.NewMem(), errorfs.InjectorFunc(func(op errorfs.Op) error {
		if !armed.Load() || op.Kind != errorfs.OpFileReadAt || !strings.HasSuffix(op.Path, ".sst") {
			return nil
		}
		pcs := make([]uintptr, 32)
		n := runtime.Callers(2, pcs)
		frames := runtime.CallersFrames(pcs[:n])
		inBuild := false
		for {
			f, more := frames.Next()
			if strings.Contains(f.Function, "ApplicationExport).BuildManifest") {
				inBuild = true
			}
			if !more {
				break
			}
		}
		if !inBuild {
			return nil
		}
		reads.Add(1)
		startedOnce.Do(func() { close(started) })
		<-release
		return cause
	}))
	d, _ := senderFixtureStorage(t, fs, ApplicationSnapshotSendLimits{MaxOffers: 1, MaxOwnedBytes: 1 << 20}, true)
	senderLeader(t, d)
	senderRequest(t, d, 3)
	e := senderOffer(t, d)
	t.Cleanup(unblock)
	armed.Store(true)
	result := make(chan error, 1)
	go func() { _, err := e.Build(t.Context(), raftlog.ReadBudget{Rows: 1, Bytes: 4096}); result <- err }()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("no physical SST read reached Build")
	}
	tick := make(chan error, 1)
	go func() { _, err := d.Tick(); tick <- err }()
	select {
	case err := <-tick:
		if !errors.Is(err, ErrUnavailable) {
			t.Fatal("busy event touched RawNode", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Driver.mu held across page")
	}
	canceled := make(chan error, 1)
	go func() { canceled <- e.Close() }()
	// Close detaches promptly, but keeps both owner slot and owned bytes while
	// the blocked page still owns the export. Observe under the short Driver lock.
	deadline := time.Now().Add(time.Second)
	detached := false
	for time.Now().Before(deadline) {
		d.mu.Lock()
		detached = e.closed
		count := d.snapshotSender.offerCount()
		charge := d.snapshotSender.ownedBytes
		d.mu.Unlock()
		if detached {
			if count != 1 || charge <= snapshotSenderBaseBytes {
				t.Fatal("early cleanup quota reuse", count, charge)
			}
			break
		}
		runtime.Gosched()
	}
	if !detached {
		t.Fatal("cancel could not detach")
	}
	select {
	case err := <-canceled:
		t.Fatal("cancel returned before blocked ownership release", err)
	default:
	}
	unblock()
	select {
	case err := <-result:
		if !errors.Is(err, cause) || errors.Is(err, raftlog.ErrCorrupt) || pebble.IsCorruptionError(err) {
			t.Fatal("lost operational cause", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("build did not finish")
	}
	select {
	case err := <-canceled:
		if !errors.Is(err, cause) {
			t.Fatal("cancel Close lost source cause", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancel did not finish")
	}
	if reads.Load() == 0 {
		t.Fatal("not a real SST failure")
	}
	if _, err := d.Tick(); !errors.Is(err, ErrStopped) || !errors.Is(err, cause) {
		t.Fatal("canceled fatal owner did not stop Driver", err)
	}
	d.mu.Lock()
	count, charge := d.snapshotSender.offerCount(), d.snapshotSender.ownedBytes
	d.mu.Unlock()
	if count != 0 || charge != snapshotSenderBaseBytes {
		t.Fatal("leaked fatal owner", count, charge)
	}
}

func TestSnapshotSendCallbackFatalUsesExactSentinelAndNormalStop(t *testing.T) {
	var armed atomic.Bool
	cause := errors.New("sender published image SST read failure")
	var reached atomic.Int32
	fs := errorfs.Wrap(vfs.NewMem(), errorfs.InjectorFunc(func(op errorfs.Op) error {
		if armed.Load() && op.Kind == errorfs.OpFileReadAt && strings.HasSuffix(op.Path, ".sst") {
			reached.Add(1)
			return cause
		}
		return nil
	}))
	d, w := senderFixtureStorage(t, fs, ApplicationSnapshotSendLimits{MaxOffers: 1, MaxOwnedBytes: 1 << 20}, true)
	senderLeader(t, d)
	armed.Store(true)
	d.mu.Lock()
	snap, err := w.Snapshot()
	if snap != nil || err != raft.ErrSnapshotTemporarilyUnavailable || !errors.Is(d.snapshotSender.fatal, cause) || errors.Is(d.snapshotSender.fatal, raftlog.ErrCorrupt) {
		d.unlock()
		t.Fatal(snap, err, d.snapshotSender.fatal)
	}
	out, stopErr := d.drain()
	d.unlock()
	if !errors.Is(stopErr, cause) || len(out.Packets) != 0 || len(out.SnapshotSends) != 0 || reached.Load() == 0 {
		t.Fatal("callback failed to stop before output", out, stopErr, reached.Load())
	}
	if _, err := d.Tick(); !errors.Is(err, ErrStopped) || !errors.Is(err, cause) {
		t.Fatal(err)
	}
}

func TestSnapshotSendDistinctFollowerStreamsAndPinRefusal(t *testing.T) {
	d, w := senderFixture(t, vfs.NewMem(), ApplicationSnapshotSendLimits{MaxOffers: 3, MaxOwnedBytes: 1 << 20})
	senderLeader(t, d)
	callback := func() *pb.Snapshot {
		t.Helper()
		d.mu.Lock()
		snap, err := w.Snapshot()
		d.unlock()
		if err != nil && err != raft.ErrSnapshotTemporarilyUnavailable {
			t.Fatal(err)
		}
		return snap
	}
	if callback() != nil {
		t.Fatal("unbuilt callback")
	}
	one := senderOffer(t, d)
	buildSender(t, one)
	if err := d.SealSnapshotSend(one); err != nil {
		t.Fatal(err)
	}
	snapOne := callback()
	if snapOne == nil {
		t.Fatal("sealed callback missing")
	}
	if callback() != nil {
		t.Fatal("second unbuilt callback")
	}
	two := senderOffer(t, d)
	buildSender(t, two)
	if err := d.SealSnapshotSend(two); err != nil {
		t.Fatal(err)
	}
	snapTwo := callback()
	if snapTwo == nil || snapTwo == snapOne || one.id == two.id {
		t.Fatal("shared follower offer")
	}
	// Store MaxExports=2 independently refuses a third source; the sender count
	// policy has room, but its pre-reservation must roll back without leaked bytes.
	before, _ := d.store.ApplicationTransferUsage()
	charge := d.snapshotSender.ownedBytes
	if callback() != nil {
		t.Fatal("pin-refused callback returned snapshot")
	}
	after, _ := d.store.ApplicationTransferUsage()
	if !reflect.DeepEqual(before, after) || d.snapshotSender.ownedBytes != charge || d.snapshotSender.offerCount() != 2 {
		t.Fatal("pin refusal leak", before, after)
	}
	d.mu.Lock()
	for j, item := range []struct {
		s *pb.Snapshot
		e *SnapshotSend
	}{{snapOne, one}, {snapTwo, two}} {
		e, err := d.bindSnapshotSend(&pb.Message{Type: pb.MsgSnap.Enum(), From: new(uint64(1)), To: new(uint64(j + 2)), Term: new(uint64(2)), Snapshot: item.s})
		if err != nil || e != item.e {
			d.unlock()
			t.Fatal("distinct actual-message binding", e, err)
		}
	}
	d.unlock()
	a, err := one.Next(t.Context(), raftlog.ReadBudget{Rows: 1, Bytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	b, err := two.Next(t.Context(), raftlog.ReadBudget{Rows: 1, Bytes: 4096})
	if err != nil || !reflect.DeepEqual(a, b) {
		t.Fatal("shared replay cursor", a, b, err)
	}
	// These direct callback-message fixtures do not mutate tracker progress, so
	// terminal release uses Driver.Close rather than forging transport feedback.
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	if err := one.Close(); err != nil {
		t.Fatal(err)
	}
	if err := two.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSnapshotSendReadyOutputUsesReservedPartition(t *testing.T) {
	d, _ := senderFixture(t, vfs.NewMem(), ApplicationSnapshotSendLimits{MaxOffers: 1, MaxOwnedBytes: 1 << 20})
	senderLeader(t, d)
	senderRequest(t, d, 3)
	e := senderOffer(t, d)
	buildSender(t, e)
	if err := d.SealSnapshotSend(e); err != nil {
		t.Fatal(err)
	}
	// Sender-only drain fixture: the public application Ready guard still forbids
	// all outgoing traffic. Temporarily remove that mode guard only in the test
	// after the exact Store/Stage history is already applied; no incoming snapshot
	// or committed application entry is present in this outgoing Ready.
	d.mu.Lock()
	var previous uint64
	d.raw.WithProgress(func(id uint64, _ raft.ProgressType, p tracker.Progress) {
		if id == 3 {
			previous = p.Next - 1
		}
	})
	if err := d.raw.Step(&pb.Message{Type: pb.MsgAppResp.Enum(), From: new(uint64(3)), To: new(uint64(1)), Term: new(uint64(2)), Index: new(previous), Reject: new(true), RejectHint: new(uint64(1)), LogTerm: new(uint64(1))}); err != nil {
		d.unlock()
		t.Fatal(err)
	}
	app := d.applicationMachine
	d.applicationMachine = nil
	// Read results consume only the ordinary partition; the callback's reserved
	// snapshot owner is returned beside its exact Packet, without a second quota.
	d.config.MaxOutputBytes = d.config.MaxPacketBytes + snapshotSendOutputBytes
	d.reads["fixture"] = &pendingRead{resolved: true, index: d.applied, user: []byte("read")}
	out, err := d.drain()
	d.applicationMachine = app
	d.unlock()
	if err != nil || len(out.SnapshotSends) != 1 || out.SnapshotSends[0] != e || len(out.Reads) != 1 {
		t.Fatal(out, err)
	}
	count := 0
	for _, p := range out.Packets {
		if p.Snapshot {
			count++
			var m pb.Message
			if err := proto.Unmarshal(p.Payload, &m); err != nil || !bytes.Equal(m.GetSnapshot().Data, e.descriptor) || m.GetTo() != 3 {
				t.Fatal(&m, err)
			}
		}
	}
	if count != 1 {
		t.Fatal("missing paired packet", out)
	}
	if _, err := d.ReportSnapshotSend(e, false); err != nil {
		t.Fatal(err)
	}
}

func TestSnapshotSendGuardsCancellationAndOwnership(t *testing.T) {
	var nilDriver *Driver
	if _, err := nilDriver.SnapshotSends(); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if err := nilDriver.SealSnapshotSend(nil); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := nilDriver.ReportSnapshotSend(nil, false); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	scalar, _ := driver(t, 1, []uint64{1}, vfs.NewMem())
	if _, err := scalar.SnapshotSends(); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	d, w := senderFixture(t, vfs.NewMem(), ApplicationSnapshotSendLimits{MaxOffers: 1, MaxOwnedBytes: 1 << 20})
	senderLeader(t, d)
	senderRequest(t, d, 3)
	e := senderOffer(t, d)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := e.Build(ctx, raftlog.ReadBudget{Rows: 1, Bytes: 4096}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := e.Build(nil, raftlog.ReadBudget{}); !errors.Is(err, ErrInvalid) { //nolint:staticcheck // Deliberately verify the nil-context refusal contract.
		t.Fatal(err)
	}
	if _, err := e.Build(t.Context(), raftlog.ReadBudget{Rows: 1, Bytes: 1}); !errors.Is(err, raftlog.ErrLimit) {
		t.Fatal(err)
	}
	if d.stopped != nil || e.ready {
		t.Fatal("retryable refusal stopped driver")
	}
	buildSender(t, e)
	m, err := e.Manifest()
	if err != nil {
		t.Fatal(err)
	}
	m.Image[0] ^= 1
	m.ConfState.Voters[0] = 99
	again, err := e.Manifest()
	if err != nil || again.Image[0] == m.Image[0] || again.ConfState.Voters[0] == 99 {
		t.Fatal("Manifest alias", again, err)
	}
	if err := scalar.SealSnapshotSend(e); !errors.Is(err, ErrInvalid) {
		t.Fatal("foreign driver", err)
	}
	if _, err := scalar.ReportSnapshotSend(e, false); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	d.mu.Lock()
	d.snapshotSender.busy = true
	snap, err := w.Snapshot()
	d.snapshotSender.busy = false
	d.unlock()
	if err != raft.ErrSnapshotTemporarilyUnavailable || snap != nil {
		t.Fatal("busy exact sentinel", err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := d.SnapshotSends(); !errors.Is(err, ErrStopped) {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSnapshotSendOddImageExactCapacityAndZeroHandleReopen(t *testing.T) {
	fs := vfs.NewMem()
	d, w := senderFixtureStorage(t, fs, ApplicationSnapshotSendLimits{MaxOffers: 1, MaxOwnedBytes: 1 << 20}, false, []byte("odd-nine!"))
	senderLeader(t, d)
	cut, err := d.store.PublishedApplicationCut()
	if err != nil {
		t.Fatal(err)
	}
	if cut.ImageBytes != 9 {
		t.Fatal("odd fixture", cut)
	}
	need := snapshotSenderBaseBytes + snapshotSendFixedBytes + 9
	d.mu.Lock()
	d.snapshotSender.policy.MaxOwnedBytes = need
	snap, err := w.Snapshot()
	d.unlock()
	if snap != nil || err != raft.ErrSnapshotTemporarilyUnavailable {
		t.Fatal(snap, err)
	}
	e := senderOffer(t, d)
	m := buildSender(t, e)
	if len(e.manifest.Image) != 9 || cap(e.manifest.Image) != 9 || d.snapshotSender.ownedBytes != need {
		t.Fatal("rounded backing undercharged", len(e.manifest.Image), cap(e.manifest.Image), d.snapshotSender.ownedBytes)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	p := raftlog.DefaultApplicationPolicy(1)
	tc := raftlog.ApplicationTransferConfig{Identity: raftlog.ApplicationIdentity{Graph: [16]byte{1}, Partition: 1, Group: [16]byte{2}}, Contract: raftlog.ApplicationContractForPolicy(p), Limits: raftlog.DefaultApplicationTransferLimits()}
	limits := raftlog.DefaultLimits()
	limits.CacheBytes = 1
	s, err := raftlog.Open(raftlog.Config{Dir: "sender", FS: fs, Application: p, Transfer: tc, Limits: limits, Generations: raftlog.ApplicationGenerationLimits{MaxBytes: 2 << 30, MaxRecords: 2_000_000}, PublishedCuts: raftlog.ApplicationPublishedCutLimits{MaxTransferChunks: 10000}, Replication: raftlog.ApplicationReplicationConfig{Voters: [3]uint64{1, 2, 3}}, SemanticContractID: raftlog.ApplicationSemanticContractID{7}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	recovered, err := s.PublishedApplicationCut()
	if err != nil || recovered.ID != m.CutID || recovered.Index != 2 {
		t.Fatal("durable cut moved after zero handles/reopen", recovered, err)
	}
	export, err := s.BeginPublishedApplicationExport(t.Context(), recovered.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer export.Close()
	for range 100 {
		ready, err := export.BuildManifest(t.Context(), raftlog.ReadBudget{Rows: 1, Bytes: 4096})
		if err != nil {
			t.Fatal(err)
		}
		if ready {
			again, err := export.Manifest()
			if err != nil || !bytes.Equal(again.Image, m.Image) || again.CutID != m.CutID {
				t.Fatal(again, err)
			}
			return
		}
	}
	t.Fatal("unbounded reopened build")
}

type senderCorruptFS struct {
	vfs.FS
	armed   atomic.Bool
	reached atomic.Int32
}
type senderCorruptFile struct {
	vfs.File
	fs   *senderCorruptFS
	path string
}

func (fs *senderCorruptFS) Open(path string, opts ...vfs.OpenOption) (vfs.File, error) {
	f, err := fs.FS.Open(path, opts...)
	if err != nil {
		return nil, err
	}
	return &senderCorruptFile{File: f, fs: fs, path: path}, nil
}
func (f *senderCorruptFile) ReadAt(b []byte, off int64) (int, error) {
	n, err := f.File.ReadAt(b, off)
	if err == nil && n > 0 && f.fs.armed.Load() && strings.HasSuffix(f.path, ".sst") {
		b[0] ^= 1
		f.fs.reached.Add(1)
	}
	return n, err
}
func TestSnapshotSendPhysicalChecksumCorruptionStopsBeforeSeal(t *testing.T) {
	if os.Getenv("RHO_SENDER_CORRUPTION_CHILD") != "" {
		fs := &senderCorruptFS{FS: vfs.NewMem()}
		d, _ := senderFixtureStorage(t, fs, ApplicationSnapshotSendLimits{MaxOffers: 1, MaxOwnedBytes: 1 << 20}, true)
		senderLeader(t, d)
		senderRequest(t, d, 3)
		e := senderOffer(t, d)
		fs.armed.Store(true)
		ready, err := e.Build(t.Context(), raftlog.ReadBudget{Rows: 1, Bytes: 4096})
		_, _ = os.Stdout.WriteString("CORRUPT_SENDER_BUILD_RETURNED\n")
		t.Fatalf("default corruption fail-stop returned: %v %v", ready, err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestSnapshotSendPhysicalChecksumCorruptionStopsBeforeSeal$")
	cmd.Env = append(os.Environ(), "RHO_SENDER_CORRUPTION_CHILD=1")
	output, err := cmd.CombinedOutput()
	exit, ok := errors.AsType[*exec.ExitError](err)
	if ctx.Err() != nil || !ok || exit.ExitCode() != 1 || !bytes.Contains(output, []byte("on-disk corruption")) || !bytes.Contains(output, []byte("crc32c checksum mismatch")) || bytes.Contains(output, []byte("CORRUPT_SENDER_BUILD_RETURNED")) {
		t.Fatalf("physical checksum fail-stop: %v %s", err, output)
	}
}

func TestSnapshotSendAppResponseRetiresStaleOwnerWithoutFeedback(t *testing.T) {
	d, _ := senderFixture(t, vfs.NewMem(), ApplicationSnapshotSendLimits{MaxOffers: 1, MaxOwnedBytes: 1 << 20})
	senderLeader(t, d)
	senderRequest(t, d, 3)
	e := senderOffer(t, d)
	buildSender(t, e)
	if err := d.SealSnapshotSend(e); err != nil {
		t.Fatal(err)
	}
	_, sends := senderRequest(t, d, 3)
	if len(sends) != 1 {
		t.Fatal(sends)
	}
	d.mu.Lock()
	// A real append response at the pending index transitions Progress out of
	// StateSnapshot without transport feedback. The old offer must then release.
	if err := d.raw.Step(&pb.Message{Type: pb.MsgAppResp.Enum(), From: new(uint64(3)), To: new(uint64(1)), Term: new(uint64(2)), Index: new(e.manifest.Index)}); err != nil {
		d.unlock()
		t.Fatal(err)
	}
	var before tracker.Progress
	d.raw.WithProgress(func(id uint64, _ raft.ProgressType, p tracker.Progress) {
		if id == 3 {
			before = p
		}
	})
	if before.State == tracker.StateSnapshot {
		d.unlock()
		t.Fatal("fixture did not leave snapshot progress", before)
	}
	d.reconcileSnapshotSends()
	d.unlock()
	if !e.closed || d.snapshotSender.offerCount() != 0 {
		t.Fatal("stale bound pin retained")
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := d.ReportSnapshotSend(e, false); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	d.mu.Lock()
	var after tracker.Progress
	d.raw.WithProgress(func(id uint64, _ raft.ProgressType, p tracker.Progress) {
		if id == 3 {
			after = p
		}
	})
	d.mu.Unlock()
	if !reflect.DeepEqual(before, after) {
		t.Fatal("late feedback altered progressed peer", before, after)
	}
	u, err := d.store.ApplicationTransferUsage()
	if err != nil || u.Exports != 0 || u.PinnedLogicalBytes != 0 {
		t.Fatal(u, err)
	}
}

func TestSnapshotSendExactAssociationRejectsCopiesAndChangedMetadata(t *testing.T) {
	d, w := senderFixture(t, vfs.NewMem(), ApplicationSnapshotSendLimits{MaxOffers: 1, MaxOwnedBytes: 1 << 20})
	senderLeader(t, d)
	senderRequest(t, d, 3)
	e := senderOffer(t, d)
	buildSender(t, e)
	if err := d.SealSnapshotSend(e); err != nil {
		t.Fatal(err)
	}
	d.mu.Lock()
	snap, err := w.Snapshot()
	if err != nil {
		d.unlock()
		t.Fatal(err)
	}
	m := &pb.Message{Type: pb.MsgSnap.Enum(), From: new(uint64(1)), To: new(uint64(3)), Term: new(uint64(2)), Snapshot: snap}
	copied := proto.Clone(m).(*pb.Message)
	if _, err := d.bindSnapshotSend(copied); !errors.Is(err, ErrInvalid) {
		d.unlock()
		t.Fatal("copied snapshot associated", err)
	}
	original := snap.Metadata.Index
	snap.Metadata.Index = new(uint64(99))
	if _, err := d.bindSnapshotSend(m); !errors.Is(err, ErrInvalid) {
		d.unlock()
		t.Fatal("changed callback metadata", err)
	}
	snap.Metadata.Index = original
	data := bytes.Clone(snap.Data)
	snap.Data[0] ^= 1
	if _, err := d.bindSnapshotSend(m); !errors.Is(err, ErrInvalid) {
		d.unlock()
		t.Fatal("changed descriptor", err)
	}
	copy(snap.Data, data)
	got, err := d.bindSnapshotSend(m)
	if err != nil || got != e {
		d.unlock()
		t.Fatal(got, err)
	}
	if _, err := d.bindSnapshotSend(m); !errors.Is(err, ErrInvalid) {
		d.unlock()
		t.Fatal("duplicate association", err)
	}
	d.unlock()
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSnapshotSendBusyCloseReleasesDriverLockAndSource(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	var once, releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	var armed atomic.Bool
	fs := errorfs.Wrap(vfs.NewMem(), errorfs.InjectorFunc(func(op errorfs.Op) error {
		if armed.Load() && op.Kind == errorfs.OpFileReadAt && strings.HasSuffix(op.Path, ".sst") {
			once.Do(func() { close(started) })
			<-release
		}
		return nil
	}))
	d, _ := senderFixtureStorage(t, fs, ApplicationSnapshotSendLimits{MaxOffers: 1, MaxOwnedBytes: 1 << 20}, true)
	senderLeader(t, d)
	senderRequest(t, d, 3)
	e := senderOffer(t, d)
	t.Cleanup(unblock)
	armed.Store(true)
	built := make(chan error, 1)
	go func() { _, err := e.Build(t.Context(), raftlog.ReadBudget{Rows: 1, Bytes: 4096}); built <- err }()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("no physical read")
	}
	closed := make(chan error, 1)
	go func() { closed <- d.Close() }()
	// Close's store wait cannot prevent an independent short Driver read.
	deadline := time.Now().Add(time.Second)
	stopped := false
	for time.Now().Before(deadline) {
		d.mu.Lock()
		stopped = d.stopped != nil
		d.mu.Unlock()
		if stopped {
			break
		}
		runtime.Gosched()
	}
	if !stopped {
		t.Fatal("Close did not stop promptly")
	}
	read := make(chan uint64, 1)
	go func() { read <- d.Applied() }()
	select {
	case <-read:
	case <-time.After(time.Second):
		t.Fatal("Close holds Driver.mu while waiting for Store")
	}
	unblock()
	select {
	case err := <-built:
		if err != nil && !errors.Is(err, raftlog.ErrClosed) {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("page did not finish")
	}
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not finish")
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	if d.snapshotSender.offerCount() != 0 || d.snapshotSender.ownedBytes != snapshotSenderBaseBytes {
		t.Fatal("Close retained owner")
	}
}

func TestSnapshotSendProgressOnlyNoReadyReleasesOwner(t *testing.T) {
	d, _ := senderFixture(t, vfs.NewMem(), ApplicationSnapshotSendLimits{MaxOffers: 1, MaxOwnedBytes: 1 << 20})
	senderLeader(t, d)
	senderEvent(t, d, func() error {
		return d.raw.Step(&pb.Message{Type: pb.MsgAppResp.Enum(), From: new(uint64(3)), To: new(uint64(1)), Term: new(uint64(2)), Index: new(uint64(3))})
	})
	senderEvent(t, d, func() error { return d.raw.Propose(command(11)) })
	senderEvent(t, d, func() error {
		return d.raw.Step(&pb.Message{Type: pb.MsgAppResp.Enum(), From: new(uint64(2)), To: new(uint64(1)), Term: new(uint64(2)), Index: new(uint64(4))})
	})
	if err := d.store.PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	senderRequest(t, d, 3)
	e := senderOffer(t, d)
	buildSender(t, e)
	if err := d.SealSnapshotSend(e); err != nil {
		t.Fatal(err)
	}
	_, sends := senderRequest(t, d, 3)
	if len(sends) != 1 {
		t.Fatal(sends)
	}
	d.mu.Lock()
	if err := d.raw.Step(&pb.Message{Type: pb.MsgAppResp.Enum(), From: new(uint64(3)), To: new(uint64(1)), Term: new(uint64(2)), Index: new(uint64(4))}); err != nil {
		d.unlock()
		t.Fatal(err)
	}
	if d.raw.HasReady() {
		d.unlock()
		t.Fatal("fixture did not produce progress-only/no-Ready transition")
	}
	out, err := d.drain()
	d.unlock()
	if err != nil || len(out.Packets) != 0 || !e.closed || d.snapshotSender.offerCount() != 0 {
		t.Fatal("no-Ready event stranded snapshot owner", out, err, e.closed)
	}
	u, err := d.store.ApplicationTransferUsage()
	if err != nil || u.Exports != 0 || u.PinnedLogicalBytes != 0 {
		t.Fatal(u, err)
	}
}

func assertSenderOldReplay(t *testing.T, data []byte, m raftlog.ApplicationSnapshotManifest) {
	t.Helper()
	expected := map[string][]byte{}
	for _, tag := range []byte{9, 10, 11} {
		for index := uint64(1); index <= 2; index++ {
			key := binary.BigEndian.AppendUint64([]byte{tag}, index)
			expected[string(key)] = nil
		}
	}
	dataKey := binary.BigEndian.AppendUint64([]byte{8, 'a', 0, 0}, ^uint64(2))
	expected[string(dataKey)] = command(7)
	expected[string(binary.BigEndian.AppendUint64([]byte{9}, 2))] = command(7)
	expected[string(binary.BigEndian.AppendUint64([]byte{10}, 2))] = append(command(0), command(7)...)
	expected[string(binary.BigEndian.AppendUint64([]byte{11}, 2))] = []byte("accepted")
	var namespaceBytes, namespaceRows [4]uint64
	var previous []byte
	for len(data) > 0 {
		if len(data) < 8 {
			t.Fatal("truncated canonical frame")
		}
		kl, vl := int(binary.BigEndian.Uint32(data[:4])), int(binary.BigEndian.Uint32(data[4:8]))
		data = data[8:]
		if kl < 9 || vl < 36 || kl > len(data) || vl > len(data)-kl {
			t.Fatal("invalid frame lengths")
		}
		key, raw := data[:kl], data[kl:kl+vl]
		data = data[kl+vl:]
		value, wanted := expected[string(key)]
		if !wanted || !bytes.Equal(raw[36:], value) || raw[3] != 0 || previous != nil && bytes.Compare(previous, key) >= 0 {
			t.Fatal("unexpected/duplicate/current-cut replay", key, raw[36:], value)
		}
		index := binary.BigEndian.Uint64(key[len(key)-8:])
		if key[0] == 8 {
			index = ^index
		}
		if bytes.Equal(raw[36:], command(18)) || index > 2 {
			t.Fatal("later concrete value/index leaked", key, raw[36:])
		}
		h := sha256.New()
		_, _ = h.Write(key)
		_, _ = h.Write(raw[:4])
		_, _ = h.Write(raw[36:])
		if !bytes.Equal(raw[4:36], h.Sum(nil)) {
			t.Fatal("bad canonical frame checksum")
		}
		ns := int(key[0] - 8)
		namespaceRows[ns]++
		namespaceBytes[ns] += uint64(kl + vl)
		delete(expected, string(key))
		previous = bytes.Clone(key)
	}
	if len(expected) != 0 || namespaceRows != m.NamespaceRecords || namespaceBytes != m.NamespaceBytes {
		t.Fatal("missing/inflated old history", expected, namespaceRows, m.NamespaceRecords, namespaceBytes, m.NamespaceBytes)
	}
}

type senderCloseScene struct {
	d                        *Driver
	old, current             *SnapshotSend
	view                     *raftlog.ApplicationView
	fs                       vfs.FS
	manifest                 raftlog.ApplicationSnapshotManifest
	cause                    error
	armed, block             atomic.Bool
	reached                  atomic.Int32
	started, release         chan struct{}
	startedOnce, releaseOnce sync.Once
}

func (f *senderCloseScene) unblock() { f.releaseOnce.Do(func() { close(f.release) }) }
func senderCloseFixture(t *testing.T, bound bool) *senderCloseScene {
	t.Helper()
	f := &senderCloseScene{cause: errors.New("sender retired-bank SST read poison"), started: make(chan struct{}), release: make(chan struct{})}
	armed, reached, cause := &f.armed, &f.reached, f.cause
	fs := errorfs.Wrap(vfs.NewMem(), errorfs.InjectorFunc(func(op errorfs.Op) error {
		if armed.Load() && op.Kind == errorfs.OpFileReadAt && strings.HasSuffix(op.Path, ".sst") {
			reached.Add(1)
			if f.block.Load() {
				f.startedOnce.Do(func() { close(f.started) })
				<-f.release
			}
			return cause
		}
		return nil
	}))
	d, w := senderFixtureStorage(t, fs, ApplicationSnapshotSendLimits{MaxOffers: 2, MaxOwnedBytes: 1 << 20}, true)
	senderLeader(t, d)
	senderRequest(t, d, 3)
	old := senderOffer(t, d)
	buildSender(t, old)
	if err := d.SealSnapshotSend(old); err != nil {
		t.Fatal(err)
	}
	var snapshot *pb.Snapshot
	var err error
	if bound {
		_, sends := senderRequest(t, d, 3)
		if len(sends) != 1 || sends[0] != old {
			t.Fatal(sends)
		}
	} else {
		d.mu.Lock()
		snapshot, err = w.Snapshot()
		d.unlock()
		if err != nil || snapshot == nil {
			t.Fatal(err)
		}
	}
	view, err := d.store.ApplicationView(2)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = view.Close() })
	donor, _ := senderFixture(t, vfs.NewMem(), ApplicationSnapshotSendLimits{MaxOffers: 1, MaxOwnedBytes: 1 << 20})
	senderLeader(t, donor)
	senderEvent(t, donor, func() error { return donor.raw.Propose(command(11)) })
	senderEvent(t, donor, func() error {
		return donor.raw.Step(&pb.Message{Type: pb.MsgAppResp.Enum(), From: new(uint64(2)), To: new(uint64(1)), Term: new(uint64(2)), Index: new(uint64(4))})
	})
	if err := donor.store.PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	cut, err := donor.store.PublishedApplicationCut()
	if err != nil {
		t.Fatal(err)
	}
	export, err := donor.store.BeginPublishedApplicationExport(t.Context(), cut.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer export.Close()
	ready := false
	for range 100 {
		ready, err = export.BuildManifest(t.Context(), raftlog.ReadBudget{Rows: 1, Bytes: 4096})
		if err != nil {
			t.Fatal(err)
		}
		if ready {
			break
		}
	}
	if !ready {
		t.Fatal("unbounded fixture build")
	}
	m, err := export.Manifest()
	if err != nil {
		t.Fatal(err)
	}
	imported, err := d.store.BeginApplicationImport(t.Context(), m)
	if err != nil {
		t.Fatal(err)
	}
	defer imported.Abort()
	final := false
	for range 100 {
		chunk, err := export.Next(t.Context(), raftlog.ReadBudget{Rows: 1, Bytes: 4096})
		if err != nil {
			t.Fatal(err)
		}
		if err = imported.Append(t.Context(), chunk); err != nil {
			t.Fatal(err)
		}
		if chunk.Final {
			final = true
			break
		}
	}
	if !final {
		t.Fatal("unbounded fixture import")
	}
	if err = imported.Verify(t.Context()); err != nil {
		t.Fatal(err)
	}
	prepared, err := imported.Prepare()
	if err != nil {
		t.Fatal(err)
	}
	descriptor, err := raftlog.EncodeApplicationSnapshotDescriptor(m)
	if err != nil {
		t.Fatal(err)
	}
	expected := &pb.Snapshot{Data: descriptor, Metadata: &pb.SnapshotMetadata{Index: new(m.Index), Term: new(m.Term), ConfState: m.ConfState}}
	claim, err := prepared.Claim(expected)
	if err != nil {
		t.Fatal(err)
	}
	defer claim.Close()
	hard, _, err := d.store.InitialState()
	if err != nil {
		t.Fatal(err)
	}
	hard.Commit = new(m.Index)
	// Existing accepted Store activation is used only to construct two retained
	// physical banks. The Driver never steps/restores an incoming snapshot here.
	root, err := d.store.PersistApplicationReady(raft.Ready{Snapshot: expected, HardState: hard}, claim)
	if err != nil {
		t.Fatal(err)
	}
	if root.Generation == 1 {
		t.Fatal("fixture did not switch bank")
	}
	if err = claim.Close(); err != nil {
		t.Fatal(err)
	}
	d.mu.Lock()
	snapshot, err = w.Snapshot()
	d.unlock()
	if err != raft.ErrSnapshotTemporarilyUnavailable || snapshot != nil {
		t.Fatal(snapshot, err)
	}
	current := senderOffer(t, d)
	if current == old || d.snapshotSender.offerCount() != 2 {
		t.Fatal("missing distinct-bank owner")
	}
	f.d, f.old, f.current, f.view, f.fs, f.manifest = d, old, current, view, fs, m
	t.Cleanup(f.unblock)
	return f
}
func assertSenderCloseReopen(t *testing.T, f *senderCloseScene) {
	t.Helper()
	// Fault-free reopen proves owner cleanup did not mutate/damage the accepted
	// active image/history or retain any ephemeral export/pin ownership.
	f.armed.Store(false)
	p := raftlog.DefaultApplicationPolicy(1)
	tc := raftlog.ApplicationTransferConfig{Identity: f.manifest.Identity, Contract: f.manifest.Contract, Limits: raftlog.DefaultApplicationTransferLimits()}
	limits := raftlog.DefaultLimits()
	limits.CacheBytes = 1
	reopened, err := raftlog.Open(raftlog.Config{Dir: "sender", FS: f.fs, Limits: limits, Application: p, Transfer: tc, Generations: raftlog.ApplicationGenerationLimits{MaxBytes: 2 << 30, MaxRecords: 2_000_000}, PublishedCuts: raftlog.ApplicationPublishedCutLimits{MaxTransferChunks: 10000}, Replication: raftlog.ApplicationReplicationConfig{Voters: [3]uint64{1, 2, 3}}, SemanticContractID: f.manifest.SemanticContractID})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	usage, err := reopened.ApplicationTransferUsage()
	if err != nil || usage.Exports != 0 || usage.PinnedLogicalBytes != 0 {
		t.Fatal("reopen leaked pins", usage, err)
	}
	active, err := reopened.ApplicationView(4)
	if err != nil {
		t.Fatal(err)
	}
	defer active.Close()
	row, found, err := active.Get(t.Context(), []byte("a"), 4096)
	if err != nil || !found || !bytes.Equal(row.Value, command(18)) {
		t.Fatal("cleanup damaged active data", row, found, err)
	}
}
func TestSnapshotSendCloseFailureDrainsOtherGenerationOwners(t *testing.T) {
	f := senderCloseFixture(t, false)
	d, old, current, view, cause := f.d, f.old, f.current, f.view, f.cause
	var err error
	f.armed.Store(true)
	if _, _, err = view.Get(t.Context(), []byte("a"), 4096); !errors.Is(err, cause) || errors.Is(err, raftlog.ErrCorrupt) || f.reached.Load() == 0 {
		t.Fatal("not actual old-bank operational read", err, f.reached.Load())
	}
	// Remove the view's old-bank reference first. Old.Close then reaches the
	// poisoned last-reference release while current still owns the other bank.
	if err = view.Close(); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err = old.Close(); !errors.Is(err, cause) || !errors.Is(err, raftlog.ErrPoisoned) {
			t.Fatal("owner Close lost cause", err)
		}
	}
	if _, err = d.Tick(); !errors.Is(err, ErrStopped) || !errors.Is(err, cause) {
		t.Fatal("cleanup lost original cause", err)
	}
	if !old.closed || !current.closed || d.snapshotSender.offerCount() != 0 || d.snapshotSender.cleanupN != 0 || d.snapshotSender.ownedBytes != snapshotSenderBaseBytes {
		t.Fatal("terminal cleanup left owners", old.closed, current.closed, d.snapshotSender.ownedBytes)
	}
	if _, err = d.store.ApplicationTransferUsage(); !errors.Is(err, raftlog.ErrPoisoned) || !errors.Is(err, cause) {
		t.Fatal("poison ledger diagnostic", err)
	}
	for range 2 {
		if err = d.Close(); !errors.Is(err, cause) || !errors.Is(err, raftlog.ErrPoisoned) {
			t.Fatal("Driver Close lost cleanup cause", err)
		}
	}
	if _, err = d.Tick(); !errors.Is(err, cause) || !errors.Is(err, ErrStopped) {
		t.Fatal("Close hid stopped cause", err)
	}
	assertSenderCloseReopen(t, f)
}
func TestSnapshotSendBusyDriverCloseCollectsConcurrentCleanupFailure(t *testing.T) {
	f := senderCloseFixture(t, true)
	f.block.Store(true)
	f.armed.Store(true)
	page := make(chan error, 1)
	go func() { _, err := f.old.Next(t.Context(), raftlog.ReadBudget{Rows: 1, Bytes: 4096}); page <- err }()
	select {
	case <-f.started:
	case <-time.After(5 * time.Second):
		t.Fatal("no physical old-bank page read")
	}
	closeResult := make(chan error, 1)
	go func() { closeResult <- f.d.Close() }()
	deadline := time.Now().Add(time.Second)
	stopped := false
	for time.Now().Before(deadline) {
		f.d.mu.Lock()
		stopped = f.d.stopped != nil
		f.d.mu.Unlock()
		if stopped {
			break
		}
		runtime.Gosched()
	}
	if !stopped {
		t.Fatal("Close held Driver.mu across page")
	}
	f.unblock()
	select {
	case err := <-page:
		if !errors.Is(err, f.cause) {
			t.Fatal("page cause", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("page did not finish")
	}
	select {
	case err := <-closeResult:
		if !errors.Is(err, f.cause) || !errors.Is(err, raftlog.ErrPoisoned) {
			t.Fatal("Close missed concurrent cleanup result", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not finish")
	}
	for range 2 {
		if err := f.current.Close(); !errors.Is(err, f.cause) {
			t.Fatal("current owner result", err)
		}
		if err := f.d.Close(); !errors.Is(err, f.cause) {
			t.Fatal("repeated Driver result", err)
		}
	}
	if !f.old.closed || !f.current.closed || f.d.snapshotSender.offerCount() != 0 || f.d.snapshotSender.cleanupN != 0 || f.d.snapshotSender.ownedBytes != snapshotSenderBaseBytes {
		t.Fatal("busy Close retained ownership")
	}
	if _, err := f.d.Tick(); !errors.Is(err, f.cause) || !errors.Is(err, ErrStopped) {
		t.Fatal("Close erased cause", err)
	}
	assertSenderCloseReopen(t, f)
}
