package replica

import (
	"bytes"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/cockroachdb/pebble/v2/vfs/errorfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

type replicatedApplication struct {
	applicationFixture
	semantic    raftlog.ApplicationSemanticContractID
	restored    []uint64
	staged      int
	restoreHook func(uint64, []byte) error
}

func (m *replicatedApplication) SemanticContractID() raftlog.ApplicationSemanticContractID {
	return m.semantic
}
func (m *replicatedApplication) Restore(index uint64, image []byte) error {
	if m.restoreHook != nil {
		if err := m.restoreHook(index, image); err != nil {
			return err
		}
	}
	if err := m.applicationFixture.Restore(index, image); err != nil {
		return err
	}
	m.restored = append(m.restored, index)
	return nil
}
func (m *replicatedApplication) Stage(e Entry, b raftlog.ApplicationBudget) (raftlog.ApplicationBatch, error) {
	m.staged++
	return m.applicationFixture.Stage(e, b)
}

func replicatedDriver(t *testing.T, id uint64, identity raftlog.ApplicationIdentity, fs vfs.FS) (*Driver, *replicatedApplication, raftlog.Config) {
	t.Helper()
	p := raftlog.DefaultApplicationPolicy(id)
	cfg := raftlog.Config{Dir: fmt.Sprintf("replica-%d", id), FS: fs, Create: true, Application: p, Transfer: raftlog.ApplicationTransferConfig{Identity: identity, Contract: raftlog.ApplicationContractForPolicy(p), Limits: raftlog.DefaultApplicationTransferLimits()}, Generations: raftlog.ApplicationGenerationLimits{MaxBytes: 2 << 30, MaxRecords: 2_000_000}, PublishedCuts: raftlog.ApplicationPublishedCutLimits{MaxTransferChunks: 100000}, Replication: raftlog.ApplicationReplicationConfig{Voters: [3]uint64{1, 2, 3}}, SemanticContractID: raftlog.ApplicationSemanticContractID{17}}
	s, err := raftlog.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Initialize([]uint64{1, 2, 3}, nil); err != nil {
		t.Fatal(err)
	}
	m := &replicatedApplication{semantic: cfg.SemanticContractID}
	d, err := Open(Config{ID: id, Store: s, ApplicationMachine: m, ApplicationSnapshotSends: ApplicationSnapshotSendLimits{MaxOffers: 2, MaxOwnedBytes: 4 << 20}})
	if err != nil {
		_ = s.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := d.Close(); err != nil {
			t.Error(err)
		}
	})
	return d, m, cfg
}
func applicationGroup(t *testing.T) (map[uint64]*Driver, map[uint64]*replicatedApplication) {
	t.Helper()
	nodes := map[uint64]*Driver{}
	machines := map[uint64]*replicatedApplication{}
	for id := uint64(1); id <= 3; id++ {
		nodes[id], machines[id], _ = replicatedDriver(t, id, raftlog.ApplicationIdentity{Graph: [16]byte{1}, Partition: 1, Group: [16]byte{2}}, vfs.NewMem())
	}
	return nodes, machines
}
func deliverApplication(t *testing.T, nodes map[uint64]*Driver, queue []Packet) {
	t.Helper()
	for n := 0; len(queue) > 0; n++ {
		if n > 10000 {
			t.Fatal("unbounded application transport")
		}
		p := queue[0]
		queue = queue[1:]
		if p.Snapshot {
			t.Fatal("snapshot requires verified transfer fixture")
		}
		if d := nodes[p.To]; d != nil {
			out, err := d.Step(p)
			if err != nil {
				t.Fatalf("bound step %d->%d: %v", p.From, p.To, err)
			}
			if len(out.SnapshotSends) > 0 {
				deliverPreparedApplication(t, nodes, d, out)
			} else {
				queue = append(queue, out.Packets...)
			}
		}
	}
}
func TestApplicationReplicationPublicThreeVotersAndHistoricalRoots(t *testing.T) {
	nodes, machines := applicationGroup(t)
	deliverApplication(t, nodes, packets(t, nodes[1].Campaign))
	deliverApplication(t, nodes, packets(t, func() (Output, error) { return nodes[3].Propose(command(7)) }))
	old := nodes[1].Applied()
	deliverApplication(t, nodes, packets(t, func() (Output, error) { return nodes[1].Propose(command(11)) }))
	for _, d := range nodes {
		deliverApplication(t, nodes, packets(t, d.Tick))
	}
	for id, d := range nodes {
		if machines[id].value != 18 || d.Applied() <= old {
			t.Fatal(id, machines[id].value, d.Applied(), old)
		}
		fixtureRead(t, d.store, old, 7, false)
		fixtureRead(t, d.store, d.Applied(), 18, false)
		if err := d.store.Scrub(); err != nil {
			t.Fatal(err)
		}
	}
	for _, size := range []int{1000, 1024} {
		user := bytes.Repeat([]byte{'r'}, size)
		out, err := nodes[1].ReadIndex(user)
		if err != nil {
			t.Fatal(err)
		}
		if len(out.Reads) != 0 {
			t.Fatal("barrier before quorum", out)
		}
		var responses []Packet
		for _, p := range out.Packets {
			reply, err := nodes[p.To].Step(p)
			if err != nil {
				t.Fatal(err)
			}
			responses = append(responses, reply.Packets...)
		}
		found := false
		for _, p := range responses {
			reply, err := nodes[1].Step(p)
			if err != nil {
				t.Fatal(err)
			}
			for _, r := range reply.Reads {
				if !bytes.Equal(r.Context, user) || r.Index > nodes[1].Applied() {
					t.Fatal(r)
				}
				found = true
			}
		}
		if !found {
			t.Fatal("bound read barrier missing", size)
		}
	}
}
func TestApplicationReplicationOwnerRepresentationIsFinite(t *testing.T) {
	fixed := unsafe.Sizeof(applicationSnapshotSender{}) + unsafe.Sizeof(applicationEvent{}) + unsafe.Sizeof(applicationClose{}) + unsafe.Sizeof(raftlog.ApplicationBinding{}) + 2*128 + unsafe.Sizeof([16]*SnapshotSend{}) + 2*unsafe.Sizeof((*applicationEvent)(nil)) + unsafe.Sizeof(error(nil))
	if fixed > uintptr(snapshotSenderBaseBytes) {
		t.Fatal("owner reservation too small", fixed, snapshotSenderBaseBytes)
	}
}
func TestStepApplicationSnapshotNilDriverRefuses(t *testing.T) {
	var d *Driver
	if out, err := d.StepApplicationSnapshot(Packet{}, nil); !errors.Is(err, ErrInvalid) || len(out.Packets) != 0 {
		t.Fatal(out, err)
	}
}

// snapshot delivery is withheld until the independently streamed import is
// verified. This routes public Driver APIs only; no private RawNode injection.
func deliverPreparedApplication(t *testing.T, nodes map[uint64]*Driver, source *Driver, out Output) {
	t.Helper()
	snapAt := 0
	var queue []Packet
	for _, p := range out.Packets {
		if !p.Snapshot {
			queue = append(queue, p)
			continue
		}
		if snapAt >= len(out.SnapshotSends) {
			t.Fatal("snapshot output lost owner")
		}
		send := out.SnapshotSends[snapAt]
		snapAt++
		target := nodes[p.To]
		if target == nil {
			t.Fatal("snapshot target missing")
		}
		manifest, err := send.Manifest()
		if err != nil {
			t.Fatal(err)
		}
		imported, err := target.store.BeginApplicationImport(t.Context(), manifest)
		if err != nil {
			t.Fatal(err)
		}
		final := false
		for range 100 {
			chunk, err := send.Next(t.Context(), raftlog.ReadBudget{Rows: 1, Bytes: 4096})
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
			t.Fatal("unbounded stream")
		}
		if err = imported.Verify(t.Context()); err != nil {
			t.Fatal(err)
		}
		prepared, err := imported.Prepare()
		if err != nil {
			t.Fatal(err)
		}
		reply, err := target.StepApplicationSnapshot(p, prepared)
		if err != nil {
			t.Fatal(err)
		}
		if err = prepared.Close(); err != nil {
			t.Fatal(err)
		}
		feedback, err := source.ReportSnapshotSend(send, true)
		if err != nil {
			t.Fatal(err)
		}
		queue = append(queue, feedback.Packets...)
		queue = append(queue, reply.Packets...)
	}
	if snapAt != len(out.SnapshotSends) {
		t.Fatal("extra send owner")
	}
	deliverApplication(t, nodes, queue)
}
func TestApplicationReplicationPublicPreparedSnapshotCatchupAndTail(t *testing.T) {
	nodes, machines := applicationGroup(t)
	deliverApplication(t, nodes, packets(t, nodes[1].Campaign))
	isolated := nodes[3]
	delete(nodes, 3)
	deliverApplication(t, nodes, packets(t, func() (Output, error) { return nodes[1].Propose(command(7)) }))
	old := nodes[1].Applied()
	deliverApplication(t, nodes, packets(t, func() (Output, error) { return nodes[1].Propose(command(11)) }))
	if machines[3].value != 0 {
		t.Fatal("minority invented effect")
	}
	if err := nodes[1].store.PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	nodes[3] = isolated
	transferred := false
	for range 20 {
		out, err := nodes[1].Tick()
		if err != nil {
			t.Fatal(err)
		}
		if len(out.SnapshotSends) > 0 {
			deliverPreparedApplication(t, nodes, nodes[1], out)
			transferred = true
			break
		}
		deliverApplication(t, nodes, out.Packets)
		if isolated.store.ApplicationGeneration() == 2 {
			transferred = true
			break
		}
		offers, err := nodes[1].SnapshotSends()
		if err != nil {
			t.Fatal(err)
		}
		for _, send := range offers {
			buildSender(t, send)
			if err := nodes[1].SealSnapshotSend(send); err != nil {
				t.Fatal(err)
			}
		}
	}
	if !transferred || machines[3].value != 18 || isolated.store.ApplicationGeneration() != 2 {
		t.Fatal("snapshot did not activate", transferred, machines[3].value, isolated.store.ApplicationGeneration())
	}
	fixtureRead(t, isolated.store, old, 7, false)
	fixtureRead(t, isolated.store, isolated.Applied(), 18, false)
	deliverApplication(t, nodes, packets(t, func() (Output, error) { return nodes[1].Propose(command(5)) }))
	for _, d := range nodes {
		deliverApplication(t, nodes, packets(t, d.Tick))
	}
	for id, d := range nodes {
		if machines[id].value != 23 {
			t.Fatal("tail divergence", id, machines[id].value)
		}
		fixtureRead(t, d.store, old, 7, false)
		if err := d.store.Scrub(); err != nil {
			t.Fatal(err)
		}
	}
}

func packetForApplication(t *testing.T, d *Driver, m *pb.Message) Packet {
	t.Helper()
	wire, err := proto.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	p, err := EncodeApplicationPacket(d.store.ApplicationBinding(), Packet{From: m.GetFrom(), To: m.GetTo(), Payload: wire, Snapshot: m.GetType() == pb.MsgSnap}, d.config.MaxPacketBytes)
	if err != nil {
		t.Fatal(err)
	}
	return p
}
func prepareApplicationPacket(t *testing.T, target, source *Driver) (*raftlog.ApplicationImport, *raftlog.PreparedApplicationSnapshot, Packet) {
	t.Helper()
	cut, err := source.store.PublishedApplicationCut()
	if err != nil {
		t.Fatal(err)
	}
	export, err := source.store.BeginPublishedApplicationExport(t.Context(), cut.ID)
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
		t.Fatal("unbounded build")
	}
	manifest, err := export.Manifest()
	if err != nil {
		t.Fatal(err)
	}
	imported, err := target.store.BeginApplicationImport(t.Context(), manifest)
	if err != nil {
		t.Fatal(err)
	}
	final := false
	for range 100 {
		c, err := export.Next(t.Context(), raftlog.ReadBudget{Rows: 1, Bytes: 4096})
		if err != nil {
			t.Fatal(err)
		}
		if err = imported.Append(t.Context(), c); err != nil {
			t.Fatal(err)
		}
		if c.Final {
			final = true
			break
		}
	}
	if !final {
		t.Fatal("unbounded import")
	}
	if err = imported.Verify(t.Context()); err != nil {
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
	hard, _, err := source.store.InitialState()
	if err != nil {
		t.Fatal(err)
	}
	snap := &pb.Snapshot{Data: descriptor, Metadata: &pb.SnapshotMetadata{Index: new(manifest.Index), Term: new(manifest.Term), ConfState: manifest.ConfState}}
	packet := packetForApplication(t, source, &pb.Message{Type: pb.MsgSnap.Enum(), From: new(source.config.ID), To: new(target.config.ID), Term: new(hard.GetTerm()), Snapshot: snap})
	return imported, prepared, packet
}
func TestApplicationReplicationRealFastForwardAndStaleNeverActivate(t *testing.T) {
	nodes, machines := applicationGroup(t)
	deliverApplication(t, nodes, packets(t, nodes[1].Campaign))
	target := nodes[3]
	delete(nodes, 3)
	deliverApplication(t, nodes, packets(t, func() (Output, error) { return nodes[1].Propose(command(7)) }))
	if err := nodes[1].store.PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	// Persist the same index/term locally without its commit. The pinned Raft's
	// restore(matchTerm) must fast-forward this entry, yielding no Ready.Snapshot.
	appendPacket := packetForApplication(t, nodes[1], &pb.Message{Type: pb.MsgApp.Enum(), From: new(uint64(1)), To: new(uint64(3)), Term: new(uint64(2)), Index: new(uint64(2)), LogTerm: new(uint64(2)), Commit: new(uint64(2)), Entries: []*pb.Entry{{Type: pb.EntryNormal.Enum(), Index: new(uint64(3)), Term: new(uint64(2)), Data: command(7)}}})
	if _, err := target.Step(appendPacket); err != nil {
		t.Fatal(err)
	}
	if machines[3].value != 0 || target.Applied() != 2 {
		t.Fatal("uncommitted append visible")
	}
	imported, prepared, p := prepareApplicationPacket(t, target, nodes[1])
	defer prepared.Close()
	generation := target.store.ApplicationGeneration()
	restoreCount := len(machines[3].restored)
	out, err := target.StepApplicationSnapshot(p, prepared)
	if err != nil || target.Applied() != 3 || machines[3].value != 7 || target.store.ApplicationGeneration() != generation || len(machines[3].restored) != restoreCount+1 {
		t.Fatal("fast-forward activated or failed apply", out, err, target.Applied(), target.store.ApplicationGeneration())
	}
	audit, err := target.store.ApplicationActivationAudit()
	if err != nil || audit.ManifestID != ([32]byte{}) {
		t.Fatal("fast-forward activation audit", audit, err)
	}
	status, err := imported.Status()
	if err != nil || !status.Verified {
		t.Fatal("unused claim destroyed Prepared", status, err)
	}
	usage, err := target.store.ApplicationTransferUsage()
	if err != nil || usage.Claims != 0 || usage.Prepared != 1 {
		t.Fatal("fast-forward leaked claim", usage, err)
	}
	restoreCount = len(machines[3].restored)
	out, err = target.StepApplicationSnapshot(p, prepared)
	if err != nil || len(machines[3].restored) != restoreCount || target.store.ApplicationGeneration() != generation {
		t.Fatal("stale snapshot activated", out, err)
	}
	fixtureRead(t, target.store, 3, 7, false)
	if err := prepared.Close(); err != nil {
		t.Fatal(err)
	}
	usage, err = target.store.ApplicationTransferUsage()
	if err != nil || usage.Claims != 0 || usage.Prepared != 0 || usage.Import {
		t.Fatal("unused token ownership", usage, err)
	}
}
func TestApplicationReplicationWrongStoreClaimRefusesBeforeRawNode(t *testing.T) {
	nodes, _ := applicationGroup(t)
	deliverApplication(t, nodes, packets(t, nodes[1].Campaign))
	target := nodes[3]
	delete(nodes, 3)
	deliverApplication(t, nodes, packets(t, func() (Output, error) { return nodes[1].Propose(command(7)) }))
	if err := nodes[1].store.PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	// The foreign Store has the same group/semantic/membership/index bytes. Only
	// physical capability ownership distinguishes this token, BEFORE RawNode.
	foreign, _, _ := replicatedDriver(t, 3, target.binding.Identity, vfs.NewMem())
	_, prepared, p := prepareApplicationPacket(t, foreign, nodes[1])
	defer prepared.Close()
	before := target.raw.BasicStatus()
	idx, image, err := target.store.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	usage, err := target.store.ApplicationTransferUsage()
	if err != nil {
		t.Fatal(err)
	}
	for _, token := range []*raftlog.PreparedApplicationSnapshot{nil, new(raftlog.PreparedApplicationSnapshot), prepared} {
		out, err := target.StepApplicationSnapshot(p, token)
		if !errors.Is(err, raftlog.ErrInvalid) || len(out.Packets) != 0 || len(out.Reads) != 0 || out.Applied != 0 {
			t.Fatal(out, err)
		}
	}
	after := target.raw.BasicStatus()
	now, bytesAfter, err := target.store.Checkpoint()
	if err != nil || now != idx || !bytes.Equal(image, bytesAfter) || before.GetTerm() != after.GetTerm() || before.GetCommit() != after.GetCommit() || before.Lead != after.Lead {
		t.Fatal("wrong token reached RawNode/store", before, after, err)
	}
	afterUsage, err := target.store.ApplicationTransferUsage()
	if err != nil || usage != afterUsage {
		t.Fatal("wrong token leaked ownership", usage, afterUsage, err)
	}
}

// The finalization seam is private only to force a cleanup failure after an
// already-computed Output. The blocked read and release use real Store owners;
// this test neither steps a receiver nor changes its consensus acceptance.
func TestApplicationReplicationFinalizationFencesCleanupAndCachesClose(t *testing.T) {
	f := senderCloseFixture(t, false)
	f.block.Store(true)
	f.armed.Store(true)
	read := make(chan error, 1)
	go func() { _, _, err := f.view.Get(t.Context(), []byte("a"), 4096); read <- err }()
	select {
	case <-f.started:
	case <-time.After(5 * time.Second):
		t.Fatal("no actual blocked SST read")
	}
	d := f.d
	d.mu.Lock()
	owner := d.beginCleanup()
	d.detachSnapshotSend(f.current)
	type result struct {
		out Output
		err error
	}
	finalized := make(chan result, 1)
	go func() {
		out, err := d.finishEvent(owner, Output{Packets: []Packet{{Payload: []byte("private")}}, Reads: []ReadResult{{Context: []byte("private")}}, Applied: 99}, nil)
		finalized <- result{out, err}
	}()
	// finishEvent must release mu before waiting for the Store's blocked read.
	refused := make(chan result, 1)
	go func() {
		out, err := d.Step(Packet{})
		refused <- result{out, err}
	}()
	select {
	case r := <-refused:
		if !errors.Is(r.err, ErrUnavailable) || !emptyApplicationOutput(r.out) {
			t.Fatal(r.out, r.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("event held mu during cleanup")
	}
	if out, err := d.StepApplicationSnapshot(Packet{}, nil); !errors.Is(err, ErrUnavailable) || out.Applied != 0 {
		t.Fatal("snapshot bypassed cleanup fence", out, err)
	}
	_ = d.Applied() // Getter/unlock cannot clear the event fence.
	d.mu.Lock()
	if d.event != owner || owner.rawPhase {
		d.mu.Unlock()
		t.Fatal("getter stole cleanup owner")
	}
	d.mu.Unlock()
	oldClose := make(chan error, 1)
	go func() { oldClose <- f.old.Close() }()
	closeResult := make(chan error, 1)
	go func() { closeResult <- d.Close() }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		d.mu.Lock()
		closing := d.closeOwner != nil
		stillOwned := d.event == owner
		d.mu.Unlock()
		if !stillOwned {
			t.Fatal("Close/owner Close stole event")
		}
		if closing {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Close did not install owner")
		}
		runtime.Gosched()
	}
	f.unblock()
	select {
	case err := <-read:
		if !errors.Is(err, f.cause) || errors.Is(err, raftlog.ErrCorrupt) {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("read join")
	}
	select {
	case err := <-oldClose:
		if err != nil {
			t.Fatal("non-final reference release", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("old owner join")
	}
	select {
	case r := <-finalized:
		if !errors.Is(r.err, f.cause) || !errors.Is(r.err, raftlog.ErrPoisoned) || !emptyApplicationOutput(r.out) {
			t.Fatal("finalized output/error", r.out, r.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("finalization join")
	}
	for _, ch := range []chan error{closeResult} {
		select {
		case err := <-ch:
			if !errors.Is(err, f.cause) || !errors.Is(err, raftlog.ErrPoisoned) {
				t.Fatal("cleanup cause lost", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("cleanup join")
		}
	}
	for range 2 {
		if err := d.Close(); !errors.Is(err, f.cause) {
			t.Fatal("cached Close", err)
		}
	}
	if d.event != nil || d.snapshotSender.offerCount() != 0 || d.snapshotSender.cleanupN != 0 || d.snapshotSender.ownedBytes != snapshotSenderBaseBytes {
		t.Fatal("ownership leaked")
	}
	assertSenderCloseReopen(t, f)
}

func TestApplicationReplicationOriginalClaimCleanupResultRetained(t *testing.T) {
	// Accepted Store activation constructs a consumed claim while an immutable
	// old-bank view still owns real SST data. Driver activation is tested above.
	f := &senderCloseScene{cause: errors.New("claim cleanup SST poison"), started: make(chan struct{}), release: make(chan struct{})}
	fs := errorfs.Wrap(vfs.NewMem(), errorfs.InjectorFunc(func(op errorfs.Op) error {
		if f.armed.Load() && op.Kind == errorfs.OpFileReadAt && strings.HasSuffix(op.Path, ".sst") {
			f.reached.Add(1)
			return f.cause
		}
		return nil
	}))
	d, _ := senderFixtureStorage(t, fs, ApplicationSnapshotSendLimits{MaxOffers: 2, MaxOwnedBytes: 1 << 20}, true)
	senderLeader(t, d)
	view, err := d.store.ApplicationView(2)
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	donor, _ := senderFixture(t, vfs.NewMem(), ApplicationSnapshotSendLimits{MaxOffers: 1, MaxOwnedBytes: 1 << 20})
	senderLeader(t, donor)
	senderEvent(t, donor, func() error { return donor.raw.Propose(command(11)) })
	senderEvent(t, donor, func() error {
		return donor.raw.Step(&pb.Message{Type: pb.MsgAppResp.Enum(), From: new(uint64(2)), To: new(uint64(1)), Term: new(uint64(2)), Index: new(uint64(4))})
	})
	if err := donor.store.PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	_, prepared, packet := prepareApplicationPacket(t, d, donor)
	decoded, err := DecodeApplicationPacket(d.store.ApplicationBinding(), packet, d.config.MaxPacketBytes)
	if err != nil {
		t.Fatal(err)
	}
	message := new(pb.Message)
	if err := proto.Unmarshal(decoded.Payload, message); err != nil {
		t.Fatal(err)
	}
	claim, err := d.store.ClaimPreparedApplicationSnapshot(prepared, message.GetSnapshot())
	if err != nil {
		t.Fatal(err)
	}
	defer claim.Close()
	hard, _, err := d.store.InitialState()
	if err != nil {
		t.Fatal(err)
	}
	hard.Commit = new(message.GetSnapshot().GetMetadata().GetIndex())
	if _, err := d.store.PersistApplicationReady(raft.Ready{Snapshot: message.GetSnapshot(), HardState: hard}, claim); err != nil {
		t.Fatal(err)
	}
	f.armed.Store(true)
	if _, _, err := view.Get(t.Context(), []byte("a"), 4096); !errors.Is(err, f.cause) || errors.Is(err, raftlog.ErrCorrupt) || f.reached.Load() == 0 {
		t.Fatal("not actual SST poison", err, f.reached.Load())
	}
	d.mu.Lock()
	owner := d.beginCleanup()
	owner.claim = claim
	out, err := d.finishEvent(owner, Output{Applied: 99, Packets: []Packet{{Payload: []byte("private")}}}, nil)
	if !errors.Is(err, f.cause) || !errors.Is(err, raftlog.ErrPoisoned) || out.Applied != 0 || len(out.Packets) != 0 {
		t.Fatal("claim finalization", out, err)
	}
	if err := claim.Close(); err != nil {
		t.Fatal("claim should lose its first result on repeat", err)
	}
	for range 2 {
		if err := d.Close(); !errors.Is(err, f.cause) || !errors.Is(err, raftlog.ErrPoisoned) {
			t.Fatal("Driver must retain original result", err)
		}
	}
}

func TestApplicationReplicationBindingAndMembersRefuseBeforeMutation(t *testing.T) {
	nodes, _ := applicationGroup(t)
	d := nodes[1]
	good := packetForApplication(t, d, &pb.Message{Type: pb.MsgHeartbeat.Enum(), From: new(uint64(2)), To: new(uint64(1)), Term: new(uint64(1)), Commit: new(uint64(1))})
	wrongGroup := d.binding
	wrongGroup.Identity.Group[0]++
	wrongSemantic := d.binding
	wrongSemantic.SemanticContractID[0]++
	var cases []Packet
	for _, b := range []raftlog.ApplicationBinding{wrongGroup, wrongSemantic} {
		p, err := EncodeApplicationPacket(b, Packet{From: 2, To: 1, Payload: []byte{0xff}}, d.config.MaxPacketBytes)
		if err != nil {
			t.Fatal(err)
		}
		cases = append(cases, p)
	}
	missing := good
	missing.Payload = []byte{0xff}
	cases = append(cases, missing)
	outsider := good
	outsider.From = 4
	cases = append(cases, outsider)
	wrongTo := good
	wrongTo.To = 2
	cases = append(cases, wrongTo)
	self := good
	self.From = 1
	cases = append(cases, self)
	mismatch := packetForApplication(t, d, &pb.Message{Type: pb.MsgHeartbeat.Enum(), From: new(uint64(3)), To: new(uint64(1)), Term: new(uint64(1)), Commit: new(uint64(1))})
	mismatch.From = 2
	cases = append(cases, mismatch)
	for _, kind := range []pb.EntryType{pb.EntryConfChange, pb.EntryConfChangeV2} {
		cases = append(cases, packetForApplication(t, d, &pb.Message{Type: pb.MsgApp.Enum(), From: new(uint64(2)), To: new(uint64(1)), Term: new(uint64(1)), Index: new(uint64(1)), LogTerm: new(uint64(1)), Entries: []*pb.Entry{{Type: kind.Enum(), Index: new(uint64(2)), Term: new(uint64(1)), Data: []byte{}}}}))
	}
	for _, kind := range []pb.MessageType{pb.MsgHup, pb.MsgReadIndex, pb.MsgReadIndexResp} {
		cases = append(cases, packetForApplication(t, d, &pb.Message{Type: kind.Enum(), From: new(uint64(2)), To: new(uint64(1)), Term: new(uint64(1))}))
	}
	before := d.raw.BasicStatus()
	idx, image, err := d.store.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	for n, p := range cases {
		out, err := d.Step(p)
		if !errors.Is(err, ErrInvalid) || len(out.Packets) != 0 || len(out.Reads) != 0 || out.Applied != 0 {
			t.Fatal(n, out, err)
		}
		after := d.raw.BasicStatus()
		now, img, err := d.store.Checkpoint()
		if err != nil || now != idx || !bytes.Equal(image, img) || before.GetTerm() != after.GetTerm() || before.GetCommit() != after.GetCommit() || before.Lead != after.Lead {
			t.Fatal("bad scope/entry changed state", n, err)
		}
	}
	// The exact RP1 envelope is charged before protobuf and RawNode.
	d.config.MaxPacketBytes = len(good.Payload) - 1
	if out, err := d.Step(good); !errors.Is(err, ErrLimit) || out.Applied != 0 {
		t.Fatal(out, err)
	}
	d.config.MaxPacketBytes++
	if _, err := d.Step(good); err != nil {
		t.Fatal("exact envelope boundary", err)
	}
}

func TestApplicationReplicationOutgoingEnvelopeAndPartitionBoundaries(t *testing.T) {
	// Private limit adjustment isolates the exact byte checks; public Config's
	// larger minimum is separately preserved. All messages come from real Campaign.
	nodes, _ := applicationGroup(t)
	sample, err := nodes[1].Campaign()
	if err != nil {
		t.Fatal(err)
	}
	largest, total := 0, 0
	for _, p := range sample.Packets {
		largest = max(largest, len(p.Payload))
		total += len(p.Payload)
		if _, err := DecodeApplicationPacket(nodes[1].binding, p, len(p.Payload)); err != nil {
			t.Fatal(err)
		}
	}
	if largest <= applicationPacketHeaderBytes || len(sample.Packets) != 2 {
		t.Fatal(sample)
	}
	for _, tc := range []struct {
		name           string
		packet, output int
		want           bool
	}{{"packet-minus", largest - 1, total, false}, {"packet-exact", largest, total, true}, {"packet-plus", largest + 1, total, true}, {"output-minus", largest, total - 1, false}, {"output-exact", largest, total, true}, {"output-plus", largest, total + 1, true}} {
		t.Run(tc.name, func(t *testing.T) {
			nodes, _ := applicationGroup(t)
			d := nodes[1]
			d.config.MaxPacketBytes = tc.packet
			d.config.MaxOutputBytes = d.snapshotSender.policy.MaxOffers*snapshotSendOutputBytes + tc.output
			out, err := d.Campaign()
			if tc.want {
				if err != nil || len(out.Packets) != 2 {
					t.Fatal(out, err)
				}
			} else {
				if !errors.Is(err, ErrLimit) || len(out.Packets) != 0 || len(out.SnapshotSends) != 0 || out.Applied != 0 {
					t.Fatal(out, err)
				}
				if _, err := d.Tick(); !errors.Is(err, ErrStopped) || !errors.Is(err, ErrLimit) {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestApplicationReplicationOpenRequiresProviderBeforeRestore(t *testing.T) {
	_, _, cfg := replicatedDriver(t, 1, raftlog.ApplicationIdentity{Graph: [16]byte{1}, Partition: 1, Group: [16]byte{2}}, vfs.NewMem())
	for _, tc := range []struct {
		name     string
		semantic raftlog.ApplicationSemanticContractID
		machine  ApplicationMachine
		policy   bool
	}{{"missing-provider", cfg.SemanticContractID, &applicationFixture{}, true}, {"wrong-provider", cfg.SemanticContractID, &replicatedApplication{semantic: raftlog.ApplicationSemanticContractID{18}}, true}, {"typed-nil", cfg.SemanticContractID, (*replicatedApplication)(nil), true}, {"missing-sender", cfg.SemanticContractID, &replicatedApplication{semantic: cfg.SemanticContractID}, false}, {"unbound-three", raftlog.ApplicationSemanticContractID{}, &applicationFixture{}, false}} {
		t.Run(tc.name, func(t *testing.T) {
			local := cfg
			local.FS = vfs.NewMem()
			local.SemanticContractID = tc.semantic
			s, err := raftlog.Open(local)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			if err := s.Initialize([]uint64{1, 2, 3}, nil); err != nil {
				t.Fatal(err)
			}
			c := Config{ID: 1, Store: s, ApplicationMachine: tc.machine}
			if tc.policy {
				c.ApplicationSnapshotSends = ApplicationSnapshotSendLimits{MaxOffers: 2, MaxOwnedBytes: 4 << 20}
			}
			if d, err := Open(c); d != nil || !errors.Is(err, ErrInvalid) {
				t.Fatal(d, err)
			}
			if m, ok := tc.machine.(*replicatedApplication); ok && m != nil && len(m.restored) != 0 {
				t.Fatal("Restore before binding validation")
			}
		})
	}
}

func TestApplicationReplicationActivationSyncPrecedesRestoreAndFailedOutput(t *testing.T) {
	nodes, machines := applicationGroup(t)
	fs := vfs.NewMem()
	target, machine, cfg := replicatedDriver(t, 3, nodes[1].binding.Identity, fs)
	nodes[3] = target
	machines[3] = machine
	deliverApplication(t, nodes, packets(t, nodes[1].Campaign))
	delete(nodes, 3)
	deliverApplication(t, nodes, packets(t, func() (Output, error) { return nodes[1].Propose(command(7)) }))
	if err := nodes[1].store.PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	_, prepared, p := prepareApplicationPacket(t, target, nodes[1])
	defer prepared.Close()
	before := target.raw.BasicStatus()
	if out, err := target.Step(p); !errors.Is(err, ErrInvalid) || out.Applied != 0 {
		t.Fatal("ordinary Step bypassed prepared claim", out, err)
	}
	if after := target.raw.BasicStatus(); after.GetCommit() != before.GetCommit() || after.GetTerm() != before.GetTerm() {
		t.Fatal("ordinary snapshot mutated Raft")
	}
	cause := errors.New("volatile restore refused after activation sync")
	called := false
	machines[3].restoreHook = func(index uint64, image []byte) error {
		called = true
		idx, stored, err := target.store.Checkpoint()
		if err != nil || idx != index || !bytes.Equal(image, stored) || target.store.ApplicationGeneration() != 2 {
			t.Fatal("Restore before durable activation", idx, index, err)
		}
		return cause
	}
	out, err := target.StepApplicationSnapshot(p, prepared)
	if !called || !errors.Is(err, cause) || !errors.Is(err, ErrStopped) || len(out.Packets) != 0 || len(out.Reads) != 0 || out.Applied != 0 || machines[3].value != 0 {
		t.Fatal("failed Restore exposed output", out, err)
	}
	fixtureRead(t, target.store, 3, 7, false)
	usage, err := target.store.ApplicationTransferUsage()
	if err != nil || usage.Claims != 0 || usage.Prepared != 0 || usage.Import {
		t.Fatal("consumed token leaked", usage, err)
	}
	if out, err := target.StepApplicationSnapshot(p, prepared); !errors.Is(err, ErrStopped) || !errors.Is(err, cause) || out.Applied != 0 {
		t.Fatal("snapshot bypassed stop", out, err)
	}
	if _, err := target.Tick(); !errors.Is(err, cause) || !errors.Is(err, ErrStopped) {
		t.Fatal(err)
	}
	if err := target.Close(); err != nil {
		t.Fatal(err)
	}
	cfg.Create = false
	reopened, err := raftlog.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	recovered := &replicatedApplication{semantic: cfg.SemanticContractID}
	driver, err := Open(Config{ID: 3, Store: reopened, ApplicationMachine: recovered, ApplicationSnapshotSends: ApplicationSnapshotSendLimits{MaxOffers: 2, MaxOwnedBytes: 4 << 20}})
	if err != nil {
		_ = reopened.Close()
		t.Fatal(err)
	}
	defer driver.Close()
	if recovered.value != 7 || driver.Applied() != 3 {
		t.Fatal("fault-free recovery lost synced snapshot", recovered.value, driver.Applied())
	}
	fixtureRead(t, reopened, 3, 7, false)
	if err := reopened.Scrub(); err != nil {
		t.Fatal(err)
	}
}

func TestApplicationReplicationSnapshotValidationRefusalReleasesClaim(t *testing.T) {
	nodes, _ := applicationGroup(t)
	deliverApplication(t, nodes, packets(t, nodes[1].Campaign))
	target := nodes[3]
	delete(nodes, 3)
	deliverApplication(t, nodes, packets(t, func() (Output, error) { return nodes[1].Propose(command(7)) }))
	if err := nodes[1].store.PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	imported, prepared, p := prepareApplicationPacket(t, target, nodes[1])
	defer prepared.Close()
	before := target.raw.BasicStatus()
	decoded, err := DecodeApplicationPacket(target.binding, p, target.config.MaxPacketBytes)
	if err != nil {
		t.Fatal(err)
	}
	message := new(pb.Message)
	if err := proto.Unmarshal(decoded.Payload, message); err != nil {
		t.Fatal(err)
	}
	// Matching descriptor and token, impossible message/snapshot term relation.
	message.Term = new(uint64(1))
	bad := packetForApplication(t, nodes[1], message)
	if out, err := target.StepApplicationSnapshot(bad, prepared); !errors.Is(err, ErrInvalid) || out.Applied != 0 || len(out.Packets) != 0 {
		t.Fatal("invalid message with valid claim", out, err)
	}
	malformed := p
	malformed.Payload = []byte{0xff}
	if out, err := target.StepApplicationSnapshot(malformed, prepared); !errors.Is(err, ErrInvalid) || out.Applied != 0 {
		t.Fatal(out, err)
	}
	usage, err := target.store.ApplicationTransferUsage()
	if err != nil || usage.Claims != 0 || usage.Prepared != 1 {
		t.Fatal("refusal leaked claim", usage, err)
	}
	status, err := imported.Status()
	if err != nil || !status.Verified {
		t.Fatal("refusal aborted prepared", status, err)
	}
	after := target.raw.BasicStatus()
	if before.GetTerm() != after.GetTerm() || before.GetCommit() != after.GetCommit() || before.Lead != after.Lead {
		t.Fatal("invalid term reached RawNode")
	}
	if _, err := target.StepApplicationSnapshot(p, prepared); err != nil {
		t.Fatal("valid retry after refusal", err)
	}
	fixtureRead(t, target.store, 3, 7, false)
	legacy, _ := applicationDriver(t, vfs.NewMem(), raftlog.DefaultApplicationPolicy(1))
	if out, err := legacy.StepApplicationSnapshot(p, prepared); !errors.Is(err, ErrInvalid) || out.Applied != 0 {
		t.Fatal("legacy application mode relaxed", out, err)
	}
}

func emptyApplicationOutput(out Output) bool {
	return out.Applied == 0 && len(out.Packets) == 0 && len(out.Reads) == 0 && len(out.SnapshotSends) == 0
}

func TestApplicationReplicationFatalSnapshotAdmissionDrainsOwners(t *testing.T) {
	f := senderCloseFixture(t, false)
	f.armed.Store(true)
	_, _, cause := f.view.Get(t.Context(), []byte("a"), 4096)
	if !errors.Is(cause, f.cause) || errors.Is(cause, raftlog.ErrCorrupt) || f.reached.Load() == 0 {
		t.Fatal("not actual shared-store failure", cause)
	}
	f.d.mu.Lock()
	f.d.snapshotSender.fatal = cause
	f.d.mu.Unlock()
	out, err := f.d.StepApplicationSnapshot(Packet{}, nil)
	if !emptyApplicationOutput(out) || !errors.Is(err, f.cause) || !errors.Is(err, ErrStopped) {
		t.Fatal(out, err)
	}
	if f.d.event != nil || f.d.snapshotSender.offerCount() != 0 || f.d.snapshotSender.cleanupN != 0 || f.d.snapshotSender.ownedBytes != snapshotSenderBaseBytes {
		t.Fatal("admission left detached pins")
	}
	if err := f.d.Close(); !errors.Is(err, f.cause) {
		t.Fatal("cleanup cause missing", err)
	}
	assertSenderCloseReopen(t, f)
}

func TestApplicationReplicationBorrowedPreflightDefensiveFailures(t *testing.T) {
	// These helpers normally follow generic preflight. Independently hostile
	// truncation/type cases prove their bounded consumption also fails closed.
	for _, b := range [][]byte{{0xff}, {8, 0xff}, {58, 2, 0}, {13, 0, 0, 0, 0}, {8, 31}} {
		if err := preflightApplicationMessage(b, Packet{From: 2, To: 1}, false); !errors.Is(err, ErrInvalid) {
			t.Fatal(b, err)
		}
	}
	for _, b := range [][]byte{{0xff}, {8, 0xff}, {34, 2, 0}, {13, 0, 0, 0, 0}, {8, 1}} {
		if err := preflightNormalApplicationEntry(b); !errors.Is(err, ErrInvalid) {
			t.Fatal(b, err)
		}
	}
	if err := preflightNormalApplicationEntry([]byte{8, 0, 34, 1, 7}); err != nil {
		t.Fatal(err)
	}
	wire, err := proto.Marshal(&pb.Message{Type: pb.MsgProp.Enum(), From: new(uint64(2)), To: new(uint64(1)), Entries: []*pb.Entry{{Type: pb.EntryNormal.Enum(), Data: command(7)}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := preflightApplicationMessage(wire, Packet{From: 2, To: 1}, false); err != nil {
		t.Fatal(err)
	}
}

func TestApplicationReplicationFiniteTermAndHeartbeatRefusals(t *testing.T) {
	nodes, _ := applicationGroup(t)
	d := nodes[1]
	before := d.raw.BasicStatus()
	for _, tc := range []struct {
		m    *pb.Message
		want error
	}{{&pb.Message{Type: pb.MsgTimeoutNow.Enum(), From: new(uint64(2)), To: new(uint64(1)), Term: new(replicaTermCeiling)}, ErrLimit}, {&pb.Message{Type: pb.MsgHeartbeat.Enum(), From: new(uint64(2)), To: new(uint64(1)), Term: new(uint64(1)), Commit: new(uint64(2))}, ErrInvalid}, {&pb.Message{Type: pb.MsgHeartbeat.Enum(), From: new(uint64(2)), To: new(uint64(1)), Term: new(uint64(0))}, ErrInvalid}} {
		p := packetForApplication(t, d, tc.m)
		out, err := d.Step(p)
		if !errors.Is(err, tc.want) || !emptyApplicationOutput(out) {
			t.Fatal(out, err)
		}
		after := d.raw.BasicStatus()
		if after.GetTerm() != before.GetTerm() || after.GetCommit() != before.GetCommit() || after.Lead != before.Lead {
			t.Fatal("refusal changed RawNode")
		}
	}
}

func TestApplicationReplicationSnapshotBelowCommittedTermReleasesClaim(t *testing.T) {
	nodes, _ := applicationGroup(t)
	deliverApplication(t, nodes, packets(t, nodes[1].Campaign))
	target := nodes[3]
	donor, machine, _ := replicatedDriver(t, 1, target.binding.Identity, vfs.NewMem())
	// Construct a valid agreed fixed-membership source whose higher application
	// index belongs to term1. Target has already committed term2 at index2.
	if err := donor.store.Persist(raft.Ready{Entries: []*pb.Entry{{Type: pb.EntryNormal.Enum(), Index: new(uint64(2)), Term: new(uint64(1))}, {Type: pb.EntryNormal.Enum(), Index: new(uint64(3)), Term: new(uint64(1)), Data: command(7)}}, HardState: &pb.HardState{Term: new(uint64(1)), Commit: new(uint64(3))}}); err != nil {
		t.Fatal(err)
	}
	for _, e := range []Entry{{Generation: 1, Index: 2, Term: 1}, {Generation: 1, Index: 3, Term: 1, Data: command(7)}} {
		batch, err := machine.Stage(e, donor.store.ApplicationBudget())
		if err != nil {
			t.Fatal(err)
		}
		if err := donor.store.InstallApplication(e.Index, batch); err != nil {
			t.Fatal(err)
		}
		if err := machine.Restore(e.Index, batch.Image); err != nil {
			t.Fatal(err)
		}
	}
	if err := donor.store.PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	_, prepared, p := prepareApplicationPacket(t, target, donor)
	defer prepared.Close()
	// Message term is acceptable; the snapshot's exact term remains1.
	decoded, err := DecodeApplicationPacket(target.binding, p, target.config.MaxPacketBytes)
	if err != nil {
		t.Fatal(err)
	}
	m := new(pb.Message)
	if err := proto.Unmarshal(decoded.Payload, m); err != nil {
		t.Fatal(err)
	}
	m.Term = new(uint64(2))
	p = packetForApplication(t, donor, m)
	before := target.raw.BasicStatus()
	if out, err := target.StepApplicationSnapshot(p, prepared); !errors.Is(err, ErrInvalid) || !emptyApplicationOutput(out) {
		t.Fatal("older snapshot term admitted", out, err)
	}
	after := target.raw.BasicStatus()
	if after.GetCommit() != before.GetCommit() || after.GetTerm() != before.GetTerm() || target.store.ApplicationGeneration() != 1 {
		t.Fatal("term refusal mutated Raft/generation")
	}
	usage, err := target.store.ApplicationTransferUsage()
	if err != nil || usage.Claims != 0 || usage.Prepared != 1 {
		t.Fatal("term refusal leaked/aborted capability", usage, err)
	}
}
