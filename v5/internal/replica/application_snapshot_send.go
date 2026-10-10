package replica

import (
	"bytes"
	"context"
	"errors"
	"math"
	"slices"

	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/tracker"
	"google.golang.org/protobuf/proto"
)

// ApplicationSnapshotSendLimits reserves finite sender-owned capacity. Zero
// disables the sender foundation. This does not enable application traffic.
// MaxOffers includes detached owners until cleanup finishes. MaxOwnedBytes
// charges retained manifest image capacity and fixed objects/protobuf/descriptor
// allowance plus a fixed sender registry/control reservation, separately from the Store's export pin ledger and returned copies.
type ApplicationSnapshotSendLimits struct {
	MaxOffers     int
	MaxOwnedBytes uint64
}

const snapshotSenderBaseBytes uint64 = 1024
const snapshotSendFixedBytes uint64 = 4096
const snapshotSendOutputBytes = 2048

func (l ApplicationSnapshotSendLimits) validate(c Config) error {
	if l == (ApplicationSnapshotSendLimits{}) {
		return nil
	}
	if l.MaxOffers < 1 || l.MaxOffers > 16 || l.MaxOwnedBytes < snapshotSenderBaseBytes+snapshotSendFixedBytes || l.MaxOwnedBytes > 256<<20 || c.Store.ApplicationBinding().Validate() != nil || isNilMachine(c.ApplicationMachine) || c.MaxPacketBytes < snapshotSendOutputBytes || c.MaxOutputBytes-l.MaxOffers*snapshotSendOutputBytes < c.MaxPacketBytes {
		return ErrInvalid
	}
	return nil
}

type applicationSnapshotSender struct {
	policy             ApplicationSnapshotSendLimits
	incarnation        [16]byte
	nextID, ownedBytes uint64
	offers             [16]*SnapshotSend
	unbound            *SnapshotSend
	busy               bool
	fatal              error
	cleanupErr         error
	cleanup            [16]*SnapshotSend
	cleanupN           int
}

// SnapshotSend owns an exact immutable export. Before a successful Raft
// callback it has no destination and is discoverable through SnapshotSends.
// Build verifies one bounded page, Seal enables a later callback, and the actual
// outgoing MsgSnap binds exactly one peer/term. Next replays that same source.
// A handle never proves receiver preparation, acceptance or durable delivery.
// Physical bank/generation stays local; neither is a replicated effect identity.
type SnapshotSend struct {
	self                                                 *SnapshotSend
	cleaning                                             bool
	closeErr                                             error
	slot                                                 int
	originTerm                                           uint64
	d                                                    *Driver
	export                                               *raftlog.ApplicationExport
	incarnation                                          [16]byte
	id, ownedBytes                                       uint64
	cutID                                                [32]byte
	manifest                                             raftlog.ApplicationSnapshotManifest
	snapshot                                             *pb.Snapshot
	descriptor                                           []byte
	to, term                                             uint64
	ready, sealed, queued, bound, final, closed, working bool
	done                                                 chan struct{}
}

type applicationSnapshotStorage struct {
	*raftlog.Store
	d *Driver
}

var _ raft.Storage = (*applicationSnapshotStorage)(nil)

func newApplicationSnapshotSender(d *Driver) *applicationSnapshotSender {
	return &applicationSnapshotSender{policy: d.config.ApplicationSnapshotSends, incarnation: d.readNonce, ownedBytes: snapshotSenderBaseBytes}
}

// Snapshot is called only under Driver.mu. Every refusal returns the EXACT
// raft sentinel (the pinned library compares equality and panics otherwise).
// Fatal source failures are latched for drain before any further RawNode access.
func (w *applicationSnapshotStorage) Snapshot() (*pb.Snapshot, error) {
	d := w.d
	s := d.snapshotSender
	if s == nil || d.stopped != nil || s.busy || s.fatal != nil {
		return nil, raft.ErrSnapshotTemporarilyUnavailable
	}
	status := d.raw.BasicStatus()
	if status.RaftState != raft.StateLeader {
		return nil, raft.ErrSnapshotTemporarilyUnavailable
	}
	if e := s.unbound; e != nil {
		if status.RaftState != raft.StateLeader || status.GetTerm() != e.originTerm {
			d.detachSnapshotSend(e)
			return nil, raft.ErrSnapshotTemporarilyUnavailable
		}
		if !e.sealed {
			return nil, raft.ErrSnapshotTemporarilyUnavailable
		}
		if proto.Size(e.snapshot)+64+applicationPacketHeaderBytes > snapshotSendOutputBytes {
			s.fatal = ErrLimit
			return nil, raft.ErrSnapshotTemporarilyUnavailable
		}
		e.queued = true
		s.unbound = nil
		return e.snapshot, nil
	}
	cut, err := d.store.PublishedApplicationCut()
	if err != nil {
		s.captureFailure(err)
		return nil, raft.ErrSnapshotTemporarilyUnavailable
	}
	n := snapshotSendFixedBytes + cut.ImageBytes
	if s.offerCount() >= s.policy.MaxOffers || s.ownedBytes > s.policy.MaxOwnedBytes || n > s.policy.MaxOwnedBytes-s.ownedBytes {
		return nil, raft.ErrSnapshotTemporarilyUnavailable
	}
	if s.nextID == math.MaxUint64 {
		s.fatal = ErrLimit
		return nil, raft.ErrSnapshotTemporarilyUnavailable
	}
	// Reserve sender/output ownership before image/protobuf/backend copies.
	s.ownedBytes += n
	export, err := d.store.BeginPublishedApplicationExport(context.Background(), cut.ID)
	if err != nil {
		s.ownedBytes -= n
		s.captureFailure(err)
		return nil, raft.ErrSnapshotTemporarilyUnavailable
	}
	s.nextID++
	e := &SnapshotSend{originTerm: d.raw.BasicStatus().GetTerm(), d: d, export: export, incarnation: s.incarnation, id: s.nextID, ownedBytes: n, cutID: cut.ID, done: make(chan struct{})}
	e.self = e
	e.slot = slices.Index(s.offers[:], nil)
	s.offers[e.slot] = e
	s.unbound = e
	return nil, raft.ErrSnapshotTemporarilyUnavailable
}
func (s *applicationSnapshotSender) captureFailure(err error) {
	if snapshotSendFatal(err) {
		s.fatal = errors.Join(s.fatal, err)
	}
}
func snapshotSendFatal(err error) bool {
	if errors.Is(err, raftlog.ErrCorrupt) || errors.Is(err, raftlog.ErrPoisoned) {
		return true
	}
	return err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, raftlog.ErrLimit) && !errors.Is(err, raftlog.ErrInvalid) && !errors.Is(err, raft.ErrSnapOutOfDate)
}

// SnapshotSends returns a bounded owned list of live offers. Merely discovering
// an unbound offer does not put a peer into pending-snapshot state. Cancel a
// never-consumed offer with Close; a normal Raft retry can then reserve another.
func (d *Driver) SnapshotSends() ([]*SnapshotSend, error) {
	if d == nil {
		return nil, ErrInvalid
	}
	d.mu.Lock()
	defer d.unlock()
	if d.stopped != nil {
		return nil, errors.Join(ErrStopped, d.stopped)
	}
	s := d.snapshotSender
	if s == nil {
		return nil, ErrInvalid
	}
	out := make([]*SnapshotSend, 0, s.offerCount())
	for _, e := range s.offers {
		if e != nil && !e.closed {
			out = append(out, e)
		}
	}
	return out, nil
}
func (e *SnapshotSend) liveLocked(d *Driver) error {
	if e == nil || e.self != e || e.d == nil || e.d != d || d.snapshotSender == nil || e.incarnation != d.snapshotSender.incarnation || e.slot < 0 || e.slot >= len(d.snapshotSender.offers) || d.snapshotSender.offers[e.slot] != e {
		return ErrInvalid
	}
	if e.closed {
		return raftlog.ErrClosed
	}
	if d.stopped != nil {
		return errors.Join(ErrStopped, d.stopped)
	}
	return nil
}
func (e *SnapshotSend) begin(ctx context.Context, replay bool) (*raftlog.ApplicationExport, error) {
	if e == nil || e.d == nil || ctx == nil {
		return nil, ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	d := e.d
	d.mu.Lock()
	defer d.unlock()
	if err := e.liveLocked(d); err != nil {
		return nil, err
	}
	if !replay && e.ready {
		return nil, nil
	}
	if d.snapshotSender.busy {
		return nil, ErrUnavailable
	}
	if replay && (!e.bound || e.final) {
		return nil, ErrInvalid
	}
	d.snapshotSender.busy = true
	e.working = true
	return e.export, nil
}
func (e *SnapshotSend) finish(err error, ready bool, m raftlog.ApplicationSnapshotManifest, snap *pb.Snapshot, final bool) error {
	d := e.d
	d.mu.Lock()
	e.working = false
	d.snapshotSender.busy = false
	// Operational shared-store failures stop even an offer canceled during I/O.
	if snapshotSendFatal(err) {
		d.snapshotSender.captureFailure(err)
		_, _ = d.stop(errors.Join(d.stopped, err))
	}
	if !e.closed && err == nil {
		if ready {
			e.ready = true
			e.manifest = m
			e.snapshot = snap
			e.descriptor = bytes.Clone(snap.Data)
		}
		e.final = final
	}
	if e.closed {
		d.enqueueSnapshotCleanup(e)
		if err == nil {
			err = raftlog.ErrClosed
		}
	}
	d.unlock()
	return err
}

// Build verifies at most one bounded page outside Driver.mu. During this page
// RawNode events return retryable ErrUnavailable BEFORE accessing RawNode; Close
// and cancellation can still detach ownership promptly. Quota/cancellation is
// retryable; a fatal read failure stops the Driver before later RawNode events.
func (e *SnapshotSend) Build(ctx context.Context, b raftlog.ReadBudget) (bool, error) {
	if e == nil || e.d == nil || ctx == nil {
		return false, ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	export, err := e.begin(ctx, false)
	if err != nil {
		return false, err
	}
	if export == nil {
		return true, nil
	}
	ready, err := export.BuildManifest(ctx, b)
	var m raftlog.ApplicationSnapshotManifest
	var snap *pb.Snapshot
	if err == nil && ready {
		m, err = export.Manifest()
		if err == nil {
			var data []byte
			data, err = raftlog.EncodeApplicationSnapshotDescriptor(m)
			if err == nil {
				snap = &pb.Snapshot{Data: data, Metadata: &pb.SnapshotMetadata{Index: new(m.Index), Term: new(m.Term), ConfState: proto.Clone(m.ConfState).(*pb.ConfState)}}
			}
		}
	}
	if err == nil && ready {
		err = ctx.Err()
	}
	if err = e.finish(err, ready, m, snap, false); err != nil {
		return false, err
	}
	return ready, nil
}

// Manifest returns an owned bounded copy after Build completes. Returned copies
// belong to the caller. It does not consume a replay page or authorize feedback.
func (e *SnapshotSend) Manifest() (raftlog.ApplicationSnapshotManifest, error) {
	if e == nil || e.d == nil {
		return raftlog.ApplicationSnapshotManifest{}, ErrInvalid
	}
	d := e.d
	d.mu.Lock()
	if err := e.liveLocked(d); err != nil {
		d.unlock()
		return raftlog.ApplicationSnapshotManifest{}, err
	}
	if !e.ready {
		d.unlock()
		return raftlog.ApplicationSnapshotManifest{}, ErrInvalid
	}
	m := e.manifest
	d.unlock()
	if m.Image != nil {
		owned := make([]byte, len(m.Image))
		copy(owned, m.Image)
		m.Image = owned
	} // Exact exposed capacity is part of this API's image bound.
	m.ConfState = proto.Clone(m.ConfState).(*pb.ConfState)
	return m, nil
}

// Next emits one bounded page only after an actual MsgSnap binds this offer.
// Cancellation/quota refusal leaves the export cursor unchanged. Callers retain
// returned pages; no unbounded sender queue is accumulated internally.
func (e *SnapshotSend) Next(ctx context.Context, b raftlog.ReadBudget) (raftlog.ApplicationSnapshotChunk, error) {
	export, err := e.begin(ctx, true)
	if err != nil {
		return raftlog.ApplicationSnapshotChunk{}, err
	}
	chunk, err := export.Next(ctx, b)
	if err = e.finish(err, false, raftlog.ApplicationSnapshotManifest{}, nil, chunk.Final); err != nil {
		return raftlog.ApplicationSnapshotChunk{}, err
	}
	return chunk, nil
}

// SealSnapshotSend enables the next callback to consume this exact verified
// unbound owner. No scan/image copy occurs under Driver.mu. A later publication
// does not move it. Raft retry is required; Seal itself does not send a message.
func (d *Driver) SealSnapshotSend(e *SnapshotSend) error {
	if d == nil {
		return ErrInvalid
	}
	d.mu.Lock()
	defer d.unlock()
	if err := e.liveLocked(d); err != nil {
		return err
	}
	if err := d.check(); err != nil {
		return err
	}
	status := d.raw.BasicStatus()
	if status.RaftState != raft.StateLeader || status.GetTerm() != e.originTerm {
		d.detachSnapshotSend(e)
		return ErrInvalid
	}
	if !e.ready || e.queued || e.bound || d.snapshotSender.unbound != e {
		return ErrInvalid
	}
	e.sealed = true
	return nil
}

// bindSnapshotSend runs under Driver.mu before returning any outgoing snapshot.
// The pinned RawNode preserves the callback's exact snapshot pointer in Ready.
func (d *Driver) bindSnapshotSend(m *pb.Message) (*SnapshotSend, error) {
	s := d.snapshotSender
	if s == nil || m == nil || m.GetType() != pb.MsgSnap {
		return nil, ErrInvalid
	}
	for _, e := range s.offers {
		if e == nil || e.snapshot != m.GetSnapshot() {
			continue
		}
		if e.closed {
			return nil, nil
		}
		if !e.queued || e.bound || m.GetFrom() != d.config.ID || m.GetTo() == 0 || m.GetTo() == d.config.ID || m.GetTerm() == 0 || !proto.Equal(e.manifest.ConfState, m.GetSnapshot().GetMetadata().GetConfState()) || m.GetSnapshot().GetMetadata().GetIndex() != e.manifest.Index || m.GetSnapshot().GetMetadata().GetTerm() != e.manifest.Term || len(m.GetSnapshot().ProtoReflect().GetUnknown()) != 0 || len(m.GetSnapshot().GetMetadata().ProtoReflect().GetUnknown()) != 0 || !bytes.Equal(e.descriptor, m.GetSnapshot().Data) {
			return nil, ErrInvalid
		}
		status := d.raw.BasicStatus()
		if status.RaftState != raft.StateLeader || m.GetTerm() != status.GetTerm() {
			d.detachSnapshotSend(e)
			return nil, nil
		}
		if proto.Size(m)+applicationPacketHeaderBytes > snapshotSendOutputBytes {
			return nil, ErrLimit
		}
		for _, old := range s.offers {
			if old != nil && old != e && old.bound && old.to == m.GetTo() && !old.closed {
				d.detachSnapshotSend(old)
			}
		}
		e.to = m.GetTo()
		e.term = m.GetTerm()
		e.bound = true
		e.queued = false
		return e, nil
	}
	return nil, ErrInvalid
}

// ReportSnapshotSend applies feedback only to this incarnation/offer/peer/term.
// Success requires final replay; stale feedback cannot affect a replacement.
// The caller still decides whether receiver delivery actually succeeded.
func (d *Driver) ReportSnapshotSend(e *SnapshotSend, success bool) (Output, error) {
	if d == nil {
		return Output{}, ErrInvalid
	}
	d.mu.Lock()
	defer d.unlock()
	if err := e.liveLocked(d); err != nil {
		return Output{}, err
	}
	if err := d.check(); err != nil {
		return Output{}, err
	}
	status := d.raw.BasicStatus()
	if !e.bound || status.RaftState != raft.StateLeader || status.GetTerm() != e.term || success && !e.final {
		return Output{}, ErrInvalid
	}
	matched := false
	d.raw.WithProgress(func(id uint64, _ raft.ProgressType, p tracker.Progress) {
		if id == e.to && p.State == tracker.StateSnapshot && p.PendingSnapshot == e.manifest.Index {
			matched = true
		}
	})
	if !matched {
		return Output{}, ErrInvalid
	}
	state := raft.SnapshotFailure
	if success {
		state = raft.SnapshotFinish
	}
	d.raw.ReportSnapshot(e.to, state)
	d.detachSnapshotSend(e)
	return d.drain()
}

func (d *Driver) enqueueSnapshotCleanup(e *SnapshotSend) {
	if e.export != nil && !e.working && !e.cleaning {
		e.cleaning = true
		d.snapshotSender.cleanup[d.snapshotSender.cleanupN] = e
		d.snapshotSender.cleanupN++
	}
}
func (d *Driver) detachSnapshotSend(e *SnapshotSend) {
	if e.closed {
		return
	}
	e.closed = true
	if d.snapshotSender.unbound == e {
		d.snapshotSender.unbound = nil
	}
	d.enqueueSnapshotCleanup(e)
}
func (d *Driver) revokeSnapshotSends() {
	if d.snapshotSender != nil {
		for _, e := range d.snapshotSender.offers {
			if e != nil {
				d.detachSnapshotSend(e)
			}
		}
	}
}

// unlock releases Store-backed owners only after Driver.mu is released. Slots
// stay charged until cleanup finishes, including canceled in-flight pages.
func (d *Driver) unlock() {
	if d.snapshotSender == nil {
		d.mu.Unlock()
		return
	}
	for {
		pending := d.snapshotSender.cleanup
		count := d.snapshotSender.cleanupN
		clear(d.snapshotSender.cleanup[:])
		d.snapshotSender.cleanupN = 0
		d.mu.Unlock()
		for _, e := range pending[:count] {
			err := e.export.Close()
			d.mu.Lock()
			e.export = nil
			e.manifest = raftlog.ApplicationSnapshotManifest{}
			e.snapshot = nil
			e.descriptor = nil
			d.snapshotSender.ownedBytes -= e.ownedBytes
			e.ownedBytes = 0
			d.snapshotSender.offers[e.slot] = nil
			e.closeErr = err
			close(e.done)
			if err != nil {
				d.snapshotSender.cleanupErr = errors.Join(d.snapshotSender.cleanupErr, err)
				d.snapshotSender.captureFailure(err)
				d.stopped = errors.Join(d.stopped, err)
				d.revokeSnapshotSends()
			}
			d.mu.Unlock()
		}
		d.mu.Lock()
		if d.snapshotSender.cleanupN == 0 {
			d.mu.Unlock()
			return
		}
	}
}

// Close cancels an unbound exact owner. A live bound send returns ErrInvalid;
// use ReportSnapshotSend(false) so Raft leaves pending-snapshot state. Cleanup
// may wait for its one in-flight page,
// always outside Driver.mu. The immutable cleanup result is returned by every
// repeated Close, including its original error cause. Constructorless values refuse.
func (e *SnapshotSend) Close() error {
	if e == nil || e.d == nil {
		return ErrInvalid
	}
	d := e.d
	d.mu.Lock()
	if e.self != e || d.snapshotSender == nil || e.incarnation != d.snapshotSender.incarnation || e.done == nil {
		d.unlock()
		return ErrInvalid
	}
	if e.bound && !e.closed && d.stopped == nil {
		d.unlock()
		return ErrInvalid
	}
	d.detachSnapshotSend(e)
	done := e.done
	d.unlock()
	<-done
	return e.closeErr
}

func (s *applicationSnapshotSender) offerCount() int {
	n := 0
	for _, e := range s.offers {
		if e != nil {
			n++
		}
	}
	return n
}
func (d *Driver) reconcileSnapshotSends() {
	if d.snapshotSender == nil {
		return
	}
	status := d.raw.BasicStatus()
	var pending [16]bool
	d.raw.WithProgress(func(id uint64, _ raft.ProgressType, p tracker.Progress) {
		for j, e := range d.snapshotSender.offers {
			if e != nil && e.bound && e.to == id && p.State == tracker.StateSnapshot && p.PendingSnapshot == e.manifest.Index {
				pending[j] = true
			}
		}
	})
	for j, e := range d.snapshotSender.offers {
		if e != nil && (status.RaftState != raft.StateLeader || status.GetTerm() != e.originTerm || e.bound && !pending[j]) {
			d.detachSnapshotSend(e)
		}
	}
}
