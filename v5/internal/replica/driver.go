// Package replica owns one serialized etcd RawNode and its durable log adapter.
// This is a V2 consensus-adapter slice, not a distributed graph transaction engine.
package replica

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"math"
	"reflect"
	"slices"
	"sync"

	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/confchange"
	pb "go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/tracker"
	"google.golang.org/protobuf/proto"
)

var (
	// ErrInvalid marks rejected replica input.
	ErrInvalid = errors.New("replica: invalid input")
	// ErrStopped requires recovery before additional events.
	ErrStopped = errors.New("replica: stopped; reopen required")
	// ErrLimit marks bounded adapter admission, output or finite-term exhaustion.
	ErrLimit = errors.New("replica: resource limit")
	// ErrUnavailable requires routing or retry at an authoritative leader.
	ErrUnavailable = errors.New("replica: authoritative leader read unavailable")
)

// replicaTermCeiling reserves MaxUint64 as headroom for the pinned Raft's
// election/restore +1 paths. Reaching this final supported term is terminal:
// no further driver events or reopen are admitted, though Close remains valid.
const replicaTermCeiling uint64 = math.MaxUint64 - 1

// Entry is an owned application record. Empty normal records are Raft no-ops;
// configuration records advance Applied but are not application commands.
type Entry struct {
	// Generation is a receiver-local storage binding, zero in scalar/legacy mode.
	// Stage must echo it as BaseGeneration, never encode it in replicated effects.
	Generation  uint64
	Index, Term uint64
	Data        []byte
}

// Machine callbacks are deterministic, non-reentrant and free of network waits.
// Restore replaces all volatile state, including effects applied since its image.
// Apply must not independently acknowledge graph durability or publish private effects.
// Checkpoint must honor maxBytes; the driver also rejects oversized output, but
// cannot bound allocations performed by arbitrary application callbacks.
type Machine interface {
	Restore(index uint64, image []byte) error
	Apply(Entry) error
	Checkpoint(maxBytes int) ([]byte, error)
}

// ApplicationMachine stages private bytes for the same-store durable KV/root
// capability. Stage must honor the supplied budget before allocating output;
// storage rechecks framing and totals. Restore publishes only the synced root.
// Unbound mode remains singleton-only; semantic-bound RLM6 supports its exact
// fixed three voters and prepared snapshot protocol. Configuration changes stay closed.
type ApplicationMachine interface {
	Restore(index uint64, image []byte) error
	Stage(Entry, raftlog.ApplicationBudget) (raftlog.ApplicationBatch, error)
}

// Packet is owned serialized consensus traffic. The embedding application may
// deliver/drop/duplicate/reorder it after the method returns. Snapshot delivery
// failure uses ReportSnapshot in scalar mode or the exact SnapshotSend owner in
// bound application mode; enqueue is not durable receipt.
type Packet struct {
	From, To uint64
	Payload  []byte
	Snapshot bool
}

// ReadResult is a request-bound leader barrier whose index is already applied.
type ReadResult struct {
	Index   uint64
	Context []byte
}

// Output owns the packets and ready read barriers from one serialized event.
type Output struct {
	Packets []Packet
	// SnapshotSends pairs in order with the snapshot Packets only. Each handle
	// owns that exact source; ordinary packets do not consume an entry.
	SnapshotSends []*SnapshotSend
	Reads         []ReadResult
	Applied       uint64
}

// Config selects a provisional Pebble-backed replica. Fatal Pebble WAL/storage
// errors terminate the embedding PROCESS; this significant candidate tradeoff
// remains subject to engine comparison. MaxOutputBytes caps returned buffers,
// not all RawNode live memory or the application machine.
type Config struct {
	ApplicationSnapshotSends              ApplicationSnapshotSendLimits
	ID                                    uint64
	Store                                 *raftlog.Store
	Machine                               Machine
	ApplicationMachine                    ApplicationMachine
	Limits                                raftlog.Limits
	ElectionTick, HeartbeatTick           int
	MaxInflightMessages                   int
	MaxInflightBytes, MaxUncommittedBytes uint64
	MaxPacketBytes, MaxOutputBytes        int
}

// Driver serializes all RawNode operations and application callbacks.
// At term MaxUint64-1, driver events return ErrLimit without further mutation;
// Close and Applied remain available. No term reset or wraparound is performed.
type Driver struct {
	mu                 sync.Mutex
	snapshotSender     *applicationSnapshotSender
	binding            raftlog.ApplicationBinding
	event              *applicationEvent
	closeOwner         *applicationClose
	claimCleanupErr    error
	raw                *raft.RawNode
	store              *raftlog.Store
	machine            Machine
	applicationMachine ApplicationMachine
	config             Config
	applied            uint64
	conf               *pb.ConfState
	stopped            error
	readNonce          [16]byte
	readSeq            uint64
	reads              map[string]*pendingRead
	readBytes          int
	lastLeader         uint64
}

// Open restores the durable checkpoint and uses its index/membership together.
// It never bootstraps; first-open Initialize is an explicit separate store action.
// A durable term at or above the finite ceiling returns ErrLimit before Restore.
func Open(c Config) (*Driver, error) {
	// Normalize unused typed-nil providers before storing interface fields.
	if isNilMachine(c.Machine) {
		c.Machine = nil
	}
	if isNilMachine(c.ApplicationMachine) {
		c.ApplicationMachine = nil
	}
	if c.Store == nil || isNilMachine(c.Machine) == isNilMachine(c.ApplicationMachine) || c.ID == 0 || raft.IsLocalMsgTarget(c.ID) {
		return nil, ErrInvalid
	}
	if c.Limits == (raftlog.Limits{}) {
		c.Limits = c.Store.Limits()
	} else if c.Limits != c.Store.Limits() {
		return nil, ErrInvalid
	}
	if err := c.Limits.Validate(); err != nil {
		return nil, errors.Join(ErrInvalid, err)
	}
	if c.ElectionTick == 0 {
		c.ElectionTick = 10
	}
	if c.HeartbeatTick == 0 {
		c.HeartbeatTick = 1
	}
	if c.MaxInflightMessages == 0 {
		c.MaxInflightMessages = 16
	}
	if c.MaxInflightBytes == 0 {
		c.MaxInflightBytes = 8 << 20
	}
	if c.MaxUncommittedBytes == 0 {
		c.MaxUncommittedBytes = 16 << 20
	}
	if c.MaxPacketBytes == 0 {
		c.MaxPacketBytes = max(c.Limits.MaxSnapshotBytes, c.Limits.MaxReadBytes) + (1 << 16)
	}
	if c.MaxOutputBytes == 0 {
		c.MaxOutputBytes = 2 * c.MaxPacketBytes
	}
	if c.HeartbeatTick < 1 || c.ElectionTick <= c.HeartbeatTick || c.MaxInflightMessages < 1 || c.MaxInflightMessages > 256 || c.MaxInflightBytes > 64<<20 || c.MaxUncommittedBytes > 64<<20 || c.MaxPacketBytes > 65<<20 || c.MaxOutputBytes > 256<<20 || c.MaxPacketBytes < c.Limits.MaxReadBytes+(1<<16) || c.MaxOutputBytes < c.MaxPacketBytes || c.MaxInflightBytes < cReadBytes(c.Limits.MaxReadBytes) {
		return nil, ErrInvalid
	}
	if err := c.ApplicationSnapshotSends.validate(c); err != nil {
		return nil, err
	}
	binding := c.Store.ApplicationBinding()
	var hard *pb.HardState
	var cs *pb.ConfState
	var index uint64
	var image []byte
	var err error
	if binding != (raftlog.ApplicationBinding{}) {
		hard, cs, err = c.Store.InitialState()
		if err != nil {
			return nil, err
		}
		if err = ValidateApplicationMachineBinding(binding, c.ApplicationMachine); err != nil {
			return nil, err
		}
		if c.ApplicationSnapshotSends == (ApplicationSnapshotSendLimits{}) {
			return nil, ErrInvalid
		}
		index, image, err = c.Store.Checkpoint()
	} else {
		index, image, err = c.Store.Checkpoint()
		if err == nil {
			hard, cs, err = c.Store.InitialState()
		}
	}
	if err != nil {
		return nil, err
	}
	if hard.GetTerm() >= replicaTermCeiling {
		return nil, ErrLimit
	}
	app := c.Store.ApplicationLimits()
	if app.Enabled() != !isNilMachine(c.ApplicationMachine) {
		return nil, ErrInvalid
	}
	if app.Enabled() {
		voters := 1
		if binding != (raftlog.ApplicationBinding{}) {
			voters = 3
		}
		if c.ID != app.LocalVoter || len(cs.GetVoters()) != voters || !slices.Contains(cs.GetVoters(), c.ID) || len(cs.GetVotersOutgoing()) != 0 || len(cs.GetLearners()) != 0 || len(cs.GetLearnersNext()) != 0 || cs.GetAutoLeave() {
			return nil, ErrInvalid
		}
	}
	if len(cs.GetVoters()) == 0 {
		return nil, ErrInvalid
	}
	var restoreErr error
	if app.Enabled() {
		restoreErr = c.ApplicationMachine.Restore(index, image)
	} else {
		restoreErr = c.Machine.Restore(index, image)
	}
	if restoreErr != nil {
		return nil, restoreErr
	}
	d := &Driver{binding: binding, store: c.Store, machine: c.Machine, applicationMachine: c.ApplicationMachine, config: c, applied: index, conf: cs, reads: make(map[string]*pendingRead)}
	if _, err := rand.Read(d.readNonce[:]); err != nil {
		return nil, err
	}
	var storage raft.Storage = c.Store
	if c.ApplicationSnapshotSends != (ApplicationSnapshotSendLimits{}) {
		d.snapshotSender = newApplicationSnapshotSender(d)
		storage = &applicationSnapshotStorage{Store: c.Store, d: d}
	}
	raw, err := raft.NewRawNode(&raft.Config{ID: c.ID, Storage: storage, Applied: index, ElectionTick: c.ElectionTick, HeartbeatTick: c.HeartbeatTick, AsyncStorageWrites: false, ReadOnlyOption: raft.ReadOnlySafe, CheckQuorum: true, PreVote: true, MaxSizePerMsg: cReadBytes(c.Limits.MaxReadBytes), MaxCommittedSizePerReady: cReadBytes(c.Limits.MaxReadBytes), MaxUncommittedEntriesSize: c.MaxUncommittedBytes, MaxInflightMsgs: c.MaxInflightMessages, MaxInflightBytes: c.MaxInflightBytes, StepDownOnRemoval: true})
	if err != nil {
		return nil, err
	}
	d.raw = raw
	return d, nil
}

func (d *Driver) check() error {
	if d.event != nil && !d.event.rawPhase {
		return ErrUnavailable
	}
	if d.stopped != nil {
		return errors.Join(ErrStopped, d.stopped)
	}
	if d.snapshotSender != nil {
		if d.snapshotSender.fatal != nil {
			_, _ = d.stop(d.snapshotSender.fatal)
			return errors.Join(ErrStopped, d.stopped)
		}
		if d.snapshotSender.busy {
			return ErrUnavailable
		}
	}
	d.reconcileSnapshotSends()
	if d.raw.BasicStatus().GetTerm() >= replicaTermCeiling {
		return ErrLimit
	}
	return nil
}
func (d *Driver) stop(err error) (Output, error) {
	d.stopped = err
	d.revokeSnapshotSends()
	return Output{}, err
}

// Tick advances one logical liveness tick and processes resulting work.
// At the finite term ceiling it returns ErrLimit before election advancement.
func (d *Driver) Tick() (out Output, err error) {
	d.mu.Lock()
	owner, err := d.beginEvent()
	if err != nil {
		return d.failEventAdmission(err)
	}
	defer func() { out, err = d.finishEvent(owner, out, err) }()
	d.raw.Tick()
	return d.drain()
}

// Campaign asks the established consensus implementation to run an election.
// At the finite term ceiling it returns ErrLimit before advancing RawNode.
func (d *Driver) Campaign() (out Output, err error) {
	d.mu.Lock()
	owner, err := d.beginEvent()
	if err != nil {
		return d.failEventAdmission(err)
	}
	defer func() { out, err = d.finishEvent(owner, out, err) }()
	if err := d.raw.Campaign(); err != nil {
		return Output{}, err
	}
	return d.drain()
}

// Propose accepts a proposal only. It does NOT return a graph commit receipt;
// proposals can be dropped/lost and application request-key recovery is required.
func (d *Driver) Propose(data []byte) (out Output, err error) {
	d.mu.Lock()
	owner, err := d.beginEvent()
	if err != nil {
		return d.failEventAdmission(err)
	}
	defer func() { out, err = d.finishEvent(owner, out, err) }()
	if len(data) > d.config.Limits.MaxEntryBytes-74 {
		return Output{}, ErrLimit
	}
	if d.applicationMachine != nil {
		if err := d.store.ReclaimApplication(); err != nil {
			return d.stop(err)
		}
		if err := d.store.AdmitApplication(len(data)); err != nil {
			return Output{}, err
		}
	}
	if err := d.raw.Propose(bytes.Clone(data)); err != nil {
		return Output{}, err
	}
	return d.drain()
}

// Step rejects malformed consensus input before RawNode can mutate state.
// It also rejects local-message injection and mismatched transport identities.
// MaxUint64 transport terms are unsupported; a TimeoutNow that would elect
// beyond the finite ceiling returns ErrLimit before any term/state change.
func (d *Driver) Step(p Packet) (out Output, err error) {
	d.mu.Lock()
	var owner *applicationEvent
	if d.boundApplication() {
		owner, err = d.beginPacketEvent()
	} else {
		owner, err = d.beginEvent()
	}
	if err != nil {
		return d.failEventAdmission(err)
	}
	defer func() { out, err = d.finishEvent(owner, out, err) }()
	if d.applicationMachine != nil {
		if !d.boundApplication() {
			return Output{}, ErrInvalid
		}
		m, err := d.decodeBoundPacket(p, false)
		if err != nil {
			return Output{}, err
		}
		if err := d.validateBoundMessage(m, false); err != nil {
			return Output{}, err
		}
		if err := d.raw.Step(m); err != nil {
			return Output{}, err
		}
		return d.drain()
	}
	if len(p.Payload) > d.config.MaxPacketBytes {
		return Output{}, ErrLimit
	}
	if err := preflightWire(p.Payload, 0, d.config.Limits); err != nil {
		return Output{}, err
	}
	m := &pb.Message{}
	if err := (proto.UnmarshalOptions{RecursionLimit: 8}).Unmarshal(p.Payload, m); err != nil {
		return Output{}, errors.Join(ErrInvalid, err)
	}
	if m.GetTo() != d.config.ID || m.GetTo() != p.To || m.GetFrom() != p.From || m.GetFrom() == 0 || raft.IsLocalMsgTarget(m.GetFrom()) || raft.IsLocalMsg(m.GetType()) || m.GetType() == pb.MsgReadIndex || m.GetType() == pb.MsgReadIndexResp || m.GetType() < pb.MsgHup || m.GetType() > pb.MsgForgetLeader || p.Snapshot != (m.GetType() == pb.MsgSnap) {
		return Output{}, ErrInvalid
	}
	if len(m.GetEntries()) > d.config.Limits.MaxReadEntries || len(m.GetSnapshot().GetData()) > d.config.Limits.MaxSnapshotBytes {
		return Output{}, ErrLimit
	}
	var total int
	for _, e := range m.GetEntries() {
		if e == nil || len(e.GetData()) > d.config.Limits.MaxEntryBytes-74 {
			return Output{}, ErrLimit
		}
		total += proto.Size(e)
		if total > d.config.Limits.MaxReadBytes {
			return Output{}, ErrLimit
		}
	}
	status := d.raw.BasicStatus()
	if err := validateIncoming(m, d.conf, d.applied, status.RaftState == raft.StateLeader, d.config.Limits); err != nil {
		return Output{}, err
	}
	// TimeoutNow may first raise our term and then immediately campaign in
	// that new term; guard both increments before handing it to RawNode.
	if m.GetType() == pb.MsgTimeoutNow && max(status.GetTerm(), m.GetTerm()) >= replicaTermCeiling {
		return Output{}, ErrLimit
	}
	if m.GetType() == pb.MsgHeartbeat {
		last, err := d.store.LastIndex()
		if err != nil {
			return d.stop(err)
		}
		// sendHeartbeat caps commit at the follower's acknowledged Match.
		// Every public event drains and syncs entries before returning packets.
		if m.GetCommit() > last {
			return Output{}, ErrInvalid
		}
	}
	if m.GetType() == pb.MsgSnap && m.GetSnapshot().GetMetadata().GetIndex() > status.GetCommit() {
		term, err := d.store.Term(status.GetCommit())
		if err != nil {
			return d.stop(err)
		}
		if m.GetSnapshot().GetMetadata().GetTerm() < term {
			return Output{}, ErrInvalid
		}
	}
	if err := d.raw.Step(m); err != nil {
		return Output{}, err
	}
	return d.drain()
}

type pendingRead struct {
	user                []byte
	index               uint64
	resolved, cancelled bool
}

func isNilMachine(m any) bool {
	if m == nil {
		return true
	}
	v := reflect.ValueOf(m)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	}
	return false
}

// ReadIndex is LEADER-ONLY and requires a current-term committed entry.
// Followers/new leaders return ErrUnavailable for routing/retry without enqueue.
// Forwarded network reads are rejected. Outstanding requests are bounded BEFORE
// RawNode; repeating an eligible pending context reissues its unique token.
// A leader change drops pending requests without producing any certificate.
func (d *Driver) ReadIndex(context []byte) (out Output, err error) {
	d.mu.Lock()
	owner, err := d.beginEvent()
	if err != nil {
		return d.failEventAdmission(err)
	}
	defer func() { out, err = d.finishEvent(owner, out, err) }()
	if len(context) > 1024 {
		return Output{}, ErrLimit
	}
	status := d.raw.BasicStatus()
	if status.RaftState != raft.StateLeader {
		return Output{}, ErrUnavailable
	}
	hard, _, err := d.store.InitialState()
	if err != nil {
		return d.stop(err)
	}
	term, err := d.store.Term(hard.GetCommit())
	if err != nil {
		return d.stop(err)
	}
	if term != status.GetTerm() {
		return Output{}, ErrUnavailable
	}
	for token, r := range d.reads {
		if bytes.Equal(r.user, context) {
			if r.cancelled {
				return Output{}, ErrInvalid
			}
			d.raw.ReadIndex([]byte(token))
			return d.drain()
		}
	}

	if len(d.reads) >= 32 || len(context) > 32768-d.readBytes || d.readSeq == ^uint64(0) {
		return Output{}, ErrLimit
	}
	d.readSeq++
	token := append(bytes.Clone(d.readNonce[:]), make([]byte, 8)...)
	binary.BigEndian.PutUint64(token[16:], d.readSeq)
	d.reads[string(token)] = &pendingRead{user: bytes.Clone(context)}
	d.readBytes += len(context)
	d.raw.ReadIndex(token)
	return d.drain()
}

// CancelReadIndex suppresses delivery. Quota stays reserved until its response
// or a leader change, because RawNode cannot remove an in-flight request safely.
// Quorum loss therefore backpressures further distinct requests; it never grows
// an unbounded hidden queue or returns a stale certificate.
func (d *Driver) CancelReadIndex(context []byte) (err error) {
	d.mu.Lock()
	owner, err := d.beginEvent()
	if err != nil {
		_, err = d.failEventAdmission(err)
		return err
	}
	defer func() { _, err = d.finishEvent(owner, Output{}, err) }()
	for _, r := range d.reads {
		if bytes.Equal(r.user, context) {
			r.cancelled = true
			return nil
		}
	}
	return ErrInvalid
}

// ReportUnreachable supplies failed network-delivery feedback.
func (d *Driver) ReportUnreachable(id uint64) (out Output, err error) {
	d.mu.Lock()
	owner, err := d.beginEvent()
	if err != nil {
		return d.failEventAdmission(err)
	}
	defer func() { out, err = d.finishEvent(owner, out, err) }()
	if d.applicationMachine != nil && !d.boundApplication() {
		return Output{}, ErrInvalid
	}
	if d.boundApplication() && (id == d.config.ID || !slices.Contains(d.conf.Voters, id)) {
		return Output{}, ErrInvalid
	}
	d.raw.ReportUnreachable(id)
	return d.drain()
}

// ReportSnapshot supplies snapshot delivery/application feedback.
func (d *Driver) ReportSnapshot(id uint64, success bool) (out Output, err error) {
	d.mu.Lock()
	owner, err := d.beginEvent()
	if err != nil {
		return d.failEventAdmission(err)
	}
	defer func() { out, err = d.finishEvent(owner, out, err) }()
	if d.applicationMachine != nil || d.snapshotSender != nil {
		return Output{}, ErrInvalid
	}
	status := raft.SnapshotFailure
	if success {
		status = raft.SnapshotFinish
	}
	d.raw.ReportSnapshot(id, status)
	return d.drain()
}

// TransferLeader requests a consensus leadership transfer.
func (d *Driver) TransferLeader(id uint64) (out Output, err error) {
	d.mu.Lock()
	owner, err := d.beginEvent()
	if err != nil {
		return d.failEventAdmission(err)
	}
	defer func() { out, err = d.finishEvent(owner, out, err) }()
	if d.applicationMachine != nil {
		return Output{}, ErrInvalid
	}
	d.raw.TransferLeader(id)
	return d.drain()
}

// ProposeConfChange checks V1/V2 schema before admission. The leader checks
// resulting membership with the library changer; lagging followers may forward
// against a newer leader membership. Ownership moves remain an application protocol.
func (d *Driver) ProposeConfChange(change pb.ConfChangeI) (out Output, err error) {
	d.mu.Lock()
	owner, err := d.beginEvent()
	if err != nil {
		return d.failEventAdmission(err)
	}
	defer func() { out, err = d.finishEvent(owner, out, err) }()
	if d.applicationMachine != nil {
		return Output{}, ErrInvalid
	}
	if change == nil {
		return Output{}, ErrInvalid
	}
	var cc pb.ConfChangeI
	switch c := change.(type) {
	case *pb.ConfChange:
		if c == nil || len(c.ProtoReflect().GetUnknown()) != 0 {
			return Output{}, ErrInvalid
		}
		if proto.Size(c) > d.config.Limits.MaxEntryBytes-74 {
			return Output{}, ErrLimit
		}
		cc = proto.Clone(c).(*pb.ConfChange)
	case *pb.ConfChangeV2:
		if c == nil {
			return Output{}, ErrInvalid
		}
		if len(c.GetChanges()) > 256 || proto.Size(c) > d.config.Limits.MaxEntryBytes-74 {
			return Output{}, ErrLimit
		}
		cc = proto.Clone(c).(*pb.ConfChangeV2)
	default:
		return Output{}, ErrInvalid
	}
	if err := validateChangeSchema(cc.AsV2()); err != nil {
		return Output{}, err
	}
	if d.raw.BasicStatus().RaftState == raft.StateLeader {
		if err := validateChange(d.conf, cc.AsV2(), d.applied); err != nil {
			return Output{}, err
		}
	}
	if proto.Size(cc.AsV2()) > d.config.Limits.MaxEntryBytes-74 {
		return Output{}, ErrLimit
	}
	if err := d.raw.ProposeConfChange(cc); err != nil {
		return Output{}, err
	}
	return d.drain()
}

func validateChangeSchema(cc *pb.ConfChangeV2) error {
	if cc == nil || len(cc.ProtoReflect().GetUnknown()) != 0 || len(cc.GetChanges()) > 256 || cc.GetTransition() < pb.ConfChangeTransitionAuto || cc.GetTransition() > pb.ConfChangeTransitionJointExplicit {
		return ErrInvalid
	}
	for _, c := range cc.GetChanges() {
		if c == nil || len(c.ProtoReflect().GetUnknown()) != 0 || c.GetNodeId() == 0 || raft.IsLocalMsgTarget(c.GetNodeId()) || c.GetType() < pb.ConfChangeAddNode || c.GetType() > pb.ConfChangeAddLearnerNode {
			return ErrInvalid
		}
	}
	return nil
}

func validateChange(cs *pb.ConfState, cc *pb.ConfChangeV2, last uint64) error {
	if err := validateChangeSchema(cc); err != nil {
		return err
	}
	t := tracker.MakeProgressTracker(1, 1)
	cfg, progress, err := confchange.Restore(confchange.Changer{Tracker: t, LastIndex: last}, cs)
	if err != nil {
		return errors.Join(ErrInvalid, err)
	}
	t.Config, t.Progress = cfg, progress
	changer := confchange.Changer{Tracker: t, LastIndex: last}
	if cc.LeaveJoint() {
		_, _, err = changer.LeaveJoint()
	} else if auto, joint := cc.EnterJoint(); joint {
		_, _, err = changer.EnterJoint(auto, cc.GetChanges()...)
	} else {
		_, _, err = changer.Simple(cc.GetChanges()...)
	}
	if err != nil {
		return errors.Join(ErrInvalid, err)
	}
	return nil
}

func (d *Driver) drain() (Output, error) {
	if d.snapshotSender != nil && d.snapshotSender.fatal != nil {
		return d.stop(d.snapshotSender.fatal)
	}
	out := Output{Applied: d.applied}
	used := 0
	for {
		if d.snapshotSender != nil && d.snapshotSender.fatal != nil {
			return d.stop(d.snapshotSender.fatal)
		}
		d.reconcileSnapshotSends()
		if !d.raw.HasReady() {
			break
		}
		rd := d.raw.Ready()
		if rd.SoftState != nil && rd.Lead != d.lastLeader {
			clear(d.reads)
			d.readBytes = 0
			d.lastLeader = rd.Lead
		}
		if err := d.checkApplicationReady(rd); err != nil {
			return d.stop(err)
		}
		if d.boundApplication() && !raft.IsEmptySnap(rd.Snapshot) {
			if d.event == nil || d.event.claim == nil {
				return d.stop(ErrInvalid)
			}
			root, err := d.store.PersistApplicationReady(rd, d.event.claim)
			if err != nil {
				return d.stop(err)
			}
			if err := d.applicationMachine.Restore(root.Index, root.Image); err != nil {
				return d.stop(err)
			}
			d.applied = root.Index
			d.conf = proto.Clone(rd.Snapshot.GetMetadata().GetConfState()).(*pb.ConfState)
		} else {
			if err := d.store.Persist(rd); err != nil {
				return d.stop(err)
			}
			if !raft.IsEmptySnap(rd.Snapshot) {
				idx := rd.Snapshot.GetMetadata().GetIndex()
				if err := d.machine.Restore(idx, bytes.Clone(rd.Snapshot.GetData())); err != nil {
					return d.stop(err)
				}
				d.applied = idx
				d.conf = proto.Clone(rd.Snapshot.GetMetadata().GetConfState()).(*pb.ConfState)
			}
		}
		for _, e := range rd.CommittedEntries {
			if e.GetIndex() <= d.applied {
				continue
			}
			if e.GetIndex() != d.applied+1 {
				return d.stop(raftlog.ErrCorrupt)
			}
			switch e.GetType() {
			case pb.EntryNormal:
				entry := Entry{Index: e.GetIndex(), Term: e.GetTerm(), Data: bytes.Clone(e.GetData())}
				if d.applicationMachine != nil {
					entry.Generation = d.store.ApplicationGeneration()
				}
				if d.applicationMachine != nil {
					b, err := d.applicationMachine.Stage(entry, d.store.ApplicationBudget())
					if err != nil {
						return d.stop(err)
					}
					if err := d.store.InstallApplication(entry.Index, b); err != nil {
						return d.stop(err)
					}
					if err := d.applicationMachine.Restore(entry.Index, bytes.Clone(b.Image)); err != nil {
						return d.stop(err)
					}
				} else if err := d.machine.Apply(entry); err != nil {
					return d.stop(err)
				}
			case pb.EntryConfChange:
				cc := &pb.ConfChange{}
				if err := proto.Unmarshal(e.GetData(), cc); err != nil {
					return d.stop(errors.Join(raftlog.ErrCorrupt, err))
				}
				if len(cc.ProtoReflect().GetUnknown()) != 0 {
					return d.stop(errors.Join(raftlog.ErrCorrupt, ErrInvalid))
				}
				if err := validateChange(d.conf, cc.AsV2(), e.GetIndex()); err != nil {
					return d.stop(err)
				}
				d.conf = d.raw.ApplyConfChange(cc)
				if d.snapshotSender != nil && d.snapshotSender.fatal != nil {
					return d.stop(d.snapshotSender.fatal)
				}
			case pb.EntryConfChangeV2:
				cc := &pb.ConfChangeV2{}
				if err := proto.Unmarshal(e.GetData(), cc); err != nil {
					return d.stop(errors.Join(raftlog.ErrCorrupt, err))
				}
				if err := validateChange(d.conf, cc, e.GetIndex()); err != nil {
					return d.stop(errors.Join(raftlog.ErrCorrupt, err))
				}
				d.conf = d.raw.ApplyConfChange(cc)
				if d.snapshotSender != nil && d.snapshotSender.fatal != nil {
					return d.stop(d.snapshotSender.fatal)
				}
			default:
				return d.stop(raftlog.ErrCorrupt)
			}
			d.applied = e.GetIndex()
		}
		if d.applicationMachine != nil {
			if err := d.store.ReclaimApplication(); err != nil {
				return d.stop(err)
			}
		}
		for _, m := range rd.Messages {
			envelope := 0
			if d.boundApplication() {
				envelope = applicationPacketHeaderBytes
			}
			ordinaryLimit := d.config.MaxOutputBytes
			if d.snapshotSender != nil {
				ordinaryLimit -= d.snapshotSender.policy.MaxOffers * snapshotSendOutputBytes
				if m.GetType() == pb.MsgSnap {
					e, err := d.bindSnapshotSend(m)
					if err != nil {
						return d.stop(err)
					}
					if e == nil {
						continue
					}
					out.SnapshotSends = append(out.SnapshotSends, e)
					b, err := proto.Marshal(m)
					if err != nil {
						return d.stop(err)
					}
					packet := Packet{From: m.GetFrom(), To: m.GetTo(), Payload: b, Snapshot: true}
					if d.boundApplication() {
						packet, err = EncodeApplicationPacket(d.binding, packet, d.config.MaxPacketBytes)
						if err != nil {
							return d.stop(err)
						}
					}
					out.Packets = append(out.Packets, packet)
					continue
				}
			}
			n := proto.Size(m)
			if n+envelope > d.config.MaxPacketBytes || n+envelope > ordinaryLimit-used {
				return d.stop(ErrLimit)
			}
			b, err := proto.Marshal(m)
			if err != nil {
				return d.stop(err)
			}
			if len(b)+envelope > d.config.MaxPacketBytes || len(b)+envelope > ordinaryLimit-used {
				return d.stop(ErrLimit)
			}
			packet := Packet{From: m.GetFrom(), To: m.GetTo(), Payload: b, Snapshot: m.GetType() == pb.MsgSnap}
			if d.boundApplication() {
				packet, err = EncodeApplicationPacket(d.binding, packet, d.config.MaxPacketBytes)
				if err != nil {
					return d.stop(err)
				}
			}
			used += len(packet.Payload)
			out.Packets = append(out.Packets, packet)
		}
		for _, r := range rd.ReadStates {
			if pending := d.reads[string(r.RequestCtx)]; pending != nil {
				pending.index = r.Index
				pending.resolved = true
			}
		}
		d.raw.Advance(rd)
		if d.snapshotSender != nil && d.snapshotSender.fatal != nil {
			return d.stop(d.snapshotSender.fatal)
		}
	}
	for token, r := range d.reads {
		if r.resolved && r.index <= d.applied {
			if !r.cancelled {
				if len(r.user) > d.ordinaryOutputLimit()-used {
					return d.stop(ErrLimit)
				}
				used += len(r.user)
				out.Reads = append(out.Reads, ReadResult{Index: r.index, Context: bytes.Clone(r.user)})
			}
			d.readBytes -= len(r.user)
			delete(d.reads, token)
		}
	}
	out.Applied = d.applied
	return out, nil
}

// SaveCheckpoint makes the machine's image and exact applied membership durable.
// It does not reclaim history or create a graph cut. A failure stops this driver.
func (d *Driver) SaveCheckpoint() (err error) {
	d.mu.Lock()
	owner, err := d.beginEvent()
	if err != nil {
		_, err = d.failEventAdmission(err)
		return err
	}
	defer func() { _, err = d.finishEvent(owner, Output{}, err) }()
	if d.applicationMachine != nil {
		index, _, err := d.store.Checkpoint()
		if err == nil && index != d.applied {
			err = raftlog.ErrCorrupt
		}
		if err != nil {
			_, _ = d.stop(err)
		}
		return err
	}
	image, err := d.machine.Checkpoint(d.config.Limits.MaxSnapshotBytes)
	if err == nil && len(image) > d.config.Limits.MaxSnapshotBytes {
		err = ErrLimit
	}
	if err == nil {
		err = d.store.SaveCheckpoint(d.applied, d.conf, image)
	}
	if err != nil {
		_, _ = d.stop(err)
	}
	return err
}

// Applied returns volatile application progress, never a graph cut or receipt.
func (d *Driver) Applied() uint64 { d.mu.Lock(); defer d.unlock(); return d.applied }

// Close stops the driver and closes its store; it deliberately does not checkpoint.
func (d *Driver) Close() error {
	if d.fencedApplication() {
		return d.closeApplication()
	}
	d.mu.Lock()
	if d.stopped == nil {
		d.stopped = ErrStopped
	} else if !errors.Is(d.stopped, ErrStopped) {
		d.stopped = errors.Join(ErrStopped, d.stopped)
	}
	var owners [16]*SnapshotSend
	if d.snapshotSender != nil {
		owners = d.snapshotSender.offers
	}
	d.revokeSnapshotSends()
	d.unlock()
	storeErr := d.store.Close()
	// An in-flight page can publish a cleanup failure during Store.Close. Wait
	// outside Driver.mu, then read the immutable per-owner/sender result.
	for _, e := range owners {
		if e != nil {
			<-e.done
		}
	}
	d.mu.Lock()
	var cleanupErr error
	if d.snapshotSender != nil {
		cleanupErr = d.snapshotSender.cleanupErr
	}
	if cleanupErr != nil && !errors.Is(d.stopped, cleanupErr) {
		d.stopped = errors.Join(d.stopped, cleanupErr)
	}
	if storeErr != nil {
		d.stopped = errors.Join(d.stopped, storeErr)
	}
	d.unlock()
	if storeErr == nil {
		return cleanupErr
	}
	return errors.Join(cleanupErr, storeErr)
}

func cReadBytes(n int) uint64 {
	if n < 0 {
		return 0
	}
	return uint64(n)
}

// checkApplicationReady is a second guard BEFORE destructive persistence, even
// if a caller bypasses Step and injects directly into the underlying RawNode.
func (d *Driver) checkApplicationReady(rd raft.Ready) error {
	if d.applicationMachine == nil {
		return nil
	}
	if !raft.IsEmptySnap(rd.Snapshot) && (!d.boundApplication() || d.event == nil || d.event.claim == nil) {
		return ErrInvalid
	}
	for _, e := range rd.Entries {
		if e == nil || e.GetType() != pb.EntryNormal {
			return ErrInvalid
		}
	}
	for _, e := range rd.CommittedEntries {
		if e == nil || e.GetType() != pb.EntryNormal {
			return ErrInvalid
		}
	}
	if len(rd.Messages) != 0 && !d.boundApplication() {
		return ErrInvalid
	}
	return nil
}

func (d *Driver) ordinaryOutputLimit() int {
	n := d.config.MaxOutputBytes
	if d.snapshotSender != nil {
		n -= d.snapshotSender.policy.MaxOffers * snapshotSendOutputBytes
	}
	return n
}
