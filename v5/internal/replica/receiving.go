package replica

import (
	"errors"
	"slices"
	"sync"

	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// ApplicationReceivingConfig selects privileged mechanical receiving state.
// Binding, source and term are expected scalar coordinates, NOT authenticated
// identity, retirement or quorum authority. The sole graphapply owner must check
// its live replacement admission and actual transport association before each
// invocation and again before delivering any returned packets. Store is borrowed.
type ApplicationReceivingConfig struct {
	Store                          *raftlog.Store
	Binding                        raftlog.ApplicationBinding
	ID, Source, Term               uint64
	Limits                         raftlog.Limits
	MaxPacketBytes, MaxOutputBytes int
}

// ApplicationReceiver owns a serialized empty-membership RawNode. It exposes
// no campaign, tick, proposal, read, bootstrap, activation or RawNode accessor.
// It never creates a normal Driver, Host or application materializer. Scalar
// configuration/returned reject packets grant no replacement or graph authority.
type ApplicationReceiver struct {
	self    *ApplicationReceiver
	mu      sync.Mutex
	config  ApplicationReceivingConfig
	raw     *raft.RawNode
	closed  bool
	stopped error
}

// OpenApplicationReceiver borrows a same-binding, fixed-three configured Store
// whose active membership/application/log state is genuinely empty. Known durable
// term may be lower than the expected source term, never higher. No initialization
// or term write occurs here, and failed construction does not close the Store.
func OpenApplicationReceiver(c ApplicationReceivingConfig) (*ApplicationReceiver, error) {
	if c.Store == nil || c.ID == 0 || c.Source == 0 || c.Source == c.ID || c.Term == 0 || c.Binding.Validate() != nil {
		return nil, ErrInvalid
	}
	if c.Term >= replicaTermCeiling {
		return nil, ErrLimit
	}
	if c.Limits == (raftlog.Limits{}) {
		c.Limits = c.Store.Limits()
	}
	if c.Limits != c.Store.Limits() {
		return nil, ErrInvalid
	}
	if err := c.Limits.Validate(); err != nil {
		return nil, errors.Join(ErrInvalid, err)
	}
	if c.MaxPacketBytes == 0 {
		c.MaxPacketBytes = max(c.Limits.MaxReadBytes, c.Limits.MaxSnapshotBytes) + (1 << 16)
	}
	if c.MaxOutputBytes == 0 {
		c.MaxOutputBytes = 512
	}
	if c.MaxPacketBytes < applicationPacketHeaderBytes+64 || c.MaxPacketBytes > 65<<20 || c.MaxOutputBytes < 512 || c.MaxOutputBytes > 256<<20 {
		return nil, ErrInvalid
	}
	state, err := c.Store.ApplicationReceivingState()
	if err != nil {
		return nil, errors.Join(ErrInvalid, err)
	}
	if state.Binding != c.Binding || state.LocalVoter != c.ID || !slices.Contains(state.Voters[:], c.Source) || state.Term > c.Term {
		return nil, ErrInvalid
	}
	raw, err := raft.NewRawNode(&raft.Config{ID: c.ID, Storage: c.Store, Applied: 0,
		ElectionTick: 10, HeartbeatTick: 1, AsyncStorageWrites: false, CheckQuorum: true, PreVote: true,
		MaxSizePerMsg: cReadBytes(c.Limits.MaxReadBytes), MaxCommittedSizePerReady: cReadBytes(c.Limits.MaxReadBytes), MaxInflightMsgs: 2})
	if err != nil {
		return nil, err
	}
	// Construction performs multiple Storage calls; recheck the borrowed Store
	// before exposing the owner if another privileged caller changed it meanwhile.
	current, err := c.Store.ApplicationReceivingState()
	if err != nil {
		return nil, errors.Join(ErrInvalid, err)
	}
	if current != state || raw.BasicStatus().GetTerm() != state.Term || raw.HasReady() {
		return nil, ErrInvalid
	}
	r := &ApplicationReceiver{config: c, raw: raw}
	r.self = r
	return r, nil
}

func (r *ApplicationReceiver) checkLocked() error {
	if r.closed || r.stopped != nil {
		return errors.Join(ErrStopped, r.stopped)
	}
	return nil
}

func (r *ApplicationReceiver) stopLocked(err error) ([]Packet, error) {
	r.stopped = errors.Join(r.stopped, err)
	return nil, errors.Join(ErrStopped, r.stopped)
}

// StepReceiving accepts only an exact-source/target/term RP1 ordinary MsgApp
// with a nonzero missing predecessor. Empty membership alone does not prevent
// votes, and predecessor zero could admit log entries; both are gated BEFORE
// RawNode.Step. The sole privileged owner supplies authenticated association and
// live-admission checks; this method does not infer those from packet fields.
// Genuine Ready term persistence succeeds before SAME-Ready Advance and before
// any original reject packet is returned. An error after Step stops the handle
// and discards all output; failed persistence never advances or acknowledges.
func (r *ApplicationReceiver) StepReceiving(p Packet) ([]Packet, error) {
	if r == nil || r.self != r {
		return nil, ErrInvalid
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.checkLocked(); err != nil {
		return nil, err
	}
	c := r.config
	if p.From != c.Source || p.To != c.ID || p.Snapshot {
		return nil, ErrInvalid
	}
	inner, err := DecodeApplicationPacket(c.Binding, p, c.MaxPacketBytes)
	if err != nil {
		return nil, err
	}
	limits := c.Limits
	limits.MaxSnapshotBytes = 0
	if err := preflightWire(inner.Payload, 0, limits); err != nil {
		return nil, err
	}
	if err := preflightApplicationMessage(inner.Payload, p, false); err != nil {
		return nil, err
	}
	m := new(pb.Message)
	if err := (proto.UnmarshalOptions{RecursionLimit: 8}).Unmarshal(inner.Payload, m); err != nil {
		return nil, errors.Join(ErrInvalid, err)
	}
	if m.GetType() != pb.MsgApp || m.GetTerm() != c.Term || m.GetIndex() == 0 || m.GetSnapshot() != nil || m.GetReject() || len(m.GetContext()) != 0 {
		return nil, ErrInvalid
	}
	if err := validateIncoming(m, nil, 0, false, c.Limits); err != nil {
		return nil, err
	}
	state, err := c.Store.ApplicationReceivingState()
	if err != nil {
		return r.stopLocked(err)
	}
	if state.Binding != c.Binding || state.LocalVoter != c.ID || state.Term > c.Term || r.raw.BasicStatus().GetTerm() != state.Term || r.raw.HasReady() {
		return r.stopLocked(ErrInvalid)
	}
	if err := r.raw.Step(m); err != nil {
		return r.stopLocked(err)
	}
	var packets []Packet
	used := 0
	for steps := 0; r.raw.HasReady(); steps++ {
		if steps >= 2 {
			return r.stopLocked(ErrLimit)
		}
		rd := r.raw.Ready()
		status := r.raw.BasicStatus()
		if status.RaftState != raft.StateFollower || status.GetTerm() != c.Term || status.GetVote() != 0 || status.GetCommit() != 0 || status.Applied != 0 ||
			len(rd.Entries) != 0 || len(rd.CommittedEntries) != 0 || len(rd.ReadStates) != 0 || !raft.IsEmptySnap(rd.Snapshot) || rd.GetVote() != 0 || rd.GetCommit() != 0 {
			return r.stopLocked(ErrInvalid)
		}
		if len(rd.Messages) != 1 {
			return r.stopLocked(ErrInvalid)
		}
		response := rd.Messages[0]
		if response == nil || response.GetType() != pb.MsgAppResp || !response.GetReject() || response.GetFrom() != c.ID || response.GetTo() != c.Source || response.GetTerm() != c.Term || response.GetIndex() != m.GetIndex() || response.GetRejectHint() != 0 || response.GetLogTerm() != 0 || len(response.GetEntries()) != 0 || response.GetSnapshot() != nil {
			return r.stopLocked(ErrInvalid)
		}
		if err := c.Store.PersistApplicationReceiving(rd); err != nil {
			return r.stopLocked(err)
		}
		// Marshal exactly the original actual response, never a synthetic hint/ACK.
		wire, err := proto.Marshal(response)
		if err != nil {
			return r.stopLocked(err)
		}
		packet, err := EncodeApplicationPacket(c.Binding, Packet{From: response.GetFrom(), To: response.GetTo(), Payload: wire}, c.MaxPacketBytes)
		if err != nil {
			return r.stopLocked(err)
		}
		if len(packet.Payload) > c.MaxOutputBytes-used {
			return r.stopLocked(ErrLimit)
		}
		used += len(packet.Payload)
		packets = append(packets, packet)
		r.raw.Advance(rd)
	}
	if len(packets) != 1 {
		return r.stopLocked(ErrInvalid)
	}
	return packets, nil
}

// Close stops and releases only receiving state. It never closes the borrowed
// Store or an embedding association/import needed by the future activation owner.
// Repeating Close is safe; a stopped cause remains available on later Step.
func (r *ApplicationReceiver) Close() error {
	if r == nil || r.self != r {
		return ErrInvalid
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	r.closed = true
	r.raw = nil
	r.config = ApplicationReceivingConfig{}
	return nil
}
