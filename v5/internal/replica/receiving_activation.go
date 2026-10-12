package replica

import (
	"context"
	"errors"
	"slices"

	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// ApplicationReceivingActivation supplies selected privileged embedding hooks.
// Scalars/decoded snapshots grant no quorum/authentication authority. Check must
// verify the SAME live replacement admission/association; NewDriver supplies a
// real initialized semantic machine only AFTER durable snapshot activation.
type ApplicationReceivingActivation struct {
	Prepared     *raftlog.PreparedApplicationSnapshot
	Auditor      ApplicationSnapshotAuditor
	Budget       ApplicationAuditBudget
	MinimumIndex uint64
	Check        func() error
	NewDriver    func() (Config, error)
}

// ActivateApplicationSnapshot audits a genuine prepared snapshot, then feeds
// the original MsgSnap to this SAME receiving RawNode. Its actual Ready alone
// activates Store; Restore and SAME-Ready Advance precede unique RawNode transfer
// to the normal Driver. No callback runs under a Host or Store lock. The receiver
// mutex serializes its bounded operation; cancellation belongs to the embedding.
// Errors after RawStep/sync stop the receiver, withhold packets, and never imply
// rollback. The caller must recheck admission before Host/output publication.
func (r *ApplicationReceiver) ActivateApplicationSnapshot(ctx context.Context, packet Packet, a ApplicationReceivingActivation) (driver *Driver, out Output, err error) {
	if r == nil || r.self != r || ctx == nil || a.Prepared == nil || isNilMachine(a.Auditor) || a.Check == nil || a.NewDriver == nil || a.MinimumIndex == 0 || a.Budget.WorkBytes == 0 || a.Budget.OwnedBytes == 0 {
		return nil, Output{}, ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return nil, Output{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.checkLocked(); err != nil {
		return nil, Output{}, err
	}
	c := r.config
	if err := a.Check(); err != nil {
		return nil, Output{}, err
	}
	if packet.From != c.Source || packet.To != c.ID || !packet.Snapshot {
		return nil, Output{}, ErrInvalid
	}
	inner, err := DecodeApplicationPacket(c.Binding, packet, c.MaxPacketBytes)
	if err != nil {
		return nil, Output{}, err
	}
	l := c.Limits
	l.MaxSnapshotBytes = 512
	if err := preflightWire(inner.Payload, 0, l); err != nil {
		return nil, Output{}, err
	}
	if err := preflightApplicationMessage(inner.Payload, packet, true); err != nil {
		return nil, Output{}, err
	}
	m := new(pb.Message)
	if err := (proto.UnmarshalOptions{RecursionLimit: 8}).Unmarshal(inner.Payload, m); err != nil {
		return nil, Output{}, errors.Join(ErrInvalid, err)
	}
	md := m.GetSnapshot().GetMetadata()
	if m.GetType() != pb.MsgSnap || m.GetTerm() != c.Term || md == nil || md.GetIndex() < a.MinimumIndex || md.GetTerm() != c.Term || len(m.GetEntries()) != 0 {
		return nil, Output{}, ErrInvalid
	}
	if err := validateIncoming(m, nil, 0, false, c.Limits); err != nil {
		return nil, Output{}, err
	}
	state, err := c.Store.ApplicationReceivingState()
	if err != nil {
		return nil, Output{}, err
	}
	if state.Binding != c.Binding || state.LocalVoter != c.ID || state.Term > c.Term || !slices.Equal(md.GetConfState().GetVoters(), state.Voters[:]) || r.raw.BasicStatus().GetTerm() != state.Term || r.raw.HasReady() {
		return nil, Output{}, ErrInvalid
	}
	if err := c.Store.CheckPreparedApplicationSnapshot(a.Prepared, m.GetSnapshot()); err != nil {
		return nil, Output{}, err
	}
	checked, err := applicationSnapshotContext(m.GetSnapshot().GetData(), md)
	if err != nil {
		return nil, Output{}, err
	}
	audit, err := a.Auditor.BeginApplicationSnapshotAudit(a.Prepared, checked, a.Budget)
	if err != nil {
		if !isNilMachine(audit) {
			err = errors.Join(err, audit.Close())
		}
		return nil, Output{}, err
	}
	if isNilMachine(audit) {
		return nil, Output{}, ErrInvalid
	}
	closed := false
	defer func() {
		if !closed {
			err = errors.Join(err, audit.Close())
		}
		if err != nil {
			driver = nil
			out = Output{}
		}
	}()
	for {
		if err := ctx.Err(); err != nil {
			return nil, Output{}, err
		}
		done, err := audit.Advance(ctx, a.Budget)
		if err != nil {
			return nil, Output{}, err
		}
		if done {
			break
		}
	}
	if err := audit.Finish(ctx, a.Budget); err != nil {
		return nil, Output{}, err
	}
	// All audit readers/cleanup are released BEFORE Claim or RawStep.
	closeErr := audit.Close()
	closed = true
	if closeErr != nil {
		return nil, Output{}, closeErr
	}
	if err := errors.Join(ctx.Err(), a.Check()); err != nil {
		return nil, Output{}, err
	}
	claim, err := c.Store.ClaimPreparedApplicationSnapshot(a.Prepared, m.GetSnapshot())
	if err != nil {
		return nil, Output{}, err
	}
	defer func() {
		driver, out, err = r.finishActivationLocked(driver, out, err, claim.Close)
	}()
	if err := r.raw.Step(m); err != nil {
		_, err = r.stopLocked(err)
		return nil, Output{}, err
	}
	if !r.raw.HasReady() {
		_, err = r.stopLocked(ErrInvalid)
		return nil, Output{}, err
	}
	rd := r.raw.Ready()
	if raft.IsEmptySnap(rd.Snapshot) || len(rd.Entries) != 0 || len(rd.CommittedEntries) != 0 || len(rd.ReadStates) != 0 || rd.GetVote() != 0 || rd.GetCommit() != md.GetIndex() || rd.GetTerm() != c.Term {
		_, err = r.stopLocked(ErrInvalid)
		return nil, Output{}, err
	}
	if err := errors.Join(ctx.Err(), a.Check()); err != nil {
		_, err = r.stopLocked(err)
		return nil, Output{}, err
	}
	root, err := c.Store.PersistApplicationReady(rd, claim)
	if err != nil {
		_, err = r.stopLocked(err)
		return nil, Output{}, err
	}
	if root.Index != md.GetIndex() {
		_, err = r.stopLocked(ErrInvalid)
		return nil, Output{}, err
	}
	if err := errors.Join(ctx.Err(), a.Check()); err != nil {
		_, err = r.stopLocked(err)
		return nil, Output{}, err
	}
	cfg, err := a.NewDriver()
	if err != nil {
		_, err = r.stopLocked(err)
		return nil, Output{}, err
	}
	if cfg.Store != c.Store || cfg.ID != c.ID || isNilMachine(cfg.ApplicationMachine) || !isNilMachine(cfg.Machine) {
		_, err = r.stopLocked(ErrInvalid)
		return nil, Output{}, err
	}
	if err := ValidateApplicationMachineBinding(c.Binding, cfg.ApplicationMachine); err != nil {
		_, err = r.stopLocked(err)
		return nil, Output{}, err
	}
	if _, err := applicationCallbackProfile(cfg.ApplicationMachine); err != nil {
		_, err = r.stopLocked(err)
		return nil, Output{}, err
	}
	// Prepare normal callbacks/limits after activation, then transfer only the
	// same already-advanced RawNode. Its construction profile is preserved.
	cfg.ElectionTick, cfg.HeartbeatTick, cfg.MaxInflightMessages = 10, 1, 2
	cfg.MaxInflightBytes, cfg.MaxUncommittedBytes = max(uint64(8<<20), cReadBytes(c.Limits.MaxReadBytes)), 16<<20
	restoreErr := cfg.ApplicationMachine.Restore(root.Index, root.Image)
	if err := errors.Join(restoreErr, closeApplicationCleanup(takeApplicationCleanup(cfg.ApplicationMachine))); err != nil {
		_, err = r.stopLocked(err)
		return nil, Output{}, err
	}
	used := 0
	for _, response := range rd.Messages {
		if response == nil || response.GetType() != pb.MsgAppResp || response.GetReject() || response.GetFrom() != c.ID || response.GetTo() != c.Source || response.GetTerm() != c.Term || response.GetIndex() != root.Index {
			_, err = r.stopLocked(ErrInvalid)
			return nil, Output{}, err
		}
		wire, e := proto.Marshal(response)
		if e != nil {
			_, err = r.stopLocked(e)
			return nil, Output{}, err
		}
		p, e := EncodeApplicationPacket(c.Binding, Packet{From: c.ID, To: c.Source, Payload: wire}, c.MaxPacketBytes)
		if e != nil {
			_, err = r.stopLocked(e)
			return nil, Output{}, err
		}
		if len(p.Payload) > c.MaxOutputBytes-used {
			_, err = r.stopLocked(ErrLimit)
			return nil, Output{}, err
		}
		used += len(p.Payload)
		out.Packets = append(out.Packets, p)
	}
	if len(out.Packets) != 1 {
		_, err = r.stopLocked(ErrInvalid)
		return nil, Output{}, err
	}
	if err := errors.Join(ctx.Err(), a.Check()); err != nil {
		_, err = r.stopLocked(err)
		return nil, Output{}, err
	}
	r.raw.Advance(rd)
	driver, err = openDriver(cfg, r)
	if err != nil {
		_, err = r.stopLocked(err)
		return nil, Output{}, err
	}
	if err := errors.Join(ctx.Err(), a.Check()); err != nil {
		err = errors.Join(err, driver.Close())
		_, err = r.stopLocked(err)
		return nil, Output{}, err
	}
	out.Applied = root.Index
	r.closed = true
	r.raw = nil
	r.storage = nil
	r.config = ApplicationReceivingConfig{}
	return driver, out, nil
}

// Final claim cleanup can fail AFTER unique Driver adoption. Closing the
// adopted Driver here prevents losing its Store owner when output is withheld.
func (r *ApplicationReceiver) finishActivationLocked(driver *Driver, out Output, err error, closeClaim func() error) (*Driver, Output, error) {
	err = errors.Join(err, closeClaim())
	if err == nil {
		return driver, out, nil
	}
	if driver != nil {
		err = errors.Join(err, driver.Close())
	}
	_, err = r.stopLocked(err)
	return nil, Output{}, err
}
