package replica

import (
	"errors"
	"slices"

	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// One owner spans RawNode work and outside-mu cleanup. Its identity is private;
// getters and token Close cannot release a different event's fence. The fixed
// sender base reservation covers one event, one close owner and channel/control
// representation. Original backend errors are borrowed diagnostic causes.
type applicationEvent struct {
	done     chan struct{}
	claim    *raftlog.ApplicationSnapshotClaim
	rawPhase bool
}
type applicationClose struct {
	done   chan struct{}
	result error
}

func (d *Driver) boundApplication() bool  { return d.binding != (raftlog.ApplicationBinding{}) }
func (d *Driver) fencedApplication() bool { return d.boundApplication() || d.snapshotSender != nil }

// Called with mu held. A competing event refuses before any RawNode accessor.
func (d *Driver) admissionNoRaw() error {
	if d.event != nil || d.closeOwner != nil {
		if d.stopped != nil {
			return errors.Join(ErrStopped, d.stopped)
		}
		return ErrUnavailable
	}
	if d.stopped != nil {
		return errors.Join(ErrStopped, d.stopped)
	}
	if d.snapshotSender != nil {
		if d.snapshotSender.fatal != nil {
			_, err := d.stop(d.snapshotSender.fatal)
			return errors.Join(ErrStopped, err)
		}
		if d.snapshotSender.busy {
			return ErrUnavailable
		}
	}
	return nil
}
func (d *Driver) beginEvent() (*applicationEvent, error) {
	if err := d.admissionNoRaw(); err != nil {
		return nil, err
	}
	if err := d.check(); err != nil {
		return nil, err
	}
	return d.installEvent(), nil
}
func (d *Driver) beginPacketEvent() (*applicationEvent, error) {
	if err := d.admissionNoRaw(); err != nil {
		return nil, err
	}
	return d.installEvent(), nil
}
func (d *Driver) installEvent() *applicationEvent {
	if !d.fencedApplication() {
		return nil
	}
	e := &applicationEvent{done: make(chan struct{}), rawPhase: true}
	d.event = e
	return e
}
func (d *Driver) beginCleanup() *applicationEvent {
	e := &applicationEvent{done: make(chan struct{})}
	d.event = e
	return e
}

// finishEvent owns mu on entry and releases it on return. This exact owner
// stays installed during Store calls, so later events cannot access RawNode.
func (d *Driver) finishEvent(e *applicationEvent, out Output, opErr error) (Output, error) {
	if e == nil {
		d.unlock()
		return out, opErr
	}
	if d.event != e {
		d.mu.Unlock()
		return Output{}, ErrInvalid
	}
	e.rawPhase = false
	claim := e.claim
	e.claim = nil
	d.mu.Unlock()
	var claimErr error
	if claim != nil {
		claimErr = claim.Close()
	}
	d.mu.Lock()
	if claimErr != nil {
		// Claim.Close repeats lose this result; retain the ORIGINAL exactly once.
		d.claimCleanupErr = errors.Join(d.claimCleanupErr, claimErr)
		d.stopped = errors.Join(d.stopped, claimErr)
		d.revokeSnapshotSends()
	}
	for {
		var pending [16]*SnapshotSend
		count := 0
		if d.snapshotSender != nil {
			pending = d.snapshotSender.cleanup
			count = d.snapshotSender.cleanupN
			clear(d.snapshotSender.cleanup[:])
			d.snapshotSender.cleanupN = 0
		}
		d.mu.Unlock()
		for _, s := range pending[:count] {
			err := s.export.Close()
			d.mu.Lock()
			d.completeSnapshotCleanup(s, err)
			d.mu.Unlock()
		}
		d.mu.Lock()
		if d.snapshotSender == nil || d.snapshotSender.cleanupN == 0 {
			break
		}
	}
	resultErr := errors.Join(opErr, claimErr)
	if d.stopped != nil {
		resultErr = errors.Join(resultErr, ErrStopped, d.stopped)
	}
	if d.snapshotSender != nil && d.snapshotSender.cleanupErr != nil {
		resultErr = errors.Join(resultErr, d.snapshotSender.cleanupErr)
	}
	if resultErr != nil {
		out = Output{}
	}
	d.event = nil
	close(e.done)
	d.mu.Unlock()
	return out, resultErr
}

func (d *Driver) completeSnapshotCleanup(s *SnapshotSend, err error) {
	s.export = nil
	s.manifest = raftlog.ApplicationSnapshotManifest{}
	s.snapshot = nil
	s.descriptor = nil
	d.snapshotSender.ownedBytes -= s.ownedBytes
	s.ownedBytes = 0
	d.snapshotSender.offers[s.slot] = nil
	s.closeErr = err
	close(s.done)
	if err != nil {
		d.snapshotSender.cleanupErr = errors.Join(d.snapshotSender.cleanupErr, err)
		d.snapshotSender.captureFailure(err)
		d.stopped = errors.Join(d.stopped, err)
		d.revokeSnapshotSends()
	}
}

func (d *Driver) closeApplication() error {
	d.mu.Lock()
	if old := d.closeOwner; old != nil {
		d.mu.Unlock()
		<-old.done
		return old.result
	}
	c := &applicationClose{done: make(chan struct{})}
	d.closeOwner = c
	if d.stopped == nil {
		d.stopped = ErrStopped
	} else if !errors.Is(d.stopped, ErrStopped) {
		d.stopped = errors.Join(ErrStopped, d.stopped)
	}
	d.revokeSnapshotSends()
	event := d.event
	if event == nil {
		event = d.beginCleanup()
		_, _ = d.finishEvent(event, Output{}, nil)
	} else {
		d.mu.Unlock()
		<-event.done
	}
	// No event can start after closeOwner is installed. Claim cleanup is already
	// complete before Store.Close can invalidate any prepared/consumed owner.
	storeErr := d.store.Close()
	d.mu.Lock()
	var senderErr error
	if d.snapshotSender != nil {
		senderErr = d.snapshotSender.cleanupErr
	}
	c.result = errors.Join(d.claimCleanupErr, senderErr, storeErr)
	if c.result != nil {
		d.stopped = errors.Join(d.stopped, c.result)
	}
	close(c.done)
	d.mu.Unlock()
	return c.result
}

func (d *Driver) decodeBoundPacket(p Packet, wantSnapshot bool) (*pb.Message, error) {
	if !d.boundApplication() || p.To != d.config.ID || p.From == d.config.ID || !slices.Contains(d.conf.GetVoters(), p.From) {
		return nil, ErrInvalid
	}
	decoded, err := DecodeApplicationPacket(d.binding, p, d.config.MaxPacketBytes)
	if err != nil {
		return nil, err
	}
	l := d.config.Limits
	l.MaxSnapshotBytes = 512
	if err := preflightWire(decoded.Payload, 0, l); err != nil {
		return nil, err
	}
	// Actual protobuf From/To and entry kinds are checked by a borrowed bounded
	// schema pass too; malformed/mismatched peers cannot reach proto allocation.
	if err := preflightApplicationMessage(decoded.Payload, p, wantSnapshot); err != nil {
		return nil, err
	}
	m := new(pb.Message)
	if err := (proto.UnmarshalOptions{RecursionLimit: 8}).Unmarshal(decoded.Payload, m); err != nil {
		return nil, errors.Join(ErrInvalid, err)
	}
	return m, nil
}
func (d *Driver) validateBoundMessage(m *pb.Message, wantSnapshot bool) error {
	if err := d.check(); err != nil {
		return err
	}
	status := d.raw.BasicStatus()
	if err := validateIncoming(m, d.conf, d.applied, status.RaftState == raft.StateLeader, d.config.Limits); err != nil {
		return err
	}
	if m.GetType() == pb.MsgTimeoutNow && max(status.GetTerm(), m.GetTerm()) >= replicaTermCeiling {
		return ErrLimit
	}
	if m.GetType() == pb.MsgHeartbeat {
		last, err := d.store.LastIndex()
		if err != nil {
			_, err = d.stop(err)
			return err
		}
		if m.GetCommit() > last {
			return ErrInvalid
		}
	}
	if wantSnapshot && m.GetSnapshot().GetMetadata().GetIndex() > status.GetCommit() {
		term, err := d.store.Term(status.GetCommit())
		if err != nil {
			_, err = d.stop(err)
			return err
		}
		if m.GetSnapshot().GetMetadata().GetTerm() < term {
			return ErrInvalid
		}
	}
	if m.GetType() == pb.MsgProp {
		if len(m.GetEntries()) != 1 {
			return ErrInvalid
		}
		if err := d.store.ReclaimApplication(); err != nil {
			_, err = d.stop(err)
			return err
		}
		if err := d.store.AdmitApplication(len(m.Entries[0].Data)); err != nil {
			return err
		}
	}
	return nil
}

// StepApplicationSnapshot requires a verified capability from this exact Store
// before RawNode can accept input. Only its actual nonempty Ready activates;
// ignored/stale/term-match fast-forward releases the unused claim to Prepared.
// Errors after commit/sync do not prove abort; no Output escapes failed cleanup.
func (d *Driver) StepApplicationSnapshot(p Packet, prepared *raftlog.PreparedApplicationSnapshot) (out Output, err error) {
	if d == nil {
		return Output{}, ErrInvalid
	}
	d.mu.Lock()
	owner, err := d.beginPacketEvent()
	if err != nil {
		return d.failEventAdmission(err)
	}
	defer func() { out, err = d.finishEvent(owner, out, err) }()
	if !d.boundApplication() {
		return Output{}, ErrInvalid
	}
	m, err := d.decodeBoundPacket(p, true)
	if err != nil {
		return Output{}, err
	}
	claim, err := d.store.ClaimPreparedApplicationSnapshot(prepared, m.GetSnapshot())
	if err != nil {
		return Output{}, err
	}
	owner.claim = claim
	if err = d.validateBoundMessage(m, true); err != nil {
		return Output{}, err
	}
	if err = d.raw.Step(m); err != nil {
		return Output{}, err
	}
	return d.drain()
}

func (d *Driver) failEventAdmission(err error) (Output, error) {
	if d.event == nil && d.snapshotSender != nil && d.snapshotSender.cleanupN > 0 {
		return d.finishEvent(d.beginCleanup(), Output{}, err)
	}
	d.mu.Unlock()
	return Output{}, err
}
