package raftlog

import (
	"crypto/sha256"
	"errors"

	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

// ApplicationReceivingState describes an empty, bound fixed-membership store.
// Its configured voters are placement information, not active ConfState. This
// owned value grants no retirement, quorum, snapshot or graph authority.
type ApplicationReceivingState struct {
	Binding          ApplicationBinding
	Voters           [3]uint64
	LocalVoter, Term uint64
}

// ApplicationReceivingState checks the empty active state and returns owned
// configuration and durable term. A valid inactive import may remain live.
// Success neither initializes membership nor authorizes replacement traffic.
func (s *Store) ApplicationReceivingState() (ApplicationReceivingState, error) {
	if s == nil {
		return ApplicationReceivingState{}, ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.applicationReceivingLocked()
}

// PersistApplicationReceiving stores only the term of an ACTUAL ordinary Ready
// from a trusted receiving RawNode with empty membership. The caller owns the
// Ready and keeps it immutable during this call. Vote, commit, entries and
// snapshot activation remain forbidden; the ordinary Persist gate is unchanged.
// A changed or explicitly supplied term is synced once before success. An absent
// HardState is a checked no-op. Write/sync/close errors are uncertain, poison the
// handle and never permit an acknowledgment, rollback inference or prefix retry.
// Retirement/quorum authorization belongs to the embedding's live admission.
func (s *Store) PersistApplicationReceiving(rd raft.Ready) error {
	if s == nil {
		return ErrInvalid
	}
	s.publicationMu.Lock()
	defer s.publicationMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.applicationReceivingLocked(); err != nil {
		return err
	}
	if len(rd.Entries) != 0 || len(rd.CommittedEntries) != 0 || !receivingSnapshotEmpty(rd.Snapshot) ||
		len(rd.HardState.ProtoReflect().GetUnknown()) != 0 || rd.GetVote() != 0 || rd.GetCommit() != 0 {
		return ErrInvalid
	}
	if rd.HardState == nil || rd.Term == nil && rd.Vote == nil && rd.Commit == nil {
		return nil
	}
	if rd.GetTerm() < s.meta.Hard.GetTerm() {
		return ErrInvalid
	}
	m := s.meta
	// Own only this bounded scalar header, without borrowing caller pointers or
	// changing any inactive bank, recovery reservation, claim or import cursor.
	m.Hard = &pb.HardState{Term: new(rd.GetTerm())}
	if err := s.validate(m); err != nil {
		return err
	}
	if err := checkMetadataLimit(m, s.limits); err != nil {
		return err
	}
	return s.commit(m, nil)
}

func receivingConfEmpty(c *pb.ConfState) bool {
	return c == nil || len(c.ProtoReflect().GetUnknown()) == 0 && len(c.GetVoters()) == 0 &&
		len(c.GetVotersOutgoing()) == 0 && len(c.GetLearners()) == 0 && len(c.GetLearnersNext()) == 0 && !c.GetAutoLeave()
}

func receivingSnapshotEmpty(s *pb.Snapshot) bool {
	if s == nil {
		return true
	}
	if len(s.ProtoReflect().GetUnknown()) != 0 || len(s.GetData()) != 0 {
		return false
	}
	m := s.GetMetadata()
	return m == nil || len(m.ProtoReflect().GetUnknown()) == 0 && m.GetIndex() == 0 && m.GetTerm() == 0 && receivingConfEmpty(m.GetConfState())
}

// Caller holds Store.mu. Metadata eligibility precedes constant-count first-key
// probes and two zero-image point reads. No inactive namespace is traversed.
func (s *Store) applicationReceivingLocked() (ApplicationReceivingState, error) {
	var zero ApplicationReceivingState
	if err := s.check(); err != nil {
		return zero, err
	}
	if err := s.applicationProgressPublicationLocked(); err != nil {
		return zero, err
	}
	m := s.meta
	binding := s.applicationBindingLocked()
	if binding.Validate() != nil || !m.App.Policy.Enabled() || !m.Rep.Config.enabled() || m.Hard == nil || m.Conf == nil || m.Snap == nil {
		return zero, ErrInvalid
	}
	if m.Base != 0 || m.BaseTerm != 0 || m.Last != 0 || m.Applied != 0 || m.Hard.GetVote() != 0 || m.Hard.GetCommit() != 0 ||
		m.LogBytes != 0 || m.LogCount != 0 || m.BaseHash != [32]byte{} || m.LastHash != [32]byte{} ||
		m.ImageBytes != 0 || m.SnapBytes != 0 || m.ImageHash != sha256.Sum256(nil) || m.SnapHash != sha256.Sum256(nil) ||
		!receivingConfEmpty(m.Conf) || !receivingSnapshotEmpty(m.Snap) ||
		m.App.Bytes != 0 || m.App.Records != 0 || m.App.Through != 0 || m.App.TailBytes != 0 ||
		m.Controls.Bytes != 0 || m.Controls.Records != 0 || m.Recovery.Bytes != 0 || m.Recovery.Records != 0 ||
		m.Recovery.Reservation.ID != [32]byte{} || m.Recovery.Reservation.Digest != [32]byte{} || len(m.Recovery.Reservation.Credits) != 0 || len(m.Recovery.Queued) != 0 ||
		m.Rep.LastActivatedManifestID != [32]byte{} {
		return zero, ErrInvalid
	}
	p := m.Gen.Publication
	if p.Valid || p.Bank != 0 || p.Generation != 0 || p.Bytes != 0 || p.Records != 0 || p.ID != [32]byte{} ||
		p.ControlBytes != 0 || p.ControlRecords != 0 || p.RecoveryBytes != 0 || p.RecoveryRecords != 0 || p.RecoveryHash != [32]byte{} {
		return zero, ErrInvalid
	}
	// Full validation preserves valid inactive staging/reservations and checks
	// configured fixed-three membership, local voter and generation invariants.
	if err := s.validate(m); err != nil {
		return zero, err
	}
	fail := func(err error) (ApplicationReceivingState, error) { s.poison = err; return zero, err }
	if err := s.checkEntryBounds(); err != nil {
		return fail(err)
	}
	// Include disabled control/recovery ranges too: orphan active keys must not
	// become receiving state simply because their ledger/config is empty.
	it, err := newApplicationIterator(s.db.NewIter, s.activeBank(), true, true)
	if err != nil {
		return fail(err)
	}
	present := it.First()
	err = it.Error()
	if present {
		err = errors.Join(err, ErrCorrupt)
	}
	err = errors.Join(err, it.Close())
	if err != nil {
		return fail(err)
	}
	if _, err := s.loadImage(imageKey, 0, m.ImageHash); err != nil {
		return fail(err)
	}
	if _, err := s.loadImage(snapshotKey, 0, m.SnapHash); err != nil {
		return fail(err)
	}
	return ApplicationReceivingState{Binding: binding, Voters: m.Rep.Config.Voters, LocalVoter: m.App.Policy.LocalVoter, Term: m.Hard.GetTerm()}, nil
}
