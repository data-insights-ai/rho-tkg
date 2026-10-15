package raftlog

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"

	pb "go.etcd.io/raft/v3/raftpb"
)

// Private no-effect receipt. A copied callback result or ordinary error cannot
// construct it. The terminal entry is already durable or earlier in this exact
// immutable Ready; progress still needs its actual RawNode committed index.
type applicationRecoveryDependency struct {
	self      *applicationRecoveryDependency
	s         *Store
	before    applicationRecoveryReady
	owner     ApplicationRecoveryOwner
	entry     uint64
	command   [32]byte
	inputHash [32]byte
	terminal  applicationRecoveryQueued
	input     []*pb.Entry
	refusal   error
}

func (d *applicationRecoveryDependency) Error() string { return ErrApplicationPrerequisite.Error() }
func (d *applicationRecoveryDependency) Unwrap() []error {
	return []error{ErrApplicationPrerequisite, d.refusal}
}

func terminalRecoveryUse(q applicationRecoveryQueued, reservation ApplicationRecoveryReservation) bool {
	version := uint32(1)
	if len(reservation.Spans) != 0 {
		version = 3
	}
	return terminalRecoveryUseFor(version, q, reservation)
}

func terminalRecoveryUseFor(version uint32, q applicationRecoveryQueued, reservation ApplicationRecoveryReservation) bool {
	use := q.Purpose.Use
	if q.Purpose.Reserve != nil || use == nil || use.ID != reservation.ID || use.Digest != reservation.Digest || use.CommandDigest != q.CommandDigest || use.Phase == 0 || use.RetireBefore != use.Phase || q.Credit.Phase != use.Phase {
		return false
	}
	for _, span := range reservation.Spans {
		if span.Last > use.Phase {
			return false
		}
	}
	for _, credit := range reservation.Credits {
		if credit.Phase > use.Phase {
			return false
		}
	}
	credit, err := (applicationRecoveryMetadata{Config: ApplicationRecoveryConfig{Version: version}, Reservation: reservation}).credit(*use)
	return err == nil && credit == q.Credit
}

func (s *Store) captureRecoveryDependency(entry *pb.Entry, input []*pb.Entry, captured applicationRecoveryMetadata, before *applicationRecoveryReady, need ApplicationRecoveryApplyBefore) (*applicationRecoveryDependency, error) {
	owner := ApplicationRecoveryOwner{ID: captured.Reservation.ID, Digest: captured.Reservation.Digest}
	if entry == nil || before == nil || need.Owner.ID == ([32]byte{}) || need.Owner != owner {
		return nil, ErrInvalid
	}
	for i := len(before.Queued) - 1; i >= 0; i-- {
		terminal := before.Queued[i]
		if terminal.Index <= before.Applied || terminal.Index >= entry.GetIndex() || !terminalRecoveryUseFor(captured.Config.Version, terminal, captured.Reservation) {
			continue
		}
		found := false
		for _, candidate := range input {
			if candidate != nil && candidate.GetIndex() == terminal.Index {
				found = candidate.GetType() == pb.EntryNormal && sha256.Sum256(candidate.GetData()) == terminal.CommandDigest
				break
			}
		}
		if !found {
			if terminal.Index > before.Last {
				return nil, ErrInvalid
			}
			s.mu.Lock()
			stored, _, _, err := s.get(terminal.Index)
			s.mu.Unlock()
			if err != nil {
				return nil, err
			}
			found = stored.GetType() == pb.EntryNormal && sha256.Sum256(stored.GetData()) == terminal.CommandDigest
		}
		if !found {
			return nil, ErrInvalid
		}
		d := &applicationRecoveryDependency{s: s, before: *before, owner: owner, entry: entry.GetIndex(), terminal: terminal, input: input, command: sha256.Sum256(entry.GetData()), inputHash: recoveryDependencyInputHash(input), refusal: need.Refusal}
		d.self = d
		return d, d
	}
	return nil, errors.Join(ErrApplicationPrerequisite, need.Refusal)
}

// Caller holds Store.mu. Revalidate every captured frontier and the exact
// terminal recipe; a stale/copied receipt or uncommitted retirement cannot
// grant prefix progress. New classification after actual installation is required.
func (d *applicationRecoveryDependency) matches(s *Store, committed uint64) bool {
	if d == nil || d.self != d || d.s != s || !d.before.matches(s) || d.inputHash != recoveryDependencyInputHash(d.input) || d.owner.ID == ([32]byte{}) || d.owner != (ApplicationRecoveryOwner{ID: s.meta.Recovery.Reservation.ID, Digest: s.meta.Recovery.Reservation.Digest}) || d.terminal.Index <= s.meta.Applied || d.terminal.Index >= d.entry || d.terminal.Index > committed || !terminalRecoveryUseFor(s.meta.Recovery.Config.Version, d.terminal, s.meta.Recovery.Reservation) {
		return false
	}
	deferrer := false
	for _, entry := range d.input {
		if entry != nil && entry.GetIndex() == d.entry {
			deferrer = entry.GetType() == pb.EntryNormal && sha256.Sum256(entry.GetData()) == d.command
		}
	}
	if !deferrer {
		return false
	}
	for _, entry := range d.input {
		if entry != nil && entry.GetIndex() == d.terminal.Index {
			return entry.GetType() == pb.EntryNormal && sha256.Sum256(entry.GetData()) == d.terminal.CommandDigest
		}
	}
	for _, queued := range s.meta.Recovery.Queued {
		if queued.Index == d.terminal.Index {
			return queued.CommandDigest == d.terminal.CommandDigest && terminalRecoveryUseFor(s.meta.Recovery.Config.Version, queued, s.meta.Recovery.Reservation)
		}
	}
	return false
}

// Hash exact bounded ordinary Ready entry headers and command bytes in order.
func recoveryDependencyInputHash(entries []*pb.Entry) [32]byte {
	h := sha256.New()
	var header [32]byte
	binary.BigEndian.PutUint64(header[:8], uint64(len(entries)))
	_, _ = h.Write(header[:8])
	for _, entry := range entries {
		if entry == nil {
			_, _ = h.Write([]byte{0})
			continue
		}
		_, _ = h.Write([]byte{1})
		binary.BigEndian.PutUint64(header[:8], entry.GetIndex())
		binary.BigEndian.PutUint64(header[8:16], entry.GetTerm())
		binary.BigEndian.PutUint64(header[16:24], uint64(entry.GetType())) // #nosec G115 -- enum hashed only as its deterministic modulo-2^64 bit pattern, never a size/index; ordinary Ready validation separately rejects unsupported types.
		binary.BigEndian.PutUint64(header[24:32], uint64(len(entry.GetData())))
		_, _ = h.Write(header[:])
		_, _ = h.Write(entry.GetData())
	}
	var digest [32]byte
	h.Sum(digest[:0])
	return digest
}
