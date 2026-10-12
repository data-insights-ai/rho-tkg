package raftlog

import (
	"context"
	"errors"

	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// ApplicationReadyProgress is one trusted bound Driver's actual ordinary Ready.
// It borrows the immutable admitted input until all entries are durable; actual RawNode
// applied bookkeeping must then catch the physical application checkpoint. It grants
// no graph/cut authority. It blocks publication/compaction while alive; uncertain
// failures require closing/reopening the Store rather than releasing its fence.
type ApplicationReadyProgress struct {
	self                              *ApplicationReadyProgress
	s                                 *Store
	rd                                raft.Ready
	hard                              *pb.HardState
	committed, rawApplied             uint64
	offset, entries                   int
	tried, closed, busy, persistedAll bool
}

// BeginApplicationReadyProgress validates the COMPLETE actual Ready before
// partial persistence. The caller must keep the input immutable until
// PersistFull succeeds or AppliedLimit reports done; no entries/data are copied
// or retained beyond that point. committed/rawApplied are captured from the
// same actual RawNode under its event lock, never from packet claims. Snapshot
// activation and scalar/unbound modes use their existing separate contracts.
func (s *Store) BeginApplicationReadyProgress(binding ApplicationBinding, rd raft.Ready, committed, rawApplied uint64) (*ApplicationReadyProgress, error) {
	if s == nil {
		return nil, ErrInvalid
	}
	s.publicationMu.Lock()
	defer s.publicationMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(); err != nil {
		return nil, err
	}
	if s.readyProgress != nil || binding.Validate() != nil || !s.meta.Rep.Config.enabled() || binding != s.applicationBindingLocked() || !raft.IsEmptySnap(rd.Snapshot) || rawApplied != s.meta.Applied || committed < rawApplied {
		return nil, ErrInvalid
	}
	hard := s.meta.Hard
	if !raft.IsEmptyHardState(rd.HardState) {
		hard = rd.HardState
	}
	if hard.GetCommit() != committed {
		return nil, ErrInvalid
	}
	for _, e := range rd.CommittedEntries {
		if e == nil || e.GetType() != pb.EntryNormal || e.GetIndex() > committed {
			return nil, ErrInvalid
		}
	}
	if _, err := s.prepareOrdinaryReadyLocked(rd, nil, false); err != nil {
		return nil, err
	}
	// RawNode guarantees the actual Ready is immutable until Advance. Borrow
	// only its entry/header input; never mutate it, and drop it once all entries
	// are durable. The small effective HardState below remains independently owned.
	owned := raft.Ready{HardState: rd.HardState, Entries: rd.Entries}
	p := &ApplicationReadyProgress{s: s, rd: owned, hard: proto.Clone(hard).(*pb.HardState), committed: committed, rawApplied: rawApplied, entries: len(rd.Entries)}
	p.self = p
	s.readyProgress = p
	return p, nil
}

// Caller holds Store.mu; this is exactly the existing immutable binding.
func (s *Store) applicationBindingLocked() ApplicationBinding {
	return ApplicationBinding{Identity: s.meta.Transfer.Identity, SemanticContractID: s.meta.Rep.SemanticContractID}
}
func (p *ApplicationReadyProgress) validLocked() error {
	if p == nil || p.self != p || p.s == nil {
		return ErrInvalid
	}
	if err := p.s.check(); err != nil {
		return err
	}
	if p.closed {
		return ErrClosed
	}
	if p.s.readyProgress != p {
		return ErrInvalid
	}
	return nil
}

// PersistFull preserves the one-sync complete-Ready fast path. false/nil means
// our own no-effect queue-allocation or prepared-metadata quota refusal
// permits bounded progress. A separately captured exact owner-retirement
// dependency also permits progress through its actual committed predecessor;
// a write/sync/close failure is returned and must NEVER be retried as a prefix.
func (p *ApplicationReadyProgress) PersistFull() (bool, error) {
	if p == nil || p.s == nil {
		return false, ErrInvalid
	}
	s := p.s
	s.mu.Lock()
	err := p.validLocked()
	if err == nil && (p.tried || p.offset != 0 || p.busy) {
		err = ErrInvalid
	}
	if err == nil {
		p.tried = true
		p.busy = true
	}
	s.mu.Unlock()
	if err != nil {
		return false, err
	}
	defer p.finishOperation()
	r, ownRefusal, err := s.classifyRecoveryReadyOutcome(p.rd.Entries)
	s.mu.Lock()
	defer s.mu.Unlock()
	if e := p.validLocked(); e != nil {
		return false, errors.Join(err, e)
	}
	if err != nil {
		if ownRefusal.matches(s, len(p.rd.Entries), p.committed) {
			return false, nil
		}
		// A verifier/read/view-close error is terminal and retains every cause.
		// Only a Store-owned allocation/committed-retirement receipt or our
		// later pure prepared-metadata quota decision enables prefix work.
		return false, err
	}
	plan, err := s.prepareOrdinaryReadyLocked(p.rd, r, true)
	if err != nil {
		if s.poison == nil && errors.Is(err, ErrLimit) {
			return false, nil
		}
		return false, err
	}
	if err := s.writeOrdinaryReadyLocked(p.rd, plan); err != nil {
		return false, err
	}
	p.offset = p.entries
	p.persistedAll = true
	p.rd = raft.Ready{}
	return true, nil
}

// CommitPredecessor advances OLD durable commit only through the unchanged
// predecessor of an incoming replacement. It never commits old replaced bytes.
func (p *ApplicationReadyProgress) CommitPredecessor() (uint64, error) {
	if p == nil || p.s == nil {
		return 0, ErrInvalid
	}
	s := p.s
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := p.validLocked(); err != nil {
		return 0, err
	}
	if !p.tried || p.offset != 0 || p.busy {
		return 0, ErrInvalid
	}
	through := min(p.committed, s.meta.Last)
	if len(p.rd.Entries) > 0 {
		through = min(through, p.rd.Entries[0].GetIndex()-1)
	}
	hard := proto.Clone(p.hard).(*pb.HardState)
	hard.Commit = new(through)
	rd := raft.Ready{HardState: hard}
	plan, err := s.prepareOrdinaryReadyLocked(rd, nil, true)
	if err != nil {
		return 0, err
	}
	if err := s.writeOrdinaryReadyLocked(rd, plan); err != nil {
		return 0, err
	}
	return through, nil
}

// PersistPrefix selects the FIRST actually fitting remaining contiguous prefix.
// Candidate entries are classified once in forward order against the genuine
// current immutable application view. Protected costs need not be monotonic;
// resource-refused candidates may be followed by a fitting verified Use. No
// candidate is retried until the caller makes real committed state progress.
func (p *ApplicationReadyProgress) PersistPrefix() (last uint64, err error) {
	if p == nil || p.s == nil {
		return 0, ErrInvalid
	}
	s := p.s
	s.mu.Lock()
	if err := p.validLocked(); err != nil {
		s.mu.Unlock()
		return 0, err
	}
	if !p.tried || p.persistedAll || p.offset >= p.entries || p.busy {
		s.mu.Unlock()
		return 0, ErrInvalid
	}
	p.busy = true
	defer p.finishOperation()
	remaining := p.rd.Entries[p.offset:]
	first := remaining[0].GetIndex()
	hard := proto.Clone(p.hard).(*pb.HardState)
	// The cursor validates terms against the original genuine HardState. Each
	// candidate receives a fresh capped commit without mutating that input.
	rd := raft.Ready{HardState: hard, Entries: remaining}
	captured := s.meta.Recovery
	verifier := s.recoveryVerifier
	r := &applicationRecoveryReady{Applied: s.meta.Applied, Last: s.meta.Last, Generation: s.activeGeneration(), ImageHash: s.meta.ImageHash, LastHash: s.meta.LastHash, HardTerm: s.meta.Hard.GetTerm(), HardVote: s.meta.Hard.GetVote(), HardCommit: s.meta.Hard.GetCommit()}
	maximum := (maxMetadataBytes - recoveryMetadataFixedBytes) / 256
	old := captured.Queued
	r.Queued = make([]applicationRecoveryQueued, 0, min(maximum, len(old)+len(remaining)))
	for _, q := range old {
		if q.Index < first {
			r.Queued = append(r.Queued, q)
		}
	}
	c, err := s.ordinaryReadyCursorLocked(rd, r)
	if err != nil {
		s.mu.Unlock()
		return 0, err
	}
	s.mu.Unlock()
	var view *ApplicationView
	if captured.Config.enabled() && verifier != nil {
		view, err = s.ApplicationView(r.Applied)
		if err != nil {
			return 0, err
		}
		if _, err = view.RootBounded(context.Background(), s.applicationPolicy.MaxImageBytes); err != nil {
			return 0, errors.Join(err, view.Close())
		}
		defer func() { err = errors.Join(err, view.Close()) }()
	}
	for i, e := range remaining {
		if view != nil {
			if err := s.classifyRecoveryEntries([]*pb.Entry{e}, view, verifier, captured, r); err != nil {
				return 0, err
			}
		}
		if err := c.append(e); err != nil {
			return 0, err
		}
		capped := proto.Clone(p.hard).(*pb.HardState)
		capped.Commit = new(min(p.committed, e.GetIndex()))
		c.plan.meta.Hard = capped
		s.mu.Lock()
		if err := p.validLocked(); err != nil {
			s.mu.Unlock()
			return 0, err
		}
		if !r.matches(s) {
			s.mu.Unlock()
			return 0, ErrInvalid
		}
		plan, capacityErr := s.ordinaryReadyCapacityLocked(&c, r, true)
		if capacityErr != nil {
			s.mu.Unlock()
			if errors.Is(capacityErr, ErrLimit) {
				continue
			}
			return 0, capacityErr
		}
		prefix := raft.Ready{HardState: capped, Entries: remaining[:i+1]}
		err = s.writeOrdinaryReadyLocked(prefix, plan)
		if err == nil {
			p.offset += i + 1
			p.persistedAll = p.offset == p.entries
			if p.persistedAll {
				p.rd = raft.Ready{}
			}
			last = e.GetIndex()
		}
		s.mu.Unlock()
		return last, err
	}
	return 0, ErrLimit
}

// AppliedLimit returns only the original actual consensus commit intersected
// with durable canonical log. Before replacement persistence, use only the
// unchanged predecessor returned by CommitPredecessor.
func (p *ApplicationReadyProgress) AppliedLimit() (uint64, bool, error) {
	if p == nil || p.s == nil {
		return 0, false, ErrInvalid
	}
	s := p.s
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := p.validLocked(); err != nil {
		return 0, false, err
	}
	return min(p.committed, s.meta.Hard.GetCommit(), s.meta.Last), p.persistedAll, nil
}

// AdvanceRaw records ACTUAL RawNode applied bookkeeping after Advance(original
// Ready). It neither applies entries nor invents a Raft continuation message.
func (p *ApplicationReadyProgress) AdvanceRaw(index uint64) error {
	if p == nil || p.s == nil {
		return ErrInvalid
	}
	s := p.s
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := p.validLocked(); err != nil {
		return err
	}
	if index < p.rawApplied || index > s.meta.Applied {
		return ErrInvalid
	}
	p.rawApplied = index
	return nil
}

// Close releases only completed persistence whose real RawNode applied fence
// equals the physical checkpoint. Failure retains the compaction fence until
// Store.Close/reopen; returning an error is never proof of rollback.
func (p *ApplicationReadyProgress) Close() error {
	if p == nil || p.s == nil {
		return ErrInvalid
	}
	s := p.s
	s.publicationMu.Lock()
	defer s.publicationMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := p.validLocked(); err != nil {
		return err
	}
	if !p.persistedAll || p.busy || p.rawApplied != s.meta.Applied {
		return ErrInvalid
	}
	p.closed = true
	s.readyProgress = nil
	// Borrowed entry input was already dropped after successful persistence.
	return nil
}

func (s *Store) applicationProgressPublicationLocked() error {
	if s.readyProgress != nil {
		return ErrInvalid
	}
	return nil
}

func (p *ApplicationReadyProgress) finishOperation() {
	p.s.mu.Lock()
	p.busy = false
	p.s.mu.Unlock()
}
