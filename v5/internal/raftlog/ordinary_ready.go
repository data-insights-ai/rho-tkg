package raftlog

import (
	"errors"
	"math"

	"github.com/cockroachdb/pebble/v2"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// This private prospective metadata is never published. LastHash is filled only
// by the original entry encoder in writeOrdinaryReadyLocked before final verify.
type ordinaryReadyPlan struct {
	meta        metadata
	predecessor [32]byte
	first       uint64
}

// Every ordinary Ready uses this one complete structure/ledger validator before
// any mutation. The cursor also lets progress scan candidate prefixes once.
type ordinaryReadyCursor struct {
	plan         ordinaryReadyPlan
	limits       Limits
	application  bool
	previousTerm uint64
	next         uint64
	wire         uint64
}

func (s *Store) ordinaryReadyCursorLocked(rd raft.Ready, r *applicationRecoveryReady) (ordinaryReadyCursor, error) {
	var c ordinaryReadyCursor
	if err := s.check(); err != nil {
		return c, err
	}
	if !raft.IsEmptySnap(rd.Snapshot) || r != nil && !r.matches(s) {
		return c, ErrInvalid
	}
	if len(rd.HardState.ProtoReflect().GetUnknown()) != 0 || len(rd.Snapshot.ProtoReflect().GetUnknown()) != 0 || len(rd.Snapshot.GetMetadata().ProtoReflect().GetUnknown()) != 0 {
		return c, ErrInvalid
	}
	if len(rd.Entries) > 4096 {
		return c, ErrLimit
	}
	m := s.meta
	if r != nil {
		m.Recovery.Queued = r.Queued
	}
	c.limits, c.application = s.limits, m.App.Policy.Enabled()
	if c.application {
		if m.Applied == 0 && (!raft.IsEmptyHardState(rd.HardState) || len(rd.Entries) > 0) {
			return c, ErrInvalid
		}
		if !raft.IsEmptyHardState(rd.HardState) && !applicationVote(m, rd.GetVote()) {
			return c, ErrInvalid
		}
	}
	if !raft.IsEmptyHardState(rd.HardState) {
		if rd.GetTerm() < m.Hard.GetTerm() || rd.GetCommit() < m.Hard.GetCommit() || rd.GetTerm() == m.Hard.GetTerm() && m.Hard.GetVote() != 0 && rd.GetVote() != m.Hard.GetVote() {
			return c, ErrInvalid
		}
		m.Hard = rd.HardState
	}
	if err := checkMetadataLimit(m, s.limits); err != nil {
		return c, err
	}
	c.previousTerm = m.BaseTerm
	c.plan.predecessor = m.BaseHash
	if len(rd.Entries) > 0 {
		first := rd.Entries[0]
		if first == nil {
			return c, ErrInvalid
		}
		c.plan.first = first.GetIndex()
		if c.plan.first <= m.Base || c.plan.first <= s.meta.Hard.GetCommit() || c.plan.first > m.Last+1 {
			return c, ErrInvalid
		}
		c.next = c.plan.first
		if c.plan.first > m.Base+1 {
			before, _, hash, err := s.get(c.plan.first - 1)
			if err != nil {
				s.poison = err
				return c, err
			}
			c.previousTerm, c.plan.predecessor = before.GetTerm(), hash
		}
		if c.plan.first <= m.Last {
			removed, err := s.rangeBytes(c.plan.first, m.Last+1)
			if err != nil {
				s.poison = err
				return c, err
			}
			if removed > m.LogBytes {
				s.poison = ErrCorrupt
				return c, ErrCorrupt
			}
			if c.application && removed > m.App.TailBytes {
				return c, ErrCorrupt
			}
			m.LogBytes -= removed
			if c.application {
				m.App.TailBytes -= removed
			}
		}
		m.Last = c.plan.first - 1
		m.LogCount = m.Last - m.Base
	}
	c.plan.meta = m
	return c, nil
}

func (c *ordinaryReadyCursor) append(e *pb.Entry) error {
	m := &c.plan.meta
	if e == nil || len(e.ProtoReflect().GetUnknown()) != 0 || e.GetIndex() != c.next || e.GetIndex() == math.MaxUint64 || e.GetTerm() == 0 || e.GetTerm() > m.Hard.GetTerm() || e.GetTerm() < c.previousTerm || e.GetType() < pb.EntryNormal || e.GetType() > pb.EntryConfChangeV2 || c.application && e.GetType() != pb.EntryNormal {
		return ErrInvalid
	}
	if len(e.GetData()) > c.limits.MaxEntryBytes-frameOverhead {
		return ErrLimit
	}
	size := uint64(len(e.GetData()) + frameOverhead + 9)
	c.wire += size + 32
	if c.wire > unsignedLimit(c.limits.MaxReadyBytes) {
		return ErrLimit
	}
	m.Last = e.GetIndex()
	m.LogCount = m.Last - m.Base
	m.LogBytes += size
	if c.application {
		m.App.TailBytes += size
	}
	c.previousTerm = e.GetTerm()
	c.next++
	return nil
}

func (s *Store) ordinaryReadyCapacityLocked(c *ordinaryReadyCursor, r *applicationRecoveryReady, checkCapacity bool) (ordinaryReadyPlan, error) {
	m := c.plan.meta
	if r != nil {
		m.Recovery.Queued = r.Queued
	}
	if m.Hard.GetCommit() > m.Last || m.Applied > m.Hard.GetCommit() {
		return ordinaryReadyPlan{}, ErrInvalid
	}
	if err := checkMetadataLimit(m, s.limits); err != nil {
		return ordinaryReadyPlan{}, err
	}
	if metadataBytes(m)+readyEnvelopeBytes+c.wire > unsignedLimit(s.limits.MaxReadyBytes) {
		return ordinaryReadyPlan{}, ErrLimit
	}
	syncActiveGeneration(&m)
	if checkCapacity {
		if m.LogBytes > s.limits.MaxRetainedBytes || m.LogCount > s.limits.MaxRetainedEntries {
			return ordinaryReadyPlan{}, ErrLimit
		}
		if err := s.validate(m); err != nil {
			return ordinaryReadyPlan{}, err
		}
	}
	result := c.plan
	result.meta = m
	return result, nil
}

// checkCapacity separates whole-input structural validation from capacity
// selection. An unchecked prospective plan is never passed to the writer.
func (s *Store) prepareOrdinaryReadyLocked(rd raft.Ready, r *applicationRecoveryReady, checkCapacity bool) (ordinaryReadyPlan, error) {
	c, err := s.ordinaryReadyCursorLocked(rd, r)
	if err != nil {
		return ordinaryReadyPlan{}, err
	}
	if raft.IsEmptyHardState(rd.HardState) && len(rd.Entries) == 0 {
		return c.plan, nil
	}
	if len(rd.Snapshot.GetData()) > s.limits.MaxSnapshotBytes || proto.Size(rd.HardState) > 64 || proto.Size(rd.Snapshot.GetMetadata()) > 32768 {
		return ordinaryReadyPlan{}, ErrLimit
	}
	for _, e := range rd.Entries {
		if err := c.append(e); err != nil {
			return ordinaryReadyPlan{}, err
		}
	}
	return s.ordinaryReadyCapacityLocked(&c, r, checkCapacity)
}

// Caller holds Store.mu and passes a capacity-validated plan from the same
// captured Store state. A failed sync/close poisons the handle; no retry grants
// abort authority. The on-disk entry/hash/metadata format is unchanged.
func (s *Store) writeOrdinaryReadyLocked(rd raft.Ready, p ordinaryReadyPlan) (err error) {
	if raft.IsEmptyHardState(rd.HardState) && len(rd.Entries) == 0 {
		return nil
	}
	m := p.meta
	m.Hard = proto.Clone(m.Hard).(*pb.HardState)
	batch := s.db.NewBatch()
	defer func() {
		if e := batch.Close(); e != nil {
			s.poison = e
			err = errors.Join(err, ErrPoisoned, e)
		}
	}()
	if p.first != 0 && p.first <= s.meta.Last {
		if err := batch.DeleteRange(entryKey(p.first), entryEnd, nil); err != nil {
			return err
		}
	}
	prev := p.predecessor
	for _, e := range rd.Entries {
		raw, hash := encodeEntry(e, prev)
		prev = hash
		if err := batch.Set(entryKey(e.GetIndex()), raw, nil); err != nil {
			return err
		}
	}
	if len(rd.Entries) > 0 {
		m.LastHash = prev
	}
	if err := s.validate(m); err != nil {
		return err
	}
	raw, err := encodeMeta(m)
	if err != nil {
		return err
	}
	if err := batch.Set(metaKey, raw, nil); err != nil {
		return err
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		s.poison = err
		return errors.Join(ErrPoisoned, err)
	}
	s.meta = m
	return nil
}
