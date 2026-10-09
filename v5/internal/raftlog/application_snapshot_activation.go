package raftlog

import (
	"crypto/sha256"
	"errors"
	"math"

	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// PersistApplicationReady is the explicit trusted Raft-Ready/token activation
// door. The caller must pass the ACTUAL Ready after RawNode accepted a snapshot,
// not its input MsgSnap. Empty/ignored/fast-forward snapshots refuse activation.
// One sync co-commits consensus state, bank, complete checkpoint IMAGE, ledgers
// and published cut. Returning errors are not proof of abort after uncertain IO.
// Restore/acknowledgments may occur only after success. Driver wiring is separate.
func (s *Store) PersistApplicationReady(rd raft.Ready, c *ApplicationSnapshotClaim) (root ApplicationRoot, err error) {
	if s == nil || c == nil || c.p == nil || c.p.i == nil || c.p.s != s {
		return root, ErrInvalid
	}
	s.publicationMu.Lock()
	defer s.publicationMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(); err != nil {
		return root, err
	}
	if !s.meta.Rep.Config.enabled() {
		return root, ErrInvalid
	}
	if c.closed || c.consumed {
		return root, ErrClosed
	}
	if c.p.claim != c {
		return root, ErrInvalid
	}
	if err := c.p.live(); err != nil {
		return root, err
	}
	if raft.IsEmptySnap(rd.Snapshot) || !proto.Equal(rd.Snapshot, c.expected) {
		return root, ErrInvalid
	}
	if len(rd.Entries) > 4096 {
		return root, ErrLimit
	}
	if len(rd.HardState.ProtoReflect().GetUnknown()) != 0 {
		return root, ErrInvalid
	}
	for _, e := range rd.CommittedEntries {
		if e == nil || e.GetType() != pb.EntryNormal {
			return root, ErrInvalid
		}
	}
	idx, term := c.expected.GetMetadata().GetIndex(), c.expected.GetMetadata().GetTerm()
	old := s.meta
	if idx <= old.Base || idx <= old.Hard.GetCommit() || term == 0 {
		return root, ErrInvalid
	}
	committedTerm := old.BaseTerm
	if old.Hard.GetCommit() > old.Base {
		e, _, _, err := s.get(old.Hard.GetCommit())
		if err != nil {
			s.poison = err
			return root, err
		}
		committedTerm = e.GetTerm()
	}
	if term < committedTerm {
		return root, ErrInvalid
	}
	m := old
	if !raft.IsEmptyHardState(rd.HardState) {
		h := rd.HardState
		if h.GetTerm() < old.Hard.GetTerm() || h.GetCommit() < old.Hard.GetCommit() || h.GetTerm() == old.Hard.GetTerm() && old.Hard.GetVote() != 0 && h.GetVote() != old.Hard.GetVote() || !applicationVote(m, h.GetVote()) {
			return root, ErrInvalid
		}
		m.Hard = h
	}
	m.Base, m.BaseTerm, m.Last, m.Applied = idx, term, idx, idx
	m.BaseHash, m.LastHash = [32]byte{}, [32]byte{}
	m.LogBytes, m.LogCount, m.App.TailBytes = 0, 0, 0
	m.Snap = &pb.Snapshot{Metadata: c.expected.Metadata}
	m.Conf = c.expected.GetMetadata().GetConfState()
	m.ImageBytes = uint64(len(c.image))
	m.SnapBytes = m.ImageBytes
	m.ImageHash = sha256.Sum256(c.image)
	m.SnapHash = m.ImageHash
	m.Gen.Active = c.p.bank
	m.Gen.Banks[old.Gen.Active].State = bankRetired
	target := &m.Gen.Banks[c.p.bank]
	target.State = bankActive
	target.ReservedBytes, target.ReservedRecords = 0, 0
	m.App.Through, m.App.Bytes, m.App.Records = idx, target.Bytes, target.Records
	m.Rep.LastActivatedManifestID = c.manifestID
	if err := bindPublication(&m, c.p.bank, c.p.generation, m.App.Bytes, m.App.Records); err != nil {
		return root, err
	}
	if m.Gen.Publication.ID != c.p.i.manifest.CutID {
		return root, ErrInvalid
	}
	// With no old views/exports/capture, removal can share the activation batch.
	oldRef := s.generationRefs[old.Gen.Active]
	clearOld := oldRef == nil || oldRef.refs == 0
	if clearOld {
		m.Gen.Banks[old.Gen.Active] = applicationBank{Generation: old.Gen.Banks[old.Gen.Active].Generation, State: bankFree}
	}
	total := metadataBytes(m) + readyEnvelopeBytes + uint64(len(c.expected.Data))
	previousTerm := term
	last := idx
	for _, e := range rd.Entries {
		if e == nil || len(e.ProtoReflect().GetUnknown()) != 0 || e.GetType() != pb.EntryNormal || last == math.MaxUint64-1 || e.GetIndex() != last+1 || e.GetTerm() < previousTerm || e.GetTerm() == 0 || e.GetTerm() > m.Hard.GetTerm() {
			return root, ErrInvalid
		}
		if len(e.GetData()) > s.limits.MaxEntryBytes-frameOverhead {
			return root, ErrLimit
		}
		total += uint64(len(e.GetData()) + frameOverhead + 9 + 32)
		if total > unsignedLimit(s.limits.MaxReadyBytes) {
			return root, ErrLimit
		}
		last = e.GetIndex()
		previousTerm = e.GetTerm()
		m.LogBytes += uint64(len(e.GetData()) + frameOverhead + 9)
	}
	m.Last = last
	m.LogCount = last - idx
	m.App.TailBytes = m.LogBytes
	if total > unsignedLimit(s.limits.MaxReadyBytes) {
		return root, ErrLimit
	}
	syncActiveGeneration(&m)
	// LastHash is populated during encoding; preflight checks its empty-log shape
	// only after allocating the bounded batch. All resource checks precede copies.
	if err := validateReplicationMeta(m, s.limits); err != nil {
		return root, err
	}
	if err := s.validateGenerationMeta(m); err != nil {
		return root, err
	}
	if err := s.validateApplicationMeta(m); err != nil {
		return root, err
	}
	if m.LogBytes > s.limits.MaxRetainedBytes || m.LogCount > s.limits.MaxRetainedEntries {
		return root, ErrLimit
	}
	if m.Hard.GetCommit() < idx || m.Hard.GetCommit() > last || m.Hard.GetTerm() < term {
		return root, ErrInvalid
	}
	if err := checkMetadataLimit(m, s.limits); err != nil {
		return root, err
	}
	m.Hard = proto.Clone(m.Hard).(*pb.HardState)
	m.Snap = proto.Clone(m.Snap).(*pb.Snapshot)
	m.Conf = proto.Clone(m.Conf).(*pb.ConfState)
	batch := s.db.NewBatch()
	fail := func(e error) (ApplicationRoot, error) { return ApplicationRoot{}, errors.Join(e, batch.Close()) }
	if err := batch.DeleteRange(entryKey(0), entryEnd, nil); err != nil {
		return fail(err)
	}
	if clearOld {
		if err := batch.DeleteRange([]byte{bankTag(old.Gen.Active, appDataTag)}, []byte{bankTag(old.Gen.Active, appOutcomeTag) + 1}, nil); err != nil {
			return fail(err)
		}
	}
	for _, key := range [][]byte{imageKey, snapshotKey} {
		if err := batch.Set(key, c.image, nil); err != nil {
			return fail(err)
		}
	}
	prev := m.BaseHash
	for _, e := range rd.Entries {
		raw, hash := encodeEntry(e, prev)
		prev = hash
		if err := batch.Set(entryKey(e.GetIndex()), raw, nil); err != nil {
			return fail(err)
		}
	}
	m.LastHash = prev
	if err := s.validate(m); err != nil {
		return fail(err)
	}
	if err := batch.Delete(dormantKey, nil); err != nil {
		return fail(err)
	}
	if s.activationCommitHook != nil {
		s.activationCommitHook()
	}
	if err := s.commit(m, batch); err != nil {
		return root, err
	}
	if s.activationAfterSyncHook != nil {
		s.activationAfterSyncHook()
	}
	c.consumed = true
	s.releasePreparedOwnership(c.p)
	i := c.p.i
	i.closed = true
	i.prepared = nil
	i.manifest = ApplicationSnapshotManifest{}
	i.after = nil
	i.state.last = nil
	s.applicationImport = nil
	ref := i.ref
	i.ref = nil
	if err := s.releaseGeneration(ref); err != nil {
		return root, err
	}
	root = ApplicationRoot{Generation: c.p.generation, Index: idx, Image: copyApplicationBytes(c.image), ImageHash: m.ImageHash}
	// Keep the expected/image/ref until claim Close, so concurrent lifecycle calls
	// cannot revoke the in-flight owner after durable activation but before Restore.
	return root, nil
}
