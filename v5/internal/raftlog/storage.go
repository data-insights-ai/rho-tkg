package raftlog

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/confchange"
	pb "go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/tracker"
	"google.golang.org/protobuf/proto"
)

// Limits bound adapter buffers and logical retained log. Pebble's cache/memtable
// limits do not bound total RSS, OS cache, WAL or temporary compaction disk space.
// Reads additionally use at most one MaxEntryBytes lookahead/predecessor frame;
// snapshot publication uses one MaxSnapshotBytes image plus its batch copy.
// Recovery of existing WALs is controlled by Pebble, not MaxReadyBytes. Smaller
// limits reject incompatible metadata/frames; they do not migrate prior WALs or
// impose a process-memory ceiling before Pebble replays them.
type Limits struct {
	MaxEntryBytes, MaxReadBytes, MaxReadEntries, MaxReadyBytes, MaxSnapshotBytes int
	MaxRetainedBytes, MaxRetainedEntries                                         uint64
	CacheBytes                                                                   int64
	MemTableBytes                                                                uint64
}

// DefaultLimits supplies finite spike settings, not production acceptance targets.
func DefaultLimits() Limits {
	return Limits{MaxEntryBytes: 1 << 20, MaxReadBytes: 2 << 20, MaxReadEntries: 128, MaxReadyBytes: 4 << 20, MaxSnapshotBytes: 16 << 20, MaxRetainedBytes: 256 << 20, MaxRetainedEntries: 1_000_000, CacheBytes: 4 << 20, MemTableBytes: 1 << 20}
}

// Validate rejects missing, inconsistent and non-finite resource limits.
func (l Limits) Validate() error {
	if l.MaxEntryBytes < frameOverhead || l.MaxEntryBytes > 16<<20 || l.MaxReadBytes < l.MaxEntryBytes+32 || l.MaxReadBytes > 32<<20 || l.MaxReadEntries < 1 || l.MaxReadEntries > 4096 || l.MaxReadyBytes < l.MaxEntryBytes+512 || l.MaxReadyBytes > 64<<20 || l.MaxSnapshotBytes < 1 || l.MaxSnapshotBytes > 64<<20 || l.MaxRetainedBytes < uint64(l.MaxEntryBytes+9) || l.MaxRetainedBytes > 1<<40 || l.MaxRetainedEntries < 1 || l.MaxRetainedEntries > 10_000_000 || l.CacheBytes < 1 || l.CacheBytes > 1<<30 || l.MemTableBytes < 64<<10 || l.MemTableBytes > 64<<20 {
		return ErrInvalid
	}
	return nil
}

// Config selects explicit first-open-only creation; recovery never creates a missing DB.
type Config struct {
	Dir    string
	FS     vfs.FS
	Create bool
	Limits Limits
}

// Store implements durable raft.Storage with bounded reads and no per-entry RAM index.
// Fatal Pebble WAL/storage errors terminate the embedding process; this candidate
// fail-stop tradeoff requires evaluation before engine selection.
type Store struct {
	mu            sync.Mutex
	db            *pebble.DB
	limits        Limits
	meta          metadata
	poison        error
	canInitialize bool
	closed        bool
}

var _ raft.Storage = (*Store)(nil)
var metaKey = []byte{0}
var entryEnd = []byte{2}
var imageKey = []byte{3}
var snapshotKey = []byte{4}

// Open explicitly creates a fresh database or recovers an existing one.
func Open(c Config) (*Store, error) {
	if strings.TrimSpace(c.Dir) == "" {
		return nil, ErrInvalid
	}
	if c.Limits == (Limits{}) {
		c.Limits = DefaultLimits()
	}
	if err := c.Limits.Validate(); err != nil {
		return nil, err
	}
	if c.FS == nil {
		c.FS = vfs.Default
	}
	db, err := pebble.Open(c.Dir, &pebble.Options{FS: c.FS, ErrorIfExists: c.Create, ErrorIfNotExists: !c.Create, CacheSize: c.Limits.CacheBytes, MemTableSize: c.Limits.MemTableBytes, MemTableStopWritesThreshold: 2, MaxOpenFiles: 74, FormatMajorVersion: pebble.FormatMinSupported, MaxManifestFileSize: 1 << 20})
	if err != nil {
		return nil, err
	}
	s := &Store{db: db, limits: c.Limits, canInitialize: c.Create}
	fail := func(err error) (*Store, error) { return nil, errors.Join(err, db.Close()) }
	if c.Create {
		s.meta = metadata{Hard: &pb.HardState{}, Conf: &pb.ConfState{}, Snap: &pb.Snapshot{}, ImageHash: sha256.Sum256(nil), SnapHash: sha256.Sum256(nil)}
		b := db.NewBatch()
		_ = b.Set(imageKey, nil, nil)
		_ = b.Set(snapshotKey, nil, nil)
		if err := s.commit(s.meta, b); err != nil {
			return fail(err)
		}
	} else {
		value, closer, err := db.Get(metaKey)
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				return fail(errors.Join(ErrCorrupt, err))
			}
			return fail(err)
		}
		m, decodeErr := decodeMeta(value, c.Limits)
		closeErr := closer.Close()
		if err := errors.Join(decodeErr, closeErr); err != nil {
			return fail(err)
		}
		if err := s.validate(m); err != nil {
			return fail(fmt.Errorf("%w: metadata: %v", ErrCorrupt, err))
		}
		s.meta = m
		if err := s.checkEntryBounds(); err != nil {
			return fail(err)
		}
		// Check bounds and roots without decoding the whole retained log at open.
		if m.Last > m.Base {
			first, prev, _, err := s.get(m.Base + 1)
			if err != nil || prev != m.BaseHash || first.GetTerm() < m.BaseTerm || first.GetTerm() > m.Hard.GetTerm() {
				return fail(errors.Join(ErrCorrupt, err))
			}
			last, _, hash, err := s.get(m.Last)
			if err != nil || hash != m.LastHash || last.GetTerm() < first.GetTerm() || last.GetTerm() > m.Hard.GetTerm() {
				return fail(errors.Join(ErrCorrupt, err))
			}
		}
	}
	return s, nil
}

func validateConf(cs *pb.ConfState, last uint64) error {
	if cs == nil || len(cs.ProtoReflect().GetUnknown()) != 0 {
		return ErrInvalid
	}
	for _, ids := range [][]uint64{cs.Voters, cs.VotersOutgoing, cs.Learners, cs.LearnersNext} {
		if len(ids) > 256 {
			return ErrLimit
		}
		seen := make(map[uint64]bool, len(ids))
		for _, id := range ids {
			if id == 0 || raft.IsLocalMsgTarget(id) || seen[id] {
				return ErrInvalid
			}
			seen[id] = true
		}
	}
	t := tracker.MakeProgressTracker(1, 1)
	cfg, progress, err := confchange.Restore(confchange.Changer{Tracker: t, LastIndex: last}, cs)
	if err != nil {
		return errors.Join(ErrInvalid, err)
	}
	t.Config, t.Progress = cfg, progress
	if err := cs.Equivalent(t.ConfState()); err != nil {
		return errors.Join(ErrInvalid, err)
	}
	return nil
}

func (s *Store) validate(m metadata) error {
	if len(m.Hard.ProtoReflect().GetUnknown()) != 0 || len(m.Snap.ProtoReflect().GetUnknown()) != 0 || len(m.Snap.GetMetadata().ProtoReflect().GetUnknown()) != 0 {
		return ErrInvalid
	}
	if m.Base > m.Applied || m.Applied > m.Hard.GetCommit() || m.Hard.GetCommit() > m.Last || m.Last == math.MaxUint64 || m.Base != m.Snap.GetMetadata().GetIndex() || m.BaseTerm != m.Snap.GetMetadata().GetTerm() || m.LogCount != m.Last-m.Base || m.LogBytes > s.limits.MaxRetainedBytes || m.LogCount > s.limits.MaxRetainedEntries || m.ImageBytes > unsignedLimit(s.limits.MaxSnapshotBytes) || m.SnapBytes > unsignedLimit(s.limits.MaxSnapshotBytes) || len(m.Snap.GetData()) != 0 || m.BaseTerm > m.Hard.GetTerm() || (m.Hard.GetTerm() == 0 && (m.Hard.GetVote() != 0 || m.Last != 0)) || raft.IsLocalMsgTarget(m.Hard.GetVote()) {
		return ErrInvalid
	}
	if m.Base > 0 && (m.BaseTerm == 0 || len(m.Conf.GetVoters()) == 0 || m.Snap.GetMetadata().GetConfState() == nil) {
		return ErrInvalid
	}
	if m.Base == 0 && (m.BaseTerm != 0 || m.BaseHash != [32]byte{}) ||
		m.LogBytes < m.LogCount*(frameOverhead+9) ||
		m.LogBytes > m.LogCount*unsignedLimit(s.limits.MaxEntryBytes+9) ||
		m.LogCount == 0 && m.LastHash != m.BaseHash {
		return ErrInvalid
	}
	if err := validateConf(m.Conf, m.Last); err != nil {
		return err
	}
	if m.Base > 0 {
		return validateConf(m.Snap.GetMetadata().GetConfState(), m.Base)
	}
	return nil
}

func (s *Store) check() error {
	if s.closed {
		return ErrClosed
	}
	if s.poison != nil {
		return errors.Join(ErrPoisoned, s.poison)
	}
	return nil
}

func (s *Store) commit(m metadata, batch *pebble.Batch) (err error) {
	if batch == nil {
		batch = s.db.NewBatch()
	}
	defer func() {
		if closeErr := batch.Close(); closeErr != nil {
			s.poison = closeErr
			err = errors.Join(err, ErrPoisoned, closeErr)
		}
	}()
	b, err := encodeMeta(m)
	if err == nil {
		err = batch.Set(metaKey, b, nil)
	}
	if err == nil {
		err = batch.Commit(pebble.Sync)
	}
	if err != nil {
		s.poison = err
		return errors.Join(ErrPoisoned, err)
	}
	s.meta = m // Publication ONLY after synchronous commit returns successfully.
	return nil
}

// Initialize installs an agreed initial voter configuration and empty-machine
// image as a synthetic index 1 snapshot. It refuses any previously initialized state.
func (s *Store) Initialize(voters []uint64, image []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(); err != nil {
		return err
	}
	if !s.canInitialize || s.meta.Last != 0 || s.meta.Hard.GetTerm() != 0 || s.meta.Applied != 0 {
		return ErrInvalid
	}
	if len(image) > s.limits.MaxSnapshotBytes {
		return ErrLimit
	}
	if len(voters) > 256 {
		return ErrLimit
	}
	cs := &pb.ConfState{Voters: append([]uint64(nil), voters...)}
	if len(voters) == 0 {
		return ErrInvalid
	}
	if err := validateConf(cs, 1); err != nil {
		return err
	}
	m := metadata{Base: 1, BaseTerm: 1, Last: 1, Applied: 1, Hard: &pb.HardState{Term: new(uint64(1)), Commit: new(uint64(1))}, Conf: cs, ImageBytes: uint64(len(image)), SnapBytes: uint64(len(image)), ImageHash: sha256.Sum256(image), SnapHash: sha256.Sum256(image), Snap: &pb.Snapshot{Metadata: &pb.SnapshotMetadata{Index: new(uint64(1)), Term: new(uint64(1)), ConfState: proto.Clone(cs).(*pb.ConfState)}}}
	b := s.db.NewBatch()
	if err := b.Set(imageKey, image, nil); err != nil {
		_ = b.Close()
		return err
	}
	if err := b.Set(snapshotKey, image, nil); err != nil {
		_ = b.Close()
		return err
	}
	if err := s.commit(m, b); err != nil {
		return err
	}
	s.canInitialize = false
	return nil
}

// InitialState returns owned durable hard state and checkpoint membership.
func (s *Store) InitialState() (*pb.HardState, *pb.ConfState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(); err != nil {
		return nil, nil, err
	}
	return proto.Clone(s.meta.Hard).(*pb.HardState), proto.Clone(s.meta.Conf).(*pb.ConfState), nil
}

// FirstIndex returns the first retained log index after the snapshot boundary.
func (s *Store) FirstIndex() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(); err != nil {
		return 0, err
	}
	return s.meta.Base + 1, nil
}

// LastIndex returns the last durably persisted log index.
func (s *Store) LastIndex() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(); err != nil {
		return 0, err
	}
	return s.meta.Last, nil
}

// Snapshot loads an owned verified image and its membership metadata.
func (s *Store) Snapshot() (*pb.Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(); err != nil {
		return nil, err
	}
	snap := proto.Clone(s.meta.Snap).(*pb.Snapshot)
	image, err := s.loadImage(snapshotKey, s.meta.SnapBytes, s.meta.SnapHash)
	if err != nil {
		s.poison = err
		return nil, err
	}
	snap.Data = image
	return snap, nil
}

func (s *Store) get(index uint64) (*pb.Entry, [32]byte, [32]byte, error) {
	value, closer, err := s.db.Get(entryKey(index))
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, [32]byte{}, [32]byte{}, ErrCorrupt
	}
	if err != nil {
		return nil, [32]byte{}, [32]byte{}, err
	}
	e, prev, hash, err := decodeEntry(index, value, s.limits.MaxEntryBytes)
	return e, prev, hash, errors.Join(err, closer.Close())
}

// Term reads one log term or the retained compacted boundary term.
func (s *Store) Term(index uint64) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(); err != nil {
		return 0, err
	}
	if index < s.meta.Base {
		return 0, raft.ErrCompacted
	}
	if index > s.meta.Last {
		return 0, raft.ErrUnavailable
	}
	if index == s.meta.Base {
		return s.meta.BaseTerm, nil
	}
	// Borrow the Pebble value only until its closer is released. Frame
	// integrity still hashes the payload, but reading a term never clones it.
	value, closer, err := s.db.Get(entryKey(index))
	if errors.Is(err, pebble.ErrNotFound) {
		err = ErrCorrupt
	}
	if err != nil {
		s.poison = err
		return 0, err
	}
	term, _, _, inspectErr := inspectEntry(index, value, s.limits.MaxEntryBytes)
	if err := errors.Join(inspectErr, closer.Close()); err != nil {
		s.poison = err
		return 0, err
	}
	return term, nil

}

// Entries returns an owned consecutive bounded page, including at least its first entry.
func (s *Store) Entries(lo, hi, maxSize uint64) ([]*pb.Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.entries(lo, hi, maxSize)
}
func (s *Store) entries(lo, hi, maxSize uint64) (entries []*pb.Entry, err error) {
	if err := s.check(); err != nil {
		return nil, err
	}
	if lo <= s.meta.Base {
		return nil, raft.ErrCompacted
	}
	if hi < lo {
		return nil, ErrInvalid
	}
	if hi > s.meta.Last+1 {
		return nil, raft.ErrUnavailable
	}
	if lo == hi {
		return []*pb.Entry{}, nil
	}
	prev := s.meta.BaseHash
	previousTerm := s.meta.BaseTerm
	if lo > s.meta.Base+1 {
		predecessor, _, h, err := s.get(lo - 1)
		if err != nil {
			s.poison = err
			return nil, err
		}
		prev = h
		previousTerm = predecessor.GetTerm()
	}
	it, err := s.db.NewIter(&pebble.IterOptions{LowerBound: entryKey(lo), UpperBound: entryKey(hi)})
	if err != nil {
		s.poison = err
		return nil, err
	}
	defer func() { err = errors.Join(err, it.Close()) }()
	var size uint64
	index := lo
	for valid := it.First(); valid; valid = it.Next() {
		if !bytes.Equal(it.Key(), entryKey(index)) {
			s.poison = ErrCorrupt
			return nil, ErrCorrupt
		}
		e, before, hash, err := decodeEntry(index, it.Value(), s.limits.MaxEntryBytes)
		if err != nil || before != prev || e.GetTerm() < previousTerm || e.GetTerm() > s.meta.Hard.GetTerm() {
			s.poison = ErrCorrupt
			return nil, errors.Join(ErrCorrupt, err)
		}
		n := unsignedLimit(proto.Size(e))
		if len(entries) > 0 && (len(entries) >= s.limits.MaxReadEntries || n > unsignedLimit(s.limits.MaxReadBytes)-size || size > maxSize || n > maxSize-size) {
			break
		}
		entries = append(entries, e)
		size += n
		prev = hash
		previousTerm = e.GetTerm()
		index++
		if len(entries) >= s.limits.MaxReadEntries || size >= min(maxSize, unsignedLimit(s.limits.MaxReadBytes)) {
			break
		}
	}
	if err := it.Error(); err != nil {
		s.poison = err
		return nil, err
	}
	if len(entries) == 0 {
		s.poison = ErrCorrupt
		return nil, ErrCorrupt
	}
	// A short page is allowed; a missing key before either budget is not.
	if index < hi && len(entries) < s.limits.MaxReadEntries && size < min(maxSize, unsignedLimit(s.limits.MaxReadBytes)) && !it.Valid() {
		s.poison = ErrCorrupt
		return nil, ErrCorrupt
	}
	return entries, nil
}

// Persist atomically replaces an uncommitted suffix and stores HardState/snapshot.
// Every changed Ready is synced, including commit-only updates (stricter than MustSync).
// Pinned Pebble fatal WAL/sync errors FAIL-STOP THE PROCESS (its supported
// default Fatalf policy); no packets return. Recoverable errors poison the
// handle. Never install a returning/no-op Fatalf: other fatal paths include
// background invariants and assume termination. Uncertain outcomes are not aborts.
func (s *Store) Persist(rd raft.Ready) (err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(); err != nil {
		return err
	}
	if len(rd.HardState.ProtoReflect().GetUnknown()) != 0 || len(rd.Snapshot.ProtoReflect().GetUnknown()) != 0 || len(rd.Snapshot.GetMetadata().ProtoReflect().GetUnknown()) != 0 {
		return ErrInvalid
	}
	if raft.IsEmptyHardState(rd.HardState) && len(rd.Entries) == 0 && raft.IsEmptySnap(rd.Snapshot) {
		return nil
	}
	// Preflight before protobuf clones or batch growth. MaxReadyBytes covers
	// frame input plus batch/metadata envelope allowance; snapshot work has its
	// own budget (two persisted copies, plus caller-owned input).
	if len(rd.Entries) > 4096 {
		return ErrLimit
	}
	small, err := encodeMeta(s.meta)
	if err != nil {
		return err
	}
	total := uint64(len(small) + 128)
	for _, e := range rd.Entries {
		if e == nil || len(e.ProtoReflect().GetUnknown()) != 0 {
			return ErrInvalid
		}
		if len(e.GetData()) > s.limits.MaxEntryBytes-frameOverhead {
			return ErrLimit
		}
		total += uint64(len(e.GetData()) + frameOverhead + 9 + 32)
		if total > unsignedLimit(s.limits.MaxReadyBytes) {
			return ErrLimit
		}
	}
	if len(rd.Snapshot.GetData()) > s.limits.MaxSnapshotBytes {
		return ErrLimit
	}
	if proto.Size(rd.HardState) > 64 || proto.Size(rd.Snapshot.GetMetadata()) > 32768 {
		return ErrLimit
	}
	m := s.meta
	m.Hard = proto.Clone(m.Hard).(*pb.HardState)
	batch := s.db.NewBatch()
	defer func() {
		if closeErr := batch.Close(); closeErr != nil {
			s.poison = closeErr
			err = errors.Join(err, ErrPoisoned, closeErr)
		}
	}()
	if !raft.IsEmptySnap(rd.Snapshot) {
		snap := rd.Snapshot
		idx := snap.GetMetadata().GetIndex()
		if idx <= m.Base || idx < m.Hard.GetCommit() || snap.GetMetadata().GetTerm() == 0 {
			return ErrInvalid
		}
		committedTerm := m.BaseTerm
		if m.Hard.GetCommit() > m.Base {
			committed, _, _, err := s.get(m.Hard.GetCommit())
			if err != nil {
				s.poison = err
				return err
			}
			committedTerm = committed.GetTerm()
		}
		if snap.GetMetadata().GetTerm() < committedTerm {
			return ErrInvalid
		}
		if len(snap.GetData()) > s.limits.MaxSnapshotBytes {
			return ErrLimit
		}
		if err := validateConf(snap.GetMetadata().GetConfState(), idx); err != nil {
			return err
		}
		m.Snap = &pb.Snapshot{Metadata: proto.Clone(snap.GetMetadata()).(*pb.SnapshotMetadata)}
		m.Base, m.Last, m.Applied = idx, idx, idx
		m.BaseTerm = snap.GetMetadata().GetTerm()
		m.BaseHash, m.LastHash = [32]byte{}, [32]byte{}
		m.Conf = proto.Clone(snap.GetMetadata().GetConfState()).(*pb.ConfState)
		m.ImageBytes = uint64(len(snap.GetData()))
		m.SnapBytes = m.ImageBytes
		m.ImageHash = sha256.Sum256(snap.GetData())
		m.SnapHash = m.ImageHash
		if err := batch.Set(imageKey, snap.GetData(), nil); err != nil {
			return err
		}
		if err := batch.Set(snapshotKey, snap.GetData(), nil); err != nil {
			return err
		}
		m.LogBytes, m.LogCount = 0, 0
		if err := batch.DeleteRange(entryKey(0), entryEnd, nil); err != nil {
			return err
		}
	}
	if !raft.IsEmptyHardState(rd.HardState) {
		if rd.GetTerm() < m.Hard.GetTerm() || rd.GetCommit() < m.Hard.GetCommit() || (rd.GetTerm() == m.Hard.GetTerm() && m.Hard.GetVote() != 0 && rd.GetVote() != m.Hard.GetVote()) {
			return ErrInvalid
		}
		m.Hard = proto.Clone(rd.HardState).(*pb.HardState)
	}
	var addedBytes uint64
	if len(rd.Entries) > 0 {
		first := rd.Entries[0].GetIndex()
		if first <= m.Base || first <= s.meta.Hard.GetCommit() || first > m.Last+1 {
			return ErrInvalid
		}
		prev := m.BaseHash
		previousTerm := m.BaseTerm
		if first > m.Base+1 {
			predecessor, _, hash, err := s.get(first - 1)
			if err != nil {
				s.poison = err
				return err
			}
			prev = hash
			previousTerm = predecessor.GetTerm()
		}
		if first <= m.Last {
			removed, err := s.rangeBytes(first, m.Last+1)
			if err != nil {
				s.poison = err
				return err
			}
			if removed > m.LogBytes {
				s.poison = ErrCorrupt
				return ErrCorrupt
			}
			m.LogBytes -= removed
			if err := batch.DeleteRange(entryKey(first), entryEnd, nil); err != nil {
				return err
			}
		}
		for offset, e := range rd.Entries {
			if e == nil || e.GetIndex() != first+uint64(offset) || e.GetIndex() == math.MaxUint64 || e.GetTerm() == 0 || e.GetTerm() > m.Hard.GetTerm() || e.GetTerm() < previousTerm || e.GetType() < pb.EntryNormal || e.GetType() > pb.EntryConfChangeV2 {
				return ErrInvalid
			}
			if len(e.GetData()) > s.limits.MaxEntryBytes-frameOverhead {
				return ErrLimit
			}
			previousTerm = e.GetTerm()
			b, hash := encodeEntry(e, prev)
			prev = hash
			addedBytes += uint64(len(b) + 9)
			if addedBytes > unsignedLimit(s.limits.MaxReadyBytes) {
				return ErrLimit
			}
			if err := batch.Set(entryKey(e.GetIndex()), b, nil); err != nil {
				return err
			}
		}
		m.Last = rd.Entries[len(rd.Entries)-1].GetIndex()
		m.LastHash = prev
		m.LogBytes += addedBytes
		m.LogCount = m.Last - m.Base
	}
	if m.LogBytes > s.limits.MaxRetainedBytes || m.LogCount > s.limits.MaxRetainedEntries {
		return ErrLimit
	}
	if err := s.validate(m); err != nil {
		return err
	}
	// Commit owns/closes its batch, so supply a separate reference only once.
	b, err := encodeMeta(m)
	if err != nil {
		return err
	}
	if err := batch.Set(metaKey, b, nil); err != nil {
		return err
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		s.poison = err
		return errors.Join(ErrPoisoned, err)
	}
	s.meta = m
	return nil
}

func (s *Store) rangeBytes(lo, hi uint64) (total uint64, err error) {
	prev := s.meta.BaseHash
	previousTerm := s.meta.BaseTerm
	if lo > s.meta.Base+1 {
		before, _, hash, err := s.get(lo - 1)
		if err != nil {
			return 0, err
		}
		prev, previousTerm = hash, before.GetTerm()
	}
	it, err := s.db.NewIter(&pebble.IterOptions{LowerBound: entryKey(lo), UpperBound: entryKey(hi)})
	if err != nil {
		return 0, err
	}
	defer func() { err = errors.Join(err, it.Close()) }()
	var n uint64
	index := lo
	for ok := it.First(); ok; ok = it.Next() {
		if !bytes.Equal(it.Key(), entryKey(index)) {
			return 0, ErrCorrupt
		}
		term, before, hash, err := inspectEntry(index, it.Value(), s.limits.MaxEntryBytes)
		if err != nil || before != prev || term < previousTerm || term > s.meta.Hard.GetTerm() {
			return 0, errors.Join(ErrCorrupt, err)
		}
		prev, previousTerm = hash, term
		n += uint64(len(it.Key()) + len(it.Value()))
		index++
	}
	if err := it.Error(); err != nil {
		return 0, err
	}
	if index != hi {
		return 0, ErrCorrupt
	}
	if hi == s.meta.Last+1 && prev != s.meta.LastHash {
		return 0, ErrCorrupt
	}
	return n, nil
}

// Checkpoint returns an owned reducer image and its durable applied position.
func (s *Store) Checkpoint() (uint64, []byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(); err != nil {
		return 0, nil, err
	}
	image, err := s.loadImage(imageKey, s.meta.ImageBytes, s.meta.ImageHash)
	if err != nil {
		s.poison = err
		return 0, nil, err
	}
	return s.meta.Applied, image, nil
}

// SaveCheckpoint co-commits reducer image, applied position and membership.
// It preserves log/CDC retention. Publication is explicit; arbitrary application
// allocations during image construction are outside this adapter's control.
func (s *Store) SaveCheckpoint(index uint64, cs *pb.ConfState, image []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(); err != nil {
		return err
	}
	if index < s.meta.Applied || index > s.meta.Hard.GetCommit() {
		return ErrInvalid
	}
	if len(image) > s.limits.MaxSnapshotBytes {
		return ErrLimit
	}
	if err := validateConf(cs, index); err != nil {
		return err
	}
	m := s.meta
	m.Applied = index
	m.Conf = proto.Clone(cs).(*pb.ConfState)
	m.ImageBytes = uint64(len(image))
	m.ImageHash = sha256.Sum256(image)
	b := s.db.NewBatch()
	if err := b.Set(imageKey, image, nil); err != nil {
		_ = b.Close()
		return err
	}
	return s.commit(m, b)
}

// PublishSnapshot replaces the recovery base and reclaims its prefix only when
// the caller confirms its image retains every application/CDC/pin obligation.
// It may publish ONLY the current durable checkpoint, never an unapplied index.
func (s *Store) PublishSnapshot() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(); err != nil {
		return err
	}
	m := s.meta
	if m.Applied <= m.Base {
		return raft.ErrSnapOutOfDate
	}
	e, _, hash, err := s.get(m.Applied)
	if err != nil {
		s.poison = err
		return err
	}
	removed, err := s.rangeBytes(m.Base+1, m.Applied+1)
	if err != nil {
		s.poison = err
		return err
	}
	if removed > m.LogBytes {
		s.poison = ErrCorrupt
		return ErrCorrupt
	}
	m.Base, m.BaseTerm, m.BaseHash = m.Applied, e.GetTerm(), hash
	m.LogBytes -= removed
	m.LogCount = m.Last - m.Base
	image, err := s.loadImage(imageKey, m.ImageBytes, m.ImageHash)
	if err != nil {
		s.poison = err
		return err
	}
	m.SnapBytes = m.ImageBytes
	m.SnapHash = m.ImageHash
	m.Snap = &pb.Snapshot{Metadata: &pb.SnapshotMetadata{Index: new(m.Applied), Term: new(e.GetTerm()), ConfState: proto.Clone(m.Conf).(*pb.ConfState)}}
	b := s.db.NewBatch()
	if err := b.Set(snapshotKey, image, nil); err != nil {
		_ = b.Close()
		return err
	}
	if err := b.DeleteRange(entryKey(0), entryKey(m.Base+1), nil); err != nil {
		_ = b.Close()
		return err
	}
	return s.commit(m, b)
}

// Scrub verifies the entire retained log in bounded pages, including chain links.
func (s *Store) Scrub() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(); err != nil {
		return err
	}
	if err := s.checkEntryBounds(); err != nil {
		s.poison = err
		return err
	}
	first, last := s.meta.Base+1, s.meta.Last
	var totalBytes, totalCount uint64
	for first <= last {
		es, err := s.entries(first, last+1, math.MaxUint64)
		if err != nil {
			return err
		}
		for _, e := range es {
			totalBytes += uint64(frameOverhead + 9 + len(e.GetData()))
			totalCount++
		}
		first = es[len(es)-1].GetIndex() + 1
	}
	if totalBytes != s.meta.LogBytes || totalCount != s.meta.LogCount {
		s.poison = ErrCorrupt
		return ErrCorrupt
	}
	if last > s.meta.Base {
		_, _, hash, err := s.get(last)
		if err != nil || hash != s.meta.LastHash {
			s.poison = ErrCorrupt
			return errors.Join(ErrCorrupt, err)
		}
	} else if s.meta.LastHash != s.meta.BaseHash {
		s.poison = ErrCorrupt
		return ErrCorrupt
	}
	if _, err := s.loadImage(imageKey, s.meta.ImageBytes, s.meta.ImageHash); err != nil {
		s.poison = err
		return err
	}
	if _, err := s.loadImage(snapshotKey, s.meta.SnapBytes, s.meta.SnapHash); err != nil {
		s.poison = err
		return err
	}
	return nil
}

// checkEntryBounds verifies that the first and last visible entry keys match
// the ledger without decoding or indexing the retained log at open. Prefix
// tombstones are respected by Pebble; stale physical LSM records are allowed.
func (s *Store) checkEntryBounds() (err error) {
	it, err := s.db.NewIter(&pebble.IterOptions{LowerBound: entryKey(0), UpperBound: entryEnd})
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, it.Close()) }()
	first := it.First()
	if err := it.Error(); err != nil {
		return err
	}
	if s.meta.LogCount == 0 {
		if first {
			return ErrCorrupt
		}
		return nil
	}
	if !first || !bytes.Equal(it.Key(), entryKey(s.meta.Base+1)) {
		return ErrCorrupt
	}
	last := it.Last()
	if err := it.Error(); err != nil {
		return err
	}
	if !last || !bytes.Equal(it.Key(), entryKey(s.meta.Last)) {
		return ErrCorrupt
	}

	return it.Error()
}

func (s *Store) loadImage(key []byte, n uint64, hash [32]byte) (image []byte, err error) {
	value, closer, err := s.db.Get(key)
	if err != nil {
		return nil, errors.Join(ErrCorrupt, err)
	}
	defer func() { err = errors.Join(err, closer.Close()) }()
	if n > unsignedLimit(s.limits.MaxSnapshotBytes) || uint64(len(value)) != n || sha256.Sum256(value) != hash {
		return nil, ErrCorrupt
	}
	return bytes.Clone(value), nil
}

// Close releases the database and is safe to repeat.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	return s.db.Close()
}

// Limits returns this store's immutable adapter configuration.
func (s *Store) Limits() Limits {
	if s == nil {
		return Limits{}
	}
	return s.limits
}
