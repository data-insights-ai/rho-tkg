package raftlog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math"
	"sync"

	"github.com/cockroachdb/pebble/v2"
	pb "go.etcd.io/raft/v3/raftpb"
)

// ApplicationPolicy selects a provisional local-root checkpoint mode. The
// immutable KV/install contracts are independent of graph layout; LocalVoter
// fences this integration until transferable application snapshots exist.
// RetainedApplicationBytes counts ALL immutable logical keys/frames/values,
// roots, changes and outcomes; it is NOT a physical disk quota. WAL/SST and
// compaction amplification, OS cache and caller-retained copies are separate.
// KV/envelope reads are serialized and expose at most MaxPageBytes owned output
// capacity plus one bounded key/value lookahead. Root image capacity is bounded
// separately by MaxImageBytes; retained views share MaxViewBytes. These visible
// slice capacities do not measure allocator rounding, heap/RSS or temporary work.
// Policies are explicit, versioned and must match exactly on reopen.
type ApplicationPolicy struct {
	LocalVoter                                                         uint64
	MaxKeyBytes, MaxValueBytes, MaxImageBytes                          int
	MaxPageRows, MaxPageBytes                                          int
	MaxInstallWrites, MaxInstallBytes, MaxChangeBytes, MaxOutcomeBytes int
	MaxViews, MaxViewBytes                                             int
	RetainedApplicationBytes, RetainedApplicationRecords               uint64
	MaxTailEntries, MaxTailBytes, ReclaimEntries                       uint64
}

// DefaultApplicationPolicy returns finite candidate limits, not capacity/SLOs.
func DefaultApplicationPolicy(voter uint64) ApplicationPolicy {
	return ApplicationPolicy{voter, 1024, 1 << 20, 64 << 10, 4096, 2 << 20, 4096, 4 << 20, 1 << 20, 64 << 10, 16, 1 << 20, 1 << 30, 1_000_000, 64, 16 << 20, 32}
}

// Enabled distinguishes explicit application mode from the scalar adapter.
func (p ApplicationPolicy) Enabled() bool { return p != (ApplicationPolicy{}) }

// Validate checks finite limits and consensus admission headroom.
func (p ApplicationPolicy) Validate(l Limits) error { return p.validate(l) }
func (p ApplicationPolicy) validate(l Limits) error {
	if !p.Enabled() {
		return nil
	}
	if p.LocalVoter == 0 || p.LocalVoter >= math.MaxUint64-1 {
		return ErrInvalid
	}
	for _, n := range []int{p.MaxKeyBytes, p.MaxValueBytes, p.MaxImageBytes, p.MaxPageRows, p.MaxPageBytes, p.MaxInstallWrites, p.MaxInstallBytes, p.MaxChangeBytes, p.MaxOutcomeBytes, p.MaxViews, p.MaxViewBytes} {
		if n < 1 || n > 64<<20 {
			return ErrInvalid
		}
	}
	if p.MaxKeyBytes > 64<<10 || p.MaxValueBytes > 16<<20 || p.MaxImageBytes > l.MaxSnapshotBytes || p.MaxPageRows > 4096 || p.MaxInstallWrites > 4096 || p.MaxViews > 4096 || p.MaxViewBytes < p.MaxImageBytes || p.MaxPageBytes < 2*p.MaxKeyBytes+p.MaxValueBytes+64 || p.MaxChangeBytes > p.MaxPageBytes || p.MaxOutcomeBytes > p.MaxPageBytes || p.MaxInstallBytes < 1024+2*3*(9+appFrameBytes) || p.RetainedApplicationBytes < unsignedLimit(2*p.MaxInstallBytes) || p.RetainedApplicationBytes > 1<<40 || p.RetainedApplicationRecords < unsignedLimit(2*(p.MaxInstallWrites+3)) || p.RetainedApplicationRecords > 1<<40 || p.MaxTailEntries < 2 || p.MaxTailEntries > l.MaxRetainedEntries || p.MaxTailBytes < unsignedLimit(l.MaxEntryBytes+9+frameOverhead+9) || p.MaxTailBytes > l.MaxRetainedBytes || p.ReclaimEntries < 1 || p.ReclaimEntries > l.MaxRetainedEntries-2 || l.MaxRetainedEntries < 3 || l.MaxRetainedBytes < unsignedLimit(l.MaxEntryBytes+9+frameOverhead+9) {
		return ErrInvalid
	}
	return nil
}

type applicationMetadata struct {
	Policy                             ApplicationPolicy
	Bytes, Records, Through, TailBytes uint64
}

func applicationNumbers(a applicationMetadata) []uint64 {
	p := a.Policy
	return []uint64{p.LocalVoter, unsignedLimit(p.MaxKeyBytes), unsignedLimit(p.MaxValueBytes), unsignedLimit(p.MaxImageBytes), unsignedLimit(p.MaxPageRows), unsignedLimit(p.MaxPageBytes), unsignedLimit(p.MaxInstallWrites), unsignedLimit(p.MaxInstallBytes), unsignedLimit(p.MaxChangeBytes), unsignedLimit(p.MaxOutcomeBytes), unsignedLimit(p.MaxViews), unsignedLimit(p.MaxViewBytes), p.RetainedApplicationBytes, p.RetainedApplicationRecords, p.MaxTailEntries, p.MaxTailBytes, p.ReclaimEntries, a.Bytes, a.Records, a.Through, a.TailBytes}
} // #nosec G115 -- policy int fields validated positive before persistence.
func appendApplicationMeta(dst []byte, a applicationMetadata) []byte {
	dst = append(dst, 'A', 'M', 1, 0)
	for _, n := range applicationNumbers(a) {
		dst = binary.BigEndian.AppendUint64(dst, n)
	}
	return dst
}
func decodeApplicationMeta(src []byte) (applicationMetadata, []byte, error) {
	const size = 4 + 21*8
	if len(src) < size || !bytes.Equal(src[:4], []byte{'A', 'M', 1, 0}) {
		return applicationMetadata{}, nil, ErrCorrupt
	}
	var ns [21]uint64
	for i := range ns {
		ns[i] = binary.BigEndian.Uint64(src[4+8*i:])
	}
	for _, n := range ns[1:12] {
		if n > 64<<20 {
			return applicationMetadata{}, nil, ErrCorrupt
		}
	}
	p := ApplicationPolicy{ns[0], int(ns[1]), int(ns[2]), int(ns[3]), int(ns[4]), int(ns[5]), int(ns[6]), int(ns[7]), int(ns[8]), int(ns[9]), int(ns[10]), int(ns[11]), ns[12], ns[13], ns[14], ns[15], ns[16]}
	return applicationMetadata{p, ns[17], ns[18], ns[19], ns[20]}, src[size:], nil
} // #nosec G115 -- int fields prevalidated against 64 MiB.
func localConfiguration(c *pb.ConfState, id uint64) bool {
	return c != nil && len(c.ProtoReflect().GetUnknown()) == 0 && len(c.GetVoters()) == 1 && c.GetVoters()[0] == id && len(c.GetVotersOutgoing()) == 0 && len(c.GetLearners()) == 0 && len(c.GetLearnersNext()) == 0 && !c.GetAutoLeave()
}
func (s *Store) validateApplicationMeta(m metadata) error {
	a, p := m.App, m.App.Policy
	if !p.Enabled() {
		if a != (applicationMetadata{}) {
			return ErrInvalid
		}
		return nil
	}
	if err := p.validate(s.limits); err != nil {
		return err
	}
	if a.Bytes > p.RetainedApplicationBytes || a.Records > p.RetainedApplicationRecords || a.Through != m.Applied || a.TailBytes > p.MaxTailBytes || m.Last-m.Applied > p.MaxTailEntries {
		return ErrLimit
	}
	pending := m.Last - m.Applied
	if pending > (p.RetainedApplicationBytes-a.Bytes)/unsignedLimit(p.MaxInstallBytes) || pending > (p.RetainedApplicationRecords-a.Records)/unsignedLimit(p.MaxInstallWrites+3) {
		return ErrLimit
	}
	if m.Applied > 0 && (!applicationConfiguration(m, m.Conf) || !applicationConfiguration(m, m.Snap.GetMetadata().GetConfState()) || m.ImageBytes > unsignedLimit(p.MaxImageBytes)) {
		return ErrInvalid
	}
	if a.TailBytes < pending*(frameOverhead+9) || a.TailBytes > pending*unsignedLimit(s.limits.MaxEntryBytes+9) {
		return ErrCorrupt
	}
	return nil
}

const appFrameBytes = 36
const appDataTag byte = 8
const appRootTag byte = 9
const appChangeTag byte = 10
const appOutcomeTag byte = 11

func appIndexKey(tag byte, index uint64) []byte {
	return binary.BigEndian.AppendUint64([]byte{tag}, index)
}
func appPrefix(key []byte) []byte {
	b := make([]byte, 1, 2*len(key)+3)
	b[0] = appDataTag
	for _, c := range key {
		if c == 0 {
			b = append(b, 0, 255)
		} else {
			b = append(b, c)
		}
	}
	return append(b, 0, 0)
}
func appVersionKey(key []byte, index uint64) []byte {
	return binary.BigEndian.AppendUint64(appPrefix(key), ^index)
}
func appNextPrefix(prefix []byte) []byte { b := bytes.Clone(prefix); b[len(b)-1]++; return b }
func decodeAppKey(src []byte, maxBytes int) ([]byte, uint64, error) {
	return decodeTaggedAppKey(src, maxBytes, appDataTag)
}
func decodeTaggedAppKey(src []byte, maxBytes int, tag byte) ([]byte, uint64, error) {
	if len(src) < 12 || src[0] != tag || len(src) > 2*maxBytes+11 {
		return nil, 0, ErrCorrupt
	}
	key := make([]byte, 0, min(len(src), maxBytes))
	for i := 1; i < len(src)-8; i++ {
		c := src[i]
		if c == 0 {
			i++
			if i >= len(src)-8 {
				return nil, 0, ErrCorrupt
			}
			if src[i] == 0 {
				if i != len(src)-9 || len(key) == 0 {
					return nil, 0, ErrCorrupt
				}
				index := ^binary.BigEndian.Uint64(src[len(src)-8:])
				if index < 2 || index == math.MaxUint64 {
					return nil, 0, ErrCorrupt
				}
				return key, index, nil
			}
			if src[i] != 255 {
				return nil, 0, ErrCorrupt
			}
			c = 0
		}
		if len(key) >= maxBytes {
			return nil, 0, ErrCorrupt
		}
		key = append(key, c)
	}
	return nil, 0, ErrCorrupt
}
func appFrame(key, value []byte, deleted bool) []byte {
	b := make([]byte, appFrameBytes, appFrameBytes+len(value))
	copy(b, []byte{'A', 'V', 1, 0})
	if deleted {
		b[3] = 1
	}
	b = append(b, value...)
	h := sha256.New()
	_, _ = h.Write(key)
	_, _ = h.Write(b[:4])
	_, _ = h.Write(value)
	copy(b[4:36], h.Sum(nil))
	return b
}
func inspectAppFrame(key, src []byte, maxBytes int) ([]byte, bool, error) {
	if len(src) < appFrameBytes || len(src)-appFrameBytes > maxBytes || src[0] != 'A' || src[1] != 'V' || src[2] != 1 || src[3] > 1 || src[3] == 1 && len(src) != appFrameBytes {
		return nil, false, ErrCorrupt
	}
	h := sha256.New()
	_, _ = h.Write(key)
	_, _ = h.Write(src[:4])
	_, _ = h.Write(src[appFrameBytes:])
	if !bytes.Equal(src[4:36], h.Sum(nil)) {
		return nil, false, ErrCorrupt
	}
	return src[appFrameBytes:], src[3] == 1, nil
}
func (s *Store) appGet(key []byte, maxBytes int) ([]byte, bool, error) {
	b, c, err := s.db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	data, deleted, e := inspectAppFrame(key, b, maxBytes)
	out := []byte(nil)
	if e == nil {
		out = bytes.Clone(data)
	}
	return out, !deleted, errors.Join(e, c.Close())
}
func (s *Store) verifyApplicationRoot(index uint64) error {
	image, found, err := s.appGet(bankIndexKey(s.activeBank(), appRootTag, index), s.meta.App.Policy.MaxImageBytes)
	if err != nil || !found || uint64(len(image)) != s.meta.ImageBytes || sha256.Sum256(image) != s.meta.ImageHash {
		return errors.Join(ErrCorrupt, err)
	}
	for _, tag := range []byte{appChangeTag, appOutcomeTag} {
		limit := s.meta.App.Policy.MaxChangeBytes
		if tag == appOutcomeTag {
			limit = s.meta.App.Policy.MaxOutcomeBytes
		}
		if _, found, err := s.appGet(bankIndexKey(s.activeBank(), tag, index), limit); err != nil || !found {
			return errors.Join(ErrCorrupt, err)
		}
	}
	return nil
}
func (s *Store) addInitialApplication(b *pebble.Batch, m *metadata, image []byte) error {
	for _, v := range []struct {
		tag  byte
		data []byte
	}{{appRootTag, image}, {appChangeTag, nil}, {appOutcomeTag, nil}} {
		k := bankIndexKey(m.Gen.Active, v.tag, 1)
		value := appFrame(k, v.data, false)
		if err := b.Set(k, value, nil); err != nil {
			return err
		}
		m.App.Bytes += uint64(len(k) + len(value))
		m.App.Records++
	}
	m.App.Through = 1
	return s.validateApplicationMeta(*m)
}

// KV carries an owned value or an explicit tombstone. Empty present bytes and
// tombstones are different. Logical keys are nonempty arbitrary bounded bytes.
type KV struct {
	Key, Value []byte
	Deleted    bool
}

// ApplicationBatch is a private bounded checkpoint installation. Changes and
// Outcome are complete opaque envelopes supplied by the trusted state machine;
// this storage layer never interprets graph semantics or certifies a graph cut.
// BaseGeneration must match the captured local physical generation in RLM5;
// zero is the legacy binding, never replicated semantic time/effect identity.
type ApplicationBatch struct {
	BaseGeneration   uint64
	BaseImageHash    [32]byte
	BaseIndex        uint64
	Image            []byte
	Writes           []KV
	Changes, Outcome []byte
}

// ApplicationBudget bounds conservative encoded installation staging including
// framing/copies, not Go heap/RSS or caller-retained buffers.
type ApplicationBudget struct{ Writes, Bytes, ImageBytes, ChangeBytes, OutcomeBytes int }

// ApplicationLimits returns the immutable policy; nil returns disabled mode.
func (s *Store) ApplicationLimits() ApplicationPolicy {
	if s == nil {
		return ApplicationPolicy{}
	}
	return s.applicationPolicy
}

// ApplicationBudget returns the concrete state-machine staging limits.
func (s *Store) ApplicationBudget() ApplicationBudget {
	p := s.ApplicationLimits()
	return ApplicationBudget{p.MaxInstallWrites, p.MaxInstallBytes, p.MaxImageBytes, p.MaxChangeBytes, p.MaxOutcomeBytes}
}

func (p ApplicationPolicy) measure(b ApplicationBatch) (uint64, uint64, error) {
	if len(b.Writes) > p.MaxInstallWrites || len(b.Image) > p.MaxImageBytes || len(b.Changes) > p.MaxChangeBytes || len(b.Outcome) > p.MaxOutcomeBytes {
		return 0, 0, ErrLimit
	}
	retained := uint64(3*(9+appFrameBytes) + len(b.Image) + len(b.Changes) + len(b.Outcome))
	work := 2*retained + uint64(len(b.Image)+1024)
	for i, w := range b.Writes {
		if len(w.Key) == 0 || w.Deleted && len(w.Value) != 0 || i > 0 && bytes.Compare(b.Writes[i-1].Key, w.Key) >= 0 {
			return 0, 0, ErrInvalid
		}
		if len(w.Key) > p.MaxKeyBytes || len(w.Value) > p.MaxValueBytes {
			return 0, 0, ErrLimit
		}
		size := unsignedLimit(len(w.Key) + bytes.Count(w.Key, []byte{0}) + 3 + 8 + appFrameBytes + len(w.Value))
		retained += size
		work += 2*size + 64
		if work > unsignedLimit(p.MaxInstallBytes) {
			return 0, 0, ErrLimit
		}
	}
	if work > unsignedLimit(p.MaxInstallBytes) {
		return 0, 0, ErrLimit
	}
	return retained, uint64(len(b.Writes) + 3), nil
}

// InstallApplication synchronously co-commits immutable KV versions, a retained
// root, full change group/outcome, checkpoint image and applied/tail ledgers.
// It accepts ONLY the next already committed normal entry and matching base.
// Errors before batch commit leave durable state unchanged. Uncertain write or
// close failures poison the store; callers must stop serving and reopen.
func (s *Store) InstallApplication(index uint64, b ApplicationBatch) error {
	if s == nil {
		return ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(); err != nil {
		return err
	}
	p := s.meta.App.Policy
	if b.BaseGeneration != s.activeGeneration() {
		return ErrInvalid
	}
	if !p.Enabled() || index != s.meta.Applied+1 || index > s.meta.Hard.GetCommit() || b.BaseIndex != s.meta.Applied || b.BaseImageHash != s.meta.ImageHash {
		return ErrInvalid
	}
	added, count, err := p.measure(b)
	if err != nil {
		return err
	}
	if added > p.RetainedApplicationBytes-s.meta.App.Bytes || count > p.RetainedApplicationRecords-s.meta.App.Records {
		return ErrLimit
	}
	prospective := s.meta
	prospective.Applied = index
	prospective.App.Through = index
	prospective.App.Bytes += added
	prospective.App.Records += count
	syncActiveGeneration(&prospective)
	if err := generationCharge(prospective, prospective.Last-index); err != nil {
		return err
	}
	e, _, _, err := s.get(index)
	if err != nil {
		s.poison = err
		return err
	}
	if e.GetType() != pb.EntryNormal {
		return ErrInvalid
	}
	size := uint64(frameOverhead + 9 + len(e.GetData()))
	if size > s.meta.App.TailBytes {
		s.poison = ErrCorrupt
		return ErrCorrupt
	}
	m := s.meta
	m.Applied = index
	m.App.Through = index
	m.App.TailBytes -= size
	m.App.Bytes += added
	m.App.Records += count
	m.ImageBytes = uint64(len(b.Image))
	m.ImageHash = sha256.Sum256(b.Image)
	syncActiveGeneration(&m)
	if err := s.validate(m); err != nil {
		return err
	}
	batch := s.db.NewBatch()
	fail := func(e error) error { return errors.Join(e, batch.Close()) }
	for _, w := range b.Writes {
		k := bankVersionKey(s.activeBank(), w.Key, index)
		if err := batch.Set(k, appFrame(k, w.Value, w.Deleted), nil); err != nil {
			return fail(err)
		}
	}
	for _, v := range []struct {
		tag  byte
		data []byte
	}{{appRootTag, b.Image}, {appChangeTag, b.Changes}, {appOutcomeTag, b.Outcome}} {
		k := bankIndexKey(s.activeBank(), v.tag, index)
		if err := batch.Set(k, appFrame(k, v.data, false), nil); err != nil {
			return fail(err)
		}
	}
	if err := batch.Set(imageKey, b.Image, nil); err != nil {
		return fail(err)
	}
	return s.commit(m, batch)
}

// AdmitApplication conservatively reserves worst-case installation growth for
// the proposal AND one internal no-op before RawNode accepts work. Rejections
// are pre-proposal; unexpected failures after durable commit require fail-stop.
func (s *Store) AdmitApplication(commandBytes int) error {
	if s == nil {
		return ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(); err != nil {
		return err
	}
	p := s.meta.App.Policy
	if !p.Enabled() || s.meta.Applied == 0 || commandBytes < 0 {
		return ErrInvalid
	}
	if commandBytes > s.limits.MaxEntryBytes-frameOverhead {
		return ErrLimit
	}
	count := s.meta.Last - s.meta.Applied + 2
	extra := uint64(commandBytes + 2*(frameOverhead+9))
	if count > p.MaxTailEntries || extra > p.MaxTailBytes-s.meta.App.TailBytes || s.meta.LogCount+2 > s.limits.MaxRetainedEntries || extra > s.limits.MaxRetainedBytes-s.meta.LogBytes || count > (p.RetainedApplicationBytes-s.meta.App.Bytes)/unsignedLimit(p.MaxInstallBytes) || count > (p.RetainedApplicationRecords-s.meta.App.Records)/unsignedLimit(p.MaxInstallWrites+3) {
		return ErrLimit
	}
	return generationCharge(s.meta, count)
}

// ReclaimApplication replaces the local recovery base only at its configured
// retained-log watermark. Application versions, roots, CDC and outcomes survive.
func (s *Store) ReclaimApplication() error {
	if s == nil {
		return ErrInvalid
	}
	s.publicationMu.Lock()
	defer s.publicationMu.Unlock()
	s.mu.Lock()
	if err := s.check(); err != nil {
		s.mu.Unlock()
		return err
	}
	p := s.meta.App.Policy
	if !p.Enabled() {
		s.mu.Unlock()
		return ErrInvalid
	}
	headroom := unsignedLimit(s.limits.MaxEntryBytes + 9 + frameOverhead + 9)
	needed := (s.meta.LogCount >= p.ReclaimEntries || s.meta.LogBytes > s.limits.MaxRetainedBytes-headroom || s.meta.LogCount > s.limits.MaxRetainedEntries-2) && s.meta.Applied > s.meta.Base
	s.mu.Unlock()
	if needed {
		return s.publishSnapshot()
	}
	return nil
}

// ReadBudget bounds visited logical keys as well as owned returned bytes. A
// tombstone consumes work even though it is not emitted in a scan.
type ReadBudget struct{ Rows, Bytes int }

// ApplicationPage owns copied rows and a last-visited logical key continuation.
// Bytes conservatively covers both visited-key work and retained output, including
// the Rows backing capacity; it is not a heap/RSS or allocator-size measurement.
// Next must be reused with the same view/range. Complete is explicit; empty
// output can have a continuation when a page contains only tombstones.
type ApplicationPage struct {
	Rows           []KV
	Next           []byte
	Complete       bool
	Visited, Bytes int
}

// ApplicationRoot is an owned local checkpoint reference, NOT a certified cut.
// Generation is a local physical-bank binding, zero in legacy mode; it is not
// replicated semantic time or an effect identity.
type ApplicationRoot struct {
	Generation uint64
	Index      uint64
	Image      []byte
	ImageHash  [32]byte
}

// ApplicationView reads immutable MVCC versions at one retained root. It pins
// no backend snapshot and keeps no per-record index. Close serializes with reads.
type ApplicationView struct {
	bank       byte
	generation uint64
	ref        *generationRef
	mu         sync.Mutex
	s          *Store
	index      uint64
	image      []byte
	closed     bool
}

// ApplicationView opens a bounded root handle. Roots never expire in this slice;
// retained-data quotas backpressure writes instead of reclaiming view evidence.
func (s *Store) ApplicationView(index uint64) (*ApplicationView, error) {
	if s == nil {
		return nil, ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(); err != nil {
		return nil, err
	}
	p := s.meta.App.Policy
	if !p.Enabled() || index == 0 || index > s.meta.Applied {
		return nil, ErrInvalid
	}
	if s.views >= p.MaxViews {
		return nil, ErrLimit
	}
	key := bankIndexKey(s.activeBank(), appRootTag, index)
	raw, closer, err := s.db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		s.poison = ErrCorrupt
		return nil, ErrCorrupt
	}
	if err != nil {
		s.poison = err
		return nil, err
	}
	data, deleted, inspectErr := inspectAppFrame(key, raw, p.MaxImageBytes)
	if deleted {
		inspectErr = ErrCorrupt
	}
	fits := len(data) <= p.MaxViewBytes-s.viewBytes
	var owned []byte
	if inspectErr == nil && fits {
		owned = copyApplicationBytes(data)
	}
	if err := errors.Join(inspectErr, closer.Close()); err != nil {
		s.poison = err
		return nil, err
	}
	if !fits {
		return nil, ErrLimit
	}
	ref, err := s.pinGeneration(s.activeBank())
	if err != nil {
		return nil, err
	}
	s.views++
	s.viewBytes += len(owned)
	return &ApplicationView{s: s, index: index, image: owned, bank: s.activeBank(), generation: s.activeGeneration(), ref: ref}, nil
}
func (v *ApplicationView) lock(ctx context.Context) error {
	if v == nil || ctx == nil {
		return ErrInvalid
	}
	v.mu.Lock()
	if v.closed {
		v.mu.Unlock()
		return ErrClosed
	}
	if err := ctx.Err(); err != nil {
		v.mu.Unlock()
		return err
	}
	v.s.mu.Lock()
	if v.generation != 0 && v.s.meta.Gen.Banks[v.bank].Generation != v.generation {
		v.s.mu.Unlock()
		v.mu.Unlock()
		return ErrClosed
	}
	if err := v.s.check(); err != nil {
		v.s.mu.Unlock()
		v.mu.Unlock()
		return err
	}
	return nil
}
func (v *ApplicationView) unlock() { v.s.mu.Unlock(); v.mu.Unlock() }

// Root returns owned root bytes after checking both view and store lifetime.
// The image has exact capacity within MaxImageBytes; root metadata is fixed-size.
func (v *ApplicationView) Root() (ApplicationRoot, error) {
	if err := v.lock(context.Background()); err != nil {
		return ApplicationRoot{}, err
	}
	defer v.unlock()
	return ApplicationRoot{Generation: v.generation, Index: v.index, Image: copyApplicationBytes(v.image), ImageHash: sha256.Sum256(v.image)}, nil
}

// Close releases root accounting and is safe to repeat, including after Store.Close.
func (v *ApplicationView) Close() error {
	if v == nil {
		return ErrInvalid
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.closed {
		return nil
	}
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	v.closed = true
	v.s.views--
	v.s.viewBytes -= len(v.image)
	v.image = nil
	return v.s.releaseGeneration(v.ref)
}
func (v *ApplicationView) iterator(lower, upper []byte) (*pebble.Iterator, error) {
	return v.s.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
}

// Get returns exact old/current content, including an explicit tombstone.
// found=false means never written at this root; Deleted distinguishes retraction.
// maxBytes bounds key/value payload length; exact-cap copies and the policy
// headroom additionally bound the complete owned KV output.
func (v *ApplicationView) Get(ctx context.Context, key []byte, maxBytes int) (out KV, found bool, err error) {
	if err = v.lock(ctx); err != nil {
		return KV{}, false, err
	}
	defer v.unlock()
	p := v.s.meta.App.Policy
	if len(key) == 0 || maxBytes < 0 {
		return KV{}, false, ErrInvalid
	}
	if len(key) > p.MaxKeyBytes || maxBytes > p.MaxPageBytes {
		return KV{}, false, ErrLimit
	}
	prefix := bankPrefix(v.bank, key)
	it, err := v.iterator(prefix, appNextPrefix(prefix))
	if err != nil {
		return KV{}, false, v.fail(err)
	}
	defer func() {
		err = errors.Join(err, it.Close())
		if err != nil && !errors.Is(err, ErrLimit) {
			v.s.poison = err
		}
	}()
	if !it.SeekGE(bankVersionKey(v.bank, key, v.index)) {
		return KV{}, false, it.Error()
	}
	physical := it.Key()
	actual, index, e := decodeBankAppKey(v.bank, physical, p.MaxKeyBytes)
	if e != nil || !bytes.Equal(actual, key) || index > v.index {
		return KV{}, false, v.fail(errors.Join(ErrCorrupt, e))
	}
	value, deleted, e := inspectAppFrame(physical, it.Value(), p.MaxValueBytes)
	if e != nil {
		return KV{}, false, v.fail(e)
	}
	if len(key)+len(value) > maxBytes {
		return KV{}, false, ErrLimit
	}
	return KV{copyApplicationBytes(key), copyApplicationBytes(value), deleted}, true, nil
}
func (v *ApplicationView) fail(err error) error {
	if err != nil {
		v.s.poison = err
	}
	return err
}

// ReadLimits returns immutable configured per-call row/byte maxima, not remaining
// quota, view validity or a retention lease. Nil returns zero. A closed view may
// still report its configuration; the store's application policy never changes.
func (v *ApplicationView) ReadLimits() ReadBudget {
	if v == nil {
		return ReadBudget{}
	}
	p := v.s.ApplicationLimits()
	return ReadBudget{Rows: p.MaxPageRows, Bytes: p.MaxPageBytes}
}

// Scan enumerates logical keys in byte order, seeking over version runs without
// decoding history or building a resident key map. Tombstones are omitted but
// budgeted. nil upper is unbounded; nil lower starts the namespace. after must
// be empty or a key in this exact range. Owned bytes include keys/Next and a
// conservative 64-byte header for each retained row-capacity slot; one bounded
// lookahead is borrowed internally.
func (v *ApplicationView) Scan(ctx context.Context, lower, upper, after []byte, b ReadBudget) (page ApplicationPage, err error) {
	if err = v.lock(ctx); err != nil {
		return page, err
	}
	defer v.unlock()
	p := v.s.meta.App.Policy
	if b.Rows < 1 || b.Bytes < 1 || len(upper) > 0 && bytes.Compare(lower, upper) >= 0 || len(after) > 0 && (bytes.Compare(after, lower) < 0 || len(upper) > 0 && bytes.Compare(after, upper) >= 0) {
		return page, ErrInvalid
	}
	if b.Rows > p.MaxPageRows || b.Bytes > p.MaxPageBytes || len(lower) > p.MaxKeyBytes || len(upper) > p.MaxKeyBytes || len(after) > p.MaxKeyBytes {
		return page, ErrLimit
	}
	lo := []byte{bankTag(v.bank, appDataTag)}
	if len(lower) > 0 {
		lo = bankPrefix(v.bank, lower)
	}
	hi := []byte{bankTag(v.bank, appDataTag) + 1}
	if len(upper) > 0 {
		hi = bankPrefix(v.bank, upper)
	}
	it, err := v.iterator(lo, hi)
	if err != nil {
		return page, v.fail(err)
	}
	defer func() {
		e := it.Close()
		err = errors.Join(err, e)
		if e != nil {
			v.s.poison = e
		}
	}()
	seek := lo
	if len(after) > 0 {
		seek = appNextPrefix(bankPrefix(v.bank, after))
	}
	// Work includes every visited key. Owned output additionally reserves the
	// complete Rows backing array, including capacity beyond its length.
	workBytes, outputBytes := 0, 0
	for valid := it.SeekGE(seek); valid; {
		if e := ctx.Err(); e != nil {
			return ApplicationPage{}, e
		}
		key, index, e := decodeBankAppKey(v.bank, it.Key(), p.MaxKeyBytes)
		if e != nil {
			return ApplicationPage{}, v.fail(e)
		}
		prefix := bankPrefix(v.bank, key)
		visible := true
		if index > v.index {
			visible = it.SeekGE(bankVersionKey(v.bank, key, v.index)) && bytes.HasPrefix(it.Key(), prefix)
			if e := it.Error(); e != nil {
				return ApplicationPage{}, v.fail(e)
			}
		}
		var value []byte
		deleted := false
		if visible {
			actual, version, e := decodeBankAppKey(v.bank, it.Key(), p.MaxKeyBytes)
			if e != nil || !bytes.Equal(actual, key) || version > v.index {
				return ApplicationPage{}, v.fail(errors.Join(ErrCorrupt, e))
			}
			value, deleted, e = inspectAppFrame(it.Key(), it.Value(), p.MaxValueBytes)
			if e != nil {
				return ApplicationPage{}, v.fail(e)
			}
		}
		// Every discovered logical key consumes work, even if it was created
		// after this view. Otherwise an old empty root could scan an unbounded
		// later database in a single Rows=1 call.
		cost := len(key) + len(value) + 64
		emit := visible && !deleted
		rowBytes, capacity := 0, cap(page.Rows)
		if emit {
			rowBytes = len(key) + len(value)
			if len(page.Rows) == capacity {
				// Grow geometrically where room permits, but never retain an
				// uncharged backing array. Header accounting is conservative
				// on supported Go architectures; byte copies have exact cap.
				available := b.Bytes - (outputBytes - 64*capacity) - rowBytes - len(key)
				capacity = min(max(1, 2*capacity), available/64, b.Rows)
			}
		}
		owned := outputBytes + 64*(capacity-cap(page.Rows)) + rowBytes + len(key)
		if page.Visited >= b.Rows || cost+len(key) > b.Bytes-workBytes || emit && capacity < len(page.Rows)+1 || owned > b.Bytes {
			if page.Visited == 0 {
				return ApplicationPage{}, ErrLimit
			}
			page.Bytes = max(workBytes+len(page.Next), outputBytes+len(page.Next))
			return page, nil
		}
		page.Visited++
		workBytes += cost
		page.Next = copyApplicationBytes(key)
		if emit {
			if capacity > cap(page.Rows) {
				rows := make([]KV, len(page.Rows), capacity)
				copy(rows, page.Rows)
				outputBytes += 64 * (capacity - cap(page.Rows))
				page.Rows = rows
			}
			page.Rows = append(page.Rows, KV{Key: copyApplicationBytes(key), Value: copyApplicationBytes(value)})
			outputBytes += rowBytes
		}
		valid = it.SeekGE(appNextPrefix(prefix))
	}
	if e := it.Error(); e != nil {
		return ApplicationPage{}, v.fail(e)
	}
	page.Complete = true
	page.Next = nil
	page.Bytes = max(workBytes, outputBytes)
	return page, nil
}

// copyApplicationBytes owns only the requested length; unlike append/Clone,
// it exposes no spare byte capacity in returned application output.
func copyApplicationBytes(src []byte) []byte {
	if src == nil {
		return nil
	}
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}

// ApplicationRecord retrieves a retained complete change/outcome envelope at a
// local applied entry. Neither its index nor a root is a public database cut.
func (s *Store) ApplicationRecord(ctx context.Context, index uint64, outcome bool, maxBytes int) ([]byte, error) {
	if s == nil || ctx == nil {
		return nil, ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(); err != nil {
		return nil, err
	}
	p := s.meta.App.Policy
	if !p.Enabled() || index == 0 || index > s.meta.Applied || maxBytes < 0 {
		return nil, ErrInvalid
	}
	limit := p.MaxChangeBytes
	tag := appChangeTag
	if outcome {
		limit = p.MaxOutcomeBytes
		tag = appOutcomeTag
	}
	if maxBytes > p.MaxPageBytes {
		return nil, ErrLimit
	}
	key := bankIndexKey(s.activeBank(), tag, index)
	raw, closer, err := s.db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		s.poison = ErrCorrupt
		return nil, ErrCorrupt
	}
	if err != nil {
		s.poison = err
		return nil, err
	}
	data, deleted, inspectErr := inspectAppFrame(key, raw, limit)
	var out []byte
	if inspectErr == nil && !deleted && len(data) <= maxBytes {
		out = copyApplicationBytes(data)
	}
	if deleted {
		inspectErr = ErrCorrupt
	}
	if err := errors.Join(inspectErr, closer.Close()); err != nil {
		s.poison = err
		return nil, err
	}
	if len(data) > maxBytes {
		return nil, ErrLimit
	}
	return out, nil
}

// ApplicationUsage reports independent logical and bounded live-root ledgers.
// CheckpointBytes/SnapshotBytes are the fixed-bounded consensus image copies;
// RetainedBytes covers all immutable application namespaces. Do not interpret
// their sum as physical disk, heap or OS-cache consumption.
type ApplicationUsage struct {
	RetainedBytes, RetainedRecords, TailBytes, TailEntries uint64
	CheckpointBytes, SnapshotBytes                         uint64
	Views, ViewBytes                                       int
}

// ApplicationUsage returns a serialized accounting snapshot.
func (s *Store) ApplicationUsage() (ApplicationUsage, error) {
	if s == nil {
		return ApplicationUsage{}, ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(); err != nil {
		return ApplicationUsage{}, err
	}
	if !s.meta.App.Policy.Enabled() {
		return ApplicationUsage{}, ErrInvalid
	}
	m := s.meta
	return ApplicationUsage{m.App.Bytes, m.App.Records, m.App.TailBytes, m.Last - m.Applied, m.ImageBytes, m.SnapBytes, s.views, s.viewBytes}, nil
}

// ScrubApplication verifies every immutable version/root/change/outcome in
// constant resident scan space, recomputes retained ledgers and checks complete
// per-index envelope runs. It never materializes a database key map. Cancellation
// is checked between bounded frames; corruption/read errors poison the store.
func (s *Store) ScrubApplication(ctx context.Context) (err error) {
	if s == nil || ctx == nil {
		return ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(); err != nil {
		return err
	}
	p := s.meta.App.Policy
	if !p.Enabled() {
		return ErrInvalid
	}
	it, err := s.db.NewIter(&pebble.IterOptions{LowerBound: []byte{bankTag(s.activeBank(), appDataTag)}, UpperBound: []byte{bankTag(s.activeBank(), appOutcomeTag) + 1}})
	if err != nil {
		s.poison = err
		return err
	}
	defer func() {
		closeErr := it.Close()
		err = errors.Join(err, closeErr)
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			s.poison = err
		}
	}()
	var total, records uint64
	var envelopes [3]uint64
	for valid := it.First(); valid; valid = it.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		key := it.Key()
		limit := p.MaxValueBytes
		if key[0] == bankTag(s.activeBank(), appDataTag) {
			_, index, err := decodeBankAppKey(s.activeBank(), key, p.MaxKeyBytes)
			if err != nil || index > s.meta.Applied {
				return errors.Join(ErrCorrupt, err)
			}
		} else {
			if len(key) != 9 {
				return ErrCorrupt
			}
			slot := int(key[0] - bankTag(s.activeBank(), appRootTag))
			index := binary.BigEndian.Uint64(key[1:])
			if index != envelopes[slot]+1 || index > s.meta.Applied {
				return ErrCorrupt
			}
			envelopes[slot] = index
			switch key[0] - s.activeBank()*4 {
			case appRootTag:
				limit = p.MaxImageBytes
			case appChangeTag:
				limit = p.MaxChangeBytes
			case appOutcomeTag:
				limit = p.MaxOutcomeBytes
			}
		}
		_, deleted, err := inspectAppFrame(key, it.Value(), limit)
		if err != nil || deleted && key[0] != bankTag(s.activeBank(), appDataTag) {
			return errors.Join(ErrCorrupt, err)
		}
		cost := uint64(len(key) + len(it.Value()))
		if cost > p.RetainedApplicationBytes-total || records >= p.RetainedApplicationRecords {
			return ErrCorrupt
		}
		total += cost
		records++
	}
	if err := it.Error(); err != nil {
		return err
	}
	if total != s.meta.App.Bytes || records != s.meta.App.Records {
		return ErrCorrupt
	}
	for _, index := range envelopes {
		if index != s.meta.Applied {
			return ErrCorrupt
		}
	}
	return nil
}
