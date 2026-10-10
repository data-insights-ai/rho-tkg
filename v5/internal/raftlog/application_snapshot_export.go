package raftlog

import (
	"bytes"
	"context"
	"errors"

	"github.com/cockroachdb/pebble/v2"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// ApplicationExport owns one counted Pebble snapshot and a bounded root image.
// The whole shared DB is snapshotted: obsolete log/SST versions may remain pinned
// until Close. Logical byte ledgers do not bound that physical retention. Each
// Next call uses bounded page capacity plus one contract-bounded borrowed frame;
// framing/copy work is at most two page buffers plus that lookahead. There is no
// whole-database image or retained iterator/key map. Store.Close invalidates handles.
type ApplicationExport struct {
	published     *publishedExportState
	bank          byte
	generation    uint64
	ref           *generationRef
	s             *Store
	snapshot      *pebble.Snapshot
	manifest      ApplicationSnapshotManifest
	after         []byte
	sequence      uint64
	pinBytes      uint64
	closed, final bool
}

func cloneSnapshotManifest(m ApplicationSnapshotManifest) ApplicationSnapshotManifest {
	m.Image = copyApplicationBytes(m.Image)
	m.ConfState = proto.Clone(m.ConfState).(*pb.ConfState)
	return m
}

// BeginApplicationExport captures the current applied image, term, membership,
// ledgers and backend snapshot under the same store lock. A bounded-page pass
// verifies all retained history before returning a manifest. Concurrent later
// keys/roots and log appends cannot inflate this captured stream. Cancellation
// releases the snapshot; verified durable corruption poisons the source store.
func (s *Store) BeginApplicationExport(ctx context.Context) (*ApplicationExport, error) {
	if s == nil {
		return nil, ErrInvalid
	}
	if err := snapshotContext(ctx); err != nil {
		return nil, err
	}
	s.mu.Lock()
	if err := s.check(); err != nil {
		s.mu.Unlock()
		return nil, err
	}
	if s.meta.Rep.SemanticContractID != (ApplicationSemanticContractID{}) {
		s.mu.Unlock()
		return nil, ErrInvalid
	}
	tc := s.meta.Transfer
	if !tc.enabled() || s.meta.Applied == 0 {
		s.mu.Unlock()
		return nil, ErrInvalid
	}
	metaBytes, err := encodeMeta(s.meta)
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	pin := s.meta.App.Bytes + s.meta.LogBytes + s.meta.ImageBytes + s.meta.SnapBytes + uint64(len(metaBytes)+3)
	if s.meta.Gen.Limits.enabled() {
		for j, b := range s.meta.Gen.Banks {
			if byte(j) != s.activeBank() {
				pin += b.Bytes
			}
		}
	}
	if i := s.applicationImport; i != nil {
		if !s.meta.Gen.Limits.enabled() {
			pin += i.state.bytes
		}
		pin += uint64(len(i.descriptor()) + len(dormantKey))
	}
	if len(s.applicationExports) >= tc.Limits.MaxExports || pin > tc.Limits.MaxPinnedLogicalBytes-s.pinnedApplicationBytes {
		s.mu.Unlock()
		return nil, ErrLimit
	}
	image, err := s.loadImage(imageKey, s.meta.ImageBytes, s.meta.ImageHash)
	if err != nil {
		s.poison = err
		s.mu.Unlock()
		return nil, err
	}
	m := ApplicationSnapshotManifest{Identity: tc.Identity, Contract: tc.Contract, Index: s.meta.Applied, Image: image, ImageHash: s.meta.ImageHash, ConfState: canonicalConf(s.meta.Conf)}
	if m.Index == s.meta.Base {
		m.Term = s.meta.BaseTerm
	} else {
		e, _, _, err := s.get(m.Index)
		if err != nil {
			s.poison = err
			s.mu.Unlock()
			return nil, err
		}
		m.Term = e.GetTerm()
	}
	expectedBytes, expectedRows := s.meta.App.Bytes, s.meta.App.Records
	if m.Index > expectedRows/3 {
		s.poison = ErrCorrupt
		s.mu.Unlock()
		return nil, ErrCorrupt
	}
	m.NamespaceRecords = [4]uint64{expectedRows - 3*m.Index, m.Index, m.Index, m.Index}
	// The exact per-namespace bytes are computed by the verification pass; the
	// captured total is a cap throughout, not trusted preallocation metadata.
	for j := range m.NamespaceBytes {
		m.NamespaceBytes[j] = expectedBytes
	}
	ref, err := s.pinGeneration(s.activeBank())
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	e := &ApplicationExport{bank: s.activeBank(), generation: s.activeGeneration(), ref: ref, s: s, snapshot: s.db.NewSnapshot(), manifest: m, pinBytes: pin}
	if s.applicationExports == nil {
		s.applicationExports = make(map[*ApplicationExport]struct{})
	}
	s.applicationExports[e] = struct{}{}
	s.pinnedApplicationBytes += pin
	s.mu.Unlock()
	state := snapshotVerifier{digest: snapshotSeed(m)}
	for {
		chunk, err := e.Next(ctx, ReadBudget{tc.Limits.MaxChunkRows, tc.Limits.MaxChunkBytes})
		if err == nil {
			err = walkSnapshotChunk(chunk.Data, m.Contract, func(k, v []byte) error { var x error; state, x = state.accept(m, k, v); return x })
		}
		if err != nil {
			_ = e.Close()
			if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, ErrLimit) && !errors.Is(err, ErrClosed) {
				s.mu.Lock()
				s.poison = err
				s.mu.Unlock()
			}
			return nil, err
		}
		if chunk.Final {
			break
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(); err != nil {
		return nil, errors.Join(err, e.closeLocked())
	}
	if e.closed {
		return nil, ErrClosed
	}
	if state.bytes != expectedBytes || state.rows != expectedRows {
		err := errors.Join(ErrCorrupt, e.closeLocked())
		s.poison = err
		return nil, err
	}
	for _, index := range state.envelopes {
		if index != m.Index {
			err := errors.Join(ErrCorrupt, e.closeLocked())
			s.poison = err
			return nil, err
		}
	}
	m.NamespaceBytes = state.namespaceBytes
	m.RecordsHash = state.digest
	if err := validateManifest(m); err != nil {
		err = errors.Join(err, e.closeLocked())
		s.poison = err
		return nil, err
	}
	e.manifest = m
	e.after = nil
	e.sequence = 0
	e.final = false
	return e, nil
}

// Manifest returns owned portable metadata and root bytes for this captured stream.
func (e *ApplicationExport) Manifest() (ApplicationSnapshotManifest, error) {
	if e == nil {
		return ApplicationSnapshotManifest{}, ErrInvalid
	}
	e.s.mu.Lock()
	defer e.s.mu.Unlock()
	if e.closed {
		return ApplicationSnapshotManifest{}, ErrClosed
	}
	if err := e.s.check(); err != nil {
		return ApplicationSnapshotManifest{}, err
	}
	if e.published != nil && !e.published.ready {
		return ApplicationSnapshotManifest{}, ErrInvalid
	}
	return cloneSnapshotManifest(e.manifest), nil
}
func appendBoundedSnapshot(dst, k, v []byte, limit int) []byte {
	need := len(dst) + 8 + len(k) + len(v)
	if need > cap(dst) {
		next := make([]byte, len(dst), min(limit, max(need, 2*cap(dst))))
		copy(next, dst)
		dst = next
	}
	return appendSnapshotFrame(dst, k, v)
}

// Next returns strictly ordered canonical records with an explicit final page.
// ErrLimit for a frame that cannot fit leaves the cursor unchanged. Budgets cap
// owned output capacity; internal transient work additionally has one lookahead
// and at most two capped page buffers. Next after final returns ErrInvalid.
func (e *ApplicationExport) Next(ctx context.Context, b ReadBudget) (chunk ApplicationSnapshotChunk, err error) {
	if e == nil {
		return chunk, ErrInvalid
	}
	if err := snapshotContext(ctx); err != nil {
		return chunk, err
	}
	s := e.s
	s.mu.Lock()
	defer s.mu.Unlock()
	if e.closed {
		return chunk, ErrClosed
	}
	if err := s.check(); err != nil {
		return chunk, err
	}
	if e.generation != 0 && s.meta.Gen.Banks[e.bank].Generation != e.generation {
		return chunk, ErrClosed
	}
	if e.final {
		return chunk, ErrInvalid
	}
	if e.published != nil {
		return e.nextPublished(ctx, b)
	}
	l := s.meta.Transfer.Limits
	if b.Rows < 1 || b.Bytes < 1 {
		return chunk, ErrInvalid
	}
	if b.Rows > l.MaxChunkRows || b.Bytes > l.MaxChunkBytes {
		return chunk, ErrLimit
	}
	it, err := e.snapshot.NewIter(&pebble.IterOptions{LowerBound: []byte{bankTag(e.bank, appDataTag)}, UpperBound: []byte{bankTag(e.bank, appOutcomeTag) + 1}})
	if err != nil {
		s.poison = err
		return chunk, err
	}
	defer func() {
		if ce := it.Close(); ce != nil {
			s.poison = ce
			err = errors.Join(err, ce)
		}
	}()
	valid := it.First()
	if len(e.after) > 0 {
		valid = it.SeekGE(e.after)
		if valid && bytes.Equal(it.Key(), e.after) {
			valid = it.Next()
		}
	}
	data := make([]byte, 0, min(b.Bytes, 4096))
	var last []byte
	rows := 0
	for valid {
		if err := ctx.Err(); err != nil {
			return ApplicationSnapshotChunk{}, err
		}
		k := it.Key()
		v, readErr := it.ValueAndErr()
		if readErr != nil {
			s.poison = storedReadFailure(readErr)
			return ApplicationSnapshotChunk{}, s.poison
		}
		if len(v) < appFrameBytes || len(k) > 2*e.manifest.Contract.MaxKeyBytes+11 || len(v) > max(e.manifest.Contract.MaxValueBytes, e.manifest.Contract.MaxImageBytes, e.manifest.Contract.MaxChangeBytes, e.manifest.Contract.MaxOutcomeBytes)+appFrameBytes {
			s.poison = ErrCorrupt
			return ApplicationSnapshotChunk{}, ErrCorrupt
		}
		cost := 8 + len(k) + len(v)
		if rows >= b.Rows || cost > b.Bytes-len(data) {
			if rows == 0 {
				return ApplicationSnapshotChunk{}, ErrLimit
			}
			break
		}
		if e.bank == 0 {
			data = appendBoundedSnapshot(data, k, v, b.Bytes)
		} else {
			value, deleted, inspectErr := inspectAppFrame(k, v, len(v)-appFrameBytes)
			if inspectErr != nil {
				s.poison = inspectErr
				return chunk, inspectErr
			}
			canonical := copyApplicationBytes(k)
			canonical[0] -= e.bank * 4
			data = appendBoundedSnapshot(data, canonical, appFrame(canonical, value, deleted), b.Bytes)
		}
		last = copyApplicationBytes(k)
		rows++
		valid = it.Next()
	}
	if err := it.Error(); err != nil {
		s.poison = err
		return ApplicationSnapshotChunk{}, err
	}
	chunk = ApplicationSnapshotChunk{Sequence: e.sequence, Data: copyApplicationBytes(data), Final: !valid}
	if e.sequence == ^uint64(0) {
		return ApplicationSnapshotChunk{}, ErrLimit
	}
	e.after = last
	e.sequence++
	e.final = chunk.Final
	return chunk, nil
}
func (e *ApplicationExport) closeLocked() error {
	if e.closed {
		return nil
	}
	e.closed = true
	delete(e.s.applicationExports, e)
	e.s.pinnedApplicationBytes -= e.pinBytes
	e.manifest = ApplicationSnapshotManifest{}
	e.after = nil
	e.published = nil
	err := e.snapshot.Close()
	e.snapshot = nil
	if err != nil {
		e.s.poison = err
	}
	return errors.Join(err, e.s.releaseGeneration(e.ref))
}

// Close releases the backend snapshot and its ledgers; repeated calls are safe.
func (e *ApplicationExport) Close() error {
	if e == nil {
		return ErrInvalid
	}
	e.s.mu.Lock()
	defer e.s.mu.Unlock()
	return e.closeLocked()
}
