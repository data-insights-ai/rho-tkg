package raftlog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"

	"github.com/cockroachdb/pebble/v2"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

// Wire width and owned Go representation are separate bounds. The fixed
// allowances are verified against unsafe.Sizeof in tests, including both slice
// headers in a returned chunk and the retained handle/state/backend snapshot.
const snapshotChunkV2WireHeaderBytes = 4 + 8 + 32 + 32 + 8 + 8 + 4 + 1
const snapshotChunkV2FixedBytes = 160
const publishedExportFixedBytes = 1024

type publishedExportState struct {
	expectedBytes, expectedRows uint64
	verifier                    snapshotVerifier
	buildChunks                 uint64
	manifestID                  [32]byte
	ready                       bool
}

// publishedManifestID commits to the cut (including image hash and membership),
// canonical history and namespace ledgers without copying/hashing the image a
// second time. Callers first validate or verify the manifest and its image.
func publishedManifestID(m ApplicationSnapshotManifest) [32]byte {
	domain := []byte("rho-tkg:application-snapshot-manifest:v2\x00")
	if m.Version == 3 {
		domain = append([]byte("rho-tkg:application-snapshot-manifest:v3\x00"), m.SemanticContractID[:]...)
	}
	b := append(domain, m.CutID[:]...)
	b = append(b, m.RecordsHash[:]...)
	for _, ns := range [][4]uint64{m.NamespaceBytes, m.NamespaceRecords} {
		for _, n := range ns {
			b = binary.BigEndian.AppendUint64(b, n)
		}
	}
	return sha256.Sum256(b)
}

// BeginPublishedApplicationExport reserves a counted immutable source for the
// exact durable cut, including after reopen with later applied records. It does
// no history scan. BuildManifest must finish before Manifest or Next is usable.
// Replacement never moves this handle. Close releases its generation and whole
// Pebble snapshot; the durable publication independently retains its bank.
// Pin admission includes the whole DB logical ledger and bounded handle-owned
// image, protobuf/header and cursor/verifier capacities. Returned Manifest copies
// belong to the caller; they are each contract-bounded, not tracked after return.
func (s *Store) BeginPublishedApplicationExport(ctx context.Context, id [32]byte) (*ApplicationExport, error) {
	if s == nil {
		return nil, ErrInvalid
	}
	if err := snapshotContext(ctx); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(); err != nil {
		return nil, err
	}
	p := s.meta.Gen.Publication
	tc := s.meta.Transfer
	if !p.Limits.enabled() || !p.Valid {
		return nil, ErrInvalid
	}
	if id != p.ID {
		return nil, raft.ErrSnapOutOfDate
	}
	// All components are policy-bounded at admission, hence this sum cannot wrap.
	pin := s.meta.App.Bytes + s.meta.LogBytes + s.meta.ImageBytes + s.meta.SnapBytes + metadataBytes(s.meta) + 3
	for j, b := range s.meta.Gen.Banks {
		if byte(j) != s.activeBank() {
			pin += b.Bytes
		}
	}
	if i := s.applicationImport; i != nil {
		pin += uint64(importDescriptorBytes(i) + len(dormantKey)) // #nosec G115 -- descriptor is fixed metadata plus a validated cursor <= 2*64KiB+11.
	}
	pin += s.meta.SnapBytes + uint64(snapshotConfCapacity(s.meta.Snap.GetMetadata().GetConfState())+publishedExportFixedBytes+4*(2*tc.Contract.MaxKeyBytes+11)) // #nosec G115 -- validated ConfState wire <= 32KiB and key <= 64KiB bound this positive fixed-capacity sum below 1MiB.
	if len(s.applicationExports) >= tc.Limits.MaxExports || s.pinnedApplicationBytes > tc.Limits.MaxPinnedLogicalBytes || pin > tc.Limits.MaxPinnedLogicalBytes-s.pinnedApplicationBytes {
		return nil, ErrLimit
	}
	image, err := s.loadPublishedImage()
	if err != nil {
		s.poison = err
		return nil, err
	}
	ref, err := s.pinGeneration(p.Bank)
	if err != nil {
		return nil, err
	}
	cut := cutReference(s.meta)
	m := ApplicationSnapshotManifest{Version: semanticManifestVersion(cut.SemanticContractID), SemanticContractID: cut.SemanticContractID, CutID: id, Identity: cut.Identity, Contract: cut.Contract, Index: cut.Index, Term: cut.Term, ConfState: canonicalConf(cut.ConfState), Image: image, ImageHash: cut.ImageHash}
	m.NamespaceRecords = [4]uint64{p.Records - 3*m.Index, m.Index, m.Index, m.Index}
	for j := range m.NamespaceBytes {
		m.NamespaceBytes[j] = p.Bytes
	}
	e := &ApplicationExport{bank: p.Bank, generation: p.Generation, ref: ref, s: s, snapshot: s.db.NewSnapshot(), manifest: m, pinBytes: pin}
	e.published = &publishedExportState{expectedBytes: p.Bytes, expectedRows: p.Records, verifier: snapshotVerifier{digest: snapshotSeed(m)}}
	if s.applicationExports == nil {
		s.applicationExports = make(map[*ApplicationExport]struct{})
	}
	s.applicationExports[e] = struct{}{}
	s.pinnedApplicationBytes += pin
	return e, nil
}

// BuildManifest verifies at most one row/byte-bounded physical page, charging
// future rows even though they will not transfer. Cancellation or ErrLimit
// leaves all cursors, counters and digest unchanged and retryable. A completed
// handle is idempotently ready. Durable corruption poisons the source; it never
// silently certifies an incomplete envelope run. AS1 handles are already ready.
func (e *ApplicationExport) BuildManifest(ctx context.Context, b ReadBudget) (bool, error) {
	if e == nil {
		return false, ErrInvalid
	}
	if err := snapshotContext(ctx); err != nil {
		return false, err
	}
	s := e.s
	s.mu.Lock()
	defer s.mu.Unlock()
	if e.closed {
		return false, ErrClosed
	}
	if err := s.check(); err != nil {
		return false, err
	}
	p := e.published
	if p == nil || p.ready {
		return true, nil
	}
	if p.buildChunks >= s.meta.Gen.Publication.Limits.MaxTransferChunks {
		return false, ErrLimit
	}
	chunk, state, err := e.publishedPage(ctx, b, true)
	if err != nil {
		if publishedExportFailure(err) {
			s.poison = err
		}
		return false, err
	}
	if chunk.Final {
		if state.bytes != p.expectedBytes || state.rows != p.expectedRows {
			s.poison = ErrCorrupt
			return false, ErrCorrupt
		}
		for _, index := range state.envelopes {
			if index != e.manifest.Index {
				s.poison = ErrCorrupt
				return false, ErrCorrupt
			}
		}
		m := e.manifest
		m.NamespaceBytes = state.namespaceBytes
		m.RecordsHash = state.digest
		// The image was checked at capture. Its cut ID must agree with the exact
		// verified totals; no full-image encode/hash is hidden in this page's finish.
		id, err := cutID(ApplicationCutReference{SemanticContractID: m.SemanticContractID, Identity: m.Identity, Contract: m.Contract, Index: m.Index, Term: m.Term, ConfState: m.ConfState, ImageBytes: uint64(len(m.Image)), ImageHash: m.ImageHash, RetainedBytes: state.bytes, RetainedRecords: state.rows})
		if err != nil || id != m.CutID {
			s.poison = ErrCorrupt
			return false, ErrCorrupt
		}
		e.manifest = m
		p.manifestID = publishedManifestID(m)
		p.ready = true
		p.verifier = snapshotVerifier{}
		e.after = nil
		e.sequence = 0
		e.final = false
	} else {
		p.verifier = state
		e.after = chunk.After
	}
	p.buildChunks++
	return p.ready, nil
}

// publishedPage does not mutate the handle, even on iterator-close failure.
// Bytes conservatively cap visited framing work plus output capacity, fixed
// header, old cursor/verifier-last and three current cursor/key copies. At most
// two capped data buffers and one contract-bounded frame lookahead coexist.
// The manifest/image are separately included in pin admission, not recopied.
func (e *ApplicationExport) publishedPage(ctx context.Context, b ReadBudget, build bool) (chunk ApplicationSnapshotChunk, state snapshotVerifier, err error) {
	l := e.s.meta.Transfer.Limits
	m := e.manifest
	p := e.published
	state = p.verifier
	if b.Rows < 1 || b.Bytes < 1 {
		return chunk, state, ErrInvalid
	}
	if b.Rows > l.MaxChunkRows || b.Bytes > l.MaxChunkBytes {
		return chunk, state, ErrLimit
	}
	base := snapshotChunkV2FixedBytes + 2*cap(e.after) + cap(state.last)
	if base > b.Bytes {
		return chunk, state, ErrLimit
	}
	it, err := e.snapshot.NewIter(&pebble.IterOptions{LowerBound: []byte{bankTag(e.bank, appDataTag)}, UpperBound: []byte{bankTag(e.bank, appOutcomeTag) + 1}})
	if err != nil {
		return chunk, state, err
	}
	defer func() { err = errors.Join(err, it.Close()) }()
	valid := it.First()
	if len(e.after) > 0 {
		after := copyApplicationBytes(e.after)
		after[0] += e.bank * 4
		valid = it.SeekGE(after)
		if valid && bytes.Equal(it.Key(), after) {
			valid = it.Next()
		}
	}
	var data, last []byte
	rows, work := 0, 0
	for valid {
		if err := ctx.Err(); err != nil {
			return ApplicationSnapshotChunk{}, p.verifier, err
		}
		local := it.Key()
		raw, readErr := it.ValueAndErr()
		if readErr != nil {
			return chunk, state, storedReadFailure(readErr)
		}
		if len(local) == 0 || len(local) > 2*m.Contract.MaxKeyBytes+11 || len(raw) < appFrameBytes || len(raw) > max(m.Contract.MaxValueBytes, m.Contract.MaxImageBytes, m.Contract.MaxChangeBytes, m.Contract.MaxOutcomeBytes)+appFrameBytes {
			return chunk, state, ErrCorrupt
		}
		cost := 8 + len(local) + len(raw)
		if rows >= b.Rows || 2*(work+cost)+3*len(local) > b.Bytes-base {
			if rows == 0 {
				return chunk, state, ErrLimit
			}
			break
		}
		k := copyApplicationBytes(local)
		k[0] -= e.bank * 4
		index, err := snapshotCursorIndex(k, m.Contract)
		if err != nil {
			return chunk, state, err
		}
		// Future records still consume row/byte work and a canonical progress key.
		// Their values are not part of this cut and are never emitted or certified.
		if index <= m.Index {
			value, deleted, err := inspectAppFrame(local, raw, len(raw)-appFrameBytes)
			if err != nil {
				return chunk, state, err
			}
			canonical := raw
			if e.bank != 0 {
				canonical = appFrame(k, value, deleted)
			}
			if build {
				state, err = state.accept(m, k, canonical)
				if err != nil {
					return chunk, state, err
				}
			} else {
				data = appendBoundedSnapshot(data, k, canonical, work+cost)
			}
		}
		last = k
		rows++
		work += cost
		valid = it.Next()
	}
	if err := it.Error(); err != nil {
		return chunk, state, err
	}
	if err := ctx.Err(); err != nil {
		return chunk, state, err
	}
	if rows == 0 {
		if base+2*len(e.after) > b.Bytes {
			return chunk, state, ErrLimit
		}
		last = copyApplicationBytes(e.after)
	}
	chunk = ApplicationSnapshotChunk{Version: m.Version, CutID: m.CutID, ManifestID: p.manifestID, Sequence: e.sequence, Data: data, After: last, Visited: uint64(rows), VisitedBytes: uint64(work), Final: !valid}
	return chunk, state, nil
}
func snapshotCursorIndex(k []byte, c ApplicationContract) (uint64, error) {
	if len(k) == 0 {
		return 0, ErrInvalid
	}
	if k[0] == appDataTag {
		_, index, err := decodeAppKey(k, c.MaxKeyBytes)
		if err != nil {
			return 0, ErrCorrupt
		}
		return index, nil
	}
	if k[0] < appRootTag || k[0] > appOutcomeTag || len(k) != 9 {
		return 0, ErrCorrupt
	}
	index := binary.BigEndian.Uint64(k[1:])
	if index == 0 || index == ^uint64(0) {
		return 0, ErrCorrupt
	}
	return index, nil
}
func (e *ApplicationExport) nextPublished(ctx context.Context, b ReadBudget) (ApplicationSnapshotChunk, error) {
	p := e.published
	if !p.ready {
		return ApplicationSnapshotChunk{}, ErrInvalid
	}
	if e.sequence >= e.s.meta.Gen.Publication.Limits.MaxTransferChunks {
		return ApplicationSnapshotChunk{}, ErrLimit
	}
	chunk, _, err := e.publishedPage(ctx, b, false)
	if err != nil {
		if publishedExportFailure(err) {
			e.s.poison = err
		}
		return ApplicationSnapshotChunk{}, err
	}
	e.after = copyApplicationBytes(chunk.After)
	e.sequence++
	e.final = chunk.Final
	return chunk, nil
}

// validateChunkHeader checks transport progress claims as finite bounds only.
// Completion is separately proved by ordered emitted frames, all envelope runs,
// exact ledgers and the manifest digest; skipped source records are not trusted.
func (i *ApplicationImport) validateChunkHeader(c ApplicationSnapshotChunk) error {
	if i.manifest.Version < 2 {
		if c.Version > 1 || c.CutID != [32]byte{} || c.ManifestID != [32]byte{} || len(c.After) != 0 || c.Visited != 0 || c.VisitedBytes != 0 || len(c.Data) == 0 && !c.Final {
			return ErrInvalid
		}
		return nil
	}
	p := i.s.meta.Gen.Publication.Limits
	l := i.s.meta.Transfer.Limits
	if !p.enabled() || c.Version != i.manifest.Version || c.CutID != i.manifest.CutID || c.ManifestID != i.id {
		return ErrInvalid
	}
	if i.sequence >= p.MaxTransferChunks {
		return ErrLimit
	}
	if len(c.After) > 2*i.manifest.Contract.MaxKeyBytes+11 || c.Visited > uint64(l.MaxChunkRows) || c.VisitedBytes > uint64(l.MaxChunkBytes) || snapshotChunkV2FixedBytes+len(c.Data)+len(c.After)+cap(i.after)+cap(i.state.last) > l.MaxChunkBytes { // #nosec G115 -- persisted local limits are validated positive and <= 4096 rows / 32MiB bytes.
		return ErrLimit
	}
	if c.Visited == 0 {
		if !c.Final || len(c.Data) != 0 || c.VisitedBytes != 0 || !bytes.Equal(c.After, i.after) {
			return ErrInvalid
		}
		return nil
	}
	if len(c.After) == 0 || bytes.Compare(c.After, i.after) <= 0 {
		return ErrInvalid
	}
	if _, err := snapshotCursorIndex(c.After, i.manifest.Contract); err != nil {
		return err
	}
	if c.VisitedBytes < uint64(8+len(c.After)+appFrameBytes) {
		return ErrInvalid
	}
	return nil
}

func publishedExportFailure(err error) bool {
	return err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, ErrLimit) && !errors.Is(err, ErrInvalid) && !errors.Is(err, ErrClosed)
}

// Protobuf containers need a capacity bound in addition to their wire size.
// Fixed overhead and twice the payload width conservatively cover cloned
// slice capacity; this is a logical bound, not allocator/RSS accounting.
func snapshotConfCapacity(c *pb.ConfState) int {
	return 256 + 16*(len(c.Voters)+len(c.VotersOutgoing)+len(c.Learners)+len(c.LearnersNext)) + 2*len(c.ProtoReflect().GetUnknown())
}
func (s *Store) loadPublishedImage() (image []byte, err error) {
	raw, closer, err := s.db.Get(snapshotKey)
	if err != nil {
		return nil, storedReadFailure(err)
	}
	defer func() { err = errors.Join(err, closer.Close()) }()
	if uint64(len(raw)) != s.meta.SnapBytes || len(raw) > s.meta.Transfer.Contract.MaxImageBytes || sha256.Sum256(raw) != s.meta.SnapHash {
		return nil, ErrCorrupt
	}
	return copyApplicationBytes(raw), nil
}

// Admission computes the small dormant descriptor size before any copy.
func importDescriptorBytes(i *ApplicationImport) int {
	n := 4 + 32 + 3*8 + 32 + 1
	if i.generation != 0 {
		n += 16
	}
	if i.manifest.Version >= 2 {
		n += 32 + 4 + len(i.after)
	}
	return n
}
