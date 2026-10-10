package raftlog

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"

	"github.com/cockroachdb/pebble/v2"
)

// ApplicationImport owns the sole inactive application bank. Verified contents
// are dormant; no method can activate them. Abort revokes the session; a
// generation-mode verifier keeps its captured bank until its final reference
// is released. Reopen removes only the inactive bank after validating metadata.
// Append uses at most one bounded chunk batch and one bounded frame lookahead;
// transferred values are reframed for local keys. Network validation/cancellation
// never poisons active data. Uncertain local durable failures remain fail-stop.
type ApplicationImport struct {
	prepared                *PreparedApplicationSnapshot
	after                   []byte
	bank                    byte
	generation              uint64
	ref                     *generationRef
	s                       *Store
	manifest                ApplicationSnapshotManifest
	id                      [32]byte
	state                   snapshotVerifier
	sequence                uint64
	final, verified, closed bool
	verifying               bool
	verifyPageHook          func() // private deterministic concurrency-test seam, set before Verify
}

// BeginApplicationImport validates identity/contract/quotas before copying or
// staging. One existing dormant session backpressures even when verified. Sender
// LocalVoter, read-view quotas and local policies are intentionally absent.
func (s *Store) BeginApplicationImport(ctx context.Context, m ApplicationSnapshotManifest) (*ApplicationImport, error) {
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
	tc := s.meta.Transfer
	if m.SemanticContractID != s.meta.Rep.SemanticContractID {
		return nil, ErrInvalid
	}
	if !tc.enabled() || m.Identity != tc.Identity || m.Contract != tc.Contract {
		return nil, ErrInvalid
	}
	if s.meta.Rep.Config.enabled() && !s.meta.Rep.Config.matches(m.ConfState) {
		return nil, ErrInvalid
	}
	if m.Version >= 2 && !s.meta.Gen.Publication.Limits.enabled() {
		return nil, ErrInvalid
	}
	if s.applicationImport != nil {
		return nil, ErrLimit
	}
	if err := validateManifest(m); err != nil {
		return nil, err
	}
	b, r, err := snapshotTotals(m)
	if err != nil {
		return nil, err
	}
	if b > tc.Limits.MaxStagedBytes || r > tc.Limits.MaxStagedRecords || b > s.meta.App.Policy.RetainedApplicationBytes || r > s.meta.App.Policy.RetainedApplicationRecords {
		return nil, ErrLimit
	}
	prospective := s.meta
	bank := byte(1)
	if s.meta.Gen.Limits.enabled() {
		var err error
		prospective, bank, err = s.reserveGeneration(m)
		if err != nil {
			return nil, err
		}
	}
	id, err := manifestID(m)
	if err != nil {
		return nil, err
	}
	i := &ApplicationImport{bank: bank, generation: prospective.Gen.Banks[bank].Generation, s: s, manifest: cloneSnapshotManifest(m), id: id, state: snapshotVerifier{digest: snapshotSeed(m)}}
	batch := s.db.NewBatch()
	if err := batch.Set(dormantKey, i.descriptor(), nil); err != nil {
		return nil, errors.Join(err, batch.Close())
	}
	if err := s.commit(prospective, batch); err != nil {
		return nil, err
	}
	ref, err := s.pinGeneration(bank)
	if err != nil {
		s.poison = err
		return nil, err
	}
	i.ref = ref
	s.applicationImport = i
	return i, nil
}
func (i *ApplicationImport) descriptor() []byte {
	b := append([]byte{'A', 'I', 1, 0}, i.id[:]...)
	if i.generation != 0 {
		b[2] = 2
		b = binary.BigEndian.AppendUint64(b, uint64(i.bank))
		b = binary.BigEndian.AppendUint64(b, i.generation)
	}
	for _, n := range []uint64{i.state.bytes, i.state.rows, i.sequence} {
		b = binary.BigEndian.AppendUint64(b, n)
	}
	b = append(b, i.state.digest[:]...)
	var flags byte
	if i.final {
		flags |= 1
	}
	if i.verified {
		flags |= 2
	}
	if i.manifest.Version >= 2 {
		b[2] = 3
		b = append(b, i.manifest.CutID[:]...)
		b = binary.BigEndian.AppendUint32(b, uint32(len(i.after))) // #nosec G115 -- AS2 cursor length is contract-bounded before admission.
		b = append(b, i.after...)
	}
	return append(b, flags)
}
func (i *ApplicationImport) check(ctx context.Context) error {
	if i.closed {
		return ErrClosed
	}
	if err := snapshotContext(ctx); err != nil {
		return err
	}
	if i.s.meta.Gen.Limits.enabled() && (i.s.applicationImport != i || i.bank == i.s.activeBank() || i.s.meta.Gen.Banks[i.bank].Generation != i.generation || i.s.meta.Gen.Banks[i.bank].State != bankStaging) {
		return ErrClosed
	}
	return i.s.check()
}

// Append validates the entire chunk and all cumulative ledgers before any write.
// Bad order, duplicate sequence, truncation, digest errors or post-final input
// leave the session cursor and active store usable; the caller may retry or Abort.
// The caller must keep chunk bytes unchanged for the duration of Append.
func (i *ApplicationImport) Append(ctx context.Context, c ApplicationSnapshotChunk) error {
	if i == nil {
		return ErrInvalid
	}
	s := i.s
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := i.check(ctx); err != nil {
		return err
	}
	if i.final || c.Sequence != i.sequence {
		return ErrInvalid
	}
	if err := i.validateChunkHeader(c); err != nil {
		return err
	}
	l := s.meta.Transfer.Limits
	if len(c.Data) > l.MaxChunkBytes {
		return ErrLimit
	}
	state := i.state
	rows := 0
	err := walkSnapshotChunk(c.Data, i.manifest.Contract, func(k, v []byte) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		rows++
		if i.manifest.Version >= 2 && (bytes.Compare(k, i.after) <= 0 || bytes.Compare(k, c.After) > 0) {
			return ErrInvalid
		}
		if rows > l.MaxChunkRows {
			return ErrLimit
		}
		var err error
		state, err = state.accept(i.manifest, k, v)
		if err != nil {
			return err
		}
		if state.bytes > l.MaxStagedBytes || state.rows > l.MaxStagedRecords {
			return ErrLimit
		}
		return nil
	})
	if err != nil {
		return err
	}
	if i.manifest.Version >= 2 && (c.Visited < uint64(rows) || c.VisitedBytes < uint64(len(c.Data))) {
		return ErrInvalid
	}
	if c.Final {
		if err := state.complete(i.manifest); err != nil {
			return err
		}
	}
	if i.sequence == ^uint64(0) {
		return ErrLimit
	}
	next := *i
	next.state = state
	next.sequence++
	next.final = c.Final
	if i.manifest.Version >= 2 {
		next.after = copyApplicationBytes(c.After)
	}
	prospective := s.meta
	if prospective.Gen.Limits.enabled() {
		bank := &prospective.Gen.Banks[i.bank]
		bank.Bytes, bank.Records = state.bytes, state.rows
		if err := s.validateGenerationMeta(prospective); err != nil {
			return err
		}
	}
	batch := s.db.NewBatch()
	fail := func(e error) error { return errors.Join(e, batch.Close()) }
	err = walkSnapshotChunk(c.Data, i.manifest.Contract, func(k, v []byte) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		local := copyApplicationBytes(k)
		local[0] += i.bank * 4
		// accept already verified the canonical frame; local physical key hashes
		// are reconstructed rather than copied from the sender.
		value, deleted, err := inspectAppFrame(k, v, len(v)-appFrameBytes)
		if err != nil {
			return err
		}
		return batch.Set(local, appFrame(local, value, deleted), nil)
	})
	if err != nil {
		return fail(err)
	}
	if err := batch.Set(dormantKey, next.descriptor(), nil); err != nil {
		return fail(err)
	}
	if err := s.commit(prospective, batch); err != nil {
		return err
	}
	*i = next
	return nil
}

// Verify re-reads all staged frames in bounded resident space and seals only a
// complete exact stream. One verifier per Store is admitted, including aborted/replaced sessions; each lock hold
// covers one row/byte-bounded page, with one frame lookahead. It grants no
// publication authority. A corrupted dormant
// bank leaves the active generation usable and can be removed with Abort.
func (i *ApplicationImport) Verify(ctx context.Context) (err error) {
	if i == nil {
		return ErrInvalid
	}
	s := i.s
	s.mu.Lock()
	if err := i.check(ctx); err != nil {
		s.mu.Unlock()
		return err
	}
	if !i.final {
		s.mu.Unlock()
		return ErrInvalid
	}
	if i.verified {
		s.mu.Unlock()
		return nil
	}
	if i.verifying || s.applicationVerifier {
		s.mu.Unlock()
		return ErrLimit
	}
	ref, pinErr := s.pinGeneration(i.bank)
	if pinErr != nil {
		s.mu.Unlock()
		return pinErr
	}
	i.verifying = true
	s.applicationVerifier = true
	s.applicationVerifierImageBytes = len(i.manifest.Image)
	defer func() {
		s.mu.Lock()
		i.verifying = false
		s.applicationVerifier = false
		s.applicationVerifierImageBytes = 0
		err = errors.Join(err, s.releaseGeneration(ref))
		s.mu.Unlock()
	}()
	m, hook := i.manifest, i.verifyPageHook
	s.mu.Unlock()
	state := snapshotVerifier{digest: snapshotSeed(m)}
	for {
		s.mu.Lock()
		if err := i.check(ctx); err != nil {
			s.mu.Unlock()
			return err
		}
		if s.applicationImport != i {
			s.mu.Unlock()
			return ErrClosed
		}
		next, complete, err := i.verifyPage(ctx, m, state)
		s.mu.Unlock()
		if err != nil {
			return err
		}
		state = next
		if hook != nil {
			hook()
		}
		if complete {
			break
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := i.check(ctx); err != nil {
		return err
	}
	if s.applicationImport != i {
		return ErrClosed
	}
	if err := state.complete(m); err != nil {
		return err
	}
	if i.verified {
		return nil
	}
	next := *i
	next.verified = true
	batch := s.db.NewBatch()
	if err := batch.Set(dormantKey, next.descriptor(), nil); err != nil {
		return errors.Join(err, batch.Close())
	}
	if err := s.commit(s.meta, batch); err != nil {
		return err
	}
	*i = next
	return nil
}

// verifyPage holds the store lock for at most one configured row/byte page.
// Every iterator is closed before the lock is released. Final input is immutable;
// the next page/final seal rechecks this exact live session after Abort or Close.
func (i *ApplicationImport) verifyPage(ctx context.Context, m ApplicationSnapshotManifest, state snapshotVerifier) (next snapshotVerifier, complete bool, err error) {
	s := i.s
	l := s.meta.Transfer.Limits
	it, err := s.db.NewIter(&pebble.IterOptions{LowerBound: []byte{bankTag(i.bank, appDataTag)}, UpperBound: []byte{bankTag(i.bank, appOutcomeTag) + 1}})
	if err != nil {
		return state, false, err
	}
	defer func() { err = errors.Join(err, it.Close()) }()
	valid := it.First()
	if len(state.last) > 0 {
		after := copyApplicationBytes(state.last)
		after[0] += i.bank * 4
		valid = it.SeekGE(after)
		if valid && bytes.Equal(it.Key(), after) {
			valid = it.Next()
		}
	}
	rows, work := 0, 0
	for valid {
		if err := ctx.Err(); err != nil {
			return state, false, err
		}
		local, raw := it.Key(), it.Value()
		if len(local) > 2*m.Contract.MaxKeyBytes+11 || len(raw) > max(m.Contract.MaxValueBytes, m.Contract.MaxImageBytes, m.Contract.MaxChangeBytes, m.Contract.MaxOutcomeBytes)+appFrameBytes {
			return state, false, ErrCorrupt
		}
		cost := len(local) + len(raw) + 8
		if rows >= l.MaxChunkRows || cost > l.MaxChunkBytes-work {
			if rows == 0 {
				return state, false, ErrLimit
			}
			return state, false, nil
		}
		value, deleted, err := inspectAppFrame(local, raw, len(raw)-appFrameBytes)
		if err != nil {
			return state, false, err
		}
		k := copyApplicationBytes(local)
		k[0] -= i.bank * 4
		canonical := appFrame(k, value, deleted)
		state, err = state.accept(m, k, canonical)
		if err != nil {
			return state, false, err
		}
		rows++
		work += cost
		valid = it.Next()
	}
	if err := it.Error(); err != nil {
		return state, false, err
	}
	return state, true, nil
}

// Status returns fixed-sized progress for the dormant session.
func (i *ApplicationImport) Status() (ApplicationImportStatus, error) {
	if i == nil {
		return ApplicationImportStatus{}, ErrInvalid
	}
	i.s.mu.Lock()
	defer i.s.mu.Unlock()
	if err := i.check(context.Background()); err != nil {
		return ApplicationImportStatus{}, err
	}
	return ApplicationImportStatus{i.id, i.state.bytes, i.state.rows, i.sequence, i.final, i.verified}, nil
}

// Abort synchronously revokes only the dormant session. Generation-mode bank
// deletion waits for captured verifier references; active keys are never removed.
// It is safe to repeat, including after Store.Close; it never deletes active keys.
func (i *ApplicationImport) Abort() error {
	if i == nil {
		return ErrInvalid
	}
	s := i.s
	s.mu.Lock()
	defer s.mu.Unlock()
	return i.abortLocked()
}
func (i *ApplicationImport) abortLocked() error {
	s := i.s
	if i.closed {
		return nil
	}
	if i.prepared != nil && i.prepared.claim != nil {
		return ErrLimit
	}
	if s.closed {
		i.closed = true
		i.manifest = ApplicationSnapshotManifest{}
		i.after = nil
		i.state.last = nil
		return nil
	}
	if err := s.check(); err != nil {
		return err
	}
	if s.meta.Gen.Limits.enabled() {
		if err := s.retireImport(i); err != nil {
			return err
		}
	} else if err := s.cleanupApplicationImport(); err != nil {
		return err
	}
	i.closed = true
	if i.prepared != nil {
		s.releasePreparedOwnership(i.prepared)
		i.prepared = nil
	}
	i.manifest = ApplicationSnapshotManifest{}
	i.state.last = nil
	i.after = nil
	s.applicationImport = nil
	ref := i.ref
	i.ref = nil
	return s.releaseGeneration(ref)
}
func (s *Store) cleanupApplicationImport() error {
	if s.meta.Gen.Limits.enabled() {
		bank := byte(1) - s.activeBank()
		if holdsPublishedGeneration(s.meta, bank, s.meta.Gen.Banks[bank].Generation) {
			batch := s.db.NewBatch()
			if err := batch.Delete(dormantKey, nil); err != nil {
				return errors.Join(err, batch.Close())
			}
			return s.commit(s.meta, batch)
		}
		return s.clearGenerationBank(bank, s.meta.Gen.Banks[bank].Generation)
	}
	batch := s.db.NewBatch()
	if err := batch.DeleteRange([]byte{dormantDataTag}, dormantEnd, nil); err != nil {
		return errors.Join(err, batch.Close())
	}
	return s.commit(s.meta, batch)
}
