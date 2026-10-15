package raftlog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"

	"github.com/cockroachdb/pebble/v2"
)

const retentionCursorTag byte = 22

// ApplicationRetentionProgress reports local collection accounting, not a
// portable cut or graph-time watermark. Cursor rows never enter AS7 totals.
type ApplicationRetentionProgress struct {
	PlanID                      [32]byte
	Generation                  uint64
	SourceIndex, InstalledIndex uint64
	CursorBytes, CursorRecords  uint64
	Section                     byte
	Complete                    bool
}

type retentionCollectionCursor struct {
	logical []byte
	index   uint64
}

type retentionCollectionBudget struct {
	limit ReadBudget
	work  ApplicationProofWork
}

func (q *retentionCollectionBudget) spend(n int) error {
	if n < 0 || n > q.limit.Bytes-q.work.Bytes {
		return ErrLimit
	}
	q.work.Bytes += n
	return nil
}
func (q *retentionCollectionBudget) record() error {
	if q.work.Records >= q.limit.Rows {
		return ErrLimit
	}
	q.work.Records++
	return nil
}
func retentionCursorKey(bank byte) []byte { return []byte{retentionCursorTag + bank} }
func retentionCursorUsage(m metadata) (uint64, uint64) {
	if m.Retention.Cursor.Bytes == 0 {
		return 0, 0
	}
	return 1 + appFrameBytes + uint64(m.Retention.Cursor.Bytes), 1
}
func retentionProgress(m metadata) ApplicationRetentionProgress {
	b, n := retentionCursorUsage(m)
	return ApplicationRetentionProgress{PlanID: m.Retention.State.ID, Generation: m.Gen.Banks[m.Gen.Active].Generation, SourceIndex: m.Retention.State.SourceIndex, InstalledIndex: m.Retention.State.InstalledIndex, CursorBytes: b, CursorRecords: n, Section: m.Retention.Cursor.Section, Complete: m.Retention.Cursor.Section == 6}
}

// The portable installed plan and exact local bank/generation/section bind the
// separately accounted cursor. Its payload owns only a logical key/index, never
// a caller-supplied physical seek key. All hashing is admitted before use.
func retentionCursorDigest(m metadata, payload []byte) [32]byte {
	plan := retentionStateHash(m.Retention.State)
	h := sha256.New()
	_, _ = h.Write([]byte("rho-tkg:native-retention-cursor:v1\x00"))
	_, _ = h.Write(plan[:])
	_, _ = h.Write([]byte{m.Gen.Active, m.Retention.Cursor.Section})
	var generation [8]byte
	binary.BigEndian.PutUint64(generation[:], m.Gen.Banks[m.Gen.Active].Generation)
	_, _ = h.Write(generation[:])
	_, _ = h.Write(payload)
	var sum [32]byte
	copy(sum[:], h.Sum(nil))
	return sum
}
func encodeRetentionCursor(c retentionCollectionCursor) []byte {
	b := make([]byte, 0, 16+len(c.logical))
	b = append(b, 'G', 'R', 'C', 1)
	b = binary.BigEndian.AppendUint64(b, c.index)
	b = binary.BigEndian.AppendUint32(b, uint32(len(c.logical))) // #nosec G115 -- canonical key admitted <=64KiB before cursor ownership.
	return append(b, c.logical...)
}

// Store.mu is held. Reopen has no request ledger; lengths still use actual
// configured caps before frame/hash/decode. Collection supplies its SAME ledger.
func (s *Store) loadRetentionCursor(m metadata, q *retentionCollectionBudget) (out retentionCollectionCursor, err error) {
	c := m.Retention.Cursor
	if !m.Retention.Config.enabled() || c.Bytes > uint32(m.App.Policy.MaxKeyBytes+16) || c.Section > 6 || (c.Section == 0 || c.Section == 6) && c.Bytes != 0 {
		return out, ErrCorrupt
	}
	if q != nil {
		if err := q.record(); err != nil {
			return out, err
		}
		if err := q.spend(64); err != nil {
			return out, err
		}
	}
	key := retentionCursorKey(m.Gen.Active)
	raw, closer, err := s.db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		if c.Bytes != 0 || c.Hash != ([32]byte{}) {
			return out, ErrCorrupt
		}
		return out, nil
	}
	if err != nil {
		s.poison = storedReadFailure(err)
		return out, s.poison
	}
	defer func() {
		if closeErr := closer.Close(); closeErr != nil {
			s.poison = closeErr
			err = errors.Join(err, closeErr)
		}
	}()
	if c.Bytes == 0 || len(raw) != appFrameBytes+int(c.Bytes) {
		return out, ErrCorrupt
	}
	if q != nil {
		if err := q.spend(3*len(raw) + int(c.Bytes)); err != nil {
			return out, err
		}
	}
	payload, deleted, err := inspectAppFrame(key, raw, m.App.Policy.MaxKeyBytes+16)
	if err != nil || deleted || len(payload) < 16 || !bytes.Equal(payload[:4], []byte{'G', 'R', 'C', 1}) || retentionCursorDigest(m, payload) != c.Hash {
		return out, ErrCorrupt
	}
	out.index = binary.BigEndian.Uint64(payload[4:12])
	n := uint64(binary.BigEndian.Uint32(payload[12:16]))
	if n > uint64(m.App.Policy.MaxKeyBytes) || n != uint64(len(payload)-16) || out.index == 0 || out.index > m.Applied || c.Section == 1 && n == 0 || c.Section != 1 && n != 0 {
		return retentionCollectionCursor{}, ErrCorrupt
	}
	if n != 0 {
		out.logical = copyApplicationBytes(payload[16:])
	}
	return out, nil
}

// Only the active generation can own local progress. Retired/staging views do
// not use it; activation and bank clearing remove it atomically. Check absence
// without hashing or copying any unexpected inactive payload.
func (s *Store) checkInactiveRetentionCursor(m metadata) (err error) {
	_, closer, err := s.db.Get(retentionCursorKey(1 - m.Gen.Active))
	if errors.Is(err, pebble.ErrNotFound) {
		return nil
	}
	if err != nil {
		return storedReadFailure(err)
	}
	return errors.Join(ErrCorrupt, closer.Close())
}

// Collection protects EVERY registered external owner. The publication mutex
// excludes its internal capture across the whole delete/metadata sync. No count
// adjustment or randomly selected view can create an internal exemption.
func (s *Store) retentionCollectionOwnersLocked() error {
	if s.applicationViews != nil || len(s.applicationExports) != 0 || s.applicationImport != nil || s.applicationVerifier || len(s.applicationSnapshotClaims) != 0 || s.readyProgress != nil {
		return ErrLimit
	}
	return nil
}
func retentionSectionTag(bank, section byte) byte {
	if section == 5 {
		return recoveryRootTag + bank
	}
	return bankTag(bank, appDataTag+section-1)
}

// keepRetentionData authenticates the genuine predecessor at DataFloor and
// every explicit Data anchor. The oldest key in a version run is never guessed
// to be an anchor. Queries share the original cumulative collector ledger.
func (s *Store) keepRetentionData(it *pebble.Iterator, logical []byte, index uint64, q *retentionCollectionBudget) (bool, error) {
	r := s.meta.Retention.State
	if index >= r.DataFloor {
		return true, nil
	}
	probe := func(cut uint64) (bool, error) {
		if err := q.spend(4*(2*len(logical)+11) + 64); err != nil {
			return false, err
		}
		if err := q.record(); err != nil {
			return false, err
		}
		if !it.SeekGE(bankVersionKey(s.activeBank(), logical, cut)) {
			return false, it.Error()
		}
		physical := it.Key()
		if len(physical) > 2*s.meta.App.Policy.MaxKeyBytes+11 {
			return false, ErrCorrupt
		}
		if err := q.spend(3 * len(physical)); err != nil {
			return false, err
		}
		key, candidate, err := decodeBankAppKey(s.activeBank(), physical, s.meta.App.Policy.MaxKeyBytes)
		if err != nil {
			return false, err
		}
		if !bytes.Equal(key, logical) {
			return false, nil // authenticated absence at this earlier cut
		}
		if candidate > cut {
			return false, ErrCorrupt
		}
		if candidate == index {
			return true, nil // same immutable frame was authenticated by the main pass
		}
		raw, err := it.ValueAndErr()
		if err != nil {
			return false, storedReadFailure(err)
		}
		if len(raw) > s.meta.App.Policy.MaxValueBytes+appFrameBytes {
			return false, ErrCorrupt
		}
		if err := q.spend(len(raw) + len(physical)); err != nil {
			return false, err
		}
		if _, _, err := inspectAppFrame(physical, raw, s.meta.App.Policy.MaxValueBytes); err != nil {
			return false, err
		}
		return false, nil
	}
	if keep, err := probe(r.DataFloor); keep || err != nil {
		return keep, err
	}
	for _, root := range r.Roots {
		if root.Data && root.Index < r.DataFloor {
			if keep, err := probe(root.Index); keep || err != nil {
				return keep, err
			}
		}
	}
	return false, nil
}
func (s *Store) keepRetentionEnvelope(section byte, index uint64) bool {
	r := s.meta.Retention.State
	switch section {
	case 2:
		if index >= r.RootFloor || index == r.SourceIndex {
			return true
		}
		for _, root := range r.Roots {
			if root.Index == index {
				return true
			}
		}
	case 3:
		return index >= r.FeedFloor
	case 4:
		return index >= r.OutcomeFloor
	case 5:
		if index >= r.RecoveryFloor {
			return true
		}
		// An actual queued duplicate may still require its exact prior native
		// reservation root. It cannot borrow a newer owner's root after GC.
		for _, pending := range s.meta.Recovery.Queued {
			if pending.ReservationOriginIndex == index {
				return true
			}
		}
	}
	return false
}

// CollectApplicationRetention advances one bounded physical section/page of the
// ACTUAL installed plan. Rows+bytes include lookups, predecessor inspection,
// cursor ownership, metadata and staging. Bytes use existing MaxInstallBytes;
// Rows use existing MaxPageRows. Too-small budgets refuse without progress.
// Every page co-syncs authentic deletes, precise namespace debits and its local
// cursor. All immutable controls/originals are retained. Cancellation/corruption
// commits no partial page; earlier successful pages remain durable.
func (s *Store) CollectApplicationRetention(ctx context.Context, planID [32]byte, budget ReadBudget) (progress ApplicationRetentionProgress, work ApplicationProofWork, err error) {
	if s == nil || ctx == nil {
		return progress, work, ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return progress, work, err
	}
	s.publicationMu.Lock()
	defer s.publicationMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(); err != nil {
		return progress, work, err
	}
	m, p := s.meta, s.meta.App.Policy
	if !m.Retention.Config.enabled() || planID == ([32]byte{}) || planID != m.Retention.State.ID || budget.Rows < 1 || budget.Bytes < 1 {
		return progress, work, ErrInvalid
	}
	if budget.Rows > p.MaxPageRows || budget.Bytes > p.MaxInstallBytes {
		return progress, work, ErrLimit
	}
	if err := s.retentionCollectionOwnersLocked(); err != nil {
		return progress, work, err
	}
	q := retentionCollectionBudget{limit: budget}
	defer func() { work = q.work }()
	if err := q.spend(int(metadataBytes(m)) + 4*portableRetentionBytes(m.Retention.State) + 256); err != nil {
		return progress, work, err
	}
	cursor, err := s.loadRetentionCursor(m, &q)
	if err != nil {
		if !errors.Is(err, ErrLimit) {
			s.poison = err
		}
		return progress, work, err
	}
	if m.Retention.Cursor.Section == 6 {
		return retentionProgress(m), q.work, nil
	}
	section := max(byte(1), m.Retention.Cursor.Section)
	tag := retentionSectionTag(s.activeBank(), section)
	it, err := s.db.NewIter(&pebble.IterOptions{LowerBound: []byte{tag}, UpperBound: []byte{tag + 1}})
	if err != nil {
		s.poison = err
		return progress, work, err
	}
	defer func() {
		if it != nil {
			if closeErr := it.Close(); closeErr != nil {
				s.poison = closeErr
				err = errors.Join(err, closeErr)
			}
		}
	}()
	var predecessor *pebble.Iterator
	if section == 1 {
		predecessor, err = s.db.NewIter(&pebble.IterOptions{LowerBound: []byte{tag}, UpperBound: []byte{tag + 1}})
		if err != nil {
			s.poison = err
			return progress, work, err
		}
		defer func() {
			if predecessor != nil {
				if closeErr := predecessor.Close(); closeErr != nil {
					s.poison = closeErr
					err = errors.Join(err, closeErr)
				}
			}
		}()
	}
	// Cursor buffers and their final owned encoding coexist with the batch.
	cursorAllowance := 5*(16+len(cursor.logical)) + 128
	if err := q.spend(cursorAllowance); err != nil {
		return progress, work, err
	}
	valid := it.First()
	if cursor.index != 0 {
		key := bankIndexKey(s.activeBank(), appDataTag+section-1, cursor.index)
		if section == 1 {
			key = bankVersionKey(s.activeBank(), cursor.logical, cursor.index)
		} else if section == 5 {
			key = appIndexKey(recoveryRootTag+s.activeBank(), cursor.index)
		}
		valid = it.SeekGE(key)
		if valid && bytes.Equal(it.Key(), key) {
			valid = it.Next()
		}
	}
	batch := s.db.NewBatch()
	defer func() {
		if batch != nil {
			if closeErr := batch.Close(); closeErr != nil {
				s.poison = closeErr
				err = errors.Join(err, closeErr)
			}
		}
	}()
	processed, deletedBytes, deletedRows := 0, uint64(0), uint64(0)
	retirePublication := false
	for valid {
		if err := ctx.Err(); err != nil {
			return progress, work, err
		}
		if e := q.record(); e != nil {
			break
		}
		physical := it.Key()
		raw, readErr := it.ValueAndErr()
		if readErr != nil {
			s.poison = storedReadFailure(readErr)
			return progress, work, s.poison
		}
		limit := p.MaxValueBytes
		switch section {
		case 2:
			limit = p.MaxImageBytes
		case 3:
			limit = p.MaxChangeBytes
		case 4:
			limit = p.MaxOutcomeBytes
		case 5:
			limit = min(p.MaxValueBytes, maxMetadataBytes)
		}
		if len(physical) > 2*p.MaxKeyBytes+11 || len(raw) > appFrameBytes+limit {
			s.poison = ErrCorrupt
			return progress, work, ErrCorrupt
		}
		if e := q.spend(6*len(physical) + len(raw) + 128); e != nil {
			break
		}
		var logical []byte
		var index uint64
		if section == 1 {
			logical, index, err = decodeBankAppKey(s.activeBank(), physical, p.MaxKeyBytes)
		} else if len(physical) == 9 {
			index = binary.BigEndian.Uint64(physical[1:])
		} else {
			err = ErrCorrupt
		}
		if err != nil || index == 0 || index > m.Applied {
			s.poison = ErrCorrupt
			return progress, work, ErrCorrupt
		}
		payload, deleted, e := inspectAppFrame(physical, raw, limit)
		if e != nil || deleted && section != 1 {
			s.poison = ErrCorrupt
			return progress, work, ErrCorrupt
		}
		keep := s.keepRetentionEnvelope(section, index)
		if section == 1 {
			keep, e = s.keepRetentionData(predecessor, logical, index, &q)
		} else if section == 2 {
			if index == m.Retention.State.SourceIndex {
				e = q.spend(len(payload))
				if e == nil && sha256.Sum256(payload) != m.Retention.State.SourceHash {
					e = ErrCorrupt
				}
			}
			for _, root := range m.Retention.State.Roots {
				if root.Index == index && e == nil {
					e = q.spend(len(payload))
					if e == nil && sha256.Sum256(payload) != root.ImageHash {
						e = ErrCorrupt
					}
				}
			}
		} else if section == 5 {
			if e = q.spend(8 * len(payload)); e == nil {
				var reservation ApplicationRecoveryReservation
				var bank applicationTerminalBank
				reservation, bank, e = readNativeRecoveryRoot(payload, p, s.limits)
				if e == nil && (payload[2] != 2 || index == m.Applied && (!sameRecoveryReservation(reservation, m.Recovery.Reservation) || !sameTerminalBank(bank, m.Recovery.Terminal))) {
					e = ErrCorrupt
				}
			}
		}
		if errors.Is(e, ErrLimit) {
			break
		}
		if e != nil {
			s.poison = e
			return progress, work, e
		}
		neededCursor := 5*(16+len(logical)) + 128
		if neededCursor > cursorAllowance {
			if e := q.spend(neededCursor - cursorAllowance); e != nil {
				break
			}
			cursorAllowance = neededCursor
		}
		if !keep {
			if e := batch.Delete(physical, nil); e != nil {
				return progress, work, e
			}
			deletedBytes += uint64(len(physical) + len(raw))
			deletedRows++
			retirePublication = retirePublication || index <= m.Base && m.Gen.Publication.Bank == m.Gen.Active && m.Gen.Publication.Generation == m.Gen.Banks[m.Gen.Active].Generation
		}
		cursor = retentionCollectionCursor{logical: copyApplicationBytes(logical), index: index}
		processed++
		valid = it.Next()
	}
	if err := it.Error(); err != nil {
		s.poison = err
		return progress, work, err
	}
	if valid && processed == 0 {
		return progress, work, ErrLimit
	}
	closeErr := it.Close()
	it = nil
	if predecessor != nil {
		closeErr = errors.Join(closeErr, predecessor.Close())
		predecessor = nil
	}
	if closeErr != nil {
		s.poison = closeErr
		return progress, work, closeErr
	}
	if section == 5 {
		if deletedBytes > m.Recovery.Bytes || deletedRows > m.Recovery.Records {
			s.poison = ErrCorrupt
			return progress, work, ErrCorrupt
		}
		m.Recovery.Bytes -= deletedBytes
		m.Recovery.Records -= deletedRows
	} else {
		if deletedBytes > m.App.Bytes || deletedRows > m.App.Records {
			s.poison = ErrCorrupt
			return progress, work, ErrCorrupt
		}
		m.App.Bytes -= deletedBytes
		m.App.Records -= deletedRows
	}
	if retirePublication && m.Gen.Publication.Valid {
		m.Gen.Publication.Retired = true
	}
	m.Retention.Cursor = applicationRetentionCursor{Section: section}
	if !valid {
		m.Retention.Cursor.Section++
		cursor = retentionCollectionCursor{}
	}
	key := retentionCursorKey(m.Gen.Active)
	if cursor.index == 0 {
		if err := batch.Delete(key, nil); err != nil {
			return progress, work, err
		}
	} else {
		payload := encodeRetentionCursor(cursor)
		m.Retention.Cursor.Bytes = uint32(len(payload)) // #nosec G115 -- logical cursor <=MaxKeyBytes+16<=64KiB+16.
		m.Retention.Cursor.Hash = retentionCursorDigest(m, payload)
		if err := batch.Set(key, appFrame(key, payload, false), nil); err != nil {
			return progress, work, err
		}
	}
	syncActiveGeneration(&m)
	if err := s.validate(m); err != nil {
		return progress, work, err
	}
	if err := ctx.Err(); err != nil {
		return progress, work, err
	}
	if s.stateObserver != nil {
		identity := stateIdentity(m)
		identity.SourceIndex, identity.PlanID = m.Retention.State.SourceIndex, m.Retention.State.ID
		identity.SourceImageSHA256 = m.Retention.State.SourceHash
		identity.ProgressSHA256, identity.Section = m.Retention.Cursor.Hash, m.Retention.Cursor.Section
		identity.Final = m.Retention.Cursor.Section == 6
		err = s.commitState(m, batch, StableStateRetentionPage, identity)
	} else {
		err = s.commit(m, batch)
	}
	batch = nil // commit owns and closes the batch, including failures
	if err != nil {
		return progress, work, err
	}
	return retentionProgress(m), q.work, nil
}

// Fresh sparse scrub uses the native canonical frame verifier, not a graph or
// protocol acceptance model. Per-namespace ceilings below are the actual total
// graph ledger; only the final aggregate is authoritative until scanned. Space
// remains bounded by one configured frame/key and the exact current image.
func (s *Store) scrubRetainedApplicationLocked(ctx context.Context) (err error) {
	defer func() {
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			s.poison = err
		}
	}()
	m, p := s.meta, s.meta.App.Policy
	if _, err := s.loadRetentionCursor(m, nil); err != nil {
		return err
	}
	if err := s.checkInactiveRetentionCursor(m); err != nil {
		return err
	}
	if m.Applied == 0 {
		return nil
	}
	image, err := s.loadImage(imageKey, m.ImageBytes, m.ImageHash)
	if err != nil {
		return err
	}
	manifest := ApplicationSnapshotManifest{Version: 7, Index: m.Applied, Contract: m.Transfer.Contract, Image: image, Retention: m.Retention.State,
		ControlBytes: m.Controls.Bytes, ControlRecords: m.Controls.Records, RecoveryBytes: m.Recovery.Bytes, RecoveryRecords: m.Recovery.Records,
		RecoveryReservation: m.Recovery.Reservation, TerminalReservations: []ApplicationRecoveryReservation(m.Recovery.Terminal)}
	for i := range manifest.NamespaceBytes {
		manifest.NamespaceBytes[i], manifest.NamespaceRecords[i] = m.App.Bytes, m.App.Records
	}
	it, err := newApplicationIterator(s.db.NewIter, s.activeBank(), true, true)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := it.Close(); closeErr != nil {
			s.poison = closeErr // cancellation cannot disguise a backend close failure
			err = errors.Join(err, closeErr)
		}
	}()
	var state snapshotVerifier
	for valid := it.First(); valid; valid = it.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		key := it.Key()
		raw, readErr := it.ValueAndErr()
		if readErr != nil {
			return storedReadFailure(readErr)
		}
		if len(key) == 0 || len(key) > 2*p.MaxKeyBytes+11 {
			return ErrCorrupt
		}
		limit := p.MaxValueBytes
		switch key[0] {
		case bankTag(s.activeBank(), appRootTag):
			limit = p.MaxImageBytes
		case bankTag(s.activeBank(), appChangeTag):
			limit = p.MaxChangeBytes
		case bankTag(s.activeBank(), appOutcomeTag):
			limit = p.MaxOutcomeBytes
		case recoveryRootTag + s.activeBank():
			limit = min(p.MaxValueBytes, maxMetadataBytes)
		}
		// Inspect physical-key CRC before reconstructing the bounded canonical
		// portable frame, whose CRC also binds its bank-independent key.
		data, deleted, err := inspectAppFrame(key, raw, limit)
		if err != nil {
			return err
		}
		if key[0] == recoveryRootTag+s.activeBank() {
			if _, _, err := readNativeRecoveryRoot(data, p, s.limits); err != nil {
				return err
			}
		}
		canonical := canonicalApplicationKey(s.activeBank(), key)
		state, err = state.accept(manifest, canonical, appFrame(canonical, data, deleted))
		if err != nil {
			return err
		}
	}
	if err := it.Error(); err != nil {
		return err
	}
	var graphBytes, graphRows uint64
	for i := range state.namespaceBytes {
		graphBytes += state.namespaceBytes[i]
		graphRows += state.namespaceRows[i]
	}
	if graphBytes != m.App.Bytes || graphRows != m.App.Records || state.controlBytes != m.Controls.Bytes || state.controlRows != m.Controls.Records || state.recoveryBytes != m.Recovery.Bytes || state.recoveryRows != m.Recovery.Records {
		return ErrCorrupt
	}
	return state.completeRetentionRuns(manifest)
}
