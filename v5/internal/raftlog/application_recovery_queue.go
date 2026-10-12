package raftlog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math"

	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

// Queued recipes are local recovery metadata, not graph authority. They bind the
// exact typed intent issued by the selected verifier to its immutable log entry.
// The existing metadata frame bounds their total backing; no resident map grows
// with graph records. Applied ownership stays separate until Install commits.
type applicationRecoveryQueued struct {
	Index                  uint64
	ReservationOriginIndex uint64
	CommandDigest          [32]byte
	Purpose                ApplicationRecoveryPurpose
	Credit                 ApplicationRecoveryCredit
}

// Projection replays only pending recipes, in log order, from applied ownership.
// A queued terminal command converts its future credit into pending-install
// headroom; it does not release that capacity to unrelated traffic.
func recoveryProjected(m metadata, l Limits, pending uint64) (applicationRecoveryGrowth, uint64, uint64, error) {
	state := m.Recovery
	state.Queued = nil
	p := m.App.Policy
	actualPending := m.Last - m.Applied
	if pending < actualPending {
		return applicationRecoveryGrowth{}, 0, 0, ErrInvalid
	}
	// Each unclassified entry keeps the existing worst-case installation charge.
	bytes, records := uint64(0), uint64(0)
	if p.MaxInstallBytes < 1 || p.MaxInstallWrites < 1 {
		return applicationRecoveryGrowth{}, 0, 0, ErrInvalid
	}
	if pending > math.MaxUint64/unsignedLimit(p.MaxInstallBytes) || pending > math.MaxUint64/applicationInstallRecords(m) {
		return applicationRecoveryGrowth{}, 0, 0, ErrLimit
	}
	bytes = pending * unsignedLimit(p.MaxInstallBytes)
	records = pending * applicationInstallRecords(m)
	previous := m.Applied
	for _, q := range m.Recovery.Queued {
		if q.Index <= previous || q.Index > m.Last || q.CommandDigest == ([32]byte{}) || q.Purpose.Reserve != nil && q.Purpose.Use != nil || q.Purpose.Reserve == nil && q.Purpose.Use == nil {
			return applicationRecoveryGrowth{}, 0, 0, ErrInvalid
		}
		previous = q.Index
		var credit ApplicationRecoveryCredit
		var err error
		state, credit, err = advanceRecoveryQueued(state, q, p, l)
		if err != nil {
			return applicationRecoveryGrowth{}, 0, 0, err
		}
		if q.Purpose.Use != nil {
			bytes -= unsignedLimit(p.MaxInstallBytes) - min(unsignedLimit(p.MaxInstallBytes), credit.RetainedBytes)
			records -= applicationInstallRecords(m) - min(applicationInstallRecords(m), credit.RetainedRecords)
		}
	}
	var protected applicationRecoveryGrowth
	var err error
	if state.Reservation.ID != ([32]byte{}) {
		protected, err = recoveryGrowth(state.Reservation, p, l)
	}
	return protected, bytes, records, err
}

type applicationRecoveryReady struct {
	Applied, Last, Generation uint64
	ImageHash, LastHash       [32]byte
	Queued                    []applicationRecoveryQueued
}

func (r *applicationRecoveryReady) matches(s *Store) bool {
	return r != nil && r.Applied == s.meta.Applied && r.Last == s.meta.Last && r.Generation == s.activeGeneration() && r.ImageHash == s.meta.ImageHash && r.LastHash == s.meta.LastHash
}

// Classify outside Store's mutex: a verifier may read the actual registered
// immutable application view without inverting lock order. Recheck the captured
// log/application fence before any metadata or batch mutation.
// Existing Persist callers retain the exact ordinary refusal/error contract.
func (s *Store) classifyRecoveryReady(entries []*pb.Entry) (*applicationRecoveryReady, error) {
	ready, _, err := s.classifyRecoveryReadyOutcome(entries)
	return ready, err
}

// A private captured receipt exists only for the pre-verifier queue-allocation
// guard. No callback, application view or IO has run on this path. Ordinary
// Persist still refuses; bounded progress may classify actual fitting prefixes.
type applicationRecoveryReadyRefusal struct {
	s               *Store
	before          applicationRecoveryReady
	entries, queued int
}

func (r *applicationRecoveryReadyRefusal) matches(s *Store, entries int) bool {
	return r != nil && r.s == s && r.entries == entries && r.queued == len(s.meta.Recovery.Queued) && r.before.matches(s) && r.queued+r.entries > (maxMetadataBytes-recoveryMetadataFixedBytes)/256
}

func (s *Store) classifyRecoveryReadyOutcome(entries []*pb.Entry) (*applicationRecoveryReady, *applicationRecoveryReadyRefusal, error) {
	s.mu.Lock()
	if err := s.check(); err != nil {
		s.mu.Unlock()
		return nil, nil, err
	}
	if !s.meta.Recovery.Config.enabled() {
		s.mu.Unlock()
		return nil, nil, nil
	}
	r := &applicationRecoveryReady{Applied: s.meta.Applied, Last: s.meta.Last, Generation: s.activeGeneration(), ImageHash: s.meta.ImageHash, LastHash: s.meta.LastHash}
	verifier := s.recoveryVerifier
	captured := s.meta.Recovery
	old := captured.Queued
	first := s.meta.Last + 1
	if len(entries) > 0 && entries[0] != nil {
		first = entries[0].GetIndex()
	}
	// Exact-cap owner bounds use the fixed metadata allowance BEFORE copying.
	if len(old)+len(entries) > (maxMetadataBytes-recoveryMetadataFixedBytes)/256 {
		s.mu.Unlock()
		refusal := &applicationRecoveryReadyRefusal{s: s, before: *r, entries: len(entries), queued: len(old)}
		return nil, refusal, ErrLimit
	}
	r.Queued = make([]applicationRecoveryQueued, 0, len(old)+len(entries))
	for _, q := range old {
		if q.Index < first {
			r.Queued = append(r.Queued, q)
		}
	}
	s.mu.Unlock()
	if len(entries) == 0 || verifier == nil {
		return r, nil, nil
	}
	view, err := s.ApplicationView(r.Applied)
	if err != nil {
		return nil, nil, err
	}
	_, err = view.RootBounded(context.Background(), s.applicationPolicy.MaxImageBytes)
	if err != nil {
		return nil, nil, errors.Join(err, view.Close())
	}
	err = s.classifyRecoveryEntries(entries, view, verifier, captured, r)
	err = errors.Join(err, view.Close())
	if err != nil {
		return nil, nil, err
	}
	return r, nil, nil
}

func (m applicationRecoveryMetadata) afterApplied(index uint64) applicationRecoveryMetadata {
	if len(m.Queued) == 0 {
		return m
	}
	// Do not mutate the original slice on a prospective/rejected installation.
	n := 0
	for _, q := range m.Queued {
		if q.Index > index {
			n++
		}
	}
	queued := make([]applicationRecoveryQueued, 0, n)
	for _, q := range m.Queued {
		if q.Index > index {
			queued = append(queued, q)
		}
	}
	m.Queued = queued
	return m
}

func appendRecoveryQueue(dst []byte, queued []applicationRecoveryQueued) []byte {
	dst = append(dst, 'R', 'Q', 2, 0)
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(queued))) // #nosec G115 -- queued owners admitted under the existing 64 KiB metadata frame.
	for _, q := range queued {
		dst = binary.BigEndian.AppendUint64(dst, q.Index)
		dst = append(dst, q.CommandDigest[:]...)
		if q.Purpose.Reserve != nil {
			r := q.Purpose.Reserve
			dst = append(dst, 1)
			dst = binary.BigEndian.AppendUint32(dst, uint32(76+recoveryCreditBytes*len(r.Credits))) // #nosec G115 -- reservation frame admitted <=64 KiB before encoding.
			dst = binary.BigEndian.AppendUint64(dst, q.ReservationOriginIndex)
			dst = append(dst, r.ID[:]...)
			dst = append(dst, r.Digest[:]...)
			dst = binary.BigEndian.AppendUint32(dst, uint32(len(r.Credits))) // #nosec G115 -- fixed metadata capacity bounds count.
			for _, c := range r.Credits {
				dst = binary.BigEndian.AppendUint32(dst, c.Phase)
				dst = binary.BigEndian.AppendUint32(dst, uint32(c.CommandBytes)) // #nosec G115 -- validated nonnegative entry-policy bound.
				dst = binary.BigEndian.AppendUint64(dst, c.RetainedBytes)
				dst = binary.BigEndian.AppendUint64(dst, c.RetainedRecords)
			}
		} else {
			u := q.Purpose.Use
			dst = append(dst, 2)
			dst = binary.BigEndian.AppendUint32(dst, 128)
			dst = append(dst, u.ID[:]...)
			dst = append(dst, u.Digest[:]...)
			dst = append(dst, u.CommandDigest[:]...)
			dst = binary.BigEndian.AppendUint32(dst, u.Phase)
			dst = binary.BigEndian.AppendUint32(dst, q.Credit.Phase)
			dst = binary.BigEndian.AppendUint32(dst, uint32(q.Credit.CommandBytes)) // #nosec G115 -- credited command bound validated under concrete entry policy.
			dst = binary.BigEndian.AppendUint64(dst, q.Credit.RetainedBytes)
			dst = binary.BigEndian.AppendUint64(dst, q.Credit.RetainedRecords)
			dst = binary.BigEndian.AppendUint32(dst, u.RetireBefore)
		}
	}
	return dst
}

func decodeRecoveryQueue(b []byte, p ApplicationPolicy, l Limits) ([]applicationRecoveryQueued, []byte, error) {
	if len(b) < 8 || !bytes.Equal(b[:4], []byte{'R', 'Q', 2, 0}) {
		return nil, nil, ErrCorrupt
	}
	count := uint64(binary.BigEndian.Uint32(b[4:8]))
	b = b[8:]
	if count > uint64((maxMetadataBytes-recoveryMetadataFixedBytes)/256) || count > uint64(len(b)/145) {
		return nil, nil, ErrCorrupt
	}
	if count == 0 {
		return nil, b, nil
	}
	queued := make([]applicationRecoveryQueued, int(count)) // #nosec G115 -- count <= bounded delivered metadata/145.
	for i := range queued {
		if len(b) < 45 {
			return nil, nil, ErrCorrupt
		}
		q := &queued[i]
		q.Index = binary.BigEndian.Uint64(b[:8])
		copy(q.CommandDigest[:], b[8:40])
		kind := b[40]
		n := uint64(binary.BigEndian.Uint32(b[41:45]))
		b = b[45:]
		if n > uint64(len(b)) {
			return nil, nil, ErrCorrupt
		}
		payload := b[:int(n)] // #nosec G115 -- n <= uint64(len(b)) above; conversion fits int before slicing.
		b = b[int(n):]        // #nosec G115 -- n checked against delivered <=64 KiB frame.
		switch kind {
		case 1:
			if len(payload) < 76 {
				return nil, nil, ErrCorrupt
			}
			r := &ApplicationRecoveryReservation{}
			q.ReservationOriginIndex = binary.BigEndian.Uint64(payload[:8])
			if q.ReservationOriginIndex == 0 || q.ReservationOriginIndex > q.Index {
				return nil, nil, ErrCorrupt
			}
			copy(r.ID[:], payload[8:40])
			copy(r.Digest[:], payload[40:72])
			nc := uint64(binary.BigEndian.Uint32(payload[72:76]))
			if nc > uint64((len(payload)-76)/recoveryCreditBytes) || nc*recoveryCreditBytes != uint64(len(payload)-76) { // #nosec G115 -- prior guard requires len(payload)>=76; nonnegative differences are representable in uint64.
				return nil, nil, ErrCorrupt
			}
			r.Credits = make([]ApplicationRecoveryCredit, int(nc)) // #nosec G115 -- delivered metadata count checked before allocation.
			for j := range r.Credits {
				row := payload[76+j*recoveryCreditBytes : 76+(j+1)*recoveryCreditBytes]
				command := uint64(binary.BigEndian.Uint32(row[4:8]))
				if command > unsignedLimit(l.MaxEntryBytes-frameOverhead) {
					return nil, nil, ErrCorrupt
				}
				r.Credits[j] = ApplicationRecoveryCredit{Phase: binary.BigEndian.Uint32(row[:4]), CommandBytes: int(command), RetainedBytes: binary.BigEndian.Uint64(row[8:16]), RetainedRecords: binary.BigEndian.Uint64(row[16:24])} // #nosec G115 -- command checked under <=16 MiB entry ceiling.
			}
			if _, err := recoveryGrowth(*r, p, l); err != nil {
				return nil, nil, ErrCorrupt
			}
			q.Purpose.Reserve = r
		case 2:
			if len(payload) != 128 {
				return nil, nil, ErrCorrupt
			}
			u := &ApplicationRecoveryUse{}
			copy(u.ID[:], payload[:32])
			copy(u.Digest[:], payload[32:64])
			copy(u.CommandDigest[:], payload[64:96])
			u.Phase = binary.BigEndian.Uint32(payload[96:100])
			u.RetireBefore = binary.BigEndian.Uint32(payload[124:128])
			if u.RetireBefore > u.Phase {
				return nil, nil, ErrCorrupt
			}
			q.Purpose.Use = u
			command := uint64(binary.BigEndian.Uint32(payload[104:108]))
			if command > unsignedLimit(l.MaxEntryBytes-frameOverhead) {
				return nil, nil, ErrCorrupt
			}
			q.Credit = ApplicationRecoveryCredit{Phase: binary.BigEndian.Uint32(payload[100:104]), CommandBytes: int(command), RetainedBytes: binary.BigEndian.Uint64(payload[108:116]), RetainedRecords: binary.BigEndian.Uint64(payload[116:124])} // #nosec G115 -- command checked against <=16 MiB delivered entry ceiling.
		default:
			return nil, nil, ErrCorrupt
		}
	}
	return queued, b, nil
}

func (s *Store) verifyRecoveryQueue() error {
	for _, q := range s.meta.Recovery.Queued {
		e, _, _, err := s.get(q.Index)
		if err != nil {
			return storedReadFailure(err)
		}
		if sha256.Sum256(e.GetData()) != q.CommandDigest {
			return ErrCorrupt
		}
		if q.Purpose.Reserve != nil && q.ReservationOriginIndex < q.Index {
			if q.ReservationOriginIndex <= s.meta.Applied {
				owner, err := s.loadRecoveryOwnerAt(s.activeBank(), q.ReservationOriginIndex)
				if err != nil {
					return err
				}
				if owner != (ApplicationRecoveryOwner{ID: q.Purpose.Reserve.ID, Digest: q.Purpose.Reserve.Digest}) {
					return ErrCorrupt
				}
			} else {
				found := false
				for _, origin := range s.meta.Recovery.Queued {
					if origin.Index == q.ReservationOriginIndex && origin.ReservationOriginIndex == origin.Index && origin.Purpose.Reserve != nil && sameRecoveryReservation(*origin.Purpose.Reserve, *q.Purpose.Reserve) {
						found = true
						break
					}
				}
				if !found {
					return ErrCorrupt
				}
			}
		}
	}
	return nil
}

// Validated queued-use bounds survive an exact replay after its first copy has
// installed. Replays reserve pending-install space but consume future credit
// only while the applied/projected original phase still exists.
func advanceRecoveryQueued(state applicationRecoveryMetadata, q applicationRecoveryQueued, p ApplicationPolicy, l Limits) (applicationRecoveryMetadata, ApplicationRecoveryCredit, error) {
	if q.Purpose.Reserve != nil {
		if q.ReservationOriginIndex == 0 || q.ReservationOriginIndex > q.Index {
			return state, ApplicationRecoveryCredit{}, ErrInvalid
		}
		if q.ReservationOriginIndex < q.Index {
			return state, ApplicationRecoveryCredit{}, nil
		} // verified retry adds no future credits
		if _, err := recoveryGrowth(*q.Purpose.Reserve, p, l); err != nil {
			return state, ApplicationRecoveryCredit{}, err
		}
		if state.Reservation.ID != ([32]byte{}) {
			if !sameRecoveryReservation(state.Reservation, *q.Purpose.Reserve) {
				return state, ApplicationRecoveryCredit{}, ErrLimit
			}
		} else {
			state.Reservation = *q.Purpose.Reserve
		} // already owned immutable queued backing
		return state, ApplicationRecoveryCredit{}, nil
	}
	u := *q.Purpose.Use
	c := q.Credit
	if u.ID == ([32]byte{}) || u.Digest == ([32]byte{}) || u.CommandDigest != q.CommandDigest || u.Phase == 0 || c.Phase != u.Phase || u.RetireBefore > u.Phase {
		return state, c, ErrInvalid
	}
	if _, err := recoveryGrowth(ApplicationRecoveryReservation{ID: u.ID, Digest: u.Digest, Credits: []ApplicationRecoveryCredit{c}}, p, l); err != nil {
		return state, c, err
	}
	actual, err := state.credit(u)
	if err != nil {
		return state, c, nil
	} // bound already checked before persistence; exact replay
	if c != actual {
		return state, c, ErrInvalid
	}
	projected := u
	projected.RetireBefore = 0 // unused credits retire only at actual installation
	state, _, err = state.consume(projected)
	return state, c, err
}

func (s *Store) classifyRecoveryEntries(entries []*pb.Entry, view *ApplicationView, verifier ApplicationRecoveryVerifier, captured applicationRecoveryMetadata, r *applicationRecoveryReady) error {
	if verifier == nil {
		return nil
	}
	for _, e := range entries {
		if e == nil {
			return ErrInvalid
		}
		purpose, eErr := verifier.VerifyApplicationRecovery(context.Background(), e.GetData(), view)
		if eErr != nil {
			return eErr
		}
		if purpose.Reserve != nil && purpose.Use != nil {
			return ErrInvalid
		}
		if purpose.Reserve == nil && purpose.Use == nil {
			continue
		}
		if purpose.Reserve != nil {
			if _, eErr = recoveryGrowth(*purpose.Reserve, s.applicationPolicy, s.limits); eErr != nil {
				return eErr
			}
			// Bound variable ownership and the final metadata frame before the clone.
			used := recoveryMetadataFixedBytes + 256*cap(r.Queued)
			for _, q := range r.Queued {
				if q.Purpose.Reserve != nil {
					used += recoveryCreditBytes * len(q.Purpose.Reserve.Credits)
				}
			}
			if recoveryCreditBytes*len(purpose.Reserve.Credits) > maxMetadataBytes-used {
				return ErrLimit
			}
			owned := *purpose.Reserve
			owned.Credits = make([]ApplicationRecoveryCredit, len(purpose.Reserve.Credits))
			copy(owned.Credits, purpose.Reserve.Credits)
			purpose.Reserve = &owned
		} else {
			use := *purpose.Use
			if use.CommandDigest != sha256.Sum256(e.GetData()) {
				return ErrInvalid
			}
			purpose.Use = &use
		}
		q := applicationRecoveryQueued{Index: e.GetIndex(), CommandDigest: sha256.Sum256(e.GetData()), Purpose: purpose}
		if purpose.Reserve != nil {
			q.ReservationOriginIndex = q.Index
			if purpose.Reserve.ID == captured.Reservation.ID && purpose.Reserve.Digest == captured.Reservation.Digest {
				owner, _, e := view.RecoveryOwner(context.Background(), ReadBudget{Rows: 1, Bytes: s.applicationPolicy.MaxPageBytes})
				if e != nil {
					return e
				}
				if owner != (ApplicationRecoveryOwner{ID: purpose.Reserve.ID, Digest: purpose.Reserve.Digest}) {
					return ErrCorrupt
				}
				q.ReservationOriginIndex = view.index
			} else {
				for _, prior := range r.Queued {
					if prior.Purpose.Reserve != nil && sameRecoveryReservation(*prior.Purpose.Reserve, *purpose.Reserve) {
						q.ReservationOriginIndex = prior.ReservationOriginIndex
						break
					}
				}
			}
		}
		if purpose.Use != nil {
			// A genuine original or earlier pending registration must supply the
			// phase bound. Retried exact commands retain that same bound; they never
			// mint another future credit or change an unrelated active owner.
			q.Credit, eErr = captured.credit(*purpose.Use)
			if eErr != nil {
				for _, prior := range r.Queued {
					if prior.Purpose.Reserve != nil {
						priorState := applicationRecoveryMetadata{Config: captured.Config, Reservation: *prior.Purpose.Reserve}
						if c, cErr := priorState.credit(*purpose.Use); cErr == nil {
							q.Credit = c
							eErr = nil
						}
					} else if prior.Purpose.Use != nil && *prior.Purpose.Use == *purpose.Use {
						q.Credit = prior.Credit
						eErr = nil
					}
				}
			}
			if eErr != nil {
				return eErr
			}
		}
		r.Queued = append(r.Queued, q)
	}
	return nil
}

// A Ready may install an incoming image and its following recovery entries in
// one sync. Verify those entries against the claimed image, not the old active
// bank; attach their recipes only when that same claim is durably activated.
func (s *Store) classifyRecoverySnapshotReady(rd raft.Ready, c *ApplicationSnapshotClaim) (*applicationRecoveryReady, error) {
	s.mu.Lock()
	if err := s.check(); err != nil {
		s.mu.Unlock()
		return nil, err
	}
	if len(rd.Entries) == 0 || !s.meta.Recovery.Config.enabled() {
		s.mu.Unlock()
		return nil, nil
	}
	if c == nil || c.p == nil || c.p.s != s {
		s.mu.Unlock()
		return nil, ErrInvalid
	}
	if c.closed || c.consumed {
		s.mu.Unlock()
		return nil, ErrClosed
	}
	if !applicationSnapshotReadyMatches(rd.Snapshot, c.expected) {
		s.mu.Unlock()
		return nil, ErrInvalid
	}
	if len(rd.Entries) > (maxMetadataBytes-recoveryMetadataFixedBytes)/256 {
		s.mu.Unlock()
		return nil, ErrLimit
	}
	verifier := s.recoveryVerifier
	captured := applicationRecoveryMetadata{Config: s.meta.Recovery.Config, Reservation: c.p.i.manifest.RecoveryReservation}
	index := c.p.i.manifest.Index
	s.mu.Unlock()
	view, err := c.p.openReadView(context.Background(), index, s.applicationPolicy.MaxImageBytes, c)
	if err != nil {
		return nil, err
	}
	r := &applicationRecoveryReady{Queued: make([]applicationRecoveryQueued, 0, len(rd.Entries))}
	err = s.classifyRecoveryEntries(rd.Entries, view, verifier, captured, r)
	err = errors.Join(err, view.Close())
	if err != nil {
		return nil, err
	}
	return r, nil
}
