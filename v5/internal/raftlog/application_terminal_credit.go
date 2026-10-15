package raftlog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"slices"

	pb "go.etcd.io/raft/v3/raftpb"
)

// A terminal bank is independent of the single transient recovery reservation.
// The fresh profile reserves every cleanup alternative before publishing an
// owner. These are shared logical quotas, never additional capacity or authority.
const maxApplicationTerminalOwners = 16
const maxApplicationTerminalAlternatives = 3

type applicationTerminalBank []ApplicationRecoveryReservation

func terminalGrowth(bank applicationTerminalBank, p ApplicationPolicy, l Limits) (applicationRecoveryGrowth, error) {
	var total applicationRecoveryGrowth
	if len(bank) > maxApplicationTerminalOwners {
		return total, ErrLimit
	}
	for i, owner := range bank {
		if len(owner.Spans) != 0 || len(owner.Credits) > maxApplicationTerminalAlternatives || i > 0 && bytes.Compare(bank[i-1].ID[:], owner.ID[:]) >= 0 {
			return applicationRecoveryGrowth{}, ErrInvalid
		}
		for _, credit := range owner.Credits {
			if credit.Phase > maxApplicationTerminalAlternatives {
				return applicationRecoveryGrowth{}, ErrInvalid
			}
		}
		g, err := recoveryGrowth(owner, p, l)
		if err != nil {
			return applicationRecoveryGrowth{}, err
		}
		total, err = addRecoveryGrowth(total, g)
		if err != nil {
			return applicationRecoveryGrowth{}, err
		}
	}
	return total, nil
}

func addRecoveryGrowth(a, b applicationRecoveryGrowth) (applicationRecoveryGrowth, error) {
	var out applicationRecoveryGrowth
	for _, row := range []struct {
		a, b uint64
		out  *uint64
	}{
		{a.bytes, b.bytes, &out.bytes}, {a.records, b.records, &out.records},
		{a.logBytes, b.logBytes, &out.logBytes}, {a.logRecords, b.logRecords, &out.logRecords},
	} {
		n, err := checkedRecoveryAdd(row.a, row.b)
		if err != nil {
			return applicationRecoveryGrowth{}, err
		}
		*row.out = n
	}
	return out, nil
}

func (bank applicationTerminalBank) find(id [32]byte) int {
	return slices.IndexFunc(bank, func(r ApplicationRecoveryReservation) bool { return r.ID == id })
}

func (m applicationRecoveryMetadata) reserveTerminal(r ApplicationRecoveryReservation, p ApplicationPolicy, l Limits) (applicationRecoveryMetadata, error) {
	if m.Config.Version < 2 {
		return m, ErrInvalid
	}
	if _, err := terminalGrowth(applicationTerminalBank{r}, p, l); err != nil {
		return m, err
	}
	if i := m.Terminal.find(r.ID); i >= 0 {
		if !sameRecoveryReservation(m.Terminal[i], r) {
			return m, ErrInvalid
		}
		return m, nil
	}
	if len(m.Terminal) >= maxApplicationTerminalOwners {
		return m, ErrLimit
	}
	bank := make(applicationTerminalBank, len(m.Terminal)+1)
	copy(bank, m.Terminal)
	bank[len(m.Terminal)] = cloneRecoveryReservation(r)
	slices.SortFunc(bank, func(a, b ApplicationRecoveryReservation) int { return bytes.Compare(a.ID[:], b.ID[:]) })
	m.Terminal = bank
	return m, nil
}

func (m applicationRecoveryMetadata) terminalCredit(u ApplicationRecoveryUse) (ApplicationRecoveryCredit, error) {
	if m.Config.Version < 2 || u.RetireBefore != 0 {
		return ApplicationRecoveryCredit{}, ErrInvalid
	}
	i := m.Terminal.find(u.ID)
	if i < 0 {
		return ApplicationRecoveryCredit{}, ErrInvalid
	}
	state := applicationRecoveryMetadata{Config: ApplicationRecoveryConfig{Version: 2}, Reservation: m.Terminal[i]}
	return state.credit(u)
}

// Projection spends only the selected credit. Unused alternatives remain
// protected until the exact terminal command actually installs durably.
func (m applicationRecoveryMetadata) consumeTerminal(u ApplicationRecoveryUse, installed bool) (applicationRecoveryMetadata, ApplicationRecoveryCredit, error) {
	credit, err := m.terminalCredit(u)
	if err != nil {
		return m, ApplicationRecoveryCredit{}, err
	}
	i := m.Terminal.find(u.ID)
	owner := m.Terminal[i]
	if !installed {
		state := applicationRecoveryMetadata{Config: ApplicationRecoveryConfig{Version: 2}, Reservation: owner}
		state, _, err = state.consume(u)
		if err != nil {
			return m, ApplicationRecoveryCredit{}, err
		}
		owner = state.Reservation
	}
	bank := make(applicationTerminalBank, 0, len(m.Terminal))
	for j, r := range m.Terminal {
		if j != i {
			bank = append(bank, r)
		} else if !installed && owner.ID != ([32]byte{}) {
			bank = append(bank, owner)
		}
	}
	m.Terminal = bank
	return m, credit, nil
}

func terminalBankBytes(bank applicationTerminalBank) int {
	n := 8
	for _, owner := range bank {
		n += 4 + recoveryRootBytes(owner)
	}
	return n
}
func appendTerminalBank(dst []byte, bank applicationTerminalBank) []byte {
	dst = append(dst, 'T', 'B', 1, 0)
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(bank))) // #nosec G115 -- bank admitted at <=16 owners.
	for _, owner := range bank {
		dst = binary.BigEndian.AppendUint32(dst, uint32(recoveryRootBytes(owner))) // #nosec G115 -- <=3 credits bound the framed owner to144 bytes.
		dst = append(dst, appendRecoveryRoot(owner)...)
	}
	return dst
}
func decodeTerminalBank(b []byte, p ApplicationPolicy, l Limits) (applicationTerminalBank, []byte, error) {
	if len(b) < 8 || !bytes.Equal(b[:4], []byte{'T', 'B', 1, 0}) {
		return nil, nil, ErrCorrupt
	}
	n := uint64(binary.BigEndian.Uint32(b[4:8]))
	b = b[8:]
	if n > maxApplicationTerminalOwners || n > uint64(len(b)/(4+recoveryRootFixedBytes+recoveryCreditBytes)) {
		return nil, nil, ErrCorrupt
	}
	bank := make(applicationTerminalBank, int(n)) // #nosec G115 -- <=16 owners and delivered frame bound checked before allocation.
	for i := range bank {
		if len(b) < 4 {
			return nil, nil, ErrCorrupt
		}
		length := uint64(binary.BigEndian.Uint32(b[:4]))
		b = b[4:]
		if length > recoveryRootFixedBytes+maxApplicationTerminalAlternatives*recoveryCreditBytes || length > uint64(len(b)) {
			return nil, nil, ErrCorrupt
		}
		owner, err := readRecoveryRoot(b[:int(length)], p, l) // #nosec G115 -- length bounded by delivered<=144 bytes.
		if err != nil {
			return nil, nil, err
		}
		bank[i] = owner
		b = b[int(length):] // #nosec G115 -- delivered-length bound precedes conversion.
	}
	if _, err := terminalGrowth(bank, p, l); err != nil {
		return nil, nil, ErrCorrupt
	}
	return bank, b, nil
}

func nativeRecoveryRootBytes(m applicationRecoveryMetadata) int {
	n := recoveryRootBytes(m.Reservation)
	if m.Config.Version == 4 {
		n = recoveryPoolRootBytes(m.Reservation)
	}
	if m.Config.Version >= 2 {
		n += terminalBankBytes(m.Terminal)
	}
	return n
}
func appendNativeRecoveryRoot(m applicationRecoveryMetadata) []byte {
	var b []byte
	if m.Config.Version == 4 {
		b = appendRecoveryPoolRoot(m.Reservation)
	} else if m.Config.Version == 3 {
		b = appendRecoverySpanRoot(m.Reservation)
	} else {
		b = appendRecoveryRoot(m.Reservation)
	}
	if m.Config.Version >= 2 {
		b[2] = byte(m.Config.Version)
		b = appendTerminalBank(b, m.Terminal)
	}
	return b
}
func nativeRecoveryRootDigest(m applicationRecoveryMetadata) [32]byte {
	if m.Config.Version < 2 {
		return recoveryRootDigest(m.Reservation)
	}
	// Encoding is only used for an actual bound root/hash. Reservation sizing
	// uses nativeRecoveryRootBytes and never materializes an uncharged encoding.
	return sha256.Sum256(appendNativeRecoveryRoot(m))
}
func readNativeRecoveryRoot(b []byte, p ApplicationPolicy, l Limits) (ApplicationRecoveryReservation, applicationTerminalBank, error) {
	if len(b) < recoveryRootFixedBytes || len(b) > maxMetadataBytes {
		return ApplicationRecoveryReservation{}, nil, ErrCorrupt
	}
	if b[2] == 4 {
		r, end, err := readRecoveryPoolRoot(b, p, l)
		if err != nil {
			return ApplicationRecoveryReservation{}, nil, err
		}
		bank, tail, err := decodeTerminalBank(b[end:], p, l)
		if err != nil || len(tail) != 0 {
			return ApplicationRecoveryReservation{}, nil, ErrCorrupt
		}
		return r, bank, nil
	}
	if b[2] == 3 {
		r, end, err := readRecoverySpanRoot(b, p, l)
		if err != nil {
			return ApplicationRecoveryReservation{}, nil, err
		}
		bank, tail, err := decodeTerminalBank(b[end:], p, l)
		if err != nil || len(tail) != 0 {
			return ApplicationRecoveryReservation{}, nil, ErrCorrupt
		}
		return r, bank, nil
	}
	if b[2] == 1 {
		r, err := readRecoveryRoot(b, p, l)
		return r, nil, err
	}
	if !bytes.Equal(b[:4], []byte{'R', 'S', 2, 0}) {
		return ApplicationRecoveryReservation{}, nil, ErrCorrupt
	}
	n := uint64(binary.BigEndian.Uint32(b[68:72]))
	if n > uint64((len(b)-recoveryRootFixedBytes)/recoveryCreditBytes) {
		return ApplicationRecoveryReservation{}, nil, ErrCorrupt
	}
	end := recoveryRootFixedBytes + int(n)*recoveryCreditBytes // #nosec G115 -- n bounded by delivered<=64KiB root.
	old := bytes.Clone(b[:end])
	old[2] = 1
	r, err := readRecoveryRoot(old, p, l)
	if err != nil {
		return ApplicationRecoveryReservation{}, nil, err
	}
	bank, tail, err := decodeTerminalBank(b[end:], p, l)
	if err != nil || len(tail) != 0 {
		return ApplicationRecoveryReservation{}, nil, ErrCorrupt
	}
	return r, bank, nil
}

func sameTerminalBank(a, b applicationTerminalBank) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !sameRecoveryReservation(a[i], b[i]) {
			return false
		}
	}
	return true
}
func inspectNativeRecoveryOwner(b []byte, p ApplicationPolicy, l Limits) (ApplicationRecoveryOwner, int, error) {
	if len(b) >= 4 && b[2] == 1 {
		return inspectRecoveryOwner(b, p, l)
	}
	r, _, err := readNativeRecoveryRoot(b, p, l)
	if err != nil {
		return ApplicationRecoveryOwner{}, 0, err
	}
	return ApplicationRecoveryOwner{ID: r.ID, Digest: r.Digest}, recoveryEntries(r), nil
}
func expectedNativeRecoveryDigest(m *applicationRecoveryMetadata, r ApplicationRecoveryReservation) [32]byte {
	if m != nil {
		return nativeRecoveryRootDigest(*m)
	}
	return recoveryRootDigest(r)
}
func nativeRecoveryRootOverhead(m applicationRecoveryMetadata) (uint64, uint64, error) {
	if m.Config.Version < 2 {
		return ApplicationRecoveryRootOverhead(len(m.Reservation.Credits))
	}
	n := nativeRecoveryRootBytes(m)
	if n > maxMetadataBytes {
		return 0, 0, ErrLimit
	}
	return uint64(9 + appFrameBytes + n), 1, nil // #nosec G115 -- n bounded by the concrete64KiB metadata frame.
}
func (p ApplicationPolicy) measureNativeRecovery(b ApplicationBatch, m applicationRecoveryMetadata) (uint64, uint64, error) {
	if m.Config.Version == 4 {
		added, count, err := p.measure(b)
		if err != nil {
			return 0, 0, err
		}
		extra, rows, err := nativeRecoveryRootOverhead(m)
		if err != nil || nativeRecoveryRootBytes(m) > p.MaxValueBytes {
			return 0, 0, ErrLimit
		}
		work := 2*added + uint64(len(b.Image)+1024) + uint64(64*(len(b.Writes)+len(b.ControlPuts))) // #nosec G115 -- p.measure validated finite concrete shape.
		for _, w := range b.ControlPuts {
			work += uint64(128 + 4*controlPhysicalKeyBytes(w.Key))
		} // #nosec G115 -- key shape admitted by p.measure.
		if work > unsignedLimit(p.MaxInstallBytes) || 2*extra > unsignedLimit(p.MaxInstallBytes)-work {
			return 0, 0, ErrLimit
		}
		return added + extra, count + rows, nil
	}
	added, count, err := p.measureRecovery(b, m.Reservation)
	if err != nil || m.Config.Version < 2 {
		return added, count, err
	}
	extra := uint64(terminalBankBytes(m.Terminal)) // #nosec G115 -- <=16 admitted owners, each<=3 credits.
	if nativeRecoveryRootBytes(m) > min(p.MaxValueBytes, maxMetadataBytes) {
		return 0, 0, ErrLimit
	}
	work := 2*added + uint64(len(b.Image)+1024) + uint64(64*(len(b.Writes)+len(b.ControlPuts)))
	for _, w := range b.ControlPuts {
		work += 128 + 4*controlPhysicalKeyBytes(w.Key)
	}
	if work > unsignedLimit(p.MaxInstallBytes) || 2*extra > unsignedLimit(p.MaxInstallBytes)-work {
		return 0, 0, ErrLimit
	}
	return added + extra, count, nil
}
func (s *Store) loadNativeRecoveryRootAt(bank byte, index uint64) (r ApplicationRecoveryReservation, owners applicationTerminalBank, err error) {
	key := appIndexKey(recoveryRootTag+bank, index)
	raw, closer, err := s.db.Get(key)
	if err != nil {
		return r, nil, storedReadFailure(err)
	}
	data, deleted, readErr := inspectAppFrame(key, raw, min(s.applicationPolicy.MaxValueBytes, maxMetadataBytes))
	if readErr == nil && !deleted {
		r, owners, readErr = readNativeRecoveryRoot(data, s.applicationPolicy, s.limits)
		if readErr == nil && data[2] != byte(s.meta.Recovery.Config.Version) {
			readErr = ErrCorrupt
		}
	} else if readErr == nil {
		readErr = ErrCorrupt
	}
	return r, owners, errors.Join(readErr, closer.Close())
}

func validRecoveryPurpose(p ApplicationRecoveryPurpose) bool {
	n := 0
	if p.Reserve != nil {
		n++
	}
	if p.Use != nil {
		n++
	}
	if p.ApplyBefore != nil {
		n++
	}
	if p.TerminalReserve != nil {
		n++
	}
	if p.TerminalUse != nil {
		n++
	}
	return n <= 1
}

func (s *Store) admitTerminalCreationLocked(commandBytes int, r ApplicationRecoveryReservation) error {
	if s.meta.Recovery.Config.Version < 2 {
		return ErrInvalid
	}
	if err := s.admitApplicationLocked(commandBytes); err != nil {
		return err
	}
	p := s.meta.App.Policy
	g, err := terminalGrowth(applicationTerminalBank{r}, p, s.limits)
	if err != nil {
		return err
	}
	current, err := recoveryOutstanding(s.meta, s.limits)
	if err != nil {
		return err
	}
	if i := s.meta.Recovery.Terminal.find(r.ID); i >= 0 {
		if !sameRecoveryReservation(s.meta.Recovery.Terminal[i], r) {
			return ErrInvalid
		}
		g = applicationRecoveryGrowth{}
	} else if len(s.meta.Recovery.Terminal) >= maxApplicationTerminalOwners || recoveryMetadataBytes(s.meta.Recovery)+4+recoveryRootBytes(r) > maxMetadataBytes {
		return ErrLimit
	}
	future, err := addRecoveryGrowth(current, g)
	if err != nil {
		return err
	}
	future, err = addRecoveryGrowth(future, retentionCursorGrowth(s.meta))
	if err != nil {
		return err
	}
	if s.meta.Recovery.Config.Version == 4 {
		// The ordinary preflight protected the window before adding this new
		// terminal owner. Its added bank cannot spend that existing backing.
		future, err = addRecoveryPoolPendingWindow(future, s.meta.Recovery.Reservation, p, 0)
		if err != nil {
			return err
		}
	}
	retained, records := applicationTotals(s.meta)
	pending := s.meta.Last - s.meta.Applied + 2
	if err := recoveryFits(future, retained+pending*unsignedLimit(p.MaxInstallBytes), records+pending*applicationInstallRecords(s.meta), s.meta.LogBytes+uint64(commandBytes+2*(frameOverhead+9)), s.meta.LogCount+2, p, s.limits); err != nil {
		return err
	} // #nosec G115 -- ordinary admission bounds nonnegative commandBytes under16MiB before addition.
	prospective := s.meta
	prospective.Recovery, err = prospective.Recovery.reserveTerminal(r, p, s.limits)
	if err != nil {
		return err
	}
	if err := terminalMetadataHeadroom(prospective, s.limits); err != nil {
		return err
	}
	if err := checkMetadataLimit(prospective, s.limits); err != nil {
		return err
	}
	return generationCharge(prospective, pending, s.limits)
}

func (a *ApplicationRecoveryAdmission) matchesTerminal(s *Store, data []byte, reserve *ApplicationRecoveryReservation, use *ApplicationRecoveryUse) bool {
	if a == nil || a.self != a || a.s != s || a.base.Generation != s.activeGeneration() || a.base.Index > s.meta.Applied || a.hash != sha256.Sum256(data) {
		return false
	}
	if reserve != nil {
		return a.purpose.TerminalReserve != nil && sameRecoveryReservation(*reserve, *a.purpose.TerminalReserve) && use == nil
	}
	return use != nil && a.purpose.TerminalUse != nil && *use == *a.purpose.TerminalUse
}

func (p applicationRecoveryQueuedPurpose) reservation() *ApplicationRecoveryReservation {
	if p.TerminalReserve != nil {
		return p.TerminalReserve
	}
	return p.Reserve
}
func (p applicationRecoveryQueuedPurpose) use() *ApplicationRecoveryUse {
	if p.TerminalUse != nil {
		return p.TerminalUse
	}
	return p.Use
}
func (p applicationRecoveryQueuedPurpose) terminal() bool {
	return p.TerminalReserve != nil || p.TerminalUse != nil
}
func (p applicationRecoveryQueuedPurpose) valid() bool {
	return validRecoveryPurpose(ApplicationRecoveryPurpose{Reserve: p.Reserve, Use: p.Use, TerminalReserve: p.TerminalReserve, TerminalUse: p.TerminalUse})
}

func advanceTerminalQueued(state applicationRecoveryMetadata, q applicationRecoveryQueued, p ApplicationPolicy, l Limits) (applicationRecoveryMetadata, ApplicationRecoveryCredit, error) {
	if state.Config.Version < 2 || !q.Purpose.valid() {
		return state, ApplicationRecoveryCredit{}, ErrInvalid
	}
	if r := q.Purpose.TerminalReserve; r != nil {
		if q.ReservationOriginIndex == 0 || q.ReservationOriginIndex > q.Index {
			return state, ApplicationRecoveryCredit{}, ErrInvalid
		}
		if q.ReservationOriginIndex < q.Index {
			return state, ApplicationRecoveryCredit{}, nil
		}
		next, err := state.reserveTerminal(*r, p, l)
		return next, ApplicationRecoveryCredit{}, err
	}
	u := q.Purpose.TerminalUse
	if u == nil || u.RetireBefore != 0 || u.CommandDigest != q.CommandDigest || q.Credit.Phase != u.Phase {
		return state, q.Credit, ErrInvalid
	}
	if _, err := terminalGrowth(applicationTerminalBank{{ID: u.ID, Digest: u.Digest, Credits: []ApplicationRecoveryCredit{q.Credit}}}, p, l); err != nil {
		return state, q.Credit, err
	}
	if i := state.Terminal.find(u.ID); i >= 0 && state.Terminal[i].Digest != u.Digest {
		return state, q.Credit, ErrInvalid
	}
	actual, err := state.terminalCredit(*u)
	if err != nil {
		return state, q.Credit, nil
	} // previously bound persisted replay owns its pending backing
	if actual != q.Credit {
		return state, q.Credit, ErrInvalid
	}
	next, _, err := state.consumeTerminal(*u, false)
	return next, q.Credit, err
}

func (s *Store) classifyTerminalEntry(e *pb.Entry, v *ApplicationView, captured applicationRecoveryMetadata, r *applicationRecoveryReady, purpose ApplicationRecoveryPurpose) error {
	if captured.Config.Version < 2 || !validRecoveryPurpose(purpose) {
		return ErrInvalid
	}
	q := applicationRecoveryQueued{Index: e.GetIndex(), CommandDigest: sha256.Sum256(e.GetData()), Purpose: applicationRecoveryQueuedPurpose{TerminalReserve: purpose.TerminalReserve, TerminalUse: purpose.TerminalUse}}
	if captured.Config.Version == 4 {
		q.ActualLogBytes = uint64(len(e.GetData()) + frameOverhead + 9)
	} // #nosec G115 -- native entry bound already validated.
	if reserve := purpose.TerminalReserve; reserve != nil {
		if _, err := terminalGrowth(applicationTerminalBank{*reserve}, s.applicationPolicy, s.limits); err != nil {
			return err
		}
		// Bound the complete owned queued recipe before cloning its <=3 credits.
		used := recoveryMetadataBytes(captured) + 256*cap(r.Queued)
		for _, prior := range r.Queued {
			if owner := prior.Purpose.reservation(); owner != nil {
				used += recoveryCreditBytes * len(owner.Credits)
			}
		}
		if recoveryRootBytes(*reserve) > maxMetadataBytes-used {
			return ErrLimit
		}
		owned := cloneRecoveryReservation(*reserve)
		q.Purpose.TerminalReserve = &owned
		q.ReservationOriginIndex = q.Index
		if i := captured.Terminal.find(owned.ID); i >= 0 {
			if !sameRecoveryReservation(captured.Terminal[i], owned) {
				return ErrInvalid
			}
			if err := v.lock(context.Background()); err != nil {
				return err
			}
			_, bank, err := s.loadNativeRecoveryRootAt(v.bank, v.index)
			v.unlock()
			if err != nil {
				return err
			}
			j := bank.find(owned.ID)
			if j < 0 || !sameRecoveryReservation(bank[j], owned) {
				return ErrCorrupt
			}
			q.ReservationOriginIndex = v.index
		} else {
			for _, prior := range r.Queued {
				if prior.Purpose.TerminalReserve != nil && sameRecoveryReservation(*prior.Purpose.TerminalReserve, owned) {
					q.ReservationOriginIndex = prior.ReservationOriginIndex
					break
				}
			}
		}
	} else {
		u := *purpose.TerminalUse
		if u.CommandDigest != q.CommandDigest || u.RetireBefore != 0 {
			return ErrInvalid
		}
		q.Purpose.TerminalUse = &u
		credit, err := captured.terminalCredit(u)
		if err != nil {
			for _, prior := range r.Queued {
				if owner := prior.Purpose.TerminalReserve; owner != nil {
					state := applicationRecoveryMetadata{Config: captured.Config, Terminal: applicationTerminalBank{*owner}}
					if c, e := state.terminalCredit(u); e == nil {
						credit, err = c, nil
					}
				} else if prior.Purpose.TerminalUse != nil && *prior.Purpose.TerminalUse == u {
					credit, err = prior.Credit, nil
				}
			}
		}
		if err != nil {
			return err
		}
		if len(e.GetData()) > credit.CommandBytes {
			return ErrLimit
		}
		q.Credit = credit
	}
	r.Queued = append(r.Queued, q)
	return nil
}

func (s *Store) verifyTerminalQueued(q applicationRecoveryQueued) error {
	if s.meta.Recovery.Config.Version < 2 || !q.Purpose.valid() {
		return ErrCorrupt
	}
	r := q.Purpose.TerminalReserve
	if r == nil {
		_, _, err := advanceTerminalQueued(s.meta.Recovery, q, s.applicationPolicy, s.limits)
		return err
	}
	if q.ReservationOriginIndex == q.Index {
		return nil
	}
	if q.ReservationOriginIndex == 0 || q.ReservationOriginIndex > q.Index {
		return ErrCorrupt
	}
	if q.ReservationOriginIndex <= s.meta.Applied {
		_, bank, err := s.loadNativeRecoveryRootAt(s.activeBank(), q.ReservationOriginIndex)
		if err != nil {
			return err
		}
		i := bank.find(r.ID)
		if i < 0 || !sameRecoveryReservation(bank[i], *r) {
			return ErrCorrupt
		}
		return nil
	}
	for _, origin := range s.meta.Recovery.Queued {
		if origin.Index == q.ReservationOriginIndex && origin.ReservationOriginIndex == origin.Index && origin.Purpose.TerminalReserve != nil && sameRecoveryReservation(*origin.Purpose.TerminalReserve, *r) {
			return nil
		}
	}
	return ErrCorrupt
}

// One owner must always be able to persist one authentic terminal recipe. The
// existing byte/log credits protect the effects; this protects the metadata and
// single-command Ready envelope as well. Actual queued duplicates still count
// in metadataBytes and never mint another reservation or free another slot.
func terminalMetadataHeadroom(m metadata, l Limits) error {
	if m.Recovery.Config.Version < 2 {
		return nil
	}
	const terminalRecipeBytes = 45 + 128
	owners, maxCommand, reserved := 0, 0, uint64(0)
	charge := func(r ApplicationRecoveryReservation) error {
		owners++
		if owners > maxApplicationTerminalOwners {
			return ErrLimit
		}
		queued := false
		for _, q := range m.Recovery.Queued {
			if u := q.Purpose.TerminalUse; u != nil && u.ID == r.ID && u.Digest == r.Digest {
				queued = true
			}
		}
		if !queued {
			reserved += terminalRecipeBytes
		}
		for _, credit := range r.Credits {
			maxCommand = max(maxCommand, credit.CommandBytes)
		}
		return nil
	}
	for _, r := range m.Recovery.Terminal {
		if err := charge(r); err != nil {
			return err
		}
	}
	for i, q := range m.Recovery.Queued {
		r := q.Purpose.TerminalReserve
		if r == nil || m.Recovery.Terminal.find(r.ID) >= 0 {
			continue
		}
		duplicate := false
		for _, before := range m.Recovery.Queued[:i] {
			if prior := before.Purpose.TerminalReserve; prior != nil && prior.ID == r.ID {
				duplicate = true
				break
			}
		}
		if !duplicate {
			if err := charge(*r); err != nil {
				return err
			}
		}
	}
	// Fixed future scalar widths protect an election/commit-only metadata update.
	// Membership is immutable in these profiles; no variable unknown fields enter.
	n := metadataBytes(m) + reserved + 64
	if n > maxMetadataBytes || n+readyEnvelopeBytes+uint64(maxCommand+frameOverhead+9+32) > unsignedLimit(l.MaxReadyBytes) {
		return ErrLimit
	} // #nosec G115 -- validated credit command ceiling is nonnegative <=16MiB.
	return nil
}

// CheckTerminalBank compares the complete expected roster to the authentic
// native bank at this owned view's exact bank/index. Expected is inert: equality
// grants no publication, lease, cleanup or GC authority. Unknown extra native
// owners make equality false. Work, including bounded decode ownership, spends
// the caller's existing read ledger; no second allowance is created here.
func (v *ApplicationView) CheckTerminalBank(ctx context.Context, expected []ApplicationRecoveryReservation, budget ReadBudget) (equal bool, work ApplicationProofWork, err error) {
	if err = v.lock(ctx); err != nil {
		return false, work, err
	}
	defer v.unlock()
	p := v.s.meta.App.Policy
	if v.s.meta.Recovery.Config.Version < 2 || budget.Rows < 1 || budget.Bytes < 1 {
		return false, work, ErrInvalid
	}
	if budget.Rows > p.MaxPageRows || budget.Bytes > p.MaxPageBytes {
		return false, work, ErrLimit
	}
	if _, err := terminalGrowth(applicationTerminalBank(expected), p, v.s.limits); err != nil {
		return false, work, err
	}
	if v.index < v.retention.RecoveryFloor {
		return false, work, ErrApplicationExpired
	}
	const fixed = 256 + 4*9
	if budget.Bytes < fixed {
		return false, work, ErrLimit
	}
	work.Bytes = fixed
	key := appIndexKey(recoveryRootTag+v.bank, v.index)
	work.Records = 1
	raw, closer, e := v.s.db.Get(key)
	if e != nil {
		return false, work, v.fail(storedReadFailure(e))
	}
	defer func() {
		if e := closer.Close(); e != nil {
			err = errors.Join(err, v.fail(e))
			equal = false
		}
	}()
	if len(raw) < appFrameBytes || len(raw) > appFrameBytes+min(p.MaxValueBytes, maxMetadataBytes) {
		return false, work, v.fail(ErrCorrupt)
	}
	dataBytes := len(raw) - appFrameBytes
	cost := len(key) + len(raw) + len(key) + 4 + 8*dataBytes + 256 + terminalBankBytes(applicationTerminalBank(expected))
	if cost > budget.Bytes-work.Bytes {
		return false, work, ErrLimit
	}
	work.Bytes += cost
	data, deleted, e := inspectAppFrame(key, raw, min(p.MaxValueBytes, maxMetadataBytes))
	if e != nil || deleted || len(data) < 4 || data[2] != byte(v.s.meta.Recovery.Config.Version) {
		return false, work, v.fail(ErrCorrupt)
	}
	r, bank, e := readNativeRecoveryRoot(data, p, v.s.limits)
	if e != nil {
		return false, work, v.fail(e)
	}
	var current *applicationRecoveryMetadata
	if v.prepared != nil && v.index == v.prepared.i.manifest.Index {
		captured := snapshotRecoveryState(v.prepared.i.manifest)
		current = &captured
	} else if v.bank == v.s.activeBank() && v.generation == v.s.activeGeneration() && v.index == v.s.meta.Applied {
		current = &v.s.meta.Recovery
	}
	if current != nil && (!sameRecoveryReservation(r, current.Reservation) || !sameTerminalBank(bank, current.Terminal)) {
		return false, work, v.fail(ErrCorrupt)
	}
	if err := ctx.Err(); err != nil {
		return false, work, err
	}
	return sameTerminalBank(bank, applicationTerminalBank(expected)), work, nil
}

// CheckRetainedRootFeed inspects actual retained source-root and feed-start
// evidence at this exact owned bank. A start at index+1 denotes the authenticated
// empty suffix after this captured prefix, not an invented empty record. Hash
// and coordinates are inert; graph admission must bind them to its actual native
// certificate. No payload copy or extra view/generation owner is created.
func (v *ApplicationView) CheckRetainedRootFeed(ctx context.Context, sourceIndex uint64, imageHash [32]byte, feedStart uint64, budget ReadBudget) (equal bool, work ApplicationProofWork, err error) {
	if err = v.lock(ctx); err != nil {
		return false, work, err
	}
	defer v.unlock()
	p := v.s.meta.App.Policy
	if v.s.meta.Recovery.Config.Version < 2 || sourceIndex == 0 || sourceIndex > v.index || imageHash == ([32]byte{}) || feedStart == 0 || feedStart > v.index+1 || budget.Rows < 1 || budget.Bytes < 1 {
		return false, work, ErrInvalid
	}
	if budget.Rows > p.MaxPageRows || budget.Bytes > p.MaxPageBytes {
		return false, work, ErrLimit
	}
	if sourceIndex < max(v.retention.RootFloor, v.retention.DataFloor) || feedStart < v.retention.FeedFloor {
		return false, work, ErrApplicationExpired
	}
	const fixed = 256
	if budget.Bytes < fixed {
		return false, work, ErrLimit
	}
	work.Bytes = fixed
	// Future logical GC must enforce this owner's captured native lower bound
	// before these DB lookups. This profile currently reclaims no logical history.
	inspect := func(index uint64, tag byte, limit int, root bool) (matches bool, opErr error) {
		if work.Records >= budget.Rows {
			return false, ErrLimit
		}
		key := bankIndexKey(v.bank, tag, index)
		work.Records++
		raw, closer, e := v.s.db.Get(key)
		if e != nil {
			return false, v.fail(storedReadFailure(e))
		}
		defer func() {
			if e := closer.Close(); e != nil {
				opErr = errors.Join(opErr, v.fail(e))
				matches = false
			}
		}()
		if len(raw) < appFrameBytes || len(raw) > appFrameBytes+limit {
			return false, v.fail(ErrCorrupt)
		}
		dataBytes := len(raw) - appFrameBytes
		cost := 2*len(key) + len(raw) + 4 + dataBytes
		if root {
			cost += dataBytes
		}
		if cost > budget.Bytes-work.Bytes {
			return false, ErrLimit
		}
		work.Bytes += cost
		data, deleted, e := inspectAppFrame(key, raw, limit)
		if e != nil || deleted {
			return false, v.fail(ErrCorrupt)
		}
		if e := ctx.Err(); e != nil {
			return false, e
		}
		return !root || sha256.Sum256(data) == imageHash, nil
	}
	root, err := inspect(sourceIndex, appRootTag, p.MaxImageBytes, true)
	if err != nil {
		return false, work, err
	}
	if feedStart <= v.index {
		if _, err := inspect(feedStart, appChangeTag, p.MaxChangeBytes, false); err != nil {
			return false, work, err
		}
	}
	return root, work, nil
}
