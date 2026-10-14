package raftlog

import (
	"bytes"
	"encoding/binary"
	"math"
)

// ApplicationRecoveryConfig opts fresh stores into protected application/log
// completion capacity. It adds no quota: every live credit subtracts from the
// existing policy. Version zero preserves disabled legacy behavior.
type ApplicationRecoveryConfig struct{ Version uint32 }

// ApplicationRecoveryCredit is one distinct completion phase's maximum logical
// installation growth and encoded command bound. It is capacity, not admission
// of graph effects, a decision, a lease or a quorum certificate.
type ApplicationRecoveryCredit struct {
	Phase                          uint32
	CommandBytes                   int
	RetainedBytes, RetainedRecords uint64
}

// ApplicationRecoveryReservation binds a finite completion schedule to one
// operation and its immutable logical template. The trusted materializer must
// validate that template and the actual phase; arbitrary entry headers are not
// accepted as credit claims.
type ApplicationRecoveryReservation struct {
	ID, Digest [32]byte
	Credits    []ApplicationRecoveryCredit
}

// ApplicationRecoveryUse identifies a single phase. Its command digest is
// checked against the exact data admitted by the serialized driver/store seam.
type ApplicationRecoveryUse struct {
	ID, Digest, CommandDigest [32]byte
	Phase                     uint32
	// RetireBefore is 0, or <= Phase. Only actual installation may retire
	// still-unused earlier phases of this exact operation; later credits survive.
	RetireBefore uint32
}

type applicationRecoveryMetadata struct {
	Config         ApplicationRecoveryConfig
	Reservation    ApplicationRecoveryReservation
	Terminal       applicationTerminalBank
	Queued         []applicationRecoveryQueued
	Bytes, Records uint64
	Staged         applicationRecoveryGrowth
}

type applicationRecoveryGrowth struct {
	bytes, records, logBytes, logRecords uint64
}

func (c ApplicationRecoveryConfig) enabled() bool { return c.Version != 0 }
func (c ApplicationRecoveryConfig) validate() error {
	if c.Version > 2 {
		return ErrInvalid
	}
	return nil
}

func checkedRecoveryAdd(a, b uint64) (uint64, error) {
	if b > math.MaxUint64-a {
		return 0, ErrLimit
	}
	return a + b, nil
}

func recoveryGrowth(r ApplicationRecoveryReservation, p ApplicationPolicy, l Limits) (applicationRecoveryGrowth, error) {
	var growth applicationRecoveryGrowth
	if r.ID == ([32]byte{}) || r.Digest == ([32]byte{}) || len(r.Credits) == 0 || len(r.Credits) > (maxMetadataBytes-recoveryMetadataFixedBytes)/recoveryCreditBytes {
		return growth, ErrInvalid
	}
	for i, credit := range r.Credits {
		if credit.Phase == 0 || i > 0 && r.Credits[i-1].Phase >= credit.Phase || credit.CommandBytes < 0 || credit.CommandBytes > l.MaxEntryBytes-frameOverhead || credit.RetainedBytes == 0 || credit.RetainedRecords == 0 {
			return applicationRecoveryGrowth{}, ErrInvalid
		}
		if credit.RetainedBytes > p.RetainedApplicationBytes || credit.RetainedRecords > p.RetainedApplicationRecords {
			return applicationRecoveryGrowth{}, ErrLimit
		}
		var err error
		growth.bytes, err = checkedRecoveryAdd(growth.bytes, credit.RetainedBytes)
		if err != nil {
			return applicationRecoveryGrowth{}, err
		}
		growth.records, err = checkedRecoveryAdd(growth.records, credit.RetainedRecords)
		if err != nil {
			return applicationRecoveryGrowth{}, err
		}
		growth.logBytes, err = checkedRecoveryAdd(growth.logBytes, uint64(credit.CommandBytes+frameOverhead+9))
		if err != nil {
			return applicationRecoveryGrowth{}, err
		}
		growth.logRecords++
	}
	return growth, nil
} // #nosec G115 -- command bytes validated nonnegative and bounded by the concrete entry policy.

// All quantities describe actual remaining logical quotas. No refund or
// reservation is performed by this inspection; Store must commit the selected
// metadata together with the corresponding application installation.
func recoveryFits(g applicationRecoveryGrowth, retainedBytes, retainedRecords, logBytes, logRecords uint64, p ApplicationPolicy, l Limits) error {
	if retainedBytes > p.RetainedApplicationBytes || retainedRecords > p.RetainedApplicationRecords || logBytes > l.MaxRetainedBytes || logRecords > l.MaxRetainedEntries {
		return ErrLimit
	}
	if g.bytes > p.RetainedApplicationBytes-retainedBytes || g.records > p.RetainedApplicationRecords-retainedRecords || g.logBytes > l.MaxRetainedBytes-logBytes || g.logRecords > l.MaxRetainedEntries-logRecords {
		return ErrLimit
	}
	return nil
}

func sameRecoveryReservation(a, b ApplicationRecoveryReservation) bool {
	if a.ID != b.ID || a.Digest != b.Digest || len(a.Credits) != len(b.Credits) {
		return false
	}
	for i := range a.Credits {
		if a.Credits[i] != b.Credits[i] {
			return false
		}
	}
	return true
}

func (m applicationRecoveryMetadata) reserve(r ApplicationRecoveryReservation, p ApplicationPolicy, l Limits) (applicationRecoveryMetadata, error) {
	if !m.Config.enabled() || m.Config.validate() != nil {
		return m, ErrInvalid
	}
	if _, err := recoveryGrowth(r, p, l); err != nil {
		return m, err
	}
	if m.Reservation.ID != ([32]byte{}) {
		if sameRecoveryReservation(m.Reservation, r) {
			return m, nil
		}
		return m, ErrLimit
	}
	// The enclosing metadata/driver admission must precharge this exact typed
	// array before ownership. This helper is not a public reservation door.
	credits := make([]ApplicationRecoveryCredit, len(r.Credits))
	copy(credits, r.Credits)
	m.Reservation = ApplicationRecoveryReservation{ID: r.ID, Digest: r.Digest, Credits: credits}
	return m, nil
}

func (m applicationRecoveryMetadata) credit(use ApplicationRecoveryUse) (ApplicationRecoveryCredit, error) {
	if !m.Config.enabled() || use.ID == ([32]byte{}) || use.Digest == ([32]byte{}) || use.CommandDigest == ([32]byte{}) || use.Phase == 0 || use.RetireBefore > use.Phase {
		return ApplicationRecoveryCredit{}, ErrInvalid
	}
	if use.ID != m.Reservation.ID || use.Digest != m.Reservation.Digest {
		return ApplicationRecoveryCredit{}, ErrInvalid
	}
	for _, credit := range m.Reservation.Credits {
		if credit.Phase == use.Phase {
			return credit, nil
		}
	}
	return ApplicationRecoveryCredit{}, ErrInvalid
}

func recoveryCommandMatches(use ApplicationRecoveryUse, commandDigest []byte) bool {
	return len(commandDigest) == len(use.CommandDigest) && bytes.Equal(commandDigest, use.CommandDigest[:])
}

const recoveryMetadataFixedBytes = 4 + 4 + 32 + 32 + 4
const recoveryCreditBytes = 4 + 4 + 8 + 8

func recoveryMetadataBytes(m applicationRecoveryMetadata) int {
	n := recoveryMetadataFixedBytes + recoveryCreditBytes*len(m.Reservation.Credits) + 8 + 16 + 32
	if m.Config.Version == 2 {
		n += terminalBankBytes(m.Terminal)
	}
	for _, q := range m.Queued {
		n += 45
		if r := q.Purpose.reservation(); r != nil {
			n += 76 + recoveryCreditBytes*len(r.Credits)
		} else {
			n += 128
		}
	}
	return n
}
func validateRecoveryMetadata(m metadata, l Limits) error {
	c := m.Recovery.Config
	if err := c.validate(); err != nil {
		return err
	}
	if m.Recovery.Staged.bytes > m.App.Policy.RetainedApplicationBytes || m.Recovery.Staged.records > m.App.Policy.RetainedApplicationRecords || m.Recovery.Staged.logBytes > l.MaxRetainedBytes || m.Recovery.Staged.logRecords > l.MaxRetainedEntries {
		return ErrLimit
	}
	r := m.Recovery.Reservation
	if c.Version != 2 && len(m.Recovery.Terminal) != 0 {
		return ErrInvalid
	}
	if c.Version == 2 {
		if err := terminalMetadataHeadroom(m, l); err != nil {
			return err
		}
		if _, err := terminalGrowth(m.Recovery.Terminal, m.App.Policy, l); err != nil {
			return err
		}
	}
	contract := 2 + c.Version
	if m.Retention.Config.enabled() {
		contract = 5
	}
	if c.enabled() && (m.Transfer.Contract.Version != contract || m.Recovery.Bytes > m.App.Policy.RetainedApplicationBytes || m.Recovery.Records != m.Applied || m.Recovery.Records > m.Recovery.Bytes/(9+appFrameBytes+recoveryRootFixedBytes)) {
		return ErrInvalid
	}
	if !c.enabled() {
		if r.ID != ([32]byte{}) || r.Digest != ([32]byte{}) || len(r.Credits) != 0 || len(m.Recovery.Queued) != 0 || m.Recovery.Bytes != 0 || m.Recovery.Records != 0 || m.Recovery.Staged != (applicationRecoveryGrowth{}) {
			return ErrInvalid
		}
		return nil
	}
	if !m.App.Policy.Enabled() || !m.Rep.Config.enabled() || m.Rep.SemanticContractID == (ApplicationSemanticContractID{}) || !m.Controls.Config.enabled() {
		return ErrInvalid
	}
	if r.ID == ([32]byte{}) {
		if r.Digest != ([32]byte{}) || len(r.Credits) != 0 {
			return ErrInvalid
		}
	} else if _, err := recoveryGrowth(r, m.App.Policy, l); err != nil {
		return err
	}
	_, _, _, err := recoveryProjected(m, l, m.Last-m.Applied)
	return err
}
func appendRecoveryMetadata(b []byte, m applicationRecoveryMetadata) []byte {
	version := byte(3)
	if m.Config.Version == 2 {
		version = 4
	}
	b = append(b, 'R', 'Y', version, 0)
	b = binary.BigEndian.AppendUint32(b, m.Config.Version)
	b = append(b, m.Reservation.ID[:]...)
	b = append(b, m.Reservation.Digest[:]...)
	b = binary.BigEndian.AppendUint32(b, uint32(len(m.Reservation.Credits))) // #nosec G115 -- metadata count preflight bounded by the existing64KiB metadata frame.
	for _, c := range m.Reservation.Credits {
		b = binary.BigEndian.AppendUint32(b, c.Phase)
		b = binary.BigEndian.AppendUint32(b, uint32(c.CommandBytes)) // #nosec G115 -- command bytes validated0..16MiB before encoding.
		b = binary.BigEndian.AppendUint64(b, c.RetainedBytes)
		b = binary.BigEndian.AppendUint64(b, c.RetainedRecords)
	}
	b = appendRecoveryQueue(b, m.Queued)
	b = binary.BigEndian.AppendUint64(b, m.Bytes)
	b = binary.BigEndian.AppendUint64(b, m.Records)
	for _, n := range []uint64{m.Staged.bytes, m.Staged.records, m.Staged.logBytes, m.Staged.logRecords} {
		b = binary.BigEndian.AppendUint64(b, n)
	}
	if m.Config.Version == 2 {
		b = appendTerminalBank(b, m.Terminal)
	}
	return b
}
func decodeRecoveryMetadata(b []byte, p ApplicationPolicy, l Limits) (applicationRecoveryMetadata, []byte, error) {
	var m applicationRecoveryMetadata
	if len(b) < recoveryMetadataFixedBytes || (!bytes.Equal(b[:4], []byte{'R', 'Y', 3, 0}) && !bytes.Equal(b[:4], []byte{'R', 'Y', 4, 0})) {
		return m, nil, ErrCorrupt
	}
	m.Config.Version = binary.BigEndian.Uint32(b[4:8])
	if m.Config.Version != uint32(b[2]-2) {
		return m, nil, ErrCorrupt
	}
	copy(m.Reservation.ID[:], b[8:40])
	copy(m.Reservation.Digest[:], b[40:72])
	count := uint64(binary.BigEndian.Uint32(b[72:76]))
	if count > uint64((maxMetadataBytes-recoveryMetadataFixedBytes)/recoveryCreditBytes) || count > uint64((len(b)-recoveryMetadataFixedBytes)/recoveryCreditBytes) {
		return m, nil, ErrCorrupt
	} // #nosec G115 -- concrete positive policy and delivered-length bounds.
	n := int(count) // #nosec G115 -- count bounded by delivered<=64KiB metadata.
	if n > 0 {
		m.Reservation.Credits = make([]ApplicationRecoveryCredit, n)
	}
	for i := range m.Reservation.Credits {
		row := b[76+i*recoveryCreditBytes : 76+(i+1)*recoveryCreditBytes]
		command := binary.BigEndian.Uint32(row[4:8])
		if uint64(command) > uint64(l.MaxEntryBytes-frameOverhead) {
			return applicationRecoveryMetadata{}, nil, ErrCorrupt
		} // #nosec G115 -- concrete validated entry policy.
		m.Reservation.Credits[i] = ApplicationRecoveryCredit{Phase: binary.BigEndian.Uint32(row[:4]), CommandBytes: int(command), RetainedBytes: binary.BigEndian.Uint64(row[8:16]), RetainedRecords: binary.BigEndian.Uint64(row[16:24])} // #nosec G115 -- command bounded<=16MiB.
	}
	if m.Reservation.ID == ([32]byte{}) {
		if m.Reservation.Digest != ([32]byte{}) || n != 0 {
			return applicationRecoveryMetadata{}, nil, ErrCorrupt
		}
	} else if _, err := recoveryGrowth(m.Reservation, p, l); err != nil {
		return applicationRecoveryMetadata{}, nil, ErrCorrupt
	}
	tail := b[76+n*recoveryCreditBytes:]
	var err error
	m.Queued, tail, err = decodeRecoveryQueue(tail, p, l)
	if err != nil {
		return applicationRecoveryMetadata{}, nil, err
	}
	if len(tail) < 48 {
		return applicationRecoveryMetadata{}, nil, ErrCorrupt
	}
	m.Bytes = binary.BigEndian.Uint64(tail[:8])
	m.Records = binary.BigEndian.Uint64(tail[8:16])
	m.Staged = applicationRecoveryGrowth{bytes: binary.BigEndian.Uint64(tail[16:24]), records: binary.BigEndian.Uint64(tail[24:32]), logBytes: binary.BigEndian.Uint64(tail[32:40]), logRecords: binary.BigEndian.Uint64(tail[40:48])}
	tail = tail[48:]
	if m.Config.Version == 2 {
		m.Terminal, tail, err = decodeTerminalBank(tail, p, l)
		if err != nil {
			return applicationRecoveryMetadata{}, nil, err
		}
	} else {
		for _, q := range m.Queued {
			if q.Purpose.terminal() {
				return applicationRecoveryMetadata{}, nil, ErrCorrupt
			}
		}
	}
	return m, tail, nil
}

func recoveryOutstanding(m metadata, l Limits) (applicationRecoveryGrowth, error) {
	if err := validateRecoveryMetadata(m, l); err != nil {
		return applicationRecoveryGrowth{}, err
	}
	if m.Recovery.Config.Version != 2 {
		return recoveryGrowthIfPresent(m.Recovery.Reservation, m.App.Policy, l)
	}
	g, err := recoveryGrowthIfPresent(m.Recovery.Reservation, m.App.Policy, l)
	if err != nil {
		return applicationRecoveryGrowth{}, err
	}
	t, err := terminalGrowth(m.Recovery.Terminal, m.App.Policy, l)
	if err != nil {
		return applicationRecoveryGrowth{}, err
	}
	return addRecoveryGrowth(g, t)
}

func (m applicationRecoveryMetadata) consume(use ApplicationRecoveryUse) (applicationRecoveryMetadata, ApplicationRecoveryCredit, error) {
	credit, err := m.credit(use)
	if err != nil {
		return m, ApplicationRecoveryCredit{}, err
	}
	n := 0
	keep := func(c ApplicationRecoveryCredit) bool {
		return c.Phase != use.Phase && (use.RetireBefore == 0 || c.Phase >= use.RetireBefore)
	}
	for _, c := range m.Reservation.Credits {
		if keep(c) {
			n++
		}
	}
	if n == 0 {
		m.Reservation = ApplicationRecoveryReservation{}
		return m, credit, nil
	}
	// Exact backing is owned before changing a prospective metadata value.
	// Retirement is installed atomically; queued projection keeps unused credits.
	remaining := make([]ApplicationRecoveryCredit, n)
	i := 0
	for _, c := range m.Reservation.Credits {
		if keep(c) {
			remaining[i] = c
			i++
		}
	}
	m.Reservation = ApplicationRecoveryReservation{ID: m.Reservation.ID, Digest: m.Reservation.Digest, Credits: remaining}
	return m, credit, nil
}
