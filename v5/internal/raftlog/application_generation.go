package raftlog

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
)

// ApplicationGenerationLimits opts a fresh transfer-enabled store into two
// physical application banks. MaxBytes/MaxRecords cap their aggregate logical
// rows plus reserved staging and pending installations, not physical disk/RSS.
// Zero disables generation mode; enabling it never enables snapshot activation.
type ApplicationGenerationLimits struct{ MaxBytes, MaxRecords uint64 }

func (l ApplicationGenerationLimits) enabled() bool { return l != (ApplicationGenerationLimits{}) }
func (l ApplicationGenerationLimits) validate(p ApplicationPolicy, t ApplicationTransferConfig) error {
	if !l.enabled() {
		return nil
	}
	if !p.Enabled() || !t.enabled() || l.MaxBytes < 1 || l.MaxBytes > 1<<40 || l.MaxRecords < 1 || l.MaxRecords > 1<<40 {
		return ErrInvalid
	}
	return nil
}

type applicationBankState uint64

const (
	bankFree applicationBankState = iota
	bankActive
	bankStaging
	bankRetired
)

type applicationBank struct {
	Generation, Through, Bytes, Records, ReservedBytes, ReservedRecords uint64
	State                                                               applicationBankState
}
type generationMetadata struct {
	Publication publicationMetadata
	Limits      ApplicationGenerationLimits
	HighWater   uint64
	Active      byte
	Banks       [2]applicationBank
}

const generationMetaBytes = 4 + 4*8 + 2*7*8

func appendGenerationMeta(b []byte, g generationMetadata) []byte {
	version := byte(1)
	if g.Publication.Limits.enabled() {
		version = 2
	}
	b = append(b, 'A', 'G', version, 0)
	for _, n := range []uint64{g.Limits.MaxBytes, g.Limits.MaxRecords, g.HighWater, uint64(g.Active)} {
		b = binary.BigEndian.AppendUint64(b, n)
	}
	for _, bank := range g.Banks {
		for _, n := range []uint64{bank.Generation, bank.Through, bank.Bytes, bank.Records, bank.ReservedBytes, bank.ReservedRecords, uint64(bank.State)} {
			b = binary.BigEndian.AppendUint64(b, n)
		}
	}
	if g.Publication.Limits.enabled() {
		b = appendPublicationMeta(b, g.Publication)
	}
	return b
}
func decodeGenerationMeta(b []byte) (generationMetadata, []byte, error) {
	if len(b) < generationMetaBytes || (!bytes.Equal(b[:4], []byte{'A', 'G', 1, 0}) && !bytes.Equal(b[:4], []byte{'A', 'G', 2, 0})) {
		return generationMetadata{}, nil, ErrCorrupt
	}
	version2 := b[2] == 2
	var n [18]uint64
	for j := range n {
		n[j] = binary.BigEndian.Uint64(b[4+j*8:])
	}
	if n[3] > 1 || n[10] > uint64(bankRetired) || n[17] > uint64(bankRetired) {
		return generationMetadata{}, nil, ErrCorrupt
	}
	g := generationMetadata{Limits: ApplicationGenerationLimits{n[0], n[1]}, HighWater: n[2], Active: byte(n[3])} // #nosec G115 -- n[3] is checked <= 1 before conversion to a bank tag.
	for j := range g.Banks {
		o := 4 + j*7
		g.Banks[j] = applicationBank{n[o], n[o+1], n[o+2], n[o+3], n[o+4], n[o+5], applicationBankState(n[o+6])}
	}
	if !g.Limits.enabled() || g.Limits.MaxBytes > 1<<40 || g.Limits.MaxRecords > 1<<40 || g.Limits.MaxBytes == 0 || g.Limits.MaxRecords == 0 {
		return generationMetadata{}, nil, ErrCorrupt
	}
	tail := b[generationMetaBytes:]
	if version2 {
		var err error
		g.Publication, tail, err = decodePublicationMeta(tail)
		if err != nil {
			return generationMetadata{}, nil, err
		}
	}
	return g, tail, nil
}
func syncActiveGeneration(m *metadata) {
	if !m.Gen.Limits.enabled() || m.Gen.Active > 1 {
		return
	}
	b := &m.Gen.Banks[m.Gen.Active]
	b.Through, b.Bytes, b.Records = m.App.Through, m.App.Bytes, m.App.Records
}
func generationCharge(m metadata, pending uint64) error {
	g := m.Gen
	if !g.Limits.enabled() {
		return nil
	}
	var totalBytes, totalRecords uint64
	for _, b := range g.Banks {
		totalBytes += max(b.Bytes, b.ReservedBytes)
		totalRecords += max(b.Records, b.ReservedRecords)
	}
	if totalBytes > g.Limits.MaxBytes || totalRecords > g.Limits.MaxRecords {
		return ErrLimit
	}
	p := m.App.Policy
	if p.MaxInstallBytes < 1 || p.MaxInstallWrites < 1 {
		return ErrInvalid
	}
	if pending > (g.Limits.MaxBytes-totalBytes)/unsignedLimit(p.MaxInstallBytes) || pending > (g.Limits.MaxRecords-totalRecords)/unsignedLimit(p.MaxInstallWrites+3) {
		return ErrLimit
	}
	return nil
}
func (s *Store) validateGenerationMeta(m metadata) error {
	g := m.Gen
	if !g.Limits.enabled() {
		if g != (generationMetadata{}) {
			return ErrInvalid
		}
		return nil
	}
	if err := g.Limits.validate(m.App.Policy, m.Transfer); err != nil {
		return err
	}
	if g.Active > 1 || g.HighWater == 0 || g.HighWater != max(g.Banks[0].Generation, g.Banks[1].Generation) || g.Banks[0].Generation != 0 && g.Banks[0].Generation == g.Banks[1].Generation {
		return ErrInvalid
	}
	for j, b := range g.Banks {
		if b.State > bankRetired || b.Generation > g.HighWater {
			return ErrInvalid
		}
		if b.Bytes > g.Limits.MaxBytes || b.Records > g.Limits.MaxRecords || b.ReservedBytes > g.Limits.MaxBytes || b.ReservedRecords > g.Limits.MaxRecords {
			return ErrLimit
		}
		if b.Bytes > m.App.Policy.RetainedApplicationBytes || b.Records > m.App.Policy.RetainedApplicationRecords {
			return ErrLimit
		}
		if b.State == bankStaging && (b.ReservedBytes > min(m.Transfer.Limits.MaxStagedBytes, m.App.Policy.RetainedApplicationBytes) || b.ReservedRecords > min(m.Transfer.Limits.MaxStagedRecords, m.App.Policy.RetainedApplicationRecords)) {
			return ErrLimit
		}
		if byte(j) == g.Active {
			if b.State != bankActive || b.Generation == 0 || b.Through != m.App.Through || b.Bytes != m.App.Bytes || b.Records != m.App.Records || b.ReservedBytes != 0 || b.ReservedRecords != 0 {
				return ErrInvalid
			}
		} else {
			if b.State == bankActive || b.State != bankFree && (b.Generation == 0 || b.Through == 0 || b.Through == math.MaxUint64 || b.Through > g.Limits.MaxRecords/3) {
				return ErrInvalid
			}
			if b.State == bankFree && (b.Through != 0 || b.Bytes != 0 || b.Records != 0 || b.ReservedBytes != 0 || b.ReservedRecords != 0) {
				return ErrInvalid
			}
			if b.State != bankStaging && (b.ReservedBytes != 0 || b.ReservedRecords != 0) {
				return ErrInvalid
			}
			if b.State == bankStaging && (b.Bytes > b.ReservedBytes || b.Records > b.ReservedRecords || b.Through > b.ReservedRecords/3 || b.ReservedBytes < b.Through*3*(9+appFrameBytes)) {
				return ErrInvalid
			}
		}
	}
	if m.Last < m.Applied {
		return ErrInvalid
	}
	if err := validatePublicationMeta(m); err != nil {
		return err
	}
	return generationCharge(m, m.Last-m.Applied)
}
func (s *Store) activeBank() byte { return s.meta.Gen.Active }
func (s *Store) activeGeneration() uint64 {
	if !s.meta.Gen.Limits.enabled() {
		return 0
	}
	return s.meta.Gen.Banks[s.meta.Gen.Active].Generation
}
func bankTag(bank, tag byte) byte                      { return tag + bank*4 }
func bankIndexKey(bank, tag byte, index uint64) []byte { return appIndexKey(bankTag(bank, tag), index) }
func bankPrefix(bank byte, key []byte) []byte {
	b := appPrefix(key)
	b[0] = bankTag(bank, appDataTag)
	return b
}
func bankVersionKey(bank byte, key []byte, index uint64) []byte {
	b := appVersionKey(key, index)
	b[0] = bankTag(bank, appDataTag)
	return b
}
func decodeBankAppKey(bank byte, b []byte, limit int) ([]byte, uint64, error) {
	if len(b) == 0 || b[0] != bankTag(bank, appDataTag) {
		return nil, 0, ErrCorrupt
	}
	return decodeTaggedAppKey(b, limit, bankTag(bank, appDataTag))
}

type generationRef struct {
	bank       byte
	generation uint64
	refs       int
}

func (s *Store) pinGeneration(bank byte) (*generationRef, error) {
	if !s.meta.Gen.Limits.enabled() {
		return nil, nil
	}
	if bank > 1 {
		return nil, ErrInvalid
	}
	gen := s.meta.Gen.Banks[bank].Generation
	if gen == 0 || s.meta.Gen.Banks[bank].State == bankFree {
		return nil, ErrInvalid
	}
	r := s.generationRefs[bank]
	if r == nil || r.generation != gen {
		if r != nil && r.refs != 0 {
			return nil, ErrCorrupt
		}
		r = &generationRef{bank: bank, generation: gen}
		s.generationRefs[bank] = r
	}
	r.refs++
	return r, nil
}
func (s *Store) releaseGeneration(r *generationRef) error {
	if r == nil {
		return nil
	}
	if r.refs < 1 {
		return ErrCorrupt
	}
	r.refs--
	if s.closed || r.refs != 0 {
		return nil
	}
	if err := s.check(); err != nil {
		return err
	}
	b := s.meta.Gen.Banks[r.bank]
	if b.Generation == r.generation && b.State == bankRetired && !holdsPublishedGeneration(s.meta, r.bank, r.generation) {
		return s.clearGenerationBank(r.bank, r.generation)
	}
	return nil
}
func (s *Store) clearGenerationBank(bank byte, generation uint64) error {
	if bank > 1 || bank == s.activeBank() || s.meta.Gen.Banks[bank].Generation != generation {
		return ErrInvalid
	}
	if holdsPublishedGeneration(s.meta, bank, generation) {
		return ErrLimit
	}
	if r := s.generationRefs[bank]; r != nil && r.refs != 0 {
		return ErrLimit
	}
	m := s.meta
	m.Gen.Banks[bank] = applicationBank{Generation: generation, State: bankFree}
	b := s.db.NewBatch()
	if err := b.DeleteRange([]byte{bankTag(bank, appDataTag)}, []byte{bankTag(bank, appOutcomeTag) + 1}, nil); err != nil {
		return errors.Join(err, b.Close())
	}
	if err := b.Delete(dormantKey, nil); err != nil {
		return errors.Join(err, b.Close())
	}
	return s.commit(m, b)
}
func (s *Store) reserveGeneration(m ApplicationSnapshotManifest) (metadata, byte, error) {
	prospective := s.meta
	bank := byte(1) - prospective.Gen.Active
	if prospective.Gen.HighWater == math.MaxUint64 {
		return prospective, bank, ErrLimit
	}
	if prospective.Gen.Banks[bank].State != bankFree {
		return prospective, bank, ErrLimit
	}
	if r := s.generationRefs[bank]; r != nil && r.refs != 0 {
		return prospective, bank, ErrLimit
	}
	size, rows, err := snapshotTotals(m)
	if err != nil {
		return prospective, bank, err
	}
	prospective.Gen.HighWater++
	prospective.Gen.Banks[bank] = applicationBank{Generation: prospective.Gen.HighWater, Through: m.Index, ReservedBytes: size, ReservedRecords: rows, State: bankStaging}
	if err := s.validateGenerationMeta(prospective); err != nil {
		return s.meta, bank, err
	}
	return prospective, bank, nil
}
func (s *Store) retireImport(i *ApplicationImport) error {
	bank := i.bank
	gen := i.generation
	if bank == s.activeBank() || s.meta.Gen.Banks[bank].Generation != gen || s.meta.Gen.Banks[bank].State != bankStaging {
		return ErrInvalid
	}
	m := s.meta
	b := &m.Gen.Banks[bank]
	b.State = bankRetired
	b.ReservedBytes, b.ReservedRecords = 0, 0
	batch := s.db.NewBatch()
	if err := batch.Delete(dormantKey, nil); err != nil {
		return errors.Join(err, batch.Close())
	}
	if err := s.commit(m, batch); err != nil {
		return err
	}
	return nil
}

// ApplicationGeneration reports the current local physical generation. Nil and
// legacy stores report zero; closed stores may report their last generation.
// It grants no validity, lease, semantic time or replicated effect identity.
func (s *Store) ApplicationGeneration() uint64 {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.activeGeneration()
}
