package raftlog

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math"

	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// ApplicationPublishedCutLimits opts a fresh RLM5 store into durable published
// application references. MaxTransferChunks bounds later exact transfer passes;
// it does not enable snapshot activation or multi-voter application machines.
type ApplicationPublishedCutLimits struct{ MaxTransferChunks uint64 }

func (l ApplicationPublishedCutLimits) enabled() bool { return l != (ApplicationPublishedCutLimits{}) }
func (l ApplicationPublishedCutLimits) validate(g ApplicationGenerationLimits) error {
	if !l.enabled() {
		return nil
	}
	if !g.enabled() || l.MaxTransferChunks > 1<<32 {
		return ErrInvalid
	}
	return nil
}

// ApplicationCutReference binds a published retained application checkpoint.
// It is not a certified graph cut or a commitment to all historical records;
// a transfer manifest supplies the verified canonical history digest separately.
// Local bank/generation, voter identity and HardState are intentionally absent.
type ApplicationCutReference struct {
	SemanticContractID                         ApplicationSemanticContractID
	Identity                                   ApplicationIdentity
	Contract                                   ApplicationContract
	Index, Term                                uint64
	ConfState                                  *pb.ConfState
	ImageBytes, RetainedBytes, RetainedRecords uint64
	ImageHash, ID                              [32]byte
	ControlBytes, ControlRecords               uint64
}

type publicationMetadata struct {
	ControlEnabled               bool
	ControlBytes, ControlRecords uint64
	Limits                       ApplicationPublishedCutLimits
	Bank                         byte
	Generation, Bytes, Records   uint64
	ID                           [32]byte
	Valid                        bool
}

const publicationMetaBytes = 4 + 6*8 + 32

func appendPublicationMeta(b []byte, p publicationMetadata) []byte {
	version := byte(1)
	if p.ControlEnabled {
		version = 2
	}
	b = append(b, 'A', 'P', version, 0)
	var valid uint64
	if p.Valid {
		valid = 1
	}
	for _, n := range []uint64{p.Limits.MaxTransferChunks, valid, uint64(p.Bank), p.Generation, p.Bytes, p.Records} {
		b = binary.BigEndian.AppendUint64(b, n)
	}
	b = append(b, p.ID[:]...)
	if p.ControlEnabled {
		b = binary.BigEndian.AppendUint64(b, p.ControlBytes)
		b = binary.BigEndian.AppendUint64(b, p.ControlRecords)
	}
	return b
}
func decodePublicationMeta(b []byte) (publicationMetadata, []byte, error) {
	if len(b) < publicationMetaBytes || (!bytes.Equal(b[:4], []byte{'A', 'P', 1, 0}) && !bytes.Equal(b[:4], []byte{'A', 'P', 2, 0})) {
		return publicationMetadata{}, nil, ErrCorrupt
	}
	var n [6]uint64
	for j := range n {
		n[j] = binary.BigEndian.Uint64(b[4+j*8:])
	}
	if n[0] == 0 || n[0] > 1<<32 || n[1] > 1 || n[2] > 1 {
		return publicationMetadata{}, nil, ErrCorrupt
	}
	p := publicationMetadata{Limits: ApplicationPublishedCutLimits{n[0]}, Valid: n[1] == 1, Bank: byte(n[2]), Generation: n[3], Bytes: n[4], Records: n[5]} // #nosec G115 -- n[2] is checked <= 1 before conversion to a bank tag.
	copy(p.ID[:], b[52:84])
	tail := b[publicationMetaBytes:]
	if b[2] == 2 {
		if len(tail) < 16 {
			return publicationMetadata{}, nil, ErrCorrupt
		}
		p.ControlEnabled = true
		p.ControlBytes = binary.BigEndian.Uint64(tail[:8])
		p.ControlRecords = binary.BigEndian.Uint64(tail[8:16])
		tail = tail[16:]
	}
	return p, tail, nil
}
func cutReference(m metadata) ApplicationCutReference {
	p := m.Gen.Publication
	return ApplicationCutReference{SemanticContractID: m.Rep.SemanticContractID, Identity: m.Transfer.Identity, Contract: m.Transfer.Contract, Index: m.Base, Term: m.BaseTerm, ConfState: m.Snap.GetMetadata().GetConfState(), ImageBytes: m.SnapBytes, ImageHash: m.SnapHash, RetainedBytes: p.Bytes, RetainedRecords: p.Records, ID: p.ID, ControlBytes: p.ControlBytes, ControlRecords: p.ControlRecords}
}
func cutID(c ApplicationCutReference) ([32]byte, error) {
	if err := c.Identity.validate(); err != nil {
		return [32]byte{}, err
	}
	if err := c.Contract.validate(); err != nil {
		return [32]byte{}, err
	}
	if c.Index == 0 || c.Index == math.MaxUint64 || c.Term == 0 || c.Term >= math.MaxUint64-1 || c.ImageBytes > unsignedLimit(c.Contract.MaxImageBytes) || c.RetainedRecords > 1<<40 || c.RetainedBytes > 1<<40 || c.RetainedRecords < 3 || c.Index > c.RetainedRecords/3 || c.Index > c.RetainedBytes/(3*(9+appFrameBytes)) {
		return [32]byte{}, ErrInvalid
	}
	if err := validateConf(c.ConfState, c.Index); err != nil {
		return [32]byte{}, err
	}
	if proto.Size(c.ConfState) > 32768 {
		return [32]byte{}, ErrLimit
	}
	cs, err := proto.MarshalOptions{Deterministic: true}.Marshal(canonicalConf(c.ConfState))
	if err != nil {
		return [32]byte{}, err
	}
	domain := []byte("rho-tkg:published-application-cut:v1\x00")
	if c.SemanticContractID != (ApplicationSemanticContractID{}) {
		domain = append([]byte("rho-tkg:published-application-cut:v2\x00"), c.SemanticContractID[:]...)
	}
	if c.Contract.Version == 2 {
		domain = append([]byte("rho-tkg:published-application-cut:v3\x00"), c.SemanticContractID[:]...)
	} else if c.ControlBytes != 0 || c.ControlRecords != 0 {
		return [32]byte{}, ErrInvalid
	}
	if c.ControlBytes > c.RetainedBytes || c.ControlRecords > c.RetainedRecords || c.Index > (c.RetainedRecords-c.ControlRecords)/3 {
		return [32]byte{}, ErrInvalid
	}
	b := appendIdentity(domain, c.Identity)
	b = appendContract(b, c.Contract)
	for _, n := range []uint64{c.Index, c.Term, c.ImageBytes, c.RetainedBytes, c.RetainedRecords} {
		b = binary.BigEndian.AppendUint64(b, n)
	}
	if c.Contract.Version == 2 {
		b = binary.BigEndian.AppendUint64(b, c.ControlBytes)
		b = binary.BigEndian.AppendUint64(b, c.ControlRecords)
	}
	b = append(b, c.ImageHash[:]...)
	b = binary.BigEndian.AppendUint64(b, uint64(len(cs)))
	b = append(b, cs...)
	return sha256.Sum256(b), nil
}
func bindPublication(m *metadata, bank byte, generation, retainedBytes, retainedRecords uint64) error {
	m.Gen.Publication.Bank, m.Gen.Publication.Generation = bank, generation
	m.Gen.Publication.Bytes, m.Gen.Publication.Records = retainedBytes, retainedRecords
	m.Gen.Publication.Valid = true
	m.Gen.Publication.ControlBytes, m.Gen.Publication.ControlRecords = m.Controls.Bytes, m.Controls.Records
	id, err := cutID(cutReference(*m))
	if err != nil {
		return err
	}
	m.Gen.Publication.ID = id
	return nil
}
func holdsPublishedGeneration(m metadata, bank byte, generation uint64) bool {
	p := m.Gen.Publication
	return p.Limits.enabled() && p.Valid && p.Bank == bank && p.Generation == generation
}
func validatePublicationMeta(m metadata) error {
	p := m.Gen.Publication
	if !p.Limits.enabled() {
		if p != (publicationMetadata{}) {
			return ErrInvalid
		}
		return nil
	}
	if p.ControlEnabled != m.Controls.Config.enabled() || p.ControlBytes > p.Bytes || p.ControlRecords > p.Records || !p.ControlEnabled && (p.ControlBytes != 0 || p.ControlRecords != 0) {
		return ErrInvalid
	}
	if err := p.Limits.validate(m.Gen.Limits); err != nil {
		return err
	}
	if !p.Valid {
		if m.Base != 0 || p.Bank != 0 || p.Generation != 0 || p.Bytes != 0 || p.Records != 0 || p.ID != [32]byte{} {
			return ErrInvalid
		}
		return nil
	}
	if p.Bank > 1 || p.Generation == 0 || m.Base == 0 {
		return ErrInvalid
	}
	bank := m.Gen.Banks[p.Bank]
	if bank.Generation != p.Generation || bank.State != bankActive && bank.State != bankRetired || bank.Through < m.Base || bank.Bytes < p.Bytes || bank.Records < p.Records {
		return ErrInvalid
	}
	id, err := cutID(cutReference(m))
	if err != nil {
		return err
	}
	if id != p.ID {
		return ErrInvalid
	}
	return nil
}

// PublishedApplicationCut returns owned portable metadata for the durable Base.
// Later applied entries do not move it. Disabled/uninitialized stores refuse;
// reporting this reference neither grants a lease nor activates imported data.
func (s *Store) PublishedApplicationCut() (ApplicationCutReference, error) {
	if s == nil {
		return ApplicationCutReference{}, ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(); err != nil {
		return ApplicationCutReference{}, err
	}
	if !s.meta.Gen.Publication.Limits.enabled() || !s.meta.Gen.Publication.Valid {
		return ApplicationCutReference{}, ErrInvalid
	}
	c := cutReference(s.meta)
	c.ConfState = proto.Clone(c.ConfState).(*pb.ConfState)
	return c, nil
}

// Publication-mode admission reserves fixed-membership metadata headroom for
// the maximum supported hard-state and snapshot index/term widths. This is
// specific to the new opt-in; legacy metadata limits are unchanged.
func checkPublicationHeadroom(m metadata, l Limits) error {
	if !m.Gen.Publication.Limits.enabled() {
		return nil
	}
	worst := m
	vote := m.App.Policy.LocalVoter
	if m.Rep.Config.enabled() {
		vote = m.Rep.Config.Voters[2]
	}
	worst.Hard = &pb.HardState{Term: new(uint64(math.MaxUint64)), Vote: new(vote), Commit: new(uint64(math.MaxUint64))}
	cs := &pb.ConfState{Voters: []uint64{m.App.Policy.LocalVoter}}
	if m.Rep.Config.enabled() {
		cs.Voters = append([]uint64(nil), m.Rep.Config.Voters[:]...)
		cs.AutoLeave = new(false)
	}
	worst.Conf = cs
	worst.Snap = &pb.Snapshot{Metadata: &pb.SnapshotMetadata{Index: new(uint64(math.MaxUint64)), Term: new(uint64(math.MaxUint64)), ConfState: cs}}
	if err := checkMetadataLimit(worst, l); err != nil {
		return err
	}
	if m.Rep.Config.enabled() && metadataBytes(worst)+readyEnvelopeBytes+unsignedLimit(descriptorBytesForContract(m.Rep.SemanticContractID, m.Transfer.Contract))+unsignedLimit(proto.Size(cs)) > unsignedLimit(l.MaxReadyBytes) {
		return ErrLimit
	}
	return nil
}

type publicationCapture struct {
	bank                                    byte
	generation, index, term, bytes, records uint64
	hash                                    [32]byte
	conf                                    *pb.ConfState
	image                                   []byte
	ref                                     *generationRef
	controlBytes, controlRecords            uint64
}

// publishApplicationCut is called with publicationMu held. It never scans an
// application namespace. Only one bounded image and descriptor are retained;
// store.mu is released during descriptor preparation, allowing later installs.
func (s *Store) publishApplicationCut() (err error) {
	s.mu.Lock()
	if err := s.check(); err != nil {
		s.mu.Unlock()
		return err
	}
	m := s.meta
	if m.Applied <= m.Base {
		s.mu.Unlock()
		return raft.ErrSnapOutOfDate
	}
	if err := s.verifyApplicationRoot(m.Applied); err != nil {
		s.poison = err
		s.mu.Unlock()
		return err
	}
	e, _, hash, err := s.get(m.Applied)
	if err != nil {
		s.poison = err
		s.mu.Unlock()
		return err
	}
	image, err := s.loadImage(imageKey, m.ImageBytes, m.ImageHash)
	if err != nil {
		s.poison = err
		s.mu.Unlock()
		return err
	}
	ref, err := s.pinGeneration(s.activeBank())
	if err != nil {
		s.mu.Unlock()
		return err
	}
	totalBytes, totalRecords := applicationTotals(m)
	c := publicationCapture{controlBytes: m.Controls.Bytes, controlRecords: m.Controls.Records, bank: s.activeBank(), generation: s.activeGeneration(), index: m.Applied, term: e.GetTerm(), bytes: totalBytes, records: totalRecords, hash: hash, conf: canonicalConf(m.Conf), image: image, ref: ref}
	hook := s.publicationCaptureHook
	s.mu.Unlock()
	defer func() { s.mu.Lock(); err = errors.Join(err, s.releaseGeneration(c.ref)); s.mu.Unlock() }()
	prospective := m
	prospective.Base, prospective.BaseTerm, prospective.BaseHash = c.index, c.term, c.hash
	prospective.Snap = &pb.Snapshot{Metadata: &pb.SnapshotMetadata{Index: new(c.index), Term: new(c.term), ConfState: c.conf}}
	prospective.SnapBytes = uint64(len(c.image))
	prospective.SnapHash = sha256.Sum256(c.image)
	prospective.Controls.Bytes, prospective.Controls.Records = c.controlBytes, c.controlRecords
	if err := bindPublication(&prospective, c.bank, c.generation, c.bytes, c.records); err != nil {
		return err
	}
	if err := checkMetadataLimit(prospective, s.limits); err != nil {
		return err
	}
	if hook != nil {
		hook()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(); err != nil {
		return err
	}
	current := s.meta
	if s.activeBank() != c.bank || s.activeGeneration() != c.generation {
		return ErrInvalid
	}
	if c.index <= current.Base {
		return raft.ErrSnapOutOfDate
	}
	if c.index > current.Applied || c.index > current.Hard.GetCommit() || c.conf.Equivalent(current.Conf) != nil {
		return ErrInvalid
	}
	retained, _, retainedHash, err := s.get(c.index)
	if err != nil {
		s.poison = err
		return err
	}
	if retained.GetTerm() != c.term || retainedHash != c.hash {
		s.poison = ErrCorrupt
		return ErrCorrupt
	}
	removed, err := s.rangeBytes(current.Base+1, c.index+1)
	if err != nil {
		s.poison = err
		return err
	}
	if removed > current.LogBytes {
		s.poison = ErrCorrupt
		return ErrCorrupt
	}
	current.Base, current.BaseTerm, current.BaseHash = c.index, c.term, c.hash
	current.LogBytes -= removed
	current.LogCount = current.Last - current.Base
	current.Snap = prospective.Snap
	current.SnapBytes = prospective.SnapBytes
	current.SnapHash = prospective.SnapHash
	current.Gen.Publication = prospective.Gen.Publication
	current.Rep.LastActivatedManifestID = [32]byte{}
	if err := s.validate(current); err != nil {
		return err
	}
	if err := checkMetadataLimit(current, s.limits); err != nil {
		return err
	}
	batch := s.db.NewBatch()
	fail := func(e error) error { return errors.Join(e, batch.Close()) }
	if err := batch.Set(snapshotKey, c.image, nil); err != nil {
		return fail(err)
	}
	if err := batch.DeleteRange(entryKey(0), entryKey(c.index+1), nil); err != nil {
		return fail(err)
	}
	if err := s.commit(current, batch); err != nil {
		return err
	}
	bank := byte(1) - s.activeBank()
	b := s.meta.Gen.Banks[bank]
	r := s.generationRefs[bank]
	if b.State == bankRetired && !holdsPublishedGeneration(s.meta, bank, b.Generation) && (r == nil || r.refs == 0) {
		return s.clearGenerationBank(bank, b.Generation)
	}
	return nil
}
