package raftlog

import (
	"bytes"
	"context"
	"encoding/binary"

	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

const applicationSnapshotDescriptorLimit = 512
const applicationSnapshotDescriptorFixedBytes = 4 + 40 + 80 + 5*8 + 3*32 + 4
const preparedSnapshotFixedBytes uint64 = 1024
const applicationSnapshotClaimFixedBytes uint64 = 2048

// EncodeApplicationSnapshotDescriptor binds an AS2/AS3 cut and verified manifest.
// It contains no application image, local generation/voter policy or HardState.
// Encoding does not prove receipt, Raft acceptance or authorize activation.
func EncodeApplicationSnapshotDescriptor(m ApplicationSnapshotManifest) ([]byte, error) {
	if m.Version != 2 && m.Version != 3 {
		return nil, ErrInvalid
	}
	if err := validateManifest(m); err != nil {
		return nil, err
	}
	if proto.Size(m.ConfState) > 64 {
		return nil, ErrLimit
	}
	cs, err := proto.MarshalOptions{Deterministic: true}.Marshal(m.ConfState)
	if err != nil {
		return nil, err
	}
	b := make([]byte, 0, descriptorFixedBytes(m.SemanticContractID)+len(cs))
	version := byte(1)
	if m.Version == 3 {
		version = 2
	}
	b = append(b, 'A', 'D', version, 0)
	if m.Version == 3 {
		b = append(b, m.SemanticContractID[:]...)
	}
	b = appendIdentity(b, m.Identity)
	b = appendContract(b, m.Contract)
	totals, rows, err := snapshotTotals(m)
	if err != nil {
		return nil, err
	}
	for _, n := range []uint64{m.Index, m.Term, uint64(len(m.Image)), totals, rows} {
		b = binary.BigEndian.AppendUint64(b, n)
	}
	b = append(b, m.ImageHash[:]...)
	b = append(b, m.CutID[:]...)
	id := publishedManifestID(m)
	b = append(b, id[:]...)
	b = binary.BigEndian.AppendUint32(b, uint32(len(cs)))
	b = append(b, cs...)
	if len(b) > applicationSnapshotDescriptorLimit {
		return nil, ErrLimit
	}
	return b, nil
} // #nosec G115 -- ConfState wire size checked <=64 before uint32 encoding.

// PreparedApplicationSnapshot is a receiver-local, same-Store capability for a
// final synced verified AS2/AS3 import. It never proves Raft accepted the snapshot.
// A live claim freezes Abort/Prepared.Close; only that claim's owner can release
// it. Dormant evidence remains after a non-activating claim is released.
type PreparedApplicationSnapshot struct {
	reservation                uint64
	s                          *Store
	i                          *ApplicationImport
	bank                       byte
	generation, baseGeneration uint64
	descriptor                 []byte
	manifestID                 [32]byte
	claim                      *ApplicationSnapshotClaim
	closed                     bool
}

// ApplicationSnapshotClaim owns one expected snapshot and captured generation
// reference for one trusted Raft step/drain. PersistApplicationReady requires
// the actual nonempty Ready.Snapshot to match it. Close returns an unused token
// to its caller; after activation it releases only captured references.
type ApplicationSnapshotClaim struct {
	reservation      uint64
	p                *PreparedApplicationSnapshot
	expected         *pb.Snapshot
	image            []byte
	ref              *generationRef
	manifestID       [32]byte
	closed, consumed bool
}

// Prepare admits one bounded local capability only after the AS2/AS3 import is
// final and its complete verification seal has been synced. It scans no source
// rows and is idempotent for the same live import.
func (i *ApplicationImport) Prepare() (*PreparedApplicationSnapshot, error) {
	if i == nil || i.s == nil {
		return nil, ErrInvalid
	}
	s := i.s
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := i.check(context.Background()); err != nil {
		return nil, err
	}
	if !s.meta.Rep.Config.enabled() || i.manifest.Version != semanticManifestVersion(s.meta.Rep.SemanticContractID) || i.manifest.SemanticContractID != s.meta.Rep.SemanticContractID || !i.final || !i.verified || !s.meta.Rep.Config.matches(i.manifest.ConfState) {
		return nil, ErrInvalid
	}
	if i.prepared != nil {
		return i.prepared, nil
	}
	if err := s.admitSnapshotOwnership(preparedSnapshotFixedBytes); err != nil {
		return nil, err
	}
	descriptor, err := EncodeApplicationSnapshotDescriptor(i.manifest)
	if err != nil {
		return nil, err
	}
	p := &PreparedApplicationSnapshot{reservation: preparedSnapshotFixedBytes, s: s, i: i, bank: i.bank, generation: i.generation, baseGeneration: s.activeGeneration(), descriptor: descriptor, manifestID: i.id}
	i.prepared = p
	s.pinnedApplicationBytes += p.reservation
	return p, nil
}
func (p *PreparedApplicationSnapshot) live() error {
	if p.closed {
		return ErrClosed
	}
	if err := p.s.check(); err != nil {
		return err
	}
	i := p.i
	if p.s.applicationImport != i || i.closed || i.prepared != p || !i.verified || !i.final || i.bank != p.bank || i.generation != p.generation || p.s.activeGeneration() != p.baseGeneration || p.s.meta.Gen.Banks[p.bank].State != bankStaging || p.s.meta.Gen.Banks[p.bank].Generation != p.generation {
		return ErrInvalid
	}
	return nil
}

// Claim clones the bounded expected snapshot; later caller mutation cannot
// change the binding. The caller keeps expected unchanged during this call.
func (p *PreparedApplicationSnapshot) Claim(expected *pb.Snapshot) (*ApplicationSnapshotClaim, error) {
	if p == nil || p.s == nil || p.i == nil || expected == nil {
		return nil, ErrInvalid
	}
	s := p.s
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := p.live(); err != nil {
		return nil, err
	}
	if p.claim != nil {
		return nil, ErrLimit
	}
	if len(expected.Data) > applicationSnapshotDescriptorLimit || proto.Size(expected.GetMetadata()) > 128 {
		return nil, ErrLimit
	}
	m := p.i.manifest
	if len(expected.ProtoReflect().GetUnknown()) != 0 || expected.GetMetadata() == nil || len(expected.GetMetadata().ProtoReflect().GetUnknown()) != 0 || expected.GetMetadata().GetIndex() != m.Index || expected.GetMetadata().GetTerm() != m.Term || !s.meta.Rep.Config.matches(expected.GetMetadata().GetConfState()) || !proto.Equal(expected.GetMetadata().GetConfState(), m.ConfState) || !bytes.Equal(expected.Data, p.descriptor) {
		return nil, ErrInvalid
	}
	if len(s.applicationSnapshotClaims) >= 2 {
		return nil, ErrLimit
	}
	reservation := applicationSnapshotClaimFixedBytes + uint64(cap(p.i.manifest.Image))
	if err := s.admitSnapshotOwnership(reservation); err != nil {
		return nil, err
	}
	ref, err := s.pinGeneration(p.bank)
	if err != nil {
		return nil, err
	}
	c := &ApplicationSnapshotClaim{reservation: reservation, p: p, expected: proto.Clone(expected).(*pb.Snapshot), image: p.i.manifest.Image, ref: ref, manifestID: p.manifestID}
	p.claim = c
	if s.applicationSnapshotClaims == nil {
		s.applicationSnapshotClaims = make(map[*ApplicationSnapshotClaim]struct{})
	}
	s.applicationSnapshotClaims[c] = struct{}{}
	s.pinnedApplicationBytes += c.reservation
	return c, nil
}

// Close revokes an unclaimed prepared import. ErrLimit preserves an in-flight
// claim; it cannot be revoked by a caller retaining only Prepared.
func (p *PreparedApplicationSnapshot) Close() error {
	if p == nil || p.s == nil || p.i == nil {
		return ErrInvalid
	}
	s := p.s
	s.mu.Lock()
	defer s.mu.Unlock()
	if p.closed {
		return nil
	}
	if p.claim != nil {
		return ErrLimit
	}
	return p.i.abortLocked()
}

// Close releases this exact captured owner, including a consumed image. An
// unused claim returns its still-dormant Prepared capability to caller ownership.
func (c *ApplicationSnapshotClaim) Close() error {
	if c == nil || c.p == nil || c.p.s == nil || c.p.i == nil {
		return ErrInvalid
	}
	s := c.p.s
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.invalidateClaim(c)
}
func (s *Store) invalidateClaim(c *ApplicationSnapshotClaim) error {
	if c.closed {
		return nil
	}
	c.closed = true
	s.pinnedApplicationBytes -= c.reservation
	c.reservation = 0
	if c.p.claim == c {
		c.p.claim = nil
	}
	c.expected = nil
	c.image = nil
	ref := c.ref
	c.ref = nil
	delete(s.applicationSnapshotClaims, c)
	return s.releaseGeneration(ref)
}
func (s *Store) invalidatePrepared(i *ApplicationImport) error {
	p := i.prepared
	if p == nil {
		return nil
	}
	s.releasePreparedOwnership(p)
	p.descriptor = nil
	i.prepared = nil
	c := p.claim
	if c == nil {
		return nil
	}
	return s.invalidateClaim(c)
}

// Ownership shares the finite transfer pin ceiling with exports. It reserves
// fixed object/descriptor/protobuf capacity separately from any DB-byte charge;
// claims additionally charge the exact image backing capacity until Close,
// including after activation transfers import ownership to the active bank.
func (s *Store) admitSnapshotOwnership(n uint64) error {
	limit := s.meta.Transfer.Limits.MaxPinnedLogicalBytes
	if s.pinnedApplicationBytes > limit || n > limit-s.pinnedApplicationBytes {
		return ErrLimit
	}
	return nil
}
func (s *Store) releasePreparedOwnership(p *PreparedApplicationSnapshot) {
	p.closed = true
	p.descriptor = nil
	s.pinnedApplicationBytes -= p.reservation
	p.reservation = 0
}
