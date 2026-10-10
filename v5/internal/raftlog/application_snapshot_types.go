package raftlog

import (
	"context"
	"math"

	pb "go.etcd.io/raft/v3/raftpb"
)

// ApplicationIdentity durably binds graph, logical partition and a stable Raft
// group incarnation. It never identifies a voter, server, root or ownership epoch.
type ApplicationIdentity struct {
	Graph     [16]byte
	Partition uint64
	Group     [16]byte
}

func (n ApplicationIdentity) validate() error {
	if n.Graph == ([16]byte{}) || n.Group == ([16]byte{}) || n.Partition == 0 {
		return ErrInvalid
	}
	return nil
}

// ApplicationContract versions shared storage/wire and staging/read bounds.
// It does not bind materializer meaning; an explicit ApplicationSemanticContractID
// supplies that agreement. Local voters, retained quotas, views and reclamation
// remain local. This prerequisite does not yet integrate a replicated machine.
type ApplicationContract struct {
	Version                                                            uint32
	MaxKeyBytes, MaxValueBytes, MaxImageBytes                          int
	MaxPageRows, MaxPageBytes                                          int
	MaxInstallWrites, MaxInstallBytes, MaxChangeBytes, MaxOutcomeBytes int
}

// ApplicationContractForPolicy returns explicit shared bounds, without local identity.
func ApplicationContractForPolicy(p ApplicationPolicy) ApplicationContract {
	return ApplicationContract{1, p.MaxKeyBytes, p.MaxValueBytes, p.MaxImageBytes, p.MaxPageRows, p.MaxPageBytes, p.MaxInstallWrites, p.MaxInstallBytes, p.MaxChangeBytes, p.MaxOutcomeBytes}
}
func (c ApplicationContract) numbers() []uint64 {
	return []uint64{uint64(c.Version), unsignedLimit(c.MaxKeyBytes), unsignedLimit(c.MaxValueBytes), unsignedLimit(c.MaxImageBytes), unsignedLimit(c.MaxPageRows), unsignedLimit(c.MaxPageBytes), unsignedLimit(c.MaxInstallWrites), unsignedLimit(c.MaxInstallBytes), unsignedLimit(c.MaxChangeBytes), unsignedLimit(c.MaxOutcomeBytes)}
}
func (c ApplicationContract) validate() error {
	if c.Version != 1 && c.Version != 2 {
		return ErrInvalid
	}
	for _, n := range c.numbers()[1:] {
		if n == 0 || n > 64<<20 {
			return ErrInvalid
		}
	}
	if c.MaxKeyBytes > 64<<10 || c.MaxValueBytes > 16<<20 || c.MaxImageBytes > 64<<20 || c.MaxPageRows > 4096 || c.MaxInstallWrites > 4096 {
		return ErrInvalid
	}
	return nil
}

// ApplicationTransferLimits bound transfer-owned capacities and logical staging.
// One dormant import occupies the inactive bank until Abort. MaxExports and
// MaxPinnedLogicalBytes bound snapshot handle count and a conservative captured
// application+log+metadata+dormant logical byte sum, NOT Pebble's pinned SST/WAL/compaction disk,
// allocator-rounded memory, OS cache or RSS. Actual physical costs need measurement.
type ApplicationTransferLimits struct {
	MaxExports, MaxChunkRows, MaxChunkBytes                 int
	MaxStagedBytes, MaxStagedRecords, MaxPinnedLogicalBytes uint64
}

// DefaultApplicationTransferLimits supplies finite experimental settings, not SLOs.
func DefaultApplicationTransferLimits() ApplicationTransferLimits {
	return ApplicationTransferLimits{2, 4096, 4 << 20, 1 << 30, 1_000_000, 3 << 30}
}
func (l ApplicationTransferLimits) validate() error {
	if l.MaxExports < 1 || l.MaxExports > 16 || l.MaxChunkRows < 1 || l.MaxChunkRows > 4096 || l.MaxChunkBytes < 64 || l.MaxChunkBytes > 32<<20 || l.MaxStagedBytes < 1 || l.MaxStagedBytes > 1<<40 || l.MaxStagedRecords < 1 || l.MaxStagedRecords > 1<<40 || l.MaxPinnedLogicalBytes < 1 || l.MaxPinnedLogicalBytes > 1<<40 {
		return ErrInvalid
	}
	return nil
}

// ApplicationTransferConfig is an explicit fresh-store opt-in. All fields must
// match on reopen. Existing unbound stores cannot silently acquire an identity.
type ApplicationTransferConfig struct {
	Identity ApplicationIdentity
	Contract ApplicationContract
	Limits   ApplicationTransferLimits
}

func (c ApplicationTransferConfig) enabled() bool { return c != (ApplicationTransferConfig{}) }
func (c ApplicationTransferConfig) validate(p ApplicationPolicy) error {
	if !c.enabled() {
		return nil
	}
	if !p.Enabled() || c.Contract.Version != 1 && c.Contract.Version != 2 || func() bool {
		want := ApplicationContractForPolicy(p)
		want.Version = c.Contract.Version
		return c.Contract != want
	}() {
		return ErrInvalid
	}
	if err := c.Identity.validate(); err != nil {
		return err
	}
	if err := c.Contract.validate(); err != nil {
		return err
	}
	return c.Limits.validate()
}

// ApplicationSnapshotManifest is portable retained application evidence at an
// immutable local applied index, not a certified database cut. Image and ConfState
// are owned bounded copies. Local generation IDs/policies and HardState are absent.
// The digest binds ordered canonical records; ManifestID binds all metadata too.
// Version zero or one selects the original AS1 bytes; AS1 decode normalizes to
// zero. Version two binds an unbound CutID; version three requires and binds
// SemanticContractID. Older formats require its zero value. Other versions refuse.
type ApplicationSnapshotManifest struct {
	SemanticContractID               ApplicationSemanticContractID
	Version                          uint32
	CutID                            [32]byte
	Identity                         ApplicationIdentity
	Contract                         ApplicationContract
	Index, Term                      uint64
	ConfState                        *pb.ConfState
	Image                            []byte
	ImageHash, RecordsHash           [32]byte
	NamespaceBytes, NamespaceRecords [4]uint64
	ControlBytes, ControlRecords     uint64
}

// ApplicationSnapshotChunk is one bounded ordered canonical page. Final marks
// exact completion. AS1 (Version zero/one) rejects empty non-final pages and
// requires all AS2 fields zero. AS2 counts visited physical work, including
// skipped future rows; those sender-declared counts are limits, not evidence
// proving unseen data. AS3 uses the same progress fields with semantic-bound
// CutID/ManifestID and Version three. After is an owned canonical progress cursor.
type ApplicationSnapshotChunk struct {
	Version               uint32
	CutID, ManifestID     [32]byte
	After                 []byte
	Visited, VisitedBytes uint64
	Sequence              uint64
	Data                  []byte
	Final                 bool
}

// ApplicationImportStatus reports dormant evidence only, with no activation authority.
type ApplicationImportStatus struct {
	ManifestID                   [32]byte
	Bytes, Records, NextSequence uint64
	Final, Verified              bool
}

// ApplicationTransferUsage separates active logical retention, staged logical
// records and snapshot/capability-owned capacity/count. Prepared and claim
// reservations share the pin ceiling; ClaimImageBytes remains charged after
// activation until claim Close. ClaimImageBytes is a subset of ClaimBytes;
// PreparedBytes and ClaimBytes are included in PinnedLogicalBytes. Physical
// pinned storage is not known
// from these counters; every export snapshots the whole shared Pebble database.
type ApplicationTransferUsage struct {
	Prepared, Claims                                                           int
	PreparedBytes, ClaimBytes, ClaimImageBytes                                 uint64
	ActiveBytes, ActiveRecords, StagedBytes, StagedRecords, PinnedLogicalBytes uint64
	Exports, ExportImageBytes, ImportImageBytes, VerifierImageBytes            int
	Import, Verified, Verifier                                                 bool
}

// ApplicationTransferUsage returns the serialized local transfer ledger.
func (s *Store) ApplicationTransferUsage() (ApplicationTransferUsage, error) {
	if s == nil {
		return ApplicationTransferUsage{}, ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(); err != nil {
		return ApplicationTransferUsage{}, err
	}
	if !s.meta.Transfer.enabled() {
		return ApplicationTransferUsage{}, ErrInvalid
	}
	totalBytes, totalRecords := applicationTotals(s.meta)
	u := ApplicationTransferUsage{ActiveBytes: totalBytes, ActiveRecords: totalRecords, PinnedLogicalBytes: s.pinnedApplicationBytes, Exports: len(s.applicationExports)}
	for e := range s.applicationExports {
		u.ExportImageBytes += len(e.manifest.Image)
	}
	for c := range s.applicationSnapshotClaims {
		u.Claims++
		u.ClaimBytes += c.reservation
		u.ClaimImageBytes += uint64(cap(c.image))
	}
	if i := s.applicationImport; i != nil && i.prepared != nil {
		u.Prepared = 1
		u.PreparedBytes = i.prepared.reservation
	}
	u.Verifier = s.applicationVerifier
	u.VerifierImageBytes = s.applicationVerifierImageBytes
	if i := s.applicationImport; i != nil {
		u.ImportImageBytes = len(i.manifest.Image)
		u.Import = true
		u.Verified = i.verified
		u.StagedBytes = i.state.bytes
		u.StagedRecords = i.state.rows
	}
	return u, nil
}
func snapshotContext(ctx context.Context) error {
	if ctx == nil {
		return ErrInvalid
	}
	return ctx.Err()
}
func snapshotTotals(m ApplicationSnapshotManifest) (uint64, uint64, error) {
	var b, r uint64
	for j := range 4 {
		if m.NamespaceBytes[j] > math.MaxUint64-b || m.NamespaceRecords[j] > math.MaxUint64-r {
			return 0, 0, ErrLimit
		}
		b += m.NamespaceBytes[j]
		r += m.NamespaceRecords[j]
	}
	if m.ControlBytes > math.MaxUint64-b || m.ControlRecords > math.MaxUint64-r {
		return 0, 0, ErrLimit
	}
	return b + m.ControlBytes, r + m.ControlRecords, nil
}
