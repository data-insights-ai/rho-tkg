package raftlog

import (
	"bytes"
	"encoding/binary"
	"slices"

	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

// ApplicationReplicationConfig opts a fresh RLM6 store into exact fixed
// membership. All replicas agree Voters; ApplicationPolicy.LocalVoter remains
// local. Zero preserves singleton formats. This does not enable Driver traffic.
type ApplicationReplicationConfig struct{ Voters [3]uint64 }

func (c ApplicationReplicationConfig) enabled() bool { return c != (ApplicationReplicationConfig{}) }
func (c ApplicationReplicationConfig) validate(m metadata, l Limits) error {
	if !c.enabled() {
		return nil
	}
	if m.Transfer.Limits.MaxPinnedLogicalBytes < preparedSnapshotFixedBytes+applicationSnapshotClaimFixedBytes+unsignedLimit(m.App.Policy.MaxImageBytes) {
		return ErrInvalid
	}
	if !m.Gen.Limits.enabled() || !m.Gen.Publication.Limits.enabled() || !m.Transfer.enabled() || l.MaxSnapshotBytes < applicationSnapshotDescriptorLimit {
		return ErrInvalid
	}
	for j, id := range c.Voters {
		if id == 0 || raft.IsLocalMsgTarget(id) || j > 0 && id <= c.Voters[j-1] {
			return ErrInvalid
		}
	}
	if !slices.Contains(c.Voters[:], m.App.Policy.LocalVoter) {
		return ErrInvalid
	}
	return nil
}
func (c ApplicationReplicationConfig) matches(cs *pb.ConfState) bool {
	return cs != nil && len(cs.ProtoReflect().GetUnknown()) == 0 && slices.Equal(cs.GetVoters(), c.Voters[:]) && len(cs.GetVotersOutgoing()) == 0 && len(cs.GetLearners()) == 0 && len(cs.GetLearnersNext()) == 0 && !cs.GetAutoLeave()
}
func applicationConfiguration(m metadata, cs *pb.ConfState) bool {
	if m.Rep.Config.enabled() {
		return m.Rep.Config.matches(cs)
	}
	return localConfiguration(cs, m.App.Policy.LocalVoter)
}
func applicationVote(m metadata, vote uint64) bool {
	if vote == 0 {
		return true
	}
	if m.Rep.Config.enabled() {
		return slices.Contains(m.Rep.Config.Voters[:], vote)
	}
	return vote == m.App.Policy.LocalVoter
}

// LastActivatedManifestID applies only to an imported current Base. It is
// checksum-protected recovery/audit metadata, not proof of semantic correctness,
// quorum agreement, preparation or permission to activate. Advancing Applied
// preserves it; a later local publication clears it rather than reusing it for
// the new cut. Activation always requires a live verified claim independently.
type replicationMetadata struct {
	SemanticContractID      ApplicationSemanticContractID
	Config                  ApplicationReplicationConfig
	LastActivatedManifestID [32]byte
}

const replicationMetaBytes = 4 + 3*8 + 32

func appendReplicationMeta(b []byte, r replicationMetadata) []byte {
	version := byte(1)
	if r.SemanticContractID != (ApplicationSemanticContractID{}) {
		version = 2
	}
	b = append(b, 'A', 'R', version, 0)
	for _, id := range r.Config.Voters {
		b = binary.BigEndian.AppendUint64(b, id)
	}
	b = append(b, r.LastActivatedManifestID[:]...)
	if version == 2 {
		b = append(b, r.SemanticContractID[:]...)
	}
	return b
}
func decodeReplicationMeta(b []byte) (replicationMetadata, []byte, error) {
	if len(b) < replicationMetaBytes || (!bytes.Equal(b[:4], []byte{'A', 'R', 1, 0}) && !bytes.Equal(b[:4], []byte{'A', 'R', 2, 0})) {
		return replicationMetadata{}, nil, ErrCorrupt
	}
	var r replicationMetadata
	for j := range r.Config.Voters {
		r.Config.Voters[j] = binary.BigEndian.Uint64(b[4+j*8:])
	}
	copy(r.LastActivatedManifestID[:], b[28:60])
	tail := b[60:]
	if b[2] == 2 {
		if len(tail) < 32 {
			return r, nil, ErrCorrupt
		}
		copy(r.SemanticContractID[:], tail[:32])
		tail = tail[32:]
		if r.SemanticContractID == (ApplicationSemanticContractID{}) {
			return r, nil, ErrCorrupt
		}
	}
	return r, tail, nil
}
func validateReplicationMeta(m metadata, l Limits) error {
	if !m.Rep.Config.enabled() {
		if m.Rep != (replicationMetadata{}) {
			return ErrInvalid
		}
		return nil
	}
	if err := m.Rep.Config.validate(m, l); err != nil {
		return err
	}
	if m.Applied > 0 && (!m.Rep.Config.matches(m.Conf) || !m.Rep.Config.matches(m.Snap.GetMetadata().GetConfState())) {
		return ErrInvalid
	}
	if m.Base == 0 && m.Rep.LastActivatedManifestID != [32]byte{} {
		return ErrInvalid
	}
	if !applicationVote(m, m.Hard.GetVote()) {
		return ErrInvalid
	}
	return nil
}

// ApplicationActivationAudit reports a non-authoritative imported current Base;
// a fresh or locally republished Base reports zero. It grants no lease/token.
type ApplicationActivationAudit struct {
	Index, Term       uint64
	CutID, ManifestID [32]byte
}

// ApplicationActivationAudit returns owned fixed audit fields after checking
// Store lifetime. It never resurrects a prepared capability on recovery.
func (s *Store) ApplicationActivationAudit() (ApplicationActivationAudit, error) {
	if s == nil {
		return ApplicationActivationAudit{}, ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(); err != nil {
		return ApplicationActivationAudit{}, err
	}
	if !s.meta.Rep.Config.enabled() {
		return ApplicationActivationAudit{}, ErrInvalid
	}
	id := s.meta.Rep.LastActivatedManifestID
	if id == [32]byte{} {
		return ApplicationActivationAudit{}, nil
	}
	return ApplicationActivationAudit{Index: s.meta.Base, Term: s.meta.BaseTerm, CutID: s.meta.Gen.Publication.ID, ManifestID: id}, nil
}
