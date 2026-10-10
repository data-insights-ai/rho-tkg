package raftlog

// ApplicationSemanticContractID identifies an explicit versioned agreement on
// application meaning. The materializer supplies it; storage never derives it
// from wire bounds, local physical limits, generation or voter identity.
// Zero preserves unbound legacy formats and does not enable replica traffic.
type ApplicationSemanticContractID [32]byte

// ApplicationBinding is immutable namespace and semantic configuration, not a
// cut, lease, quorum certificate or authority to activate/supply traffic.
type ApplicationBinding struct {
	Identity           ApplicationIdentity
	SemanticContractID ApplicationSemanticContractID
}

// Validate requires an explicit nonzero semantic agreement and complete scope.
func (b ApplicationBinding) Validate() error {
	if b.SemanticContractID == (ApplicationSemanticContractID{}) {
		return ErrInvalid
	}
	return b.Identity.validate()
}

// ApplicationBinding returns immutable configured scope by value. Nil and
// unbound stores return zero; a closed store may report configuration but grants
// no validity/lease. All metadata reads remain serialized with Store commits.
func (s *Store) ApplicationBinding() ApplicationBinding {
	if s == nil {
		return ApplicationBinding{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.meta.Rep.SemanticContractID == (ApplicationSemanticContractID{}) {
		return ApplicationBinding{}
	}
	return ApplicationBinding{Identity: s.meta.Transfer.Identity, SemanticContractID: s.meta.Rep.SemanticContractID}
}
func semanticManifestVersion(id ApplicationSemanticContractID) uint32 {
	if id == (ApplicationSemanticContractID{}) {
		return 2
	}
	return 3
}
func replicationMetadataBytes(r replicationMetadata) uint64 {
	n := uint64(replicationMetaBytes)
	if r.SemanticContractID != (ApplicationSemanticContractID{}) {
		n += 32
	}
	return n
}
func descriptorFixedBytes(id ApplicationSemanticContractID) int {
	n := applicationSnapshotDescriptorFixedBytes
	if id != (ApplicationSemanticContractID{}) {
		n += 32
	}
	return n
}
