package graphapply

import (
	"crypto/sha256"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

// A fresh native binding adds range handoff. Certified-v1 stores and binaries
// keep their original agreement; a decoder never upgrades their authority.
func rangeMoveSemanticContractID() raftlog.ApplicationSemanticContractID {
	base := certifiedGraphSemanticContractID()
	return raftlog.ApplicationSemanticContractID(sha256.Sum256(append(base[:], []byte("range-move=streamed-logical-and-protocol-preservation-v2\x00admission=coarse-resolved-fence-v1\x00placement=stable-range-epoch-v1\x00replay=original-qualified-identity-v1\x00retention=source-certified-lineage-v1\x00recovery=protected-pages-and-precommit-abort-v1\x00")...)))
}

func (m *declaredMaterializer) rangeMoves() bool {
	return m != nil && rangeMoveGraphAgreement(m.SemanticContractID())
}

func rangeMoveGraphAgreement(id raftlog.ApplicationSemanticContractID) bool {
	return id == rangeMoveSemanticContractID() || conditionalGraphAgreement(id)
}

func newRangeMoveMaterializer(s *raftlog.Store, n namespace, d graphstore.OwnershipDeclaration, l materializerLimits) (*declaredMaterializer, error) {
	if s == nil || s.ApplicationControls().Version != 1 || s.ApplicationRecovery().Version != 1 {
		return nil, errInvalid
	}
	return newDeclaredMaterializerWithAgreement(s, n, d, l, rangeMoveSemanticContractID())
}
