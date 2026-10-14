package graphapply

import (
	"crypto/sha256"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

// A fresh binding is mandatory: the earlier write-only agreement does not
// promise initialized-empty maps or round-versioned routing publications.
func certifiedGraphSemanticContractID() raftlog.ApplicationSemanticContractID {
	b := crossPartitionSemanticContractID()
	return raftlog.ApplicationSemanticContractID(sha256.Sum256(append(b[:], []byte("certified-graph-reads=v1\x00round-prefix=graph-and-routing-v1\x00separate-fence=1\x00")...)))
}
func crossGraphAgreement(id raftlog.ApplicationSemanticContractID) bool {
	return id == crossPartitionSemanticContractID() || certifiedGraphAgreement(id)
}
func (m *declaredMaterializer) certifiedReads() bool {
	return m != nil && certifiedGraphAgreement(m.SemanticContractID())
}

func certifiedGraphAgreement(id raftlog.ApplicationSemanticContractID) bool {
	return id == certifiedGraphSemanticContractID() || rangeMoveGraphAgreement(id)
}
func newCertifiedGraphMaterializer(s *raftlog.Store, n namespace, d graphstore.OwnershipDeclaration, l materializerLimits) (*declaredMaterializer, error) {
	if s == nil || s.ApplicationControls().Version != 1 || s.ApplicationRecovery().Version != 1 {
		return nil, errInvalid
	}
	return newDeclaredMaterializerWithAgreement(s, n, d, l, certifiedGraphSemanticContractID())
}
