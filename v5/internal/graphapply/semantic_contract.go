package graphapply

import (
	"crypto/sha256"

	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

// One reviewed agreement, not six configurable or independently negotiable
// policies. Versions name current-root command meaning; exact native axis/value
// laws; component correction/lifecycle/schema/uniqueness; qualified identities
// and independent allocator/recipient fences; request/bootstrap/recovery rules;
// and logical CDC/epoch/digest canonicalization. Meaning changes require an
// explicit new agreement and conformance evidence.
const semanticContractDescriptor = "rho-tkg:graphapply:semantic-contract:v1\x00" +
	"current-root-commands=1\x00" +
	"typed-native-values=1\x00" +
	"graph-components=1\x00" +
	"allocation-admission=1\x00" +
	"request-recovery=1\x00" +
	"logical-effects=1\x00"

// SemanticContractID returns the static database-meaning agreement by value.
// Physical root/page/index/container formats, compression, local ordinals,
// generations/cursors, namespace/group/voters and operational limits are not
// inputs. Their compatibility and lifetime checks remain separate. Rho supplies
// database values/corrections; sigma owns inference and reasoning execution.
// This identifier grants no live-machine, coverage, cut or traffic authority.
func SemanticContractID() raftlog.ApplicationSemanticContractID {
	return raftlog.ApplicationSemanticContractID(sha256.Sum256([]byte(semanticContractDescriptor)))
}

// SemanticContractID reports this implementation's static capability even for
// a nil receiver. Driver machine/lifetime checks must independently reject nil
// or unavailable machines before using any callback or enabling traffic.
func (*materializer) SemanticContractID() raftlog.ApplicationSemanticContractID {
	return SemanticContractID()
}
