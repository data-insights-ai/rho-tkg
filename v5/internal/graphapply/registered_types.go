package graphapply

import (
	"crypto/sha256"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

// Private codec data grants no live ownership, readiness or execution authority.

const registeredSemanticDescriptor = "rho-tkg:graphapply:registered-bootstrap:semantic-contract:v1\x00" +
	"scope=genesis-complete-v1\x00" +
	"genesis=actual-entry-before-register\x00" +
	"lookup=hki1-v1\x00" +
	"prepared=pij1-bootstrap-v1\x00" +
	"submission=sij1-bootstrap-v1\x00" +
	"bootstrap-domain=canonical-native-policy-v1\x00" +
	"revision=none\x00" +
	"graph-effects=none\x00" +
	"recovery=current-term-applied-v1\x00" +
	"legacy-dispatch=separate\x00"

// Derive once at package initialization; per-operation calls return a value copy.
var registeredCompiledAgreement = raftlog.ApplicationSemanticContractID(sha256.Sum256([]byte(registeredSemanticDescriptor)))

func registeredSemanticContractID() raftlog.ApplicationSemanticContractID {
	return registeredCompiledAgreement
}

type registeredOriginScope struct {
	graph     [16]byte
	partition uint64
	group     [16]byte
	lineage   [32]byte
	protocol  uint16
}

// These are immutable format allowances, distinct from new admission caps and
// current operation ownership/source/output/storage allowances.
type registeredFormatBudget struct {
	preparedBytes, domainBytes, axes, descriptorBytes int
	schemas, schemaNameBytes, walkBytes               int
}

type registeredMappingSpec struct {
	tag                  byte
	version              uint16
	sourceReference      string
	targetAxis           temporal.AxisID
	targetDefinitionHash [32]byte
	rule                 byte
}

type registeredDomainPolicy struct {
	version             uint16
	flags               byte
	axes                []temporal.AxisDescriptor
	defaultValidityAxis temporal.AxisID
	mapping             registeredMappingSpec
}

type registeredAllocatorIntent struct {
	tag      byte
	owner    [16]byte
	epoch    uint64
	maxBlock uint64
}

type registeredBootstrapSpec struct {
	kind        byte
	domain      registeredDomainPolicy
	schemas     []graphstate.PropertyDefinition
	allocator   registeredAllocatorIntent
	revisionTag byte
}
