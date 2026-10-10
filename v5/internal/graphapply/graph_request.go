package graphapply

import (
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/idalloc"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
)

const (
	initGraph              commandKind = 5
	graphOperations        commandKind = 6
	guardedGraphOperations commandKind = 7
)

type bindingRole byte

const (
	entityBinding bindingRole = iota + 1
	lifeBinding
	valueBinding
)

type grantReference struct {
	session  idalloc.RecipientSession
	sequence uint64
}

// Numeric LifeIDs remain owner qualified. Claims prove actual newly bound
// identities, not globally single-use untyped numbers or cursor authority.
type freshBinding struct {
	role  bindingRole
	owner graphstate.EntityID
	id    uint64
	grant grantReference
}

func isGraphMutation(kind commandKind) bool {
	return kind == graphOperations || kind == guardedGraphOperations
}
func isGraphCommand(kind commandKind) bool { return kind == initGraph || isGraphMutation(kind) }
func validGraphWire(version byte, kind commandKind) bool {
	return version == 2 && (kind == initGraph || kind == graphOperations) || version == 3 && kind == guardedGraphOperations
}

type graphRequest struct {
	readBase   graphReadBase
	ns         namespace
	kind       commandKind
	id         requestID
	attempt    bootstrapAttemptID
	authority  idalloc.Authority
	maxBlock   uint64
	schemas    []graphstate.PropertyDefinition
	revision   state.Revision
	operations []graphstate.Operation
	claims     []freshBinding
}

func (r graphRequest) identity() [16]byte {
	if r.kind == initGraph {
		return [16]byte(r.attempt)
	}
	return [16]byte(r.id)
}

type materializerLimits struct {
	allocation                                                                     limits
	catalog                                                                        graphstore.Limits
	graph                                                                          graphstore.GraphLimits
	commandBytes, commandOwnedBytes, maxOperations, maxClaims, maxSchemas, maxAxes int
	sourceRows, sourceBytes, stageRows, stageBytes, outputBytes, changeBytes       int
}

func defaultMaterializerLimits() materializerLimits {
	return materializerLimits{allocation: limits{512, 1024, 4 << 20, 4096, 16 << 20, 16 << 20}, catalog: graphstore.DefaultLimits(), graph: graphstore.DefaultGraphLimits(), commandBytes: 1 << 20, commandOwnedBytes: 8 << 20, maxOperations: 128, maxClaims: 384, maxSchemas: 64, maxAxes: 384, sourceRows: 16384, sourceBytes: 16 << 20, stageRows: 1024, stageBytes: 16 << 20, outputBytes: 32 << 20, changeBytes: 4 << 20}
}
func (l materializerLimits) validate() error {
	if err := l.allocation.validate(); err != nil {
		return err
	}
	if err := l.catalog.Validate(); err != nil {
		return err
	}
	if err := l.graph.Validate(); err != nil {
		return err
	}
	for _, n := range []int{l.commandBytes, l.commandOwnedBytes, l.maxOperations, l.maxClaims, l.maxSchemas, l.maxAxes, l.sourceRows, l.sourceBytes, l.stageRows, l.stageBytes, l.outputBytes, l.changeBytes} {
		if n < 1 || n > 64<<20 {
			return errInvalid
		}
	}
	if l.commandBytes > 4<<20 || l.maxOperations > 4096 || l.maxClaims > 12288 || l.maxSchemas > 4096 || l.maxAxes > 12288 || l.stageRows > 4096 || l.sourceRows > 1<<20 {
		return errInvalid
	}
	return nil
}

// Allowances cover fixed metadata independently tested against typed Go values;
// variable backing is separately charged. These are not heap/RSS guarantees.
const graphRequestMetadataBytes = 384
const operationMetadataBytes = 640
const claimMetadataBytes = 128
const axisMetadataBytes = 128
const graphResultMetadataBytes = 512
