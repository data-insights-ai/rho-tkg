package graphstore

import (
	"cmp"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

// GraphLimits bounds a complete reader's aggregate source work, planner output
// and all owned graph effects. Pages separately bounds each physical operation
// and one shared cursor pool; Catalog/application policy applies additionally.
// These are representation/work ledgers, not allocator or process RSS limits.
type GraphLimits struct {
	Pages                                         PageLimits
	Planner                                       graphstate.Limits
	MaxSourceRows, MaxSourceBytes, MaxOutputBytes int
}

// DefaultGraphLimits supplies finite local single-partition integration policy.
func DefaultGraphLimits() GraphLimits {
	return GraphLimits{Pages: DefaultPageLimits(), Planner: graphstate.DefaultLimits(), MaxSourceRows: 16384, MaxSourceBytes: 16 << 20, MaxOutputBytes: 16 << 20}
}

// Validate checks all source, planning, page and output bounds before work.
func (l GraphLimits) Validate() error { _, err := l.resolve(); return err }
func (l GraphLimits) resolve() (GraphLimits, error) {
	var err error
	l.Pages, err = l.Pages.resolve()
	if err != nil {
		return GraphLimits{}, err
	}
	if err := l.Planner.Validate(); err != nil {
		return GraphLimits{}, callerError(err)
	}
	d := DefaultGraphLimits()
	l.MaxSourceRows = cmp.Or(l.MaxSourceRows, d.MaxSourceRows)
	l.MaxSourceBytes = cmp.Or(l.MaxSourceBytes, d.MaxSourceBytes)
	l.MaxOutputBytes = cmp.Or(l.MaxOutputBytes, d.MaxOutputBytes)
	if l.MaxSourceRows < 1 || l.MaxSourceRows > 1<<20 || l.MaxSourceBytes < 1 || l.MaxSourceBytes > 64<<20 || l.MaxOutputBytes < 1 || l.MaxOutputBytes > 64<<20 {
		return GraphLimits{}, ErrInvalid
	}
	return l, nil
}

// GraphEffects owns validated graph effects for exactly one captured base. Root
// includes private physical reservations; semantic epoch/effect advancement is
// still the outer materializer's Root.AdvanceEffects contract. It is not a
// receipt, caller-Delta admission, allocator proof or installed application.
type GraphEffects struct {
	Base         raftlog.ApplicationRoot
	Root         Root
	Writes       []raftlog.KV
	Groups       []ComponentChangeGroup
	Dependencies []graphstate.Dependency
	Delta        graphstate.Delta
	Work         PageWork
	// OwnedBytes is conservative diagnostic retained representation accounting,
	// never authority/admission. Groups/Delta.Patches share scopes/changes/names;
	// Dependencies/Delta.Dependencies share backing. Writes are owned once.
	OwnedBytes int
}

var legacyFullTopology = topologyDeclaration{epoch: 1, schema: 1, index: 2}
var fullTopology = topologyDeclaration{epoch: 1, schema: 1, index: 3}

func isFullTopology(t topologyDeclaration) bool { return t == legacyFullTopology || t == fullTopology }

const fullStageMetadataBytes = 512
const fullStageBaseBytes = 128 + fullStageMetadataBytes
const fullViewMetadataBytes = 1536

type fullIndexDescriptor struct {
	owner, topology, schema, format uint64
	keys                            componentKeyTreeRoot
	unique, canonical, declared     postingTreeRoot
	own                             currentPresenceTreeRoot
}
type fullStageState struct {
	root       Root
	descriptor fullIndexDescriptor
	pages      PageLimits
}
