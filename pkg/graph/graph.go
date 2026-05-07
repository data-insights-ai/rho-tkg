package graph

import (
	"sync"
	"sync/atomic"

	snowflake "github.com/bds421/rho-snowflake-2026"
	eventspkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/events"
	indexpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/index"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/locks"
)

// Graph is the central graph layer. It owns the label and relationship type
// registries, snowflake ID generators, store, and provides string resolution
// for token-based entities.
//
// Entity locks serialize AddRelationship and DeleteNode on overlapping entities
// to prevent write-skew (concurrent AddRelationship(→X) + DeleteNodeCascade(X)
// producing a dangling edge).
type Graph struct {
	labels        *indexpkg.LabelRegistry
	relTypes      *indexpkg.RelTypeRegistry
	nodeIDGen     *snowflake.Node
	relIDGen      *snowflake.Node
	store         Store
	entityLocks   *locks.Manager
	validation    ValidationLimits
	constraints   ConstraintSet       // temporal constraints checked at relationship write time
	events        eventspkg.Publisher // nil = no event publishing; set via SetEventBus/SetAsyncEventBus
	txEventBuffer *[]Event            // non-nil while a tx holds g.mu.Lock — events buffered, not dispatched
	mu            sync.RWMutex        // serializes batch/tx writes vs standalone mutations and reads
	closeOnce     sync.Once

	// Index providers registered via RegisterIndexProvider. Keyed by Name().
	// Each entry holds an unsubscribe closure so UnregisterIndexProvider can
	// detach cleanly. See index_provider.go for semantics.
	indexProviders map[string]*indexProviderEntry

	// Operation counters — incremented atomically on every successful operation.
	opNodeAdds    atomic.Int64
	opNodeReads   atomic.Int64
	opNodeUpdates atomic.Int64
	opNodeDeletes atomic.Int64
	opRelAdds     atomic.Int64
	opRelReads    atomic.Int64
	opRelUpdates  atomic.Int64
	opRelDeletes  atomic.Int64
}
