package graph

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/dgraph-io/badger/v4/options"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// Sentinel errors for entity management.
var (
	ErrNoLabels         = errors.New("graph: node requires at least one label")
	ErrNilNode          = errors.New("graph: node must not be nil")
	ErrZeroID           = errors.New("graph: zero ID is not valid for import")
	ErrNotTieredStore   = errors.New("graph: operation requires TieredStore")
	ErrAlreadyClosed    = errors.New("graph: entity already closed")
	ErrInvalidTimeRange = errors.New("graph: invalid time range")
	ErrLabelNotFound    = errors.New("graph: node does not have the specified label")
	ErrLastLabel        = errors.New("graph: cannot remove the last label from a node")
	// ErrDepthTemporalUnsupported is returned when QueryOpts combines a
	// non-default Depth with a temporal filter. The history-aware
	// resolution path enumerates IDs through ForEach* iterators that have
	// no QueryOpts, so the underlying Store cannot honor Depth in that
	// path — surface the limitation to the caller rather than silently
	// returning entities the caller asked to exclude.
	ErrDepthTemporalUnsupported = errors.New("graph: opts.Depth is not supported with a temporal filter")
)

// Sentinel errors for validation limits.
var (
	ErrTooManyLabels     = errors.New("graph: too many labels")
	ErrTooManyProperties = errors.New("graph: too many properties")
	ErrKeyTooLong        = errors.New("graph: property key too long")
	ErrValueTooLarge     = errors.New("graph: property value too large")
	ErrNameTooLong       = errors.New("graph: name too long")
	ErrSelfLoop          = errors.New("graph: self-loop relationship not allowed; set AllowSelfLoops in ValidationLimits to permit")
)

// Default validation limits — generous enough for normal use, restrictive enough
// to catch runaway callers.
const (
	defaultMaxLabelsPerNode       = 50
	defaultMaxPropertiesPerEntity = 1000
	defaultMaxPropertyKeyLength   = 256
	defaultMaxPropertyValueSize   = 65536 // 64 KiB, string values only
	defaultMaxNameLength          = 256   // label and reltype names
)

// ValidationLimits configures limits on entity structure.
// Zero values are resolved to defaults in New().
type ValidationLimits struct {
	MaxLabelsPerNode       int  // Default: 50
	MaxPropertiesPerEntity int  // Default: 1000
	MaxPropertyKeyLength   int  // Default: 256
	MaxPropertyValueSize   int  // Default: 65536 (string values only)
	MaxNameLength          int  // Default: 256 (label and reltype names)
	AllowSelfLoops         bool // Default: false — reject relationships where startNode == endNode
}

// snowflakeEpoch is the custom epoch for all snowflake ID generation (2026-01-01 UTC).
var snowflakeEpoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// snowflakeLayout is the package-level Layout matching the graph's snowflake
// generators. Used by standalone functions (entityValidFrom, shardIndex,
// DecomposeID) that don't have access to a *Node.
var snowflakeLayout = func() snowflake.Layout {
	l, err := snowflake.NewLayout(
		snowflake.WithEpoch(snowflakeEpoch),
		snowflake.WithMicroseconds(),
		snowflake.WithNodeBits(5),
		snowflake.WithStepBits(10),
	)
	if err != nil {
		panic("graph: snowflakeLayout: " + err.Error())
	}
	return l
}()

// Config holds configuration for the Graph.
type Config struct {
	// SnowflakeNodeID identifies this graph instance (0-15).
	// Internally mapped to even/odd generator pair (nodeGen=ID*2, relGen=ID*2+1)
	// to guarantee value-level uniqueness across node and relationship IDs.
	// Each concurrent instance must use a different value.
	SnowflakeNodeID int64

	// Store is the persistence backend. If nil, NewMemoryStore() is used
	// unless BadgerDir or BadgerInMemory is set.
	Store Store

	// BadgerDir is the Badger data directory. If set and Store is nil,
	// a BadgerStore is created. Ignored if Store is non-nil.
	BadgerDir string

	// BadgerInMemory enables in-memory Badger mode (useful for testing).
	// If true and Store is nil, a BadgerStore with InMemory=true is created.
	BadgerInMemory bool

	// Validation configures limits on entity structure.
	// Zero fields use defaults.
	Validation ValidationLimits

	// SyncWrites enables synchronous disk writes for every mutation.
	// When true, each write is flushed to stable storage before returning.
	// Ignored for MemoryStore or when Store is explicitly provided.
	// Default false (async background flush via FlushInterval).
	SyncWrites bool

	// Compression sets the SSTable compression algorithm for the convenience
	// BadgerStore path (BadgerDir / BadgerInMemory). Ignored when Store is set.
	// Valid values: options.None (0), options.Snappy (1), options.ZSTD (2).
	// Zero keeps the Badger default (Snappy).
	Compression options.CompressionType
	// ZSTDCompressionLevel sets the ZSTD compression level (1-15) for the
	// convenience BadgerStore path. Ignored when Store is set.
	// Only effective when Compression is options.ZSTD.
	// Zero keeps the Badger default (1).
	ZSTDCompressionLevel int
}

// Graph is the central graph layer. It owns the label and relationship type
// registries, snowflake ID generators, store, and provides string resolution
// for token-based entities.
//
// Entity locks serialize AddRelationship and DeleteNode on overlapping entities
// to prevent write-skew (concurrent AddRelationship(→X) + DeleteNodeCascade(X)
// producing a dangling edge).
type Graph struct {
	labels        *labelRegistry
	relTypes      *relTypeRegistry
	nodeIDGen     *snowflake.Node
	relIDGen      *snowflake.Node
	store         Store
	entityLocks   *entityLockManager
	validation    ValidationLimits
	constraints   ConstraintSet  // temporal constraints checked at relationship write time
	events        eventPublisher // nil = no event publishing; set via SetEventBus/SetAsyncEventBus
	txEventBuffer *[]Event       // non-nil while a tx holds g.mu.Lock — events buffered, not dispatched
	mu            sync.RWMutex   // serializes batch/tx writes vs standalone mutations and reads
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

// New creates a new Graph with the given configuration.
// Returns an error if SnowflakeNodeID is out of range (0-15).
// The ID is mapped to an even/odd pair (ID*2 for nodes, ID*2+1 for rels)
// to guarantee value-level uniqueness across entity types.
//
// Store selection priority:
//  1. config.Store (explicit injection)
//  2. BadgerStore (if BadgerDir or BadgerInMemory is set)
//  3. MemoryStore (default)
//
// When a BadgerStore is created, registries are loaded from persisted data.
// Call Close() when done to save registries and close the store.
func New(config Config) (*Graph, error) {
	if config.SnowflakeNodeID < 0 || config.SnowflakeNodeID > 15 {
		return nil, fmt.Errorf("graph: SnowflakeNodeID must be 0-15, got %d", config.SnowflakeNodeID)
	}

	nodeGen, err := snowflake.NewNode(config.SnowflakeNodeID*2,
		snowflake.WithEpoch(snowflakeEpoch),
		snowflake.WithMicroseconds(),
		snowflake.WithNodeBits(5),
		snowflake.WithStepBits(10),
	)
	if err != nil {
		return nil, fmt.Errorf("graph: node ID generator: %w", err)
	}
	relGen, err := snowflake.NewNode(config.SnowflakeNodeID*2+1,
		snowflake.WithEpoch(snowflakeEpoch),
		snowflake.WithMicroseconds(),
		snowflake.WithNodeBits(5),
		snowflake.WithStepBits(10),
	)
	if err != nil {
		return nil, fmt.Errorf("graph: rel ID generator: %w", err)
	}

	// Resolve zero validation limits to defaults.
	v := config.Validation
	if v.MaxLabelsPerNode == 0 {
		v.MaxLabelsPerNode = defaultMaxLabelsPerNode
	}
	if v.MaxPropertiesPerEntity == 0 {
		v.MaxPropertiesPerEntity = defaultMaxPropertiesPerEntity
	}
	if v.MaxPropertyKeyLength == 0 {
		v.MaxPropertyKeyLength = defaultMaxPropertyKeyLength
	}
	if v.MaxPropertyValueSize == 0 {
		v.MaxPropertyValueSize = defaultMaxPropertyValueSize
	}
	if v.MaxNameLength == 0 {
		v.MaxNameLength = defaultMaxNameLength
	}

	if v.MaxLabelsPerNode < 0 || v.MaxPropertiesPerEntity < 0 ||
		v.MaxPropertyKeyLength < 0 || v.MaxPropertyValueSize < 0 ||
		v.MaxNameLength < 0 {
		return nil, fmt.Errorf("graph: validation limits must not be negative")
	}

	g := &Graph{
		labels:         newLabelRegistry(),
		relTypes:       newRelTypeRegistry(),
		nodeIDGen:      nodeGen,
		relIDGen:       relGen,
		entityLocks:    newEntityLockManager(),
		validation:     v,
		indexProviders: make(map[string]*indexProviderEntry),
	}

	// Validate BadgerDir: reject whitespace-only strings (silent fallback hazard).
	if config.Store == nil && config.BadgerDir != "" {
		if strings.TrimSpace(config.BadgerDir) == "" {
			return nil, fmt.Errorf("graph: BadgerDir is whitespace-only; use a valid path or omit for MemoryStore")
		}
	}

	store := config.Store
	if store == nil {
		if config.BadgerDir != "" || config.BadgerInMemory {
			bs, err := NewBadgerStore(BadgerStoreConfig{
				Dir:                  config.BadgerDir,
				InMemory:             config.BadgerInMemory,
				SyncWrites:           config.SyncWrites,
				Compression:          config.Compression,
				ZSTDCompressionLevel: config.ZSTDCompressionLevel,
			})
			if err != nil {
				return nil, fmt.Errorf("graph: badger store: %w", err)
			}

			// Load persisted registries. Fail fast if the saved data is corrupt.
			if _, err := bs.LoadLabelRegistry(g.labels); err != nil {
				_ = bs.Close() // best-effort cleanup; returning primary error
				return nil, fmt.Errorf("graph: load label registry: %w", err)
			}
			if _, err := bs.LoadRelTypeRegistry(g.relTypes); err != nil {
				_ = bs.Close() // best-effort cleanup; returning primary error
				return nil, fmt.Errorf("graph: load reltype registry: %w", err)
			}

			store = bs
		} else {
			store = NewMemoryStore()
		}
	}

	g.store = store

	// Wire TieredStore to the label registry for ontology token resolution.
	if ts, ok := store.(*TieredStore); ok {
		ts.SetLabelRegistry(g.labels)
		if _, err := ts.LoadLabelRegistry(g.labels); err != nil {
			_ = ts.Close()
			return nil, fmt.Errorf("graph: load label registry: %w", err)
		}
		if _, err := ts.LoadRelTypeRegistry(g.relTypes); err != nil {
			_ = ts.Close()
			return nil, fmt.Errorf("graph: load reltype registry: %w", err)
		}
	}

	return g, nil
}

// Close saves registries (if Badger) and closes the underlying store.
// Safe to call concurrently and multiple times.
//
// store.Close() always runs even if registry saves fail — prevents resource leaks.
// Returns all errors joined; subsequent calls return nil.
func (g *Graph) Close() error {
	var closeErr error
	g.closeOnce.Do(func() {
		// Close index providers before the store so they can flush their
		// own state. Errors are collected; store close still runs.
		closeErr = errors.Join(closeErr, g.closeIndexProviders())

		// Save registries if the store supports it.
		switch s := g.store.(type) {
		case *BadgerStore:
			if err := s.SaveLabelRegistry(g.labels); err != nil {
				closeErr = fmt.Errorf("graph: save label registry: %w", err)
			}
			if err := s.SaveRelTypeRegistry(g.relTypes); err != nil {
				closeErr = errors.Join(closeErr, fmt.Errorf("graph: save reltype registry: %w", err))
			}
		case *TieredStore:
			if err := s.SaveRegistries(g.labels, g.relTypes); err != nil {
				closeErr = fmt.Errorf("graph: save registries: %w", err)
			}
		}
		// Always close the store — even if registry saves failed.
		closeErr = errors.Join(closeErr, g.store.Close())
	})
	return closeErr
}

// ValidationDefaults returns the resolved validation limits (for testing).
func (g *Graph) ValidationDefaults() ValidationLimits {
	return g.validation
}

// AddTemporalConstraint appends a constraint to the current constraint set.
// Constraints are checked at relationship write time (AddRelationship, ImportRelationshipWithID).
// Typically called once at startup before any writes.
func (g *Graph) AddTemporalConstraint(c TemporalConstraint) {
	g.mu.Lock()
	g.constraints = g.constraints.Add(c)
	g.mu.Unlock()
}

// SetTemporalConstraints replaces the entire constraint set.
// Pass an empty ConstraintSet to remove all constraints.
func (g *Graph) SetTemporalConstraints(cs ConstraintSet) {
	g.mu.Lock()
	g.constraints = cs
	g.mu.Unlock()
}

// TemporalConstraints returns the current constraint set (defensive copy).
func (g *Graph) TemporalConstraints() ConstraintSet {
	g.mu.RLock()
	cs := g.constraints
	g.mu.RUnlock()
	return cs
}

// validateName checks a label or relationship type name against MaxNameLength.
func (g *Graph) validateName(name string) error {
	if len(name) > g.validation.MaxNameLength {
		return fmt.Errorf("%w: %q (%d > %d)", ErrNameTooLong, name, len(name), g.validation.MaxNameLength)
	}
	return nil
}

// validatePropertyEntry checks a single key-value pair against validation limits.
// Checks MaxPropertyKeyLength and MaxPropertyValueSize (string values only).
func (g *Graph) validatePropertyEntry(key string, val any) error {
	if len(key) > g.validation.MaxPropertyKeyLength {
		return fmt.Errorf("%w: %q (%d > %d)", ErrKeyTooLong, key, len(key), g.validation.MaxPropertyKeyLength)
	}
	if s, ok := val.(string); ok {
		if len(s) > g.validation.MaxPropertyValueSize {
			return fmt.Errorf("%w: key %q (%d > %d)", ErrValueTooLarge, key, len(s), g.validation.MaxPropertyValueSize)
		}
	}
	return nil
}

// validateProperties checks all entries in a properties map against validation limits.
func (g *Graph) validateProperties(props map[string]any) error {
	if len(props) > g.validation.MaxPropertiesPerEntity {
		return fmt.Errorf("%w: %d > %d", ErrTooManyProperties, len(props), g.validation.MaxPropertiesPerEntity)
	}
	for key, val := range props {
		if err := g.validatePropertyEntry(key, val); err != nil {
			return err
		}
	}
	return nil
}

// NextNodeID generates a unique typed ID for a new node.
func (g *Graph) NextNodeID() types.NodeID {
	return types.NodeID(g.nodeIDGen.Generate())
}

// NextRelID generates a unique typed ID for a new relationship.
func (g *Graph) NextRelID() types.RelID {
	return types.RelID(g.relIDGen.Generate())
}

// --- Registry passthrough ---

// GetOrCreateLabel returns the token for a label name, creating it if needed.
func (g *Graph) GetOrCreateLabel(name string) (uint16, error) {
	return g.labels.GetOrCreate(name)
}

// GetOrCreateRelType returns the token for a relationship type name, creating it if needed.
func (g *Graph) GetOrCreateRelType(name string) (uint16, error) {
	return g.relTypes.GetOrCreate(name)
}

// LookupLabel returns the token for a label name without creating it.
func (g *Graph) LookupLabel(name string) (uint16, bool) {
	return g.labels.Lookup(name)
}

// LookupRelType returns the token for a relationship type name without creating it.
func (g *Graph) LookupRelType(name string) (uint16, bool) {
	return g.relTypes.Lookup(name)
}

// --- Resolution methods ---

// NodeLabels resolves all label tokens on the node to strings.
func (g *Graph) NodeLabels(n *types.Node) []string {
	tokens := n.AllLabelTokens()
	raw := make([]uint16, len(tokens))
	for i, t := range tokens {
		raw[i] = t.Value()
	}
	return g.labels.ResolveAll(raw)
}

// NodePrimaryLabel resolves the node's primary label token to a string.
func (g *Graph) NodePrimaryLabel(n *types.Node) string {
	return g.labels.Resolve(n.PrimaryLabelToken().Value())
}

// NodeHasLabel checks if the node has the given label (by name).
// Returns false if the label is not registered.
func (g *Graph) NodeHasLabel(n *types.Node, label string) bool {
	tok, ok := g.labels.Lookup(label)
	if !ok {
		return false
	}
	return n.HasLabelTokenRaw(tok)
}

// RelationshipType resolves the relationship's type token to a string.
func (g *Graph) RelationshipType(r *types.Relationship) string {
	return g.relTypes.Resolve(r.TypeToken().Value())
}

// RelationshipHasType checks if the relationship has the given type (by name).
// Returns false if the type is not registered.
func (g *Graph) RelationshipHasType(r *types.Relationship, typ string) bool {
	tok, ok := g.relTypes.Lookup(typ)
	if !ok {
		return false
	}
	return r.HasTypeTokenRaw(tok)
}

// --- Entity management ---

// AddNode creates a new node with the given labels and properties.
// Labels are resolved to tokens (created if needed). Properties are bulk-validated
// and sorted in O(N log N). Returns the created node with a generated snowflake ID.
func (g *Graph) AddNode(labels []string, props map[string]any) (*types.Node, error) {
	return g.AddNodeWithContext(context.Background(), labels, props)
}

// AddRelationship creates a new directed relationship between two nodes.
// The type name is resolved to a token (created if needed). Properties are
// bulk-validated and sorted in O(N log N).
func (g *Graph) AddRelationship(typeName string, startNode, endNode *types.Node, props map[string]any) (*types.Relationship, error) {
	return g.AddRelationshipWithContext(context.Background(), typeName, startNode, endNode, props)
}

// AddRelationshipByID creates a relationship using endpoint node IDs
// without fetching the endpoint nodes. See AddRelationshipByIDWithContext for details.
func (g *Graph) AddRelationshipByID(typeName string, startID, endID types.NodeID, props map[string]any) (*types.Relationship, error) {
	return g.AddRelationshipByIDWithContext(context.Background(), typeName, startID, endID, props)
}

// AddRelationshipByIDIfAbsent atomically creates a relationship using endpoint
// node IDs only if no relationship of the same type between the same endpoints
// already exists. See AddRelationshipByIDIfAbsentWithContext for details.
func (g *Graph) AddRelationshipByIDIfAbsent(typeName string, startID, endID types.NodeID, props map[string]any) (*types.Relationship, bool, error) {
	return g.AddRelationshipByIDIfAbsentWithContext(context.Background(), typeName, startID, endID, props)
}

// DeleteNode atomically removes a node and all connected relationships.
// Acquires the entity lock for the node to prevent write-skew with concurrent
// AddRelationship targeting the same node.
// Returns ErrNodeNotFound if the node does not exist.
func (g *Graph) DeleteNode(id types.NodeID) error {
	return g.DeleteNodeWithContext(context.Background(), id)
}

// DeleteRelationship removes a relationship from the store.
// Returns ErrRelNotFound if the relationship does not exist.
func (g *Graph) DeleteRelationship(id types.RelID) error {
	return g.DeleteRelationshipWithContext(context.Background(), id)
}

// --- Update operations ---

// UpdateNode applies property updates to an existing node.
// The updates map keys are property names; values are the new values.
// A nil value deletes the property. Keys with the "tkg_" prefix are rejected.
// Returns the updated node. Empty updates map is a no-op (no version bump).
func (g *Graph) UpdateNode(id types.NodeID, updates map[string]any) (*types.Node, error) {
	return g.UpdateNodeWithContext(context.Background(), id, updates)
}

// UpdateRelationship applies property updates to an existing relationship.
// The updates map keys are property names; values are the new values.
// A nil value deletes the property. Keys with the "tkg_" prefix are rejected.
// Returns the updated relationship. Empty updates map is a no-op.
func (g *Graph) UpdateRelationship(id types.RelID, updates map[string]any) (*types.Relationship, error) {
	return g.UpdateRelationshipWithContext(context.Background(), id, updates)
}

// SetNodeProperty sets a single property on an existing node.
func (g *Graph) SetNodeProperty(id types.NodeID, key string, value any) error {
	_, err := g.UpdateNode(id, map[string]any{key: value})
	return err
}

// DeleteNodeProperty removes a single property from an existing node.
func (g *Graph) DeleteNodeProperty(id types.NodeID, key string) error {
	_, err := g.UpdateNode(id, map[string]any{key: nil})
	return err
}

// SetRelationshipProperty sets a single property on an existing relationship.
func (g *Graph) SetRelationshipProperty(id types.RelID, key string, value any) error {
	_, err := g.UpdateRelationship(id, map[string]any{key: value})
	return err
}

// DeleteRelationshipProperty removes a single property from an existing relationship.
func (g *Graph) DeleteRelationshipProperty(id types.RelID, key string) error {
	_, err := g.UpdateRelationship(id, map[string]any{key: nil})
	return err
}

// CompareAndSetProperty atomically compares and swaps a single node property.
// Returns (true, nil) on match+update, (false, nil) on mismatch, (false, error) on real error.
// expected == nil means "property must not exist". newVal == nil means "delete the property".
// Value comparison uses reflect.DeepEqual — type must match exactly (int(42) != int64(42)).
func (g *Graph) CompareAndSetProperty(id types.NodeID, key string, expected, newVal any) (bool, error) {
	return g.CompareAndSetPropertyWithContext(context.Background(), id, key, expected, newVal)
}

// CompareAndSetPropertyWithContext is the context-aware variant of CompareAndSetProperty.
// Acquires g.mu.RLock for transaction isolation — blocked while a tx holds g.mu.Lock.
func (g *Graph) CompareAndSetPropertyWithContext(ctx context.Context, id types.NodeID, key string, expected, newVal any) (bool, error) {
	g.mu.RLock()
	ok, mutated, err := g.compareAndSetPropertyInternal(ctx, id, key, expected, newVal)
	ep := g.events
	g.mu.RUnlock()
	if ok && mutated && err == nil {
		dispatchEvent(ep, Event{Type: EventNodeUpdate, EntityID: types.EntityID(id), Timestamp: nowInstant(), Priority: PriorityNormal})
	}
	return ok, err
}

// compareAndSetPropertyInternal is the lock-free implementation.
// Callers must hold g.mu.RLock (standalone) or g.mu.Lock (tx/batch).
func (g *Graph) compareAndSetPropertyInternal(ctx context.Context, id types.NodeID, key string, expected, newVal any) (bool, bool, error) {
	if err := checkCtx(ctx); err != nil {
		return false, false, err
	}

	// Reject reserved keys.
	if types.IsShadowKey(key) {
		return false, false, fmt.Errorf("graph: compare-and-set: %w: %q", types.ErrReservedPrefix, key)
	}

	// Pre-validate newVal if non-nil.
	if newVal != nil {
		if err := types.ValidatePropertyValue(newVal); err != nil {
			return false, false, fmt.Errorf("graph: compare-and-set property %q: %w", key, err)
		}
		if err := g.validatePropertyEntry(key, newVal); err != nil {
			return false, false, err
		}
	} else {
		// Even for deletions, check key length.
		if len(key) > g.validation.MaxPropertyKeyLength {
			return false, false, fmt.Errorf("%w: %q (%d > %d)", ErrKeyTooLong, key, len(key), g.validation.MaxPropertyKeyLength)
		}
	}

	// Entity lock → read-modify-write under serialization.
	g.entityLocks.LockEntity(id.SnowflakeID())
	defer g.entityLocks.UnlockEntity(id.SnowflakeID())

	if err := checkCtx(ctx); err != nil {
		return false, false, err
	}

	current, err := g.store.GetNode(id)
	if err != nil {
		return false, false, err
	}

	// --- Compare gate ---
	cur, found := current.GetProperty(key)
	if expected == nil {
		// "property must not exist"
		if found {
			return false, false, nil
		}
	} else {
		// "property must exist and match"
		if !found || !reflect.DeepEqual(cur, expected) {
			return false, false, nil
		}
	}

	// --- No-op check: deleting an absent property ---
	if newVal == nil && !found {
		return true, false, nil
	}

	// --- Capture pre-mutation state ---
	prevVersion := current.Version()
	prevState := current.DeepCopy()
	prevHash := ""
	if ig := current.Integrity(); ig != nil {
		prevHash = ig.Hash
	}

	// --- Mutate ---
	if newVal == nil {
		if _, err := current.DeleteProperty(key); err != nil {
			return false, false, fmt.Errorf("graph: compare-and-set property %q: %w", key, err)
		}
	} else {
		if err := current.SetProperty(key, newVal); err != nil {
			return false, false, fmt.Errorf("graph: compare-and-set property %q: %w", key, err)
		}
	}

	current.SetVersion(current.Version() + 1)

	now := types.Instant(time.Now().UnixMilli())
	tm := current.Temporal()
	if tm == nil {
		tm = &types.TemporalMetadata{}
		current.SetTemporal(tm)
	}
	tm.UpdatedAt = now

	// TxTo on previous version.
	if ptm := prevState.Temporal(); ptm == nil {
		ptm2 := &types.TemporalMetadata{}
		prevState.SetTemporal(ptm2)
		ptm2.TxTo = now
	} else {
		ptm.TxTo = now
	}
	tm.TxFrom = now

	nodeLabels := g.NodeLabels(current)
	hash := ComputeNodeHash(current, nodeLabels)
	current.SetIntegrity(&types.NodeIntegrity{
		Hash:     hash,
		PrevHash: prevHash,
	})

	// Atomic replace + history.
	if err := g.store.ReplaceNodeWithHistory(current, prevVersion, prevState); err != nil {
		return false, false, err
	}

	g.opNodeUpdates.Add(1)
	return true, true, nil
}

// --- Version history passthrough ---

// GetNodeHistory returns all version history snapshots for the given node.
func (g *Graph) GetNodeHistory(id types.NodeID) ([]*types.Node, error) {
	return g.store.GetNodeHistory(id)
}

// GetRelHistory returns all version history snapshots for the given relationship.
func (g *Graph) GetRelHistory(id types.RelID) ([]*types.Relationship, error) {
	return g.store.GetRelHistory(id)
}

// --- Store passthrough queries ---

// GetNode retrieves a node by snowflake ID.
func (g *Graph) GetNode(id types.NodeID) (*types.Node, error) {
	return g.GetNodeWithContext(context.Background(), id)
}

// GetRelationship retrieves a relationship by snowflake ID.
func (g *Graph) GetRelationship(id types.RelID) (*types.Relationship, error) {
	return g.GetRelationshipWithContext(context.Background(), id)
}

// NodesByLabel returns nodes with the given label (resolved from string),
// with optional pagination. Returns nil if the label is not registered.
//
// When opts carries a temporal filter (ValidAt or ValidStart/ValidEnd) the
// query is history-aware: every known node (current + history) is scanned and
// any version whose label set contained the requested label at the requested
// time matches. Without a temporal filter, the call falls through to the
// store-level label index for O(matches) lookup.
func (g *Graph) NodesByLabel(label string, opts QueryOpts) ([]*types.Node, error) {
	tok, ok := g.labels.Lookup(label)
	if !ok {
		return nil, nil
	}
	if !opts.hasTemporalFilter() {
		return g.store.NodesByLabel(tok, opts)
	}
	if opts.Depth != DepthAll {
		return nil, ErrDepthTemporalUnsupported
	}
	// Indexed candidate set: current nodes that carry the label NOW, plus all
	// node IDs that ever appeared in history (covering the case where the
	// label was held in a previous version but not the current one). Avoids
	// a full ForEachNodeID scan.
	current, err := g.store.NodesByLabel(tok, QueryOpts{})
	if err != nil {
		return nil, err
	}
	currentIDs := make([]types.NodeID, 0, len(current))
	for _, n := range current {
		currentIDs = append(currentIDs, n.ID())
	}

	pred := func(n *types.Node) bool { return n.HasLabelTokenRaw(tok) }
	var result []*types.Node
	if err := g.forEachNodeCandidateID(currentIDs, func(id types.NodeID) error {
		n, err := g.findNodeVersionForOpts(id, opts, pred)
		if err != nil {
			if errors.Is(err, ErrNoVersionValidAt) || errors.Is(err, ErrNodeNotFound) {
				return nil
			}
			return err
		}
		result = append(result, n)
		return nil
	}); err != nil {
		return nil, err
	}
	sortNodesByID(result)
	return paginateNodes(result, opts.After, opts.Limit), nil
}

// RelationshipsByType returns relationships with the given type (resolved from string),
// with optional pagination. Returns nil if the type is not registered.
//
// When opts carries a temporal filter, every known relationship (current +
// history) is scanned. The type token is structurally immutable, so the only
// history-relevant information added by this scan is deleted/closed-out
// relationships that the current type index no longer references. Without a
// temporal filter, the call falls through to the store-level type index.
func (g *Graph) RelationshipsByType(typeName string, opts QueryOpts) ([]*types.Relationship, error) {
	tok, ok := g.relTypes.Lookup(typeName)
	if !ok {
		return nil, nil
	}
	if !opts.hasTemporalFilter() {
		return g.store.RelationshipsByType(tok, opts)
	}
	if opts.Depth != DepthAll {
		return nil, ErrDepthTemporalUnsupported
	}
	// Indexed candidate set: current rels of this type, plus history IDs.
	// Type tokens are structurally immutable so the type predicate is only
	// needed for safety; history IDs cover the deleted-rel case.
	current, err := g.store.RelationshipsByType(tok, QueryOpts{})
	if err != nil {
		return nil, err
	}
	currentIDs := make([]types.RelID, 0, len(current))
	for _, r := range current {
		currentIDs = append(currentIDs, r.ID())
	}

	pred := func(r *types.Relationship) bool { return r.HasTypeTokenRaw(tok) }
	var result []*types.Relationship
	if err := g.forEachRelCandidateID(currentIDs, func(id types.RelID) error {
		r, err := g.findRelVersionForOpts(id, opts, pred)
		if err != nil {
			if errors.Is(err, ErrNoVersionValidAt) || errors.Is(err, ErrRelNotFound) {
				return nil
			}
			return err
		}
		result = append(result, r)
		return nil
	}); err != nil {
		return nil, err
	}
	sortRelsByID(result)
	return paginateRels(result, opts.After, opts.Limit), nil
}

// OutgoingRelationships returns all outgoing relationships from the given node.
// If typeName is empty, all types are returned. If typeName is non-empty, only
// relationships of that type are returned (nil if the type is not registered).
func (g *Graph) OutgoingRelationships(nodeID types.NodeID, typeName string) ([]*types.Relationship, error) {
	var tok uint16
	if typeName != "" {
		t, ok := g.relTypes.Lookup(typeName)
		if !ok {
			return nil, nil
		}
		tok = t
	}
	return g.store.OutgoingRelationships(nodeID, tok)
}

// OutgoingRelationshipsForNodes returns outgoing relationships for multiple nodes
// in a single batched operation. Returns a map from nodeID to its outgoing rels
// (sorted by ID). Nodes with zero outgoing rels are absent from the map.
// If typeName is non-empty, only relationships of that type are returned.
// Unregistered typeName returns nil, nil. nil/empty nodeIDs returns nil, nil.
func (g *Graph) OutgoingRelationshipsForNodes(nodeIDs []types.NodeID, typeName string) (map[types.NodeID][]*types.Relationship, error) {
	if len(nodeIDs) == 0 {
		return nil, nil
	}
	var tok uint16
	if typeName != "" {
		t, ok := g.relTypes.Lookup(typeName)
		if !ok {
			return nil, nil
		}
		tok = t
	}
	return g.store.OutgoingRelationshipsForNodes(nodeIDs, tok)
}

// IncomingRelationshipsForNodes returns incoming relationships for multiple nodes
// in a single batched operation. Returns a map from nodeID to its incoming rels
// (sorted by ID). Nodes with zero incoming rels are absent from the map.
// If typeName is non-empty, only relationships of that type are returned.
// Unregistered typeName returns nil, nil. nil/empty nodeIDs returns nil, nil.
func (g *Graph) IncomingRelationshipsForNodes(nodeIDs []types.NodeID, typeName string) (map[types.NodeID][]*types.Relationship, error) {
	if len(nodeIDs) == 0 {
		return nil, nil
	}
	var tok uint16
	if typeName != "" {
		t, ok := g.relTypes.Lookup(typeName)
		if !ok {
			return nil, nil
		}
		tok = t
	}
	return g.store.IncomingRelationshipsForNodes(nodeIDs, tok)
}

// IncomingRelationships returns all incoming relationships to the given node.
// If typeName is empty, all types are returned. If typeName is non-empty, only
// relationships of that type are returned (nil if the type is not registered).
func (g *Graph) IncomingRelationships(nodeID types.NodeID, typeName string) ([]*types.Relationship, error) {
	var tok uint16
	if typeName != "" {
		t, ok := g.relTypes.Lookup(typeName)
		if !ok {
			return nil, nil
		}
		tok = t
	}
	return g.store.IncomingRelationships(nodeID, tok)
}

// NodeCount returns the number of nodes in the store.
func (g *Graph) NodeCount() (int, error) {
	return g.store.NodeCount()
}

// RelationshipCount returns the number of relationships in the store.
func (g *Graph) RelationshipCount() (int, error) {
	return g.store.RelationshipCount()
}

// AllNodes returns all nodes in the store, with optional pagination.
//
// History-aware when opts carries a temporal filter (ValidAt or
// ValidStart/ValidEnd): merges current and history IDs, resolves each via
// the version chain, and surfaces deleted entities that were valid at the
// query time. Without a temporal filter the fast store-side pushdown path
// is preserved.
func (g *Graph) AllNodes(opts QueryOpts) ([]*types.Node, error) {
	if !opts.hasTemporalFilter() {
		return g.store.AllNodes(opts)
	}
	if opts.Depth != DepthAll {
		return nil, ErrDepthTemporalUnsupported
	}
	var result []*types.Node
	err := g.forEachKnownNodeID(func(id types.NodeID) error {
		n, err := g.findNodeVersionForOpts(id, opts, nil)
		if err != nil {
			if errors.Is(err, ErrNoVersionValidAt) || errors.Is(err, ErrNodeNotFound) {
				return nil
			}
			return err
		}
		result = append(result, n)
		return nil
	})
	if err != nil {
		return nil, err
	}
	sortNodesByID(result)
	return paginateNodes(result, opts.After, opts.Limit), nil
}

// AllRelationships returns all relationships in the store, with optional
// pagination.
//
// History-aware when opts carries a temporal filter (ValidAt or
// ValidStart/ValidEnd): merges current and history IDs, resolves each via
// the version chain, and surfaces deleted relationships that were valid at
// the query time. Without a temporal filter the fast store-side pushdown
// path is preserved.
func (g *Graph) AllRelationships(opts QueryOpts) ([]*types.Relationship, error) {
	if !opts.hasTemporalFilter() {
		return g.store.AllRelationships(opts)
	}
	if opts.Depth != DepthAll {
		return nil, ErrDepthTemporalUnsupported
	}
	var result []*types.Relationship
	err := g.forEachKnownRelID(func(id types.RelID) error {
		r, err := g.findRelVersionForOpts(id, opts, nil)
		if err != nil {
			if errors.Is(err, ErrNoVersionValidAt) || errors.Is(err, ErrRelNotFound) {
				return nil
			}
			return err
		}
		result = append(result, r)
		return nil
	})
	if err != nil {
		return nil, err
	}
	sortRelsByID(result)
	return paginateRels(result, opts.After, opts.Limit), nil
}

// GetNodesByIDs returns nodes matching the given IDs. Missing IDs are skipped.
func (g *Graph) GetNodesByIDs(ids []types.NodeID) ([]*types.Node, error) {
	return g.store.GetNodesByIDs(ids)
}

// GetRelationshipsByIDs returns relationships matching the given IDs. Missing IDs are skipped.
func (g *Graph) GetRelationshipsByIDs(ids []types.RelID) ([]*types.Relationship, error) {
	return g.store.GetRelationshipsByIDs(ids)
}

// --- Per-label / per-type statistics ---

// NodeCountByLabel returns the number of nodes with the given label. O(1).
// Returns 0 if the label has never been registered.
func (g *Graph) NodeCountByLabel(label string) (int, error) {
	tok, ok := g.labels.Lookup(label)
	if !ok {
		return 0, nil
	}
	return g.store.NodeCountByLabel(tok)
}

// RelCountByType returns the number of relationships with the given type. O(1).
// Returns 0 if the type has never been registered.
func (g *Graph) RelCountByType(typeName string) (int, error) {
	tok, ok := g.relTypes.Lookup(typeName)
	if !ok {
		return 0, nil
	}
	return g.store.RelCountByType(tok)
}

// AllLabelCounts returns a map of label name to node count for all registered labels.
// Labels with zero nodes are omitted.
func (g *Graph) AllLabelCounts() (map[string]int, error) {
	names := g.labels.ExportNames()
	result := make(map[string]int)

	// Skip index 0 (reserved empty string).
	for i := 1; i < len(names); i++ {
		count, err := g.store.NodeCountByLabel(uint16(i))
		if err != nil {
			return nil, err
		}
		if count > 0 {
			result[names[i]] = count
		}
	}
	return result, nil
}

// AllRelTypeCounts returns a map of relationship type name to relationship count
// for all registered types. Types with zero relationships are omitted.
func (g *Graph) AllRelTypeCounts() (map[string]int, error) {
	names := g.relTypes.ExportNames()
	result := make(map[string]int)

	// Skip index 0 (reserved empty string).
	for i := 1; i < len(names); i++ {
		count, err := g.store.RelCountByType(uint16(i))
		if err != nil {
			return nil, err
		}
		if count > 0 {
			result[names[i]] = count
		}
	}
	return result, nil
}

// --- Property indexes ---

// CreatePropertyIndex creates a property index on the given label and property key.
// Resolves the label name to a token. Returns ErrIndexExists if the index already exists.
// Returns nil if the label has never been registered (nothing to index).
func (g *Graph) CreatePropertyIndex(label, propertyKey string) error {
	tok, ok := g.labels.Lookup(label)
	if !ok {
		return nil
	}
	return g.store.CreatePropertyIndex(tok, propertyKey)
}

// DropPropertyIndex removes a property index.
// Resolves the label name to a token. Returns ErrIndexNotFound if the index does not exist.
// Returns nil if the label has never been registered.
func (g *Graph) DropPropertyIndex(label, propertyKey string) error {
	tok, ok := g.labels.Lookup(label)
	if !ok {
		return nil
	}
	return g.store.DropPropertyIndex(tok, propertyKey)
}

// CreateTemporalIndex creates a temporal index on nodes with the given label.
// Accelerates temporal queries (ValidAt/interval filter) for that label.
// Returns ErrTemporalIndexExists if the index already exists.
// Returns nil if the label has never been registered.
func (g *Graph) CreateTemporalIndex(label string) error {
	tok, ok := g.labels.Lookup(label)
	if !ok {
		return nil
	}
	return g.store.CreateTemporalIndex(tok)
}

// DropTemporalIndex removes a temporal index for the given label.
// Returns ErrTemporalIndexNotFound if the index does not exist.
// Returns nil if the label has never been registered.
func (g *Graph) DropTemporalIndex(label string) error {
	tok, ok := g.labels.Lookup(label)
	if !ok {
		return nil
	}
	return g.store.DropTemporalIndex(tok)
}

// --- High-frequency indexes ---

// CreateHighFrequencyIndex creates a time-bucketed high-frequency temporal index
// on nodes with the given label. The bucketSize parameter controls the time
// width of each bucket (e.g., time.Hour).
// Designed for high-write-rate scenarios (thousands of event writes/sec).
// Only one temporal index type (temporal or high-frequency) can exist per label.
// Returns nil if the label has never been registered.
// Returns ErrTemporalIndexExists if any temporal index already exists for this label.
// Not persisted: the index must be rebuilt via CreateHighFrequencyIndex after restart.
func (g *Graph) CreateHighFrequencyIndex(label string, bucketSize time.Duration) error {
	tok, ok := g.labels.Lookup(label)
	if !ok {
		return nil
	}
	return g.store.CreateHighFrequencyIndex(tok, bucketSize)
}

// DropHighFrequencyIndex removes the high-frequency temporal index for the given label.
// Returns nil if the label has never been registered.
// Returns ErrTemporalIndexNotFound if no high-frequency index exists.
func (g *Graph) DropHighFrequencyIndex(label string) error {
	tok, ok := g.labels.Lookup(label)
	if !ok {
		return nil
	}
	return g.store.DropHighFrequencyIndex(tok)
}

// --- Vector indexes ---

// CreateVectorIndex creates a vector similarity index on the given label and property key.
// dims is the expected vector dimension. metric selects the distance function.
// Returns nil if the label has never been registered (no-op).
// Returns ErrVectorIndexExists if the index already exists.
func (g *Graph) CreateVectorIndex(label, propertyKey string, dims int, metric DistanceMetric) error {
	tok, ok := g.labels.Lookup(label)
	if !ok {
		return nil
	}
	return g.store.CreateVectorIndex(tok, propertyKey, dims, metric)
}

// DropVectorIndex removes a vector index.
// Returns nil if the label has never been registered.
// Returns ErrVectorIndexNotFound if the index does not exist.
func (g *Graph) DropVectorIndex(label, propertyKey string) error {
	tok, ok := g.labels.Lookup(label)
	if !ok {
		return nil
	}
	return g.store.DropVectorIndex(tok, propertyKey)
}

// SearchNearestNodes returns the k nodes with vectors closest to query
// under the index defined for label+propertyKey.
// Returns ErrVectorIndexNotFound if no index exists.
// Returns ErrDimensionMismatch if query length differs from the index's dims.
// Returns nil, nil if the index exists but has no entries.
// Returns nil, nil if the label has never been registered.
func (g *Graph) SearchNearestNodes(label, propertyKey string, query []float32, k int, opts QueryOpts) ([]*types.Node, error) {
	tok, ok := g.labels.Lookup(label)
	if !ok {
		return nil, nil
	}
	return g.store.SearchNearestNodes(tok, propertyKey, query, k, opts)
}

// NodesByLabelAndProperty returns nodes matching the label and property value,
// with optional pagination. Resolves the label name to a token.
// Returns nil if the label is not registered.
//
// Without a temporal filter, the call falls through to the store-level
// property index for O(matches) lookup. When opts carries a temporal filter,
// the candidate set is the union of (nodes currently matching label+property
// — seeded via the same property-index lookup) and (every known history ID).
// Each candidate is then resolved to its version overlapping the requested
// time and the predicate re-checked against that historical version, so a
// node whose label and property held at the requested time is included even
// if a later version no longer matches.
func (g *Graph) NodesByLabelAndProperty(label, key string, value any, opts QueryOpts) ([]*types.Node, error) {
	tok, ok := g.labels.Lookup(label)
	if !ok {
		return nil, nil
	}
	if !opts.hasTemporalFilter() {
		return g.store.NodesByLabelAndProperty(tok, key, value, opts)
	}
	if opts.Depth != DepthAll {
		return nil, ErrDepthTemporalUnsupported
	}
	targetKey := propertyValueKey(value)
	if targetKey == "" {
		return nil, nil
	}
	// Indexed candidate set: nodes that currently match label+property via the
	// store-level property index (when available — falls back internally to a
	// label scan if no property index is registered), merged with all history
	// IDs to cover deleted/changed nodes whose historical version matched.
	currentMatching, err := g.store.NodesByLabelAndProperty(tok, key, value, QueryOpts{})
	if err != nil {
		return nil, err
	}
	currentIDs := make([]types.NodeID, 0, len(currentMatching))
	for _, n := range currentMatching {
		currentIDs = append(currentIDs, n.ID())
	}

	pred := func(n *types.Node) bool {
		if !n.HasLabelTokenRaw(tok) {
			return false
		}
		v, found := n.GetProperty(key)
		return found && propertyValueKey(v) == targetKey
	}
	var result []*types.Node
	err = g.forEachNodeCandidateID(currentIDs, func(id types.NodeID) error {
		n, err := g.findNodeVersionForOpts(id, opts, pred)
		if err != nil {
			if errors.Is(err, ErrNoVersionValidAt) || errors.Is(err, ErrNodeNotFound) {
				return nil
			}
			return err
		}
		result = append(result, n)
		return nil
	})
	if err != nil {
		return nil, err
	}
	sortNodesByID(result)
	return paginateNodes(result, opts.After, opts.Limit), nil
}

// ArchiveNode moves a reference node and its relationships from the reference
// shard to the reference archive. Only available with TieredStore.
// Returns ErrNodeNotFound if the node is not in the reference shard.
//
// Concurrency: takes g.mu.Lock — same exclusion class as a transaction.
// ArchiveNode reads adjacency, then runs cascade; without this lock a
// concurrent AddRelationship between the pre-scan and the cascade can
// create a cross-shard rel touching the soon-to-be-archived node, which
// the pre-scan misses and the cascade then partially destroys (the
// rel's adjacency entries on the partner shard would dangle). Archiving
// is a rare, batch-style admin operation; serializing against all
// writers is acceptable and mirrors the tx/batch lock discipline.
func (g *Graph) ArchiveNode(id types.NodeID) error {
	if ts, ok := g.store.(*TieredStore); ok {
		g.mu.Lock()
		defer g.mu.Unlock()
		return ts.ArchiveNode(id)
	}
	return ErrNotTieredStore
}

// RestoreNode moves a reference node and its relationships from the reference
// archive back to the reference shard. Only available with TieredStore.
// Returns ErrNodeNotFound if the node is not in the archive.
//
// Concurrency: takes g.mu.Lock — see ArchiveNode for the rationale.
func (g *Graph) RestoreNode(id types.NodeID) error {
	if ts, ok := g.store.(*TieredStore); ok {
		g.mu.Lock()
		defer g.mu.Unlock()
		return ts.RestoreNode(id)
	}
	return ErrNotTieredStore
}

// DecomposeID extracts the creation time, node ID, and sequence number from
// a snowflake ID. Works with any store type.
func (g *Graph) DecomposeID(id snowflake.ID) IDComponents {
	return DecomposeID(id)
}

// ForceRotate triggers a hot-shard rotation. Only available with TieredStore.
func (g *Graph) ForceRotate() error {
	if ts, ok := g.store.(*TieredStore); ok {
		return ts.ForceRotate()
	}
	return ErrNotTieredStore
}

// ListShards returns information about all shards. Only available with TieredStore.
func (g *Graph) ListShards() ([]ShardInfo, error) {
	if ts, ok := g.store.(*TieredStore); ok {
		return ts.ListShards()
	}
	return nil, ErrNotTieredStore
}

// RebuildCatalog reconstructs the shard catalog from live state.
// Only available with TieredStore.
func (g *Graph) RebuildCatalog() error {
	if ts, ok := g.store.(*TieredStore); ok {
		return ts.RebuildCatalog()
	}
	return ErrNotTieredStore
}

// RunRepair scans for cross-shard consistency issues and fixes them.
// Only available with TieredStore.
func (g *Graph) RunRepair() (*RepairResult, error) {
	if ts, ok := g.store.(*TieredStore); ok {
		return ts.RunRepair()
	}
	return nil, ErrNotTieredStore
}

// VerifyShard runs hash chain verification on all entities in a shard.
// Only available with TieredStore.
func (g *Graph) VerifyShard(shardName string) (*VerifyResult, error) {
	if ts, ok := g.store.(*TieredStore); ok {
		return ts.VerifyShard(g, shardName)
	}
	return nil, ErrNotTieredStore
}

// Reset atomically clears all entities, indexes, history, and counters from
// the graph while preserving registries (label and relationship type tokens).
// Acquires the graph write lock to prevent concurrent operations.
func (g *Graph) Reset() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.store.Clear()
}

// --- Event bus ---

// SetEventBus attaches a synchronous EventBus to the graph.
// All subsequent mutations publish lifecycle events to the bus.
// Pass nil to detach and disable event publishing.
func (g *Graph) SetEventBus(bus *EventBus) {
	g.mu.Lock()
	if bus == nil {
		g.events = nil
	} else {
		g.events = bus
	}
	g.mu.Unlock()
}

// GetEventBus returns the attached synchronous EventBus, or nil if none is set
// (including when an AsyncEventBus is attached instead).
func (g *Graph) GetEventBus() *EventBus {
	g.mu.RLock()
	ep := g.events
	g.mu.RUnlock()
	if eb, ok := ep.(*EventBus); ok {
		return eb
	}
	return nil
}

// SetAsyncEventBus attaches an AsyncEventBus to the graph for async event delivery.
// All subsequent mutations publish lifecycle events to the bus.
// Pass nil to detach and disable event publishing.
func (g *Graph) SetAsyncEventBus(bus *AsyncEventBus) {
	g.mu.Lock()
	if bus == nil {
		g.events = nil
	} else {
		g.events = bus
	}
	g.mu.Unlock()
}

// publishEvent delivers a lifecycle event to the attached EventBus.
// During a transaction (txEventBuffer != nil), events are buffered instead of
// dispatched — published on Commit, discarded on Rollback.
// No-op if no EventBus is attached (nil-safe).
//
// IMPORTANT: callers must hold g.mu.RLock or g.mu.Lock when calling this method,
// because it reads g.events and g.txEventBuffer without additional synchronization.
// For dispatch AFTER releasing g.mu, capture ep := g.events under lock and call
// dispatchEvent(ep, ...) instead.
func (g *Graph) publishEvent(typ EventType, id types.EntityID, t types.Instant, priority EventPriority) {
	if g.events == nil {
		return
	}
	e := Event{Type: typ, EntityID: id, Timestamp: t, Priority: priority}
	if g.txEventBuffer != nil {
		*g.txEventBuffer = append(*g.txEventBuffer, e)
		return
	}
	g.events.publish(e)
}

// dispatchEvent delivers an event to the given publisher. No-op if ep is nil.
// Use this for event dispatch after releasing g.mu — the caller captures g.events
// while the lock is held, then calls dispatchEvent with the captured reference.
func dispatchEvent(ep eventPublisher, e Event) {
	if ep == nil {
		return
	}
	ep.publish(e)
}

// Stats returns a snapshot of graph operation counters and optional cache metrics.
// Cache metrics are populated only when the underlying store implements StoreStats
// (currently BadgerStore only); all cache fields are zero for MemoryStore and TieredStore.
func (g *Graph) Stats() GraphStats {
	s := GraphStats{
		NodesAdded:   g.opNodeAdds.Load(),
		NodesRead:    g.opNodeReads.Load(),
		NodesUpdated: g.opNodeUpdates.Load(),
		NodesDeleted: g.opNodeDeletes.Load(),
		RelsAdded:    g.opRelAdds.Load(),
		RelsRead:     g.opRelReads.Load(),
		RelsUpdated:  g.opRelUpdates.Load(),
		RelsDeleted:  g.opRelDeletes.Load(),
	}
	if ss, ok := g.store.(StoreStats); ok {
		s.NodeCacheHits = ss.NodeCacheHits()
		s.NodeCacheMisses = ss.NodeCacheMisses()
		s.RelCacheHits = ss.RelCacheHits()
		s.RelCacheMisses = ss.RelCacheMisses()
	}
	return s
}

// AddNodeLabel adds the given label to an existing node.
// Idempotent: returns nil without bumping version or writing history if the
// node already has the label. Validates label name length and enforces
// MaxLabelsPerNode. Returns ErrNodeNotFound if the node does not exist.
// Acquires g.mu.RLock for transaction isolation — blocked while a tx holds g.mu.Lock.
func (g *Graph) AddNodeLabel(id types.NodeID, label string) error {
	g.mu.RLock()
	mutated, err := g.addNodeLabelInternal(id, label)
	ep := g.events
	g.mu.RUnlock()
	if err == nil && mutated {
		dispatchEvent(ep, Event{Type: EventNodeUpdate, EntityID: types.EntityID(id), Timestamp: nowInstant(), Priority: PriorityNormal})
	}
	return err
}

// addNodeLabelInternal is the lock-free implementation of AddNodeLabel.
// Callers must hold g.mu.RLock (standalone) or g.mu.Lock (tx/batch).
// Returns mutated=false if the node already has the label.
func (g *Graph) addNodeLabelInternal(id types.NodeID, label string) (bool, error) {
	if err := g.validateName(label); err != nil {
		return false, err
	}

	g.entityLocks.LockEntity(id.SnowflakeID())
	defer g.entityLocks.UnlockEntity(id.SnowflakeID())

	current, err := g.store.GetNode(id)
	if err != nil {
		return false, err
	}

	// Resolve or create the token only after confirming the node exists, so
	// we don't pollute the registry for unknown IDs.
	tok, err := g.labels.GetOrCreate(label)
	if err != nil {
		return false, fmt.Errorf("graph: add label: %w", err)
	}

	// Idempotent: node already carries the label — no mutation, no history.
	if current.HasLabelTokenRaw(tok) {
		return false, nil
	}

	// Enforce MaxLabelsPerNode against the post-addition count.
	if current.LabelTokenCount()+1 > g.validation.MaxLabelsPerNode {
		return false, fmt.Errorf("%w: %d > %d", ErrTooManyLabels, current.LabelTokenCount()+1, g.validation.MaxLabelsPerNode)
	}

	// Capture pre-mutation state for version history (before any modification).
	prevVersion := current.Version()
	prevState := current.DeepCopy()

	copy := current.DeepCopy()
	if !copy.AddLabelTokenRaw(tok) {
		// Defensive: should never hit — idempotence check above already handled presence.
		return false, nil
	}
	copy.SetVersion(prevVersion + 1)

	// Advance hash chain: PrevHash = current Hash (link new version back to current).
	prevHash := ""
	if ig := current.Integrity(); ig != nil {
		prevHash = ig.Hash
	}
	nodeLabels := g.NodeLabels(copy)
	hash := ComputeNodeHash(copy, nodeLabels)
	copy.SetIntegrity(&types.NodeIntegrity{Hash: hash, PrevHash: prevHash})

	// Set transaction/update time on both sides of the version boundary.
	now := types.Instant(time.Now().UnixMilli())
	if ptm := prevState.Temporal(); ptm == nil {
		ptm2 := &types.TemporalMetadata{}
		prevState.SetTemporal(ptm2)
		ptm2.TxTo = now
	} else {
		ptm.TxTo = now
	}
	tm := copy.Temporal()
	if tm == nil {
		tm = &types.TemporalMetadata{}
		copy.SetTemporal(tm)
	}
	tm.UpdatedAt = now
	tm.TxFrom = now

	// Atomic: write history entry + add label index + persist updated node in one call.
	if err := g.store.AddNodeLabelTokenWithHistory(id, tok, copy, prevVersion, prevState); err != nil {
		return false, err
	}
	g.opNodeUpdates.Add(1)
	return true, nil
}

// RemoveNodeLabel removes the given label from an existing node.
// Acquires g.mu.RLock for transaction isolation — blocked while a tx holds g.mu.Lock.
func (g *Graph) RemoveNodeLabel(id types.NodeID, label string) error {
	g.mu.RLock()
	err := g.removeNodeLabelInternal(id, label)
	ep := g.events
	g.mu.RUnlock()
	if err == nil {
		dispatchEvent(ep, Event{Type: EventNodeUpdate, EntityID: types.EntityID(id), Timestamp: nowInstant(), Priority: PriorityNormal})
	}
	return err
}

// removeNodeLabelInternal is the lock-free implementation of RemoveNodeLabel.
// Callers must hold g.mu.RLock (standalone) or g.mu.Lock (tx/batch).
func (g *Graph) removeNodeLabelInternal(id types.NodeID, label string) error {
	tok, ok := g.labels.Lookup(label)
	if !ok {
		return ErrLabelNotFound
	}

	g.entityLocks.LockEntity(id.SnowflakeID())
	defer g.entityLocks.UnlockEntity(id.SnowflakeID())

	current, err := g.store.GetNode(id)
	if err != nil {
		return err
	}

	if !current.HasLabelTokenRaw(tok) {
		return ErrLabelNotFound
	}
	if current.LabelTokenCount() == 1 {
		return ErrLastLabel
	}

	// Capture pre-mutation state for version history (before any modification).
	prevVersion := current.Version()
	prevState := current.DeepCopy()

	copy := current.DeepCopy()
	copy.RemoveLabelTokenRaw(tok)
	copy.SetVersion(prevVersion + 1)

	// Advance hash chain: PrevHash = current Hash (link new version back to current).
	prevHash := ""
	if ig := current.Integrity(); ig != nil {
		prevHash = ig.Hash
	}
	nodeLabels := g.NodeLabels(copy)
	hash := ComputeNodeHash(copy, nodeLabels)
	copy.SetIntegrity(&types.NodeIntegrity{Hash: hash, PrevHash: prevHash})

	// Set transaction/update time on both sides of the version boundary.
	now := types.Instant(time.Now().UnixMilli())
	if ptm := prevState.Temporal(); ptm == nil {
		ptm2 := &types.TemporalMetadata{}
		prevState.SetTemporal(ptm2)
		ptm2.TxTo = now
	} else {
		ptm.TxTo = now
	}
	tm := copy.Temporal()
	if tm == nil {
		tm = &types.TemporalMetadata{}
		copy.SetTemporal(tm)
	}
	tm.UpdatedAt = now
	tm.TxFrom = now

	// Atomic: write history entry + remove label index + persist updated node in one call.
	if err := g.store.RemoveNodeLabelTokenWithHistory(id, tok, copy, prevVersion, prevState); err != nil {
		return err
	}
	g.opNodeUpdates.Add(1)
	return nil
}

// --- Version chain navigation ---

// GetPreviousNodeVersion returns the version immediately before the given version.
// Returns nil, nil if version == 0 or if the predecessor does not exist in history.
func (g *Graph) GetPreviousNodeVersion(id types.NodeID, version uint32) (*types.Node, error) {
	if version == 0 {
		return nil, nil
	}
	n, err := g.store.GetNodeVersion(id, version-1)
	if errors.Is(err, ErrVersionNotFound) {
		return nil, nil
	}
	return n, err
}

// GetNextNodeVersion returns the version immediately after the given version.
// Returns nil, nil if no newer version exists (the given version IS the current tip).
// Checks history first, then falls back to the current node (which may be version+1).
func (g *Graph) GetNextNodeVersion(id types.NodeID, version uint32) (*types.Node, error) {
	n, err := g.store.GetNodeVersion(id, version+1)
	if err == nil {
		return n, nil
	}
	if !errors.Is(err, ErrVersionNotFound) {
		return nil, err
	}
	// Not in history: the current node itself may be version+1.
	current, err2 := g.store.GetNode(id)
	if err2 != nil {
		if errors.Is(err2, ErrNodeNotFound) {
			return nil, nil
		}
		return nil, err2
	}
	if current.Version() == version+1 {
		return current, nil
	}
	return nil, nil
}

// CloseNodeVersion sets ValidTo on the current node to t, marking it temporally
// expired without deleting it or incrementing its version number.
// Acquires g.mu.RLock for transaction isolation — blocked while a tx holds g.mu.Lock.
func (g *Graph) CloseNodeVersion(id types.NodeID, t types.Instant) error {
	g.mu.RLock()
	err := g.closeNodeVersionInternal(id, t)
	ep := g.events
	g.mu.RUnlock()
	if err == nil {
		dispatchEvent(ep, Event{Type: EventNodeUpdate, EntityID: types.EntityID(id), Timestamp: nowInstant(), Priority: PriorityNormal})
	}
	return err
}

// closeNodeVersionInternal is the lock-free implementation of CloseNodeVersion.
// Callers must hold g.mu.RLock (standalone) or g.mu.Lock (tx/batch).
func (g *Graph) closeNodeVersionInternal(id types.NodeID, t types.Instant) error {
	g.entityLocks.LockEntity(id.SnowflakeID())
	defer g.entityLocks.UnlockEntity(id.SnowflakeID())

	// GetNode returns a deep copy (Store contract). Mutations below are safe.
	current, err := g.store.GetNode(id)
	if err != nil {
		return err
	}

	if tm := current.Temporal(); tm != nil && tm.ValidTo != 0 {
		return ErrAlreadyClosed
	}

	tm := current.Temporal()
	if tm == nil {
		tm = &types.TemporalMetadata{}
		current.SetTemporal(tm)
	}
	tm.ValidTo = t

	// Preserve existing chain position; recompute hash after temporal change.
	prevHash := ""
	if ig := current.Integrity(); ig != nil {
		prevHash = ig.PrevHash
	}
	nodeLabels := g.NodeLabels(current)
	hash := ComputeNodeHash(current, nodeLabels)
	current.SetIntegrity(&types.NodeIntegrity{Hash: hash, PrevHash: prevHash})

	if err := g.store.ReplaceNode(current); err != nil {
		return err
	}
	return nil
}

// GetPreviousRelVersion returns the version immediately before the given version.
// Returns nil, nil if version == 0 or if the predecessor does not exist in history.
func (g *Graph) GetPreviousRelVersion(id types.RelID, version uint32) (*types.Relationship, error) {
	if version == 0 {
		return nil, nil
	}
	r, err := g.store.GetRelVersion(id, version-1)
	if errors.Is(err, ErrVersionNotFound) {
		return nil, nil
	}
	return r, err
}

// GetNextRelVersion returns the version immediately after the given version.
// Returns nil, nil if no newer version exists (the given version IS the current tip).
// Checks history first, then falls back to the current relationship (which may be version+1).
func (g *Graph) GetNextRelVersion(id types.RelID, version uint32) (*types.Relationship, error) {
	r, err := g.store.GetRelVersion(id, version+1)
	if err == nil {
		return r, nil
	}
	if !errors.Is(err, ErrVersionNotFound) {
		return nil, err
	}
	// Not in history: the current rel itself may be version+1.
	current, err2 := g.store.GetRelationship(id)
	if err2 != nil {
		if errors.Is(err2, ErrRelNotFound) {
			return nil, nil
		}
		return nil, err2
	}
	if current.Version() == version+1 {
		return current, nil
	}
	return nil, nil
}

// CloseRelVersion sets ValidTo on the current relationship to t, marking it
// temporally expired without deleting it or incrementing its version number.
// Acquires g.mu.RLock for transaction isolation — blocked while a tx holds g.mu.Lock.
func (g *Graph) CloseRelVersion(id types.RelID, t types.Instant) error {
	g.mu.RLock()
	err := g.closeRelVersionInternal(id, t)
	ep := g.events
	g.mu.RUnlock()
	if err == nil {
		dispatchEvent(ep, Event{Type: EventRelUpdate, EntityID: types.EntityID(id), Timestamp: nowInstant(), Priority: PriorityNormal})
	}
	return err
}

// closeRelVersionInternal is the lock-free implementation of CloseRelVersion.
// Callers must hold g.mu.RLock (standalone) or g.mu.Lock (tx/batch).
func (g *Graph) closeRelVersionInternal(id types.RelID, t types.Instant) error {
	g.entityLocks.LockEntity(id.SnowflakeID())
	defer g.entityLocks.UnlockEntity(id.SnowflakeID())

	// GetRelationship returns a deep copy (Store contract). Mutations below are safe.
	current, err := g.store.GetRelationship(id)
	if err != nil {
		return err
	}

	if tm := current.Temporal(); tm != nil && tm.ValidTo != 0 {
		return ErrAlreadyClosed
	}

	tm := current.Temporal()
	if tm == nil {
		tm = &types.TemporalMetadata{}
		current.SetTemporal(tm)
	}
	tm.ValidTo = t

	// Preserve existing chain position; recompute hash after temporal change.
	prevHash := ""
	if ig := current.Integrity(); ig != nil {
		prevHash = ig.PrevHash
	}
	relTypeName := g.RelationshipType(current)
	hash := ComputeRelHash(current, relTypeName)
	current.SetIntegrity(&types.RelIntegrity{Hash: hash, PrevHash: prevHash})

	if err := g.store.ReplaceRelationship(current); err != nil {
		return err
	}
	return nil
}
