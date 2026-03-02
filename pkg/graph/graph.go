package graph

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg-v3/pkg/types"
)

// Sentinel errors for entity management.
var (
	ErrNoLabels = errors.New("graph: node requires at least one label")
	ErrNilNode  = errors.New("graph: node must not be nil")
	ErrZeroID   = errors.New("graph: zero ID is not valid for import")
)

// Sentinel errors for validation limits.
var (
	ErrTooManyLabels     = errors.New("graph: too many labels")
	ErrTooManyProperties = errors.New("graph: too many properties")
	ErrKeyTooLong        = errors.New("graph: property key too long")
	ErrValueTooLarge     = errors.New("graph: property value too large")
	ErrNameTooLong       = errors.New("graph: name too long")
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
	MaxLabelsPerNode       int // Default: 50
	MaxPropertiesPerEntity int // Default: 1000
	MaxPropertyKeyLength   int // Default: 256
	MaxPropertyValueSize   int // Default: 65536 (string values only)
	MaxNameLength          int // Default: 256 (label and reltype names)
}

// snowflakeEpoch is the custom epoch for all snowflake ID generation (2026-01-01 UTC).
var snowflakeEpoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// Config holds configuration for the Graph.
type Config struct {
	// SnowflakeNodeID identifies this graph instance (0-511).
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
}

// Graph is the central graph layer. It owns the label and relationship type
// registries, snowflake ID generators, store, and provides string resolution
// for token-based entities.
//
// Entity locks serialize AddRelationship and DeleteNode on overlapping entities
// to prevent write-skew (concurrent AddRelationship(→X) + DeleteNodeCascade(X)
// producing a dangling edge).
type Graph struct {
	labels      *labelRegistry
	relTypes    *relTypeRegistry
	nodeIDGen   *snowflake.Node
	relIDGen    *snowflake.Node
	store       Store
	entityLocks *entityLockManager
	validation  ValidationLimits
	mu          sync.RWMutex // serializes batch writes vs whole-graph temporal reads (Snapshot)
	closeOnce   sync.Once
}

// New creates a new Graph with the given configuration.
// Returns an error if SnowflakeNodeID is out of range (0-511).
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
	if config.SnowflakeNodeID < 0 || config.SnowflakeNodeID > 511 {
		return nil, fmt.Errorf("graph: SnowflakeNodeID must be 0-511, got %d", config.SnowflakeNodeID)
	}

	nodeGen, err := snowflake.NewNode(config.SnowflakeNodeID*2,
		snowflake.WithEpoch(snowflakeEpoch),
		snowflake.WithNodeBits(10),
		snowflake.WithStepBits(12),
	)
	if err != nil {
		return nil, fmt.Errorf("graph: node ID generator: %w", err)
	}
	relGen, err := snowflake.NewNode(config.SnowflakeNodeID*2+1,
		snowflake.WithEpoch(snowflakeEpoch),
		snowflake.WithNodeBits(10),
		snowflake.WithStepBits(12),
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

	g := &Graph{
		labels:      newLabelRegistry(),
		relTypes:    newRelTypeRegistry(),
		nodeIDGen:   nodeGen,
		relIDGen:    relGen,
		entityLocks: newEntityLockManager(),
		validation:  v,
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
				Dir:      config.BadgerDir,
				InMemory: config.BadgerInMemory,
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
			if err := s.SaveLabelRegistry(g.labels); err != nil {
				closeErr = fmt.Errorf("graph: save label registry: %w", err)
			}
			if err := s.SaveRelTypeRegistry(g.relTypes); err != nil {
				closeErr = errors.Join(closeErr, fmt.Errorf("graph: save reltype registry: %w", err))
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

// NextNodeID generates a unique snowflake ID for a new node.
func (g *Graph) NextNodeID() snowflake.ID {
	return g.nodeIDGen.Generate()
}

// NextRelID generates a unique snowflake ID for a new relationship.
func (g *Graph) NextRelID() snowflake.ID {
	return g.relIDGen.Generate()
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

// DeleteNode atomically removes a node and all connected relationships.
// Acquires the entity lock for the node to prevent write-skew with concurrent
// AddRelationship targeting the same node.
// Returns ErrNodeNotFound if the node does not exist.
func (g *Graph) DeleteNode(id snowflake.ID) error {
	return g.DeleteNodeWithContext(context.Background(), id)
}

// DeleteRelationship removes a relationship from the store.
// Returns ErrRelNotFound if the relationship does not exist.
func (g *Graph) DeleteRelationship(id snowflake.ID) error {
	return g.DeleteRelationshipWithContext(context.Background(), id)
}

// --- Update operations ---

// UpdateNode applies property updates to an existing node.
// The updates map keys are property names; values are the new values.
// A nil value deletes the property. Keys with the "tkg_" prefix are rejected.
// Returns the updated node. Empty updates map is a no-op (no version bump).
func (g *Graph) UpdateNode(id snowflake.ID, updates map[string]any) (*types.Node, error) {
	return g.UpdateNodeWithContext(context.Background(), id, updates)
}

// UpdateRelationship applies property updates to an existing relationship.
// The updates map keys are property names; values are the new values.
// A nil value deletes the property. Keys with the "tkg_" prefix are rejected.
// Returns the updated relationship. Empty updates map is a no-op.
func (g *Graph) UpdateRelationship(id snowflake.ID, updates map[string]any) (*types.Relationship, error) {
	return g.UpdateRelationshipWithContext(context.Background(), id, updates)
}

// SetNodeProperty sets a single property on an existing node.
func (g *Graph) SetNodeProperty(id snowflake.ID, key string, value any) error {
	_, err := g.UpdateNode(id, map[string]any{key: value})
	return err
}

// DeleteNodeProperty removes a single property from an existing node.
func (g *Graph) DeleteNodeProperty(id snowflake.ID, key string) error {
	_, err := g.UpdateNode(id, map[string]any{key: nil})
	return err
}

// SetRelationshipProperty sets a single property on an existing relationship.
func (g *Graph) SetRelationshipProperty(id snowflake.ID, key string, value any) error {
	_, err := g.UpdateRelationship(id, map[string]any{key: value})
	return err
}

// DeleteRelationshipProperty removes a single property from an existing relationship.
func (g *Graph) DeleteRelationshipProperty(id snowflake.ID, key string) error {
	_, err := g.UpdateRelationship(id, map[string]any{key: nil})
	return err
}

// --- Version history passthrough ---

// GetNodeHistory returns all version history snapshots for the given node.
func (g *Graph) GetNodeHistory(id snowflake.ID) ([]*types.Node, error) {
	return g.store.GetNodeHistory(id)
}

// GetRelHistory returns all version history snapshots for the given relationship.
func (g *Graph) GetRelHistory(id snowflake.ID) ([]*types.Relationship, error) {
	return g.store.GetRelHistory(id)
}

// --- Store passthrough queries ---

// GetNode retrieves a node by snowflake ID.
func (g *Graph) GetNode(id snowflake.ID) (*types.Node, error) {
	return g.GetNodeWithContext(context.Background(), id)
}

// GetRelationship retrieves a relationship by snowflake ID.
func (g *Graph) GetRelationship(id snowflake.ID) (*types.Relationship, error) {
	return g.GetRelationshipWithContext(context.Background(), id)
}

// NodesByLabel returns nodes with the given label (resolved from string),
// with optional pagination. Returns nil if the label is not registered.
func (g *Graph) NodesByLabel(label string, opts QueryOpts) ([]*types.Node, error) {
	tok, ok := g.labels.Lookup(label)
	if !ok {
		return nil, nil
	}
	return g.store.NodesByLabel(tok, opts)
}

// RelationshipsByType returns relationships with the given type (resolved from string),
// with optional pagination. Returns nil if the type is not registered.
func (g *Graph) RelationshipsByType(typeName string, opts QueryOpts) ([]*types.Relationship, error) {
	tok, ok := g.relTypes.Lookup(typeName)
	if !ok {
		return nil, nil
	}
	return g.store.RelationshipsByType(tok, opts)
}

// OutgoingRelationships returns all outgoing relationships from the given node.
// If typeName is empty, all types are returned. If typeName is non-empty, only
// relationships of that type are returned (nil if the type is not registered).
func (g *Graph) OutgoingRelationships(nodeID snowflake.ID, typeName string) ([]*types.Relationship, error) {
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

// IncomingRelationships returns all incoming relationships to the given node.
// If typeName is empty, all types are returned. If typeName is non-empty, only
// relationships of that type are returned (nil if the type is not registered).
func (g *Graph) IncomingRelationships(nodeID snowflake.ID, typeName string) ([]*types.Relationship, error) {
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
func (g *Graph) AllNodes(opts QueryOpts) ([]*types.Node, error) { return g.store.AllNodes(opts) }

// AllRelationships returns all relationships in the store, with optional pagination.
func (g *Graph) AllRelationships(opts QueryOpts) ([]*types.Relationship, error) {
	return g.store.AllRelationships(opts)
}

// GetNodesByIDs returns nodes matching the given IDs. Missing IDs are skipped.
func (g *Graph) GetNodesByIDs(ids []snowflake.ID) ([]*types.Node, error) {
	return g.store.GetNodesByIDs(ids)
}

// GetRelationshipsByIDs returns relationships matching the given IDs. Missing IDs are skipped.
func (g *Graph) GetRelationshipsByIDs(ids []snowflake.ID) ([]*types.Relationship, error) {
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

// NodesByLabelAndProperty returns nodes matching the label and property value,
// with optional pagination. Resolves the label name to a token.
// Returns nil if the label is not registered.
func (g *Graph) NodesByLabelAndProperty(label, key string, value any, opts QueryOpts) ([]*types.Node, error) {
	tok, ok := g.labels.Lookup(label)
	if !ok {
		return nil, nil
	}
	return g.store.NodesByLabelAndProperty(tok, key, value, opts)
}

// ArchiveNode moves a reference node and its relationships from the reference
// shard to the reference archive. Only available with TieredStore.
// Returns ErrNodeNotFound if the node is not in the reference shard.
func (g *Graph) ArchiveNode(id snowflake.ID) error {
	if ts, ok := g.store.(*TieredStore); ok {
		return ts.ArchiveNode(id)
	}
	return fmt.Errorf("graph: ArchiveNode requires TieredStore")
}

// RestoreNode moves a reference node and its relationships from the reference
// archive back to the reference shard. Only available with TieredStore.
// Returns ErrNodeNotFound if the node is not in the archive.
func (g *Graph) RestoreNode(id snowflake.ID) error {
	if ts, ok := g.store.(*TieredStore); ok {
		return ts.RestoreNode(id)
	}
	return fmt.Errorf("graph: RestoreNode requires TieredStore")
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
	return fmt.Errorf("graph: ForceRotate requires TieredStore")
}

// ListShards returns information about all shards. Only available with TieredStore.
func (g *Graph) ListShards() ([]ShardInfo, error) {
	if ts, ok := g.store.(*TieredStore); ok {
		return ts.ListShards(), nil
	}
	return nil, fmt.Errorf("graph: ListShards requires TieredStore")
}

// RebuildCatalog reconstructs the shard catalog from live state.
// Only available with TieredStore.
func (g *Graph) RebuildCatalog() error {
	if ts, ok := g.store.(*TieredStore); ok {
		return ts.RebuildCatalog()
	}
	return fmt.Errorf("graph: RebuildCatalog requires TieredStore")
}

// RunRepair scans for cross-shard consistency issues and fixes them.
// Only available with TieredStore.
func (g *Graph) RunRepair() (*RepairResult, error) {
	if ts, ok := g.store.(*TieredStore); ok {
		return ts.RunRepair()
	}
	return nil, fmt.Errorf("graph: RunRepair requires TieredStore")
}

// VerifyShard runs hash chain verification on all entities in a shard.
// Only available with TieredStore.
func (g *Graph) VerifyShard(shardName string) (*VerifyResult, error) {
	if ts, ok := g.store.(*TieredStore); ok {
		return ts.VerifyShard(g, shardName)
	}
	return nil, fmt.Errorf("graph: VerifyShard requires TieredStore")
}

// Reset atomically clears all entities, indexes, history, and counters from
// the graph while preserving registries (label and relationship type tokens).
// Acquires the graph write lock to prevent concurrent operations.
func (g *Graph) Reset() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.store.Clear()
}
