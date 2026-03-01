package graph

import (
	"context"
	"fmt"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg-v3/pkg/types"
)

// checkCtx performs a non-blocking context cancellation check.
// Returns ctx.Err() if the context is done, nil otherwise.
// Zero overhead when the context is not cancelled.
func checkCtx(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

// GetNodeWithContext retrieves a node by snowflake ID with context support.
func (g *Graph) GetNodeWithContext(ctx context.Context, id snowflake.ID) (*types.Node, error) {
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}
	return g.store.GetNode(id)
}

// GetRelationshipWithContext retrieves a relationship by snowflake ID with context support.
func (g *Graph) GetRelationshipWithContext(ctx context.Context, id snowflake.ID) (*types.Relationship, error) {
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}
	return g.store.GetRelationship(id)
}

// AddNodeWithContext creates a new node with the given labels and properties.
// Checks context at entry and before the store write.
func (g *Graph) AddNodeWithContext(ctx context.Context, labels []string, props map[string]any) (*types.Node, error) {
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	if len(labels) == 0 {
		return nil, ErrNoLabels
	}

	// Bulk-build properties first — fail fast before generating an ID.
	ps, err := types.NewPropertySlice(props)
	if err != nil {
		return nil, fmt.Errorf("graph: node properties: %w", err)
	}

	// Resolve labels to tokens.
	primaryToken, err := g.labels.GetOrCreate(labels[0])
	if err != nil {
		return nil, fmt.Errorf("graph: primary label: %w", err)
	}

	var extraTokens []uint16
	for _, label := range labels[1:] {
		tok, err := g.labels.GetOrCreate(label)
		if err != nil {
			return nil, fmt.Errorf("graph: extra label %q: %w", label, err)
		}
		extraTokens = append(extraTokens, tok)
	}

	id := g.NextNodeID()
	n := types.NewNode(id, primaryToken, extraTokens)
	n.SetProperties(ps)

	hash := ComputeNodeHash(n, labels)
	n.SetIntegrity(&types.NodeIntegrity{Hash: hash, PrevHash: ""})

	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	if err := g.store.PutNode(n); err != nil {
		return nil, err
	}

	return n, nil
}

// AddRelationshipWithContext creates a new directed relationship between two nodes.
// Checks context at entry, before acquiring endpoint locks, and before the store write.
func (g *Graph) AddRelationshipWithContext(ctx context.Context, typeName string, startNode, endNode *types.Node, props map[string]any) (*types.Relationship, error) {
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	if startNode == nil || endNode == nil {
		return nil, ErrNilNode
	}

	// Bulk-build properties first — fail fast before generating an ID.
	ps, err := types.NewPropertySlice(props)
	if err != nil {
		return nil, fmt.Errorf("graph: relationship properties: %w", err)
	}

	typeToken, err := g.relTypes.GetOrCreate(typeName)
	if err != nil {
		return nil, fmt.Errorf("graph: relationship type: %w", err)
	}

	startID := startNode.InternalID().SnowflakeID()
	endID := endNode.InternalID().SnowflakeID()

	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	// Lock both endpoints to prevent write-skew with concurrent DeleteNode.
	// Lock ordering: ascending shard index — deadlock-free.
	g.entityLocks.LockTwo(startID, endID)
	defer g.entityLocks.UnlockTwo(startID, endID)

	id := g.NextRelID()
	r := types.NewRelationship(id, typeToken, startID, endID)
	r.SetProperties(ps)

	hash := ComputeRelHash(r, typeName)
	r.SetIntegrity(&types.RelIntegrity{Hash: hash, PrevHash: ""})

	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	if err := g.store.PutRelationship(r); err != nil {
		return nil, err
	}

	return r, nil
}

// DeleteNodeWithContext atomically removes a node and all connected relationships.
// Checks context at entry and under the entity lock before cascade.
func (g *Graph) DeleteNodeWithContext(ctx context.Context, id snowflake.ID) error {
	if err := checkCtx(ctx); err != nil {
		return err
	}

	g.entityLocks.LockEntity(id)
	defer g.entityLocks.UnlockEntity(id)

	if err := checkCtx(ctx); err != nil {
		return err
	}

	return g.store.DeleteNodeCascade(id)
}

// DeleteRelationshipWithContext removes a relationship from the store.
// Checks context at entry before the store call.
func (g *Graph) DeleteRelationshipWithContext(ctx context.Context, id snowflake.ID) error {
	if err := checkCtx(ctx); err != nil {
		return err
	}
	return g.store.DeleteRelationship(id)
}

// UpdateNodeWithContext applies property updates to an existing node with context support.
// Checks context at entry, before acquiring the entity lock, before the store read,
// before saving version history, and before the final store write.
func (g *Graph) UpdateNodeWithContext(ctx context.Context, id snowflake.ID, updates map[string]any) (*types.Node, error) {
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	if len(updates) == 0 {
		return g.GetNodeWithContext(ctx, id)
	}

	// Phase 1: Pre-validate before acquiring entity lock (fail fast).
	for key, val := range updates {
		if types.IsShadowKey(key) {
			return nil, fmt.Errorf("graph: update node: %w: %q", types.ErrReservedPrefix, key)
		}
		if val != nil {
			if err := types.ValidatePropertyValue(val); err != nil {
				return nil, fmt.Errorf("graph: update node property %q: %w", key, err)
			}
		}
	}

	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	// Phase 2: Entity lock → read-modify-write under serialization.
	g.entityLocks.LockEntity(id)
	defer g.entityLocks.UnlockEntity(id)

	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	current, err := g.store.GetNode(id)
	if err != nil {
		return nil, err
	}

	// Capture current hash for the PrevHash chain.
	prevHash := ""
	if ig := current.Integrity(); ig != nil {
		prevHash = ig.Hash
	}

	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	// Save pre-mutation state to version history.
	if err := g.store.PutNodeVersion(id, current.Version(), current); err != nil {
		return nil, fmt.Errorf("graph: save node version: %w", err)
	}

	for key, val := range updates {
		if val == nil {
			if _, err := current.DeleteProperty(key); err != nil {
				return nil, fmt.Errorf("graph: update node property %q: %w", key, err)
			}
		} else {
			if err := current.SetProperty(key, val); err != nil {
				return nil, fmt.Errorf("graph: update node property %q: %w", key, err)
			}
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

	nodeLabels := g.NodeLabels(current)
	hash := ComputeNodeHash(current, nodeLabels)
	current.SetIntegrity(&types.NodeIntegrity{Hash: hash, PrevHash: prevHash})

	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	if err := g.store.ReplaceNode(current); err != nil {
		return nil, err
	}

	return current, nil
}

// UpdateRelationshipWithContext applies property updates to an existing relationship with context support.
// Checks context at entry, before acquiring the entity lock, before the store read,
// before saving version history, and before the final store write.
func (g *Graph) UpdateRelationshipWithContext(ctx context.Context, id snowflake.ID, updates map[string]any) (*types.Relationship, error) {
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	if len(updates) == 0 {
		return g.GetRelationshipWithContext(ctx, id)
	}

	// Phase 1: Pre-validate before acquiring entity lock (fail fast).
	for key, val := range updates {
		if types.IsShadowKey(key) {
			return nil, fmt.Errorf("graph: update relationship: %w: %q", types.ErrReservedPrefix, key)
		}
		if val != nil {
			if err := types.ValidatePropertyValue(val); err != nil {
				return nil, fmt.Errorf("graph: update relationship property %q: %w", key, err)
			}
		}
	}

	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	// Phase 2: Entity lock on rel ID only — property changes don't affect adjacency.
	g.entityLocks.LockEntity(id)
	defer g.entityLocks.UnlockEntity(id)

	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	current, err := g.store.GetRelationship(id)
	if err != nil {
		return nil, err
	}

	// Capture current hash for the PrevHash chain.
	prevHash := ""
	if ig := current.Integrity(); ig != nil {
		prevHash = ig.Hash
	}

	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	// Save pre-mutation state to version history.
	if err := g.store.PutRelVersion(id, current.Version(), current); err != nil {
		return nil, fmt.Errorf("graph: save rel version: %w", err)
	}

	for key, val := range updates {
		if val == nil {
			if _, err := current.DeleteProperty(key); err != nil {
				return nil, fmt.Errorf("graph: update relationship property %q: %w", key, err)
			}
		} else {
			if err := current.SetProperty(key, val); err != nil {
				return nil, fmt.Errorf("graph: update relationship property %q: %w", key, err)
			}
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

	relTypeName := g.RelationshipType(current)
	hash := ComputeRelHash(current, relTypeName)
	current.SetIntegrity(&types.RelIntegrity{Hash: hash, PrevHash: prevHash})

	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	if err := g.store.ReplaceRelationship(current); err != nil {
		return nil, err
	}

	return current, nil
}
