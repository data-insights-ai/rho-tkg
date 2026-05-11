package core

import (
	"fmt"
	"reflect"
	"strings"

	temporalpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/temporal"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// Add appends a constraint to the current constraint set.
// Constraints are checked at relationship write time.
// Typically called once at startup before any writes.
func (co *ConstraintOps) Add(constraint temporalpkg.TemporalConstraint) error {
	if err := constraint.Validate(); err != nil {
		return err
	}
	c := co.c
	if err := c.checkOpen(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed.Load() {
		return ErrGraphClosed
	}
	c.constraints = c.constraints.Add(constraint)
	return nil
}

// Set replaces the entire constraint set.
// Pass an empty ConstraintSet to remove all constraints.
func (co *ConstraintOps) Set(cs ConstraintSet) error {
	if err := cs.Validate(); err != nil {
		return err
	}
	c := co.c
	if err := c.checkOpen(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed.Load() {
		return ErrGraphClosed
	}
	c.constraints = cs
	return nil
}

// Get returns the current constraint set (defensive copy).
func (co *ConstraintOps) Get() ConstraintSet {
	c := co.c
	c.mu.RLock()
	cs := c.constraints
	c.mu.RUnlock()
	return cs
}

// validateName checks a label or relationship type name against name limits.
func (c *Core) validateName(name string) error {
	if strings.TrimSpace(name) == "" {
		return ErrEmptyName
	}
	if len(name) > c.validation.MaxNameLength {
		return fmt.Errorf("%w: %q (%d > %d)", ErrNameTooLong, name, len(name), c.validation.MaxNameLength)
	}
	return nil
}

func (c *Core) validateRegistryNames(kind string, names []string) error {
	for i := 1; i < len(names); i++ {
		if err := c.validateName(names[i]); err != nil {
			return fmt.Errorf("%s registry name[%d]: %w", kind, i, err)
		}
	}
	return nil
}

const maxPropertyValueLimitDepth = 32

// validatePropertyEntry checks a single key-value pair against validation limits.
// MaxPropertyValueSize applies to every string nested inside the property value.
func (c *Core) validatePropertyEntry(key string, val any) error {
	if len(key) > c.validation.MaxPropertyKeyLength {
		return fmt.Errorf("%w: %q (%d > %d)", ErrKeyTooLong, key, len(key), c.validation.MaxPropertyKeyLength)
	}
	if err := c.validatePropertyValueLimit(key, reflect.ValueOf(val), 0); err != nil {
		return err
	}
	return nil
}

type updateProvenance struct {
	authorID     string
	signature    []byte
	authorizedBy string
	authLevel    uint8
}

func (c *Core) prepareUpdateProperties(updates map[string]any, operation string) (updateProvenance, map[string]any, error) {
	authorID, sig, authorizedBy, authLevel, filtered, err := extractProvenance(updates)
	if err != nil {
		return updateProvenance{}, nil, err
	}
	if err := c.validatePropertyUpdates(filtered, operation); err != nil {
		return updateProvenance{}, nil, err
	}
	return updateProvenance{
		authorID:     authorID,
		signature:    sig,
		authorizedBy: authorizedBy,
		authLevel:    authLevel,
	}, filtered, nil
}

func (c *Core) cloneQueuedUpdateMap(updates map[string]any, operation string) (map[string]any, error) {
	if updates == nil {
		return nil, nil
	}
	_, filtered, err := c.prepareUpdateProperties(updates, operation)
	if err != nil {
		return nil, err
	}
	cloned := cloneProvenanceUpdateKeys(updates)
	ps, err := types.NewPropertySlice(filtered)
	if err != nil {
		return nil, err
	}
	for _, p := range ps {
		cloned[p.Key] = p.Value
	}
	return cloned, nil
}

func cloneProvenanceUpdateKeys(updates map[string]any) map[string]any {
	out := make(map[string]any, len(updates))
	for _, key := range []string{"tkg_author_id", "tkg_signature", "tkg_authorized_by", "tkg_auth_level"} {
		v, ok := updates[key]
		if !ok {
			continue
		}
		if key == "tkg_signature" {
			if b, ok := v.([]byte); ok {
				out[key] = types.CloneBytes(b)
				continue
			}
		}
		out[key] = v
	}
	return out
}

func (c *Core) validatePropertyUpdates(updates map[string]any, operation string) error {
	for key, val := range updates {
		if types.IsShadowKey(key) {
			return fmt.Errorf("graph: %s: %w: %q", operation, types.ErrReservedPrefix, key)
		}
		if val != nil {
			if err := types.ValidatePropertyValue(val); err != nil {
				return fmt.Errorf("graph: %s property %q: %w", operation, key, err)
			}
			if err := c.validatePropertyEntry(key, val); err != nil {
				return err
			}
		} else if len(key) > c.validation.MaxPropertyKeyLength {
			return fmt.Errorf("%w: %q (%d > %d)", ErrKeyTooLong, key, len(key), c.validation.MaxPropertyKeyLength)
		}
	}
	return nil
}

func (c *Core) validatePropertyValueLimit(key string, rv reflect.Value, depth int) error {
	if !rv.IsValid() {
		return nil
	}
	if depth > maxPropertyValueLimitDepth {
		return fmt.Errorf("%w: key %q", types.ErrMaxDepthExceeded, key)
	}

	switch rv.Kind() {
	case reflect.Interface:
		if rv.IsNil() {
			return nil
		}
		return c.validatePropertyValueLimit(key, rv.Elem(), depth)
	case reflect.String:
		if rv.Len() > c.validation.MaxPropertyValueSize {
			return fmt.Errorf("%w: key %q (%d > %d)", ErrValueTooLarge, key, rv.Len(), c.validation.MaxPropertyValueSize)
		}
		return nil
	case reflect.Slice:
		if rv.IsNil() {
			return nil
		}
		for i := range rv.Len() {
			if err := c.validatePropertyValueLimit(key, rv.Index(i), depth+1); err != nil {
				return err
			}
		}
		return nil
	case reflect.Map:
		if rv.IsNil() {
			return nil
		}
		iter := rv.MapRange()
		for iter.Next() {
			k := iter.Key()
			if k.Kind() == reflect.String && k.Len() > c.validation.MaxPropertyValueSize {
				return fmt.Errorf("%w: key %q nested map key (%d > %d)", ErrValueTooLarge, key, k.Len(), c.validation.MaxPropertyValueSize)
			}
			if err := c.validatePropertyValueLimit(key, iter.Value(), depth+1); err != nil {
				return err
			}
		}
		return nil
	default:
		return nil
	}
}

// validateProperties checks all entries in a properties map against validation limits.
func (c *Core) validateProperties(props map[string]any) error {
	if len(props) > c.validation.MaxPropertiesPerEntity {
		return fmt.Errorf("%w: %d > %d", ErrTooManyProperties, len(props), c.validation.MaxPropertiesPerEntity)
	}
	for key, val := range props {
		if err := c.validatePropertyEntry(key, val); err != nil {
			return err
		}
	}
	return nil
}

// NextID generates a unique typed ID for a new node.
func (n *NodeOps) NextID() types.NodeID {
	c := n.c
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed.Load() {
		return 0
	}
	return c.nextNodeID()
}

// NextID generates a unique typed ID for a new relationship.
func (r *RelOps) NextID() types.RelID {
	c := r.c
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed.Load() {
		return 0
	}
	return c.nextRelID()
}

func (c *Core) nextNodeID() types.NodeID {
	return types.NodeID(c.nodeIDGen.Generate())
}

func (c *Core) nextRelID() types.RelID {
	return types.RelID(c.relIDGen.Generate())
}
