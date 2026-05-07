package core

import (
	"fmt"

	temporalpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/temporal"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// AddTemporalConstraint appends a constraint to the current constraint set.
// Constraints are checked at relationship write time (AddRelationship, ImportRelationshipWithID).
// Typically called once at startup before any writes.
func (c *Core) AddTemporalConstraint(constraint temporalpkg.TemporalConstraint) {
	c.mu.Lock()
	c.constraints = c.constraints.Add(constraint)
	c.mu.Unlock()
}

// SetTemporalConstraints replaces the entire constraint set.
// Pass an empty ConstraintSet to remove all constraints.
func (c *Core) SetTemporalConstraints(cs ConstraintSet) {
	c.mu.Lock()
	c.constraints = cs
	c.mu.Unlock()
}

// TemporalConstraints returns the current constraint set (defensive copy).
func (c *Core) TemporalConstraints() ConstraintSet {
	c.mu.RLock()
	cs := c.constraints
	c.mu.RUnlock()
	return cs
}

// validateName checks a label or relationship type name against MaxNameLength.
func (c *Core) validateName(name string) error {
	if len(name) > c.validation.MaxNameLength {
		return fmt.Errorf("%w: %q (%d > %d)", ErrNameTooLong, name, len(name), c.validation.MaxNameLength)
	}
	return nil
}

// validatePropertyEntry checks a single key-value pair against validation limits.
// Checks MaxPropertyKeyLength and MaxPropertyValueSize (string values only).
func (c *Core) validatePropertyEntry(key string, val any) error {
	if len(key) > c.validation.MaxPropertyKeyLength {
		return fmt.Errorf("%w: %q (%d > %d)", ErrKeyTooLong, key, len(key), c.validation.MaxPropertyKeyLength)
	}
	if s, ok := val.(string); ok {
		if len(s) > c.validation.MaxPropertyValueSize {
			return fmt.Errorf("%w: key %q (%d > %d)", ErrValueTooLarge, key, len(s), c.validation.MaxPropertyValueSize)
		}
	}
	return nil
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

// NextNodeID generates a unique typed ID for a new node.
func (c *Core) NextNodeID() types.NodeID {
	return types.NodeID(c.nodeIDGen.Generate())
}

// NextRelID generates a unique typed ID for a new relationship.
func (c *Core) NextRelID() types.RelID {
	return types.RelID(c.relIDGen.Generate())
}
