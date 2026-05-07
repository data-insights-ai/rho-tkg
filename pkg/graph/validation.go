package graph

import (
	"fmt"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

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
