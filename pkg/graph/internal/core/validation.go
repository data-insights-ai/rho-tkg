package core

import (
	"fmt"
	"reflect"
	"unicode"
	"unicode/utf8"

	temporalpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Add appends a constraint to the current constraint set.
// Constraints are checked at relationship write time.
// Typically called once at startup before any writes.
func (co *ConstraintOps) Add(constraint temporalpkg.TemporalConstraint) error {
	if co == nil || co.c == nil {
		return ErrNilGraph
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
	if err := constraint.Validate(); err != nil {
		return err
	}
	c.constraints = c.constraints.Add(constraint)
	return nil
}

// Set replaces the entire constraint set.
// Pass an empty ConstraintSet to remove all constraints.
func (co *ConstraintOps) Set(cs ConstraintSet) error {
	if co == nil || co.c == nil {
		return ErrNilGraph
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
	if err := cs.Validate(); err != nil {
		return err
	}
	c.constraints = cs
	return nil
}

// Get returns the current constraint set (defensive copy).
func (co *ConstraintOps) Get() ConstraintSet {
	if co == nil || co.c == nil {
		return ConstraintSet{}
	}
	c := co.c
	c.mu.RLock()
	cs := c.constraints
	c.mu.RUnlock()
	return cs
}

// validateName checks a label or relationship type name against name limits.
func (c *Core) validateName(name string) error {
	if isBlankName(name) {
		return ErrEmptyName
	}
	if len(name) > c.validation.MaxNameLength {
		return fmt.Errorf("%w: %q (%d > %d)", ErrNameTooLong, name, len(name), c.validation.MaxNameLength)
	}
	return nil
}

func (c *Core) validateNodeCreateLabels(labels []string) error {
	if len(labels) == 0 {
		return ErrNoLabels
	}

	for _, label := range labels {
		if err := c.validateName(label); err != nil {
			return err
		}
	}
	if len(labels) <= c.validation.MaxLabelsPerNode {
		return nil
	}

	seen := make(map[string]struct{}, len(labels))
	for _, label := range labels {
		seen[label] = struct{}{}
	}
	if len(seen) > c.validation.MaxLabelsPerNode {
		return fmt.Errorf("%w: %d > %d", ErrTooManyLabels, len(seen), c.validation.MaxLabelsPerNode)
	}
	return nil
}

func isBlankName(name string) bool {
	if name == "" {
		return true
	}
	for i := 0; i < len(name); {
		c := name[i]
		if c < utf8.RuneSelf {
			if c != ' ' && (c < '\t' || c > '\r') {
				return false
			}
			i++
			continue
		}
		r, size := utf8.DecodeRuneInString(name[i:])
		if !unicode.IsSpace(r) {
			return false
		}
		i += size
	}
	return true
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

// validateOwnedPropertyEntryForCreate checks graph-level limits before a create
// path immediately hands the same map to types.NewOwnedPropertySlice. Exact
// primitive values do not need an early type-validation pass because the types
// constructor will perform the authoritative validation and defensive copy.
// Dynamic containers and custom/reflective shapes are still prevalidated so an
// unsupported value does not get reported as a graph size-limit error.
func (c *Core) validateOwnedPropertyEntryForCreate(key string, val any) error {
	if err := c.validatePropertyKeyLength(key); err != nil {
		return err
	}
	if propertyValueNeedsLimitPrevalidation(val) {
		if err := types.ValidatePropertyValue(val); err != nil {
			return fmt.Errorf("graph: property %q: %w", key, err)
		}
	}
	if c.propKeys != nil && !types.IsShadowKey(key) {
		_, _ = c.propKeys.GetOrCreate(key)
	}
	return c.validatePropertyValueLimitTyped(key, val, 0)
}

// validatePropertyEntryLimits assumes the caller has already accepted val via
// types.ValidatePropertyValue and only checks graph-level size limits.
func (c *Core) validatePropertyEntryLimits(key string, val any) error {
	if err := c.validatePropertyKeyLength(key); err != nil {
		return err
	}
	if c.propKeys != nil && !types.IsShadowKey(key) {
		_, _ = c.propKeys.GetOrCreate(key)
	}
	return c.validatePropertyValueLimitTyped(key, val, 0)
}

func propertyValueNeedsLimitPrevalidation(val any) bool {
	switch val.(type) {
	case nil,
		bool,
		string,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64,
		[]int, []int64, []float32, []float64, []byte, []bool, []string,
		map[string]string:
		return false
	default:
		return true
	}
}

func (c *Core) validatePropertyKeyLength(key string) error {
	if len(key) > c.validation.MaxPropertyKeyLength {
		return fmt.Errorf("%w: %q (%d > %d)", ErrKeyTooLong, key, len(key), c.validation.MaxPropertyKeyLength)
	}
	return nil
}

func (c *Core) validatePropertyValueLimitTyped(key string, val any, depth int) error {
	if depth > maxPropertyValueLimitDepth {
		return fmt.Errorf("%w: key %q", types.ErrMaxDepthExceeded, key)
	}

	switch v := val.(type) {
	case nil,
		bool,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64:
		return nil
	case string:
		if len(v) > c.validation.MaxPropertyValueSize {
			return fmt.Errorf("%w: key %q (%d > %d)", ErrValueTooLarge, key, len(v), c.validation.MaxPropertyValueSize)
		}
		return nil
	case types.TemporalValue:
		// Shape (kind range, bounded rendering) is enforced by
		// types.ValidatePropertyValue; the rendering also honors the
		// graph-level size limit like any string.
		if len(v.Value) > c.validation.MaxPropertyValueSize {
			return fmt.Errorf("%w: key %q (%d > %d)", ErrValueTooLarge, key, len(v.Value), c.validation.MaxPropertyValueSize)
		}
		return nil
	case []int:
		if len(v) > 0 && depth+1 > maxPropertyValueLimitDepth {
			return fmt.Errorf("%w: key %q", types.ErrMaxDepthExceeded, key)
		}
		return nil
	case []int64:
		if len(v) > 0 && depth+1 > maxPropertyValueLimitDepth {
			return fmt.Errorf("%w: key %q", types.ErrMaxDepthExceeded, key)
		}
		return nil
	case []float32:
		if len(v) > 0 && depth+1 > maxPropertyValueLimitDepth {
			return fmt.Errorf("%w: key %q", types.ErrMaxDepthExceeded, key)
		}
		return nil
	case []float64:
		if len(v) > 0 && depth+1 > maxPropertyValueLimitDepth {
			return fmt.Errorf("%w: key %q", types.ErrMaxDepthExceeded, key)
		}
		return nil
	case []byte:
		if len(v) > 0 && depth+1 > maxPropertyValueLimitDepth {
			return fmt.Errorf("%w: key %q", types.ErrMaxDepthExceeded, key)
		}
		return nil
	case []bool:
		if len(v) > 0 && depth+1 > maxPropertyValueLimitDepth {
			return fmt.Errorf("%w: key %q", types.ErrMaxDepthExceeded, key)
		}
		return nil
	case []string:
		if len(v) > 0 && depth+1 > maxPropertyValueLimitDepth {
			return fmt.Errorf("%w: key %q", types.ErrMaxDepthExceeded, key)
		}
		for _, s := range v {
			if len(s) > c.validation.MaxPropertyValueSize {
				return fmt.Errorf("%w: key %q (%d > %d)", ErrValueTooLarge, key, len(s), c.validation.MaxPropertyValueSize)
			}
		}
		return nil
	case []any:
		for _, item := range v {
			if err := c.validatePropertyValueLimitTyped(key, item, depth+1); err != nil {
				return err
			}
		}
		return nil
	case map[string]string:
		for nestedKey, nestedValue := range v {
			if len(nestedKey) > c.validation.MaxPropertyValueSize {
				return fmt.Errorf("%w: key %q nested map key (%d > %d)", ErrValueTooLarge, key, len(nestedKey), c.validation.MaxPropertyValueSize)
			}
			if depth+1 > maxPropertyValueLimitDepth {
				return fmt.Errorf("%w: key %q", types.ErrMaxDepthExceeded, key)
			}
			if len(nestedValue) > c.validation.MaxPropertyValueSize {
				return fmt.Errorf("%w: key %q (%d > %d)", ErrValueTooLarge, key, len(nestedValue), c.validation.MaxPropertyValueSize)
			}
		}
		return nil
	case map[string]any:
		for nestedKey, nestedValue := range v {
			if len(nestedKey) > c.validation.MaxPropertyValueSize {
				return fmt.Errorf("%w: key %q nested map key (%d > %d)", ErrValueTooLarge, key, len(nestedKey), c.validation.MaxPropertyValueSize)
			}
			if err := c.validatePropertyValueLimitTyped(key, nestedValue, depth+1); err != nil {
				return err
			}
		}
		return nil
	}
	if err := c.validatePropertyValueLimit(key, reflect.ValueOf(val), depth); err != nil {
		return err
	}
	return nil
}

// updateProvenance carries the four caller-supplied integrity fields
// (`tkg_author_id`, `tkg_signature`, `tkg_authorized_by`, `tkg_auth_level`)
// extracted from an update map, along with per-field presence flags. The
// `present` collective flag is true iff at least one field was supplied —
// it gates the "this update is non-empty" check. The per-field flags drive
// the merge against existing integrity in update paths: a field NOT
// restated is preserved from the previous version (B3 — previously every
// Update wiped all four fields if any were restated, since extractProvenance
// returned zeros for missing fields).
type updateProvenance struct {
	authorID        string
	signature       []byte
	authorizedBy    string
	authLevel       uint8
	hasAuthorID     bool
	hasSignature    bool
	hasAuthorizedBy bool
	hasAuthLevel    bool
	present         bool
}

// updateTemporal carries caller-supplied tkg_valid_from / tkg_valid_to from an
// Update map. Per-field presence flags distinguish "caller did not supply"
// from "caller supplied 0" (which is a valid sentinel meaning "open/unset").
// The collective `present` flag gates the "this update is non-empty" check.
type updateTemporal struct {
	validFrom    types.Instant
	validTo      types.Instant
	hasValidFrom bool
	hasValidTo   bool
	present      bool
}

type preparedUpdateProperties struct {
	provenance  updateProvenance
	temporal    updateTemporal
	properties  map[string]any
	originalLen int
}

func (c *Core) prepareUpdateProperties(updates map[string]any, operation string) (updateProvenance, updateTemporal, map[string]any, error) {
	prov, filtered, err := extractProvenanceTracked(updates)
	if err != nil {
		return updateProvenance{}, updateTemporal{}, nil, err
	}
	tmp, filtered, err := extractTemporalTracked(filtered)
	if err != nil {
		return updateProvenance{}, updateTemporal{}, nil, err
	}
	if err := c.validatePropertyUpdates(filtered, operation); err != nil {
		return updateProvenance{}, updateTemporal{}, nil, err
	}
	return prov, tmp, filtered, nil
}

func preparedUpdateCanBeReadOnlyNoOp(prov updateProvenance, tmp updateTemporal) bool {
	return !prov.present && !tmp.present
}

func (c *Core) prepareQueuedUpdateProperties(updates map[string]any, operation string) (preparedUpdateProperties, error) {
	if updates == nil {
		return preparedUpdateProperties{}, nil
	}
	prov, tmp, filtered, err := c.prepareUpdateProperties(updates, operation)
	if err != nil {
		return preparedUpdateProperties{}, err
	}
	ps, err := types.NewPropertySlice(filtered)
	if err != nil {
		return preparedUpdateProperties{}, err
	}
	cloned := make(map[string]any, len(ps))
	for _, p := range ps {
		cloned[p.Key] = p.Value
	}
	return preparedUpdateProperties{
		provenance:  prov,
		temporal:    tmp,
		properties:  cloned,
		originalLen: len(updates),
	}, nil
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
			if err := c.validatePropertyEntryLimits(key, val); err != nil {
				return err
			}
		} else if err := c.validatePropertyKeyLength(key); err != nil {
			return err
		}
		// Register the key in the property-key registry so subsequent wire
		// reads / monitoring see consistent cardinality. The wire encoder
		// doesn't consult the registry yet (deferred until call-site
		// threading lands); for now this populates the registry so
		// consumers can monitor growth via g.Stats().PropertyKeyCount().
		if c.propKeys != nil {
			_, _ = c.propKeys.GetOrCreate(key)
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
	case reflect.Pointer:
		if rv.IsNil() {
			return nil
		}
		return c.validatePropertyValueLimit(key, rv.Elem(), depth+1)
	case reflect.Struct:
		for i := range rv.NumField() {
			if err := c.validatePropertyValueLimit(key, rv.Field(i), depth+1); err != nil {
				return err
			}
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
		if err := c.validateOwnedPropertyEntryForCreate(key, val); err != nil {
			return err
		}
	}
	return nil
}

// NextID generates a unique typed ID for a new node.
func (n *NodeOps) NextID() types.NodeID {
	c := n.c
	if c.closed.Load() {
		return 0
	}
	return c.nextNodeID()
}

// NextID generates a unique typed ID for a new relationship.
func (r *RelOps) NextID() types.RelID {
	c := r.c
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
