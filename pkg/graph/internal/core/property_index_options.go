package core

import storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"

// PropertyOptions returns the options the property index on (label,
// propertyKey) was created with (round 4 R2), ok=false when there is none.
// A store that cannot create indexes with options reports an existing index
// with zero options (it is a plain index).
func (i *IndexOps) PropertyOptions(label, propertyKey string) (storepkg.PropertyIndexOptions, bool, error) {
	c := i.c
	var (
		out storepkg.PropertyIndexOptions
		ok  bool
	)
	err := c.readUnderRLock(func() error {
		if err := c.validateIndexLabel(label); err != nil {
			return err
		}
		if err := c.validateIndexPropertyKey(propertyKey); err != nil {
			return err
		}
		tok, found := c.labels.Lookup(label)
		if !found {
			return nil
		}
		if oc, native := c.store.(storepkg.PropertyIndexOptionsCapability); native {
			var err error
			out, ok, err = oc.PropertyIndexOptions(tok, propertyKey)
			return err
		}
		has, err := i.hasPropertyIndexLocked(tok, propertyKey)
		ok = has
		return err
	})
	if err != nil {
		return storepkg.PropertyIndexOptions{}, false, err
	}
	return out, ok, nil
}

// RelPropertyOptions is the relationship mirror of PropertyOptions.
func (i *IndexOps) RelPropertyOptions(typeName, propertyKey string) (storepkg.PropertyIndexOptions, bool, error) {
	c := i.c
	var (
		out storepkg.PropertyIndexOptions
		ok  bool
	)
	err := c.readUnderRLock(func() error {
		if err := c.validateIndexName(typeName); err != nil {
			return err
		}
		if err := c.validateIndexPropertyKey(propertyKey); err != nil {
			return err
		}
		tok, found := c.relTypes.Lookup(typeName)
		if !found {
			return nil
		}
		if oc, native := c.store.(storepkg.RelPropertyIndexOptionsCapability); native {
			var err error
			out, ok, err = oc.RelPropertyIndexOptions(tok, propertyKey)
			return err
		}
		cap, has := c.store.(storepkg.RelPropertyIndexIntrospectionCapability)
		if !has {
			return nil
		}
		var err error
		ok, err = cap.HasRelPropertyIndex(tok, propertyKey)
		return err
	})
	if err != nil {
		return storepkg.PropertyIndexOptions{}, false, err
	}
	return out, ok, nil
}

// hasPropertyIndexLocked asks the store's introspection door, false without
// one. Caller holds the read lock.
func (i *IndexOps) hasPropertyIndexLocked(tok uint16, propertyKey string) (bool, error) {
	cap, ok := i.c.store.(storepkg.PropertyIndexIntrospectionCapability)
	if !ok {
		return false, nil
	}
	return cap.HasPropertyIndex(tok, propertyKey)
}
