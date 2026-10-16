package memory

import (
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Round 9 R9-3 (value statistics of an indexed property): the index's
// current value statistics and the membership sidecar's members of every
// value (store.NodePropertyTxMemberAnyCapability).

var (
	_ storecontract.NodePropertyTxMemberAnyCapability = (*Store)(nil)
	_ storecontract.RelPropertyTxMemberAnyCapability  = (*Store)(nil)
)

// NodePropertyValueStats returns the value statistics of the property index
// on (labelToken, propertyKey); exact=false without an index, while it is
// built, or for an Elements index.
func (ms *Store) NodePropertyValueStats(labelToken uint16, propertyKey string) (indexpkg.ValueStats, bool, error) {
	if ms == nil {
		return indexpkg.ValueStats{}, false, ErrNilStore
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if err := ms.checkOpenLocked(); err != nil {
		return indexpkg.ValueStats{}, false, err
	}
	s, exact := ms.propertyIndexes[indexpkg.PropertyIndexKey{LabelToken: labelToken, PropertyKey: propertyKey}].ValueStats()
	return s, exact, nil
}

// RelPropertyValueStats is NodePropertyValueStats for a relationship type.
func (ms *Store) RelPropertyValueStats(relTypeToken uint16, propertyKey string) (indexpkg.ValueStats, bool, error) {
	if ms == nil {
		return indexpkg.ValueStats{}, false, ErrNilStore
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if err := ms.checkOpenLocked(); err != nil {
		return indexpkg.ValueStats{}, false, err
	}
	s, exact := ms.relPropertyIndexes[indexpkg.RelPropertyIndexKey{RelTypeToken: relTypeToken, PropertyKey: propertyKey}].ValueStats()
	return s, exact, nil
}

// ForEachNodePropertyTxMemberAny implements
// store.NodePropertyTxMemberAnyCapability: the sidecar's members of every
// value, read under the read lock once built (the first call builds it).
func (ms *Store) ForEachNodePropertyTxMemberAny(labelToken uint16, propertyKey string, fn func(id types.NodeID, firstTxFrom types.Instant) bool) error {
	if ms == nil {
		return ErrNilStore
	}
	if fn == nil {
		return errNilIterationCallback()
	}
	key := indexpkg.PropertyIndexKey{LabelToken: labelToken, PropertyKey: propertyKey}
	var members []indexpkg.TxMember[types.NodeID]
	err := ms.readOrBuild(func() (bool, error) {
		if err := validateIndexTarget(storecontract.ValidateLabelToken(labelToken), propertyKey); err != nil {
			return true, err
		}
		if idx, ok := ms.propertyIndexes[key]; !ok || idx.Mutated != nil {
			return true, ErrIndexNotFound
		}
		m := ms.nodePropTxMembers[key]
		members = m.MembersAll()
		return m != nil, nil
	}, func() error {
		m := ms.nodePropTxSidecarLocked(key)
		if m == nil {
			return ErrIndexNotFound
		}
		members = m.MembersAll()
		return nil
	})
	if err != nil {
		return err
	}
	for _, mb := range members {
		if !fn(mb.ID, mb.FirstTx) {
			return nil
		}
	}
	return nil
}

// ForEachRelPropertyTxMemberAny implements
// store.RelPropertyTxMemberAnyCapability (the relationship mirror).
func (ms *Store) ForEachRelPropertyTxMemberAny(relTypeToken uint16, propertyKey string, fn func(id types.RelID, firstTxFrom types.Instant) bool) error {
	if ms == nil {
		return ErrNilStore
	}
	if fn == nil {
		return errNilIterationCallback()
	}
	key := indexpkg.RelPropertyIndexKey{RelTypeToken: relTypeToken, PropertyKey: propertyKey}
	var members []indexpkg.TxMember[types.RelID]
	err := ms.readOrBuild(func() (bool, error) {
		if err := validateIndexTarget(storecontract.ValidateRelTypeToken(relTypeToken), propertyKey); err != nil {
			return true, err
		}
		if idx, ok := ms.relPropertyIndexes[key]; !ok || idx.Mutated != nil {
			return true, ErrIndexNotFound
		}
		m := ms.relPropTxMembers[key]
		members = m.MembersAll()
		return m != nil, nil
	}, func() error {
		m, err := ms.relPropTxSidecarLocked(key)
		if err != nil {
			return err
		}
		if m == nil {
			return ErrIndexNotFound
		}
		members = m.MembersAll()
		return nil
	})
	if err != nil {
		return err
	}
	for _, mb := range members {
		if !fn(mb.ID, mb.FirstTx) {
			return nil
		}
	}
	return nil
}
