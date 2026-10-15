package memory

import (
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// ForEachNodePropertyTxMemberInRange implements
// store.NodePropertyTxMemberRangeCapability: built lookups read under the
// read lock; the first call per sidecar builds it (when needed) and opens its
// numeric view under the write lock.
func (ms *Store) ForEachNodePropertyTxMemberInRange(labelToken uint16, propertyKey string, lo, hi float64, fn func(id types.NodeID, firstTxFrom types.Instant) bool) error {
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
		var ok bool
		members, ok = ms.nodePropTxMembers[key].MembersInRange(lo, hi)
		return ok && ms.nodePropTxMembers[key] != nil, nil
	}, func() error {
		m := ms.nodePropTxSidecarLocked(key)
		if m == nil {
			return ErrIndexNotFound
		}
		m.OrderNumeric()
		members, _ = m.MembersInRange(lo, hi)
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

// ForEachRelPropertyTxMemberInRange implements
// store.RelPropertyTxMemberRangeCapability (the relationship mirror).
func (ms *Store) ForEachRelPropertyTxMemberInRange(relTypeToken uint16, propertyKey string, lo, hi float64, fn func(id types.RelID, firstTxFrom types.Instant) bool) error {
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
		var ok bool
		members, ok = ms.relPropTxMembers[key].MembersInRange(lo, hi)
		return ok && ms.relPropTxMembers[key] != nil, nil
	}, func() error {
		m, err := ms.relPropTxSidecarLocked(key)
		if err != nil {
			return err
		}
		if m == nil {
			return ErrIndexNotFound
		}
		m.OrderNumeric()
		members, _ = m.MembersInRange(lo, hi)
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
