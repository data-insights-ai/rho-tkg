package memory

import (
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var (
	_ storecontract.RelPropertyTxMemberPageCapability  = (*Store)(nil)
	_ storecontract.NodePropertyTxMemberPageCapability = (*Store)(nil)
)

// Paged property membership (backlog 69): the sidecar of a declared index
// pages one value's members in ID order through its ordered view
// (indexpkg.PropertyTxMembers.MembersAfter). The first page of a value builds
// the sidecar if needed and opens the view under ms.mu (write); every row
// recorded after that keeps it, so later pages read under the shared lock.

// ForEachRelPropertyTxMemberAfter implements
// store.RelPropertyTxMemberPageCapability.
func (ms *Store) ForEachRelPropertyTxMemberAfter(relTypeToken uint16, propertyKey, valueKey string, after types.RelID, limit int, fn func(id types.RelID, firstTxFrom types.Instant) bool) error {
	if ms == nil {
		return ErrNilStore
	}
	if fn == nil {
		return errNilIterationCallback()
	}
	key := indexpkg.RelPropertyIndexKey{RelTypeToken: relTypeToken, PropertyKey: propertyKey}
	var members []indexpkg.TxMember[types.RelID]
	err := ms.readOrBuild(func() (bool, error) {
		if err := storecontract.ValidateRelTypeToken(relTypeToken); err != nil {
			return true, err
		}
		if err := storecontract.ValidateIndexPropertyKey(propertyKey); err != nil {
			return true, err
		}
		if idx, ok := ms.relPropertyIndexes[key]; !ok || idx.Mutated != nil {
			return true, ErrIndexNotFound
		}
		m := ms.relPropTxMembers[key]
		if m == nil {
			return false, nil
		}
		var ok bool
		members, ok = m.MembersAfter(valueKey, after, limit)
		return ok, nil
	}, func() error {
		m, err := ms.relPropTxSidecarLocked(key)
		if err != nil {
			return err
		}
		if m == nil {
			return ErrIndexNotFound
		}
		m.OrderValue(valueKey)
		members, _ = m.MembersAfter(valueKey, after, limit)
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

// ForEachNodePropertyTxMemberAfter implements
// store.NodePropertyTxMemberPageCapability.
func (ms *Store) ForEachNodePropertyTxMemberAfter(labelToken uint16, propertyKey, valueKey string, after types.NodeID, limit int, fn func(id types.NodeID, firstTxFrom types.Instant) bool) error {
	if ms == nil {
		return ErrNilStore
	}
	if fn == nil {
		return errNilIterationCallback()
	}
	key := indexpkg.PropertyIndexKey{LabelToken: labelToken, PropertyKey: propertyKey}
	var members []indexpkg.TxMember[types.NodeID]
	err := ms.readOrBuild(func() (bool, error) {
		if err := storecontract.ValidateLabelToken(labelToken); err != nil {
			return true, err
		}
		if err := storecontract.ValidateIndexPropertyKey(propertyKey); err != nil {
			return true, err
		}
		if idx, ok := ms.propertyIndexes[key]; !ok || idx.Mutated != nil {
			return true, ErrIndexNotFound
		}
		m := ms.nodePropTxMembers[key]
		if m == nil {
			return false, nil
		}
		var ok bool
		members, ok = m.MembersAfter(valueKey, after, limit)
		return ok, nil
	}, func() error {
		m := ms.nodePropTxSidecarLocked(key)
		if m == nil {
			return ErrIndexNotFound
		}
		m.OrderValue(valueKey)
		members, _ = m.MembersAfter(valueKey, after, limit)
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
