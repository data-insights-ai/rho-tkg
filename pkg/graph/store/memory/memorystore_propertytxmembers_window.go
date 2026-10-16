package memory

import (
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var (
	_ storecontract.RelPropertyTxMemberWindowCapability  = (*Store)(nil)
	_ storecontract.NodePropertyTxMemberWindowCapability = (*Store)(nil)
)

// Windowed property membership (ledger row 62): the sidecar of a declared
// index answers one value's members whose rows may meet a valid-time window
// through the value's window view (indexpkg.PropertyTxMembers.MembersDuring).
// The first windowed lookup of a value builds the sidecar if needed and opens
// the view under ms.mu (write); every row recorded after that keeps it, so
// later lookups read under the shared lock.

// ForEachRelPropertyTxMemberDuring implements
// store.RelPropertyTxMemberWindowCapability.
func (ms *Store) ForEachRelPropertyTxMemberDuring(relTypeToken uint16, propertyKey, valueKey string, validStart, validEnd, txAt types.Instant, fn func(id types.RelID, firstTxFrom types.Instant) bool) error {
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
		members, ok = m.MembersDuring(valueKey, validStart, validEnd, txAt)
		return ok, nil
	}, func() error {
		m, err := ms.relPropTxSidecarLocked(key)
		if err != nil {
			return err
		}
		if m == nil {
			return ErrIndexNotFound
		}
		m.OpenWindow(valueKey)
		members, _ = m.MembersDuring(valueKey, validStart, validEnd, txAt)
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

// ForEachNodePropertyTxMemberDuring implements
// store.NodePropertyTxMemberWindowCapability.
func (ms *Store) ForEachNodePropertyTxMemberDuring(labelToken uint16, propertyKey, valueKey string, validStart, validEnd, txAt types.Instant, fn func(id types.NodeID, firstTxFrom types.Instant) bool) error {
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
		members, ok = m.MembersDuring(valueKey, validStart, validEnd, txAt)
		return ok, nil
	}, func() error {
		m := ms.nodePropTxSidecarLocked(key)
		if m == nil {
			return ErrIndexNotFound
		}
		m.OpenWindow(valueKey)
		members, _ = m.MembersDuring(valueKey, validStart, validEnd, txAt)
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
