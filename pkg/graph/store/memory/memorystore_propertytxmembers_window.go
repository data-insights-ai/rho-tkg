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

// Windowed property membership: the sidecar of a declared
// index answers one value's members whose rows may meet a valid-time window
// through the value's window view (indexpkg.PropertyTxMembers.MembersDuring).
// The first windowed lookup of a value builds the sidecar if needed and opens
// the view under ms.mu (write); every row recorded after that keeps it, so
// later lookups read under the shared lock.

// ForEachRelPropertyTxMemberDuring implements
// store.RelPropertyTxMemberWindowCapability.
func (ms *Store) ForEachRelPropertyTxMemberDuring(relTypeToken uint16, propertyKey, valueKey string, validStart, validEnd, txAt types.Instant, fn func(id types.RelID, firstTxFrom types.Instant) bool) error {
	return ms.forEachRelPropertyTxMemberDuring(relTypeToken, propertyKey, valueKey, validStart, validEnd, txAt, fn, nil)
}

func (ms *Store) forEachRelPropertyTxMemberDuring(relTypeToken uint16, propertyKey, valueKey string, validStart, validEnd, txAt types.Instant, fn func(id types.RelID, firstTxFrom types.Instant) bool, tally *storecontract.ReadTally) error {
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
		var err error
		members, ok, err = m.MembersDuringAdmit(valueKey, validStart, validEnd, txAt, propertyWindowAdmit(tally))
		return ok, err
	}, func() error {
		m, err := ms.relPropTxSidecarUnderLocked(key, tally)
		if err != nil {
			return err
		}
		if m == nil {
			return ErrIndexNotFound
		}
		if _, err := m.OpenWindowAdmit(valueKey, tally.Candidates); err != nil {
			return err
		}
		members, _, err = m.MembersDuringAdmit(valueKey, validStart, validEnd, txAt, propertyWindowAdmit(tally))
		return err
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
	return ms.forEachNodePropertyTxMemberDuring(labelToken, propertyKey, valueKey, validStart, validEnd, txAt, fn, nil)
}

func (ms *Store) forEachNodePropertyTxMemberDuring(labelToken uint16, propertyKey, valueKey string, validStart, validEnd, txAt types.Instant, fn func(id types.NodeID, firstTxFrom types.Instant) bool, tally *storecontract.ReadTally) error {
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
		var err error
		members, ok, err = m.MembersDuringAdmit(valueKey, validStart, validEnd, txAt, propertyWindowAdmit(tally))
		return ok, err
	}, func() error {
		m, err := ms.nodePropTxSidecarUnderLocked(key, tally)
		if err != nil {
			return err
		}
		if m == nil {
			return ErrIndexNotFound
		}
		if _, err := m.OpenWindowAdmit(valueKey, tally.Candidates); err != nil {
			return err
		}
		members, _, err = m.MembersDuringAdmit(valueKey, validStart, validEnd, txAt, propertyWindowAdmit(tally))
		return err
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
