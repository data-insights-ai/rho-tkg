package memory

import (
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var _ storecontract.PropertyTxMemberMeteredCapability = (*Store)(nil)

// ForEachNodePropertyTxMemberMetered shares the read's allowance across all
// value keys. The native helpers charge cold builds, window creation and the
// member snapshot before their work; callbacks run outside the store lock.
func (ms *Store) ForEachNodePropertyTxMemberMetered(label uint16, key string, values []string, window *storecontract.PropertyTxMemberWindow, tally *storecontract.ReadTally, fn func(types.NodeID, types.Instant) bool) error {
	if ms == nil {
		return ErrNilStore
	}
	if fn == nil {
		return errNilIterationCallback()
	}
	if err := storecontract.ValidateLabelToken(label); err != nil {
		return err
	}
	if err := storecontract.ValidateIndexPropertyKey(key); err != nil {
		return err
	}
	if len(values) == 0 {
		ms.mu.RLock()
		defer ms.mu.RUnlock()
		return ms.checkOpenLocked()
	}
	for _, value := range values {
		stopped := false
		one := func(id types.NodeID, tx types.Instant) bool {
			stopped = !fn(id, tx)
			return !stopped
		}
		var err error
		if window == nil {
			err = ms.forEachNodePropertyTxMember(label, key, value, one, tally)
		} else {
			err = ms.forEachNodePropertyTxMemberDuring(label, key, value, window.ValidStart, window.ValidEnd, window.TxAt, one, tally)
		}
		if err != nil || stopped {
			return err
		}
	}
	return nil
}

// ForEachRelPropertyTxMemberMetered is the relationship counterpart.
func (ms *Store) ForEachRelPropertyTxMemberMetered(relType uint16, key string, values []string, window *storecontract.PropertyTxMemberWindow, tally *storecontract.ReadTally, fn func(types.RelID, types.Instant) bool) error {
	if ms == nil {
		return ErrNilStore
	}
	if fn == nil {
		return errNilIterationCallback()
	}
	if err := storecontract.ValidateRelTypeToken(relType); err != nil {
		return err
	}
	if err := storecontract.ValidateIndexPropertyKey(key); err != nil {
		return err
	}
	if len(values) == 0 {
		ms.mu.RLock()
		defer ms.mu.RUnlock()
		return ms.checkOpenLocked()
	}
	for _, value := range values {
		stopped := false
		one := func(id types.RelID, tx types.Instant) bool {
			stopped = !fn(id, tx)
			return !stopped
		}
		var err error
		if window == nil {
			err = ms.forEachRelPropertyTxMember(relType, key, value, one, tally)
		} else {
			err = ms.forEachRelPropertyTxMemberDuring(relType, key, value, window.ValidStart, window.ValidEnd, window.TxAt, one, tally)
		}
		if err != nil || stopped {
			return err
		}
	}
	return nil
}

func propertyKeyAdmit(t *storecontract.ReadTally) func(int) error {
	if t == nil {
		return nil
	}
	return func(n int) error {
		if err := t.Candidates(1); err != nil {
			return err
		}
		return t.Buffer(n)
	}
}

func propertyMemberAdmit(t *storecontract.ReadTally) func() error {
	if t == nil {
		return nil
	}
	return func() error {
		if err := t.Candidates(1); err != nil {
			return err
		}
		return t.Buffer(16) // ID and first transaction stamp
	}
}

func propertyWindowAdmit(t *storecontract.ReadTally) *indexpkg.WindowAdmit {
	if t == nil {
		return nil
	}
	return &indexpkg.WindowAdmit{
		Entry: func() error { return t.Candidates(1) },
		Add:   func() error { return t.Buffer(16) },
		Later: func() error { return t.Buffer(24) },
	}
}
