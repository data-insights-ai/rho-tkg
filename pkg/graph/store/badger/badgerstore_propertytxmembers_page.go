package badger

import (
	"cmp"
	"fmt"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var (
	_ storecontract.RelPropertyTxMemberPageCapability  = (*Store)(nil)
	_ storecontract.NodePropertyTxMemberPageCapability = (*Store)(nil)
)

// Paged property membership (backlog 69): a built sidecar pages one value's
// members in ID order through its ordered view
// (indexpkg.PropertyTxMembers.MembersAfter) under idxMu.RLock. The first page
// of a value opens the view under idxMu.Lock; every row recorded after that
// (recordRelPropTxLocked / recordNodePropTxLocked, under idxMu.Lock) keeps it.

// ForEachRelPropertyTxMemberAfter implements
// store.RelPropertyTxMemberPageCapability.
func (bs *Store) ForEachRelPropertyTxMemberAfter(relTypeToken uint16, propertyKey, valueKey string, after types.RelID, limit int, fn func(id types.RelID, firstTxFrom types.Instant) bool) error {
	if err := bs.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return errNilIterationCallback()
	}
	if err := storecontract.ValidateRelTypeToken(relTypeToken); err != nil {
		return err
	}
	if err := storecontract.ValidateIndexPropertyKey(propertyKey); err != nil {
		return err
	}
	key := indexpkg.RelPropertyIndexKey{RelTypeToken: relTypeToken, PropertyKey: propertyKey}
	return pagePropTxMembers(bs, valueKey, after, limit, fn,
		func() (*propTxSidecar[types.RelID], bool) {
			return bs.relPropTx[key], bs.relPropIndexUsableLocked(key)
		},
		func() error { return bs.buildRelPropTx(key) })
}

// ForEachNodePropertyTxMemberAfter implements
// store.NodePropertyTxMemberPageCapability.
func (bs *Store) ForEachNodePropertyTxMemberAfter(labelToken uint16, propertyKey, valueKey string, after types.NodeID, limit int, fn func(id types.NodeID, firstTxFrom types.Instant) bool) error {
	if err := bs.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return errNilIterationCallback()
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return err
	}
	if err := storecontract.ValidateIndexPropertyKey(propertyKey); err != nil {
		return err
	}
	key := indexpkg.PropertyIndexKey{LabelToken: labelToken, PropertyKey: propertyKey}
	return pagePropTxMembers(bs, valueKey, after, limit, fn,
		func() (*propTxSidecar[types.NodeID], bool) {
			return bs.nodePropTx[key], bs.nodePropIndexUsableLocked(key)
		},
		func() error { return bs.buildNodePropTx(key) })
}

// pagePropTxMembers is the body of both paged lookups: sidecar returns the
// sidecar and whether its index is usable (idxMu held), build builds it.
func pagePropTxMembers[ID cmp.Ordered](bs *Store, valueKey string, after ID, limit int, fn func(ID, types.Instant) bool,
	sidecar func() (*propTxSidecar[ID], bool), build func() error) error {
	for attempt := 0; attempt < propTxBuildAttempts; attempt++ {
		bs.idxMu.RLock()
		sc, usable := sidecar()
		if !usable {
			bs.idxMu.RUnlock()
			return ErrIndexNotFound
		}
		if sc == nil || !sc.built {
			bs.idxMu.RUnlock()
			if err := build(); err != nil {
				return err
			}
			continue
		}
		members, ordered := sc.members.MembersAfter(valueKey, after, limit)
		bs.idxMu.RUnlock()
		if !ordered {
			bs.idxMu.Lock()
			if cur, ok := sidecar(); ok && cur == sc {
				sc.members.OrderValue(valueKey)
			}
			bs.idxMu.Unlock()
			continue
		}
		// fn runs outside every store lock: it re-enters the store.
		for _, m := range members {
			if !fn(m.ID, m.FirstTx) {
				return nil
			}
		}
		return nil
	}
	return fmt.Errorf("graph: property membership page: lookup raced %d invalidations: %w", propTxBuildAttempts, ErrIndexNotFound)
}
