package badger

import (
	"cmp"
	"fmt"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var (
	_ storecontract.RelPropertyTxMemberWindowCapability  = (*Store)(nil)
	_ storecontract.NodePropertyTxMemberWindowCapability = (*Store)(nil)
)

// Windowed property membership (ledger row 62): a built sidecar answers one
// value's members whose rows may meet a valid-time window through the value's
// window view (indexpkg.PropertyTxMembers.MembersDuring) under idxMu.RLock.
// The first windowed lookup of a value opens the view under idxMu.Lock; every
// row recorded after that (recordRelPropTxLocked / recordNodePropTxLocked,
// under idxMu.Lock) keeps it.

// ForEachRelPropertyTxMemberDuring implements
// store.RelPropertyTxMemberWindowCapability.
func (bs *Store) ForEachRelPropertyTxMemberDuring(relTypeToken uint16, propertyKey, valueKey string, validStart, validEnd, txAt types.Instant, fn func(id types.RelID, firstTxFrom types.Instant) bool) error {
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
	if members, ok, err := bs.relDiskMembers(key, memberQuery{valueKeys: []string{valueKey}}); ok {
		if validStart >= validEnd {
			members = nil // an empty window answers nothing
		}
		return emitTxMembers(members, err, fn) // RELDISK: every member of the value, a sound superset
	}
	return windowPropTxMembers(bs, valueKey, validStart, validEnd, txAt, fn,
		func() (*propTxSidecar[types.RelID], bool) {
			return bs.relPropTx[key], bs.relPropIndexUsableLocked(key)
		},
		func() error { return bs.buildRelPropTx(key) })
}

// ForEachNodePropertyTxMemberDuring implements
// store.NodePropertyTxMemberWindowCapability.
func (bs *Store) ForEachNodePropertyTxMemberDuring(labelToken uint16, propertyKey, valueKey string, validStart, validEnd, txAt types.Instant, fn func(id types.NodeID, firstTxFrom types.Instant) bool) error {
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
	if members, ok, err := bs.nodeDiskMembers(key, memberQuery{valueKeys: []string{valueKey}}); ok {
		if validStart >= validEnd {
			members = nil // an empty window answers nothing
		}
		return emitTxMembers(members, err, fn) // RELDISK: every member of the value, a sound superset
	}
	return windowPropTxMembers(bs, valueKey, validStart, validEnd, txAt, fn,
		func() (*propTxSidecar[types.NodeID], bool) {
			return bs.nodePropTx[key], bs.nodePropIndexUsableLocked(key)
		},
		func() error { return bs.buildNodePropTx(key) })
}

// windowPropTxMembers is the body of both windowed lookups (the
// pagePropTxMembers protocol): sidecar returns the sidecar and whether its
// index is usable (idxMu held), build builds it.
func windowPropTxMembers[ID cmp.Ordered](bs *Store, valueKey string, validStart, validEnd, txAt types.Instant, fn func(ID, types.Instant) bool,
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
		members, opened := sc.members.MembersDuring(valueKey, validStart, validEnd, txAt)
		bs.idxMu.RUnlock()
		if !opened {
			bs.idxMu.Lock()
			if cur, ok := sidecar(); ok && cur == sc {
				sc.members.OpenWindow(valueKey)
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
	return fmt.Errorf("graph: windowed property membership: lookup raced %d invalidations: %w", propTxBuildAttempts, ErrIndexNotFound)
}
