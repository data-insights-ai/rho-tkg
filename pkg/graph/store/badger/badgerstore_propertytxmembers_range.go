package badger

import (
	"cmp"
	"fmt"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var (
	_ storecontract.NodePropertyTxMemberRangeCapability = (*Store)(nil)
	_ storecontract.RelPropertyTxMemberRangeCapability  = (*Store)(nil)
)

// ForEachNodePropertyTxMemberInRange implements
// store.NodePropertyTxMemberRangeCapability: a built sidecar with its numeric
// view open is read under idxMu's read lock; otherwise the sidecar is built
// (without blocking writers, as for the equality lookup) and its view opened
// under idxMu.Lock, then the read is retried.
func (bs *Store) ForEachNodePropertyTxMemberInRange(labelToken uint16, propertyKey string, lo, hi float64, fn func(id types.NodeID, firstTxFrom types.Instant) bool) error {
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
	return rangePropTxMembers(bs, lo, hi, fn,
		func() (*propTxSidecar[types.NodeID], bool) {
			return bs.nodePropTx[key], bs.nodePropIndexUsableLocked(key)
		},
		func() error { return bs.buildNodePropTx(key) })
}

// ForEachRelPropertyTxMemberInRange implements
// store.RelPropertyTxMemberRangeCapability (the relationship mirror).
func (bs *Store) ForEachRelPropertyTxMemberInRange(relTypeToken uint16, propertyKey string, lo, hi float64, fn func(id types.RelID, firstTxFrom types.Instant) bool) error {
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
	return rangePropTxMembers(bs, lo, hi, fn,
		func() (*propTxSidecar[types.RelID], bool) {
			return bs.relPropTx[key], bs.relPropIndexUsableLocked(key)
		},
		func() error { return bs.buildRelPropTx(key) })
}

// rangePropTxMembers is the body of both range lookups (pagePropTxMembers'
// shape): sidecar returns the sidecar and whether its index is usable (idxMu
// held), build builds it.
func rangePropTxMembers[ID cmp.Ordered](bs *Store, lo, hi float64, fn func(ID, types.Instant) bool,
	sidecar func() (*propTxSidecar[ID], bool), build func() error) error {
	return pickPropTxMembers(bs, func(m *indexpkg.PropertyTxMembers[ID]) ([]indexpkg.TxMember[ID], bool) {
		return m.MembersInRange(lo, hi)
	}, fn, sidecar, build)
}

// pickPropTxMembers reads the members pick selects from a built sidecar
// (the range lookups', the all-values walk's); pick's open=false asks for
// the numeric view, opened under idxMu.Lock before the read is retried.
func pickPropTxMembers[ID cmp.Ordered](bs *Store, pick func(*indexpkg.PropertyTxMembers[ID]) ([]indexpkg.TxMember[ID], bool),
	fn func(ID, types.Instant) bool, sidecar func() (*propTxSidecar[ID], bool), build func() error) error {
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
		members, open := pick(sc.members)
		bs.idxMu.RUnlock()
		if !open {
			bs.idxMu.Lock()
			if cur, ok := sidecar(); ok && cur == sc {
				sc.members.OrderNumeric()
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
	return fmt.Errorf("graph: property membership range: lookup raced %d invalidations: %w", propTxBuildAttempts, ErrIndexNotFound)
}

var (
	_ storecontract.NodePropertyTxMemberAnyCapability = (*Store)(nil)
	_ storecontract.RelPropertyTxMemberAnyCapability  = (*Store)(nil)
)

// ForEachNodePropertyTxMemberAny implements
// store.NodePropertyTxMemberAnyCapability (round 9): the sidecar's members
// of every value, built as for the equality lookup.
func (bs *Store) ForEachNodePropertyTxMemberAny(labelToken uint16, propertyKey string, fn func(id types.NodeID, firstTxFrom types.Instant) bool) error {
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
	return pickPropTxMembers(bs, membersAll[types.NodeID], fn,
		func() (*propTxSidecar[types.NodeID], bool) {
			return bs.nodePropTx[key], bs.nodePropIndexUsableLocked(key)
		},
		func() error { return bs.buildNodePropTx(key) })
}

// ForEachRelPropertyTxMemberAny implements
// store.RelPropertyTxMemberAnyCapability (the relationship mirror).
func (bs *Store) ForEachRelPropertyTxMemberAny(relTypeToken uint16, propertyKey string, fn func(id types.RelID, firstTxFrom types.Instant) bool) error {
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
	return pickPropTxMembers(bs, membersAll[types.RelID], fn,
		func() (*propTxSidecar[types.RelID], bool) {
			return bs.relPropTx[key], bs.relPropIndexUsableLocked(key)
		},
		func() error { return bs.buildRelPropTx(key) })
}

func membersAll[ID cmp.Ordered](m *indexpkg.PropertyTxMembers[ID]) ([]indexpkg.TxMember[ID], bool) {
	return m.MembersAll(), true
}

// NodePropertyValueStats returns the value statistics of the property index
// on (labelToken, propertyKey) (round 9); exact=false without an index,
// while it is built, for an Elements index or one kept on disk.
func (bs *Store) NodePropertyValueStats(labelToken uint16, propertyKey string) (indexpkg.ValueStats, bool, error) {
	if err := bs.checkOpen(); err != nil {
		return indexpkg.ValueStats{}, false, err
	}
	bs.idxMu.RLock()
	defer bs.idxMu.RUnlock()
	s, exact := bs.propertyIndexes[indexpkg.PropertyIndexKey{LabelToken: labelToken, PropertyKey: propertyKey}].ValueStats()
	return s, exact, nil
}

// RelPropertyValueStats is NodePropertyValueStats for a relationship type's
// property index (kept in RAM in every mode).
func (bs *Store) RelPropertyValueStats(relTypeToken uint16, propertyKey string) (indexpkg.ValueStats, bool, error) {
	if err := bs.checkOpen(); err != nil {
		return indexpkg.ValueStats{}, false, err
	}
	bs.idxMu.RLock()
	defer bs.idxMu.RUnlock()
	s, exact := bs.relPropertyIndexes[indexpkg.RelPropertyIndexKey{RelTypeToken: relTypeToken, PropertyKey: propertyKey}].ValueStats()
	return s, exact, nil
}
