package badger

import (
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var _ storecontract.PropertyTxMemberMeteredCapability = (*Store)(nil)

// Metered membership (R2, gap B): the store.PropertyTxMemberMeteredCapability
// forms of badger. The store is the one owner of the cost of the read: it
// counts what it examines and what it collects, before it goes on, and nothing
// above it counts those again (the graph counts only the members of a store
// without this form). So the Candidates limit holds across the value keys of a
// call, and on tiered across the shards, whichever layer hands the members on.
//
//   - A definition kept on disk reads its member entries into a fold before it
//     hands a member on (the exact walk, the window seek). Each member is
//     counted as a candidate when the walk collects it (the first entry of an
//     entity), each entry the window walk judges and rejects, or steps over as
//     stale (an older version, a tombstone, an entry the write buffer
//     overrides), when it is met (walkBudget), so the walk stops at the entry
//     that passes the room the meter has left and returns the typed
//     *ReadBudgetError before fn. The write buffer's values it clones and the
//     list the fold becomes are charged as BufferBytes before they are made.
//   - A definition in RAM charges the same before its work: the snapshot of the
//     value's members (counted when it is made), the one-time open of the
//     value's window view (refused up front when the value's members pass the
//     room) and the view's walk (the entries it judges and rejects, each
//     member before it is added). The sidecar's first build, one scan of the
//     label's or type's rows, is not charged.
//
// A nil t is the unmetered door. One call per key, as the core's per-key loop,
// stopping at the first error, ErrIndexNotFound or fn's false; fn may see an
// entity once per key. Shard opens are the tiered store's to charge.

// ForEachNodePropertyTxMemberMetered implements
// store.PropertyTxMemberMeteredCapability: ForEachNodePropertyTxMember (window
// nil) or ForEachNodePropertyTxMemberDuring over every key of valueKeys.
func (bs *Store) ForEachNodePropertyTxMemberMetered(labelToken uint16, propertyKey string, valueKeys []string, window *storecontract.PropertyTxMemberWindow, t *storecontract.ReadTally, fn func(id types.NodeID, firstTxFrom types.Instant) bool) error {
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
	for _, vk := range valueKeys {
		stopped := false
		one := func(id types.NodeID, tx types.Instant) bool { // counted where it was collected
			if !fn(id, tx) {
				stopped = true
			}
			return !stopped
		}
		if members, ok, err := bs.nodeDiskMembers(key, memberQueryOf(vk, window, t)); ok {
			if window != nil && window.ValidStart >= window.ValidEnd {
				members = nil // an empty window answers nothing
			}
			if err := emitTxMembers(members, err, one); err != nil {
				return err
			}
		} else if err := bs.nodeMembersRAM(labelToken, propertyKey, vk, window, t, one); err != nil {
			return err
		}
		if stopped {
			return nil
		}
	}
	return nil
}

// ForEachRelPropertyTxMemberMetered is ForEachNodePropertyTxMemberMetered for
// a relationship (type, key) definition.
func (bs *Store) ForEachRelPropertyTxMemberMetered(relTypeToken uint16, propertyKey string, valueKeys []string, window *storecontract.PropertyTxMemberWindow, t *storecontract.ReadTally, fn func(id types.RelID, firstTxFrom types.Instant) bool) error {
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
	for _, vk := range valueKeys {
		stopped := false
		one := func(id types.RelID, tx types.Instant) bool { // counted where it was collected
			if !fn(id, tx) {
				stopped = true
			}
			return !stopped
		}
		if members, ok, err := bs.relDiskMembers(key, memberQueryOf(vk, window, t)); ok {
			if window != nil && window.ValidStart >= window.ValidEnd {
				members = nil
			}
			if err := emitTxMembers(members, err, one); err != nil {
				return err
			}
		} else if err := bs.relMembersRAM(relTypeToken, propertyKey, vk, window, t, one); err != nil {
			return err
		}
		if stopped {
			return nil
		}
	}
	return nil
}

// memberQueryOf is the membership lookup of one value key, windowed when
// window is set, admitted under t.
func memberQueryOf(valueKey string, window *storecontract.PropertyTxMemberWindow, t *storecontract.ReadTally) memberQuery {
	q := memberQuery{valueKeys: []string{valueKey}, tally: t}
	if window != nil {
		q.window = &memberWindow{window.ValidStart, window.ValidEnd, window.TxAt}
	}
	return q
}

// nodeMembersRAM answers a lookup of a definition kept in RAM (its membership
// sidecar) under t: the sidecar's snapshot of the value, or its window view's
// walk, is charged before it is made (admitSidecarMembers, windowPropTxMembers).
func (bs *Store) nodeMembersRAM(labelToken uint16, propertyKey, valueKey string, window *storecontract.PropertyTxMemberWindow, t *storecontract.ReadTally, fn func(types.NodeID, types.Instant) bool) error {
	if window != nil {
		return bs.forEachNodePropertyTxMemberDuring(labelToken, propertyKey, valueKey, window.ValidStart, window.ValidEnd, window.TxAt, fn, nil, t)
	}
	return bs.forEachNodePropertyTxMember(labelToken, propertyKey, valueKey, fn, t)
}

// relMembersRAM is nodeMembersRAM for a relationship definition.
func (bs *Store) relMembersRAM(relTypeToken uint16, propertyKey, valueKey string, window *storecontract.PropertyTxMemberWindow, t *storecontract.ReadTally, fn func(types.RelID, types.Instant) bool) error {
	if window != nil {
		return bs.forEachRelPropertyTxMemberDuring(relTypeToken, propertyKey, valueKey, window.ValidStart, window.ValidEnd, window.TxAt, fn, nil, t)
	}
	return bs.forEachRelPropertyTxMember(relTypeToken, propertyKey, valueKey, fn, t)
}
