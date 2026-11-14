package badger

import (
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var _ storecontract.PropertyTxMemberMeteredCapability = (*Store)(nil)

// Metered membership (R2, gap B): the store.PropertyTxMemberMeteredCapability
// forms of badger. A definition kept on disk reads its member entries into a
// fold before it hands a member on (the exact walk, the window seek), and the
// door that counts the members only sees them after: under a candidate limit
// the walk would fold the whole value first. With t, the fold is admitted as it
// grows (foldAdmit): the walk stops at the member that passes the meter's room
// and the store returns the *ReadBudgetError, before fn. A definition in RAM
// (the sidecar's lists are held already) answers as the unmetered doors, and
// so does a nil t. One call per key, as the core's per-key loop, stopping at
// the first error, ErrIndexNotFound or fn's false; fn may see an entity once
// per key. Shard opens are the tiered store's to charge. Each member is
// counted as a candidate before it is handed on (ReadTally.HandedOn), so a
// direct caller is bounded across keys and, on tiered, shards; a door that
// counts the members itself passes ReadTally.DoorCounts.

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
		var refused error
		one := func(id types.NodeID, tx types.Instant) bool {
			if refused = t.HandedOn(); refused != nil { // a direct caller's member, counted before it is handed on
				stopped = true
			} else if !fn(id, tx) {
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
		} else if err := bs.nodeMembersUnmetered(labelToken, propertyKey, vk, window, one); err != nil {
			return err
		}
		if refused != nil {
			return refused
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
		var refused error
		one := func(id types.RelID, tx types.Instant) bool {
			if refused = t.HandedOn(); refused != nil { // a direct caller's member, counted before it is handed on
				stopped = true
			} else if !fn(id, tx) {
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
		} else if err := bs.relMembersUnmetered(relTypeToken, propertyKey, vk, window, one); err != nil {
			return err
		}
		if refused != nil {
			return refused
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

func (bs *Store) nodeMembersUnmetered(labelToken uint16, propertyKey, valueKey string, window *storecontract.PropertyTxMemberWindow, fn func(types.NodeID, types.Instant) bool) error {
	if window != nil {
		return bs.ForEachNodePropertyTxMemberDuring(labelToken, propertyKey, valueKey, window.ValidStart, window.ValidEnd, window.TxAt, fn)
	}
	return bs.ForEachNodePropertyTxMember(labelToken, propertyKey, valueKey, fn)
}

func (bs *Store) relMembersUnmetered(relTypeToken uint16, propertyKey, valueKey string, window *storecontract.PropertyTxMemberWindow, fn func(types.RelID, types.Instant) bool) error {
	if window != nil {
		return bs.ForEachRelPropertyTxMemberDuring(relTypeToken, propertyKey, valueKey, window.ValidStart, window.ValidEnd, window.TxAt, fn)
	}
	return bs.ForEachRelPropertyTxMember(relTypeToken, propertyKey, valueKey, fn)
}
