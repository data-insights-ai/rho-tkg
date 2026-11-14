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

// Windowed property membership: a built sidecar answers one
// value's members whose rows may meet a valid-time window through the value's
// window view (indexpkg.PropertyTxMembers.MembersDuring) under idxMu.RLock.
// The first windowed lookup of a value opens the view under idxMu.Lock; every
// row recorded after that (recordRelPropTxLocked / recordNodePropTxLocked,
// under idxMu.Lock) keeps it. A definition kept on disk answers the same from
// its span entries (RELDISKWIN, badgerstore_member_span.go): it reads the
// value's entries, folds each member's envelope and hands on only the members
// that meet the window; one a read-only open found built before that layout
// hands on every member of the value.

// ForEachRelPropertyTxMemberDuring implements
// store.RelPropertyTxMemberWindowCapability.
func (bs *Store) ForEachRelPropertyTxMemberDuring(relTypeToken uint16, propertyKey, valueKey string, validStart, validEnd, txAt types.Instant, fn func(id types.RelID, firstTxFrom types.Instant) bool) error {
	return bs.forEachRelPropertyTxMemberDuring(relTypeToken, propertyKey, valueKey, validStart, validEnd, txAt, fn, nil, nil)
}

// forEachRelPropertyTxMemberDuring is the windowed lookup under work (nil: no
// diagnostic) and a read budget's tally t (nil: none): a definition in RAM
// charges the entries its window view judges and the members it adds, and the
// one-time open of the view, to t before the work (windowPropTxMembers).
func (bs *Store) forEachRelPropertyTxMemberDuring(relTypeToken uint16, propertyKey, valueKey string, validStart, validEnd, txAt types.Instant, fn func(id types.RelID, firstTxFrom types.Instant) bool, work *storecontract.PropertyMemberWork, t *storecontract.ReadTally) error {
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
	if members, ok, err := bs.relDiskMembers(key, memberQuery{valueKeys: []string{valueKey}, work: work, window: &memberWindow{validStart, validEnd, txAt}}); ok {
		if validStart >= validEnd {
			members = nil // an empty window answers nothing
		}
		return emitTxMembers(members, err, memberWorkCallback(fn, work)) // RELDISKWIN: the members whose envelope meets the window (every member of the value, a sound superset, from a definition built before it)
	}
	return windowPropTxMembers(bs, valueKey, validStart, validEnd, txAt, memberWorkCallback(fn, work), t,
		func() (*propTxSidecar[types.RelID], bool) {
			return bs.relPropTx[key], bs.relPropIndexUsableLocked(key)
		},
		func() error { return bs.buildRelPropTx(key) })
}

// ForEachNodePropertyTxMemberDuring implements
// store.NodePropertyTxMemberWindowCapability.
func (bs *Store) ForEachNodePropertyTxMemberDuring(labelToken uint16, propertyKey, valueKey string, validStart, validEnd, txAt types.Instant, fn func(id types.NodeID, firstTxFrom types.Instant) bool) error {
	return bs.forEachNodePropertyTxMemberDuring(labelToken, propertyKey, valueKey, validStart, validEnd, txAt, fn, nil, nil)
}

// forEachNodePropertyTxMemberDuring is forEachRelPropertyTxMemberDuring for a
// node definition.
func (bs *Store) forEachNodePropertyTxMemberDuring(labelToken uint16, propertyKey, valueKey string, validStart, validEnd, txAt types.Instant, fn func(id types.NodeID, firstTxFrom types.Instant) bool, work *storecontract.PropertyMemberWork, t *storecontract.ReadTally) error {
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
	if members, ok, err := bs.nodeDiskMembers(key, memberQuery{valueKeys: []string{valueKey}, work: work, window: &memberWindow{validStart, validEnd, txAt}}); ok {
		if validStart >= validEnd {
			members = nil // an empty window answers nothing
		}
		return emitTxMembers(members, err, memberWorkCallback(fn, work)) // RELDISKWIN: the members whose envelope meets the window (every member of the value, a sound superset, from a definition built before it)
	}
	return windowPropTxMembers(bs, valueKey, validStart, validEnd, txAt, memberWorkCallback(fn, work), t,
		func() (*propTxSidecar[types.NodeID], bool) {
			return bs.nodePropTx[key], bs.nodePropIndexUsableLocked(key)
		},
		func() error { return bs.buildNodePropTx(key) })
}

// windowPropTxMembers is the body of both windowed lookups (the
// pagePropTxMembers protocol): sidecar returns the sidecar and whether its
// index is usable (idxMu held), build builds it. Under a read budget's tally t
// (nil: none) the first lookup of a value, which opens its window view (one
// pass over every member, O(n log n)), is charged the view's bytes and refused
// before the open when the value's members pass the room, and the view's walk charges each entry
// it judges and rejects and each member before it is added (indexpkg.WindowAdmit),
// so the room bounds the walk, not only the members handed on; the sidecar's
// build (one scan of the label's or type's rows) is not charged.
func windowPropTxMembers[ID cmp.Ordered](bs *Store, valueKey string, validStart, validEnd, txAt types.Instant, fn func(ID, types.Instant) bool, t *storecontract.ReadTally,
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
		members, opened, err := sc.members.MembersDuringAdmit(valueKey, validStart, validEnd, txAt, windowAdmitOf(t))
		viewMembers := 0
		if err == nil && !opened {
			viewMembers = sc.members.Count(valueKey) // read under the lock: a write updates the value map
		}
		bs.idxMu.RUnlock()
		if err != nil {
			return err
		}
		if !opened {
			if err := openWindowViewAdmit(t, viewMembers); err != nil {
				return err
			}
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

// openWindowViewAdmit is a read budget's charge for opening a value's window
// view, one pass over its n members: refused before the open when n passes the
// room the meter has left (the members counted as the work held ahead). The
// view is a persistent structure, kept and maintained by the sidecar from then
// on, not a buffer of this read, so it is not charged as BufferBytes, and the
// members are counted when the walk examines them, not here: the first budgeted
// reader of a value pays its build (the refusal when the value passes the
// room), and later readers do not.
func openWindowViewAdmit(t *storecontract.ReadTally, n int) error {
	return t.CandidatesAhead(n)
}

// windowAdmitOf is the admission of a RAM window view's walk under t (nil: none
// for a nil tally): each entry the walk examines is counted as a candidate
// before it is looked at, each member added is charged txMemberBytes of
// BufferBytes and each entry of the map of decided members foldEntryBytes,
// before they grow.
func windowAdmitOf(t *storecontract.ReadTally) *indexpkg.WindowAdmit {
	if t == nil {
		return nil
	}
	return &indexpkg.WindowAdmit{
		Entry: func() error { return t.Candidates(1) },
		Add:   func() error { return t.Buffer(txMemberBytes) },
		Later: func() error { return t.Buffer(foldEntryBytes) },
	}
}
