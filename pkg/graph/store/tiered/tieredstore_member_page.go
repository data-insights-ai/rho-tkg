package tiered

import (
	"slices"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var (
	_ storecontract.RelPropertyTxMemberPageCapability  = (*Store)(nil)
	_ storecontract.NodePropertyTxMemberPageCapability = (*Store)(nil)
	_ storecontract.MemberIDsCapability                = (*Store)(nil)
)

// Paged property membership (backlog 69, ledger row 106) and member IDs over
// the shards the other membership doors read (allShards: the reference shard,
// the archive and every event shard, cold ones opened): each shard pages its
// own sidecar above the cursor, so the store's first limit members are among
// the union of the shards' first limit, merged in ascending ID order, an
// entity on several shards once with the lowest first tx time. The anchor
// rule is unionPropertyTxMembers': store.ErrIndexNotFound before fn unless
// every shard carries the index.

// ForEachRelPropertyTxMemberAfter implements
// store.RelPropertyTxMemberPageCapability.
func (ts *Store) ForEachRelPropertyTxMemberAfter(relTypeToken uint16, propertyKey, valueKey string, after types.RelID, limit int, fn func(id types.RelID, firstTxFrom types.Instant) bool) error {
	return pagePropertyTxMembers(ts, func(bs *BadgerStore) (storecontract.PropertyIndexOptions, bool, error) {
		return bs.RelPropertyIndexOptions(relTypeToken, propertyKey)
	}, func(bs *BadgerStore, f func(types.RelID, types.Instant) bool) error {
		return bs.ForEachRelPropertyTxMemberAfter(relTypeToken, propertyKey, valueKey, after, limit, f)
	}, limit, fn)
}

// ForEachNodePropertyTxMemberAfter implements
// store.NodePropertyTxMemberPageCapability.
func (ts *Store) ForEachNodePropertyTxMemberAfter(labelToken uint16, propertyKey, valueKey string, after types.NodeID, limit int, fn func(id types.NodeID, firstTxFrom types.Instant) bool) error {
	return pagePropertyTxMembers(ts, func(bs *BadgerStore) (storecontract.PropertyIndexOptions, bool, error) {
		return bs.PropertyIndexOptions(labelToken, propertyKey)
	}, func(bs *BadgerStore, f func(types.NodeID, types.Instant) bool) error {
		return bs.ForEachNodePropertyTxMemberAfter(labelToken, propertyKey, valueKey, after, limit, f)
	}, limit, fn)
}

// pagePropertyTxMembers is the body of both paged doors: every shard's page
// (ascending, at most limit), k-way merged.
func pagePropertyTxMembers[ID ~int64](ts *Store, options func(*BadgerStore) (storecontract.PropertyIndexOptions, bool, error), page func(*BadgerStore, func(ID, types.Instant) bool) error, limit int, fn func(ID, types.Instant) bool) error {
	if fn == nil {
		if err := ts.checkOpen(); err != nil {
			return err
		}
		return errNilIterationCallback()
	}
	ref, others, err := propertyTxMemberAnswers(ts, options, page)
	if err != nil {
		return err
	}
	mergeMemberPages(append(others, ref), limit, fn)
	return nil
}

// mergeMemberPages hands fn the limit smallest IDs of the ascending lists,
// ascending, each once with the merged first tx time (indexpkg.MergeFirstTx).
func mergeMemberPages[ID ~int64](lists [][]txMember[ID], limit int, fn func(ID, types.Instant) bool) {
	pos := make([]int, len(lists))
	for emitted := 0; emitted < limit; emitted++ {
		best := -1
		for i, l := range lists {
			if pos[i] < len(l) && (best < 0 || l[pos[i]].id < lists[best][pos[best]].id) {
				best = i
			}
		}
		if best < 0 {
			return
		}
		m := lists[best][pos[best]]
		pos[best]++
		for i, l := range lists {
			if pos[i] < len(l) && l[pos[i]].id == m.id {
				m.tx = indexpkg.MergeFirstTx(m.tx, l[pos[i]].tx)
				pos[i]++
			}
		}
		if !fn(m.id, m.tx) {
			return
		}
	}
}

// NodeIDsByLabel implements store.MemberIDsCapability: the label's current
// members on every shard (NodesByLabel at DepthAll), ascending, each once,
// without reading a row.
func (ts *Store) NodeIDsByLabel(labelToken uint16) ([]types.NodeID, error) {
	ts.noteTypeScan("NodeIDsByLabel")
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return nil, err
	}
	ref, others, err := allShards(ts, func(bs *BadgerStore, _ bool) ([]types.NodeID, error) {
		return bs.NodeIDsByLabel(labelToken)
	})
	if err != nil {
		return nil, err
	}
	return slices.Compact(mergeNodeIDSlices(append(others, ref))), nil
}

// RelIDsByType implements store.MemberIDsCapability: the type's current
// members on every shard (RelationshipsByType at DepthAll), ascending, each
// once, without reading a row.
func (ts *Store) RelIDsByType(typeToken uint16) ([]types.RelID, error) {
	ts.noteTypeScan("RelIDsByType")
	if err := storecontract.ValidateRelTypeToken(typeToken); err != nil {
		return nil, err
	}
	ref, others, err := allShards(ts, func(bs *BadgerStore, _ bool) ([]types.RelID, error) {
		return bs.RelIDsByType(typeToken)
	})
	if err != nil {
		return nil, err
	}
	return slices.Compact(mergeRelIDSlices(append(others, ref))), nil
}
