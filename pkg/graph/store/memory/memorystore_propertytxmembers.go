package memory

import (
	"time"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Property membership sidecars (backlog 8, memory arm):
// store.RelPropertyTxMembershipCapability / NodePropertyTxMembershipCapability.
//
// One indexpkg.PropertyTxMembers per DECLARED property index that a temporal
// lookup has asked for: value key -> every rel (node) whose rows ever carried
// the value under the type (label), with the lowest TxFrom of those rows.
// Append-only sound supersets; the core chain resolver stays the authority.
//
// LAZY: a sidecar exists only after the first ForEach*PropertyTxMember call
// for its (scope, key) built it from the current rows (sealed segment rows
// included) and every history row. After that every row the store keeps is
// recorded where it enters the store: storedRel / historyRel / storedNode /
// historyNode (memorystore_ordinal.go), the one seam every door's current and
// history row passes, so no door can forget it. Everything runs under ms.mu,
// so a build cannot miss a concurrent write.
//
// Removal: none per posting. Clear, an index drop, retention purge and exact
// erasure drop the sidecars (an erased value must not survive in RAM); the
// next lookup rebuilds them. Truncation and compaction keep them (a superset).

// recordRelRowLocked records one relationship row the store keeps (current or
// history) into every lazily built membership sidecar: the K1 rel-type set and
// the property sets. Every row, not only creates: a row's TxFrom lowers the
// member's first-TxFrom bound (an unstamped row makes it 0), so a door that
// rewrites or demotes a row must be seen too. Caller holds ms.mu (write).
func (ms *Store) recordRelRowLocked(r *types.Relationship) {
	ms.recordRelTypeMemberLocked(r)
	ms.recordRelPropTxLocked(r)
}

// recordNodeRowLocked is recordRelRowLocked for node rows (K1 label set and
// node property sets). Caller holds ms.mu (write).
func (ms *Store) recordNodeRowLocked(n *types.Node) {
	ms.recordNodeLabelMembersLocked(n)
	ms.recordNodePropTxLocked(n)
}

// recordRelPropTxLocked records r's indexable values into the built rel
// sidecars of its type. Caller holds ms.mu (write).
func (ms *Store) recordRelPropTxLocked(r *types.Relationship) {
	if len(ms.relPropTxMembers) == 0 || r == nil {
		return
	}
	tok := r.TypeToken().Value()
	id := r.ID()
	tx := relTxFrom(r)
	r.ForEachIndexablePropertyValueKey(func(propertyKey, valueKey string) bool {
		if m := ms.relPropTxMembers[indexpkg.RelPropertyIndexKey{RelTypeToken: tok, PropertyKey: propertyKey}]; m != nil {
			m.Record(valueKey, id, tx)
		}
		return true
	})
}

// recordNodePropTxLocked records n's indexable values into the built node
// sidecars of every label the row carries. Caller holds ms.mu (write).
func (ms *Store) recordNodePropTxLocked(n *types.Node) {
	if len(ms.nodePropTxMembers) == 0 || n == nil {
		return
	}
	id := n.ID()
	tx := nodeTxFrom(n)
	for i := 0; i < n.LabelTokenCount(); i++ {
		label := n.LabelTokenRawAt(i)
		n.ForEachIndexablePropertyValueKey(func(propertyKey, valueKey string) bool {
			if m := ms.nodePropTxMembers[indexpkg.PropertyIndexKey{LabelToken: label, PropertyKey: propertyKey}]; m != nil {
				m.Record(valueKey, id, tx)
			}
			return true
		})
	}
}

// dropPropertyTxMembersLocked discards every property sidecar; the next lookup
// rebuilds from what the store then holds. Caller holds ms.mu (write).
func (ms *Store) dropPropertyTxMembersLocked() {
	ms.relPropTxMembers = nil
	ms.nodePropTxMembers = nil
}

// relPropTxSidecarLocked returns the built sidecar for key, building it first,
// or nil when no rel property index is declared for key (or it is still being
// created). Caller holds ms.mu (write).
func (ms *Store) relPropTxSidecarLocked(key indexpkg.RelPropertyIndexKey) (*indexpkg.PropertyTxMembers[types.RelID], error) {
	if idx, ok := ms.relPropertyIndexes[key]; !ok || idx.Mutated != nil {
		return nil, nil
	}
	if m := ms.relPropTxMembers[key]; m != nil {
		return m, nil
	}
	start := time.Now()
	m := indexpkg.NewPropertyTxMembers[types.RelID]()
	record := func(r *types.Relationship) {
		if !r.HasTypeTokenRaw(key.RelTypeToken) {
			return
		}
		if vk, found := r.IndexablePropertyValueKey(key.PropertyKey); found {
			m.Record(vk, r.ID(), relTxFrom(r))
		}
	}
	// ADR-0011: every current row, sealed ones included. A decode error
	// leaves the sidecar unbuilt and fails the read: it must be a superset.
	if err := ms.forEachCurrentRelLocked(func(r *types.Relationship) bool {
		record(r)
		return true
	}); err != nil {
		return nil, err
	}
	for _, versions := range ms.relHistory {
		for _, r := range versions {
			record(r)
		}
	}
	if ms.relPropTxMembers == nil {
		ms.relPropTxMembers = make(map[indexpkg.RelPropertyIndexKey]*indexpkg.PropertyTxMembers[types.RelID])
	}
	ms.relPropTxMembers[key] = m
	ms.propTxBuilds++
	ms.propTxBuildTime += time.Since(start)
	return m, nil
}

// nodePropTxSidecarLocked is relPropTxSidecarLocked for a node index.
func (ms *Store) nodePropTxSidecarLocked(key indexpkg.PropertyIndexKey) *indexpkg.PropertyTxMembers[types.NodeID] {
	if idx, ok := ms.propertyIndexes[key]; !ok || idx.Mutated != nil {
		return nil
	}
	if m := ms.nodePropTxMembers[key]; m != nil {
		return m
	}
	start := time.Now()
	m := indexpkg.NewPropertyTxMembers[types.NodeID]()
	record := func(n *types.Node) {
		if !n.HasLabelTokenRaw(key.LabelToken) {
			return
		}
		if vk, found := n.IndexablePropertyValueKey(key.PropertyKey); found {
			m.Record(vk, n.ID(), nodeTxFrom(n))
		}
	}
	for _, n := range ms.nodes {
		record(n)
	}
	for _, versions := range ms.nodeHistory {
		for _, n := range versions {
			record(n)
		}
	}
	if ms.nodePropTxMembers == nil {
		ms.nodePropTxMembers = make(map[indexpkg.PropertyIndexKey]*indexpkg.PropertyTxMembers[types.NodeID])
	}
	ms.nodePropTxMembers[key] = m
	ms.propTxBuilds++
	ms.propTxBuildTime += time.Since(start)
	return m
}

// readOrBuild runs read under ms.mu.RLock; when it reports the sidecar
// is not built yet, it runs build under ms.mu.Lock instead (build re-checks
// everything: another caller may have built or dropped it meanwhile). A built
// sidecar is a read, so lookups do not serialize on the write lock.
func (ms *Store) readOrBuild(read func() (done bool, err error), build func() error) error {
	ms.mu.RLock()
	if err := ms.checkOpenLocked(); err != nil {
		ms.mu.RUnlock()
		return err
	}
	done, err := read()
	ms.mu.RUnlock()
	if done || err != nil {
		return err
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if err := ms.checkOpenLocked(); err != nil {
		return err
	}
	return build()
}

// ForEachRelPropertyTxMember implements store.RelPropertyTxMembershipCapability.
func (ms *Store) ForEachRelPropertyTxMember(relTypeToken uint16, propertyKey, valueKey string, fn func(id types.RelID, firstTxFrom types.Instant) bool) error {
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
		members = m.Members(valueKey)
		return true, nil
	}, func() error {
		m, err := ms.relPropTxSidecarLocked(key)
		if err != nil {
			return err
		}
		if m == nil {
			return ErrIndexNotFound
		}
		members = m.Members(valueKey)
		return nil
	})
	if err != nil {
		return err
	}
	// fn runs outside the lock (it re-enters the store).
	for _, mb := range members {
		if !fn(mb.ID, mb.FirstTx) {
			return nil
		}
	}
	return nil
}

// ForEachNodePropertyTxMember implements store.NodePropertyTxMembershipCapability.
func (ms *Store) ForEachNodePropertyTxMember(labelToken uint16, propertyKey, valueKey string, fn func(id types.NodeID, firstTxFrom types.Instant) bool) error {
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
		members = m.Members(valueKey)
		return true, nil
	}, func() error {
		m := ms.nodePropTxSidecarLocked(key)
		if m == nil {
			return ErrIndexNotFound
		}
		members = m.Members(valueKey)
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

// PropertyTxMembershipStats implements store.PropertyTxMembershipStatsCapability.
func (ms *Store) PropertyTxMembershipStats() (storecontract.PropertyTxMembershipStats, error) {
	if ms == nil {
		return storecontract.PropertyTxMembershipStats{}, ErrNilStore
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if err := ms.checkOpenLocked(); err != nil {
		return storecontract.PropertyTxMembershipStats{}, err
	}
	st := storecontract.PropertyTxMembershipStats{
		RelSidecars:   len(ms.relPropTxMembers),
		NodeSidecars:  len(ms.nodePropTxMembers),
		Builds:        ms.propTxBuilds,
		BuildDuration: ms.propTxBuildTime,
	}
	for _, m := range ms.relPropTxMembers {
		st.RelPostings += m.Postings()
	}
	for _, m := range ms.nodePropTxMembers {
		st.NodePostings += m.Postings()
	}
	return st, nil
}
