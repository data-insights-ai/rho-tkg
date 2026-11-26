package memory

import (
	"time"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Property membership sidecars (memory arm): one
// indexpkg.PropertyTxMembers per declared property index a temporal lookup asked
// for (value key -> rels/nodes whose rows ever carried it, with the lowest
// TxFrom); append-only supersets, the chain resolver stays the authority.
//
// Lazy: built on the first ForEach*PropertyTxMember call from every current row
// (sealed included) and history row, then every row is recorded where it enters
// the store (storedRel / historyRel / storedNode / historyNode in
// memorystore_ordinal.go), all under ms.mu. Clear, index drop, retention purge and
// exact erasure drop the sidecars (an erased value must not survive in RAM);
// truncation and compaction keep them (a superset).

// recordRelRowLocked records one relationship row the store keeps (current or
// history) into every built membership sidecar (K1 rel-type set and property
// sets). Every row, not only creates: a row's TxFrom lowers the member's bound,
// and its valid-time span joins the member's spans. Caller
// holds ms.mu (write).
func (ms *Store) recordRelRowLocked(r *types.Relationship, current bool) {
	ms.recordRelTypeMemberLocked(r)
	ms.recordRelPropTxLocked(r, current)
}

// recordNodeRowLocked is recordRelRowLocked for node rows (K1 label set and
// node property sets). Caller holds ms.mu (write).
func (ms *Store) recordNodeRowLocked(n *types.Node, current bool) {
	ms.recordNodeLabelMembersLocked(n)
	ms.recordNodePropTxLocked(n, current)
}

// recordRelPropTxLocked records r's indexable values into the built rel
// sidecars of its type, as a current or a history row. Caller holds ms.mu
// (write).
func (ms *Store) recordRelPropTxLocked(r *types.Relationship, current bool) {
	if len(ms.relPropTxMembers) == 0 || r == nil {
		return
	}
	tok := r.TypeToken().Value()
	id := r.ID()
	ri := indexpkg.RelRowInfo(ms.validAxis.Get(), r)
	ms.forEachRelPropTxKey(r, tok, func(propertyKey, valueKey string) bool {
		if m := ms.relPropTxMembers[indexpkg.RelPropertyIndexKey{RelTypeToken: tok, PropertyKey: propertyKey}]; m != nil {
			m.RecordRow(valueKey, id, ri, current)
		}
		return true
	})
}

// recordNodePropTxLocked records n's indexable values into the built node
// sidecars of every label the row carries. Caller holds ms.mu (write).
func (ms *Store) recordNodePropTxLocked(n *types.Node, current bool) {
	if len(ms.nodePropTxMembers) == 0 || n == nil {
		return
	}
	id := n.ID()
	ri := indexpkg.NodeRowInfo(ms.validAxis.Get(), n)
	for i := 0; i < n.LabelTokenCount(); i++ {
		label := n.LabelTokenRawAt(i)
		n.ForEachIndexKey(indexpkg.ElementsOf(ms.propertyIndexes, label), func(propertyKey, valueKey string) bool {
			if m := ms.nodePropTxMembers[indexpkg.PropertyIndexKey{LabelToken: label, PropertyKey: propertyKey}]; m != nil {
				m.RecordRow(valueKey, id, ri, current)
			}
			return true
		})
	}
}

// recordRelReplacedLocked records prev, the copy of a current row an update
// door moves to history, replaced by next: K1 as any row,
// and in the property sidecars prev ends at next (indexpkg.RecordReplaced).
func (ms *Store) recordRelReplacedLocked(prev, next *types.Relationship) {
	ms.recordRelTypeMemberLocked(prev)
	if len(ms.relPropTxMembers) == 0 || prev == nil || next == nil {
		return
	}
	tok := prev.TypeToken().Value()
	id := prev.ID()
	old, nx := indexpkg.RelRowInfo(ms.validAxis.Get(), prev), indexpkg.RelRowInfo(ms.validAxis.Get(), next)
	ms.forEachRelPropTxKey(prev, tok, func(propertyKey, valueKey string) bool {
		if sc := ms.relPropTxMembers[indexpkg.RelPropertyIndexKey{RelTypeToken: tok, PropertyKey: propertyKey}]; sc != nil {
			sc.RecordReplaced(valueKey, id, old, nx)
		}
		return true
	})
}

// recordNodeReplacedLocked is recordRelReplacedLocked for a node row.
func (ms *Store) recordNodeReplacedLocked(prev, next *types.Node) {
	ms.recordNodeLabelMembersLocked(prev)
	if len(ms.nodePropTxMembers) == 0 || prev == nil || next == nil {
		return
	}
	id := prev.ID()
	old, nx := indexpkg.NodeRowInfo(ms.validAxis.Get(), prev), indexpkg.NodeRowInfo(ms.validAxis.Get(), next)
	for i := 0; i < prev.LabelTokenCount(); i++ {
		label := prev.LabelTokenRawAt(i)
		prev.ForEachIndexKey(indexpkg.ElementsOf(ms.propertyIndexes, label), func(propertyKey, valueKey string) bool {
			if sc := ms.nodePropTxMembers[indexpkg.PropertyIndexKey{LabelToken: label, PropertyKey: propertyKey}]; sc != nil {
				sc.RecordReplaced(valueKey, id, old, nx)
			}
			return true
		})
	}
}

// forgetRelPropTxCurrentLocked notes that r, the current row a delete removes,
// is no current row any more: its members keep their history envelopes only
// Caller holds ms.mu (write).
func (ms *Store) forgetRelPropTxCurrentLocked(r *types.Relationship) {
	if len(ms.relPropTxMembers) == 0 || r == nil {
		return
	}
	tok := r.TypeToken().Value()
	id := r.ID()
	ms.forEachRelPropTxKey(r, tok, func(propertyKey, valueKey string) bool {
		if m := ms.relPropTxMembers[indexpkg.RelPropertyIndexKey{RelTypeToken: tok, PropertyKey: propertyKey}]; m != nil {
			m.ForgetCurrent(valueKey, id)
		}
		return true
	})
}

// forgetNodePropTxCurrentLocked is forgetRelPropTxCurrentLocked for a node.
func (ms *Store) forgetNodePropTxCurrentLocked(n *types.Node) {
	if len(ms.nodePropTxMembers) == 0 || n == nil {
		return
	}
	id := n.ID()
	for i := 0; i < n.LabelTokenCount(); i++ {
		label := n.LabelTokenRawAt(i)
		n.ForEachIndexKey(indexpkg.ElementsOf(ms.propertyIndexes, label), func(propertyKey, valueKey string) bool {
			if m := ms.nodePropTxMembers[indexpkg.PropertyIndexKey{LabelToken: label, PropertyKey: propertyKey}]; m != nil {
				m.ForgetCurrent(valueKey, id)
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

// relPropTxSidecarLocked returns the built sidecar for key (building it first), or
// nil when no rel property index is declared or it is still being created.
// Caller holds ms.mu (write).
func (ms *Store) relPropTxSidecarLocked(key indexpkg.RelPropertyIndexKey) (*indexpkg.PropertyTxMembers[types.RelID], error) {
	return ms.relPropTxSidecarUnderLocked(key, nil)
}

func (ms *Store) relPropTxSidecarUnderLocked(key indexpkg.RelPropertyIndexKey, tally *storecontract.ReadTally) (*indexpkg.PropertyTxMembers[types.RelID], error) {
	idx, ok := ms.relPropertyIndexes[key]
	if !ok || idx.Mutated != nil {
		return nil, nil
	}
	if m := ms.relPropTxMembers[key]; m != nil {
		return m, nil
	}
	start := time.Now()
	m := indexpkg.NewPropertyTxMembers[types.RelID]()
	elements := idx.Elements()
	// Every row of the type, so a replaced row is ended by its replacing row
	// (indexpkg.RowScan).
	var scan indexpkg.RowScan[types.RelID]
	record := func(r *types.Relationship, current bool) error {
		noteTxMemberBuildRow()
		if !r.HasTypeTokenRaw(key.RelTypeToken) {
			return nil
		}
		vks, err := indexpkg.PropertyValueKeysAdmit(func(fn func(string) bool) {
			r.ForEachPropertyIndexKey(key.PropertyKey, elements, fn)
		}, propertyKeyAdmit(tally))
		if err != nil {
			return err
		}
		return scan.AddAdmit(r.ID(), indexpkg.RelRowInfo(ms.validAxis.Get(), r), current, vks, tally.Buffer)
	}
	// The sealed walker decodes before invoking record, including dead rows.
	// Admit its complete input before entering that native decoder.
	if err := tally.Candidates(len(ms.rels)); err != nil {
		return nil, err
	}
	for _, st := range ms.segTypes {
		if err := tally.Candidates(int(st.sealedRows)); err != nil {
			return nil, err
		}
		if err := tally.Decoded(int(st.segBytes)); err != nil {
			return nil, err
		}
	}
	// ADR-0011: every current row, sealed included; a decode error leaves the
	// sidecar unbuilt (it must be a superset).
	var recordErr error
	if err := ms.forEachCurrentRelLocked(func(r *types.Relationship) bool {
		recordErr = record(r, true)
		return recordErr == nil
	}); err != nil {
		return nil, err
	}
	if recordErr != nil {
		return nil, recordErr
	}
	for _, versions := range ms.relHistory {
		for _, r := range versions {
			if err := tally.Candidates(1); err != nil {
				return nil, err
			}
			if err := tally.History(1); err != nil {
				return nil, err
			}
			if err := record(r, false); err != nil {
				return nil, err
			}
		}
	}
	scan.Into(m)
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
	m, _ := ms.nodePropTxSidecarUnderLocked(key, nil)
	return m
}

func (ms *Store) nodePropTxSidecarUnderLocked(key indexpkg.PropertyIndexKey, tally *storecontract.ReadTally) (*indexpkg.PropertyTxMembers[types.NodeID], error) {
	idx, ok := ms.propertyIndexes[key]
	if !ok || idx.Mutated != nil {
		return nil, nil
	}
	if m := ms.nodePropTxMembers[key]; m != nil {
		return m, nil
	}
	start := time.Now()
	m := indexpkg.NewPropertyTxMembers[types.NodeID]()
	elements := idx.Elements()
	var scan indexpkg.RowScan[types.NodeID] // see relPropTxSidecarLocked
	record := func(n *types.Node, current bool) error {
		noteTxMemberBuildRow()
		if !n.HasLabelTokenRaw(key.LabelToken) {
			return nil
		}
		vks, err := indexpkg.PropertyValueKeysAdmit(func(fn func(string) bool) {
			n.ForEachPropertyIndexKey(key.PropertyKey, elements, fn)
		}, propertyKeyAdmit(tally))
		if err != nil {
			return err
		}
		return scan.AddAdmit(n.ID(), indexpkg.NodeRowInfo(ms.validAxis.Get(), n), current, vks, tally.Buffer)
	}
	for _, n := range ms.nodes {
		if err := tally.Candidates(1); err != nil {
			return nil, err
		}
		if err := record(n, true); err != nil {
			return nil, err
		}
	}
	for _, versions := range ms.nodeHistory {
		for _, n := range versions {
			if err := tally.Candidates(1); err != nil {
				return nil, err
			}
			if err := tally.History(1); err != nil {
				return nil, err
			}
			if err := record(n, false); err != nil {
				return nil, err
			}
		}
	}
	scan.Into(m)
	if ms.nodePropTxMembers == nil {
		ms.nodePropTxMembers = make(map[indexpkg.PropertyIndexKey]*indexpkg.PropertyTxMembers[types.NodeID])
	}
	ms.nodePropTxMembers[key] = m
	ms.propTxBuilds++
	ms.propTxBuildTime += time.Since(start)
	return m, nil
}

// readOrBuild runs read under ms.mu.RLock; if the sidecar is not built it runs
// build under ms.mu.Lock (build re-checks everything), so built lookups do not
// serialize on the write lock.
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
	return ms.forEachRelPropertyTxMember(relTypeToken, propertyKey, valueKey, fn, nil)
}

func (ms *Store) forEachRelPropertyTxMember(relTypeToken uint16, propertyKey, valueKey string, fn func(id types.RelID, firstTxFrom types.Instant) bool, tally *storecontract.ReadTally) error {
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
		var err error
		members, err = m.MembersAdmit(valueKey, propertyMemberAdmit(tally))
		return true, err
	}, func() error {
		m, err := ms.relPropTxSidecarUnderLocked(key, tally)
		if err != nil {
			return err
		}
		if m == nil {
			return ErrIndexNotFound
		}
		members, err = m.MembersAdmit(valueKey, propertyMemberAdmit(tally))
		return err
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
	return ms.forEachNodePropertyTxMember(labelToken, propertyKey, valueKey, fn, nil)
}

func (ms *Store) forEachNodePropertyTxMember(labelToken uint16, propertyKey, valueKey string, fn func(id types.NodeID, firstTxFrom types.Instant) bool, tally *storecontract.ReadTally) error {
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
		var err error
		members, err = m.MembersAdmit(valueKey, propertyMemberAdmit(tally))
		return true, err
	}, func() error {
		m, err := ms.nodePropTxSidecarUnderLocked(key, tally)
		if err != nil {
			return err
		}
		if m == nil {
			return ErrIndexNotFound
		}
		members, err = m.MembersAdmit(valueKey, propertyMemberAdmit(tally))
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

// forEachRelPropTxKey calls fn with r's index keys for its type's sidecars:
// each indexed value (with elements, each element), and NullIndexKey for a
// sidecar's property r lacks or holds nil (the null posting a Null probe
// reads). Caller holds ms.mu.
func (ms *Store) forEachRelPropTxKey(r *types.Relationship, tok uint16, fn func(propertyKey, valueKey string) bool) {
	more := true
	r.ForEachIndexKey(indexpkg.RelElementsOf(ms.relPropertyIndexes, tok), func(propertyKey, valueKey string) bool {
		more = fn(propertyKey, valueKey)
		return more
	})
	for key := range ms.relPropTxMembers {
		if !more {
			return
		}
		if key.RelTypeToken == tok && r.HasPropertyIndexKey(key.PropertyKey, false, types.NullIndexKey) {
			more = fn(key.PropertyKey, types.NullIndexKey)
		}
	}
}
