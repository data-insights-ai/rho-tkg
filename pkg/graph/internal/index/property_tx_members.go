package index

import "github.com/data-insights-ai/rho-tkg/v4/pkg/types"

// PropertyTxMembers is the transaction-time membership sidecar of ONE declared
// property index (backlog 8): canonical value key -> the entities whose rows
// ever carried that value (store.RelPropertyTxMembershipCapability /
// NodePropertyTxMembershipCapability), each with a lower bound on the TxFrom of
// the earliest such row. APPEND-ONLY: nothing is ever removed, so it stays a
// sound superset of the entities a temporal lookup for the value can match
// (the chain resolver rejects the rest). A store drops the whole structure
// (Clear, retention purge, exact erasure, index drop) and rebuilds it rather
// than deleting postings. Not safe for concurrent use: the owning store
// guards it with its own lock.
//
// Layout (measured, tasks/evidence/pinned-property-index/): one value's first
// posting is held inline, later ones in an overflow map. 25 B per posting at
// 1 M and 10 M postings with >= 20 postings per value; about 100 B per
// posting when every value is distinct (a plain map per value costs 264 B
// there).
type PropertyTxMembers[ID comparable] struct {
	values   map[string]txPostings[ID]
	postings int64
}

type txPostings[ID comparable] struct {
	id   ID
	tx   types.Instant
	more map[ID]types.Instant
}

// TxMember is one enumerated member: the entity and the lower bound on the
// transaction time of its earliest row carrying the value (0 = unknown).
type TxMember[ID comparable] struct {
	ID      ID
	FirstTx types.Instant
}

// NewPropertyTxMembers returns an empty sidecar.
func NewPropertyTxMembers[ID comparable]() *PropertyTxMembers[ID] {
	return &PropertyTxMembers[ID]{values: make(map[string]txPostings[ID])}
}

// MergeFirstTx folds a row's TxFrom into a member's lower bound: the minimum,
// except that 0 (a row without a transaction-time stamp, which a TxAt read
// treats as visible at every pin) is the "unknown" bound and is never raised.
func MergeFirstTx(prev, tx types.Instant) types.Instant {
	if prev == 0 || tx == 0 {
		return 0
	}
	if tx < prev {
		return tx
	}
	return prev
}

// Record notes that a row of id carrying the value vk has TxFrom tx. An empty
// vk (a value that is not indexable) is ignored.
func (m *PropertyTxMembers[ID]) Record(vk string, id ID, tx types.Instant) {
	if m == nil || vk == "" {
		return
	}
	p, ok := m.values[vk]
	switch {
	case !ok:
		m.values[vk] = txPostings[ID]{id: id, tx: tx}
		m.postings++
		return
	case p.id == id:
		if merged := MergeFirstTx(p.tx, tx); merged != p.tx {
			p.tx = merged
			m.values[vk] = p
		}
		return
	}
	if p.more == nil {
		p.more = make(map[ID]types.Instant)
		m.values[vk] = p
	}
	if prev, ok := p.more[id]; ok {
		p.more[id] = MergeFirstTx(prev, tx)
		return
	}
	p.more[id] = tx
	m.postings++
}

// Merge folds every posting of other into m (other is left unchanged).
func (m *PropertyTxMembers[ID]) Merge(other *PropertyTxMembers[ID]) {
	if m == nil || other == nil {
		return
	}
	for vk, p := range other.values {
		m.Record(vk, p.id, p.tx)
		for id, tx := range p.more {
			m.Record(vk, id, tx)
		}
	}
}

// Members returns a caller-owned snapshot of vk's members.
func (m *PropertyTxMembers[ID]) Members(vk string) []TxMember[ID] {
	if m == nil {
		return nil
	}
	p, ok := m.values[vk]
	if !ok {
		return nil
	}
	out := make([]TxMember[ID], 0, 1+len(p.more))
	out = append(out, TxMember[ID]{ID: p.id, FirstTx: p.tx})
	for id, tx := range p.more {
		out = append(out, TxMember[ID]{ID: id, FirstTx: tx})
	}
	return out
}

// Postings returns the number of (entity, value) postings held.
func (m *PropertyTxMembers[ID]) Postings() int64 {
	if m == nil {
		return 0
	}
	return m.postings
}
