package index

import (
	"cmp"
	"slices"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

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
//
// Ordered views (backlog 69): OrderValue opens, for one value, an ascending
// view of its members that Record keeps from then on, so MembersAfter pages
// the members above a cursor in O(log n + page) instead of enumerating the
// value. A view exists only for the values a paged read asked for; it is
// built into full exact-capacity chunks and an ascending ID stream fills new
// ones (sortedChunks.add): 8.06 B per member opened over 1,000,000 members,
// 8.02 B grown by 1,000,000 ascending records (was 18.6 with the plain insert
// path; BenchmarkPropertyTxMembersOrderedViewBytes,
// tasks/evidence/paged-effective/09-view-bytes.txt).
type PropertyTxMembers[ID cmp.Ordered] struct {
	values   map[string]txPostings[ID]
	postings int64
	ordered  map[string]*sortedChunks[ID]
	// numeric, nil until OrderNumeric opens it (round 8 request 2): the
	// value keys by numeric sort key, so MembersInRange visits the values of
	// a range instead of every value. Record keeps it once open.
	numeric *numericValueView
}

type txPostings[ID cmp.Ordered] struct {
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
func NewPropertyTxMembers[ID cmp.Ordered]() *PropertyTxMembers[ID] {
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
		m.orderNew(vk, id)
		if m.numeric != nil {
			m.numeric.add(vk)
		}
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
	m.orderNew(vk, id)
}

// orderNew adds a new member of vk to vk's ordered view, when one is open.
func (m *PropertyTxMembers[ID]) orderNew(vk string, id ID) {
	if o := m.ordered[vk]; o != nil {
		o.add(id)
	}
}

// OrderValue opens vk's ordered view (a no-op when open) and reports whether
// vk has one: a value without members gets none (MembersAfter answers it
// empty), so a lookup of an unknown value allocates nothing. The owner calls
// it under its exclusive lock.
func (m *PropertyTxMembers[ID]) OrderValue(vk string) bool {
	if m == nil {
		return false
	}
	if m.ordered[vk] != nil {
		return true
	}
	p, ok := m.values[vk]
	if !ok {
		return false
	}
	ids := make([]ID, 0, 1+len(p.more))
	ids = append(ids, p.id)
	for id := range p.more {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	o := newSortedChunksFrom(ids)
	if m.ordered == nil {
		m.ordered = make(map[string]*sortedChunks[ID])
	}
	m.ordered[vk] = o
	return true
}

// MembersAfter returns at most limit members of vk with an ID above after,
// ascending, with their bounds (caller-owned). ok is false when vk has members
// but no ordered view (the owner opens one with OrderValue, then asks again);
// a value without members answers empty with ok. The owner calls it under at
// least its shared lock.
func (m *PropertyTxMembers[ID]) MembersAfter(vk string, after ID, limit int) (out []TxMember[ID], ok bool) {
	if m == nil {
		return nil, true
	}
	p, has := m.values[vk]
	if !has {
		return nil, true
	}
	o := m.ordered[vk]
	if o == nil {
		return nil, false
	}
	out = make([]TxMember[ID], 0, min(max(limit, 0), o.n))
	if limit <= 0 {
		return out, true
	}
	o.forEachFrom(after, func(id ID) bool {
		if id == after {
			return true
		}
		tx := p.tx
		if id != p.id {
			tx = p.more[id]
		}
		out = append(out, TxMember[ID]{ID: id, FirstTx: tx})
		return len(out) < limit
	})
	return out, true
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
