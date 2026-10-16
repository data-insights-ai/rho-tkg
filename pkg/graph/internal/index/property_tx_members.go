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
	// windows, per value a valid-time view a windowed lookup opened (ledger
	// row 62, property_tx_members_window.go); Record keeps each open one.
	windows map[string]*windowView[ID]
	// numeric, nil until OrderNumeric opens it (round 8 request 2): the
	// value keys by numeric sort key, so MembersInRange visits the values of
	// a range instead of every value. Record keeps it once open.
	numeric *numericValueView
}

type txPostings[ID cmp.Ordered] struct {
	id   ID
	p    txPosting
	more map[ID]txPosting
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

// Record notes that a row of id carrying the value vk has TxFrom tx, with an
// unknown valid-time span (the member meets every window). An empty vk (a
// value that is not indexable) is ignored.
func (m *PropertyTxMembers[ID]) Record(vk string, id ID, tx types.Instant) {
	m.update(vk, id, func(p *txPosting) {
		p.noteTx(tx, tx)
		p.flags |= spanWild
	})
}

// RecordRow notes one stored row of id carrying vk (ledger row 62): its first
// tx, its stamps and valid-time span (RowInfo), as the member's current row or
// as a history row. The store records every row it keeps here; see
// txPosting.recordCurrent / recordHistory.
func (m *PropertyTxMembers[ID]) RecordRow(vk string, id ID, ri RowInfo, current bool) {
	m.update(vk, id, func(p *txPosting) {
		if current {
			p.recordCurrent(ri)
		} else {
			p.recordHistory(ri)
		}
	})
}

// RecordReplaced notes old, the copy of id's current row an update door moved
// to history, and next, the current row that replaced it (ledger row 62): old
// joins the late envelope ended by next (RowInfo.endedBy) under every value
// old carries, whatever next carries. The update doors call it for the
// demoted row instead of RecordRow.
func (m *PropertyTxMembers[ID]) RecordReplaced(vk string, id ID, old, next RowInfo) {
	m.update(vk, id, func(p *txPosting) { p.recordReplaced(old, next) })
}

// ForgetCurrent notes that a delete door removed id's current row: the
// member's late envelope keeps its history (the delete's tombstone is
// recorded as a history row first). A no-op for an unknown member.
func (m *PropertyTxMembers[ID]) ForgetCurrent(vk string, id ID) {
	if m == nil {
		return
	}
	ps, ok := m.values[vk]
	if !ok {
		return
	}
	if _, member := ps.lookup(id); !member {
		return
	}
	m.update(vk, id, func(p *txPosting) { p.forgetCurrent() })
}

// Restore installs one posting as an open image held it (ForEachValueSpans),
// uniting it with an existing one.
func (m *PropertyTxMembers[ID]) Restore(vk string, mb SpanMember[ID]) {
	add := postingOf(mb)
	m.update(vk, mb.ID, func(p *txPosting) { p.unite(add) })
}

// update applies fn to vk's posting of id (newPosting when id is new) and
// keeps the open views. An empty vk is ignored.
func (m *PropertyTxMembers[ID]) update(vk string, id ID, fn func(*txPosting)) {
	if m == nil || vk == "" {
		return
	}
	ps, ok := m.values[vk]
	if !ok {
		q := newPosting()
		fn(&q)
		m.values[vk] = txPostings[ID]{id: id, p: q}
		m.postings++
		m.orderNew(vk, id)
		if m.numeric != nil {
			m.numeric.add(vk)
		}
		m.windowSet(vk, id, txPosting{}, false, q)
		return
	}
	if ps.id == id {
		q := ps.p
		fn(&q)
		if q != ps.p {
			old := ps.p
			ps.p = q
			m.values[vk] = ps
			m.windowSet(vk, id, old, true, q)
		}
		return
	}
	prev, member := ps.more[id]
	if !member {
		prev = newPosting()
	}
	q := prev
	fn(&q)
	if member && q == prev {
		return
	}
	if ps.more == nil {
		ps.more = make(map[ID]txPosting)
		m.values[vk] = ps
	}
	ps.more[id] = q
	if !member {
		m.postings++
		m.orderNew(vk, id)
	}
	m.windowSet(vk, id, prev, member, q)
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
		out = append(out, TxMember[ID]{ID: id, FirstTx: p.posting(id).tx})
		return len(out) < limit
	})
	return out, true
}

// Merge folds every posting of other into m (other is left unchanged). A
// current span merges as a union, not a replacement: a build scan and the
// records of concurrent writes commute, at the cost of a stale current span
// staying wide (sound, ledger row 62).
func (m *PropertyTxMembers[ID]) Merge(other *PropertyTxMembers[ID]) {
	if m == nil || other == nil {
		return
	}
	for vk, p := range other.values {
		add := p.p
		m.update(vk, p.id, func(q *txPosting) { q.unite(add) })
		for id, add := range p.more {
			m.update(vk, id, func(q *txPosting) { q.unite(add) })
		}
	}
}

// HasViews reports whether any ordered, numeric or window view is open.
func (m *PropertyTxMembers[ID]) HasViews() bool {
	return m != nil && (len(m.ordered) > 0 || len(m.windows) > 0 || m.numeric != nil)
}

// ForEachValue calls fn once per value with its members (the open image's
// encoder, backlog 74). members is reused between calls: valid only during fn.
// The owner calls it under at least its shared lock.
func (m *PropertyTxMembers[ID]) ForEachValue(fn func(vk string, members []TxMember[ID])) {
	if m == nil {
		return
	}
	var buf []TxMember[ID]
	for vk, p := range m.values {
		buf = append(buf[:0], TxMember[ID]{ID: p.id, FirstTx: p.p.tx})
		for id, q := range p.more {
			buf = append(buf, TxMember[ID]{ID: id, FirstTx: q.tx})
		}
		fn(vk, buf)
	}
}

// SpanMember is one posting with its valid-time state (the open image's
// encoder, ledger row 62): All / Late / Cur hold only when HasAll / HasLate /
// HasCur; Wild marks a member recorded without a span (Record), which meets
// every window. See txPosting for the fields.
type SpanMember[ID comparable] struct {
	ID                      ID
	FirstTx, LastTx         types.Instant
	All, Late, Cur          Span
	CurVersion              uint32
	HasAll, HasLate, HasCur bool
	Wild                    bool
}

// ForEachValueSpans is ForEachValue with each posting's spans. members is
// reused between calls: valid only during fn. The owner calls it under at
// least its shared lock.
func (m *PropertyTxMembers[ID]) ForEachValueSpans(fn func(vk string, members []SpanMember[ID])) {
	if m == nil {
		return
	}
	var buf []SpanMember[ID]
	for vk, p := range m.values {
		buf = append(buf[:0], spanMemberOf(p.id, p.p))
		for id, q := range p.more {
			buf = append(buf, spanMemberOf(id, q))
		}
		fn(vk, buf)
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
	out = append(out, TxMember[ID]{ID: p.id, FirstTx: p.p.tx})
	for id, q := range p.more {
		out = append(out, TxMember[ID]{ID: id, FirstTx: q.tx})
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
