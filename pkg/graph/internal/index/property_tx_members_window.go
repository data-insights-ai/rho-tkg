package index

import (
	"cmp"
	"math"
	"slices"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Windowed membership (ledger row 62, sigma-tkgd 2026-10-16): each posting
// carries the valid-time state of the member's rows that carry the value, so a
// lookup "members of v whose rows may meet [a, b) at transaction time P" reads
// O(log n + k + r) through a per-value view instead of every member: k the
// members whose late envelope meets the window, r the members written after P.
//
// A row's valid interval as the chain resolver derives it never reaches past
// its stored span [effective valid-from, ValidTo) (open: +inf), except that a
// delete tombstone read before its delete reopens (storeutil.DeleteWroteValidTo), so
// a posting keeps:
//
//   - all: the union of every row's span, a tombstone's reopened: it bounds
//     the member's matches at every pin;
//   - late and cur: the late envelope, which bounds them at every pin at or
//     after last (the newest stamp of any row recorded: TxFrom, TxTo,
//     DeletedAt). cur is the current row's span, replaced by the next current
//     row; late unites the history rows' spans, where a row superseded open
//     by a replacing row (recorded at its TxTo, a higher version, starting no
//     earlier) ends at that row's start — both resolver arms end it there (the
//     positional next start, and supersessionCaps). A tombstone counts as
//     stored (at those pins its delete is visible).
//
// Every later write can only narrow a row's derived interval (a new row is a
// newer belief or a nearer next start; deletes cap; retractions and excisions
// drop), so the envelopes stay sound supersets while the posting grows.

// Span is a row's valid-time interval [From, To) with To effective: an open
// row (ValidTo 0) has To = math.MaxInt64.
type Span struct {
	From, To types.Instant
}

func (s Span) union(o Span) Span {
	return Span{From: min(s.From, o.From), To: max(s.To, o.To)}
}

func (s Span) meets(start, end types.Instant) bool {
	return s.From < end && s.To > start
}

// RowInfo is what a sidecar records of one stored row: its version, its
// transaction-time stamps and its valid-time span (the rule the envelope
// indexes use: storeutil.RelValidFrom / NodeValidFrom, ValidTo open as
// math.MaxInt64).
type RowInfo struct {
	Version                 uint32
	TxFrom, TxTo, DeletedAt types.Instant
	Span                    Span
	// DeleteWrote: a tombstone whose ValidTo its delete wrote
	// (storeutil.DeleteWroteValidTo).
	DeleteWrote bool
}

// RelRowInfo returns r's RowInfo.
func RelRowInfo(r *types.Relationship) RowInfo {
	_, to, _ := r.ValidRange()
	f, t, d := r.TxStamps()
	return RowInfo{Version: r.Version(), TxFrom: f, TxTo: t, DeletedAt: d,
		Span: Span{From: storepkg.RelValidFrom(r), To: effTo(to)}, DeleteWrote: storepkg.DeleteWroteValidTo(r.Temporal())}
}

// NodeRowInfo returns n's RowInfo.
func NodeRowInfo(n *types.Node) RowInfo {
	_, to, _ := n.ValidRange()
	f, t, d := n.TxStamps()
	return RowInfo{Version: n.Version(), TxFrom: f, TxTo: t, DeletedAt: d,
		Span: Span{From: storepkg.NodeValidFrom(n), To: effTo(to)}, DeleteWrote: storepkg.DeleteWroteValidTo(n.Temporal())}
}

// last is the row's newest stamp.
func (ri RowInfo) last() types.Instant {
	return max(ri.TxFrom, ri.TxTo, ri.DeletedAt)
}

// anyPinSpan is the row's span as read at any pin: a delete that wrote the
// ValidTo reopens before the delete (storeutil.NormalizeTemporalVisibleAtTxTime);
// a ValidTo a delete kept already bounds the row at every pin.
func (ri RowInfo) anyPinSpan() Span {
	if ri.DeleteWrote {
		return Span{From: ri.Span.From, To: math.MaxInt64}
	}
	return ri.Span
}

// replaced reports whether ri is a row a later row replaced while it was
// believed (TxTo after its TxFrom, not a delete): supersessionCaps' rule.
func (ri RowInfo) replaced() bool {
	return ri.DeletedAt == 0 && ri.TxTo > ri.TxFrom
}

// endedBy returns what ri still answers at pins where next is visible: an
// open row replaced by next (next recorded at ri's TxTo, a higher version,
// starting no earlier) ends at next's start; any other row keeps its span.
func (ri RowInfo) endedBy(next RowInfo) Span {
	if ri.replaced() && ri.Span.To == math.MaxInt64 && next.TxFrom == ri.TxTo &&
		next.Version > ri.Version && next.Span.From >= ri.Span.From {
		return Span{From: ri.Span.From, To: next.Span.From}
	}
	return ri.Span
}

const (
	postAll   uint8 = 1 << iota // all holds the any-pin envelope
	postLate                    // late holds the history part of the late envelope
	postCur                     // cur holds the current row's span
	spanWild                    // a row of unknown span: meets every window
	postFresh                   // no row recorded yet: tx is unset
)

// txPosting is one (value, member) posting. tx is the lower bound on the
// first TxFrom (MergeFirstTx); last the newest stamp; all / late / cur as in
// the file comment; curVer the version of cur's row (0: unknown, after a
// union).
type txPosting struct {
	tx, last  types.Instant
	all, late Span
	cur       Span
	curVer    uint32
	flags     uint8
}

func newPosting() txPosting { return txPosting{flags: postFresh} }

func (p *txPosting) noteTx(first, last types.Instant) {
	if p.flags&postFresh != 0 {
		p.tx = first
		p.flags &^= postFresh
	} else {
		p.tx = MergeFirstTx(p.tx, first)
	}
	p.last = max(p.last, last)
}

func (p *txPosting) addAll(s Span) {
	if p.flags&postAll != 0 {
		s = p.all.union(s)
	}
	p.all = s
	p.flags |= postAll
}

func (p *txPosting) addLate(s Span) {
	if s.From >= s.To {
		return // an empty piece: nothing to answer
	}
	if p.flags&postLate != 0 {
		s = p.late.union(s)
	}
	p.late = s
	p.flags |= postLate
}

// recordCurrent notes ri as the member's current row. A different row cur
// still held (its replacement was not recorded through recordReplaced: an
// in-place rewrite) moves into late as it was; the same row recorded again
// just refreshes cur.
func (p *txPosting) recordCurrent(ri RowInfo) {
	p.noteTx(ri.TxFrom, ri.last())
	p.addAll(ri.anyPinSpan())
	if p.flags&postCur != 0 && ri.Version != p.curVer {
		p.addLate(p.cur)
	}
	p.cur, p.curVer = ri.Span, ri.Version
	p.flags |= postCur
}

// recordHistory notes ri as a history row: it joins late as stored.
func (p *txPosting) recordHistory(ri RowInfo) {
	p.noteTx(ri.TxFrom, ri.last())
	p.addAll(ri.anyPinSpan())
	p.addLate(ri.Span)
}

// recordReplaced notes old, the copy of the current row an update door moved
// to history, and next, the row that replaced it: old joins late ended by
// next (endedBy), and cur no longer holds it.
func (p *txPosting) recordReplaced(old, next RowInfo) {
	p.noteTx(old.TxFrom, old.last())
	p.addAll(old.anyPinSpan())
	p.addLate(old.endedBy(next))
	if p.flags&postCur != 0 && p.curVer == old.Version {
		p.dropCur()
	}
}

// forgetCurrent drops cur after a delete removed the current row (its
// tombstone joined late as a history row first).
func (p *txPosting) forgetCurrent() {
	if p.flags&postCur != 0 {
		p.dropCur()
	}
}

func (p *txPosting) dropCur() {
	p.cur, p.curVer = Span{}, 0
	p.flags &^= postCur
}

// unite folds add into p as a union (a build scan against concurrent records,
// an image restore): every envelope widens; two different current rows
// leave cur as their union with no clamp evidence.
func (p *txPosting) unite(add txPosting) {
	if add.flags&postFresh != 0 {
		return
	}
	p.noteTx(add.tx, add.last)
	p.flags |= add.flags & spanWild
	if add.flags&postAll != 0 {
		p.addAll(add.all)
	}
	if add.flags&postLate != 0 {
		p.addLate(add.late)
	}
	if add.flags&postCur == 0 {
		return
	}
	switch {
	case p.flags&postCur == 0:
		p.cur, p.curVer = add.cur, add.curVer
		p.flags |= postCur
	case p.cur == add.cur && p.curVer == add.curVer:
	default:
		p.cur, p.curVer = p.cur.union(add.cur), 0
	}
}

// envAll is the any-pin envelope; false: no span recorded.
func (p txPosting) envAll() (Span, bool) {
	switch {
	case p.flags&spanWild != 0:
		return Span{From: types.UnboundedValidFrom, To: math.MaxInt64}, true
	case p.flags&postAll != 0:
		return p.all, true
	}
	return Span{}, false
}

// envLate is the envelope for pins at or after last; false: none.
func (p txPosting) envLate() (Span, bool) {
	switch {
	case p.flags&spanWild != 0:
		return Span{From: types.UnboundedValidFrom, To: math.MaxInt64}, true
	case p.flags&(postLate|postCur) == postLate|postCur:
		return p.late.union(p.cur), true
	case p.flags&postLate != 0:
		return p.late, true
	case p.flags&postCur != 0:
		return p.cur, true
	}
	return Span{}, false
}

// spanMemberOf returns id's posting p as the image encoder reads it.
func spanMemberOf[ID comparable](id ID, p txPosting) SpanMember[ID] {
	return SpanMember[ID]{ID: id, FirstTx: p.tx, LastTx: p.last, All: p.all, Late: p.late, Cur: p.cur,
		CurVersion: p.curVer,
		HasAll:     p.flags&postAll != 0, HasLate: p.flags&postLate != 0, HasCur: p.flags&postCur != 0,
		Wild: p.flags&spanWild != 0}
}

// postingOf is spanMemberOf's inverse.
func postingOf[ID comparable](mb SpanMember[ID]) txPosting {
	p := txPosting{tx: mb.FirstTx, last: mb.LastTx, all: mb.All, late: mb.Late, cur: mb.Cur,
		curVer: mb.CurVersion}
	if mb.HasAll {
		p.flags |= postAll
	}
	if mb.HasLate {
		p.flags |= postLate
	}
	if mb.HasCur {
		p.flags |= postCur
	}
	if mb.Wild {
		p.flags |= spanWild
	}
	return p
}

// lookup returns id's posting in ps.
func (ps txPostings[ID]) lookup(id ID) (txPosting, bool) {
	if id == ps.id {
		return ps.p, true
	}
	q, ok := ps.more[id]
	return q, ok
}

// posting returns the posting of id in ps (zero when absent).
func (ps txPostings[ID]) posting(id ID) txPosting {
	q, _ := ps.lookup(id)
	return q
}

// windowView is one value's window: late indexes each member's late
// envelope, recent each member's last stamp.
type windowView[ID cmp.Ordered] struct {
	late   spanView[ID]
	recent lastView[ID]
}

// windowSet updates vk's open window view with id's posting p; old is the
// posting it replaces (had: id was a member), which locates id's entries in
// the views' sorted mains without a lookup per entry at the fold.
func (m *PropertyTxMembers[ID]) windowSet(vk string, id ID, old txPosting, had bool, p txPosting) {
	if w := m.windows[vk]; w != nil {
		oldSp, oldOK := old.envLate()
		sp, ok := p.envLate()
		w.late.set(id, oldSp, oldOK && had, sp, ok)
		w.recent.set(id, old.last, had, p.last)
	}
}

// OpenWindow opens vk's window view (a no-op when open) and reports whether
// vk has one: a value without members gets none (MembersDuring answers it
// empty). O(n log n) once per value; Record keeps it from then on in O(1)
// amortised plus a fold of the pending changes into the sorted trees every
// ~2 sqrt(n) of them. The owner calls it under its exclusive lock.
func (m *PropertyTxMembers[ID]) OpenWindow(vk string) bool {
	if m == nil {
		return false
	}
	if m.windows[vk] != nil {
		return true
	}
	ps, ok := m.values[vk]
	if !ok {
		return false
	}
	late := make([]spanEntry[ID], 0, 1+len(ps.more))
	recent := make([]lastEntry[ID], 0, 1+len(ps.more))
	add := func(id ID, q txPosting) {
		if sp, ok := q.envLate(); ok {
			late = append(late, spanEntry[ID]{from: sp.From, to: sp.To, id: id})
		}
		recent = append(recent, lastEntry[ID]{last: q.last, id: id})
	}
	add(ps.id, ps.p)
	for id, q := range ps.more {
		add(id, q)
	}
	w := &windowView[ID]{}
	w.late.install(late)
	w.recent.install(recent)
	if m.windows == nil {
		m.windows = make(map[string]*windowView[ID])
	}
	m.windows[vk] = w
	return true
}

// MembersDuring returns, caller-owned, the members of vk whose rows carrying
// vk may be valid somewhere in [start, end) at transaction time pin (0: no
// pin, every row), each once with its first-tx bound, unordered: those whose
// last stamp is at or before the pin by their late envelope, the others by
// their any-pin envelope. ok is false when vk has members but no window view
// (the owner opens one with OpenWindow, then asks again); a value without
// members, or start >= end, answers empty with ok. O(log n + k + r + pending
// changes). The owner calls it under at least its shared lock.
func (m *PropertyTxMembers[ID]) MembersDuring(vk string, start, end, pin types.Instant) (out []TxMember[ID], ok bool) {
	if m == nil {
		return nil, true
	}
	ps, has := m.values[vk]
	if !has || start >= end {
		return nil, true
	}
	w := m.windows[vk]
	if w == nil {
		return nil, false
	}
	var later map[ID]struct{}
	if pin != 0 && pin < math.MaxInt64 {
		w.recent.after(pin, func(id ID) {
			if later == nil {
				later = make(map[ID]struct{})
			}
			later[id] = struct{}{}
			q := ps.posting(id)
			if sp, ok := q.envAll(); ok && sp.meets(start, end) {
				out = append(out, TxMember[ID]{ID: id, FirstTx: q.tx})
			}
		})
	}
	w.late.overlap(start, end, func(id ID) {
		if _, decided := later[id]; decided {
			return
		}
		out = append(out, TxMember[ID]{ID: id, FirstTx: ps.posting(id).tx})
	})
	return out, true
}

// spanEntry is one member's envelope in a window view.
type spanEntry[ID cmp.Ordered] struct {
	from, to types.Instant
	id       ID
}

// spanChange is a pending change of a member's envelope; none: it has none.
type spanChange[ID cmp.Ordered] struct {
	spanEntry[ID]
	none bool
}

func cmpSpanEntry[ID cmp.Ordered](a, b spanEntry[ID]) int {
	if c := cmp.Compare(a.from, b.from); c != 0 {
		return c
	}
	return cmp.Compare(a.id, b.id)
}

// spanView is one value's valid-time view: main sorted by (from, id) as the
// in-order walk of an implicit balanced tree whose subMax[i] is the largest
// to under i (the TemporalIndex layout, O(log n + k) overlap), plus the
// changes since the last fold in pending (pos: id -> index), which override
// main's entry of the same id (dead: those entries' indexes in main, found by
// binary search at the change). Mutated only under the owner's exclusive
// lock; reads never write, so concurrent readers need no extra lock.
type spanView[ID cmp.Ordered] struct {
	main    []spanEntry[ID]
	subMax  []types.Instant
	pending []spanChange[ID]
	pos     map[ID]int
	dead    []int
	spare   []spanEntry[ID] // the previous main's storage, reused by the next fold
}

// foldAt is the pending size at which a view of n members folds: ~2 sqrt(n),
// so a fold's O(n) merge costs O(sqrt(n)) per change.
func foldAt(n int) int { return 64 + 2*isqrt(n) }

// set records id's new envelope (ok false: none) as a pending change and
// folds the pending changes into main once they outgrow foldAt. old is id's
// envelope as the view holds it (oldOK: it holds one).
func (w *spanView[ID]) set(id ID, old Span, oldOK bool, sp Span, ok bool) {
	e := spanChange[ID]{spanEntry: spanEntry[ID]{from: sp.From, to: sp.To, id: id}, none: !ok}
	if i, has := w.pos[id]; has {
		w.pending[i] = e
		return
	}
	if oldOK {
		key := spanEntry[ID]{from: old.From, id: id}
		if i, found := slices.BinarySearchFunc(w.main, key, cmpSpanEntry[ID]); found {
			w.dead = append(w.dead, i)
		}
	}
	if w.pos == nil {
		w.pos = make(map[ID]int)
	}
	w.pos[id] = len(w.pending)
	w.pending = append(w.pending, e)
	if len(w.pending) > foldAt(len(w.main)) {
		w.fold()
	}
}

// fold merges the pending changes (sorted, O(d log d)) into main in one
// O(n + d) pass that skips the dead entries, and rebuilds subMax.
func (w *spanView[ID]) fold() {
	add := make([]spanEntry[ID], 0, len(w.pending))
	for _, c := range w.pending {
		if !c.none {
			add = append(add, c.spanEntry)
		}
	}
	slices.SortFunc(add, cmpSpanEntry[ID])
	slices.Sort(w.dead)
	merged := w.spare[:0]
	if cap(merged) < len(w.main)+len(add) {
		merged = make([]spanEntry[ID], 0, len(w.main)+len(add)+len(add))
	}
	j, d := 0, 0
	for i, e := range w.main {
		if d < len(w.dead) && w.dead[d] == i {
			for d < len(w.dead) && w.dead[d] == i {
				d++
			}
			continue
		}
		for j < len(add) && cmpSpanEntry(add[j], e) < 0 {
			merged = append(merged, add[j])
			j++
		}
		merged = append(merged, e)
	}
	merged = append(merged, add[j:]...)
	w.spare = w.main
	w.setMain(merged)
}

// install sorts entries into main, builds subMax and clears pending.
func (w *spanView[ID]) install(entries []spanEntry[ID]) {
	slices.SortFunc(entries, cmpSpanEntry[ID])
	w.setMain(entries)
}

// setMain installs sorted entries as main, builds subMax, clears pending.
func (w *spanView[ID]) setMain(entries []spanEntry[ID]) {
	w.main = entries
	if cap(w.subMax) >= len(entries) {
		w.subMax = w.subMax[:len(entries)]
	} else {
		w.subMax = make([]types.Instant, len(entries))
	}
	w.fillSubMax(0, len(entries))
	w.pending = w.pending[:0]
	w.dead = w.dead[:0]
	clear(w.pos)
}

func (w *spanView[ID]) fillSubMax(lo, hi int) types.Instant {
	if lo >= hi {
		return math.MinInt64
	}
	mid := int(uint(lo+hi) >> 1)
	m := w.main[mid].to
	m = max(m, w.fillSubMax(lo, mid), w.fillSubMax(mid+1, hi))
	w.subMax[mid] = m
	return m
}

// overlap calls fn once per member whose envelope meets [start, end).
func (w *spanView[ID]) overlap(start, end types.Instant, fn func(ID)) {
	w.stab(0, len(w.main), start, end, fn)
	for _, e := range w.pending {
		if !e.none && e.from < end && e.to > start {
			fn(e.id)
		}
	}
}

func (w *spanView[ID]) stab(lo, hi int, start, end types.Instant, fn func(ID)) {
	if lo >= hi {
		return
	}
	mid := int(uint(lo+hi) >> 1)
	if w.subMax[mid] <= start {
		return // every envelope under mid ends at or before start
	}
	w.stab(lo, mid, start, end, fn)
	e := w.main[mid]
	if e.from >= end {
		return // the right subtree starts at or after end too
	}
	if e.to > start {
		if _, over := w.pos[e.id]; !over {
			fn(e.id)
		}
	}
	w.stab(mid+1, hi, start, end, fn)
}

// lastEntry is one member's last stamp in a recent view.
type lastEntry[ID cmp.Ordered] struct {
	last types.Instant
	id   ID
}

func cmpLastEntry[ID cmp.Ordered](a, b lastEntry[ID]) int {
	if c := cmp.Compare(a.last, b.last); c != 0 {
		return c
	}
	return cmp.Compare(a.id, b.id)
}

// lastView lists a value's members by (last stamp, id) so the members
// written after a pin are a suffix (O(log n + r)); pending changes override
// main and fold like spanView's.
type lastView[ID cmp.Ordered] struct {
	main    []lastEntry[ID]
	pending []lastEntry[ID]
	pos     map[ID]int
	dead    []int
	spare   []lastEntry[ID]
}

func (v *lastView[ID]) install(entries []lastEntry[ID]) {
	slices.SortFunc(entries, cmpLastEntry[ID])
	v.main = entries
	v.pending = v.pending[:0]
	v.dead = v.dead[:0]
	clear(v.pos)
}

// set records id's new last stamp; old is the one the view holds (had: it
// holds one).
func (v *lastView[ID]) set(id ID, old types.Instant, had bool, last types.Instant) {
	e := lastEntry[ID]{last: last, id: id}
	if i, has := v.pos[id]; has {
		v.pending[i] = e
		return
	}
	if had {
		if i, found := slices.BinarySearchFunc(v.main, lastEntry[ID]{last: old, id: id}, cmpLastEntry[ID]); found {
			v.dead = append(v.dead, i)
		}
	}
	if v.pos == nil {
		v.pos = make(map[ID]int)
	}
	v.pos[id] = len(v.pending)
	v.pending = append(v.pending, e)
	if len(v.pending) > foldAt(len(v.main)) {
		v.fold()
	}
}

func (v *lastView[ID]) fold() {
	add := slices.Clone(v.pending)
	slices.SortFunc(add, cmpLastEntry[ID])
	slices.Sort(v.dead)
	merged := v.spare[:0]
	if cap(merged) < len(v.main)+len(add) {
		merged = make([]lastEntry[ID], 0, len(v.main)+2*len(add))
	}
	j, d := 0, 0
	for i, e := range v.main {
		if d < len(v.dead) && v.dead[d] == i {
			for d < len(v.dead) && v.dead[d] == i {
				d++
			}
			continue
		}
		for j < len(add) && cmpLastEntry(add[j], e) < 0 {
			merged = append(merged, add[j])
			j++
		}
		merged = append(merged, e)
	}
	v.spare = v.main
	v.main = append(merged, add[j:]...)
	v.pending = v.pending[:0]
	v.dead = v.dead[:0]
	clear(v.pos)
}

// after calls fn once per member whose last stamp is above pin.
func (v *lastView[ID]) after(pin types.Instant, fn func(ID)) {
	i, _ := slices.BinarySearchFunc(v.main, pin, func(e lastEntry[ID], pin types.Instant) int {
		if e.last <= pin {
			return -1
		}
		return 1
	})
	for _, e := range v.main[i:] {
		if _, over := v.pos[e.id]; !over {
			fn(e.id)
		}
	}
	for _, e := range v.pending {
		if e.last > pin {
			fn(e.id)
		}
	}
}

// isqrt returns floor(sqrt(n)) for n >= 0.
func isqrt(n int) int {
	return int(math.Sqrt(float64(n)))
}

// RowScan collects what a sidecar build reads so that a replaced history row
// is ended by its replacing row (RowInfo.endedBy) the way the write path ends
// it: the rows carrying a value key in full, and of every row of the index's
// type or label (a possible replacing row, whatever it carries) only its ID,
// version, TxFrom and valid-from. Not safe for concurrent use.
type RowScan[ID cmp.Ordered] struct {
	rows []scanRow[ID]
	reps []replacer[ID]
}

type scanRow[ID cmp.Ordered] struct {
	id      ID
	ri      RowInfo
	current bool
	vks     []string
}

// replacer is the part of a row endedBy reads when it may replace another.
type replacer[ID cmp.Ordered] struct {
	id      ID
	txFrom  types.Instant
	from    types.Instant
	version uint32
}

// replacingRowWindow bounds the search for a row's replacing row among the
// next versions of its entity (it is the next one on every write path); a row
// whose replacing row is not found keeps its own span (sound).
const replacingRowWindow = 8

// Add notes one scanned row. vks is retained.
func (s *RowScan[ID]) Add(id ID, ri RowInfo, current bool, vks []string) {
	s.reps = append(s.reps, replacer[ID]{id: id, txFrom: ri.TxFrom, from: ri.Span.From, version: ri.Version})
	if len(vks) > 0 {
		s.rows = append(s.rows, scanRow[ID]{id: id, ri: ri, current: current, vks: vks})
	}
}

// Len reports the rows carrying a value key waiting to be recorded.
func (s *RowScan[ID]) Len() int { return len(s.rows) }

// Into records every collected row carrying a value key into m and empties s.
func (s *RowScan[ID]) Into(m *PropertyTxMembers[ID]) {
	rows, reps := s.rows, s.reps
	s.rows, s.reps = nil, nil
	byIDVersion := func(aID, bID ID, aV, bV uint32) int {
		if c := cmp.Compare(aID, bID); c != 0 {
			return c
		}
		return cmp.Compare(aV, bV)
	}
	slices.SortFunc(reps, func(a, b replacer[ID]) int { return byIDVersion(a.id, b.id, a.version, b.version) })
	for _, r := range rows {
		late := r.ri.Span
		if !r.current && r.ri.replaced() {
			// The first version above r's, then a bounded walk for the row
			// recorded at r's TxTo.
			j, _ := slices.BinarySearchFunc(reps, r, func(e replacer[ID], r scanRow[ID]) int {
				if c := byIDVersion(e.id, r.id, e.version, r.ri.Version); c != 0 {
					return c
				}
				return -1 // equal version sorts before: start above it
			})
			for k := j; k < len(reps) && k < j+replacingRowWindow && reps[k].id == r.id; k++ {
				if reps[k].txFrom == r.ri.TxTo && reps[k].version > r.ri.Version {
					late = r.ri.endedBy(RowInfo{Version: reps[k].version, TxFrom: reps[k].txFrom, Span: Span{From: reps[k].from}})
					break
				}
			}
		}
		for _, vk := range r.vks {
			m.update(vk, r.id, func(p *txPosting) {
				p.noteTx(r.ri.TxFrom, r.ri.last())
				p.addAll(r.ri.anyPinSpan())
				if !r.current {
					p.addLate(late)
					return
				}
				p.unite(txPosting{tx: p.tx, last: p.last, cur: r.ri.Span, curVer: r.ri.Version, flags: postCur})
			})
		}
	}
}
