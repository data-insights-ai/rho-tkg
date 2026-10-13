package index

import (
	"cmp"
	"slices"
	"sync"
	"sync/atomic"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// ValidEndIndex is one entity kind's valid-end index (backlog 51,
// store.RelValidEndCapability / NodeValidEndCapability): for the type or
// label tokens a bounded scan asked for (Track), per entity that ever carried
// one of them an upper bound on its effective valid end and the newest
// transaction stamp of its rows, ordered per token by each, so a scan bounded
// by valid end lists the entities that may still be effective at or after a
// valid instant without touching the retired ones. Entities of tokens nobody
// asked for are not held, and their writes change nothing.
//
// The bound is folded from rows, never from the chain resolver (which stays
// the authority — the caller reads each candidate's timeline): every row's
// own ValidTo (0: open) is an upper bound on any interval the resolver gives
// it, and a delete tombstone d recorded when every earlier stamp of the
// entity was at or below d ends every earlier row at or before d (lifeEnds:
// a row's life ends at the first delete recorded at or after it), so the
// bound drops to d. A tombstone that arrives out of that order (an import, a
// replica) leaves the bound as it was: looser, never unsound. The stamp makes
// the bound usable only at a pin at or above it; an entity written after the
// pin is always listed.
//
// Removing a row can raise the truth (a removed tombstone revives its life),
// and an overwritten row may have held the tombstone the bound relies on, so
// a removal, an overwrite of a history version at or below the highest
// tombstone version, and a current row replaced while it carried a delete
// move the entity to "unknown": listed by every query until the owning store
// re-reads its rows (ResolveUnknown). Tracking a token enlists its members as
// unknown, so its first query re-reads them. Refine lowers a bound to the
// exact end the caller read; the next write to the entity re-reads its rows
// (the refined bound is no longer a fold).
//
// RAM per tracked entity: a 32-byte entry in a map keyed by ID, two 16-byte
// ordered entries per tracked token it carried, a token list for a node with
// more than one tracked label. Safe for concurrent use.
type ValidEndIndex struct {
	active  atomic.Bool // a token is tracked: row writes must be noted
	mu      sync.Mutex
	ents    map[int64]veEnt
	extra   map[int64][]uint16 // tracked tokens beyond veEnt.tok (nodes with several labels)
	toks    map[uint16]*veToken
	unknown map[int64]uint64 // entity -> the seq at which it last became or stayed unknown
	seq     uint64
	notes   uint64 // row writes that changed the index (Stats)
}

// ValidEndRow is one stored row as a ValidEndIndex folds it: a history row
// (Hist, its Version) or the current row, its stamps and valid end, and the
// tokens it carries (a relationship's type, a node's labels; not retained).
type ValidEndRow struct {
	Hist                             bool
	Version                          uint32
	TxFrom, TxTo, ValidTo, DeletedAt types.Instant
	Tokens                           []uint16
}

// ValidEndRowOf describes a row by its kind, version and temporal block (nil:
// no stamps); the tokens are the caller's.
func ValidEndRowOf(hist bool, version uint32, tm *types.TemporalMetadata) ValidEndRow {
	row := ValidEndRow{Hist: hist, Version: version}
	if tm != nil {
		row.TxFrom, row.TxTo, row.ValidTo, row.DeletedAt = tm.TxFrom, tm.TxTo, tm.ValidTo, tm.DeletedAt
	}
	return row
}

// ValidEndRowOfRel describes relationship row r (hist: a history row).
func ValidEndRowOfRel(r *types.Relationship, hist bool) ValidEndRow {
	row := ValidEndRowOf(hist, r.Version(), r.Temporal())
	if tok := r.TypeToken().Value(); tok != 0 {
		row.Tokens = []uint16{tok}
	}
	return row
}

// ValidEndRowOfNode describes node row n (hist: a history row).
func ValidEndRowOfNode(n *types.Node, hist bool) ValidEndRow {
	row := ValidEndRowOf(hist, n.Version(), n.Temporal())
	if k := n.LabelTokenCount(); k > 0 {
		row.Tokens = make([]uint16, 0, k)
		for i := 0; i < k; i++ {
			if tok := n.LabelTokenRawAt(i); tok != 0 {
				row.Tokens = append(row.Tokens, tok)
			}
		}
	}
	return row
}

// veEnt is one entity's state: bound (store.ValidEndOpen: unbounded,
// store.ValidEndNone: no segment, after Refine) and stamp, the write
// generation, the highest history version holding a tombstone, its first
// tracked token, and flags.
type veEnt struct {
	bound, stamp int64
	gen          uint32
	deathVer     uint32
	tok          uint16
	flags        uint8
}

const (
	veHasDeath uint8 = 1 << iota // a history row with a delete was folded (deathVer)
	veCurDeath                   // the current row carried a delete
	veRefined                    // bound is Refine's exact end, not the fold
)

type veToken struct {
	byBound, byStamp keyOrderedIDs
}

// Active reports whether a token is tracked, i.e. whether row writes must be
// noted (a lock-free check for the write paths).
func (ix *ValidEndIndex) Active() bool { return ix.active.Load() }

// Tracked reports whether tok is tracked.
func (ix *ValidEndIndex) Tracked(tok uint16) bool {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	return ix.toks[tok] != nil
}

// TrackedTokens returns the tracked tokens.
func (ix *ValidEndIndex) TrackedTokens() []uint16 {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	out := make([]uint16, 0, len(ix.toks))
	for tok := range ix.toks {
		out = append(out, tok)
	}
	return out
}

// Track starts tracking tok and enlists members — every entity that ever
// carried it, from the owning store's membership sidecar — as unknown, re-read
// by the next ResolveUnknown; false when tok was tracked already (members not
// called). The owning store calls it while no row write is in flight, so
// every write after it is noted.
func (ix *ValidEndIndex) Track(tok uint16, members func(enlist func(id int64))) bool {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	if ix.toks[tok] != nil {
		return false
	}
	if ix.toks == nil {
		ix.ents = make(map[int64]veEnt)
		ix.extra = make(map[int64][]uint16)
		ix.toks = make(map[uint16]*veToken)
		ix.unknown = make(map[int64]uint64)
	}
	ix.toks[tok] = &veToken{}
	ix.active.Store(true)
	members(ix.markUnknownLocked)
	return true
}

// Reset drops the index (Clear); tokens are tracked again on their next query.
func (ix *ValidEndIndex) Reset() {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	ix.active.Store(false)
	ix.ents, ix.extra, ix.toks, ix.unknown = nil, nil, nil, nil
}

// Has reports whether id is held or unknown: a row write of id must be noted.
func (ix *ValidEndIndex) Has(id int64) bool {
	if !ix.active.Load() {
		return false
	}
	ix.mu.Lock()
	defer ix.mu.Unlock()
	if _, ok := ix.ents[id]; ok {
		return true
	}
	_, ok := ix.unknown[id]
	return ok
}

// NoteRemoved notes that rows of id were removed (or overwritten unseen): a
// held or unknown entity moves to unknown under a fresh seq; an entity the
// index does not hold is ignored.
func (ix *ValidEndIndex) NoteRemoved(id int64) {
	if !ix.active.Load() {
		return
	}
	ix.mu.Lock()
	defer ix.mu.Unlock()
	if ix.heldLocked(id) {
		ix.markUnknownLocked(id)
	}
}

// NoteRow folds a row write of id (a SET of its current row or of a history
// version). An entity the index does not hold is taken in when the row
// carries a tracked token, ignored otherwise.
func (ix *ValidEndIndex) NoteRow(id int64, r ValidEndRow) {
	if !ix.active.Load() {
		return
	}
	ix.mu.Lock()
	defer ix.mu.Unlock()
	if _, unknown := ix.unknown[id]; unknown {
		ix.markUnknownLocked(id)
		return
	}
	e, ok := ix.ents[id]
	if !ok {
		if ix.carriesTrackedLocked(r.Tokens) {
			ix.foldLocked(id, e, false, r, true)
		}
		return
	}
	if e.flags&veRefined != 0 ||
		(r.Hist && e.flags&veHasDeath != 0 && r.Version <= e.deathVer) ||
		(!r.Hist && e.flags&veCurDeath != 0) {
		ix.markUnknownLocked(id)
		return
	}
	ix.foldLocked(id, e, true, r, true)
}

// NoteMember notes a row that may make id a member of a token, from a store
// door that does not say whether it writes a current or a history row (the
// row's own write is noted by NoteRow, with its kind, right after). It folds
// the row's own end and stamps but no delete cap — it can only loosen the
// bound — and takes the entity in when the row carries a tracked token.
func (ix *ValidEndIndex) NoteMember(id int64, r ValidEndRow) {
	if !ix.active.Load() {
		return
	}
	ix.mu.Lock()
	defer ix.mu.Unlock()
	if _, unknown := ix.unknown[id]; unknown {
		ix.markUnknownLocked(id)
		return
	}
	e, ok := ix.ents[id]
	switch {
	case ok && e.flags&veRefined != 0:
		ix.markUnknownLocked(id)
	case ok || ix.carriesTrackedLocked(r.Tokens):
		ix.foldLocked(id, e, ok, r, false)
	}
}

// NoteCurrentDelete notes the removal of id's current row. Removing a row
// without a delete keeps the fold an upper bound (the remaining rows' own
// ends and tombstones still bound them); removing one that carried a delete,
// or the row a refined bound was read with, does not.
func (ix *ValidEndIndex) NoteCurrentDelete(id int64) {
	if !ix.active.Load() {
		return
	}
	ix.mu.Lock()
	defer ix.mu.Unlock()
	if _, unknown := ix.unknown[id]; unknown {
		ix.markUnknownLocked(id)
		return
	}
	e, ok := ix.ents[id]
	if !ok {
		return
	}
	if e.flags&(veCurDeath|veRefined) != 0 {
		ix.markUnknownLocked(id)
		return
	}
	e.gen++
	ix.notes++
	ix.ents[id] = e
}

// Install replaces id's state with the fold of rows, all its stored rows,
// keeping the tracked tokens they carry (none, or no rows: id is dropped).
func (ix *ValidEndIndex) Install(id int64, rows []ValidEndRow) {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	if ix.toks == nil {
		return
	}
	ix.installLocked(id, rows)
}

func (ix *ValidEndIndex) installLocked(id int64, rows []ValidEndRow) {
	if e, ok := ix.ents[id]; ok {
		ix.dropLocked(id, e)
	}
	delete(ix.unknown, id)
	tracked := false
	for _, r := range rows {
		tracked = tracked || ix.carriesTrackedLocked(r.Tokens)
	}
	if !tracked {
		return
	}
	rows = slices.Clone(rows)
	slices.SortStableFunc(rows, func(a, b ValidEndRow) int {
		if a.Hist != b.Hist {
			if a.Hist {
				return -1
			}
			return 1
		}
		return cmp.Compare(a.Version, b.Version)
	})
	var (
		e  veEnt
		ok bool
	)
	for _, r := range rows {
		ix.foldLocked(id, e, ok, r, true)
		e, ok = ix.ents[id]
	}
}

// ResolveUnknown re-reads the rows of every unknown entity (read returns all
// of id's stored rows; none when it is gone) and installs their fold, unless
// a write noted id again meanwhile (it then stays unknown and is listed).
// read runs without the index lock.
func (ix *ValidEndIndex) ResolveUnknown(read func(id int64) ([]ValidEndRow, error)) error {
	ix.mu.Lock()
	if len(ix.unknown) == 0 {
		ix.mu.Unlock()
		return nil
	}
	pending := make(map[int64]uint64, len(ix.unknown))
	for id, s := range ix.unknown {
		pending[id] = s
	}
	ix.mu.Unlock()
	for id, s := range pending {
		rows, err := read(id)
		if err != nil {
			return err
		}
		ix.mu.Lock()
		if cur, still := ix.unknown[id]; still && cur == s {
			ix.installLocked(id, rows)
		}
		ix.mu.Unlock()
	}
	return nil
}

// ResolveUnknownBulk is ResolveUnknown with one read for every unknown
// entity: read returns the stored rows of each id it found (an id it omits is
// gone).
func (ix *ValidEndIndex) ResolveUnknownBulk(read func(ids []int64) (map[int64][]ValidEndRow, error)) error {
	ix.mu.Lock()
	if len(ix.unknown) == 0 {
		ix.mu.Unlock()
		return nil
	}
	pending := make(map[int64]uint64, len(ix.unknown))
	ids := make([]int64, 0, len(ix.unknown))
	for id, s := range ix.unknown {
		pending[id] = s
		ids = append(ids, id)
	}
	ix.mu.Unlock()
	rows, err := read(ids)
	if err != nil {
		return err
	}
	ix.mu.Lock()
	defer ix.mu.Unlock()
	for id, s := range pending {
		if cur, still := ix.unknown[id]; still && cur == s {
			ix.installLocked(id, rows[id])
		}
	}
	return nil
}

// Candidates lists, for the given tracked tokens, the entities whose bound is
// at or above validFrom or whose stamp is above pin, each once, plus every
// unknown entity (Stamp and Bound store.ValidEndOpen: never refined).
func (ix *ValidEndIndex) Candidates(tokens []uint16, pin, validFrom types.Instant) []storepkg.ValidEndCandidate {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	var out []storepkg.ValidEndCandidate
	seen := make(map[int64]struct{})
	add := func(k veKey) bool {
		if _, dup := seen[k.id]; dup {
			return true
		}
		seen[k.id] = struct{}{}
		e := ix.ents[k.id]
		out = append(out, storepkg.ValidEndCandidate{ID: k.id, Gen: uint64(e.gen), Stamp: types.Instant(e.stamp), Bound: types.Instant(e.bound)})
		return true
	}
	for _, tok := range tokens {
		t := ix.toks[tok]
		if t == nil {
			continue
		}
		t.byBound.ascendFrom(int64(validFrom), add)
		if pin < storepkg.ValidEndOpen {
			t.byStamp.ascendFrom(int64(pin)+1, add)
		}
	}
	for id := range ix.unknown {
		if _, dup := seen[id]; !dup {
			seen[id] = struct{}{}
			out = append(out, storepkg.ValidEndCandidate{ID: id, Stamp: storepkg.ValidEndOpen, Bound: storepkg.ValidEndOpen})
		}
	}
	return out
}

// Refine lowers id's bound to end, its exact effective valid end read at a
// pin at or above its stamp, if gen is still its generation (no write since
// it was listed).
func (ix *ValidEndIndex) Refine(id int64, gen uint64, end types.Instant) {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	e, ok := ix.ents[id]
	if !ok || uint64(e.gen) != gen || int64(end) >= e.bound {
		return
	}
	old := e
	e.bound = int64(end)
	e.flags |= veRefined
	ix.ents[id] = e
	ix.forTokensLocked(id, e, func(t *veToken) {
		t.byBound.remove(veKey{old.bound, id})
		t.byBound.insert(veKey{e.bound, id})
	})
}

// Stats reports the index's size.
func (ix *ValidEndIndex) Stats() storepkg.ValidEndStats {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	st := storepkg.ValidEndStats{Tokens: len(ix.toks), Entities: len(ix.ents), Unknown: len(ix.unknown), Notes: ix.notes}
	st.Bytes = len(ix.ents)*(8+32) + len(ix.unknown)*16
	for _, toks := range ix.extra {
		st.Bytes += 8 + 24 + 2*cap(toks)
	}
	for _, t := range ix.toks {
		st.Entries += t.byBound.len() + t.byStamp.len()
		st.Bytes += t.byBound.capBytes() + t.byStamp.capBytes()
	}
	return st
}

// heldLocked reports whether id is held or unknown.
func (ix *ValidEndIndex) heldLocked(id int64) bool {
	if _, ok := ix.ents[id]; ok {
		return true
	}
	_, ok := ix.unknown[id]
	return ok
}

// markUnknownLocked drops id's state and lists it as unknown under a fresh
// seq.
func (ix *ValidEndIndex) markUnknownLocked(id int64) {
	ix.notes++
	if e, ok := ix.ents[id]; ok {
		ix.dropLocked(id, e)
	}
	ix.seq++
	ix.unknown[id] = ix.seq
}

// dropLocked removes id's entry and its ordered entries.
func (ix *ValidEndIndex) dropLocked(id int64, e veEnt) {
	ix.forTokensLocked(id, e, func(t *veToken) {
		t.byBound.remove(veKey{e.bound, id})
		t.byStamp.remove(veKey{e.stamp, id})
	})
	delete(ix.ents, id)
	delete(ix.extra, id)
}

// forTokensLocked calls fn with the ordered sets of every token id carries.
func (ix *ValidEndIndex) forTokensLocked(id int64, e veEnt, fn func(*veToken)) {
	if e.tok == 0 {
		return
	}
	fn(ix.toks[e.tok])
	for _, tok := range ix.extra[id] {
		fn(ix.toks[tok])
	}
}

func (ix *ValidEndIndex) hasTokenLocked(id int64, e veEnt, tok uint16) bool {
	return e.tok == tok || slices.Contains(ix.extra[id], tok)
}

func (ix *ValidEndIndex) carriesTrackedLocked(toks []uint16) bool {
	return slices.ContainsFunc(toks, func(tok uint16) bool { return ix.toks[tok] != nil })
}

// foldLocked folds row r into id's state e (exists: e is its current state),
// moves its ordered entries and adds the tracked tokens r carries. capDeath
// false folds a delete as a stamp only (NoteMember: the row's kind is
// unknown). Callers fold an entity only when one of its rows carries a
// tracked token (Install folds a member's earlier rows before that one).
func (ix *ValidEndIndex) foldLocked(id int64, e veEnt, exists bool, r ValidEndRow, capDeath bool) {
	old := e
	if !exists {
		e = veEnt{bound: int64(storepkg.ValidEndNone)}
	}
	own := int64(r.ValidTo)
	if own == 0 {
		own = int64(storepkg.ValidEndOpen)
	}
	e.bound = max(e.bound, own)
	if d := int64(r.DeletedAt); d != 0 && capDeath {
		if e.stamp <= d && int64(r.TxFrom) <= d {
			e.bound = min(e.bound, d)
		}
		if r.Hist {
			e.flags |= veHasDeath
			e.deathVer = max(e.deathVer, r.Version)
		} else {
			e.flags |= veCurDeath
		}
	}
	e.stamp = max(e.stamp, int64(r.TxFrom), int64(r.TxTo), int64(r.DeletedAt))
	e.gen++
	ix.notes++
	if exists && (old.bound != e.bound || old.stamp != e.stamp) {
		ix.forTokensLocked(id, old, func(t *veToken) {
			if old.bound != e.bound {
				t.byBound.remove(veKey{old.bound, id})
				t.byBound.insert(veKey{e.bound, id})
			}
			if old.stamp != e.stamp {
				t.byStamp.remove(veKey{old.stamp, id})
				t.byStamp.insert(veKey{e.stamp, id})
			}
		})
	}
	for _, tok := range r.Tokens {
		t := ix.toks[tok]
		if t == nil || ix.hasTokenLocked(id, e, tok) {
			continue
		}
		if e.tok == 0 {
			e.tok = tok
		} else {
			ix.extra[id] = append(ix.extra[id], tok)
		}
		t.byBound.insert(veKey{e.bound, id})
		t.byStamp.insert(veKey{e.stamp, id})
	}
	ix.ents[id] = e
}
