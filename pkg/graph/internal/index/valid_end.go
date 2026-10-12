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
// store.RelValidEndCapability / NodeValidEndCapability): per entity an upper
// bound on its effective valid end and the newest transaction stamp of its
// rows, ordered per type or label token by each, so a scan bounded by valid
// end lists the entities that may still be effective at or after a valid
// instant without touching the retired ones.
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
// tombstone version, a current row replaced while it carried a delete, and
// any write during a build move the entity to "unknown": listed by every
// query until the owning store re-reads its rows (ResolveUnknown). Refine
// lowers a bound to the exact end the caller read; the next write to the
// entity re-reads its rows (the refined bound is no longer a fold).
//
// RAM per entity: a 32-byte entry in a map keyed by ID, two 16-byte ordered
// entries per type or label it ever carried, a token list for a node with
// more than one label. Safe for concurrent use; the owning store decides when
// it is built and feeds it every row write after that.
type ValidEndIndex struct {
	active   atomic.Bool // built or building: notes are folded or recorded
	mu       sync.Mutex
	built    bool
	building bool
	ents     map[int64]veEnt
	extra    map[int64][]uint16 // tokens beyond veEnt.tok (nodes with several labels)
	toks     map[uint16]*veToken
	unknown  map[int64]uint64 // entity -> the seq at which it last became or stayed unknown
	seq      uint64
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

// ValidEndRowOfRel describes relationship row r (hist: a history row).
func ValidEndRowOfRel(r *types.Relationship, hist bool) ValidEndRow {
	row := ValidEndRow{Hist: hist, Version: r.Version()}
	if tm := r.Temporal(); tm != nil {
		row.TxFrom, row.TxTo, row.ValidTo, row.DeletedAt = tm.TxFrom, tm.TxTo, tm.ValidTo, tm.DeletedAt
	}
	if tok := r.TypeToken().Value(); tok != 0 {
		row.Tokens = []uint16{tok}
	}
	return row
}

// ValidEndRowOfNode describes node row n (hist: a history row).
func ValidEndRowOfNode(n *types.Node, hist bool) ValidEndRow {
	row := ValidEndRow{Hist: hist, Version: n.Version()}
	if tm := n.Temporal(); tm != nil {
		row.TxFrom, row.TxTo, row.ValidTo, row.DeletedAt = tm.TxFrom, tm.TxTo, tm.ValidTo, tm.DeletedAt
	}
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
// token, and flags.
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

// Active reports whether the index is built or building, i.e. whether row
// writes must be noted (a lock-free check for the write paths).
func (ix *ValidEndIndex) Active() bool { return ix.active.Load() }

// Built reports whether the index answers (its build finished).
func (ix *ValidEndIndex) Built() bool {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	return ix.built
}

// BeginBuild starts a build: the index is emptied and every row write noted
// from now on moves its entity to unknown until EndBuild (the build's scan may
// or may not see that row). The caller then feeds every stored row through
// BuildRow (or every entity through Install) and calls EndBuild.
func (ix *ValidEndIndex) BeginBuild() {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	ix.built, ix.building = false, true
	ix.active.Store(true)
	ix.ents = make(map[int64]veEnt)
	ix.extra = make(map[int64][]uint16)
	ix.toks = make(map[uint16]*veToken)
	ix.unknown = make(map[int64]uint64)
}

// EndBuild makes a build's index answer; rows noted from now on are folded.
func (ix *ValidEndIndex) EndBuild() {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	if ix.building {
		ix.built, ix.building = true, false
	}
}

// Reset drops the index (Clear, a failed build); the owning store rebuilds it
// on its next use.
func (ix *ValidEndIndex) Reset() {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	ix.built, ix.building = false, false
	ix.active.Store(false)
	ix.ents, ix.extra, ix.toks, ix.unknown = nil, nil, nil, nil
}

// MarkUnknown moves id to unknown (a removed row, an undecodable one, an
// overlay row a build cannot order). No-op while the index neither answers
// nor builds.
func (ix *ValidEndIndex) MarkUnknown(id int64) {
	if !ix.active.Load() {
		return
	}
	ix.mu.Lock()
	defer ix.mu.Unlock()
	if ix.built || ix.building {
		ix.markUnknownLocked(id)
	}
}

// NoteRow folds a row write of id (a SET of its current row or of a history
// version). No-op while the index neither answers nor builds.
func (ix *ValidEndIndex) NoteRow(id int64, r ValidEndRow) {
	if !ix.active.Load() {
		return
	}
	ix.mu.Lock()
	defer ix.mu.Unlock()
	if !ix.built {
		if ix.building {
			ix.markUnknownLocked(id)
		}
		return
	}
	if _, unknown := ix.unknown[id]; unknown {
		ix.markUnknownLocked(id)
		return
	}
	e, ok := ix.ents[id]
	if ok && (e.flags&veRefined != 0 ||
		(r.Hist && e.flags&veHasDeath != 0 && r.Version <= e.deathVer) ||
		(!r.Hist && e.flags&veCurDeath != 0)) {
		ix.markUnknownLocked(id)
		return
	}
	ix.foldLocked(id, e, ok, r)
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
	if !ix.built {
		if ix.building {
			ix.markUnknownLocked(id)
		}
		return
	}
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
	ix.ents[id] = e
}

// BuildRow folds one stored row into a running build (rows of one entity
// ascending by history version, the current row last, give the tightest
// bound; any order is sound). An entity a concurrent write moved to unknown
// is skipped.
func (ix *ValidEndIndex) BuildRow(id int64, r ValidEndRow) {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	if !ix.building {
		return
	}
	if _, unknown := ix.unknown[id]; unknown {
		return
	}
	e, ok := ix.ents[id]
	ix.foldLocked(id, e, ok, r)
}

// Install replaces id's state with the fold of rows, all its stored rows
// (none: id is gone). The owning store calls it under its own write lock (a
// build that holds every writer out) or through ResolveUnknown.
func (ix *ValidEndIndex) Install(id int64, rows []ValidEndRow) {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	if !ix.built && !ix.building {
		return
	}
	ix.installLocked(id, rows)
}

func (ix *ValidEndIndex) installLocked(id int64, rows []ValidEndRow) {
	if e, ok := ix.ents[id]; ok {
		ix.dropLocked(id, e)
	}
	delete(ix.unknown, id)
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
		ix.foldLocked(id, e, ok, r)
		e, ok = ix.ents[id]
	}
}

// ResolveUnknown re-reads the rows of every unknown entity (read returns all
// of id's stored rows; none when it is gone) and installs their fold, unless
// a write noted id again meanwhile (it then stays unknown and is listed).
// read runs without the index lock.
func (ix *ValidEndIndex) ResolveUnknown(read func(id int64) ([]ValidEndRow, error)) error {
	ix.mu.Lock()
	if !ix.built || len(ix.unknown) == 0 {
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
		if cur, still := ix.unknown[id]; still && cur == s && ix.built {
			ix.installLocked(id, rows)
		}
		ix.mu.Unlock()
	}
	return nil
}

// Candidates lists, for the given tokens, the entities whose bound is at or
// above validFrom or whose stamp is above pin, each once, plus every unknown
// entity (Stamp and Bound store.ValidEndOpen: never refined). nil before the
// build finished.
func (ix *ValidEndIndex) Candidates(tokens []uint16, pin, validFrom types.Instant) []storepkg.ValidEndCandidate {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	if !ix.built {
		return nil
	}
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
	if !ix.built {
		return
	}
	e, ok := ix.ents[id]
	if !ok || uint64(e.gen) != gen || int64(end) >= e.bound {
		return
	}
	if _, unknown := ix.unknown[id]; unknown {
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

// ValidEndStats is the size of a ValidEndIndex: entities bounded, entities
// unknown, ordered entries, and the RAM those structures hold (map entries
// estimated at their key and value size, chunks at capacity).
type ValidEndStats struct {
	Built    bool
	Entities int
	Unknown  int
	Entries  int
	Bytes    int
}

// Stats reports the index's size.
func (ix *ValidEndIndex) Stats() ValidEndStats {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	st := ValidEndStats{Built: ix.built, Entities: len(ix.ents), Unknown: len(ix.unknown)}
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

// markUnknownLocked drops id's state and lists it as unknown under a fresh
// seq.
func (ix *ValidEndIndex) markUnknownLocked(id int64) {
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

// foldLocked folds row r into id's state e (exists: e is its current state)
// and moves its ordered entries.
func (ix *ValidEndIndex) foldLocked(id int64, e veEnt, exists bool, r ValidEndRow) {
	old := e
	if !exists {
		e = veEnt{bound: int64(storepkg.ValidEndNone)}
	}
	own := int64(r.ValidTo)
	if own == 0 {
		own = int64(storepkg.ValidEndOpen)
	}
	e.bound = max(e.bound, own)
	if d := int64(r.DeletedAt); d != 0 {
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
		if tok == 0 || ix.hasTokenLocked(id, e, tok) {
			continue
		}
		if e.tok == 0 {
			e.tok = tok
		} else {
			ix.extra[id] = append(ix.extra[id], tok)
		}
		t := ix.toks[tok]
		if t == nil {
			t = &veToken{}
			ix.toks[tok] = t
		}
		t.byBound.insert(veKey{e.bound, id})
		t.byStamp.insert(veKey{e.stamp, id})
	}
	ix.ents[id] = e
}
