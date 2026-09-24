package memory

import (
	"errors"
	"fmt"
	"slices"
	"sync"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/segment"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Column segments for declared bulk relationship types — ADR-0011 step S2.
//
// A declared type's current rows live in one of two places:
//
//   - the memtable: the ordinary row store (rels, typeIdx, outIdx, inIdx),
//     where every write lands exactly as for an undeclared type;
//   - in-RAM segments (the S1 codec): immutable column bytes. Sealing moves
//     rows from the memtable into a new segment and out of rels, typeIdx,
//     outIdx and inIdx. The stats maps and the ID-keyed indexes (property,
//     temporal) keep describing the same logical rows, so a seal changes none
//     of them; no change-log record is written (a seal is a physical move,
//     ADR-0011 §4.8).
//
// Overlay rule (ADR-0011 §4.1, §4.5, lesson 57): a sealed row is current iff
// its ID is not in segDead. Every write door that targets an existing row
// first FAULTS IN a sealed row — decodes it back into the memtable and marks
// the sealed copy dead — and then runs its unchanged row-store logic. So an
// update writes the new version to the memtable (the newest layer wins) and a
// delete removes the faulted-in row; the dead sealed copy is never current
// again (S4 compaction folds it away). A faulted-in ID is never sealed again
// in S2, so an ID has at most one sealed row and the union is exact:
//
//	current rows = rels ∪ { sealed rows whose ID ∉ segDead }
//
// Every read door evaluates that union (per key where it looks up by ID, in
// bulk where it scans a type). Nothing here runs unless a type is declared:
// an undeclared store keeps today's code path byte for byte.

// segType is one declared type's state. Guarded by ms.mu, except sealMu.
type segType struct {
	decl          storecontract.RelSegmentDeclaration
	schema        segment.Schema
	dicts         *segment.Dicts // the store's node dictionary + this type's string-column dictionaries
	segs          []*memSeg
	sealedRows    int64
	deadRows      int64
	segBytes      int64
	unsealedBytes int64
	seals         uint64
	maxID         types.RelID
	// refused holds memtable rows (by pointer: a replaced row is a new
	// pointer and becomes eligible again) that the codec refused to seal —
	// no stored hash, or a stored hash its content does not reproduce. They
	// stay in the memtable, answered exactly as before.
	refused map[*types.Relationship]struct{}
	// sealMu serializes seals of this type; it is taken without ms.mu held.
	sealMu sync.Mutex
}

// memSeg is one sealed in-RAM segment.
type memSeg struct {
	seg    *segment.Segment
	lo, hi types.RelID
	bytes  int
}

var _ storecontract.RelSegmentCapability = (*Store)(nil)

// maxSealRetries bounds how many refused rows one seal drops and re-encodes
// around before it gives up and leaves every row in the memtable.
const maxSealRetries = 16

func segColumnsOf(cols []storecontract.SegmentColumn) []segment.Column {
	out := make([]segment.Column, len(cols))
	for i, c := range cols {
		out[i] = segment.Column{Name: c.Name, Kind: segment.Kind(c.Kind)}
	}
	return out
}

func sameDecl(a, b storecontract.RelSegmentDeclaration) bool {
	return a.TypeName == b.TypeName && a.TypeToken == b.TypeToken && a.IntegrityBlockRows == b.IntegrityBlockRows &&
		slices.Equal(a.Columns, b.Columns)
}

// DeclareRelSegment implements store.RelSegmentCapability.
func (ms *Store) DeclareRelSegment(d storecontract.RelSegmentDeclaration) error {
	if ms == nil {
		return ErrNilStore
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if err := ms.checkOpenLocked(); err != nil {
		return err
	}
	if err := storecontract.ValidateRelSegmentSpec(storecontract.RelSegmentSpec{
		Type: d.TypeName, Columns: d.Columns, IntegrityBlockRows: d.IntegrityBlockRows,
	}); err != nil {
		return err
	}
	if d.MemtableBudget < 0 {
		return fmt.Errorf("%w: memtable budget %d", storecontract.ErrRelSegmentDeclaration, d.MemtableBudget)
	}
	schema := segment.Schema{TypeName: d.TypeName, TypeToken: d.TypeToken, Columns: segColumnsOf(d.Columns)}
	if err := segment.ValidateSchema(schema); err != nil {
		return fmt.Errorf("%w: %v", storecontract.ErrRelSegmentDeclaration, err)
	}
	budget := d.MemtableBudget
	if budget == 0 {
		budget = storecontract.DefaultSegmentMemtableBudget
	}
	if ms.segBudget != 0 && d.MemtableBudget != 0 && budget != ms.segBudget {
		return fmt.Errorf("%w: memtable budget %d, already %d", storecontract.ErrRelSegmentDeclaration, budget, ms.segBudget)
	}
	for tok, st := range ms.segTypes {
		if tok == d.TypeToken {
			if sameDecl(st.decl, d) {
				return nil
			}
			return fmt.Errorf("%w: type token %d is already declared differently", storecontract.ErrRelSegmentDeclaration, tok)
		}
		if st.decl.TypeName == d.TypeName {
			return fmt.Errorf("%w: type %q is already declared under token %d", storecontract.ErrRelSegmentDeclaration, d.TypeName, tok)
		}
	}
	if ms.segBudget == 0 {
		ms.segBudget = budget
	}
	st := &segType{decl: d, schema: schema}
	st.decl.Columns = slices.Clone(d.Columns)
	if ms.segTypes == nil {
		ms.segTypes = make(map[uint16]*segType)
		ms.segDead = make(map[types.RelID]uint16)
		ms.segNodeDict = segment.NewNodeDict()
	}
	st.dicts = ms.newSegDictsLocked(st.decl)
	ms.segTypes[d.TypeToken] = st
	// Rows that already exist become the type's unsealed tail.
	for id := range ms.typeIdx[d.TypeToken] {
		if r := ms.rels[id]; r != nil {
			ms.segAccountLocked(r, 1)
		}
	}
	return nil
}

// RelSegmentStats implements store.RelSegmentCapability.
func (ms *Store) RelSegmentStats(typeToken uint16) (storecontract.RelSegmentStats, error) {
	if ms == nil {
		return storecontract.RelSegmentStats{}, ErrNilStore
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if err := ms.checkOpenLocked(); err != nil {
		return storecontract.RelSegmentStats{}, err
	}
	st := ms.segTypes[typeToken]
	if st == nil {
		return storecontract.RelSegmentStats{}, fmt.Errorf("%w: token %d", storecontract.ErrRelSegmentNotDeclared, typeToken)
	}
	return storecontract.RelSegmentStats{
		Segments:       len(st.segs),
		SealedRows:     st.sealedRows,
		LiveSealedRows: st.sealedRows - st.deadRows,
		SegmentBytes:   st.segBytes,
		UnsealedRows:   int64(len(ms.typeIdx[typeToken])),
		UnsealedBytes:  st.unsealedBytes,
		Seals:          st.seals,
	}, nil
}

// SealRelSegments implements store.RelSegmentCapability.
func (ms *Store) SealRelSegments(typeToken uint16) error {
	if ms == nil {
		return ErrNilStore
	}
	return ms.sealType(typeToken, true)
}

// segAccountLocked keeps the unsealed-byte budget: sign +1 when a row of a
// declared type enters rels, -1 when it leaves. Caller holds ms.mu (write).
func (ms *Store) segAccountLocked(r *types.Relationship, sign int64) {
	if len(ms.segTypes) == 0 || r == nil {
		return
	}
	st := ms.segTypes[r.TypeToken().Value()]
	if st == nil {
		return
	}
	if n := len(ms.rels); n > ms.relsPeak {
		ms.relsPeak = n
	}
	b := sign * int64(r.ApproxHeapBytes())
	st.unsealedBytes += b
	ms.segUnsealed += b
	if sign > 0 && ms.segUnsealed >= ms.segBudget+ms.segRefusedBytes {
		ms.segDue.Store(true)
	}
}

// sealIfDue runs a budget-triggered seal after a write door released ms.mu
// (deferred before the lock is taken). One atomic load when nothing is due.
func (ms *Store) sealIfDue() {
	if !ms.segDue.Load() || !ms.segDue.CompareAndSwap(true, false) {
		return
	}
	ms.mu.RLock()
	var tok uint16
	var most int64 = -1
	for t, st := range ms.segTypes {
		if st.unsealedBytes > most {
			tok, most = t, st.unsealedBytes
		}
	}
	ms.mu.RUnlock()
	if most > 0 {
		// A failed budget seal leaves every row in the memtable, which is
		// always correct; the next write over the budget tries again.
		_ = ms.sealType(tok, false)
	}
}

// sealType seals the type's eligible memtable rows into one new segment.
// explicit waits for a running seal of the type; a budget seal skips instead.
//
// The encode runs WITHOUT ms.mu: it reads only the snapshotted rows, which
// are frozen and immutable. The install re-takes the lock and moves a row
// out of the memtable only if rels still holds that exact pointer — a row
// replaced, deleted or faulted in meanwhile is sealed dead from the start
// (the per-row write-generation guard of lesson 63). A Clear in between
// (segEpoch moved) discards the segment.
func (ms *Store) sealType(tok uint16, explicit bool) error {
	ms.mu.RLock()
	if err := ms.checkOpenLocked(); err != nil {
		ms.mu.RUnlock()
		return err
	}
	st := ms.segTypes[tok]
	ms.mu.RUnlock()
	if st == nil {
		return fmt.Errorf("%w: token %d", storecontract.ErrRelSegmentNotDeclared, tok)
	}
	if explicit {
		st.sealMu.Lock()
	} else if !st.sealMu.TryLock() {
		return nil
	}
	defer st.sealMu.Unlock()

	ms.mu.Lock()
	if err := ms.checkOpenLocked(); err != nil {
		ms.mu.Unlock()
		return err
	}
	epoch := ms.segEpoch
	rows := make([]*types.Relationship, 0, len(ms.typeIdx[tok]))
	refused := make(map[*types.Relationship]struct{}, len(st.refused))
	for id := range ms.typeIdx[tok] {
		r := ms.rels[id]
		if r == nil {
			continue
		}
		if _, dead := ms.segDead[id]; dead {
			continue // a faulted-in ID stays in the memtable until S4
		}
		if _, bad := st.refused[r]; bad || !hasStoredHash(r) {
			refused[r] = struct{}{}
			continue
		}
		rows = append(rows, r)
	}
	schema := st.schema
	blockRows := st.decl.IntegrityBlockRows
	dicts := st.dicts
	ms.mu.Unlock()

	var data []byte
	var err error
	for try := 0; len(rows) > 0; try++ {
		data, err = segment.EncodeWithDicts(schema, rows, segment.Options{IntegrityBlockRows: blockRows}, dicts)
		var re *segment.RowError
		if err == nil || try >= maxSealRetries || !errors.As(err, &re) {
			break
		}
		kept := rows[:0]
		for _, r := range rows {
			if r.ID() == re.ID && r.Version() == re.Version {
				refused[r] = struct{}{}
				continue
			}
			kept = append(kept, r)
		}
		rows = kept
	}
	if err != nil {
		rows = nil // give up this seal: everything stays in the memtable
	}
	var seg *segment.Segment
	if len(rows) > 0 {
		if seg, err = segment.OpenWithDicts(data, dicts); err != nil {
			rows = nil
		}
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()
	if cerr := ms.checkOpenLocked(); cerr != nil {
		return cerr
	}
	if ms.segEpoch != epoch || ms.segTypes[tok] != st {
		return nil // Clear ran meanwhile: the rows are gone
	}
	st.refused = refused
	ms.segRefusedBytes = 0
	for _, t := range ms.segTypes {
		for r := range t.refused {
			ms.segRefusedBytes += int64(r.ApproxHeapBytes())
		}
	}
	if len(rows) == 0 {
		if explicit {
			return err
		}
		return nil
	}
	lo, hi := seg.IDRange()
	sg := &memSeg{seg: seg, lo: lo, hi: hi, bytes: len(data)}
	outBefore, inBefore := len(ms.outIdx), len(ms.inIdx)
	for _, r := range rows {
		id := r.ID()
		if cur, ok := ms.rels[id]; ok && cur == r {
			ms.removeRowFromMemtableLocked(r)
			continue
		}
		ms.segDead[id] = tok
		st.deadRows++
	}
	ms.shrinkMemtableMapsLocked(tok, outBefore, inBefore)
	st.segs = append(st.segs, sg)
	st.sealedRows += int64(len(rows))
	st.segBytes += int64(len(data))
	st.seals++
	if hi > st.maxID {
		st.maxID = hi
	}
	if hi > ms.segMaxID {
		ms.segMaxID = hi
	}
	return nil
}

// hasStoredHash mirrors the codec's precondition: a 64-character lowercase
// hex integrity hash.
func hasStoredHash(r *types.Relationship) bool {
	ig := r.Integrity()
	if ig == nil || len(ig.Hash) != 64 {
		return false
	}
	for i := 0; i < len(ig.Hash); i++ {
		c := ig.Hash[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// removeRowFromMemtableLocked moves a sealed row out of rels, typeIdx,
// outIdx and inIdx. Stats, indexes and sidecars are untouched: the logical
// row set did not change.
func (ms *Store) removeRowFromMemtableLocked(r *types.Relationship) {
	id := r.ID()
	tv := r.TypeToken().Value()
	if set := ms.typeIdx[tv]; set != nil {
		delete(set, id)
		if len(set) == 0 {
			delete(ms.typeIdx, tv)
		}
	}
	if set := ms.outIdx[r.StartNodeID()]; set != nil {
		delete(set, id)
		if len(set) == 0 {
			delete(ms.outIdx, r.StartNodeID())
		}
	}
	if set := ms.inIdx[r.EndNodeID()]; set != nil {
		delete(set, id)
		if len(set) == 0 {
			delete(ms.inIdx, r.EndNodeID())
		}
	}
	delete(ms.rels, id)
	ms.segAccountLocked(r, -1)
}

// clearSegmentsLocked drops every segment and overlay entry (Clear); the
// declarations and the budget stay.
func (ms *Store) clearSegmentsLocked() {
	if len(ms.segTypes) == 0 {
		return
	}
	ms.segEpoch++
	ms.segNodeDict = segment.NewNodeDict() // a seal started before Clear keeps (and discards) the old dictionaries
	for _, st := range ms.segTypes {
		st.dicts = ms.newSegDictsLocked(st.decl)
		st.segs, st.sealedRows, st.deadRows, st.segBytes = nil, 0, 0, 0
		st.unsealedBytes, st.seals, st.maxID, st.refused = 0, 0, 0, nil
	}
	ms.segDead = make(map[types.RelID]uint16)
	ms.segUnsealed, ms.segRefusedBytes, ms.segMaxID, ms.relsPeak = 0, 0, 0, 0
	ms.segDue.Store(false)
}

// --- union view helpers (caller holds ms.mu, read or write) ---

// hasSegmentsLocked reports whether type tok has any sealed row.
func (ms *Store) hasSegmentsLocked(tok uint16) bool {
	st := ms.segTypes[tok]
	return st != nil && len(st.segs) > 0
}

// anySegmentsLocked reports whether any declared type has a sealed row.
func (ms *Store) anySegmentsLocked() bool {
	return ms.segMaxID != 0
}

// sealedLocked locates the CURRENT sealed row of id.
func (ms *Store) sealedLocked(id types.RelID) (*memSeg, int, bool, error) {
	if ms.segMaxID == 0 || id > ms.segMaxID || id <= 0 {
		return nil, 0, false, nil
	}
	if _, dead := ms.segDead[id]; dead {
		return nil, 0, false, nil
	}
	for _, st := range ms.segTypes {
		if id > st.maxID {
			continue
		}
		for _, sg := range st.segs {
			if id < sg.lo || id > sg.hi {
				continue
			}
			rows, err := sg.seg.Lookup(id)
			if err != nil {
				return nil, 0, false, err
			}
			if len(rows) > 0 {
				return sg, rows[0], true, nil
			}
		}
	}
	return nil, 0, false, nil
}

// relLocked returns the current row of id — the memtable's frozen canonical
// entry, or a freshly decoded, frozen sealed row.
func (ms *Store) relLocked(id types.RelID) (*types.Relationship, bool, error) {
	if r, ok := ms.rels[id]; ok {
		return r, true, nil
	}
	sg, row, ok, err := ms.sealedLocked(id)
	if !ok {
		return nil, false, err
	}
	r, err := sg.seg.Row(row)
	if err != nil {
		return nil, false, err
	}
	r.Freeze()
	return r, true, nil
}

// relExistsLocked reports whether id is a current relationship.
func (ms *Store) relExistsLocked(id types.RelID) (bool, error) {
	if _, ok := ms.rels[id]; ok {
		return true, nil
	}
	_, _, ok, err := ms.sealedLocked(id)
	return ok, err
}

// liveSealedLocked is the number of current sealed rows of all types.
func (ms *Store) liveSealedLocked() int {
	n := int64(0)
	for _, st := range ms.segTypes {
		n += st.sealedRows - st.deadRows
	}
	return int(n)
}

// liveSealedOfLocked is the number of current sealed rows of tok.
func (ms *Store) liveSealedOfLocked(tok uint16) int {
	st := ms.segTypes[tok]
	if st == nil {
		return 0
	}
	return int(st.sealedRows - st.deadRows)
}

// forEachSealedRowLocked calls fn with every current sealed row of st in
// segment order — one page decode (segment.Batch) per 4,096 rows; a dead row
// is skipped before it is built. Rows are frozen.
func (ms *Store) forEachSealedRowLocked(st *segType, fn func(*types.Relationship) bool) error {
	if st == nil {
		return nil
	}
	checkDead := len(ms.segDead) > 0
	for _, sg := range st.segs {
		stop := false
		var rerr error
		err := sg.seg.ScanBatches(func(b *segment.Batch) bool {
			for k := 0; k < b.N; k++ {
				if checkDead {
					if _, dead := ms.segDead[types.RelID(b.IDs[k])]; dead {
						continue
					}
				}
				r, err := b.Row(k)
				if err != nil {
					rerr = err
					return false
				}
				r.Freeze()
				if !fn(r) {
					stop = true
					return false
				}
			}
			return true
		})
		if err == nil {
			err = rerr
		}
		if err != nil {
			return err
		}
		if stop {
			return nil
		}
	}
	return nil
}

// forEachTypeRowLocked calls fn with every current row of tok: memtable rows
// (map order) then sealed rows (segment order).
func (ms *Store) forEachTypeRowLocked(tok uint16, fn func(*types.Relationship) bool) error {
	for id := range ms.typeIdx[tok] {
		if r, ok := ms.rels[id]; ok {
			if !fn(r) {
				return nil
			}
		}
	}
	return ms.forEachSealedRowLocked(ms.segTypes[tok], fn)
}

// forEachCurrentRelLocked calls fn with every current row of every type.
func (ms *Store) forEachCurrentRelLocked(fn func(*types.Relationship) bool) error {
	for _, r := range ms.rels {
		if !fn(r) {
			return nil
		}
	}
	stop := false
	for _, st := range ms.segTypes {
		if err := ms.forEachSealedRowLocked(st, func(r *types.Relationship) bool {
			if !fn(r) {
				stop = true
			}
			return !stop
		}); err != nil {
			return err
		}
		if stop {
			return nil
		}
	}
	return nil
}

// sealedIDRef is a current sealed row located by a snapshot.
type sealedIDRef struct {
	id  types.RelID
	sg  *memSeg
	row int
}

// appendSealedRefsLocked appends every current sealed row of st (ID order
// within each segment).
func (ms *Store) appendSealedRefsLocked(dst []sealedIDRef, st *segType) ([]sealedIDRef, error) {
	if st == nil {
		return dst, nil
	}
	for _, sg := range st.segs {
		err := sg.seg.ForEachID(func(id types.RelID, row int) bool {
			if _, dead := ms.segDead[id]; !dead {
				dst = append(dst, sealedIDRef{id: id, sg: sg, row: row})
			}
			return true
		})
		if err != nil {
			return nil, err
		}
	}
	return dst, nil
}

// typeRefsLocked snapshots every current row of tok as a sealedIDRef (sg nil
// for a memtable row), sorted by ID.
func (ms *Store) typeRefsLocked(tok uint16) ([]sealedIDRef, error) {
	set := ms.typeIdx[tok]
	refs := make([]sealedIDRef, 0, len(set)+ms.liveSealedOfLocked(tok))
	for id := range set {
		refs = append(refs, sealedIDRef{id: id})
	}
	refs, err := ms.appendSealedRefsLocked(refs, ms.segTypes[tok])
	if err != nil {
		return nil, err
	}
	sortRefsByID(refs)
	return refs, nil
}

func sortRefsByID(refs []sealedIDRef) {
	slices.SortFunc(refs, func(a, b sealedIDRef) int {
		switch {
		case a.id < b.id:
			return -1
		case a.id > b.id:
			return 1
		}
		return 0
	})
}

// resolveRefLocked returns the CURRENT row for a snapshotted ref: the
// memtable row if the ID is there now, the snapshotted sealed row if it is
// still current (same segment generation), or a fresh lookup.
func (ms *Store) resolveRefLocked(ref sealedIDRef, epoch uint64) (*types.Relationship, bool, error) {
	if r, ok := ms.rels[ref.id]; ok {
		return r, true, nil
	}
	if ref.sg != nil && epoch == ms.segEpoch {
		if _, dead := ms.segDead[ref.id]; dead {
			return nil, false, nil
		}
		r, err := ref.sg.seg.Row(ref.row)
		if err != nil {
			return nil, false, err
		}
		r.Freeze()
		return r, true, nil
	}
	return ms.relLocked(ref.id)
}

// typeRowsByIDLocked returns tok's current rows with ID > after in ID order,
// kept by keep (nil = all), at most limit (0 = no limit). With a limit it
// walks the ID-sorted snapshot and decodes only what it returns; without one
// it decodes every sealed row page by page and sorts.
func (ms *Store) typeRowsByIDLocked(tok uint16, after types.EntityID, limit int, keep func(*types.Relationship) bool) ([]*types.Relationship, error) {
	var out []*types.Relationship
	if limit > 0 {
		refs, err := ms.typeRefsLocked(tok)
		if err != nil {
			return nil, err
		}
		for _, ref := range refs {
			if types.EntityID(ref.id) <= after {
				continue
			}
			r, ok, err := ms.resolveRefLocked(ref, ms.segEpoch)
			if err != nil {
				return nil, err
			}
			if !ok || !r.HasTypeTokenRaw(tok) || (keep != nil && !keep(r)) {
				continue
			}
			out = append(out, r)
			if len(out) >= limit {
				break
			}
		}
		return out, nil
	}
	out = make([]*types.Relationship, 0, len(ms.typeIdx[tok])+ms.liveSealedOfLocked(tok))
	err := ms.forEachTypeRowLocked(tok, func(r *types.Relationship) bool {
		if types.EntityID(r.ID()) <= after || !r.HasTypeTokenRaw(tok) || (keep != nil && !keep(r)) {
			return true
		}
		out = append(out, r)
		return true
	})
	if err != nil {
		return nil, err
	}
	storepkg.SortRelsByID(out)
	return out, nil
}

// adjacentRowsLocked calls fn with every current row adjacent to nid
// (outgoing, or incoming when incoming is set), optionally of one type:
// memtable rows first, then sealed rows via the segment CSR runs.
func (ms *Store) adjacentRowsLocked(nid types.NodeID, typeToken uint16, incoming bool, fn func(*types.Relationship) bool) error {
	set := ms.outIdx[nid]
	if incoming {
		set = ms.inIdx[nid]
	}
	var typeSet map[types.RelID]struct{}
	if typeToken != 0 {
		typeSet = ms.typeIdx[typeToken]
	}
	for id := range set {
		if typeToken != 0 {
			if _, ok := typeSet[id]; !ok {
				continue
			}
		}
		r, ok := ms.rels[id]
		if !ok {
			continue
		}
		match := relationshipMatchesOutgoing(r, nid, typeToken)
		if incoming {
			match = relationshipMatchesIncoming(r, nid, typeToken)
		}
		if match && !fn(r) {
			return nil
		}
	}
	stop := false
	err := ms.forEachSealedAdjacentLocked(nid, typeToken, incoming, func(sg *memSeg, row int, id types.RelID) (bool, error) {
		r, err := sg.seg.Row(row)
		if err != nil {
			return false, err
		}
		r.Freeze()
		if !fn(r) {
			stop = true
			return false, nil
		}
		return true, nil
	})
	if stop {
		return nil
	}
	return err
}

// forEachSealedAdjacentLocked calls fn with every current sealed row adjacent
// to nid (by position and ID, not yet built).
func (ms *Store) forEachSealedAdjacentLocked(nid types.NodeID, typeToken uint16, incoming bool,
	fn func(sg *memSeg, row int, id types.RelID) (bool, error)) error {
	for tok, st := range ms.segTypes {
		if typeToken != 0 && tok != typeToken {
			continue
		}
		for _, sg := range st.segs {
			var rows []int
			if incoming {
				in, err := sg.seg.InRows(nid)
				if err != nil {
					return err
				}
				rows = in
			} else {
				lo, hi, err := sg.seg.OutRows(nid)
				if err != nil {
					return err
				}
				for i := lo; i < hi; i++ {
					rows = append(rows, i)
				}
			}
			for _, row := range rows {
				id, err := sg.seg.IDAt(row)
				if err != nil {
					return err
				}
				if _, dead := ms.segDead[id]; dead {
					continue
				}
				more, err := fn(sg, row, id)
				if err != nil || !more {
					return err
				}
			}
		}
	}
	return nil
}

// sealedDegreeLocked counts current sealed rows adjacent to nid.
func (ms *Store) sealedDegreeLocked(nid types.NodeID, typeToken uint16, incoming bool) (int, error) {
	n := 0
	err := ms.forEachSealedAdjacentLocked(nid, typeToken, incoming, func(*memSeg, int, types.RelID) (bool, error) {
		n++
		return true, nil
	})
	return n, err
}

// hasAdjacencyLocked reports whether any current relationship touches nid.
func (ms *Store) hasAdjacencyLocked(nid types.NodeID) (bool, error) {
	if len(ms.outIdx[nid]) != 0 || len(ms.inIdx[nid]) != 0 {
		return true, nil
	}
	if !ms.anySegmentsLocked() {
		return false, nil
	}
	for _, incoming := range []bool{false, true} {
		n, err := ms.sealedDegreeLocked(nid, 0, incoming)
		if err != nil || n > 0 {
			return n > 0, err
		}
	}
	return false, nil
}

// --- fault-in (caller holds ms.mu write) ---

// faultInLocked moves id's current sealed row back into the memtable and
// marks the sealed copy dead, so the caller's row-store write logic applies
// unchanged. A no-op when id is not a current sealed row.
func (ms *Store) faultInLocked(id types.RelID) error {
	if _, ok := ms.rels[id]; ok {
		return nil
	}
	sg, row, ok, err := ms.sealedLocked(id)
	if !ok {
		return err
	}
	r, err := sg.seg.Row(row)
	if err != nil {
		return err
	}
	r.Freeze()
	tok := r.TypeToken().Value()
	ms.segDead[id] = tok
	ms.segTypes[tok].deadRows++
	ms.rels[id] = r
	if ms.typeIdx[tok] == nil {
		ms.typeIdx[tok] = make(map[types.RelID]struct{})
	}
	ms.typeIdx[tok][id] = struct{}{}
	if ms.outIdx[r.StartNodeID()] == nil {
		ms.outIdx[r.StartNodeID()] = make(map[types.RelID]struct{})
	}
	ms.outIdx[r.StartNodeID()][id] = struct{}{}
	if ms.inIdx[r.EndNodeID()] == nil {
		ms.inIdx[r.EndNodeID()] = make(map[types.RelID]struct{})
	}
	ms.inIdx[r.EndNodeID()][id] = struct{}{}
	ms.segAccountLocked(r, 1)
	return nil
}

// faultInAdjacentLocked faults in every current sealed row touching nid.
func (ms *Store) faultInAdjacentLocked(nid types.NodeID) error {
	if !ms.anySegmentsLocked() {
		return nil
	}
	var ids []types.RelID
	for _, incoming := range []bool{false, true} {
		if err := ms.forEachSealedAdjacentLocked(nid, 0, incoming, func(_ *memSeg, _ int, id types.RelID) (bool, error) {
			ids = append(ids, id)
			return true, nil
		}); err != nil {
			return err
		}
	}
	for _, id := range ids {
		if err := ms.faultInLocked(id); err != nil {
			return err
		}
	}
	return nil
}

// sealedExistsLocked reports whether id is a current SEALED row (the create
// doors' duplicate probe after their memtable probe). A generated ID above
// every sealed ID costs one compare (ADR-0011 §4.6).
func (ms *Store) sealedExistsLocked(id types.RelID) (bool, error) {
	if ms.segMaxID == 0 || id > ms.segMaxID {
		return false, nil
	}
	_, _, ok, err := ms.sealedLocked(id)
	return ok, err
}

// relExistsErr maps a sealed-existence probe to the create doors' answer.
func relExistsErr(err error) error {
	if err != nil {
		return err
	}
	return ErrRelExists
}

// adjacentSortedLocked is the union adjacency of one node, sorted by ID (nil
// when empty) — the materializing adjacency doors' segment path.
func (ms *Store) adjacentSortedLocked(nid types.NodeID, typeToken uint16, incoming bool) ([]*types.Relationship, error) {
	var out []*types.Relationship
	if err := ms.adjacentRowsLocked(nid, typeToken, incoming, func(r *types.Relationship) bool {
		out = append(out, r)
		return true
	}); err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, nil
	}
	storepkg.SortRelsByID(out)
	return out, nil
}

// adjacentForNodesLocked fills the *ForNodes doors' result map (keys are the
// validated requested nodes) from the union adjacency.
func (ms *Store) adjacentForNodesLocked(result map[types.NodeID][]*types.Relationship, typeToken uint16, incoming bool) (map[types.NodeID][]*types.Relationship, error) {
	for nid := range result {
		rels, err := ms.adjacentSortedLocked(nid, typeToken, incoming)
		if err != nil {
			return nil, err
		}
		if len(rels) == 0 {
			delete(result, nid)
			continue
		}
		result[nid] = rels
	}
	if len(result) == 0 {
		return nil, nil
	}
	return result, nil
}

// relationshipsByTypeSegmentsLocked is RelationshipsByType for a type with
// sealed rows: the same temporal-index fast path, pagination and filter
// order (After, then the temporal filter, then Limit) over the union.
func (ms *Store) relationshipsByTypeSegmentsLocked(token uint16, opts QueryOpts) ([]*types.Relationship, error) {
	hasTemporal := storepkg.HasTemporalFilter(opts)
	keep := func(r *types.Relationship) bool {
		return !hasTemporal || storepkg.MatchesTemporalFilter(r.ID().SnowflakeID(), r.Temporal(), opts)
	}
	if ti, ok := ms.relTypeTemporalIndexes[token]; ok {
		var rawIDs []snowflake.ID
		temporalQuery := false
		if opts.ValidAt != 0 {
			rawIDs = ti.QueryAt(opts.ValidAt)
			temporalQuery = true
		} else if opts.ValidStart > 0 && opts.ValidEnd > 0 {
			rawIDs = ti.QueryOverlap(opts.ValidStart, opts.ValidEnd)
			temporalQuery = true
		}
		if temporalQuery {
			if len(rawIDs) == 0 {
				return nil, nil
			}
			ids := storepkg.ToRelIDs(rawIDs)
			storepkg.SortRelIDs(ids)
			return ms.relsFromIDsLocked(ids, opts.After, opts.Limit, func(r *types.Relationship) bool {
				return r.HasTypeTokenRaw(token) && keep(r)
			})
		}
	}
	rows, err := ms.typeRowsByIDLocked(token, opts.After, opts.Limit, keep)
	if err != nil || len(rows) == 0 {
		return nil, err
	}
	return rows, nil
}

// relsFromIDsLocked resolves ID-sorted candidates through the union view:
// IDs <= after skipped, then keep, then at most limit (0 = all). nil when
// empty.
func (ms *Store) relsFromIDsLocked(ids []types.RelID, after types.EntityID, limit int, keep func(*types.Relationship) bool) ([]*types.Relationship, error) {
	ids = storepkg.PaginateRelIDs(ids, after, 0)
	var out []*types.Relationship
	for _, id := range ids {
		r, ok, err := ms.relLocked(id)
		if err != nil {
			return nil, err
		}
		if !ok || (keep != nil && !keep(r)) {
			continue
		}
		out = append(out, r)
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out, nil
}

// allRelationshipsSegmentsLocked is AllRelationships over the union: every
// current row (sealed ones decoded page by page), filtered, ID-sorted,
// paginated.
func (ms *Store) allRelationshipsSegmentsLocked(opts QueryOpts) ([]*types.Relationship, error) {
	hasTemporal := storepkg.HasTemporalFilter(opts)
	out := make([]*types.Relationship, 0, len(ms.rels)+ms.liveSealedLocked())
	if err := ms.forEachCurrentRelLocked(func(r *types.Relationship) bool {
		if !hasTemporal || storepkg.MatchesTemporalFilter(r.ID().SnowflakeID(), r.Temporal(), opts) {
			out = append(out, r)
		}
		return true
	}); err != nil {
		return nil, err
	}
	storepkg.SortRelsByID(out)
	out = storepkg.PaginateRels(out, opts.After, opts.Limit)
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}

// shrinkMemtableMapsLocked re-allocates rels and the type's typeIdx set when
// a seal emptied most of them: a Go map keeps its peak bucket array after
// deletes, which would leave the memtable's peak cost resident after every
// seal. Copying the survivors is O(memtable) per seal, amortized over the
// rows the seal moved out.
func (ms *Store) shrinkMemtableMapsLocked(tok uint16, outBefore, inBefore int) {
	// The adjacency maps' top level is keyed by node: a seal that emptied
	// most nodes' sets leaves the buckets of every node ever seen.
	if outBefore >= 1024 && len(ms.outIdx) <= outBefore/2 {
		ms.outIdx = compactAdj(ms.outIdx)
	}
	if inBefore >= 1024 && len(ms.inIdx) <= inBefore/2 {
		ms.inIdx = compactAdj(ms.inIdx)
	}
	if n := len(ms.rels); ms.relsPeak >= 4096 && n <= ms.relsPeak/2 {
		fresh := make(map[types.RelID]*types.Relationship, n)
		for id, r := range ms.rels {
			fresh[id] = r
		}
		ms.rels = fresh
		ms.relsPeak = n
	}
	if set := ms.typeIdx[tok]; set != nil {
		fresh := make(map[types.RelID]struct{}, len(set))
		for id := range set {
			fresh[id] = struct{}{}
		}
		ms.typeIdx[tok] = fresh
	}
}

// compactAdj copies an adjacency map's top level into a table sized for its
// current entries (maps.Clone would keep the old table size).
func compactAdj(m map[types.NodeID]map[types.RelID]struct{}) map[types.NodeID]map[types.RelID]struct{} {
	out := make(map[types.NodeID]map[types.RelID]struct{}, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// newSegDictsLocked builds a declared type's shared dictionaries: the
// store-wide endpoint dictionary and one value dictionary per declared
// string column.
func (ms *Store) newSegDictsLocked(d storecontract.RelSegmentDeclaration) *segment.Dicts {
	out := &segment.Dicts{Nodes: ms.segNodeDict, Strings: map[string]*segment.StringDict{}}
	for _, c := range d.Columns {
		if c.Kind == storecontract.SegmentString {
			out.Strings[c.Name] = segment.NewStringDict()
		}
	}
	return out
}
