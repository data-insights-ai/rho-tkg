package memory

import (
	"cmp"
	"errors"
	"fmt"
	"math"
	"slices"
	"sync"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/segdir"
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
	id     uint64 // > 0, unique for the store's life (RelSegmentBatch.Segment)
	seg    *segment.Segment
	lo, hi types.RelID
	bytes  int
	m      *segdir.Mapping // the mapped file (nil: an in-RAM segment)
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
		Sealing:        ms.sealerRunning,
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

// sealIfDue runs after a write door released ms.mu (deferred before the
// lock is taken): one atomic load when nothing is due. When the unsealed
// bytes crossed the budget it starts the store's background sealer (at most
// one goroutine, which exits when nothing is due) and returns: the writer
// does not seal. Only a writer a full budget ahead of the sealer (unsealed
// bytes at twice the budget) seals itself and so blocks until the memtable
// is back under it (ADR-0011 §3.5: "if sealing cannot keep up, appends
// block").
func (ms *Store) sealIfDue() {
	if !ms.segDue.Load() {
		return
	}
	ms.mu.Lock()
	if ms.closed || !ms.segDue.Load() {
		ms.mu.Unlock()
		return
	}
	over := ms.segUnsealed >= 2*ms.segBudget+ms.segRefusedBytes
	tok := ms.mostUnsealedLocked()
	if !ms.sealerRunning {
		ms.sealerRunning = true
		ms.sealers.Add(1) // under ms.mu, before closed: Close's Wait sees it
		go ms.sealLoop()
	}
	ms.mu.Unlock()
	if over && tok != 0 {
		// Explicit: waits for the running seal of the type, then seals
		// what is left. A failure leaves rows in the memtable (correct).
		_ = ms.sealType(tok, true)
	}
}

// sealLoop is the background sealer: it seals the declared type with the
// most unsealed bytes while a write has marked the budget exceeded, then
// exits. Close waits for it.
func (ms *Store) sealLoop() {
	defer ms.sealers.Done()
	for {
		ms.mu.Lock()
		if ms.closed || !ms.segDue.Load() {
			ms.sealerRunning = false
			ms.mu.Unlock()
			return
		}
		ms.segDue.Store(false)
		tok := ms.mostUnsealedLocked()
		ms.mu.Unlock()
		if tok != 0 {
			// A failed budget seal leaves every row in the memtable, which
			// is always correct; the next write over the budget retries.
			_ = ms.sealType(tok, false)
		}
	}
}

// mostUnsealedLocked returns the declared type with the most unsealed
// bytes (0 when none has any).
func (ms *Store) mostUnsealedLocked() uint16 {
	var tok uint16
	var most int64
	for t, st := range ms.segTypes {
		if st.unsealedBytes > most {
			tok, most = t, st.unsealedBytes
		}
	}
	return tok
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

	if hook := ms.sealEncodeHook; hook != nil {
		hook() // test seam: the encode window, ms.mu not held
	}
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
	ms.segSeq++
	sg := &memSeg{id: ms.segSeq, seg: seg, lo: lo, hi: hi, bytes: len(data)}
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
	ms.sealedRowBuilds.Add(1)
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
				ms.sealedRowBuilds.Add(1)
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
		ms.sealedRowBuilds.Add(1)
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

var _ storecontract.RelSegmentScanCapability = (*Store)(nil)

// ScanRelSegments implements store.RelSegmentScanCapability: every current
// row of a declared type, from one snapshot (the segment list, the type's
// dead sealed IDs and the unsealed rows, taken under one read lock), handed
// out without the lock in ascending ID order — the row doors' order — with no
// Relationship built for a sealed row (see segIDScan).
func (ms *Store) ScanRelSegments(typeToken uint16, props []string, fn func(*storecontract.RelSegmentBatch) bool) (bool, error) {
	if ms == nil {
		return false, ErrNilStore
	}
	ms.mu.RLock()
	if err := ms.checkOpenLocked(); err != nil {
		ms.mu.RUnlock()
		return false, err
	}
	st := ms.segTypes[typeToken]
	if st == nil {
		ms.mu.RUnlock()
		return false, nil
	}
	cols := make([]int, len(props))
	kinds := make([]storecontract.SegmentColumnKind, len(props))
	for i, p := range props {
		cols[i] = -1
		for j, c := range st.decl.Columns {
			if c.Name == p {
				cols[i], kinds[i] = j, c.Kind
			}
		}
		if cols[i] < 0 {
			ms.mu.RUnlock()
			return false, nil
		}
	}
	if fn == nil {
		ms.mu.RUnlock()
		return false, errNilIterationCallback()
	}
	segs := slices.Clone(st.segs)
	dead := make(map[types.RelID]struct{})
	for id, tok := range ms.segDead {
		if tok == typeToken {
			dead[id] = struct{}{}
		}
	}
	mem := make([]*types.Relationship, 0, len(ms.typeIdx[typeToken]))
	for id := range ms.typeIdx[typeToken] {
		if r := ms.rels[id]; r != nil {
			mem = append(mem, r)
		}
	}
	ms.mu.RUnlock()

	slices.SortFunc(mem, func(a, b *types.Relationship) int { return cmp.Compare(a.ID(), b.ID()) })
	slices.SortStableFunc(segs, func(a, b *memSeg) int { return cmp.Compare(a.lo, b.lo) })
	sc := &segIDScan{out: newSegScanBatch(props, kinds), cols: cols, dead: dead, fn: fn}
	sc.memBuf.cols = make([]storecontract.SegmentColumnValues, len(props))
	err := sc.run(segs, mem)
	if segScanDoneForTest != nil {
		segScanDoneForTest(sc.peakRows)
	}
	return true, err
}

// segScanDoneForTest, when set (tests only), receives each ScanRelSegments
// call's peak of decoded segment rows held at once.
var segScanDoneForTest func(peakRows int)

// segIDScan hands out the union of a snapshot's segments and unsealed rows in
// ascending ID order. A segment stores its rows in (start, end, valid_from)
// order, so it is decoded once, page by page, and each live row is written
// to its rank in the segment's ID index (segCursor): the cursor's columns are
// in ID order, and a run of them is handed out as sub-slices, no second copy.
// Segments are sorted by their lowest ID; a segment is decoded only when the
// scan reaches that ID, and released after its last row. So the scan holds
// the decoded columns of the segments whose ID ranges overlap the current ID
// (one segment when seals are ID-disjoint, the common case), never the whole
// type. Unsealed rows are already resident; they are sorted by ID and merged
// in, copied into a reused page batch.
//
// A batch holds consecutive rows (in ID order) of ONE source — one segment
// (Segment > 0) or the unsealed rows (Segment 0) — so each string column has
// one dictionary and Row(k) one decoder.
type segIDScan struct {
	out    *segScanBatch
	cols   []int
	dead   map[types.RelID]struct{}
	fn     func(*storecontract.RelSegmentBatch) bool
	active []*segCursor
	memBuf segMemBuf
	stop   bool
	// peakRows is the most decoded segment rows held at once (a measurement
	// surface for the memory bound).
	peakRows int
}

// segCursor is one segment's live rows decoded into flat columns in
// ascending ID order; pos[i] is row i's segment position (for Row).
type segCursor struct {
	sg          *memSeg
	ids         []types.RelID
	start, end  []types.NodeID
	vf, vt, tx  []int64
	versions    []uint32
	hasTemporal []bool
	pos         []int32
	cols        []storecontract.SegmentColumnValues
	at          int
}

func (c *segCursor) done() bool { return c.at >= len(c.ids) }

func (c *segCursor) headID() types.RelID { return c.ids[c.at] }

func (sc *segIDScan) run(segs []*memSeg, mem []*types.Relationship) error {
	mi, si := 0, 0
	for !sc.stop {
		// The smallest head among the decoded segments and the unsealed rows.
		best := -1
		var bestID types.RelID
		for i, c := range sc.active {
			if id := c.headID(); best < 0 || id < bestID {
				best, bestID = i, id
			}
		}
		fromMem := mi < len(mem) && (best < 0 || mem[mi].ID() < bestID)
		if fromMem {
			bestID = mem[mi].ID()
		}
		// A segment whose lowest ID is not above the next row is decoded first.
		if si < len(segs) && ((best < 0 && !fromMem) || segs[si].lo <= bestID) {
			c, err := sc.load(segs[si])
			if err != nil {
				return err
			}
			si++
			if !c.done() {
				sc.active = append(sc.active, c)
			}
			held := 0
			for _, a := range sc.active {
				held += len(a.ids)
			}
			sc.peakRows = max(sc.peakRows, held)
			continue
		}
		if best < 0 && !fromMem {
			break
		}
		// Hand out the chosen source's run of rows below every other head
		// (its first row unconditionally: a current ID lives in one source).
		limit := types.RelID(math.MaxInt64)
		if si < len(segs) {
			limit = segs[si].lo
		}
		for i, c := range sc.active {
			if (fromMem || i != best) && c.headID() < limit {
				limit = c.headID()
			}
		}
		if fromMem {
			hi := mi + 1
			for hi < len(mem) && mem[hi].ID() < limit {
				hi++
			}
			sc.emitMem(mem[mi:hi])
			mi = hi
			continue
		}
		if mi < len(mem) && mem[mi].ID() < limit {
			limit = mem[mi].ID()
		}
		c := sc.active[best]
		hi := c.at + 1
		for hi < len(c.ids) && c.ids[hi] < limit {
			hi++
		}
		sc.emitSeg(c, hi)
		if c.done() {
			sc.active = slices.Delete(sc.active, best, best+1)
		}
	}
	return nil
}

// load decodes segment sg's live rows into ID order: the ID index gives each
// segment position its rank, and one page-by-page decode writes every row to
// its rank.
func (sc *segIDScan) load(sg *memSeg) (*segCursor, error) {
	n := sg.seg.Len()
	rank := make([]int32, n)
	for i := range rank {
		rank[i] = -1
	}
	c := &segCursor{sg: sg, pos: make([]int32, 0, n)}
	err := sg.seg.ForEachID(func(id types.RelID, row int) bool {
		if _, gone := sc.dead[id]; !gone {
			rank[row] = int32(len(c.pos))     // #nosec G115 -- a segment position (< 2^31 rows)
			c.pos = append(c.pos, int32(row)) // #nosec G115 -- a segment position (< 2^31 rows)
		}
		return true
	})
	if err != nil {
		return nil, err
	}
	m := len(c.pos)
	c.ids, c.start, c.end = make([]types.RelID, m), make([]types.NodeID, m), make([]types.NodeID, m)
	c.vf, c.vt, c.tx = make([]int64, m), make([]int64, m), make([]int64, m)
	c.versions, c.hasTemporal = make([]uint32, m), make([]bool, m)
	c.cols = make([]storecontract.SegmentColumnValues, len(sc.cols))
	for i := range c.cols {
		c.cols[i] = storecontract.SegmentColumnValues{
			Name: sc.out.Cols[i].Name, Kind: sc.out.Cols[i].Kind, Present: make([]bool, m), Other: make([]bool, m),
		}
	}
	var serr error
	err = sg.seg.ScanBatches(func(pb *segment.Batch) bool {
		for i, j := range sc.cols {
			if serr = scatterColumn(&c.cols[i], pb, j, rank, m); serr != nil {
				return false
			}
		}
		for k := 0; k < pb.N; k++ {
			r := rank[pb.Base+k]
			if r < 0 {
				continue
			}
			c.ids[r], c.start[r], c.end[r] = types.RelID(pb.IDs[k]), types.NodeID(pb.StartIDs[k]), types.NodeID(pb.EndIDs[k])
			c.vf[r], c.vt[r], c.tx[r] = pb.ValidFrom[k], pb.ValidTo[k], pb.TxFrom[k]
			c.versions[r] = uint32(pb.Versions[k]) // #nosec G115 -- a stored uint32
			c.hasTemporal[r] = pb.HasTemporal(k)
		}
		return true
	})
	if err == nil {
		err = serr
	}
	return c, err
}

// scatterColumn writes declared column j of page pb's live rows to their
// ranks in col (m live rows): dictionary codes into the segment's dictionary
// (a later snapshot of a shared dictionary extends an earlier one), plain
// strings, or stored bits.
func scatterColumn(col *storecontract.SegmentColumnValues, pb *segment.Batch, j int, rank []int32, m int) error {
	var vals, codes []int64
	if col.Kind == storecontract.SegmentString {
		if cs, dict, _, ok := pb.StringColumn(j); ok {
			codes, col.Dict = cs, dict
			if col.Codes == nil {
				col.Codes = make([]uint32, m)
			}
		} else if col.Strs == nil {
			col.Strs = make([]string, m)
		}
	} else {
		vals, _, _ = pb.IntColumn(j)
		if col.Ints == nil {
			col.Ints = make([]int64, m)
		}
	}
	for k := 0; k < pb.N; k++ {
		r := rank[pb.Base+k]
		if r < 0 {
			continue
		}
		has := pb.HasValue(j, k)
		col.Present[r] = has
		col.Other[r] = !has && pb.HasFallback(k)
		switch {
		case codes != nil:
			col.Codes[r] = uint32(codes[k]) // #nosec G115 -- a validated dictionary code
		case col.Kind == storecontract.SegmentString:
			if has {
				v, err := pb.StringAt(j, k)
				if err != nil {
					return err
				}
				col.Strs[r] = v
			}
		default:
			col.Ints[r] = vals[k]
		}
	}
	return nil
}

// emitSeg hands out cursor c's rows [c.at, hi) as sub-slices, a page at a
// time.
func (sc *segIDScan) emitSeg(c *segCursor, hi int) {
	b := sc.out
	for c.at < hi && !sc.stop {
		lo, end := c.at, min(hi, c.at+segment.PageRows)
		c.at = end
		b.Segment, b.Sorted = c.sg.id, false
		b.IDs, b.StartIDs, b.EndIDs = c.ids[lo:end], c.start[lo:end], c.end[lo:end]
		b.ValidFrom, b.ValidTo, b.TxFrom = c.vf[lo:end], c.vt[lo:end], c.tx[lo:end]
		b.Versions, b.HasTemporal = c.versions[lo:end], c.hasTemporal[lo:end]
		for i := range b.Cols {
			col, src := &b.Cols[i], &c.cols[i]
			col.Present, col.Other, col.Dict = src.Present[lo:end], src.Other[lo:end], src.Dict
			col.Ints, col.Codes, col.Strs = subslice(src.Ints, lo, end), subslice(src.Codes, lo, end), subslice(src.Strs, lo, end)
		}
		seg, pos := c.sg.seg, c.pos[lo:end]
		b.SetRowFunc(func(k int) (*types.Relationship, error) {
			if k < 0 || k >= len(pos) {
				return nil, fmt.Errorf("%w: segment batch row %d", storecontract.ErrInvalidStoreMutation, k)
			}
			r, err := seg.Row(int(pos[k]))
			if err == nil {
				r.Freeze()
			}
			return r, err
		})
		sc.stop = !sc.fn(&b.RelSegmentBatch)
	}
}

// subslice is s[lo:hi], nil for a nil s (an absent representation stays
// absent: Value reads Codes only when non-nil).
func subslice[T any](s []T, lo, hi int) []T {
	if s == nil {
		return nil
	}
	return s[lo:hi]
}

// emitMem hands out unsealed rows (consecutive in ID order), a page at a
// time, copied into the reused batch buffers.
func (sc *segIDScan) emitMem(rows []*types.Relationship) {
	b := sc.out
	for len(rows) > 0 && !sc.stop {
		chunk := rows[:min(len(rows), segment.PageRows)]
		rows = rows[len(chunk):]
		b.Segment, b.Sorted = 0, false
		b.IDs, b.StartIDs, b.EndIDs = sc.memBuf.ids[:0], sc.memBuf.start[:0], sc.memBuf.end[:0]
		b.ValidFrom, b.ValidTo, b.TxFrom = sc.memBuf.vf[:0], sc.memBuf.vt[:0], sc.memBuf.tx[:0]
		b.Versions, b.HasTemporal = sc.memBuf.versions[:0], sc.memBuf.hasTemporal[:0]
		for i := range b.Cols {
			col := &b.Cols[i]
			col.Present, col.Other, col.Ints, col.Strs = sc.memBuf.cols[i].Present[:0], sc.memBuf.cols[i].Other[:0], sc.memBuf.cols[i].Ints[:0], sc.memBuf.cols[i].Strs[:0]
			col.Codes, col.Dict = nil, nil
		}
		for _, r := range chunk {
			b.IDs = append(b.IDs, r.ID())
			b.StartIDs = append(b.StartIDs, r.StartNodeID())
			b.EndIDs = append(b.EndIDs, r.EndNodeID())
			var vf, vt, tx int64
			if tm := r.Temporal(); tm != nil {
				vf, vt, tx = int64(tm.ValidFrom), int64(tm.ValidTo), int64(tm.TxFrom)
			}
			b.ValidFrom = append(b.ValidFrom, vf)
			b.ValidTo = append(b.ValidTo, vt)
			b.TxFrom = append(b.TxFrom, tx)
			b.Versions = append(b.Versions, r.Version())
			b.HasTemporal = append(b.HasTemporal, r.Temporal() != nil)
			b.appendRowValues(r)
		}
		sc.memBuf.keep(b)
		b.SetRowFunc(func(k int) (*types.Relationship, error) {
			if k < 0 || k >= len(chunk) {
				return nil, fmt.Errorf("%w: segment batch row %d", storecontract.ErrInvalidStoreMutation, k)
			}
			return chunk[k], nil
		})
		sc.stop = !sc.fn(&b.RelSegmentBatch)
	}
}

// segMemBuf keeps the unsealed-row batch buffers across batches (a segment
// batch's slices point into its cursor instead).
type segMemBuf struct {
	ids         []types.RelID
	start, end  []types.NodeID
	vf, vt, tx  []int64
	versions    []uint32
	hasTemporal []bool
	cols        []storecontract.SegmentColumnValues
}

func (m *segMemBuf) keep(b *segScanBatch) {
	m.ids, m.start, m.end = b.IDs, b.StartIDs, b.EndIDs
	m.vf, m.vt, m.tx = b.ValidFrom, b.ValidTo, b.TxFrom
	m.versions, m.hasTemporal = b.Versions, b.HasTemporal
	for i := range b.Cols {
		m.cols[i].Present, m.cols[i].Other, m.cols[i].Ints, m.cols[i].Strs = b.Cols[i].Present, b.Cols[i].Other, b.Cols[i].Ints, b.Cols[i].Strs
	}
}

// segScanBatch is the reusable batch behind ScanRelSegments.
type segScanBatch struct {
	storecontract.RelSegmentBatch
}

func newSegScanBatch(props []string, kinds []storecontract.SegmentColumnKind) *segScanBatch {
	b := &segScanBatch{}
	b.Cols = make([]storecontract.SegmentColumnValues, len(props))
	for i := range props {
		b.Cols[i].Name, b.Cols[i].Kind = props[i], kinds[i]
	}
	return b
}

// appendRowValues appends an unsealed row's requested values.
func (b *segScanBatch) appendRowValues(r *types.Relationship) {
	for i := range b.Cols {
		c := &b.Cols[i]
		v, found := r.GetProperty(c.Name)
		has := found && storecontract.SegmentColumnKind(types.PropertyHashTypeTag(v)) == c.Kind
		c.Present = append(c.Present, has)
		c.Other = append(c.Other, found && !has)
		if c.Kind == storecontract.SegmentString {
			s := ""
			if has {
				s = v.(string)
			}
			c.Strs = append(c.Strs, s)
			continue
		}
		var x int64
		if has {
			x = segment.ValueBits(v)
		}
		c.Ints = append(c.Ints, x)
	}
}

// scanRelColumnsFromSegments serves ScanRelColumns for a declared type from
// the segment columns (ADR-0011 S5): one snapshot, every sealed page decoded
// once (a row is built only when a requested value may sit outside its
// column), the rows sorted by ID and fed through the shared column driver
// (store.ScanColumnsFromRelSource) — so kinds, absences and the mixed-numeric
// refusal are the row path's. handled=false leaves the call to the row path:
// an undeclared type, a temporal filter, or a property that is not a declared
// column.
func (ms *Store) scanRelColumnsFromSegments(token uint16, props []string, opts QueryOpts,
	fn func(*storecontract.RelColumnBatch) bool) (bool, error) {
	ms.mu.RLock()
	if ms.checkOpenLocked() != nil || storecontract.ValidateRelTypeToken(token) != nil ||
		storecontract.ValidateQueryOpts(opts) != nil || storepkg.HasTemporalFilter(opts) {
		ms.mu.RUnlock()
		return false, nil
	}
	st := ms.segTypes[token]
	if st == nil || len(st.segs) == 0 {
		ms.mu.RUnlock()
		return false, nil
	}
	cols := make([]int, len(props))
	for i, p := range props {
		cols[i] = -1
		for j, c := range st.decl.Columns {
			if c.Name == p {
				cols[i] = j
			}
		}
		if cols[i] < 0 {
			ms.mu.RUnlock()
			return false, nil
		}
	}
	segs := slices.Clone(st.segs)
	dead := make(map[types.RelID]struct{})
	for id, tok := range ms.segDead {
		if tok == token {
			dead[id] = struct{}{}
		}
	}
	mem := make([]*types.Relationship, 0, len(ms.typeIdx[token]))
	for id := range ms.typeIdx[token] {
		if r := ms.rels[id]; r != nil {
			mem = append(mem, r)
		}
	}
	ms.mu.RUnlock()

	src := newSegColumnSource(len(props))
	for _, sg := range segs {
		var serr error
		segCols := sg.seg.Schema().Columns
		err := sg.seg.ScanBatches(func(pb *segment.Batch) bool {
			for k := 0; k < pb.N; k++ {
				id := types.RelID(pb.IDs[k])
				if _, gone := dead[id]; gone {
					continue
				}
				src.addRow(id, types.NodeID(pb.StartIDs[k]), types.NodeID(pb.EndIDs[k]), pb.ValidFrom[k], pb.ValidTo[k])
				var full *types.Relationship
				for c, j := range cols {
					if pb.HasValue(j, k) {
						if serr = src.addColumnValue(c, pb, segCols[j].Kind, j, k); serr != nil {
							return false
						}
						continue
					}
					if !pb.HasFallback(k) {
						src.addAbsent(c)
						continue
					}
					if full == nil { // the value may sit in the fallback column
						if full, serr = pb.Row(k); serr != nil {
							return false
						}
						ms.sealedRowBuilds.Add(1)
					}
					src.addAny(c, full, props[c])
				}
			}
			return true
		})
		if err == nil {
			err = serr
		}
		if err != nil {
			return true, err
		}
	}
	for _, r := range mem {
		vf, vt, _ := r.ValidRange()
		src.addRow(r.ID(), r.StartNodeID(), r.EndNodeID(), int64(vf), int64(vt))
		for c := range props {
			src.addAny(c, r, props[c])
		}
	}
	src.order(opts.After, opts.Limit)
	return true, storecontract.ScanColumnsFromRelSource(src, len(props), fn)
}

// segColumnSource is the row set behind scanRelColumnsFromSegments: flat
// arrays per field and per requested column, and an ID-order permutation.
type segColumnSource struct {
	ids        []types.RelID
	start, end []types.NodeID
	vf, vt     []int64
	kind       [][]storecontract.ColumnKind
	ok         [][]bool   // false: the row does not hold the value as a column scalar
	num        [][]int64  // ColInt64 value, ColFloat64 bits, ColBool 0/1
	str        [][]string // ColString value
	perm       []int
}

func newSegColumnSource(n int) *segColumnSource {
	return &segColumnSource{
		kind: make([][]storecontract.ColumnKind, n), ok: make([][]bool, n),
		num: make([][]int64, n), str: make([][]string, n),
	}
}

func (s *segColumnSource) addRow(id types.RelID, start, end types.NodeID, vf, vt int64) {
	s.ids = append(s.ids, id)
	s.start = append(s.start, start)
	s.end = append(s.end, end)
	s.vf = append(s.vf, vf)
	s.vt = append(s.vt, vt)
}

func (s *segColumnSource) put(c int, k storecontract.ColumnKind, x int64, str string, ok bool) {
	s.kind[c] = append(s.kind[c], k)
	s.ok[c] = append(s.ok[c], ok)
	s.num[c] = append(s.num[c], x)
	s.str[c] = append(s.str[c], str)
}

func (s *segColumnSource) addAbsent(c int) { s.put(c, 0, 0, "", false) }

// addAny classifies row r's property like the row path does.
func (s *segColumnSource) addAny(c int, r *types.Relationship, key string) {
	v, found := r.GetProperty(key)
	if !found {
		s.addAbsent(c)
		return
	}
	k, i64, f64, str, b, ok := storecontract.ClassifyScalar(v)
	switch k {
	case storecontract.ColFloat64:
		i64 = int64(math.Float64bits(f64)) // #nosec G115 -- bit pattern
	case storecontract.ColBool:
		i64 = 0
		if b {
			i64 = 1
		}
	}
	s.put(c, k, i64, str, ok)
}

// addColumnValue classifies declared column j's stored value of batch row k
// exactly as classifyScalar classifies the Go value it decodes to.
func (s *segColumnSource) addColumnValue(c int, pb *segment.Batch, kind segment.Kind, j, k int) error {
	if kind == segment.KindString {
		str, err := pb.StringAt(j, k)
		if err != nil {
			return err
		}
		s.put(c, storecontract.ColString, 0, str, true)
		return nil
	}
	vals, _, _ := pb.IntColumn(j)
	x := vals[k]
	switch kind {
	case segment.KindInt64, segment.KindInt, segment.KindInt32:
		s.put(c, storecontract.ColInt64, x, "", true)
	case segment.KindFloat64:
		s.put(c, storecontract.ColFloat64, x, "", true)
	case segment.KindFloat32:
		f := float64(math.Float32frombits(uint32(x)))                            // #nosec G115 -- IEEE bits
		s.put(c, storecontract.ColFloat64, int64(math.Float64bits(f)), "", true) // #nosec G115 -- bit pattern
	case segment.KindBool:
		s.put(c, storecontract.ColBool, x, "", true)
	default: // int8/int16/uints: not a column scalar (classifyScalar !ok)
		s.addAbsent(c)
	}
	return nil
}

// order sorts by ID and applies After / Limit.
func (s *segColumnSource) order(after types.EntityID, limit int) {
	perm := make([]int, 0, len(s.ids))
	for i := range s.ids {
		if types.EntityID(s.ids[i]) > after {
			perm = append(perm, i)
		}
	}
	slices.SortFunc(perm, func(a, b int) int {
		switch {
		case s.ids[a] < s.ids[b]:
			return -1
		case s.ids[a] > s.ids[b]:
			return 1
		}
		return 0
	})
	if limit > 0 && len(perm) > limit {
		perm = perm[:limit]
	}
	s.perm = perm
}

func (s *segColumnSource) Len() int { return len(s.perm) }

func (s *segColumnSource) Row(i int) (types.RelID, types.NodeID, types.NodeID, int64, int64) {
	p := s.perm[i]
	return s.ids[p], s.start[p], s.end[p], s.vf[p], s.vt[p]
}

func (s *segColumnSource) Value(i, c int) (storecontract.ColumnKind, int64, float64, string, bool, bool) {
	p := s.perm[i]
	if !s.ok[c][p] {
		return 0, 0, 0, "", false, false
	}
	k, x := s.kind[c][p], s.num[c][p]
	switch k {
	case storecontract.ColFloat64:
		return k, 0, math.Float64frombits(uint64(x)), "", false, true // #nosec G115 -- bit pattern
	case storecontract.ColBool:
		return k, 0, 0, "", x == 1, true
	case storecontract.ColString:
		return k, 0, 0, s.str[c][p], false, true
	}
	return k, x, 0, "", false, true
}

var _ storecontract.RelSegmentDirCapability = (*Store)(nil)

// OpenRelSegmentDir implements store.RelSegmentDirCapability (ADR-0011 S3).
func (ms *Store) OpenRelSegmentDir(dir string) error {
	if ms == nil {
		return ErrNilStore
	}
	return nil // S3 skeleton
}
