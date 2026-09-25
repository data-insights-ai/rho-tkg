package memory

import (
	"errors"
	"fmt"
	"slices"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/segdir"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/segment"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Segment files on disk — ADR-0011 step S3.
//
// With a segment directory attached (OpenRelSegmentDir), a seal writes its
// segment as a SELF-CONTAINED file (its own endpoint table and string
// dictionaries; the store-level shared dictionaries of S2 are an in-RAM
// format), commits it to the directory's manifest, maps the file read-only
// and only then moves the rows out of the memtable:
//
//	encode -> write .tmp, fsync, rename -> map, open -> manifest commit -> install
//
// So at every instant each row is in the memtable or in a committed file (or
// both, for the instant between the commit and the install — a crash there
// is recovered by the manifest: the rows are sealed). A failure before the
// commit removes the file and leaves every row in the memtable.
//
// What the directory makes durable is the SEALED ROWS, not the store: nodes,
// the memtable, the overlay (updates and deletes of sealed rows), the row
// store's history and every index are process memory, as before. A reopen
// serves exactly the rows the committed segments hold, as sealed; a caller
// that needs the rest replays it (a replica's change-log apply finds the
// sealed rows through the union view).
//
// Reads are unchanged: every door reads a segment through segment.Segment,
// now over the mapping. A mapped segment is reference-counted (memSeg.refs):
// the store holds it while listed, a lock-free scan pins it, and the last
// holder unmaps it. Strings handed to callers never point into a mapping
// (the codec copies every string and byte value it returns).

var _ storecontract.RelSegmentDirCapability = (*Store)(nil)

// OpenRelSegmentDir implements store.RelSegmentDirCapability.
func (ms *Store) OpenRelSegmentDir(dir string) error {
	if ms == nil {
		return ErrNilStore
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if err := ms.checkOpenLocked(); err != nil {
		return err
	}
	if ms.segDir != nil {
		return fmt.Errorf("%w: a segment directory is already open (%s)", storecontract.ErrRelSegmentDeclaration, ms.segDir.Path())
	}
	if len(ms.segTypes) == 0 {
		return fmt.Errorf("%w: a segment directory needs a declared type", storecontract.ErrRelSegmentDeclaration)
	}
	for _, st := range ms.segTypes {
		if len(st.segs) > 0 {
			return fmt.Errorf("%w: type %q already has in-RAM segments", storecontract.ErrRelSegmentDeclaration, st.decl.TypeName)
		}
	}
	d, err := segdir.Open(dir)
	if err != nil {
		return segdirErr(err)
	}
	attached := false
	defer func() {
		if !attached {
			_ = d.Close()
		}
	}()
	man := d.Manifest()
	for _, t := range man.Types {
		st := ms.segTypes[t.Token]
		if st == nil || !sameDirDecl(dirDecl(st.decl), t) {
			return fmt.Errorf("%w: the segment directory holds type %q (token %d, %d columns, blocks of %d rows), declared differently or not at all",
				storecontract.ErrRelSegmentDeclaration, t.Name, t.Token, len(t.Columns), t.BlockRows)
		}
	}
	loaded, err := ms.loadSegmentsLocked(d, man)
	if err != nil {
		return err
	}
	release := func() {
		for _, sg := range loaded {
			sg.unpin()
		}
	}
	if decls := ms.dirDeclsLocked(); !slices.EqualFunc(decls, man.Types, sameDirDecl) {
		if err := d.SetTypes(decls); err != nil {
			release()
			return segdirErr(err)
		}
	}
	if _, err := d.Cleanup(); err != nil {
		release()
		return segdirErr(err)
	}
	for i, sg := range loaded {
		ms.installLoadedSegmentLocked(man.Segments[i].Token, sg)
	}
	ms.segSeq = max(ms.segSeq, man.NextSeq)
	ms.segDir = d
	attached = true
	ms.bumpRelEpoch()
	return nil
}

// loadSegmentsLocked maps, opens and verifies every segment the manifest
// lists (in manifest order). Any failure unmaps what it opened and fails
// closed; nothing in the directory is touched.
func (ms *Store) loadSegmentsLocked(d *segdir.Dir, man segdir.Manifest) ([]*memSeg, error) {
	var out []*memSeg
	fail := func(e segdir.Entry, err error) ([]*memSeg, error) {
		for _, sg := range out {
			sg.unpin()
		}
		return nil, fmt.Errorf("%w: %s: %w", storecontract.ErrRelSegmentFileInvalid, segdir.FileName(e.Token, e.Seq), err)
	}
	for _, e := range man.Segments {
		st := ms.segTypes[e.Token]
		m, err := d.Map(e)
		if err != nil {
			return fail(e, err)
		}
		sg, err := openMapped(e, m, st)
		if err != nil {
			_ = m.Close()
			return fail(e, err)
		}
		out = append(out, sg)
	}
	return out, nil
}

// openMapped opens a mapped file as the segment its manifest entry and its
// type's declaration say it is, and verifies every integrity group (the
// manifest pins the segment root, so a rewrite with fixed-up CRCs is caught
// here, located to its IntegrityBlockRows group).
func openMapped(e segdir.Entry, m *segdir.Mapping, st *segType) (*memSeg, error) {
	seg, err := segment.Open(m.Bytes())
	if err != nil {
		return nil, err
	}
	sc := seg.Schema()
	switch {
	case seg.Len() != e.Rows:
		return nil, fmt.Errorf("%w: %d rows, the manifest lists %d", segment.ErrCorrupt, seg.Len(), e.Rows)
	case seg.Root() != e.Root:
		return nil, fmt.Errorf("%w: segment root differs from the manifest's", segment.ErrIntegrity)
	case sc.TypeName != st.schema.TypeName || sc.TypeToken != st.schema.TypeToken || !slices.Equal(sc.Columns, st.schema.Columns) ||
		seg.IntegrityBlockRows() != resolvedBlockRows(st.decl.IntegrityBlockRows):
		return nil, fmt.Errorf("%w: the file's schema (type %q, token %d) is not its type's declaration", segment.ErrCorrupt, sc.TypeName, sc.TypeToken)
	}
	if err := seg.Verify(); err != nil {
		return nil, err
	}
	return newMemSeg(e.Seq, seg, int(e.Size), m), nil
}

// installLoadedSegmentLocked adds a reopened segment to its type. A row the
// row store holds with the same ID, version and hash is the same row (a
// crash between a manifest commit and the row removal, or a caller that
// rebuilt its row store first): it leaves the row store. A row the row store
// holds in another version or hash is newer there: the sealed copy is dead.
// Every other sealed row enters the logical row set, so the stats and the
// indexes account it exactly as a put does.
func (ms *Store) installLoadedSegmentLocked(tok uint16, sg *memSeg) {
	st := ms.segTypes[tok]
	st.segs = append(st.segs, sg)
	st.sealedRows += int64(sg.seg.Len())
	st.segBytes += int64(sg.bytes)
	st.maxID = max(st.maxID, sg.hi)
	ms.segMaxID = max(ms.segMaxID, sg.hi)
	outBefore, inBefore := len(ms.outIdx), len(ms.inIdx)
	// Verified at open: a decode error here is impossible short of memory
	// corruption, and the rows already counted stay counted.
	_ = sg.seg.Scan(func(_ int, r *types.Relationship) bool {
		id := r.ID()
		if cur, ok := ms.rels[id]; ok {
			if cur.Version() == r.Version() && cur.Integrity() != nil && cur.Integrity().Hash == r.Integrity().Hash {
				ms.removeRowFromMemtableLocked(cur)
			} else {
				ms.segDead[id] = tok
				st.deadRows++
			}
			return true
		}
		ms.accountSealedRowLocked(r)
		return true
	})
	ms.shrinkMemtableMapsLocked(tok, outBefore, inBefore)
}

// accountSealedRowLocked records a sealed row that enters the logical row
// set without passing the memtable: what putRelationshipRouted does besides
// rels / typeIdx / outIdx / inIdx and the change-log.
func (ms *Store) accountSealedRowLocked(r *types.Relationship) {
	id := r.ID()
	ms.bumpRelBeliefWatermarkLocked(id, relTxFrom(r))
	ms.recordRelTypeMemberLocked(r)
	indexpkg.AddRelToPropertyIndexes(ms.relPropertyIndexes, r, id.SnowflakeID())
	ms.adjustRelPropertyTypeClassCounts(r, 1)
	ms.adjustRelPropertyKeyCounts(r, 1)
	indexpkg.AddRelToTemporalIndexes(ms.relTypeTemporalIndexes, r, id.SnowflakeID())
}

// spillSegment writes an encoded segment to the directory, maps it and
// commits it (ADR-0011 §3.1 steps 3-4). On any failure nothing is committed
// and the file is removed; the caller keeps every row in the memtable.
func spillSegment(d *segdir.Dir, tok uint16, data []byte, gen uint64) (*segment.Segment, *segdir.Mapping, uint64, error) {
	probe, err := segment.Open(data) // the codec verified these bytes; read its row count and root
	if err != nil {
		return nil, nil, 0, err
	}
	e, err := d.WriteSegment(tok, data, probe.Len(), probe.Root())
	if err != nil {
		return nil, nil, 0, err
	}
	m, err := d.Map(e)
	if err != nil {
		d.Discard(e)
		return nil, nil, 0, err
	}
	seg, err := segment.Open(m.Bytes())
	if err == nil {
		err = d.Commit(e, gen)
	}
	if err != nil {
		_ = m.Close()
		d.Discard(e)
		return nil, nil, 0, err
	}
	return seg, m, e.Seq, nil
}

// releaseSegmentDirLocked runs at Close: the store stops holding its mapped
// segments (a scan still running unmaps them when it returns) and releases
// the directory's lock.
func (ms *Store) releaseSegmentDirLocked() {
	if ms.segDir == nil {
		return
	}
	for _, st := range ms.segTypes {
		unpinAll(st.segs)
		st.segs = nil
	}
	_ = ms.segDir.Close()
	ms.segDir = nil
}

func resolvedBlockRows(b int) int {
	if b == 0 {
		return segment.DefaultIntegrityBlockRows
	}
	return b
}

// dirDecl is a declaration as the manifest persists it.
func dirDecl(d storecontract.RelSegmentDeclaration) segdir.TypeDecl {
	out := segdir.TypeDecl{Name: d.TypeName, Token: d.TypeToken, BlockRows: resolvedBlockRows(d.IntegrityBlockRows)}
	for _, c := range d.Columns {
		out.Columns = append(out.Columns, segdir.Column{Name: c.Name, Kind: uint8(c.Kind)})
	}
	return out
}

// dirDeclsLocked is every declaration, by token.
func (ms *Store) dirDeclsLocked() []segdir.TypeDecl {
	out := make([]segdir.TypeDecl, 0, len(ms.segTypes))
	for _, st := range ms.segTypes {
		out = append(out, dirDecl(st.decl))
	}
	slices.SortFunc(out, func(a, b segdir.TypeDecl) int { return int(a.Token) - int(b.Token) })
	return out
}

func sameDirDecl(a, b segdir.TypeDecl) bool {
	return a.Name == b.Name && a.Token == b.Token && a.BlockRows == b.BlockRows && slices.Equal(a.Columns, b.Columns)
}

// segdirErr maps the directory package's errors to the store sentinels,
// keeping the cause.
func segdirErr(err error) error {
	switch {
	case errors.Is(err, segdir.ErrLocked):
		return fmt.Errorf("%w: %w", storecontract.ErrRelSegmentDirLocked, err)
	case errors.Is(err, segdir.ErrManifest):
		return fmt.Errorf("%w: %w", storecontract.ErrRelSegmentManifestInvalid, err)
	case errors.Is(err, segdir.ErrFile):
		return fmt.Errorf("%w: %w", storecontract.ErrRelSegmentFileInvalid, err)
	}
	return err
}
