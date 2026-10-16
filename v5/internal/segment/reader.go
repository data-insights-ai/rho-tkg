package segment

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"slices"
	"sort"
	"strings"

	"github.com/data-insights-ai/rho-tkg/v5/pkg/types"
)

// Segment is an opened, validated segment. It reads from the byte slice it
// was opened on and never modifies it; it is safe for concurrent use. Rows it
// returns are freshly built and share nothing with the segment bytes.
type Segment struct {
	data      []byte
	rows      int
	nodes     int
	pageRows  int
	blockRows int
	schema    Schema
	root      [32]byte
	idMin     int64
	idMax     int64
	// idSparse holds the ID-index entry at every idSparseStep-th position,
	// built at Open, so Lookup reads at most idSparseStep entries instead of
	// a whole delta-coded page (rows/8 bytes of heap per segment).
	idSparse []int64
	secs     map[string]sectionInfo

	nodeIDs, outcsr, inOff, inPerm, idIDs, idRows intCol

	end, vf, vt, tx, txTo, relID, revision intCol
	vfK, vtK, txK, txToK                   intCol
	fallback                               bytesCol
	props                                  []propCol

	integrity []byte
}

type propCol struct {
	col     Column
	ints    intCol
	strs    strCol
	present intCol
	allSet  bool // no presence section: every row holds the property
}

// Open validates data and returns a reader over it. data must not be
// modified while the Segment is in use.
func Open(data []byte) (*Segment, error) {
	h, err := readHeader(data)
	if err != nil {
		return nil, err
	}
	fstart, fend, err := locateFooter(data)
	if err != nil {
		return nil, err
	}
	f, err := readFooter(data, fstart, fend)
	if err != nil {
		return nil, err
	}
	for _, sec := range f.sections {
		if crc32.Checksum(data[sec.offset:sec.offset+sec.length], castagnoli) != sec.crc {
			return nil, corrupt(sec.name, "CRC mismatch")
		}
	}
	s := &Segment{
		data: data, rows: h.rows, nodes: h.nodes, pageRows: h.pageRows, blockRows: f.blockRows,
		schema: Schema{TypeName: f.typeName, Columns: f.columns},
		root:   f.root,
		idMin:  h.idMin,
		idMax:  h.idMax,
		secs:   make(map[string]sectionInfo, len(f.sections)),
	}
	for _, sec := range f.sections {
		s.secs[sec.name] = sec
	}
	if err := s.parseSections(); err != nil {
		return nil, err
	}
	if err := s.validateStructure(); err != nil {
		return nil, err
	}
	s.buildIDSparse()
	return s, nil
}

// idSparseStep divides every legal page size (a step that crosses a page
// boundary restarts from the page).
const idSparseStep = 64

func (s *Segment) buildIDSparse() {
	if s.rows == 0 {
		return
	}
	// rows is backed by the integrity section's bytes (32 B per group of at
	// most 4,096 rows), so this allocation is bounded by bytes delivered.
	s.idSparse = make([]int64, 0, (s.rows+idSparseStep-1)/idSparseStep)
	buf := make([]int64, s.pageRows)
	for p := 0; p*s.pageRows < s.rows; p++ {
		cnt := min(s.pageRows, s.rows-p*s.pageRows)
		s.idIDs.decodePage(p, cnt, buf, nil, nil)
		for k := 0; k < cnt; k++ {
			if (p*s.pageRows+k)%idSparseStep == 0 {
				s.idSparse = append(s.idSparse, buf[k])
			}
		}
	}
}

func (s *Segment) bytesOf(name string) ([]byte, bool) {
	sec, ok := s.secs[name]
	if !ok {
		return nil, false
	}
	return s.data[sec.offset : sec.offset+sec.length], true
}

func (s *Segment) parseSections() error {
	known := map[string]bool{}
	intSec := func(dst *intCol, name string, want int, allowed uint8) error {
		known[name] = true
		b, ok := s.bytesOf(name)
		if !ok {
			*dst = intCol{name: name}
			return nil
		}
		c, err := parseIntCol(name, b, want, s.pageRows, allowed)
		*dst = c
		return err
	}
	strSec := func(dst *strCol, name string, want int) error {
		known[name] = true
		b, ok := s.bytesOf(name)
		if !ok {
			*dst = strCol{name: name}
			return nil
		}
		c, err := parseStrCol(name, b, want, s.pageRows)
		*dst = c
		return err
	}
	bytesSec := func(dst *bytesCol, name string) error {
		known[name] = true
		b, ok := s.bytesOf(name)
		if !ok {
			*dst = bytesCol{name: name}
			return nil
		}
		c, err := parseBytesCol(name, b, s.rows, s.pageRows)
		*dst = c
		return err
	}
	none := allow(transformNone)
	vsVF := allow(transformNone, transformMinusRef)
	n, nn := s.rows, s.nodes
	var nodeLevel1 int
	if nn > 0 {
		nodeLevel1 = nn + 1
	}
	steps := []func() error{
		func() error { return intSec(&s.nodeIDs, secNodes, nn, none) },
		func() error { return intSec(&s.outcsr, secOutCSR, nodeLevel1, none) },
		func() error { return intSec(&s.inOff, secInOff, nodeLevel1, none) },
		func() error { return intSec(&s.inPerm, secInPerm, n, none) },
		func() error { return intSec(&s.idIDs, secIDs, n, allow(transformNone, transformDeltaPrev)) },
		func() error { return intSec(&s.idRows, secIDRows, n, none) },
		func() error { return intSec(&s.end, colEnd, n, allow(transformNone, transformDeltaRun)) },
		func() error { return intSec(&s.vf, colValidFrom, n, none) },
		func() error { return intSec(&s.vt, colValidTo, n, vsVF) },
		func() error { return intSec(&s.tx, colTxFrom, n, vsVF) },
		func() error { return intSec(&s.txTo, colTxTo, n, vsVF) },
		func() error { return intSec(&s.vfK, colValidFromKind, n, none) },
		func() error { return intSec(&s.vtK, colValidToKind, n, none) },
		func() error { return intSec(&s.txK, colTxFromKind, n, none) },
		func() error { return intSec(&s.txToK, colTxToKind, n, none) },
		func() error { return intSec(&s.relID, colRelID, n, none) },
		func() error { return intSec(&s.revision, colRevision, n, none) },
		func() error { return bytesSec(&s.fallback, secFallback) },
	}
	for _, step := range steps {
		if err := step(); err != nil {
			return err
		}
	}
	s.props = make([]propCol, len(s.schema.Columns))
	for i, col := range s.schema.Columns {
		p := &s.props[i]
		p.col = col
		var err error
		if col.Kind == KindString {
			err = strSec(&p.strs, propPrefix+col.Name, n)
		} else {
			err = intSec(&p.ints, propPrefix+col.Name, n, none)
		}
		if err != nil {
			return err
		}
		if err := intSec(&p.present, presencePrefix+col.Name, n, none); err != nil {
			return err
		}
		p.allSet = !p.present.present
	}
	for _, raw := range []struct {
		name string
		dst  *[]byte
		size int
	}{{secIntegrity, &s.integrity, 32 * s.groups()}, {secPageIndex, nil, pageIndexEntry * s.pages()}} {
		known[raw.name] = true
		b, ok := s.bytesOf(raw.name)
		if len(b) != raw.size || ok != (raw.size > 0) {
			return corrupt(raw.name, "%d bytes, want %d", len(b), raw.size)
		}
		if raw.dst != nil {
			*raw.dst = b
		}
	}
	for name := range s.secs {
		if !known[name] {
			return corrupt("footer", "unknown section %q", name)
		}
	}
	return nil
}

func (s *Segment) groups() int { return (s.rows + s.blockRows - 1) / s.blockRows }
func (s *Segment) pages() int  { return (s.rows + s.pageRows - 1) / s.pageRows }

// validateStructure checks the node-level invariants the reader relies on:
// node IDs positive and strictly ascending, CSR offsets running from 0 to
// rowCount. Time is linear in the node count (bounded by 2 x rows, and rows
// by the integrity section's bytes); nothing is allocated.
func (s *Segment) validateStructure() error {
	if s.nodes == 0 {
		return nil
	}
	if s.nodeIDs.atNoRef(0) < 1 {
		return corrupt(secNodes, "node id %d is not in 1..MaxInt64", s.nodeIDs.atNoRef(0))
	}
	for k := 1; k < s.nodes; k++ {
		if s.nodeIDs.atNoRef(k) <= s.nodeIDs.atNoRef(k-1) {
			return corrupt(secNodes, "node ids not ascending at %d", k)
		}
	}
	for _, c := range []*intCol{&s.outcsr, &s.inOff} {
		prev := c.atNoRef(0)
		if prev != 0 {
			return corrupt(c.name, "first offset %d", prev)
		}
		for k := 1; k <= s.nodes; k++ {
			v := c.atNoRef(k)
			if v < prev {
				return corrupt(c.name, "offsets decrease at %d", k)
			}
			prev = v
		}
		if prev != int64(s.rows) {
			return corrupt(c.name, "last offset %d, want %d rows", prev, s.rows)
		}
	}
	return nil
}

// Len is the number of rows.
func (s *Segment) Len() int { return s.rows }

// Schema returns the declared schema (type name and columns from the footer).
func (s *Segment) Schema() Schema {
	out := s.schema
	out.Columns = append([]Column(nil), s.schema.Columns...)
	return out
}

// IntegrityBlockRows returns the integrity group size written in the footer.
func (s *Segment) IntegrityBlockRows() int { return s.blockRows }

// Root returns the segment integrity root: SHA-256 over the group roots.
func (s *Segment) Root() [32]byte { return s.root }

// sectionLen is the byte size of a section (0 when absent).
func (s *Segment) sectionLen(name string) int { return s.secs[name].length }

// startOrdinal returns the node ordinal whose out-run holds row i.
func (s *Segment) startOrdinal(i int) int {
	return sort.Search(s.nodes, func(k int) bool { return s.outcsr.atNoRef(k+1) > int64(i) })
}

// Row decodes row i (segment order). The row is independent of the segment.
func (s *Segment) Row(i int) (*Row, error) {
	if i < 0 || i >= s.rows {
		return nil, fmt.Errorf("%w: row %d of %d", ErrOutOfRange, i, s.rows)
	}
	so := s.startOrdinal(i)
	eo := s.endOrdinalAt(i, so)
	v := rowVals{startOrd: so, endOrd: eo}
	vf := s.vf.atNoRef(i)
	v.vf = vf
	v.vt, v.tx, v.txTo = atRef(&s.vt, i, vf), atRef(&s.tx, i, vf), atRef(&s.txTo, i, vf)
	v.vfK, v.vtK, v.txK, v.txToK = s.vfK.atNoRef(i), s.vtK.atNoRef(i), s.txK.atNoRef(i), s.txToK.atNoRef(i)
	v.id, v.revision = s.relID.atNoRef(i), s.revision.atNoRef(i)
	v.propInts = make([]int64, len(s.props))
	v.propSet = make([]bool, len(s.props))
	for c := range s.props {
		p := &s.props[c]
		v.propSet[c] = p.allSet || p.present.atNoRef(i) == 1
		if p.col.Kind != KindString {
			v.propInts[c] = p.ints.atNoRef(i)
		}
	}
	return s.build(i, &v)
}

// atRef reads value i of a column that may be stored relative to valid_from.
func atRef(c *intCol, i int, ref int64) int64 {
	if !c.present {
		return 0
	}
	p := i / c.pageRows
	pv := c.page(p)
	d := pv.d(i - p*c.pageRows)
	if pv.transform == transformMinusRef {
		return d + ref
	}
	return d
}

// endOrdinalAt reads the end ordinal of row i whose start ordinal is so.
func (s *Segment) endOrdinalAt(i, so int) int64 {
	c := &s.end
	if !c.present {
		return 0
	}
	p := i / c.pageRows
	pageStart := p * c.pageRows
	pv := c.page(p)
	if pv.transform != transformDeltaRun {
		return pv.d(i - pageStart)
	}
	run := max(int(s.outcsr.atNoRef(so)), pageStart)
	x := pv.d(run - pageStart)
	for j := run + 1; j <= i; j++ {
		x += pv.d(j - pageStart)
	}
	return x
}

// rowVals is one row's column values before they become a Row.
type rowVals struct {
	startOrd             int
	endOrd               int64
	id, revision         int64
	vf, vt, tx, txTo     int64
	vfK, vtK, txK, txToK int64
	propInts             []int64
	propSet              []bool
}

// coordOf rebuilds a coordinate from its slot and stored kind (kind -
// CoordFinite); the slot of an infinity is ignored. allowed is the set of
// kinds the column may hold.
func coordOf(slot, kind int64, allowed ...CoordKind) (Coord, bool) {
	if kind < -1 || kind > 1 {
		return Coord{}, false
	}
	k := CoordKind(kind + int64(CoordFinite)) // #nosec G115 -- -1..1 checked above
	if !slices.Contains(allowed, k) {
		return Coord{}, false
	}
	if k == CoordFinite {
		return Finite(slot), true
	}
	return Coord{kind: k}, true
}

// build assembles row i from its column values.
func (s *Segment) build(i int, v *rowVals) (*Row, error) {
	if v.endOrd < 0 || v.endOrd >= int64(s.nodes) || v.startOrd >= s.nodes {
		return nil, corrupt(colEnd, "row %d has node ordinals %d->%d of %d", i, v.startOrd, v.endOrd, s.nodes)
	}
	if v.id < 1 {
		return nil, corrupt(colRelID, "row %d has id %d", i, v.id)
	}
	if v.revision < 0 || v.revision > math.MaxUint32 {
		return nil, corrupt(colRevision, "row %d revision %d", i, v.revision)
	}
	r := &Row{
		ID: types.RelID(v.id), Revision: uint32(v.revision), // #nosec G115 -- range checked
		Start: types.NodeID(s.nodeIDs.atNoRef(v.startOrd)), End: types.NodeID(s.nodeIDs.atNoRef(int(v.endOrd))),
	}
	var ok bool
	if r.ValidFrom, ok = coordOf(v.vf, v.vfK, CoordNegInf, CoordFinite); !ok {
		return nil, corrupt(colValidFromKind, "row %d kind %d", i, v.vfK)
	}
	if r.ValidTo, ok = coordOf(v.vt, v.vtK, CoordFinite, CoordPosInf); !ok {
		return nil, corrupt(colValidToKind, "row %d kind %d", i, v.vtK)
	}
	if r.TxFrom, ok = coordOf(v.tx, v.txK, CoordNegInf, CoordFinite); !ok {
		return nil, corrupt(colTxFromKind, "row %d kind %d", i, v.txK)
	}
	if r.TxTo, ok = coordOf(v.txTo, v.txToK, CoordFinite, CoordPosInf); !ok {
		return nil, corrupt(colTxToKind, "row %d kind %d", i, v.txToK)
	}
	var props []Property
	for c := range s.props {
		p := &s.props[c]
		if p.present.present {
			if st := p.present.atNoRef(i); st != 0 && st != 1 {
				return nil, corrupt(presencePrefix+p.col.Name, "row %d presence %d", i, st)
			}
		}
		if !v.propSet[c] {
			continue
		}
		var val any
		if p.col.Kind == KindString {
			str, err := p.strs.at(i)
			if err != nil {
				return nil, err
			}
			val = str
		} else {
			x, ok := kindValue(p.col.Kind, v.propInts[c])
			if !ok {
				return nil, corrupt(propPrefix+p.col.Name, "row %d value %d does not fit kind %d", i, v.propInts[c], p.col.Kind)
			}
			val = x
		}
		props = append(props, Property{Key: p.col.Name, Value: val})
	}
	fb, err := s.fallback.at(i)
	if err != nil {
		return nil, err
	}
	if fb != nil {
		if props, err = parseFallback(fb, props); err != nil {
			return nil, corrupt(secFallback, "row %d: %v", i, err)
		}
	}
	slices.SortFunc(props, func(a, b Property) int { return strings.Compare(a.Key, b.Key) })
	for k := 1; k < len(props); k++ {
		if props[k].Key == props[k-1].Key {
			return nil, corrupt(secFallback, "row %d holds property %q twice", i, props[k].Key)
		}
	}
	r.Props = props
	return r, nil
}

// parseFallback appends the properties encoded by appendFallback to dst. The
// bytes are untrusted: every length is checked against what is left.
func parseFallback(b []byte, dst []Property) ([]Property, error) {
	for len(b) > 0 {
		if len(b) < 2 {
			return nil, errors.New("truncated key length")
		}
		kl := int(binary.LittleEndian.Uint16(b))
		b = b[2:]
		if kl == 0 || len(b) < kl+1 {
			return nil, fmt.Errorf("key of %d bytes in %d", kl, len(b))
		}
		key := string(b[:kl])
		tag := Kind(b[kl])
		b = b[kl+1:]
		var val any
		if tag == KindString {
			if len(b) < 4 {
				return nil, errors.New("truncated string length")
			}
			l := int(binary.BigEndian.Uint32(b))
			b = b[4:]
			if l > len(b) {
				return nil, fmt.Errorf("string of %d bytes in %d", l, len(b))
			}
			val, b = string(b[:l]), b[l:]
		} else {
			if tag < KindBool || tag > KindFloat64 || len(b) < 8 {
				return nil, fmt.Errorf("value tag %d with %d bytes", tag, len(b))
			}
			x, ok := kindValue(tag, int64(binary.BigEndian.Uint64(b))) // #nosec G115 -- bit pattern
			if !ok {
				return nil, fmt.Errorf("property %q does not fit kind %d", key, tag)
			}
			val, b = x, b[8:]
		}
		dst = append(dst, Property{Key: key, Value: val})
	}
	return dst, nil
}

// Scan decodes every row in segment order ((start, end, valid_from, id,
// revision) ascending) and calls fn until it returns false. It decodes page
// by page with fixed-size buffers.
func (s *Segment) Scan(fn func(i int, r *Row) bool) error {
	err := s.scanRange(0, s.rows, func(i int, r *Row) error {
		if !fn(i, r) {
			return errStopScan
		}
		return nil
	})
	if errors.Is(err, errStopScan) {
		return nil
	}
	return err
}

var errStopScan = errors.New("segment: scan stopped")

// smallRange is the row count up to which scanRange reads row by row.
const smallRange = 64

// scanRange decodes rows [lo, hi) in order.
func (s *Segment) scanRange(lo, hi int, fn func(i int, r *Row) error) error {
	if lo >= hi {
		return nil
	}
	if hi-lo <= smallRange {
		// A few rows (one small integrity group): random access beats
		// decoding whole pages of every column.
		for i := lo; i < hi; i++ {
			r, err := s.Row(i)
			if err != nil {
				return err
			}
			if err := fn(i, r); err != nil {
				return err
			}
		}
		return nil
	}
	return s.scanBatches(lo, hi, func(b *Batch) error {
		for k := max(0, lo-b.Base); k < b.N && b.Base+k < hi; k++ {
			r, err := b.row(k)
			if err != nil {
				return err
			}
			if err := fn(b.Base+k, r); err != nil {
				return err
			}
		}
		return nil
	})
}

// TimeCol names one of the four coordinate columns of a Batch.
type TimeCol uint8

// The coordinate columns.
const (
	TimeValidFrom TimeCol = iota
	TimeValidTo
	TimeTxFrom
	TimeTxTo
)

// Batch is one page of decoded columns, rows [Base, Base+N) in segment order.
// It is the single decode step of every bulk read: the row door builds rows
// from it (Row), and a columnar consumer reads the exported slices directly
// without a second decode (v4 ADR-0011 §5.3). The slices are owned by the
// scan and reused for the next page: a consumer must copy what it keeps.
type Batch struct {
	Base, N int
	// IDs, StartIDs, EndIDs and Revisions hold the page's system columns.
	// ValidFrom, ValidTo, TxFrom and TxTo hold the finite value of each
	// coordinate; for an infinity the slot is meaningless, read the kind with
	// Coord.
	IDs, StartIDs, EndIDs, Revisions, ValidFrom, ValidTo, TxFrom, TxTo []int64

	seg                              *Segment
	startOrds                        []int
	runStart                         []bool
	endOrd                           []int64
	vfK, vtK, txK, txToK             []int64
	propInts, propPresent, propCodes [][]int64
	v                                rowVals
}

// Coord returns coordinate col of batch row k. A stored kind no valid row can
// hold (a corrupt segment) reads as the invalid Coord.
func (b *Batch) Coord(col TimeCol, k int) Coord {
	var slot, kind int64
	var allowed []CoordKind
	switch col {
	case TimeValidFrom:
		slot, kind, allowed = b.ValidFrom[k], b.vfK[k], []CoordKind{CoordNegInf, CoordFinite}
	case TimeValidTo:
		slot, kind, allowed = b.ValidTo[k], b.vtK[k], []CoordKind{CoordFinite, CoordPosInf}
	case TimeTxFrom:
		slot, kind, allowed = b.TxFrom[k], b.txK[k], []CoordKind{CoordNegInf, CoordFinite}
	case TimeTxTo:
		slot, kind, allowed = b.TxTo[k], b.txToK[k], []CoordKind{CoordFinite, CoordPosInf}
	default:
		return Coord{}
	}
	c, _ := coordOf(slot, kind, allowed...)
	return c
}

// Columns returns the declared schema's columns, in the index order the
// per-column accessors use.
func (b *Batch) Columns() []Column {
	out := make([]Column, len(b.seg.props))
	for c := range b.seg.props {
		out[c] = b.seg.props[c].col
	}
	return out
}

// IntColumn returns declared column c's stored values for the page (the
// integer, bool 0/1 or float bits) and each row's presence. ok is false for a
// string column.
func (b *Batch) IntColumn(c int) (vals []int64, present func(k int) bool, ok bool) {
	p := &b.seg.props[c]
	if p.col.Kind == KindString {
		return nil, nil, false
	}
	return b.propInts[c][:b.N], b.presence(c), true
}

// StringColumn returns declared string column c as dictionary codes plus the
// dictionary they index. Callers must not modify it. ok is false when the
// column is not dictionary-encoded (plain strings, or a non-string column).
func (b *Batch) StringColumn(c int) (codes []int64, dict []string, present func(k int) bool, ok bool) {
	p := &b.seg.props[c]
	if p.col.Kind != KindString || !p.strs.coded() {
		return nil, nil, nil, false
	}
	return b.propCodes[c][:b.N], p.strs.dictionary(), b.presence(c), true
}

func (b *Batch) presence(c int) func(k int) bool {
	if b.seg.props[c].allSet {
		return func(int) bool { return true }
	}
	pr := b.propPresent[c]
	return func(k int) bool { return pr[k] == 1 }
}

// Row builds row k of the batch (segment position Base+k) as an independent
// Row.
func (b *Batch) Row(k int) (*Row, error) {
	if k < 0 || k >= b.N {
		return nil, fmt.Errorf("%w: batch row %d of %d", ErrOutOfRange, k, b.N)
	}
	return b.row(k)
}

func (b *Batch) row(k int) (*Row, error) {
	s, v := b.seg, &b.v
	v.startOrd, v.endOrd = b.startOrds[k], b.endOrd[k]
	v.id, v.revision = b.IDs[k], b.Revisions[k]
	v.vf, v.vt, v.tx, v.txTo = b.ValidFrom[k], b.ValidTo[k], b.TxFrom[k], b.TxTo[k]
	v.vfK, v.vtK, v.txK, v.txToK = b.vfK[k], b.vtK[k], b.txK[k], b.txToK[k]
	for c := range s.props {
		v.propInts[c] = b.propInts[c][k]
		v.propSet[c] = s.props[c].allSet || b.propPresent[c][k] == 1
	}
	return s.build(b.Base+k, v)
}

// ScanBatches decodes every page in segment order and calls fn with it
// until fn returns false (see Batch).
func (s *Segment) ScanBatches(fn func(b *Batch) bool) error {
	err := s.scanBatches(0, s.rows, func(b *Batch) error {
		if !fn(b) {
			return errStopScan
		}
		return nil
	})
	if errors.Is(err, errStopScan) {
		return nil
	}
	return err
}

func (s *Segment) scanBatches(lo, hi int, fn func(b *Batch) error) error {
	if lo >= hi {
		return nil
	}
	pr := s.pageRows
	buf := func() []int64 { return make([]int64, pr) }
	b := &Batch{
		seg: s, IDs: buf(), StartIDs: buf(), EndIDs: buf(), Revisions: buf(),
		ValidFrom: buf(), ValidTo: buf(), TxFrom: buf(), TxTo: buf(),
		startOrds: make([]int, pr), runStart: make([]bool, pr),
		endOrd: buf(), vfK: buf(), vtK: buf(), txK: buf(), txToK: buf(),
		propInts: make([][]int64, len(s.props)), propPresent: make([][]int64, len(s.props)), propCodes: make([][]int64, len(s.props)),
		v: rowVals{propInts: make([]int64, len(s.props)), propSet: make([]bool, len(s.props))},
	}
	for c := range s.props {
		b.propInts[c], b.propPresent[c] = buf(), buf()
		if p := &s.props[c]; p.col.Kind == KindString && p.strs.coded() {
			b.propCodes[c] = buf()
		}
	}
	for p := lo / pr; p*pr < hi; p++ {
		ps := p * pr
		cnt := min(pr, s.rows-ps)
		b.Base, b.N = ps, cnt
		// Start ordinals of the page's rows, from the out-CSR runs.
		o := s.startOrdinal(ps)
		for k := 0; k < cnt; k++ {
			for o < s.nodes-1 && s.outcsr.atNoRef(o+1) <= int64(ps+k) {
				o++
			}
			b.startOrds[k] = o
			b.runStart[k] = k == 0 || o != b.startOrds[k-1]
		}
		s.end.decodePage(p, cnt, b.endOrd, nil, b.runStart)
		s.vf.decodePage(p, cnt, b.ValidFrom, nil, nil)
		for _, c := range []struct {
			col *intCol
			out []int64
		}{{&s.vt, b.ValidTo}, {&s.tx, b.TxFrom}, {&s.txTo, b.TxTo}} {
			c.col.decodePage(p, cnt, c.out, b.ValidFrom, nil)
		}
		for _, c := range []struct {
			col *intCol
			out []int64
		}{{&s.relID, b.IDs}, {&s.revision, b.Revisions}, {&s.vfK, b.vfK}, {&s.vtK, b.vtK}, {&s.txK, b.txK}, {&s.txToK, b.txToK}} {
			c.col.decodePage(p, cnt, c.out, nil, nil)
		}
		for c := range s.props {
			s.props[c].ints.decodePage(p, cnt, b.propInts[c], nil, nil)
			s.props[c].present.decodePage(p, cnt, b.propPresent[c], nil, nil)
			if b.propCodes[c] != nil {
				s.props[c].strs.codes.decodePage(p, cnt, b.propCodes[c], nil, nil)
			}
		}
		for k := 0; k < cnt; k++ {
			b.StartIDs[k] = s.nodeIDs.atNoRef(b.startOrds[k])
			if e := b.endOrd[k]; e >= 0 && e < int64(s.nodes) {
				b.EndIDs[k] = s.nodeIDs.atNoRef(int(e))
			} else {
				return corrupt(colEnd, "row %d has end ordinal %d of %d", ps+k, e, s.nodes)
			}
		}
		if err := fn(b); err != nil {
			return err
		}
	}
	return nil
}

// IDRange returns the smallest and largest relationship ID in the segment
// (from the header; 0, 0 when empty).
func (s *Segment) IDRange() (lo, hi types.RelID) {
	if s.rows == 0 {
		return 0, 0
	}
	return types.RelID(s.idMin), types.RelID(s.idMax)
}

// Lookup returns the rows (segment order positions) holding relationship id,
// one per stored revision, ascending by revision. It binary-searches the
// sparse ID sample built at Open and reads at most one sample step of the ID
// index per match.
func (s *Segment) Lookup(id types.RelID) ([]int, error) {
	x := int64(id)
	if s.rows == 0 || x < s.idMin || x > s.idMax {
		return nil, nil
	}
	// The first sample >= x; the run of x (one entry per stored revision) may
	// begin inside the step before it.
	b := max(0, sort.Search(len(s.idSparse), func(b int) bool { return s.idSparse[b] >= x })-1)
	c := &s.idIDs
	var out []int
	j := b * idSparseStep
	for j < s.rows {
		p := j / c.pageRows
		v := c.page(p)
		base := p * c.pageRows
		k := j - base
		var val int64
		if v.transform == transformDeltaPrev {
			if j%idSparseStep == 0 {
				val = s.idSparse[j/idSparseStep]
			} else { // a page boundary inside the step: restart from the page
				val = v.first
				for kk := 1; kk <= k; kk++ {
					val += v.d(kk)
				}
			}
		} else {
			val = v.d(k)
		}
		for ; k < v.n; k++ {
			if k > j-base {
				if v.transform == transformDeltaPrev {
					val += v.d(k)
				} else {
					val = v.d(k)
				}
			}
			if val > x {
				return out, nil
			}
			if val == x {
				row := s.idRows.atNoRef(base + k)
				if row < 0 || row >= int64(s.rows) {
					return nil, corrupt(secIDRows, "entry %d names row %d of %d", base+k, row, s.rows)
				}
				out = append(out, int(row))
			}
		}
		j = base + v.n
	}
	return out, nil
}

// nodeOrdinal returns the ordinal of node n, or -1.
func (s *Segment) nodeOrdinal(n types.NodeID) int {
	x := int64(n)
	k := sort.Search(s.nodes, func(j int) bool { return s.nodeIDs.atNoRef(j) >= x })
	if k < s.nodes && s.nodeIDs.atNoRef(k) == x {
		return k
	}
	return -1
}

// OutRows returns the half-open row range [lo, hi) whose start node is n
// (empty when n starts no row of the segment).
func (s *Segment) OutRows(n types.NodeID) (lo, hi int, err error) {
	k := s.nodeOrdinal(n)
	if k < 0 {
		return 0, 0, nil
	}
	return int(s.outcsr.atNoRef(k)), int(s.outcsr.atNoRef(k + 1)), nil
}

// InRows returns the rows whose end node is n, ascending.
func (s *Segment) InRows(n types.NodeID) ([]int, error) {
	k := s.nodeOrdinal(n)
	if k < 0 {
		return nil, nil
	}
	lo, hi := int(s.inOff.atNoRef(k)), int(s.inOff.atNoRef(k+1))
	out := make([]int, 0, hi-lo)
	for j := lo; j < hi; j++ {
		row := s.inPerm.atNoRef(j)
		if row < 0 || row >= int64(s.rows) {
			return nil, corrupt(secInPerm, "entry %d names row %d of %d", j, row, s.rows)
		}
		out = append(out, int(row))
	}
	return out, nil
}

// VerifyGroup recomputes the hashes of integrity group g's rows from the
// segment and compares their SHA-256 root with the stored one.
func (s *Segment) VerifyGroup(g int) error {
	if g < 0 || g >= s.groups() {
		return fmt.Errorf("%w: integrity group %d of %d", ErrOutOfRange, g, s.groups())
	}
	return s.verifyGroups(g, g+1)
}

// Verify checks every integrity group and the segment root.
func (s *Segment) Verify() error {
	if err := s.verifyGroups(0, s.groups()); err != nil {
		return err
	}
	if sum := sha256.Sum256(s.integrity); !bytes.Equal(sum[:], s.root[:]) {
		return fmt.Errorf("%w: segment root", ErrIntegrity)
	}
	return nil
}

func (s *Segment) verifyGroups(g0, g1 int) error {
	b := s.blockRows
	leaf := make([]byte, 0, 32*b)
	var failed []string
	cur := g0
	flush := func() {
		sum := sha256.Sum256(leaf)
		if !bytes.Equal(sum[:], s.integrity[32*cur:32*cur+32]) {
			failed = append(failed, fmt.Sprint(cur))
		}
		leaf = leaf[:0]
		cur++
	}
	err := s.scanRange(g0*b, min(s.rows, g1*b), func(i int, r *Row) error {
		if i/b != cur {
			flush()
		}
		h := rowHash(s.schema.TypeName, r, r.Props)
		leaf = append(leaf, h[:]...)
		return nil
	})
	if err != nil {
		return err
	}
	if g1 > g0 {
		flush()
	}
	if len(failed) > 0 {
		return fmt.Errorf("%w: groups %s", ErrIntegrity, strings.Join(failed, ","))
	}
	return nil
}

// IDAt returns the relationship ID of row i (segment order) without building
// the row.
func (s *Segment) IDAt(i int) (types.RelID, error) {
	if i < 0 || i >= s.rows {
		return 0, fmt.Errorf("%w: row %d of %d", ErrOutOfRange, i, s.rows)
	}
	return types.RelID(s.relID.atNoRef(i)), nil
}

// ScanRange decodes rows [lo, hi) in segment order and calls fn until it
// returns false; rows are built as by Row.
func (s *Segment) ScanRange(lo, hi int, fn func(i int, r *Row) bool) error {
	if lo < 0 || hi > s.rows || lo > hi {
		return fmt.Errorf("%w: range [%d,%d) of %d", ErrOutOfRange, lo, hi, s.rows)
	}
	err := s.scanRange(lo, hi, func(i int, r *Row) error {
		if !fn(i, r) {
			return errStopScan
		}
		return nil
	})
	if errors.Is(err, errStopScan) {
		return nil
	}
	return err
}

// ForEachID calls fn with every (relationship ID, row) pair in ascending ID
// order (the ID index, decoded page by page without building any row) until
// fn returns false.
func (s *Segment) ForEachID(fn func(id types.RelID, row int) bool) error {
	if s.rows == 0 {
		return nil
	}
	pr := s.pageRows
	ids, rows := make([]int64, pr), make([]int64, pr)
	for p := 0; p*pr < s.rows; p++ {
		cnt := min(pr, s.rows-p*pr)
		s.idIDs.decodePage(p, cnt, ids, nil, nil)
		s.idRows.decodePage(p, cnt, rows, nil, nil)
		for k := 0; k < cnt; k++ {
			if rows[k] < 0 || rows[k] >= int64(s.rows) {
				return corrupt(secIDRows, "entry %d names row %d of %d", p*pr+k, rows[k], s.rows)
			}
			if !fn(types.RelID(ids[k]), int(rows[k])) {
				return nil
			}
		}
	}
	return nil
}

// SectionBytes returns every stored section's encoded size by name (the
// footer's section directory), a measurement surface for the byte ledger.
func (s *Segment) SectionBytes() map[string]int {
	out := make(map[string]int, len(s.secs))
	for name, sec := range s.secs {
		out[name] = sec.length
	}
	return out
}

// StringAt returns declared string column c's value for batch row k ("" when
// absent; see presence via StringColumn/IntColumn or HasValue).
func (b *Batch) StringAt(c, k int) (string, error) {
	if k < 0 || k >= b.N || c < 0 || c >= len(b.seg.props) || b.seg.props[c].col.Kind != KindString {
		return "", fmt.Errorf("%w: batch string column %d row %d", ErrOutOfRange, c, k)
	}
	return b.seg.props[c].strs.at(b.Base + k)
}

// HasValue reports whether batch row k holds declared column c with its
// declared kind.
func (b *Batch) HasValue(c, k int) bool {
	return b.seg.props[c].allSet || b.propPresent[c][k] == 1
}

// HasFallback reports whether batch row k carries properties outside the
// declared columns (an undeclared key, or a declared key whose value has
// another kind).
func (b *Batch) HasFallback(k int) bool {
	f := &b.seg.fallback
	return f.present && f.state.atNoRef(b.Base+k) == 1
}
