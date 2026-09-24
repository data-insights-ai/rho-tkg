package segment

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"slices"
	"sort"
	"strings"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/integrity"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
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
	typeToken uint16
	schema    Schema
	root      [32]byte
	idMin     int64
	idMax     int64
	secs      map[string]sectionInfo

	nodeIDs, outcsr, inOff, inPerm, idIDs, idRows intCol
	nodeHash, endpointExc                         strCol

	end, vf, vt, tx, relID, version, noTemporal     intCol
	txTo, createdAt, updatedAt, deletedAt           intCol
	baseEntity, authLevel, fromHash, toHash         intCol
	createdBy, updatedBy, prevHash, authorID, authz strCol
	signature, fallback                             bytesCol
	props                                           []propCol

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
		typeToken: uint16(h.typeToken),                                                              // #nosec G115 -- read from a u16
		schema:    Schema{TypeName: f.typeName, TypeToken: uint16(h.typeToken), Columns: f.columns}, // #nosec G115 -- u16
		root:      f.root,
		idMin:     h.idMin,
		idMax:     h.idMax,
		secs:      make(map[string]sectionInfo, len(f.sections)),
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
	return s, nil
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
		func() error { return strSec(&s.nodeHash, secNodeHash, nn) },
		func() error { return intSec(&s.outcsr, secOutCSR, nodeLevel1, none) },
		func() error { return intSec(&s.inOff, secInOff, nodeLevel1, none) },
		func() error { return intSec(&s.inPerm, secInPerm, n, none) },
		func() error { return intSec(&s.idIDs, secIDs, n, allow(transformNone, transformDeltaPrev)) },
		func() error { return intSec(&s.idRows, secIDRows, n, none) },
		func() error { return intSec(&s.end, colEnd, n, allow(transformNone, transformDeltaRun)) },
		func() error { return intSec(&s.vf, colValidFrom, n, none) },
		func() error { return intSec(&s.vt, colValidTo, n, vsVF) },
		func() error { return intSec(&s.tx, colTxFrom, n, vsVF) },
		func() error { return intSec(&s.relID, colRelID, n, none) },
		func() error { return intSec(&s.version, colVersion, n, none) },
		func() error { return intSec(&s.noTemporal, colNoTemporal, n, none) },
		func() error { return intSec(&s.txTo, colTxTo, n, vsVF) },
		func() error { return intSec(&s.createdAt, colCreatedAt, n, vsVF) },
		func() error { return intSec(&s.updatedAt, colUpdatedAt, n, vsVF) },
		func() error { return intSec(&s.deletedAt, colDeletedAt, n, vsVF) },
		func() error { return intSec(&s.baseEntity, colBaseEntity, n, none) },
		func() error { return intSec(&s.authLevel, colAuthLevel, n, none) },
		func() error { return intSec(&s.fromHash, colFromHash, n, none) },
		func() error { return intSec(&s.toHash, colToHash, n, none) },
		func() error { return strSec(&s.endpointExc, secEndpointExc, -1) },
		func() error { return strSec(&s.createdBy, colCreatedBy, n) },
		func() error { return strSec(&s.updatedBy, colUpdatedBy, n) },
		func() error { return strSec(&s.prevHash, colPrevHash, n) },
		func() error { return strSec(&s.authorID, colAuthorID, n) },
		func() error { return strSec(&s.authz, colAuthorizedBy, n) },
		func() error { return bytesSec(&s.signature, colSignature) },
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
// node IDs strictly ascending, CSR offsets running from 0 to rowCount. Time
// is linear in the node count (bounded by 2 × rows, and rows by the integrity
// section's bytes); nothing is allocated.
func (s *Segment) validateStructure() error {
	if s.nodes == 0 {
		return nil
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

// Schema returns the declared schema (type name from the footer, token from
// the header).
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

// Row decodes row i (segment order). The row is unfrozen and independent;
// its Integrity().Hash is recomputed from the decoded columns.
func (s *Segment) Row(i int) (*types.Relationship, error) {
	if i < 0 || i >= s.rows {
		return nil, fmt.Errorf("%w: row %d of %d", ErrOutOfRange, i, s.rows)
	}
	so := s.startOrdinal(i)
	eo := s.endOrdinalAt(i, so)
	v := rowVals{startOrd: so, endOrd: eo}
	vf := s.vf.atNoRef(i)
	v.vf = vf
	v.vt = atRef(&s.vt, i, vf)
	v.tx = atRef(&s.tx, i, vf)
	v.id, v.version, v.noTemporal = s.relID.atNoRef(i), s.version.atNoRef(i), s.noTemporal.atNoRef(i)
	v.txTo, v.createdAt = atRef(&s.txTo, i, vf), atRef(&s.createdAt, i, vf)
	v.updatedAt, v.deletedAt = atRef(&s.updatedAt, i, vf), atRef(&s.deletedAt, i, vf)
	v.baseEntity, v.authLevel = s.baseEntity.atNoRef(i), s.authLevel.atNoRef(i)
	v.fromCode, v.toCode = s.fromHash.atNoRef(i), s.toHash.atNoRef(i)
	v.propInts = make([]int64, len(s.props))
	v.propSet = make([]bool, len(s.props))
	for c := range s.props {
		p := &s.props[c]
		v.propSet[c] = p.allSet || p.present.atNoRef(i) == 1
		if p.col.Kind != KindString {
			v.propInts[c] = p.ints.atNoRef(i)
		}
	}
	return s.build(i, &v, true)
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

// rowVals is one row's column values before they become a Relationship.
type rowVals struct {
	startOrd                                int
	endOrd                                  int64
	id, version, vf, vt, tx, noTemporal     int64
	txTo, createdAt, updatedAt, deletedAt   int64
	baseEntity, authLevel, fromCode, toCode int64
	propInts                                []int64
	propSet                                 []bool
	// Scan-lifetime scratch: the property buffer (SetProperties copies it)
	// and the decoded usual hash per node ordinal (strings are immutable, so
	// rows may share them). Nil for a single Row.
	scratch   types.PropertySlice
	nodeHashs []string
	nodeKnown []bool
}

// kindValue turns a stored int64 back into a value of kind k, rejecting a
// value the kind cannot hold (a corrupt column, never a truncation).
func kindValue(k Kind, x int64) (any, bool) {
	switch k {
	case KindBool:
		return x == 1, x == 0 || x == 1
	case KindInt:
		return int(x), true
	case KindInt8:
		return int8(x), x >= math.MinInt8 && x <= math.MaxInt8 // #nosec G115 -- range checked
	case KindInt16:
		return int16(x), x >= math.MinInt16 && x <= math.MaxInt16 // #nosec G115 -- range checked
	case KindInt32:
		return int32(x), x >= math.MinInt32 && x <= math.MaxInt32 // #nosec G115 -- range checked
	case KindInt64:
		return x, true
	case KindUint:
		return uint(x), true // #nosec G115 -- bit-cast back
	case KindUint8:
		return uint8(x), x >= 0 && x <= math.MaxUint8 // #nosec G115 -- range checked
	case KindUint16:
		return uint16(x), x >= 0 && x <= math.MaxUint16 // #nosec G115 -- range checked
	case KindUint32:
		return uint32(x), x >= 0 && x <= math.MaxUint32 // #nosec G115 -- range checked
	case KindUint64:
		return uint64(x), true // #nosec G115 -- bit-cast back
	case KindFloat32:
		return math.Float32frombits(uint32(x)), x >= 0 && x <= math.MaxUint32 // #nosec G115 -- range checked
	case KindFloat64:
		return math.Float64frombits(uint64(x)), true // #nosec G115 -- bit pattern
	}
	return nil, false
}

// build assembles row i from its column values. withHash false leaves
// Integrity().Hash empty (measurement only; every public door hashes).
func (s *Segment) build(i int, v *rowVals, withHash bool) (*types.Relationship, error) {
	if v.endOrd < 0 || v.endOrd >= int64(s.nodes) || v.startOrd >= s.nodes {
		return nil, corrupt(colEnd, "row %d has node ordinals %d->%d of %d", i, v.startOrd, v.endOrd, s.nodes)
	}
	r := types.NewRelationship(types.RelID(v.id), s.typeToken,
		types.NodeID(s.nodeIDs.atNoRef(v.startOrd)), types.NodeID(s.nodeIDs.atNoRef(int(v.endOrd))))
	props := v.scratch[:0]
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
		props = append(props, types.Property{Key: p.col.Name, Value: val})
	}
	fb, err := s.fallback.at(i)
	if err != nil {
		return nil, err
	}
	if fb != nil {
		extra, err := storeutil.UnmarshalPropertySlice(fb)
		if err != nil {
			return nil, corrupt(secFallback, "row %d: %v", i, err)
		}
		props = append(props, extra...)
	}
	slices.SortFunc(props, func(a, b types.Property) int { return strings.Compare(a.Key, b.Key) })
	v.scratch = props[:0]
	for k := 1; k < len(props); k++ {
		if props[k].Key == props[k-1].Key {
			return nil, corrupt(secFallback, "row %d holds property %q twice", i, props[k].Key)
		}
	}
	if err := r.SetProperties(props); err != nil {
		return nil, corrupt("row", "row %d properties: %v", i, err)
	}
	if v.version < 0 || v.version > math.MaxUint32 {
		return nil, corrupt(colVersion, "row %d version %d", i, v.version)
	}
	r.SetVersion(uint32(v.version)) // #nosec G115 -- range checked
	switch v.noTemporal {
	case 0:
		tm := &types.TemporalMetadata{
			ValidFrom: types.Instant(v.vf), ValidTo: types.Instant(v.vt), TxFrom: types.Instant(v.tx),
			TxTo: types.Instant(v.txTo), CreatedAt: types.Instant(v.createdAt), UpdatedAt: types.Instant(v.updatedAt),
			DeletedAt: types.Instant(v.deletedAt),
		}
		if tm.CreatedBy, err = s.createdBy.at(i); err != nil {
			return nil, err
		}
		if tm.UpdatedBy, err = s.updatedBy.at(i); err != nil {
			return nil, err
		}
		if v.baseEntity != 0 {
			tm.SetBaseEntityID(types.EntityID(v.baseEntity))
		}
		r.SetTemporal(tm)
	case 1:
	default:
		return nil, corrupt(colNoTemporal, "row %d flag %d", i, v.noTemporal)
	}
	if v.authLevel < 0 || v.authLevel > math.MaxUint8 {
		return nil, corrupt(colAuthLevel, "row %d level %d", i, v.authLevel)
	}
	ig := &types.RelIntegrity{AuthorizationLevel: uint8(v.authLevel)} // #nosec G115 -- range checked
	if ig.FromNodeHash, err = s.endpointHash(v, colFromHash, i, v.fromCode, v.startOrd); err != nil {
		return nil, err
	}
	if ig.ToNodeHash, err = s.endpointHash(v, colToHash, i, v.toCode, int(v.endOrd)); err != nil {
		return nil, err
	}
	for _, f := range []struct {
		dst *string
		col *strCol
	}{{&ig.PrevHash, &s.prevHash}, {&ig.AuthorID, &s.authorID}, {&ig.AuthorizedBy, &s.authz}} {
		if *f.dst, err = f.col.at(i); err != nil {
			return nil, err
		}
	}
	if ig.Signature, err = s.signature.at(i); err != nil {
		return nil, err
	}
	if withHash {
		if ig.Hash, err = integrity.ComputeRelHashChecked(r, s.schema.TypeName); err != nil {
			return nil, corrupt("row", "row %d hash: %v", i, err)
		}
	}
	r.SetIntegrity(ig)
	return r, nil
}

func (s *Segment) endpointHash(v *rowVals, section string, i int, code int64, ord int) (string, error) {
	if code == 0 {
		if v.nodeKnown == nil {
			return s.nodeHash.at(ord)
		}
		if !v.nodeKnown[ord] {
			h, err := s.nodeHash.at(ord)
			if err != nil {
				return "", err
			}
			v.nodeHashs[ord], v.nodeKnown[ord] = h, true
		}
		return v.nodeHashs[ord], nil
	}
	n := int64(0)
	if s.endpointExc.present {
		n = int64(s.endpointExc.n)
	}
	if code < 0 || code > n {
		return "", corrupt(section, "row %d endpoint-hash code %d of %d", i, code, n)
	}
	return s.endpointExc.at(int(code - 1))
}

// Scan decodes every row in segment order ((start, end, valid_from, id,
// version) ascending) and calls fn until it returns false. It decodes page
// by page with fixed-size buffers.
func (s *Segment) Scan(fn func(i int, r *types.Relationship) bool) error {
	err := s.scanRange(0, s.rows, func(i int, r *types.Relationship) error {
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
func (s *Segment) scanRange(lo, hi int, fn func(i int, r *types.Relationship) error) error {
	return s.scan(lo, hi, true, fn)
}

func (s *Segment) scan(lo, hi int, withHash bool, fn func(i int, r *types.Relationship) error) error {
	if lo >= hi {
		return nil
	}
	if withHash && hi-lo <= smallRange {
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
			r, err := b.row(k, withHash)
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

// Batch is one page of decoded columns, rows [Base, Base+N) in segment order.
// It is the single decode step of every bulk read: the row door builds
// relationships from it (Row), and a columnar consumer reads the exported
// slices directly without a second decode (ADR-0011 §5.3). The slices are
// owned by the scan and reused for the next page: a consumer must copy what
// it keeps.
type Batch struct {
	Base, N int
	// IDs, StartIDs, EndIDs, ValidFrom, ValidTo, TxFrom and Versions hold the
	// page's system columns as stored (raw ValidFrom 0 stays 0).
	IDs, StartIDs, EndIDs, ValidFrom, ValidTo, TxFrom, Versions []int64

	seg                                      *Segment
	startOrds                                []int
	runStart                                 []bool
	endOrd, noT, txTo, ca, ua, da, base, aut []int64
	fc, tc                                   []int64
	propInts, propPresent                    [][]int64
	v                                        rowVals
}

// Row builds row k of the batch (segment position Base+k) as an unfrozen,
// independent relationship whose Integrity().Hash is recomputed from the
// columns.
func (b *Batch) Row(k int) (*types.Relationship, error) {
	if k < 0 || k >= b.N {
		return nil, fmt.Errorf("%w: batch row %d of %d", ErrOutOfRange, k, b.N)
	}
	return b.row(k, true)
}

func (b *Batch) row(k int, withHash bool) (*types.Relationship, error) {
	s, v := b.seg, &b.v
	v.startOrd, v.endOrd = b.startOrds[k], b.endOrd[k]
	v.id, v.version, v.vf, v.vt, v.tx, v.noTemporal = b.IDs[k], b.Versions[k], b.ValidFrom[k], b.ValidTo[k], b.TxFrom[k], b.noT[k]
	v.txTo, v.createdAt, v.updatedAt, v.deletedAt = b.txTo[k], b.ca[k], b.ua[k], b.da[k]
	v.baseEntity, v.authLevel, v.fromCode, v.toCode = b.base[k], b.aut[k], b.fc[k], b.tc[k]
	for c := range s.props {
		v.propInts[c] = b.propInts[c][k]
		v.propSet[c] = s.props[c].allSet || b.propPresent[c][k] == 1
	}
	return s.build(b.Base+k, v, withHash)
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
		seg: s, IDs: buf(), StartIDs: buf(), EndIDs: buf(), ValidFrom: buf(), ValidTo: buf(), TxFrom: buf(), Versions: buf(),
		startOrds: make([]int, pr), runStart: make([]bool, pr),
		endOrd: buf(), noT: buf(), txTo: buf(), ca: buf(), ua: buf(), da: buf(), base: buf(), aut: buf(), fc: buf(), tc: buf(),
		propInts: make([][]int64, len(s.props)), propPresent: make([][]int64, len(s.props)),
		v: rowVals{propInts: make([]int64, len(s.props)), propSet: make([]bool, len(s.props)),
			nodeHashs: make([]string, s.nodes), nodeKnown: make([]bool, s.nodes)},
	}
	for c := range s.props {
		b.propInts[c], b.propPresent[c] = buf(), buf()
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
		}{{&s.vt, b.ValidTo}, {&s.tx, b.TxFrom}, {&s.txTo, b.txTo}, {&s.createdAt, b.ca}, {&s.updatedAt, b.ua}, {&s.deletedAt, b.da}} {
			c.col.decodePage(p, cnt, c.out, b.ValidFrom, nil)
		}
		for _, c := range []struct {
			col *intCol
			out []int64
		}{{&s.relID, b.IDs}, {&s.version, b.Versions}, {&s.noTemporal, b.noT}, {&s.baseEntity, b.base}, {&s.authLevel, b.aut}, {&s.fromHash, b.fc}, {&s.toHash, b.tc}} {
			c.col.decodePage(p, cnt, c.out, nil, nil)
		}
		for c := range s.props {
			s.props[c].ints.decodePage(p, cnt, b.propInts[c], nil, nil)
			s.props[c].present.decodePage(p, cnt, b.propPresent[c], nil, nil)
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
// one per stored version, ascending by version. It binary-searches the ID
// index page by page (header range, page first values) and reads one page.
func (s *Segment) Lookup(id types.RelID) ([]int, error) {
	x := int64(id)
	if s.rows == 0 || x < s.idMin || x > s.idMax {
		return nil, nil
	}
	c := &s.idIDs
	// The first page whose first value is >= x; the run of x (one entry per
	// stored version) may begin on the page before it.
	p := max(0, sort.Search(c.pageCount, func(p int) bool { return c.pageFirst(p) >= x })-1)
	var out []int
	for ; p < c.pageCount; p++ {
		v := c.page(p)
		base := p * c.pageRows
		k, val := 0, int64(0)
		if v.transform == transformDeltaPrev {
			val = v.first
		} else {
			k = sort.Search(v.n, func(j int) bool { return v.d(j) >= x })
			if k < v.n {
				val = v.d(k)
			}
		}
		for ; k < v.n; k++ {
			if v.transform == transformDeltaPrev {
				if k > 0 {
					val += v.d(k)
				}
			} else {
				val = v.d(k)
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
	err := s.scanRange(g0*b, min(s.rows, g1*b), func(i int, r *types.Relationship) error {
		if i/b != cur {
			flush()
		}
		h, err := hex.DecodeString(r.Integrity().Hash)
		if err != nil {
			return corrupt("row", "row %d hash: %v", i, err)
		}
		leaf = append(leaf, h...)
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
// the row. i must be in [0, Len()).
func (s *Segment) IDAt(i int) (types.RelID, error) {
	if i < 0 || i >= s.rows {
		return 0, fmt.Errorf("%w: row %d of %d", ErrOutOfRange, i, s.rows)
	}
	return types.RelID(s.relID.atNoRef(i)), nil
}

// ScanRange decodes rows [lo, hi) in segment order and calls fn until it
// returns false; rows are built as by Row.
func (s *Segment) ScanRange(lo, hi int, fn func(i int, r *types.Relationship) bool) error {
	if lo < 0 || hi > s.rows || lo > hi {
		return fmt.Errorf("%w: range [%d,%d) of %d", ErrOutOfRange, lo, hi, s.rows)
	}
	err := s.scanRange(lo, hi, func(i int, r *types.Relationship) error {
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
// order — the ID index, decoded page by page without building any row —
// until fn returns false.
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
