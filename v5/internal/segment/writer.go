package segment

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"
	"slices"
	"sort"

	"github.com/data-insights-ai/rho-tkg/v5/pkg/types"
)

// maxRows is MaxRows; a variable so tests can exercise the bound.
var maxRows = MaxRows

// Encode writes rows as one segment of schema s. The input slice and its
// property slices are not modified; rows may come in any order and are sorted
// into segment order. See the package comment for the contract; the returned
// bytes have already been re-opened and every row decoded and compared with
// its source.
func Encode(s Schema, rows []Row, opts Options) ([]byte, error) {
	return encode(s, rows, opts, PageRows)
}

type namedSection struct {
	name string
	data []byte
}

func encode(s Schema, rows []Row, opts Options, pageRows int) ([]byte, error) {
	if err := s.validate(); err != nil {
		return nil, err
	}
	blockRows := opts.IntegrityBlockRows
	if blockRows == 0 {
		blockRows = DefaultIntegrityBlockRows
	}
	if !pow2UpTo4096(blockRows) {
		return nil, fmt.Errorf("%w: IntegrityBlockRows %d is not a power of two in [1,%d]", ErrInvalidOptions, blockRows, MaxIntegrityBlockRows)
	}
	if !pow2UpTo4096(pageRows) {
		return nil, fmt.Errorf("%w: page size %d", ErrInvalidOptions, pageRows)
	}
	n := len(rows)
	if n > maxRows {
		return nil, fmt.Errorf("%w: %d rows exceed the segment maximum %d", ErrInvalidRow, n, maxRows)
	}
	props := make([][]Property, n) // sorted copies, parallel to rows
	for i := range rows {
		r := &rows[i]
		if err := r.validate(); err != nil {
			return nil, rowErr(r, ErrInvalidRow, "row %d: %v", i, err)
		}
		ps, err := sortedProps(r.Props)
		if err != nil {
			return nil, rowErr(r, ErrInvalidRow, "row %d (id %d): %v", i, r.ID, err)
		}
		props[i] = ps
	}

	// Segment order, and the (id, revision) key must be unique.
	order := make([]int, n)
	for i := range order {
		order[i] = i
	}
	sort.Slice(order, func(a, b int) bool { return lessRow(&rows[order[a]], &rows[order[b]]) })
	byID := make([]int, n) // positions in segment order, sorted by (id, revision, position)
	for i := range byID {
		byID[i] = i
	}
	sort.Slice(byID, func(a, b int) bool {
		ra, rb := &rows[order[byID[a]]], &rows[order[byID[b]]]
		if ra.ID != rb.ID {
			return ra.ID < rb.ID
		}
		if ra.Revision != rb.Revision {
			return ra.Revision < rb.Revision
		}
		return byID[a] < byID[b]
	})
	for k := 1; k < n; k++ {
		ra, rb := &rows[order[byID[k-1]]], &rows[order[byID[k]]]
		if ra.ID == rb.ID && ra.Revision == rb.Revision {
			return nil, rowErr(rb, ErrInvalidRow, "id %d revision %d appears twice", rb.ID, rb.Revision)
		}
	}

	// Node table.
	nodeSet := make(map[types.NodeID]struct{})
	for i := range rows {
		nodeSet[rows[i].Start] = struct{}{}
		nodeSet[rows[i].End] = struct{}{}
	}
	nodes := make([]int64, 0, len(nodeSet))
	for id := range nodeSet {
		nodes = append(nodes, int64(id))
	}
	slices.Sort(nodes)
	ordOf := make(map[types.NodeID]int, len(nodes))
	for i, id := range nodes {
		ordOf[types.NodeID(id)] = i
	}

	colIdx := make(map[string]int, len(s.Columns))
	for i, c := range s.Columns {
		colIdx[c.Name] = i
	}
	w := newColumns(n, len(nodes), s.Columns)
	for i, src := range order {
		r := &rows[src]
		w.startOrd[i] = ordOf[r.Start]
		w.end[i] = int64(ordOf[r.End])
		w.relID[i] = int64(r.ID)
		w.revision[i] = int64(r.Revision)
		w.setCoords(i, r)
		w.leaves[i] = rowHash(s.TypeName, r, props[src])
		var fallback []byte
		for _, p := range props[src] {
			if c, ok := colIdx[p.Key]; ok && kindOf(p.Value) == s.Columns[c].Kind {
				w.present[c][i] = 1
				if s.Columns[c].Kind == KindString {
					w.strs[c][i], _ = p.Value.StringValue()
				} else {
					w.ints[c][i] = valueBits(p.Value)
				}
				continue
			}
			fallback = appendFallback(fallback, p)
		}
		w.fallback[i] = fallback
	}
	out := w.assemble(s, blockRows, pageRows, nodes, byID)
	if err := verifySealed(out, rows, order); err != nil {
		return nil, err
	}
	return out, nil
}

// appendFallback appends one property as u16 key length | key | tagged value
// (appendValue). Keys and values were validated by sortedProps.
func appendFallback(b []byte, p Property) []byte {
	b = binary.LittleEndian.AppendUint16(b, uint16(len(p.Key))) // #nosec G115 -- validated <= 65535
	b = append(b, p.Key...)
	b, _ = appendValue(b, p.Value)
	return b
}

// columns holds the writer's row-major-to-column staging arrays.
type columns struct {
	n, nodes             int
	startOrd             []int
	end, relID, revision []int64
	vf, vt, tx, txTo     []int64 // finite value, or (vt/tx/txTo) the valid_from slot for an infinity
	vfK, vtK, txK, txToK []int64 // kind - CoordFinite: -1 -inf, 0 finite, 1 +inf
	leaves               [][32]byte
	fallback             [][]byte
	ints                 [][]int64
	strs                 [][]string
	present              [][]int64
}

func newColumns(n, nodes int, cols []Column) *columns {
	i64 := func() []int64 { return make([]int64, n) }
	w := &columns{
		n: n, nodes: nodes, startOrd: make([]int, n),
		end: i64(), relID: i64(), revision: i64(), vf: i64(), vt: i64(), tx: i64(), txTo: i64(),
		vfK: i64(), vtK: i64(), txK: i64(), txToK: i64(),
		leaves: make([][32]byte, n), fallback: make([][]byte, n),
		ints: make([][]int64, len(cols)), strs: make([][]string, len(cols)), present: make([][]int64, len(cols)),
	}
	for c, col := range cols {
		w.present[c] = i64()
		if col.Kind == KindString {
			w.strs[c] = make([]string, n)
		} else {
			w.ints[c] = i64()
		}
	}
	return w
}

// setCoords stores row i's four coordinates: the finite value in the value
// column, or, for an infinity, the valid_from slot (so the per-page
// minus-reference delta is 0) with the kind in the kind column.
func (w *columns) setCoords(i int, r *Row) {
	put := func(val, kind []int64, c Coord, ref int64) {
		kind[i] = int64(c.kind) - int64(CoordFinite)
		if c.kind == CoordFinite {
			val[i] = c.v
		} else {
			val[i] = ref
		}
	}
	put(w.vf, w.vfK, r.ValidFrom, 0)
	ref := w.vf[i]
	put(w.vt, w.vtK, r.ValidTo, ref)
	put(w.tx, w.txK, r.TxFrom, ref)
	put(w.txTo, w.txToK, r.TxTo, ref)
}

func (w *columns) assemble(s Schema, blockRows, pageRows int, nodes []int64, byID []int) []byte {
	n := w.n
	var secs []namedSection
	addInt := func(name string, vals []int64, ctx intEncodeCtx) {
		if !allZero(vals) {
			secs = append(secs, namedSection{name, encodeIntCol(vals, pageRows, ctx)})
		}
	}
	addBytes := func(name string, vals [][]byte) {
		if !allNil(vals) {
			secs = append(secs, namedSection{name, encodeBytesCol(vals, pageRows)})
		}
	}
	none := intEncodeCtx{allowed: allow(transformNone)}
	vsVF := intEncodeCtx{allowed: allow(transformNone, transformMinusRef), ref: w.vf}

	// Node-level sections.
	addInt(secNodes, nodes, none)
	outcsr := make([]int64, w.nodes+1)
	inOff := make([]int64, w.nodes+1)
	for i := 0; i < n; i++ {
		outcsr[w.startOrd[i]+1]++
		inOff[w.end[i]+1]++
	}
	for k := 1; k <= w.nodes; k++ {
		outcsr[k] += outcsr[k-1]
		inOff[k] += inOff[k-1]
	}
	perm := make([]int64, n)
	next := append([]int64(nil), inOff[:max(w.nodes, 1)]...)
	for i := 0; i < n; i++ {
		e := w.end[i]
		perm[next[e]] = int64(i)
		next[e]++
	}
	addInt(secOutCSR, outcsr, none)
	addInt(secInOff, inOff, none)
	addInt(secInPerm, perm, none)
	ids := make([]int64, n)
	idRows := make([]int64, n)
	for k, pos := range byID {
		ids[k], idRows[k] = w.relID[pos], int64(pos)
	}
	addInt(secIDs, ids, intEncodeCtx{allowed: allow(transformNone, transformDeltaPrev)})
	addInt(secIDRows, idRows, none)

	// Integrity roots over the row hashes, in segment order.
	root := sha256.New()
	if n > 0 {
		var roots []byte
		leaf := make([]byte, 0, 32*blockRows)
		for g := 0; g*blockRows < n; g++ {
			leaf = leaf[:0]
			for i := g * blockRows; i < min(n, (g+1)*blockRows); i++ {
				leaf = append(leaf, w.leaves[i][:]...)
			}
			sum := sha256.Sum256(leaf)
			roots = append(roots, sum[:]...)
		}
		root.Write(roots)
		secs = append(secs, namedSection{secIntegrity, roots})
		secs = append(secs, namedSection{secPageIndex, w.pageIndex(pageRows)})
	}

	// Row columns.
	runStart := func(i int) bool { return i == 0 || w.startOrd[i] != w.startOrd[i-1] }
	addInt(colEnd, w.end, intEncodeCtx{allowed: allow(transformNone, transformDeltaRun), runStart: runStart})
	addInt(colValidFrom, w.vf, none)
	addInt(colValidTo, w.vt, vsVF)
	addInt(colTxFrom, w.tx, vsVF)
	addInt(colTxTo, w.txTo, vsVF)
	addInt(colValidFromKind, w.vfK, none)
	addInt(colValidToKind, w.vtK, none)
	addInt(colTxFromKind, w.txK, none)
	addInt(colTxToKind, w.txToK, none)
	addInt(colRelID, w.relID, none)
	addInt(colRevision, w.revision, none)
	for c, col := range s.Columns {
		if col.Kind == KindString {
			if !allEmpty(w.strs[c]) {
				secs = append(secs, namedSection{propPrefix + col.Name, encodeStrCol(w.strs[c], pageRows)})
			}
		} else {
			addInt(propPrefix+col.Name, w.ints[c], none)
		}
		if !allOnes(w.present[c]) {
			secs = append(secs, namedSection{presencePrefix + col.Name, encodeIntCol(w.present[c], pageRows, none)})
		}
	}
	addBytes(secFallback, w.fallback)

	// Header, sections, footer, trailer.
	h := header{format: CurrentFormatVersion, pageRows: pageRows, rows: n, nodes: w.nodes}
	if n > 0 {
		h.idMin, h.idMax = math.MaxInt64, math.MinInt64
		h.vfMin, h.vfMax = math.MaxInt64, math.MinInt64
		haveVF := false
		for i := 0; i < n; i++ {
			h.idMin, h.idMax = min(h.idMin, w.relID[i]), max(h.idMax, w.relID[i])
			if w.vfK[i] == 0 {
				h.vfMin, h.vfMax, haveVF = min(h.vfMin, w.vf[i]), max(h.vfMax, w.vf[i]), true
			}
		}
		if !haveVF {
			h.vfMin, h.vfMax = 0, 0
		}
	}
	hb := putHeader(h)
	f := footer{version: CurrentFooterVersion, blockRows: blockRows, typeName: s.TypeName, columns: s.Columns}
	copy(f.root[:], root.Sum(nil))
	off := len(hb)
	for _, sec := range secs {
		f.sections = append(f.sections, sectionInfo{name: sec.name, offset: off, length: len(sec.data), crc: crc32.Checksum(sec.data, castagnoli)})
		off += len(sec.data)
	}
	fb := putFooter(f)
	// One exact-size allocation: an in-RAM segment keeps these bytes for its
	// lifetime, so append's growth slack would be resident too.
	out := make([]byte, 0, off+len(fb)+trailerSize)
	out = append(out, hb...)
	for _, sec := range secs {
		out = append(out, sec.data...)
	}
	out = append(out, fb...)
	out = binary.LittleEndian.AppendUint32(out, uint32(len(fb))) // #nosec G115 -- a footer is a few KB
	out = binary.LittleEndian.AppendUint32(out, crc32.Checksum(fb, castagnoli))
	return append(out, trailerMagic...)
}

func allOnes(vals []int64) bool {
	for _, v := range vals {
		if v != 1 {
			return false
		}
	}
	return true
}

// pageIndex writes one pageIndexEntry per page: min/max over the finite
// values of valid_from, valid_to, tx_from, tx_to and rel_id, and a flags byte
// (bit k: some value of coordinate k is not finite).
func (w *columns) pageIndex(pageRows int) []byte {
	var out []byte
	for lo := 0; lo < w.n; lo += pageRows {
		hi := min(lo+pageRows, w.n)
		var mm [10]int64
		for k := 0; k < 10; k += 2 {
			mm[k], mm[k+1] = math.MaxInt64, math.MinInt64
		}
		var flags byte
		for i := lo; i < hi; i++ {
			cols := [5]struct{ v, kind int64 }{{w.vf[i], w.vfK[i]}, {w.vt[i], w.vtK[i]}, {w.tx[i], w.txK[i]}, {w.txTo[i], w.txToK[i]}, {w.relID[i], 0}}
			for k, c := range cols {
				if c.kind != 0 {
					flags |= 1 << k
					continue
				}
				mm[2*k], mm[2*k+1] = min(mm[2*k], c.v), max(mm[2*k+1], c.v)
			}
		}
		for k := 0; k < 10; k += 2 {
			if mm[k] > mm[k+1] { // no finite value in the page
				mm[k], mm[k+1] = 0, 0
			}
		}
		for _, v := range mm {
			out = binary.LittleEndian.AppendUint64(out, uint64(v)) // #nosec G115 -- bit pattern
		}
		out = append(out, flags)
	}
	return out
}

// verifySealed re-opens the written bytes and requires every decoded row to
// equal its source row and to recompute the hash of its source (sealing
// refuses on any mismatch: the encoder is checked like any other writer), and
// the stored integrity roots to verify.
func verifySealed(data []byte, rows []Row, order []int) error {
	return verifySealedHash(data, "", rows, order)
}

// verifySealedHash is verifySealed with the type name the source hashes are
// computed under ("" means the segment's own).
func verifySealedHash(data []byte, typeName string, rows []Row, order []int) error {
	seg, err := Open(data)
	if err != nil {
		return fmt.Errorf("segment: writer produced a segment its reader rejects: %w", err)
	}
	if typeName == "" {
		typeName = seg.schema.TypeName
	}
	err = seg.scanRange(0, seg.rows, func(i int, got *Row) error {
		want := rows[order[i]]
		ps, err := sortedProps(want.Props)
		if err != nil {
			return rowErr(&want, ErrInvalidRow, "id %d: %v", want.ID, err)
		}
		want.Props = ps
		if diff := rowDiff(&want, got); diff != "" {
			return rowErr(&want, ErrInvalidRow, "id %d revision %d does not round-trip: %s", want.ID, want.Revision, diff)
		}
		if rowHash(typeName, &want, ps) != rowHash(seg.schema.TypeName, got, got.Props) {
			return rowErr(&want, ErrHashMismatch, "id %d revision %d: decoded row hashes differently", want.ID, want.Revision)
		}
		return nil
	})
	if err != nil {
		return err
	}
	return seg.Verify()
}
