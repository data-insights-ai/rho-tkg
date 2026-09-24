package segment

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"math"
	"sort"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Encode writes rows as one segment of schema s. Rows are taken as a store
// holds them: each must carry schema.TypeToken and its stored integrity Hash
// (64 lowercase hex characters). The input slice is not modified. See the
// package comment for the contract; the returned bytes have already been
// re-opened and every row decoded and compared with its source.
func Encode(s Schema, rows []*types.Relationship, opts Options) ([]byte, error) {
	return encode(s, rows, opts, PageRows, nil)
}

// EncodeWithDicts is Encode for a segment that stores codes into the
// store-level dictionaries d (see Dicts) instead of its own endpoint table,
// endpoint hashes and string dictionaries. It interns into d (d only grows)
// and verifies the result by reopening it with d. The segment must be opened
// with OpenWithDicts and the same d.
func EncodeWithDicts(s Schema, rows []*types.Relationship, opts Options, d *Dicts) ([]byte, error) {
	if d == nil {
		return nil, fmt.Errorf("%w: nil dictionaries", ErrInvalidOptions)
	}
	return encode(s, rows, opts, PageRows, d)
}

// ValueBits maps a declared-kind value to the int64 a column stores: the
// integer (uint64 bit-cast), 0/1 for bool, the IEEE-754 bits for floats
// (0 for any other value).
func ValueBits(v any) int64 { return valueBits(v) }

// valueBits is ValueBits.
func valueBits(v any) int64 {
	switch x := v.(type) {
	case bool:
		if x {
			return 1
		}
		return 0
	case int:
		return int64(x)
	case int8:
		return int64(x)
	case int16:
		return int64(x)
	case int32:
		return int64(x)
	case int64:
		return x
	case uint:
		return int64(x) // #nosec G115 -- bit-cast, restored by kindValue
	case uint8:
		return int64(x)
	case uint16:
		return int64(x)
	case uint32:
		return int64(x)
	case uint64:
		return int64(x) // #nosec G115 -- bit-cast, restored by kindValue
	case float32:
		return int64(math.Float32bits(x))
	case float64:
		return int64(math.Float64bits(x)) // #nosec G115 -- bit pattern
	}
	return 0
}

func kindOf(v any) Kind { return Kind(types.PropertyHashTypeTag(v)) }

type rowSorter struct {
	rows  []*types.Relationship
	order []int
	vf    []int64
}

func (s *rowSorter) less(a, b int) bool {
	ra, rb := s.rows[a], s.rows[b]
	if ra.StartNodeID() != rb.StartNodeID() {
		return ra.StartNodeID() < rb.StartNodeID()
	}
	if ra.EndNodeID() != rb.EndNodeID() {
		return ra.EndNodeID() < rb.EndNodeID()
	}
	if s.vf[a] != s.vf[b] {
		return s.vf[a] < s.vf[b]
	}
	if ra.ID() != rb.ID() {
		return ra.ID() < rb.ID()
	}
	return ra.Version() < rb.Version()
}

type namedSection struct {
	name string
	data []byte
}

func encode(s Schema, rows []*types.Relationship, opts Options, pageRows int, dicts *Dicts) ([]byte, error) {
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
	if n > MaxRows {
		return nil, fmt.Errorf("%w: %d rows exceed the segment maximum %d", ErrInvalidRow, n, MaxRows)
	}
	vfIn := make([]int64, n)
	for i, r := range rows {
		if r == nil {
			return nil, fmt.Errorf("%w: row %d is nil", ErrInvalidRow, i)
		}
		if r.TypeToken().Value() != s.TypeToken {
			return nil, rowErr(r, ErrInvalidRow, "row %d (id %d) has type token %d, schema %d", i, r.ID(), r.TypeToken().Value(), s.TypeToken)
		}
		if ig := r.Integrity(); ig == nil || !isLowerHex64(ig.Hash) {
			return nil, rowErr(r, ErrHashMismatch, "row %d (id %d) carries no stored 64-hex hash", i, r.ID())
		}
		if tm := r.Temporal(); tm != nil {
			vfIn[i] = int64(tm.ValidFrom)
		}
	}

	// Segment order, and the (id, version) key must be unique.
	srt := &rowSorter{rows: rows, vf: vfIn, order: make([]int, n)}
	for i := range srt.order {
		srt.order[i] = i
	}
	sort.Slice(srt.order, func(a, b int) bool { return srt.less(srt.order[a], srt.order[b]) })
	order := srt.order
	byID := make([]int, n) // positions in segment order, sorted by (id, version, row)
	for i := range byID {
		byID[i] = i
	}
	sort.Slice(byID, func(a, b int) bool {
		ra, rb := rows[order[byID[a]]], rows[order[byID[b]]]
		if ra.ID() != rb.ID() {
			return ra.ID() < rb.ID()
		}
		if ra.Version() != rb.Version() {
			return ra.Version() < rb.Version()
		}
		return byID[a] < byID[b]
	})
	for k := 1; k < n; k++ {
		ra, rb := rows[order[byID[k-1]]], rows[order[byID[k]]]
		if ra.ID() == rb.ID() && ra.Version() == rb.Version() {
			return nil, fmt.Errorf("%w: id %d version %d appears twice", ErrInvalidRow, ra.ID(), ra.Version())
		}
	}

	// Node table.
	nodeSet := make(map[types.NodeID]struct{})
	for _, r := range rows {
		nodeSet[r.StartNodeID()] = struct{}{}
		nodeSet[r.EndNodeID()] = struct{}{}
	}
	nodes := make([]int64, 0, len(nodeSet))
	for id := range nodeSet {
		nodes = append(nodes, int64(id))
	}
	sort.Slice(nodes, func(a, b int) bool { return nodes[a] < nodes[b] })
	ordOf := make(map[types.NodeID]int, len(nodes))
	for i, id := range nodes {
		ordOf[types.NodeID(id)] = i
	}

	colIdx := make(map[string]int, len(s.Columns))
	for i, c := range s.Columns {
		colIdx[c.Name] = i
	}
	w := newColumns(n, len(nodes), s.Columns)
	nodeHashSet := make([]bool, len(nodes))
	for i, src := range order {
		r := rows[src]
		so, eo := ordOf[r.StartNodeID()], ordOf[r.EndNodeID()]
		w.startOrd[i] = so
		w.end[i] = int64(eo)
		w.relID[i] = int64(r.ID())
		w.version[i] = int64(r.Version())
		if tm := r.Temporal(); tm != nil {
			w.vf[i], w.vt[i], w.tx[i] = int64(tm.ValidFrom), int64(tm.ValidTo), int64(tm.TxFrom)
			w.txTo[i], w.createdAt[i] = int64(tm.TxTo), int64(tm.CreatedAt)
			w.updatedAt[i], w.deletedAt[i] = int64(tm.UpdatedAt), int64(tm.DeletedAt)
			w.baseEntity[i] = int64(tm.BaseEntityID())
			w.createdBy[i], w.updatedBy[i] = tm.CreatedBy, tm.UpdatedBy
		} else {
			w.noTemporal[i] = 1
		}
		ig := r.Integrity()
		w.leaves[i] = ig.Hash
		w.prevHash[i], w.authorID[i], w.authorizedBy[i] = ig.PrevHash, ig.AuthorID, ig.AuthorizedBy
		w.authLevel[i] = int64(ig.AuthorizationLevel)
		w.signature[i] = ig.Signature
		w.fromHash[i], w.toHash[i] = ig.FromNodeHash, ig.ToNodeHash
		if !nodeHashSet[so] {
			w.nodeHash[so], nodeHashSet[so] = ig.FromNodeHash, true
		}
		if !nodeHashSet[eo] {
			w.nodeHash[eo], nodeHashSet[eo] = ig.ToNodeHash, true
		}
		var fallback types.PropertySlice
		for _, p := range r.Properties() {
			if c, ok := colIdx[p.Key]; ok && kindOf(p.Value) == s.Columns[c].Kind {
				w.present[c][i] = 1
				if s.Columns[c].Kind == KindString {
					w.strs[c][i] = p.Value.(string)
				} else {
					w.ints[c][i] = valueBits(p.Value)
				}
				continue
			}
			fallback = append(fallback, p)
		}
		if len(fallback) > 0 {
			b, err := storeutil.MarshalPropertySlice(fallback)
			if err != nil {
				return nil, rowErr(r, ErrInvalidRow, "row id %d: fallback properties: %v", r.ID(), err)
			}
			w.fallback[i] = b
		}
	}
	if dicts.nodeDict() != nil {
		w.internNodes(dicts.Nodes, nodes)
	}
	w.dicts = dicts
	out := w.assemble(s, blockRows, pageRows, nodes, byID)
	if err := verifySealed(out, rows, order, dicts); err != nil {
		return nil, err
	}
	return out, nil
}

// columns holds the writer's row-major-to-column staging arrays.
type columns struct {
	n, nodes                                                     int
	startOrd                                                     []int
	end, relID, version, vf, vt, tx, noTemporal                  []int64
	txTo, createdAt, updatedAt, deletedAt, baseEntity, authLevel []int64
	createdBy, updatedBy, prevHash, authorID, authorizedBy       []string
	fromHash, toHash, leaves                                     []string
	nodeHash                                                     []string
	// Shared-dictionary encoding (dict != nil): node codes per ordinal and
	// each row's endpoint-hash codes (0 = the node's first hash, k = hash
	// code k-1).
	shared                      bool
	nodeCodes, fromDict, toDict []int64
	dicts                       *Dicts
	signature, fallback         [][]byte
	ints                        [][]int64
	strs                        [][]string
	present                     [][]int64
}

func newColumns(n, nodes int, cols []Column) *columns {
	i64 := func() []int64 { return make([]int64, n) }
	str := func() []string { return make([]string, n) }
	w := &columns{
		n: n, nodes: nodes, startOrd: make([]int, n),
		end: i64(), relID: i64(), version: i64(), vf: i64(), vt: i64(), tx: i64(), noTemporal: i64(),
		txTo: i64(), createdAt: i64(), updatedAt: i64(), deletedAt: i64(), baseEntity: i64(), authLevel: i64(),
		createdBy: str(), updatedBy: str(), prevHash: str(), authorID: str(), authorizedBy: str(),
		fromHash: str(), toHash: str(), leaves: str(), nodeHash: make([]string, nodes),
		signature: make([][]byte, n), fallback: make([][]byte, n),
		ints: make([][]int64, len(cols)), strs: make([][]string, len(cols)), present: make([][]int64, len(cols)),
	}
	for c, col := range cols {
		w.present[c] = i64()
		if col.Kind == KindString {
			w.strs[c] = str()
		} else {
			w.ints[c] = i64()
		}
	}
	return w
}

func (w *columns) assemble(s Schema, blockRows, pageRows int, nodes []int64, byID []int) []byte {
	n := w.n
	var secs []namedSection
	addInt := func(name string, vals []int64, ctx intEncodeCtx) {
		if !allZero(vals) {
			secs = append(secs, namedSection{name, encodeIntCol(vals, pageRows, ctx)})
		}
	}
	addStr := func(name string, vals []string) {
		if !allEmpty(vals) {
			secs = append(secs, namedSection{name, encodeStrCol(vals, pageRows)})
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
	if w.shared {
		addInt(secNodes, w.nodeCodes, none)
	} else {
		addInt(secNodes, nodes, none)
		addStr(secNodeHash, w.nodeHash)
	}
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

	// Integrity roots over the stored hashes, in segment order.
	root := sha256.New()
	if n > 0 {
		var roots []byte
		leaf := make([]byte, 0, 32*blockRows)
		for g := 0; g*blockRows < n; g++ {
			leaf = leaf[:0]
			for i := g * blockRows; i < min(n, (g+1)*blockRows); i++ {
				h, _ := hex.DecodeString(w.leaves[i]) // validated by isLowerHex64
				leaf = append(leaf, h...)
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
	addInt(colRelID, w.relID, none)
	addInt(colVersion, w.version, none)
	addInt(colNoTemporal, w.noTemporal, none)
	addInt(colTxTo, w.txTo, vsVF)
	addInt(colCreatedAt, w.createdAt, vsVF)
	addInt(colUpdatedAt, w.updatedAt, vsVF)
	addInt(colDeletedAt, w.deletedAt, vsVF)
	addInt(colBaseEntity, w.baseEntity, none)
	addInt(colAuthLevel, w.authLevel, none)
	if w.shared {
		addInt(colFromHash, w.fromDict, none)
		addInt(colToHash, w.toDict, none)
	} else {
		fromCode, toCode, exc := w.endpointCodes()
		addInt(colFromHash, fromCode, none)
		addInt(colToHash, toCode, none)
		addStr(secEndpointExc, exc)
	}
	addStr(colCreatedBy, w.createdBy)
	addStr(colUpdatedBy, w.updatedBy)
	addStr(colPrevHash, w.prevHash)
	addStr(colAuthorID, w.authorID)
	addStr(colAuthorizedBy, w.authorizedBy)
	addBytes(colSignature, w.signature)
	for c, col := range s.Columns {
		if col.Kind == KindString {
			if sd := w.dicts.stringDict(col.Name); sd != nil && !allEmpty(w.strs[c]) {
				if codes, ok := sd.intern(w.strs[c]); ok {
					secs = append(secs, namedSection{propPrefix + col.Name, encodeSharedStrCol(codes, pageRows)})
					goto presence
				}
			}
			addStr(propPrefix+col.Name, w.strs[c])
		} else {
			addInt(propPrefix+col.Name, w.ints[c], none)
		}
	presence:
		if !allOnes(w.present[c]) {
			secs = append(secs, namedSection{presencePrefix + col.Name, encodeIntCol(w.present[c], pageRows, none)})
		}
	}
	addBytes(secFallback, w.fallback)

	// Header, sections, footer, trailer.
	h := header{format: CurrentFormatVersion, typeToken: int(s.TypeToken), pageRows: pageRows, rows: n, nodes: w.nodes}
	if w.shared {
		h.flags = flagNodeDict
	}
	if n > 0 {
		h.idMin, h.idMax, h.vfMin, h.vfMax = math.MaxInt64, math.MinInt64, math.MaxInt64, math.MinInt64
		for i := 0; i < n; i++ {
			h.idMin, h.idMax = min(h.idMin, w.relID[i]), max(h.idMax, w.relID[i])
			h.vfMin, h.vfMax = min(h.vfMin, w.vf[i]), max(h.vfMax, w.vf[i])
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

// internNodes interns the segment's endpoints and their hashes into d and
// fills the shared-dictionary columns. A node new to d takes the hash its
// first row in this segment carries as its first hash.
func (w *columns) internNodes(d *NodeDict, nodes []int64) {
	w.shared = true
	txn := d.begin()
	defer txn.commit()
	w.nodeCodes = make([]int64, len(nodes))
	first := make([]string, len(nodes))
	for ord, id := range nodes {
		c := txn.node(id, w.nodeHash[ord])
		w.nodeCodes[ord] = int64(c)
		first[ord] = txn.s.hashString(uint64(txn.s.first[c]))
	}
	w.fromDict, w.toDict = make([]int64, w.n), make([]int64, w.n)
	for i := 0; i < w.n; i++ {
		if so := w.startOrd[i]; w.fromHash[i] != first[so] {
			c, _ := txn.hash(uint32(w.nodeCodes[so]), w.fromHash[i]) // #nosec G115 -- a code
			w.fromDict[i] = int64(c) + 1
		}
		if eo := w.end[i]; w.toHash[i] != first[eo] {
			c, _ := txn.hash(uint32(w.nodeCodes[eo]), w.toHash[i]) // #nosec G115 -- a code
			w.toDict[i] = int64(c) + 1
		}
	}
}

// endpointCodes encodes each row's endpoint hashes as 0 (the node's usual
// hash, nodehash[ordinal]) or 1+k (entry k of the sorted exception list), so
// the columns cost nothing while no endpoint changed.
func (w *columns) endpointCodes() (from, to []int64, exc []string) {
	excSet := map[string]int64{}
	for i := 0; i < w.n; i++ {
		if w.fromHash[i] != w.nodeHash[w.startOrd[i]] {
			excSet[w.fromHash[i]] = 0
		}
		if w.toHash[i] != w.nodeHash[w.end[i]] {
			excSet[w.toHash[i]] = 0
		}
	}
	exc = make([]string, 0, len(excSet))
	for h := range excSet {
		exc = append(exc, h)
	}
	sort.Strings(exc)
	for k, h := range exc {
		excSet[h] = int64(k) + 1
	}
	from, to = make([]int64, w.n), make([]int64, w.n)
	for i := 0; i < w.n; i++ {
		if w.fromHash[i] != w.nodeHash[w.startOrd[i]] {
			from[i] = excSet[w.fromHash[i]]
		}
		if w.toHash[i] != w.nodeHash[w.end[i]] {
			to[i] = excSet[w.toHash[i]]
		}
	}
	return from, to, exc
}

func (w *columns) pageIndex(pageRows int) []byte {
	var out []byte
	for lo := 0; lo < w.n; lo += pageRows {
		hi := min(lo+pageRows, w.n)
		mm := [8]int64{math.MaxInt64, math.MinInt64, math.MaxInt64, math.MinInt64, math.MaxInt64, math.MinInt64, math.MaxInt64, math.MinInt64}
		var flags byte
		for i := lo; i < hi; i++ {
			for k, v := range [4]int64{w.vf[i], w.vt[i], w.tx[i], w.relID[i]} {
				mm[2*k], mm[2*k+1] = min(mm[2*k], v), max(mm[2*k+1], v)
			}
			if w.vt[i] == 0 {
				flags |= 1
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
// equal its source row and to recompute its stored hash (ADR-0011 §4.3:
// sealing refuses on any mismatch — lesson 44 applied to our own encoder).
func verifySealed(data []byte, rows []*types.Relationship, order []int, dicts *Dicts) error {
	seg, err := OpenWithDicts(data, dicts)
	if err != nil {
		return fmt.Errorf("segment: writer produced a segment its reader rejects: %w", err)
	}
	return seg.scanRange(0, seg.rows, func(i int, got *types.Relationship) error {
		want := rows[order[i]]
		if diff := rowDiff(want, got); diff != "" {
			return rowErr(want, ErrInvalidRow, "id %d version %d does not round-trip: %s", want.ID(), want.Version(), diff)
		}
		if stored := want.Integrity().Hash; got.Integrity().Hash != stored {
			return rowErr(want, ErrHashMismatch, "id %d version %d: sealed columns hash to %s, stored hash %s",
				want.ID(), want.Version(), got.Integrity().Hash, stored)
		}
		return nil
	})
}

// rowDiff names the first field in which got differs from want ("" when
// equal). Integrity().Hash is compared by the caller.
func rowDiff(want, got *types.Relationship) string {
	if want.ID() != got.ID() || want.TypeToken() != got.TypeToken() || want.StartNodeID() != got.StartNodeID() ||
		want.EndNodeID() != got.EndNodeID() || want.Version() != got.Version() {
		return "identity"
	}
	wt, gt := want.Temporal(), got.Temporal()
	if (wt == nil) != (gt == nil) || (wt != nil && (*wt != *gt || wt.BaseEntityID() != gt.BaseEntityID())) {
		return "temporal metadata"
	}
	wi, gi := want.Integrity(), got.Integrity()
	if wi.PrevHash != gi.PrevHash || wi.FromNodeHash != gi.FromNodeHash || wi.ToNodeHash != gi.ToNodeHash ||
		wi.AuthorID != gi.AuthorID || wi.AuthorizedBy != gi.AuthorizedBy || wi.AuthorizationLevel != gi.AuthorizationLevel ||
		(wi.Signature == nil) != (gi.Signature == nil) || !bytes.Equal(wi.Signature, gi.Signature) {
		return "integrity fields"
	}
	wp, gp := want.Properties(), got.Properties()
	if len(wp) != len(gp) {
		return fmt.Sprintf("%d properties, want %d", len(gp), len(wp))
	}
	var wb, gb []byte
	for k := range wp {
		if wp[k].Key != gp[k].Key {
			return fmt.Sprintf("property key %q, want %q", gp[k].Key, wp[k].Key)
		}
		wb = types.AppendPropertyValueHashBytes(wb[:0], wp[k].Value)
		gb = types.AppendPropertyValueHashBytes(gb[:0], gp[k].Value)
		if !bytes.Equal(wb, gb) {
			return fmt.Sprintf("property %q: %T(%v), want %T(%v)", wp[k].Key, gp[k].Value, gp[k].Value, wp[k].Value, wp[k].Value)
		}
	}
	return ""
}
