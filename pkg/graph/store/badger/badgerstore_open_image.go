package badger

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"iter"
	"maps"
	"math"
	"slices"
	"strconv"
	"sync/atomic"

	snowflake "github.com/bds421/rho-snowflake-2026"
	badgerv4 "github.com/dgraph-io/badger/v4"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Clean-close open state (backlog 54).
//
// A full open decodes every current row to rebuild the RAM indexes, linear in
// the store. Most of what it rebuilds is already persisted as keys (entity,
// label, type, outgoing and incoming keys), and the open could read those keys
// alone if it knew they agree with the rows. The rest — the contents of the
// property, relationship-property, composite, temporal, relationship-type
// temporal, high-frequency and vector indexes, and the planner statistics
// (badgerstore_planner_stats_deferred.go) — Close persists here, after its
// final flush, as one chunked blob (the "open image"). An image without the
// statistics section makes the open defer them to their first read
// (ensurePlannerStats). The rebuilding open repairs index keys that disagree
// with the rows first (badgerstore_index_repair.go), so the keys an image
// later vouches for agree with them.
//
// TRUST RULE. The image header is written last, in its own commit, so its
// badger version is the newest in the store. An open trusts the image (and the
// keys) only when that still holds: header version == db.MaxVersion(). Any
// later commit — a session that crashed after writing, a write by an older
// release that knows nothing of the image, a raw badger write — moves
// MaxVersion past it and the open falls back to the full rebuild. So does a
// missing chunk, a length or CRC mismatch, an unknown format, or any decode
// error. Read-only opens read the image but never write one.

const openImageName = "tkg_open_image"

// openImageHeaderKey also prefixes the chunk keys.
var openImageHeaderKey = storepkg.MetaKey(openImageName)

func openImageChunkKey(i int) []byte {
	return storepkg.MetaKey(openImageName + "_c" + strconv.Itoa(i))
}

// openImageChunkBytes keeps each value well under badger's 1 MiB inline limit
// and its per-transaction size (see columnChunkBytes). A var so a test can
// force several chunks.
var openImageChunkBytes = 512 * 1024

// openImageMagic's last byte is the image format. 2 (v4.54.0): the temporal
// envelopes start an untimed row at its TxFrom (backlog 62); an image of format 1
// holds mint-keyed envelopes, which can start after the row (a TxFrom backfill)
// and prune it, so the open rebuilds instead of trusting it. 3 (v4.58.0): an
// Elements index built over existing lists before it kept one element per row
// (PropertyIndex.FinishBuild); an image of format 2 can hold that, so the open
// rebuilds from the rows.
const openImageMagic = "TKGOI\x03"

const (
	imgSecNodeProp byte = iota + 1
	imgSecRelProp
	imgSecComposite
	imgSecTemporal
	imgSecRelTemporal
	imgSecHF
	imgSecVector
	imgSecPlannerStats // badgerstore_planner_stats_deferred.go
	imgSecRelPropTx    // v4.56.1-v4.58.1 sidecars without valid-time spans: skipped (rebuilt lazily)
	imgSecNodePropTx
	imgSecRelPropTxSpans // built property membership sidecars with spans (backlog 74, ledger row 62)
	imgSecNodePropTxSpans
	imgSecNodeIDs     // the live node IDs, ascending, delta-encoded (EXISTMAP): the open skips the node-key walk
	imgSecRelIDs      // the live rel IDs, as imgSecNodeIDs
	imgSecLabelCounts // the live nodes per label token (OPENWALK): with the label index on disk the open skips the label-key walk
	imgSecRelTypes    // the rel-type index, an ID set per token (OPENWALK): the open skips the rel-type-key walk
)

type imgValueIDs struct {
	vk  string
	ids []snowflake.ID
}

type imgHFEntry struct {
	start types.Instant
	id    types.NodeID
}

type imgVector struct {
	id  snowflake.ID
	vec []float32
}

// openImage is a decoded image. Each index is taken (and removed) by the open
// path that installs it; an index the image lacks is rebuilt from the rows.
type openImage struct {
	nodeProps   map[indexpkg.PropertyIndexKey][]imgValueIDs
	relProps    map[indexpkg.RelPropertyIndexKey][]imgValueIDs
	composites  map[indexpkg.CompositeIndexKey][]imgValueIDs
	temporal    map[uint16][]indexpkg.IntervalEntry
	relTemporal map[uint16][]indexpkg.IntervalEntry
	hf          map[uint16][]imgHFEntry
	vectors     map[indexpkg.VectorIndexKey][]imgVector
	// restoredTemporal names the label temporal indexes installed from the
	// image: their persisted envelopes already fold the history, so New skips
	// foldTemporalHistoryEnvelopes for them.
	restoredTemporal map[uint16]bool
	// stats holds the closing process's planner statistics; nil when the
	// image has none (the open then defers them).
	stats *imgPlannerStats
	// relPropTx / nodePropTx hold the property membership sidecars the
	// closing process had built (backlog 74); one the image lacks builds
	// lazily.
	relPropTx  map[indexpkg.RelPropertyIndexKey]*indexpkg.PropertyTxMembers[types.RelID]
	nodePropTx map[indexpkg.PropertyIndexKey]*indexpkg.PropertyTxMembers[types.NodeID]
	// nodeIDs / relIDs are the closing process's live ID sets; nil when the
	// image has none (an image written before EXISTMAP), so the open walks
	// the entity keys.
	nodeIDs, relIDs *indexpkg.IDSet
	// labelCounts are the closing process's live nodes per label token; nil
	// when the image has none (written before OPENWALK).
	labelCounts map[uint16]int64
	// relTypes is the closing process's rel-type index; nil when the image
	// has none (written before OPENWALK).
	relTypes map[uint16]*indexpkg.IDSet
	// since is the badger version of an image older than the store's newest
	// commit, 0 for a trusted one; touchedNodes and touchedRels are the
	// entities a newer key names (a delta open: badgerstore_open_image_delta.go).
	since                     uint64
	touchedNodes, touchedRels *indexpkg.IDSet
	// touchedTemporal lists, per restored label temporal index, the touched
	// members a delta open added from their rows: New folds their history.
	touchedTemporal map[uint16][]types.NodeID
}

func newOpenImage() *openImage {
	return &openImage{
		nodeProps:        make(map[indexpkg.PropertyIndexKey][]imgValueIDs),
		relProps:         make(map[indexpkg.RelPropertyIndexKey][]imgValueIDs),
		composites:       make(map[indexpkg.CompositeIndexKey][]imgValueIDs),
		temporal:         make(map[uint16][]indexpkg.IntervalEntry),
		relTemporal:      make(map[uint16][]indexpkg.IntervalEntry),
		hf:               make(map[uint16][]imgHFEntry),
		vectors:          make(map[indexpkg.VectorIndexKey][]imgVector),
		restoredTemporal: make(map[uint16]bool),
		touchedTemporal:  make(map[uint16][]types.NodeID),
		relPropTx:        make(map[indexpkg.RelPropertyIndexKey]*indexpkg.PropertyTxMembers[types.RelID]),
		nodePropTx:       make(map[indexpkg.PropertyIndexKey]*indexpkg.PropertyTxMembers[types.NodeID]),
	}
}

// ---- Close side ----

// openImageAfterEncodeHook, when set, runs between the image's encode and its
// writes (tests only: a write racing Close lands there).
var openImageAfterEncodeHook func()

// persistOpenImageLocked writes the open image after Close's final flush;
// Close holds flushMu, so no flush commits between the drained check and the
// header. Best effort: the image only accelerates the next open, so a failure
// is logged and leaves no trusted header (the next open rebuilds).
func (bs *Store) persistOpenImageLocked() {
	if bs.inMemory || bs.readOnly || bs.openImageSuppressed.Load() {
		return
	}
	if err := bs.writeOpenImage(); err != nil && bs.logger != nil {
		bs.logger.Warningf("graph: open image not written (the next open rebuilds from the rows): %v", err)
	}
}

func (bs *Store) writeOpenImage() error {
	bs.idxMu.RLock()
	bs.wbMu.Lock()
	drained := len(bs.pending) == 0 && len(bs.pendingLog) == 0 && len(bs.flushing) == 0
	bs.wbMu.Unlock()
	if !drained {
		// A write raced Close past its final flush: the RAM state holds a row
		// the disk does not, so it must not be persisted as the open state.
		bs.idxMu.RUnlock()
		return errors.New("write buffer not drained at close")
	}
	blob := bs.encodeOpenImageLocked()
	bs.idxMu.RUnlock()
	if openImageAfterEncodeHook != nil {
		openImageAfterEncodeHook()
	}

	var oldChunks int
	if err := bs.db.View(func(txn *badgerv4.Txn) error {
		item, err := txn.Get(openImageHeaderKey)
		if errors.Is(err, badgerv4.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			if h, ok := parseOpenImageHeader(val); ok {
				oldChunks = h.chunks
			}
			return nil
		})
	}); err != nil {
		return err
	}

	chunks := (len(blob) + openImageChunkBytes - 1) / openImageChunkBytes
	wb := bs.db.NewWriteBatch()
	defer wb.Cancel()
	for i := 0; i < chunks; i++ {
		lo := i * openImageChunkBytes
		if err := wb.Set(openImageChunkKey(i), blob[lo:min(lo+openImageChunkBytes, len(blob))]); err != nil {
			return err
		}
	}
	for i := chunks; i < oldChunks; i++ {
		if err := wb.Delete(openImageChunkKey(i)); err != nil {
			return err
		}
	}
	if err := wb.Flush(); err != nil {
		return err
	}
	// Header last, in its own commit: its version must be the store's newest.
	hdr := binary.AppendUvarint([]byte(openImageMagic), uint64(chunks)) // #nosec G115 -- chunk count of a non-negative length
	hdr = binary.AppendUvarint(hdr, uint64(len(blob)))
	hdr = binary.BigEndian.AppendUint32(hdr, crc32.Checksum(blob, crc32cTable))
	return bs.db.Update(func(txn *badgerv4.Txn) error { return txn.Set(openImageHeaderKey, hdr) })
}

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

// appendLabelCounts renders the label counters, ascending by token, without
// the zero ones (a rebuild creates none for a label without members).
func (bs *Store) appendLabelCounts() []byte {
	counts := map[uint16]int64{}
	bs.labelCounts.Range(func(k, v any) bool {
		if n := v.(*atomic.Int64).Load(); n > 0 {
			counts[k.(uint16)] = n
		}
		return true
	})
	b := binary.AppendUvarint(nil, uint64(len(counts)))
	for _, tok := range slices.Sorted(maps.Keys(counts)) {
		b = binary.BigEndian.AppendUint16(b, tok)
		b = binary.AppendUvarint(b, uint64(counts[tok])) // #nosec G115 -- a positive count
	}
	return b
}

// encodeOpenImageLocked renders every RAM index content the rows would
// otherwise rebuild. Indexes still being created are left out (rebuilt).
// Caller holds idxMu (read).
func (bs *Store) encodeOpenImageLocked() []byte {
	var out []byte
	section := func(kind byte, payload []byte) {
		if bs.imageSectionFilterForTest != nil && !bs.imageSectionFilterForTest(kind) {
			return
		}
		out = append(out, kind)
		out = binary.AppendUvarint(out, uint64(len(payload)))
		out = append(out, payload...)
	}
	// appendIDSet is a count and the IDs as ascending deltas (the first from 0).
	appendIDSet := func(set *indexpkg.IDSet) []byte {
		b := binary.AppendUvarint(nil, uint64(set.Len())) // #nosec G115 -- a length
		prev := int64(0)
		set.Ascend(func(id int64) bool {
			b = binary.AppendUvarint(b, uint64(id-prev)) // #nosec G115 -- positive ascending snowflake IDs
			prev = id
			return true
		})
		return b
	}
	section(imgSecNodeIDs, appendIDSet(&bs.nodeIDs))
	section(imgSecRelIDs, appendIDSet(&bs.relIDs))
	section(imgSecLabelCounts, bs.appendLabelCounts())
	relTypes := binary.AppendUvarint(nil, uint64(len(bs.typeIdx)))
	for _, tok := range slices.Sorted(maps.Keys(bs.typeIdx)) {
		relTypes = binary.BigEndian.AppendUint16(relTypes, tok)
		relTypes = append(relTypes, appendIDSet(bs.typeIdx[tok])...)
	}
	section(imgSecRelTypes, relTypes)
	// appendKeyIDs is one value key with its IDs, ascending so the reload's
	// AddKey fills exact chunks.
	appendKeyIDs := func(b []byte, vk string, n int, ids iter.Seq[snowflake.ID]) []byte {
		b = appendImgString(b, vk)
		b = binary.AppendUvarint(b, uint64(n)) // #nosec G115 -- a length
		for id := range ids {
			b = binary.AppendUvarint(b, uint64(id)) // #nosec G115 -- positive snowflake ID
		}
		return b
	}
	appendValueIDs := func(b []byte, entries map[string]*indexpkg.IDPostings) []byte {
		b = binary.AppendUvarint(b, uint64(len(entries)))
		for vk, ids := range entries {
			b = appendKeyIDs(b, vk, ids.Len(), ids.All())
		}
		return b
	}
	if !bs.propIdxOnDisk {
		for key, idx := range bs.propertyIndexes {
			if idx.Mutated != nil {
				continue
			}
			b := binary.BigEndian.AppendUint16(nil, key.LabelToken)
			b = appendImgString(b, key.PropertyKey)
			section(imgSecNodeProp, appendValueIDs(b, idx.Entries))
		}
	}
	for key, idx := range bs.relPropertyIndexes {
		if idx.Mutated != nil {
			continue
		}
		b := binary.BigEndian.AppendUint16(nil, key.RelTypeToken)
		b = appendImgString(b, key.PropertyKey)
		section(imgSecRelProp, appendValueIDs(b, idx.Entries))
	}
	for key, idx := range bs.compositeIndexes {
		if idx.Mutated != nil {
			continue
		}
		b := binary.BigEndian.AppendUint16(nil, key.LabelToken)
		b = appendImgString(b, key.Keys)
		// The same section bytes as a map of posting lists (row 66 keeps
		// single-ID keys inline in RAM only): v4.60.0 images read unchanged.
		b = binary.AppendUvarint(b, uint64(idx.KeyCount())) // #nosec G115 -- a length
		idx.Range(func(vk string, n int, ids iter.Seq[snowflake.ID]) bool {
			b = appendKeyIDs(b, vk, n, ids)
			return true
		})
		section(imgSecComposite, b)
	}
	appendTemporal := func(kind byte, indexes map[uint16]*indexpkg.TemporalIndex) {
		for tok, ti := range indexes {
			if ti.Building || ti.Mutated != nil {
				continue
			}
			b := binary.BigEndian.AppendUint16(nil, tok)
			var n uint64
			var body []byte
			ti.ForEachEntry(func(e indexpkg.IntervalEntry) {
				n++
				body = binary.AppendUvarint(body, uint64(e.ID)) // #nosec G115 -- positive snowflake ID
				body = binary.AppendVarint(body, int64(e.From))
				body = binary.AppendVarint(body, int64(e.To))
			})
			b = binary.AppendUvarint(b, n)
			section(kind, append(b, body...))
		}
	}
	appendTemporal(imgSecTemporal, bs.temporalIndexes)
	appendTemporal(imgSecRelTemporal, bs.relTypeTemporalIndexes)
	for tok, hfi := range bs.hfIndexes {
		if hfi.IsBuilding() {
			continue
		}
		b := binary.BigEndian.AppendUint16(nil, tok)
		var n uint64
		var body []byte
		hfi.ForEachBucketEntry(func(start types.Instant, id types.NodeID) {
			n++
			body = binary.AppendVarint(body, int64(start))
			body = binary.AppendUvarint(body, uint64(id.SnowflakeID())) // #nosec G115 -- positive snowflake ID
		})
		b = binary.AppendUvarint(b, n)
		section(imgSecHF, append(b, body...))
	}
	for key, vi := range bs.vectorIndexes {
		if vi.IsBuilding() {
			continue
		}
		b := binary.BigEndian.AppendUint16(nil, key.LabelToken)
		b = appendImgString(b, key.PropertyKey)
		b = binary.AppendUvarint(b, uint64(vi.Dims)) // #nosec G115 -- validated positive dims
		var n uint64
		var body []byte
		vi.ForEachVector(func(id snowflake.ID, vec []float32) {
			n++
			body = binary.AppendUvarint(body, uint64(id)) // #nosec G115 -- positive snowflake ID
			for _, f := range vec {
				body = binary.BigEndian.AppendUint32(body, math.Float32bits(f))
			}
		})
		b = binary.AppendUvarint(b, n)
		section(imgSecVector, append(b, body...))
	}
	if st := bs.encodePlannerStatsLocked(); st != nil {
		section(imgSecPlannerStats, st)
	}
	// Built property membership sidecars (backlog 74): derived from the rows
	// exactly like the indexes above, so the same trust rule covers them; one
	// tracking or claimed by a running pass is left out (built lazily).
	for key, sc := range bs.relPropTx {
		if sc.built && bs.relPropIndexUsableLocked(key) {
			b := binary.BigEndian.AppendUint16(nil, key.RelTypeToken)
			b = appendImgString(b, key.PropertyKey)
			section(imgSecRelPropTxSpans, appendImgPropTx(b, sc.members))
		}
	}
	for key, sc := range bs.nodePropTx {
		if sc.built && bs.nodePropIndexUsableLocked(key) {
			b := binary.BigEndian.AppendUint16(nil, key.LabelToken)
			b = appendImgString(b, key.PropertyKey)
			section(imgSecNodePropTxSpans, appendImgPropTx(b, sc.members))
		}
	}
	return out
}

// Posting flags of an image sidecar member (ledger row 62).
const (
	imgPropTxAll  byte = 1 << iota // any-pin envelope follows (from, to)
	imgPropTxLate                  // late history envelope follows (from, to)
	imgPropTxCur                   // current row follows (from, to, version)
	imgPropTxWild                  // unknown span: meets every window
)

// appendImgPropTx renders a sidecar: the value count, then per value its key,
// member count and per member (ID, first transaction time, flags, last stamp,
// the envelopes the flags name; indexpkg.SpanMember).
func appendImgPropTx[ID ~int64](b []byte, m *indexpkg.PropertyTxMembers[ID]) []byte {
	var n uint64
	var body []byte
	span := func(sp indexpkg.Span) {
		body = binary.AppendVarint(body, int64(sp.From))
		body = binary.AppendVarint(body, int64(sp.To))
	}
	m.ForEachValueSpans(func(vk string, members []indexpkg.SpanMember[ID]) {
		n++
		body = appendImgString(body, vk)
		body = binary.AppendUvarint(body, uint64(len(members)))
		for _, mb := range members {
			body = binary.AppendUvarint(body, uint64(mb.ID)) // #nosec G115 -- positive snowflake ID
			body = binary.AppendVarint(body, int64(mb.FirstTx))
			var flags byte
			for _, f := range []struct {
				on  bool
				bit byte
			}{{mb.HasAll, imgPropTxAll}, {mb.HasLate, imgPropTxLate}, {mb.HasCur, imgPropTxCur}, {mb.Wild, imgPropTxWild}} {
				if f.on {
					flags |= f.bit
				}
			}
			body = append(body, flags)
			body = binary.AppendVarint(body, int64(mb.LastTx))
			if mb.HasAll {
				span(mb.All)
			}
			if mb.HasLate {
				span(mb.Late)
			}
			if mb.HasCur {
				span(mb.Cur)
				body = binary.AppendUvarint(body, uint64(mb.CurVersion))
			}
		}
	})
	b = binary.AppendUvarint(b, n)
	return append(b, body...)
}

func appendImgString(b []byte, s string) []byte {
	b = binary.AppendUvarint(b, uint64(len(s)))
	return append(b, s...)
}

// ---- Open side ----

type openImageHeader struct {
	chunks, length int
	crc            uint32
}

func parseOpenImageHeader(val []byte) (openImageHeader, bool) {
	if len(val) < len(openImageMagic) || string(val[:len(openImageMagic)]) != openImageMagic {
		return openImageHeader{}, false
	}
	r := imgReader{b: val[len(openImageMagic):]}
	chunks, length := r.count(1<<20), r.count(math.MaxInt32)
	if r.err != nil || len(r.b) != 4 {
		return openImageHeader{}, false
	}
	return openImageHeader{chunks: chunks, length: length, crc: binary.BigEndian.Uint32(r.b)}, true
}

// readOpenImage returns the image when the open may trust it (see the trust
// rule above), nil otherwise.
func (bs *Store) readOpenImage() *openImage {
	if bs.inMemory {
		return nil
	}
	newest := bs.db.MaxVersion()
	var img *openImage
	var since uint64 // the header's version when older than newest: a delta open
	err := bs.db.View(func(txn *badgerv4.Txn) error {
		item, err := txn.Get(openImageHeaderKey)
		if errors.Is(err, badgerv4.ErrKeyNotFound) {
			return nil // never written, or a release without it closed the store
		}
		if err != nil {
			return err
		}
		if item.Version() != newest {
			since = item.Version() // something committed after the clean close
		}
		var h openImageHeader
		var ok bool
		if err := item.Value(func(val []byte) error {
			h, ok = parseOpenImageHeader(val)
			return nil
		}); err != nil {
			return err
		}
		if !ok {
			return errors.New("unreadable header")
		}
		return bs.readOpenImageChunks(txn, h, &img)
	})
	if err != nil {
		if bs.logger != nil {
			bs.logger.Warningf("graph: open image ignored, rebuilding from the rows: %v", err)
		}
		return nil
	}
	if img != nil && since != 0 && !bs.openImageDelta(img, since) {
		return nil
	}
	return img
}

// readOpenImageChunks reassembles, verifies and decodes the chunks.
func (bs *Store) readOpenImageChunks(txn *badgerv4.Txn, h openImageHeader, img **openImage) error {
	blob := make([]byte, 0, h.length)
	for i := 0; i < h.chunks; i++ {
		item, err := txn.Get(openImageChunkKey(i))
		if err != nil {
			return fmt.Errorf("chunk %d: %w", i, err)
		}
		if err := item.Value(func(val []byte) error {
			blob = append(blob, val...)
			return nil
		}); err != nil {
			return err
		}
	}
	if len(blob) != h.length || crc32.Checksum(blob, crc32cTable) != h.crc {
		return errors.New("length or checksum mismatch")
	}
	decoded, err := decodeOpenImage(blob)
	if err != nil {
		return err
	}
	*img = decoded
	return nil
}

// imgReader decodes the image; the first error sticks and every later read
// returns zero values. Counts are bounded by the bytes left, so a damaged
// count cannot allocate beyond the blob.
type imgReader struct {
	b   []byte
	err error
}

func (r *imgReader) fail(what string) {
	if r.err == nil {
		r.err = fmt.Errorf("open image: bad %s", what)
	}
}

func (r *imgReader) uvarint() uint64 {
	if r.err != nil {
		return 0
	}
	v, n := binary.Uvarint(r.b)
	if n <= 0 {
		r.fail("uvarint")
		return 0
	}
	r.b = r.b[n:]
	return v
}

func (r *imgReader) varint() int64 {
	if r.err != nil {
		return 0
	}
	v, n := binary.Varint(r.b)
	if n <= 0 {
		r.fail("varint")
		return 0
	}
	r.b = r.b[n:]
	return v
}

// count reads a length that must not exceed limit.
func (r *imgReader) count(limit int) int {
	v := r.uvarint()
	if v > uint64(limit) { // #nosec G115 -- limit is non-negative
		r.fail("count")
		return 0
	}
	return int(v) // #nosec G115 -- bounded by limit
}

func (r *imgReader) id() snowflake.ID {
	v := r.uvarint()
	if v == 0 || v > math.MaxInt64 {
		r.fail("id")
		return 0
	}
	return snowflake.ID(int64(v)) // #nosec G115 -- checked above
}

func (r *imgReader) u16() uint16 {
	if r.err != nil {
		return 0
	}
	if len(r.b) < 2 {
		r.fail("u16")
		return 0
	}
	v := binary.BigEndian.Uint16(r.b)
	r.b = r.b[2:]
	return v
}

func (r *imgReader) bytes(n int) []byte {
	if r.err != nil {
		return nil
	}
	if n > len(r.b) {
		r.fail("length")
		return nil
	}
	v := r.b[:n]
	r.b = r.b[n:]
	return v
}

// idSet reads appendIDSet's encoding; IDs must be positive and strictly
// ascending (a zero delta or an overflow fails the image).
func (r *imgReader) idSet() *indexpkg.IDSet {
	set := &indexpkg.IDSet{}
	n := r.count(len(r.b)) // each ID takes at least one byte
	prev := int64(0)
	for range n {
		d := r.uvarint()
		if r.err != nil {
			return nil
		}
		if d == 0 || d > math.MaxInt64-uint64(prev) { // #nosec G115 -- prev >= 0
			r.fail("id set")
			return nil
		}
		prev += int64(d) // #nosec G115 -- bounded above
		set.Add(prev)
	}
	return set
}

func (r *imgReader) str() string { return string(r.bytes(r.count(len(r.b)))) }

func (r *imgReader) valueIDs() []imgValueIDs {
	out := make([]imgValueIDs, r.count(len(r.b)))
	for i := range out {
		out[i].vk = r.str()
		out[i].ids = make([]snowflake.ID, r.count(len(r.b)))
		for j := range out[i].ids {
			out[i].ids[j] = r.id()
		}
	}
	return out
}

// imgPropTx decodes appendImgPropTx's encoding.
func imgPropTx[ID ~int64](r *imgReader) *indexpkg.PropertyTxMembers[ID] {
	m := indexpkg.NewPropertyTxMembers[ID]()
	for values := r.count(len(r.b)); values > 0 && r.err == nil; values-- {
		vk := r.str()
		if vk == "" {
			r.fail("value key") // Record ignores an empty key: never written
		}
		for n := r.count(len(r.b)); n > 0 && r.err == nil; n-- {
			mb := indexpkg.SpanMember[ID]{ID: ID(r.id()), FirstTx: types.Instant(r.varint())}
			flags := r.bytes(1)
			if r.err != nil {
				break
			}
			if flags[0]&^(imgPropTxAll|imgPropTxLate|imgPropTxCur|imgPropTxWild) != 0 {
				r.fail("posting flags")
				break
			}
			mb.HasAll, mb.HasLate = flags[0]&imgPropTxAll != 0, flags[0]&imgPropTxLate != 0
			mb.HasCur, mb.Wild = flags[0]&imgPropTxCur != 0, flags[0]&imgPropTxWild != 0
			mb.LastTx = types.Instant(r.varint())
			span := func() indexpkg.Span {
				return indexpkg.Span{From: types.Instant(r.varint()), To: types.Instant(r.varint())}
			}
			if mb.HasAll {
				mb.All = span()
			}
			if mb.HasLate {
				mb.Late = span()
			}
			if mb.HasCur {
				mb.Cur = span()
				v := r.uvarint()
				if v > math.MaxUint32 {
					r.fail("current version")
					break
				}
				mb.CurVersion = uint32(v) // #nosec G115 -- range checked
			}
			if r.err == nil {
				m.Restore(vk, mb)
			}
		}
	}
	return m
}

func (r *imgReader) intervals() []indexpkg.IntervalEntry {
	out := make([]indexpkg.IntervalEntry, r.count(len(r.b)))
	for i := range out {
		out[i].ID = r.id()
		out[i].From = types.Instant(r.varint())
		out[i].To = types.Instant(r.varint())
	}
	return out
}

func decodeOpenImage(blob []byte) (*openImage, error) {
	img := newOpenImage()
	top := imgReader{b: blob}
	for len(top.b) > 0 && top.err == nil {
		kind := top.bytes(1)
		if top.err != nil {
			break
		}
		r := imgReader{b: top.bytes(top.count(len(top.b)))}
		if top.err != nil {
			break
		}
		switch kind[0] {
		case imgSecNodeProp:
			key := indexpkg.PropertyIndexKey{LabelToken: r.u16(), PropertyKey: r.str()}
			img.nodeProps[key] = r.valueIDs()
		case imgSecRelProp:
			key := indexpkg.RelPropertyIndexKey{RelTypeToken: r.u16(), PropertyKey: r.str()}
			img.relProps[key] = r.valueIDs()
		case imgSecComposite:
			key := indexpkg.CompositeIndexKey{LabelToken: r.u16(), Keys: r.str()}
			img.composites[key] = r.valueIDs()
		case imgSecTemporal:
			tok := r.u16()
			img.temporal[tok] = r.intervals()
		case imgSecRelTemporal:
			tok := r.u16()
			img.relTemporal[tok] = r.intervals()
		case imgSecHF:
			tok := r.u16()
			entries := make([]imgHFEntry, r.count(len(r.b)))
			for i := range entries {
				entries[i].start = types.Instant(r.varint())
				entries[i].id = types.NodeID(r.id())
			}
			img.hf[tok] = entries
		case imgSecVector:
			key := indexpkg.VectorIndexKey{LabelToken: r.u16(), PropertyKey: r.str()}
			dims := r.count(len(r.b))
			entries := make([]imgVector, r.count(len(r.b)))
			for i := range entries {
				entries[i].id = r.id()
				raw := r.bytes(4 * dims)
				vec := make([]float32, dims)
				for d := range vec {
					if r.err == nil {
						vec[d] = math.Float32frombits(binary.BigEndian.Uint32(raw[4*d:]))
					}
				}
				entries[i].vec = vec
			}
			img.vectors[key] = entries
		case imgSecPlannerStats:
			img.stats = decodePlannerStats(&r)
		case imgSecRelPropTx, imgSecNodePropTx:
			// An older sidecar holds no valid-time spans (ledger row 62): skip
			// it; the first lookup rebuilds the sidecar from the rows.
			r.b = nil
		case imgSecRelPropTxSpans:
			key := indexpkg.RelPropertyIndexKey{RelTypeToken: r.u16(), PropertyKey: r.str()}
			img.relPropTx[key] = imgPropTx[types.RelID](&r)
		case imgSecNodePropTxSpans:
			key := indexpkg.PropertyIndexKey{LabelToken: r.u16(), PropertyKey: r.str()}
			img.nodePropTx[key] = imgPropTx[types.NodeID](&r)
		case imgSecNodeIDs:
			img.nodeIDs = r.idSet()
		case imgSecRelIDs:
			img.relIDs = r.idSet()
		case imgSecRelTypes:
			n := r.count(len(r.b))
			img.relTypes = make(map[uint16]*indexpkg.IDSet, n)
			for range n {
				tok := r.u16()
				img.relTypes[tok] = r.idSet()
			}
		case imgSecLabelCounts:
			n := r.count(len(r.b))
			img.labelCounts = make(map[uint16]int64, n)
			for range n {
				tok := r.u16()
				v := r.uvarint()
				if v > math.MaxInt64 {
					r.fail("label count")
				}
				img.labelCounts[tok] = int64(v) // #nosec G115 -- checked above
			}
		default:
			r.fail("section kind")
		}
		if r.err == nil && len(r.b) != 0 {
			r.fail("section length")
		}
		if r.err != nil {
			return nil, r.err
		}
	}
	if top.err != nil {
		return nil, top.err
	}
	return img, nil
}

// ---- install helpers (open path, single goroutine) ----

// installValueIDs adds each value's IDs ascending: an image written before
// backlog 72 holds them in map order, and an ascending fill gives exact chunks.
func installValueIDs(entries []imgValueIDs, add func(id snowflake.ID, vk string)) {
	for _, e := range entries {
		slices.Sort(e.ids)
		for _, id := range e.ids {
			add(id, e.vk)
		}
	}
}

// takeNodeProp / takeRelProp / takeComposite / takeHF / takeVector return the
// image's entries for one index definition and drop them from the image.
func (img *openImage) takeNodeProp(key indexpkg.PropertyIndexKey) ([]imgValueIDs, bool) {
	if img == nil {
		return nil, false
	}
	v, ok := img.nodeProps[key]
	delete(img.nodeProps, key)
	return v, ok
}

func (img *openImage) takeRelProp(key indexpkg.RelPropertyIndexKey) ([]imgValueIDs, bool) {
	if img == nil {
		return nil, false
	}
	v, ok := img.relProps[key]
	delete(img.relProps, key)
	return v, ok
}

// restoreRelPropTx / restoreNodePropTx install the image's sidecar for a
// declared rel / node property index as built (open path, single goroutine): the doors record into it from
// the first write on, as into one a pass built.
func (bs *Store) restoreRelPropTx(img *openImage, key indexpkg.RelPropertyIndexKey) {
	if img == nil || img.relPropTx[key] == nil {
		return
	}
	if bs.relPropTx == nil {
		bs.relPropTx = make(map[indexpkg.RelPropertyIndexKey]*propTxSidecar[types.RelID])
	}
	bs.relPropTx[key] = &propTxSidecar[types.RelID]{members: img.relPropTx[key], built: true}
	delete(img.relPropTx, key)
}

func (bs *Store) restoreNodePropTx(img *openImage, key indexpkg.PropertyIndexKey) {
	if img == nil || img.nodePropTx[key] == nil {
		return
	}
	if bs.nodePropTx == nil {
		bs.nodePropTx = make(map[indexpkg.PropertyIndexKey]*propTxSidecar[types.NodeID])
	}
	bs.nodePropTx[key] = &propTxSidecar[types.NodeID]{members: img.nodePropTx[key], built: true}
	delete(img.nodePropTx, key)
}

func (img *openImage) takeComposite(key indexpkg.CompositeIndexKey) ([]imgValueIDs, bool) {
	if img == nil {
		return nil, false
	}
	v, ok := img.composites[key]
	delete(img.composites, key)
	return v, ok
}

func (img *openImage) takeTemporal(tok uint16) ([]indexpkg.IntervalEntry, bool) {
	if img == nil {
		return nil, false
	}
	v, ok := img.temporal[tok]
	delete(img.temporal, tok)
	if ok {
		img.restoredTemporal[tok] = true
	}
	return v, ok
}

func (img *openImage) takeRelTemporal(tok uint16) ([]indexpkg.IntervalEntry, bool) {
	if img == nil {
		return nil, false
	}
	v, ok := img.relTemporal[tok]
	delete(img.relTemporal, tok)
	return v, ok
}

func (img *openImage) takeHF(tok uint16) ([]imgHFEntry, bool) {
	if img == nil {
		return nil, false
	}
	v, ok := img.hf[tok]
	delete(img.hf, tok)
	return v, ok
}

// takeVector also checks every vector against the definition's dimensions.
func (img *openImage) takeVector(key indexpkg.VectorIndexKey, dims int) ([]imgVector, bool) {
	if img == nil {
		return nil, false
	}
	v, ok := img.vectors[key]
	delete(img.vectors, key)
	for _, e := range v {
		if len(e.vec) != dims {
			return nil, false
		}
	}
	return v, ok
}
