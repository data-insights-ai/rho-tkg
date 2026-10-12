package badger

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"strconv"

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
// temporal, high-frequency and vector indexes — Close persists here, after its
// final flush, as one chunked blob (the "open image"). Planner statistics are
// not in it: a clean open defers them to their first read (ensurePlannerStats).
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

const openImageMagic = "TKGOI\x01"

const (
	imgSecNodeProp byte = iota + 1
	imgSecRelProp
	imgSecComposite
	imgSecTemporal
	imgSecRelTemporal
	imgSecHF
	imgSecVector
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
	}
}

// ---- Close side ----

// persistOpenImage writes the open image after Close's final flush. Best
// effort: the image only accelerates the next open, so a failure is logged and
// leaves no trusted header (the next open rebuilds).
func (bs *Store) persistOpenImage() {
	if bs.inMemory || bs.readOnly {
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

// encodeOpenImageLocked renders every RAM index content the rows would
// otherwise rebuild. Indexes still being created are left out (rebuilt).
// Caller holds idxMu (read).
func (bs *Store) encodeOpenImageLocked() []byte {
	var out []byte
	section := func(kind byte, payload []byte) {
		out = append(out, kind)
		out = binary.AppendUvarint(out, uint64(len(payload)))
		out = append(out, payload...)
	}
	appendValueIDs := func(b []byte, entries map[string]map[snowflake.ID]struct{}) []byte {
		b = binary.AppendUvarint(b, uint64(len(entries)))
		for vk, ids := range entries {
			b = appendImgString(b, vk)
			b = binary.AppendUvarint(b, uint64(len(ids)))
			for id := range ids {
				b = binary.AppendUvarint(b, uint64(id)) // #nosec G115 -- positive snowflake ID
			}
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
		section(imgSecComposite, appendValueIDs(b, idx.Entries))
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
	return out
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
	err := bs.db.View(func(txn *badgerv4.Txn) error {
		item, err := txn.Get(openImageHeaderKey)
		if errors.Is(err, badgerv4.ErrKeyNotFound) {
			return nil // never written, or a release without it closed the store
		}
		if err != nil {
			return err
		}
		if item.Version() != newest {
			return nil // something committed after the clean close
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

func installValueIDs(entries []imgValueIDs, add func(id snowflake.ID, vk string)) {
	for _, e := range entries {
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
