package graphapply

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

const rangeMoveReplayCursorBytes = 129 << 10
const rangeMoveReplayPageBytes = 1 << 20
const rangeMoveReplayRecordBytes = 1 << 20
const rangeMoveReplayInherited byte = 10
const rangeMoveReplayDone byte = 11

var rangeMoveReplayTags = [...]byte{0, 4, 5, 11, 16, 'A', 'D', 'K', 'P', 'R', 20}

var errRangeMoveReplayRepresentation = errors.New("graphapply: original record exceeds replay representation")

// The key is a physical frozen-source continuation, not a record accumulator.
// Original namespaces remain in each emitted native key, including inherited
// shadows; the immediate exporter and the original owner need not be equal.
type rangeMoveReplayCursor struct {
	family                byte
	after, key            []byte
	sequence, rows, bytes uint64
	digest                [32]byte
	complete              bool
	offset, recordBytes   uint32
	startSequence         uint64
}

type rangeMoveReplayPage struct {
	before, next        rangeMoveReplayCursor
	key, value          []byte
	control, complete   bool
	offset, recordBytes uint32
	recordDone          bool
}

func rangeMoveReplayStart() rangeMoveReplayCursor {
	return rangeMoveReplayCursor{family: 1, digest: sha256.Sum256([]byte("rho-tkg:range-move-replay:v1\x00"))}
}

func rangeMoveReplayCursorValid(c rangeMoveReplayCursor) bool {
	if c.family == 0 {
		return len(c.after)+len(c.key) == 0 && c.sequence|c.rows|c.bytes|c.startSequence == 0 && c.digest == ([32]byte{}) && !c.complete && c.offset|c.recordBytes == 0
	}
	if c.family > rangeMoveReplayDone || c.digest == ([32]byte{}) || c.rows > c.sequence || c.complete != (c.family == rangeMoveReplayDone) || len(c.after) > 64<<10 || len(c.key) > 64<<10 || c.recordBytes > rangeMoveReplayRecordBytes {
		return false
	}
	if c.complete && len(c.after) != 0 || !rangeMoveReplayPathValid(c.family, c.after, false) || !rangeMoveReplayPathValid(c.family, c.key, true) {
		return false
	}
	if c.offset == 0 {
		return len(c.key) == 0 && c.recordBytes == 0 && c.startSequence == 0
	}
	return !c.complete && len(c.key) > 0 && c.offset < c.recordBytes && c.startSequence > 0 && c.startSequence <= c.sequence
}

func rangeMoveReplayPathValid(family byte, key []byte, fragment bool) bool {
	if len(key) == 0 {
		return true
	}
	if family >= rangeMoveReplayDone {
		return false
	}
	if family == rangeMoveReplayInherited {
		if len(key) != 52 || !bytes.Equal(key[:3], []byte{'g', 'a', 20}) {
			return false
		}
		d := decoder{b: key[3:27]}
		n := namespace{graph: d.array(), partition: d.number()}
		_, _, err := rangeMoveReplayUnwrap(n, key)
		return n.valid() && err == nil
	}
	control := family >= 5 && family <= 9 && fragment
	_, tag, _, valid := rangeMoveReplayOriginal(key, control)
	want := rangeMoveReplayTags[family]
	if family >= 5 && family <= 9 && !fragment {
		want = 11
	}
	return valid && tag == want
}

func rangeMoveReplayClone(c rangeMoveReplayCursor) rangeMoveReplayCursor {
	c.after, c.key = bytes.Clone(c.after), bytes.Clone(c.key)
	return c
}

func rangeMoveReplayScopePaths(c rangeMoveReplayCursor, source namespace) bool {
	if c.family == 0 || c.family == rangeMoveReplayDone {
		return len(c.after)+len(c.key) == 0
	}
	tag := rangeMoveReplayTags[c.family]
	if c.family >= 5 && c.family <= 9 {
		tag = 11
	}
	if len(c.after) != 0 && !bytes.HasPrefix(c.after, recordKey(source, tag)) {
		return false
	}
	if len(c.key) != 0 {
		if c.family >= 5 && c.family <= 9 {
			n, family, _, valid := rangeMoveReplayOriginal(c.key, true)
			return valid && n == source && family == rangeMoveReplayTags[c.family]
		}
		return bytes.HasPrefix(c.key, recordKey(source, tag))
	}
	return true
}

func encodeRangeMoveReplayCursor(c rangeMoveReplayCursor, output *graphstate.OutputBudget) ([]byte, error) {
	if output == nil || !rangeMoveReplayCursorValid(c) {
		return nil, errInvalid
	}
	if err := output.Reserve(512 + 4*(len(c.after)+len(c.key))); err != nil {
		return nil, outputBudgetFailure(err)
	}
	return boundedEncoding(rangeMoveReplayCursorBytes, func(w *boundedWriter) {
		w.add([]byte{'G', 'E', 'C', 1})
		w.tag(c.family)
		w.field(c.after)
		w.field(c.key)
		w.u64(c.sequence)
		w.u64(c.rows)
		w.u64(c.bytes)
		w.add(c.digest[:])
		if c.complete {
			w.tag(1)
		} else {
			w.tag(0)
		}
		w.u32(int(c.offset))
		w.u32(int(c.recordBytes))
		w.u64(c.startSequence)
	})
}

func decodeRangeMoveReplayCursor(wire []byte, output *graphstate.OutputBudget) (rangeMoveReplayCursor, error) {
	if output == nil {
		return rangeMoveReplayCursor{}, errInvalid
	}
	body, err := graphBody(wire, "GEC\x01", rangeMoveReplayCursorBytes)
	if err != nil {
		return rangeMoveReplayCursor{}, err
	}
	if err := output.Reserve(512 + 4*len(wire)); err != nil {
		return rangeMoveReplayCursor{}, outputBudgetFailure(err)
	}
	r := graphCursor{b: body}
	c := rangeMoveReplayCursor{family: r.tag()}
	c.after, c.key = r.field(64<<10), r.field(64<<10)
	c.sequence, c.rows, c.bytes = r.u64(), r.u64(), r.u64()
	copy(c.digest[:], r.take(32))
	flag := r.tag()
	c.complete = flag == 1
	c.offset, c.recordBytes = uint32(r.count(rangeMoveReplayRecordBytes, 0)), uint32(r.count(rangeMoveReplayRecordBytes, 0)) // #nosec G115 -- counts bounded to1MiB.
	c.startSequence = r.u64()
	if r.err != nil || len(r.b) != 0 || flag > 1 || !rangeMoveReplayCursorValid(c) {
		return rangeMoveReplayCursor{}, errors.Join(errCorrupt, r.err)
	}
	c.after, c.key = bytes.Clone(c.after), bytes.Clone(c.key)
	if len(c.after) == 0 {
		c.after = nil
	}
	if len(c.key) == 0 {
		c.key = nil
	}
	return c, nil
}

// Fixed native ga and GP keys are bijective. Every encoded byte, including the
// original graph/partition and family, survives qualification in record20.
func rangeMoveReplayOriginal(key []byte, control bool) (n namespace, family byte, id [16]byte, valid bool) {
	start := 3
	if control {
		if len(key) != 44 || !bytes.Equal(key[:2], []byte("GP")) || key[3] != 1 || !rangeMoveReplayControlTag(key[2]) {
			return n, 0, id, false
		}
		family, start = key[2], 4
	} else {
		if len(key) != 43 || !bytes.Equal(key[:2], []byte("ga")) || !rangeMoveReplayDataTag(key[2]) {
			return n, 0, id, false
		}
		family = key[2]
	}
	d := decoder{b: key[start : start+24]}
	n = namespace{graph: d.array(), partition: d.number()}
	copy(id[:], key[start+24:])
	return n, family, id, n.valid() && id != ([16]byte{})
}

func rangeMoveReplayDataTag(tag byte) bool { return tag == 4 || tag == 5 || tag == 11 || tag == 16 }
func rangeMoveReplayControlTag(tag byte) bool {
	return tag == 'A' || tag == 'D' || tag == 'K' || tag == 'P' || tag == 'R'
}

func rangeMoveReplayNativeKey(n namespace, tag byte, id [16]byte) []byte {
	if rangeMoveReplayControlTag(tag) {
		return crossControlKey(n, id, tag)
	}
	return append(recordKey(n, tag), id[:]...)
}

func rangeMoveReplayUnwrap(source namespace, key []byte) (original []byte, control bool, err error) {
	if len(key) != 52 || !bytes.Equal(key[:27], recordKey(source, 20)) {
		return nil, false, errCorrupt
	}
	origin := namespace{graph: source.graph, partition: binary.BigEndian.Uint64(key[27:35])}
	tag := key[35]
	var id [16]byte
	copy(id[:], key[36:])
	if !origin.valid() || id == ([16]byte{}) || !rangeMoveReplayDataTag(tag) && !rangeMoveReplayControlTag(tag) {
		return nil, false, errCorrupt
	}
	return rangeMoveReplayNativeKey(origin, tag, id), rangeMoveReplayControlTag(tag), nil
}

func rangeMoveReplayDigest(previous [32]byte, control bool, key, value []byte) [32]byte {
	h := sha256.New()
	_, _ = h.Write(previous[:])
	flag := byte(0)
	if control {
		flag = 1
	}
	_, _ = h.Write([]byte{flag})
	var size [8]byte
	for _, field := range [][]byte{key, value} {
		binary.BigEndian.PutUint64(size[:], uint64(len(field)))
		_, _ = h.Write(size[:])
		_, _ = h.Write(field)
	}
	var digest [32]byte
	h.Sum(digest[:0])
	return digest
}

func rangeMoveReplayPageValid(p rangeMoveReplayPage) bool {
	b, n := p.before, p.next
	if !rangeMoveReplayCursorValid(b) || b.family == 0 || b.complete || !rangeMoveReplayCursorValid(n) || b.sequence == math.MaxUint64 || n.sequence != b.sequence+1 || p.complete != n.complete || p.recordBytes > rangeMoveReplayRecordBytes || uint64(p.offset)+uint64(len(p.value)) > uint64(p.recordBytes) {
		return false
	}
	if len(p.key) == 0 {
		return len(p.value) == 0 && !p.control && !p.recordDone && p.offset|p.recordBytes == 0 && b.offset == 0 && n.offset == 0 && n.rows == b.rows && n.bytes == b.bytes && n.digest == b.digest && (n.family == b.family && bytes.Compare(n.after, b.after) > 0 || n.family == b.family+1 && len(n.after) == 0)
	}
	if _, _, _, valid := rangeMoveReplayOriginal(p.key, p.control); !valid || n.family != b.family || p.offset != b.offset {
		return false
	}
	end := uint64(p.offset) + uint64(len(p.value))
	if p.recordDone {
		cost := uint64(len(p.key)) + uint64(p.recordBytes) + 17
		return end == uint64(p.recordBytes) && n.offset == 0 && b.rows < math.MaxUint64 && n.rows == b.rows+1 && cost <= math.MaxUint64-b.bytes && n.bytes == b.bytes+cost && n.digest != ([32]byte{}) && (p.offset != 0 || n.digest == rangeMoveReplayDigest(b.digest, p.control, p.key, p.value))
	}
	return len(p.value) > 0 && end < uint64(p.recordBytes) && uint64(n.offset) == end && n.recordBytes == p.recordBytes && n.rows == b.rows && n.bytes == b.bytes && n.digest == b.digest && n.startSequence == func() uint64 {
		if b.offset == 0 {
			return n.sequence
		}
		return b.startSequence
	}()
}

func encodeRangeMoveReplayPage(p rangeMoveReplayPage, output *graphstate.OutputBudget) ([]byte, error) {
	if output == nil || !rangeMoveReplayPageValid(p) {
		return nil, errInvalid
	}
	before, err := encodeRangeMoveReplayCursor(p.before, output)
	if err != nil {
		return nil, err
	}
	next, err := encodeRangeMoveReplayCursor(p.next, output)
	if err != nil {
		return nil, err
	}
	if err := output.Reserve(1024 + 4*(len(before)+len(next)+len(p.key)+len(p.value))); err != nil {
		return nil, outputBudgetFailure(err)
	}
	return boundedEncoding(rangeMoveReplayPageBytes, func(w *boundedWriter) {
		w.add([]byte{'G', 'E', 'P', 1})
		w.field(before)
		w.field(next)
		w.field(p.key)
		w.field(p.value)
		for _, flag := range []bool{p.control, p.complete, p.recordDone} {
			if flag {
				w.tag(1)
			} else {
				w.tag(0)
			}
		}
		w.u32(int(p.offset))
		w.u32(int(p.recordBytes))
	})
}

func decodeRangeMoveReplayPage(wire []byte, output *graphstate.OutputBudget) (rangeMoveReplayPage, error) {
	if output == nil {
		return rangeMoveReplayPage{}, errInvalid
	}
	body, err := graphBody(wire, "GEP\x01", rangeMoveReplayPageBytes)
	if err != nil {
		return rangeMoveReplayPage{}, err
	}
	if err := output.Reserve(1024 + 4*len(wire)); err != nil {
		return rangeMoveReplayPage{}, outputBudgetFailure(err)
	}
	c := graphCursor{b: body}
	before, next := c.field(rangeMoveReplayCursorBytes), c.field(rangeMoveReplayCursorBytes)
	p := rangeMoveReplayPage{key: c.field(64 << 10), value: c.field(rangeMoveReplayRecordBytes)}
	control, complete, done := c.tag(), c.tag(), c.tag()
	p.control, p.complete, p.recordDone = control == 1, complete == 1, done == 1
	p.offset, p.recordBytes = uint32(c.count(rangeMoveReplayRecordBytes, 0)), uint32(c.count(rangeMoveReplayRecordBytes, 0)) // #nosec G115 -- count bounded to1MiB.
	if c.err != nil || len(c.b) != 0 || control > 1 || complete > 1 || done > 1 {
		return rangeMoveReplayPage{}, errors.Join(errCorrupt, c.err)
	}
	p.before, err = decodeRangeMoveReplayCursor(before, output)
	if err != nil {
		return rangeMoveReplayPage{}, err
	}
	p.next, err = decodeRangeMoveReplayCursor(next, output)
	if err != nil {
		return rangeMoveReplayPage{}, err
	}
	if !rangeMoveReplayPageValid(p) {
		return rangeMoveReplayPage{}, errCorrupt
	}
	p.key, p.value = bytes.Clone(p.key), bytes.Clone(p.value)
	if len(p.key) == 0 {
		p.key = nil
	}
	if len(p.value) == 0 {
		p.value = nil
	}
	return p, nil
}

func rangeMoveReplayReader(q *reader, expected namespace) (graphstore.OwnershipDeclaration, error) {
	if q == nil || q.view == nil || q.ctx == nil || q.arena == nil || q.ns != expected || !expected.valid() {
		return graphstore.OwnershipDeclaration{}, errInvalid
	}
	if err := q.ctx.Err(); err != nil {
		return graphstore.OwnershipDeclaration{}, err
	}
	rows, workBytes := min(65536, q.limits.readRows-q.rows), min(64<<20, q.limits.readBytes-q.bytes)
	if rows < 1 || workBytes < 4096 {
		return graphstore.OwnershipDeclaration{}, errLimit
	}
	// ReadOwnershipDeclaration performs its own bounded preallocation checks.
	// Its complete possible record+entry backing is prepaid on THIS parent;
	// no independent/default arena is created by the replay layer.
	const owned = 2*rangeMoveReplayRecordBytes + 8192
	if err := reserveComposer(q, owned); err != nil {
		return graphstore.OwnershipDeclaration{}, err
	}
	l := graphstore.DefaultLimits()
	l.MaxRecordBytes, l.MaxReadRows, l.MaxReadBytes = rangeMoveReplayRecordBytes, rows, workBytes
	info, work, err := graphstore.ReadOwnershipDeclaration(q.ctx, q.view, l, graphstore.OwnershipBudget{SourceRows: rows, SourceBytes: workBytes, OutputBytes: owned})
	q.rows += work.Records
	q.bytes += work.Bytes
	if err != nil {
		return graphstore.OwnershipDeclaration{}, err
	}
	ref, root := info.Reference(), info.Root()
	if !info.Published() || root.Namespace().Graph != [16]byte(expected.graph) || root.Namespace().Partition != expected.partition || q.base.Index != ref.Index || q.base.ImageHash != ref.ImageHash {
		return graphstore.OwnershipDeclaration{}, errInvalid
	}
	return info.Declaration(), nil
}

func rangeMoveReplayScan(q *reader, prefix, after []byte, maximum int, rows int) (raftlog.ApplicationPage, error) {
	if len(after) != 0 && !bytes.HasPrefix(after, prefix) {
		return raftlog.ApplicationPage{}, errInvalid
	}
	upper := bytes.Clone(prefix)
	for i := len(upper) - 1; i >= 0; i-- {
		upper[i]++
		if upper[i] != 0 {
			upper = upper[:i+1]
			break
		}
	}
	bound := min(maximum, q.view.ReadLimits().Bytes, q.limits.readBytes-q.bytes, q.arena.Remaining()-512)
	if rows < 1 || bound < 512 {
		return raftlog.ApplicationPage{}, errLimit
	}
	if err := reserveComposer(q, bound+256); err != nil {
		return raftlog.ApplicationPage{}, err
	}
	page, err := q.view.Scan(q.ctx, prefix, upper, after, raftlog.ReadBudget{Rows: min(rows, 1, q.view.ReadLimits().Rows), Bytes: bound})
	q.rows += page.Visited
	q.bytes += page.Bytes
	if err != nil {
		return raftlog.ApplicationPage{}, err
	}
	if len(page.Rows) > 1 || page.Visited > 1 || page.Bytes < 0 || len(page.Rows) == 0 && !page.Complete && len(page.Next) == 0 {
		return raftlog.ApplicationPage{}, errCorrupt
	}
	return page, nil
}

func exportRangeMoveReplayPage(q *reader, source namespace, prior rangeMoveReplayCursor, budget raftlog.ReadBudget) (page rangeMoveReplayPage, err error) {
	if !rangeMoveReplayCursorValid(prior) || prior.complete || budget.Rows < 1 || budget.Bytes <= 1024 || budget.Bytes > rangeMoveReplayPageBytes {
		return page, errInvalid
	}
	declaration, err := rangeMoveReplayReader(q, source)
	if err != nil {
		return page, err
	}
	return exportRangeMoveReplayPageDeclared(q, source, prior, budget, declaration)
}

// Called only inside the same captured-view operation that read declaration.
// Each page still checks its full origin/path and pays all record/output work.
func exportRangeMoveReplayPageDeclared(q *reader, source namespace, prior rangeMoveReplayCursor, budget raftlog.ReadBudget, declaration graphstore.OwnershipDeclaration) (page rangeMoveReplayPage, err error) {
	if q == nil || q.view == nil || q.ctx == nil || q.ns != source || !rangeMoveReplayCursorValid(prior) || prior.complete || budget.Rows < 1 || budget.Bytes <= 1024 || budget.Bytes > rangeMoveReplayPageBytes {
		return page, errInvalid
	}
	if err := q.ctx.Err(); err != nil {
		return page, err
	}
	c := prior
	if c.family == 0 {
		c = rangeMoveReplayStart()
	}
	if !rangeMoveReplayScopePaths(c, source) {
		return page, errInvalid
	}
	if c.sequence == math.MaxUint64 {
		return page, errLimit
	}
	page.before, page.next = c, c
	page.next.sequence++
	var raw raftlog.KV
	var original []byte
	control := c.family >= 5 && c.family <= 9
	tags := rangeMoveReplayTags
	if c.offset != 0 {
		if control {
			if n, tag, _, valid := rangeMoveReplayOriginal(c.key, true); !valid || n != source || tag != tags[c.family] {
				return rangeMoveReplayPage{}, errInvalid
			}
			if err := reserveComposer(q, 2*rangeMoveReplayRecordBytes+1024); err != nil {
				return rangeMoveReplayPage{}, err
			}
			r, found, e := crossControl(q, c.key, rangeMoveReplayRecordBytes)
			if e != nil || !found {
				return rangeMoveReplayPage{}, errors.Join(errCorrupt, e)
			}
			raw = raftlog.KV{Key: r.Key, Value: r.Value}
		} else {
			prefix := recordKey(source, tags[c.family])
			if !bytes.HasPrefix(c.key, prefix) {
				return rangeMoveReplayPage{}, errInvalid
			}
			value, found, e := q.getBounded(c.key, rangeMoveReplayRecordBytes)
			if e != nil || !found {
				return rangeMoveReplayPage{}, errors.Join(errCorrupt, e)
			}
			raw = raftlog.KV{Key: c.key, Value: value}
		}
	} else {
		tag := tags[c.family]
		if control {
			tag = 11
		}
		maximum := 8192
		if c.family == rangeMoveReplayInherited {
			maximum = 2*rangeMoveReplayRecordBytes + 8192
		}
		scan, e := rangeMoveReplayScan(q, recordKey(source, tag), c.after, maximum, min(budget.Rows, q.limits.readRows-q.rows))
		if e != nil {
			return rangeMoveReplayPage{}, e
		}
		if len(scan.Rows) == 0 {
			if scan.Complete {
				page.next.family++
				page.next.after = nil
				page.next.complete = page.next.family == rangeMoveReplayDone
				page.complete = page.next.complete
			} else {
				page.next.after = scan.Next
			}
			return rangeMoveReplayOwnPage(q, page)
		}
		raw = scan.Rows[0]
		if control {
			_, _, id, valid := rangeMoveReplayOriginal(raw.Key, false)
			if !valid {
				return rangeMoveReplayPage{}, errCorrupt
			}
			key := crossControlKey(source, id, tags[c.family])
			if err := reserveComposer(q, 2*rangeMoveReplayRecordBytes+1024); err != nil {
				return rangeMoveReplayPage{}, err
			}
			r, found, e := crossControl(q, key, rangeMoveReplayRecordBytes)
			if e != nil {
				return rangeMoveReplayPage{}, e
			}
			if !found {
				page.next.after = raw.Key
				return rangeMoveReplayOwnPage(q, page)
			}
			raw = raftlog.KV{Key: r.Key, Value: r.Value}
		}
	}
	original = raw.Key
	if c.family == rangeMoveReplayInherited {
		original, control, err = rangeMoveReplayUnwrap(source, raw.Key)
		if err != nil {
			return rangeMoveReplayPage{}, err
		}
	} else if n, _, _, valid := rangeMoveReplayOriginal(original, control); !valid || n != source {
		return rangeMoveReplayPage{}, errCorrupt
	}
	originalNamespace, _, _, _ := rangeMoveReplayOriginal(original, control)
	if _, found := declaration.Partition(originalNamespace.partition); !found {
		return rangeMoveReplayPage{}, errCorrupt
	}
	if len(raw.Value) > rangeMoveReplayRecordBytes {
		return rangeMoveReplayPage{}, errors.Join(errRangeMoveReplayRepresentation, errLimit)
	}
	if c.offset != 0 && (int(c.recordBytes) != len(raw.Value) || int(c.offset) >= len(raw.Value)) {
		return rangeMoveReplayPage{}, errCorrupt
	}
	end := int(c.offset) + min(budget.Bytes-1024, len(raw.Value)-int(c.offset))
	page.key, page.value, page.control = original, raw.Value[c.offset:uint32(end)], control                  // #nosec G115 -- end checked within delivered <=1MiB value.
	page.offset, page.recordBytes, page.recordDone = c.offset, uint32(len(raw.Value)), end == len(raw.Value) // #nosec G115 -- native record limit <=1MiB.
	if page.recordDone {
		cost := uint64(len(original) + len(raw.Value) + 17)
		if c.rows == math.MaxUint64 || cost > math.MaxUint64-c.bytes {
			return rangeMoveReplayPage{}, errLimit
		}
		page.next.rows++
		page.next.bytes += cost
		page.next.digest = rangeMoveReplayDigest(c.digest, control, original, raw.Value)
		page.next.key, page.next.offset, page.next.recordBytes, page.next.startSequence = nil, 0, 0, 0
		page.next.after = raw.Key
		if c.family >= 5 && c.family <= 9 {
			_, _, id, _ := rangeMoveReplayOriginal(raw.Key, true)
			page.next.after = crossKey(source, id)
		}
	} else {
		page.next.key, page.next.offset, page.next.recordBytes = raw.Key, uint32(end), uint32(len(raw.Value)) // #nosec G115 -- end/record bounded <=1MiB.
		if c.offset == 0 {
			page.next.startSequence = page.next.sequence
		}
	}
	return rangeMoveReplayOwnPage(q, page)
}

func rangeMoveReplayOwnPage(q *reader, p rangeMoveReplayPage) (rangeMoveReplayPage, error) {
	if !rangeMoveReplayPageValid(p) {
		return rangeMoveReplayPage{}, errCorrupt
	}
	if err := reserveComposer(q, 1024+len(p.key)+len(p.value)+len(p.before.after)+len(p.before.key)+len(p.next.after)+len(p.next.key)); err != nil {
		return rangeMoveReplayPage{}, err
	}
	p.before, p.next = rangeMoveReplayClone(p.before), rangeMoveReplayClone(p.next)
	p.key, p.value = bytes.Clone(p.key), bytes.Clone(p.value)
	return p, nil
}

func rangeMoveReplayFragmentKey(target namespace, move [16]byte, sequence uint64) []byte {
	return binary.BigEndian.AppendUint64(rangeMoveControlKey(target, move, 'E'), sequence)
}

func stageRangeMoveReplayPage(q *reader, origin, target namespace, move [16]byte, page rangeMoveReplayPage) error {
	if move == ([16]byte{}) || !origin.valid() || origin.graph != target.graph || origin.partition == target.partition || !rangeMoveReplayPageValid(page) {
		return errInvalid
	}
	if !rangeMoveReplayScopePaths(page.before, origin) || !rangeMoveReplayScopePaths(page.next, origin) {
		return errInvalid
	}
	declaration, err := rangeMoveReplayReader(q, target)
	if err != nil {
		return err
	}
	return stageRangeMoveReplayPageDeclared(q, origin, target, move, page, declaration)
}

// The reducer emits only fragment keys and inherited-original keys, so no
// subframe can change this operation's captured ownership declaration.
func stageRangeMoveReplayPageDeclared(q *reader, origin, target namespace, move [16]byte, page rangeMoveReplayPage, declaration graphstore.OwnershipDeclaration) error {
	if q == nil || q.view == nil || q.ctx == nil || q.ns != target || move == ([16]byte{}) || !origin.valid() || origin.graph != target.graph || origin.partition == target.partition || !rangeMoveReplayPageValid(page) || !rangeMoveReplayScopePaths(page.before, origin) || !rangeMoveReplayScopePaths(page.next, origin) {
		return errInvalid
	}
	if err := q.ctx.Err(); err != nil {
		return err
	}
	if _, found := declaration.Partition(origin.partition); !found {
		return errInvalid
	}
	if len(page.key) == 0 {
		return nil
	}
	original, tag, id, valid := rangeMoveReplayOriginal(page.key, page.control)
	if !valid || original.graph != origin.graph || page.before.family != rangeMoveReplayInherited && original != origin {
		return errInvalid
	}
	if _, found := declaration.Partition(original.partition); !found {
		return errInvalid
	}
	if page.before.family != rangeMoveReplayInherited && (tag != rangeMoveReplayTags[page.before.family] || page.control != (page.before.family >= 5 && page.before.family <= 9)) {
		return errInvalid
	}
	physical := page.key
	if page.before.family == rangeMoveReplayInherited {
		physical = rangeMoveReplayKey(origin, original.partition, id, tag)
	}
	path := physical
	if page.before.family >= 5 && page.before.family <= 9 {
		path = crossKey(origin, id)
	}
	prefixTag := rangeMoveReplayTags[page.before.family]
	if page.before.family >= 5 && page.before.family <= 9 {
		prefixTag = 11
	}
	if len(page.before.after) != 0 && !bytes.HasPrefix(page.before.after, recordKey(origin, prefixTag)) || page.before.offset == 0 && bytes.Compare(path, page.before.after) <= 0 {
		return errInvalid
	}
	if page.before.offset > 0 && (!bytes.Equal(page.before.key, physical) || page.before.recordBytes != page.recordBytes) {
		return errInvalid
	}
	if page.recordDone {
		if !bytes.Equal(page.next.after, path) {
			return errInvalid
		}
	} else if !bytes.Equal(page.next.key, physical) {
		return errInvalid
	}
	if !page.recordDone {
		return q.put(rangeMoveReplayFragmentKey(target, move, page.next.sequence), page.value)
	}
	if page.offset > 0 {
		start := page.before.startSequence
		remaining := q.limits.readRows - q.rows
		if start == 0 || start > page.before.sequence || remaining < 2 || page.before.sequence-start+1 > uint64(remaining-1) {
			return errLimit
		}
	} else if q.rows >= q.limits.readRows {
		return errLimit
	}
	if err := reserveComposer(q, 1024+int(page.recordBytes)+4*len(page.key)); err != nil {
		return err
	}
	value := make([]byte, int(page.recordBytes))
	offset := 0
	if page.offset > 0 {
		start := page.before.startSequence
		for sequence := start; sequence <= page.before.sequence; sequence++ {
			fragment, found, err := q.getBounded(rangeMoveReplayFragmentKey(target, move, sequence), rangeMoveReplayRecordBytes)
			if err != nil {
				return err
			}
			if !found || len(fragment) == 0 || len(fragment) > int(page.offset)-offset {
				return errCorrupt
			}
			copy(value[offset:], fragment)
			offset += len(fragment)
		}
		if offset != int(page.offset) {
			return errCorrupt
		}
	}
	copy(value[offset:], page.value)
	if page.next.digest != rangeMoveReplayDigest(page.before.digest, page.control, page.key, value) {
		return errInvalid
	}
	key := rangeMoveReplayKey(target, original.partition, id, tag)
	previous, found, err := rangeMoveReplayPoint(q, key)
	if err != nil {
		return err
	}
	if found {
		if !bytes.Equal(previous, value) {
			return errCorrupt
		}
		return nil
	}
	return q.put(key, value)
}

// Lookup returns only copied original bytes. The embedding materializer must
// authenticate placement/COMMIT before use; no shadow is a source proof.
func lookupRangeMoveReplay(q *reader, origin namespace, control bool, key []byte) ([]byte, bool, error) {
	if q == nil || !origin.valid() || q.ns.graph != origin.graph {
		return nil, false, errInvalid
	}
	n, tag, id, valid := rangeMoveReplayOriginal(key, control)
	if !valid || n != origin {
		return nil, false, errInvalid
	}
	declaration, err := rangeMoveReplayReader(q, q.ns)
	if err != nil {
		return nil, false, err
	}
	if _, found := declaration.Partition(origin.partition); !found {
		return nil, false, errInvalid
	}
	return rangeMoveReplayPoint(q, rangeMoveReplayKey(q.ns, origin.partition, id, tag))
}

func rangeMoveReplayPoint(q *reader, key []byte) ([]byte, bool, error) {
	if q.rows >= q.limits.readRows {
		return nil, false, errLimit
	}
	maximum := min(len(key)+rangeMoveReplayRecordBytes, q.view.ReadLimits().Bytes, q.limits.readBytes-q.bytes-64, q.arena.Remaining()-256)
	if maximum < len(key) {
		return nil, false, errLimit
	}
	if err := reserveComposer(q, maximum+256); err != nil {
		return nil, false, err
	}
	row, found, err := q.view.Get(q.ctx, key, maximum)
	q.rows++
	q.bytes += max(len(key), cap(row.Key)) + cap(row.Value) + 64
	if err != nil {
		return nil, false, err
	}
	if !found || row.Deleted {
		return nil, false, nil
	}
	return row.Value, true, nil
}
