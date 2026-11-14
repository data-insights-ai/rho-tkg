package index

import (
	"encoding/binary"
	"errors"
	"math"
)

// Encoded IDSet (R3 cold open, tasks/evidence/R3/coldread.txt).
//
// An open from a trusted image used to insert each of the image's IDs one at a
// time (a binary search of the chunks and one append each): 1.4 ms for 50,000
// IDs, half the CPU of a read-only open at ai-soc's configuration. A handle
// that serves one read and closes does not need the sorted chunks at all: it
// asks whether an ID is in the set, or lists it. NewIDSetEncoded wraps the
// image's own encoding (ascending IDs as uvarint gaps, the first from 0) after
// one validating pass, and keeps an anchor every idAnchorStride IDs, so Has
// binary-searches the anchors and decodes at most idAnchorStride-1 gaps
// (about 0.1 us against 0.05 us for the chunks), and Ascend decodes the
// stream. The set is immutable in this form: its readers need no lock among
// themselves, as for the chunks. The first Add or Remove (a write, under the
// caller's exclusive lock) builds the chunks from the stream and the set is
// an ordinary one from then on.

// idAnchorStride is how many IDs lie between two anchors.
const idAnchorStride = 64

// ErrBadEncodedIDSet is returned for a stream that is not n ascending positive
// IDs as uvarint gaps.
var ErrBadEncodedIDSet = errors.New("index: encoded ID set is not ascending positive IDs")

// idAnchor is the ID at a position that is a multiple of idAnchorStride and the
// offset of the gap after it.
type idAnchor struct {
	id  int64
	off int
}

// encodedIDs is an immutable set in the image's encoding.
type encodedIDs struct {
	b       []byte // n gaps: ascending IDs as uvarint deltas, the first from 0
	n       int
	anchors []idAnchor
}

// NewIDSetEncoded returns the set of the n IDs encoded at the start of b and
// the number of bytes they take. It validates as a decode into a set would:
// n positive gaps, each present and not overflowing the ID range, or
// ErrBadEncodedIDSet. The set keeps b[:used]: the caller must not change it.
// n is bounded by the bytes delivered (each gap takes one), so a declared
// count cannot allocate beyond b. A set of no IDs is the empty set, not
// encoded.
func NewIDSetEncoded(b []byte, n int) (set IDSet, used int, err error) {
	if n == 0 {
		return IDSet{}, 0, nil
	}
	if n < 0 || n > len(b) {
		return IDSet{}, 0, ErrBadEncodedIDSet
	}
	e := &encodedIDs{n: n, anchors: make([]idAnchor, 0, (n+idAnchorStride-1)/idAnchorStride)}
	prev, off := int64(0), 0
	for i := 0; i < n; i++ {
		d, k := binary.Uvarint(b[off:])
		if k <= 0 || d == 0 || d > uint64(math.MaxInt64-prev) { // #nosec G115 -- prev >= 0
			return IDSet{}, 0, ErrBadEncodedIDSet
		}
		prev += int64(d) // #nosec G115 -- bounded above
		off += k
		if i%idAnchorStride == 0 {
			e.anchors = append(e.anchors, idAnchor{id: prev, off: off})
		}
	}
	e.b = b[:off:off]
	return IDSet{enc: e}, off, nil
}

// has reports whether id is in the set: the last anchor not above id, then the
// gaps up to the next anchor.
func (e *encodedIDs) has(id int64) bool {
	lo, hi := 0, len(e.anchors) // the first anchor above id
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if e.anchors[mid].id > id {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	if lo == 0 {
		return false // below the first ID
	}
	a := e.anchors[lo-1]
	if a.id == id {
		return true
	}
	prev, off := a.id, a.off
	for k := 1; k < idAnchorStride && off < len(e.b); k++ {
		d, sz := binary.Uvarint(e.b[off:]) // validated at NewIDSetEncoded
		off += sz
		prev += int64(d) // #nosec G115 -- validated: no overflow
		if prev >= id {
			return prev == id
		}
	}
	return false
}

// ascend calls fn for every ID in ascending order until fn returns false.
func (e *encodedIDs) ascend(fn func(id int64) bool) {
	prev, off := int64(0), 0
	for i := 0; i < e.n; i++ {
		d, sz := binary.Uvarint(e.b[off:]) // validated at NewIDSetEncoded
		off += sz
		prev += int64(d) // #nosec G115 -- validated: no overflow
		if !fn(prev) {
			return
		}
	}
}

// thaw builds the chunks of an encoded set and drops the encoded form: the
// first write. The caller holds the exclusive lock a write needs.
func (s *IDSet) thaw() {
	e := s.enc
	if e == nil {
		return
	}
	s.enc = nil
	var buf [orderedKeyChunk]int64
	k := 0
	e.ascend(func(id int64) bool {
		buf[k] = id
		if k++; k == len(buf) {
			s.s.appendRun(buf[:k]) // exact chunks, as an ascending stream of Add makes them
			k = 0
		}
		return true
	})
	if k > 0 {
		s.s.appendRun(buf[:k])
	}
}
