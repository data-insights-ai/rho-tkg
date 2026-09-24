package segment

import (
	"encoding/hex"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Dicts are the store-level dictionaries a set of in-RAM segments share
// (ADR-0011 S2): endpoints (Nodes) and the values of declared string columns
// (Strings, by column name). Either may be nil/absent: that part of a segment
// is then self-contained. A segment encoded with dictionaries is readable
// only with the same ones; a spill to disk (S3) writes self-contained
// segments.
type Dicts struct {
	Nodes   *NodeDict
	Strings map[string]*StringDict
}

// NodeDict is an append-only dictionary of relationship endpoints shared by
// the in-RAM segments of one store (ADR-0011 S2). Each distinct endpoint
// NodeID and each distinct endpoint hash is held once for the store; a
// segment encoded with the dictionary (EncodeWithDicts) stores 32-bit
// codes into it instead of its own node table and hash strings, so endpoint
// bytes grow with the distinct endpoints, not with segments × endpoints.
//
// Codes never change once assigned, so a segment holds the snapshot current
// when it was opened and every later snapshot only extends it. Writers are
// serialized by the dictionary's mutex; readers never lock. A segment encoded
// with a dictionary is readable only with that dictionary: it is an in-RAM
// format, and a spill to disk (S3) writes a self-contained segment.
type NodeDict struct {
	mu    sync.Mutex
	codes map[types.NodeID]uint32
	extra map[nodeHashKey]uint32 // a node's second and later distinct hashes
	snap  atomic.Pointer[nodeDictSnap]
}

type nodeHashKey struct {
	node uint32
	hash string
}

// nodeDictSnap is an immutable view. Hash code c names raw[32*(c>>1):] when
// c&1 == 0 (a 64-hex lowercase hash, stored as its 32 bytes) and lit[c>>1]
// otherwise (any other string, the empty one included).
type nodeDictSnap struct {
	ids   []int64  // node code -> NodeID
	first []uint32 // node code -> hash code of the first hash interned for it
	raw   []byte
	lit   []string
}

// NewNodeDict returns an empty dictionary.
func NewNodeDict() *NodeDict {
	d := &NodeDict{codes: map[types.NodeID]uint32{}, extra: map[nodeHashKey]uint32{}}
	d.snap.Store(&nodeDictSnap{})
	return d
}

// Len returns the number of distinct endpoints and distinct endpoint hashes.
func (d *NodeDict) Len() (nodes, hashes int) {
	s := d.snap.Load()
	return len(s.ids), len(s.raw)/32 + len(s.lit)
}

// Bytes is the size of the dictionary's values (NodeIDs, first-hash codes,
// hash bytes, literal strings with their headers); the lookup maps a writer
// keeps are not included.
func (d *NodeDict) Bytes() int {
	s := d.snap.Load()
	n := 8*len(s.ids) + 4*len(s.first) + len(s.raw)
	for _, l := range s.lit {
		n += 16 + len(l)
	}
	return n
}

func (s *nodeDictSnap) hashCount() int { return len(s.raw)/32 + len(s.lit) }

// validHash reports whether hash code c exists in the snapshot.
func (s *nodeDictSnap) validHash(c uint64) bool {
	i := c >> 1
	if c&1 == 0 {
		return i < uint64(len(s.raw)/32)
	}
	return i < uint64(len(s.lit))
}

// sameHash reports whether hash code c spells h, without building a string.
func (s *nodeDictSnap) sameHash(c uint64, h string) bool {
	i := c >> 1
	if c&1 == 0 {
		var b [32]byte
		if !isLowerHex64(h) {
			return false
		}
		_, _ = hex.Decode(b[:], []byte(h))
		return string(s.raw[32*i:32*i+32]) == string(b[:])
	}
	return s.lit[i] == h
}

// hashString returns hash code c's string (c must be valid).
func (s *nodeDictSnap) hashString(c uint64) string {
	i := c >> 1
	if c&1 == 0 {
		return hex.EncodeToString(s.raw[32*i : 32*i+32])
	}
	return s.lit[i]
}

// dictTxn interns under the dictionary's lock into a working snapshot that
// shares the published one's backing arrays (appends land beyond every
// published length, which no reader indexes). commit publishes it; a txn is
// always committed, so the maps never name an unpublished code.
type dictTxn struct {
	d *NodeDict
	s nodeDictSnap
}

func (d *NodeDict) begin() *dictTxn {
	d.mu.Lock()
	return &dictTxn{d: d, s: *d.snap.Load()}
}

func (t *dictTxn) commit() *nodeDictSnap {
	s := t.s
	t.d.snap.Store(&s)
	t.d.mu.Unlock()
	return &s
}

// hashBytes returns h's 32 raw bytes when it is 64-hex lowercase.
func hashBytes(h string) ([]byte, bool) {
	if !isLowerHex64(h) {
		return nil, false
	}
	b, _ := hex.DecodeString(h) // validated
	return b, true
}

func (t *dictTxn) newHash(h string) uint32 {
	if b, ok := hashBytes(h); ok {
		c := uint32(len(t.s.raw)/32) << 1 // #nosec G115 -- bounded by rows sealed
		t.s.raw = append(t.s.raw, b...)
		return c
	}
	c := uint32(len(t.s.lit))<<1 | 1 // #nosec G115 -- bounded by rows sealed
	t.s.lit = append(t.s.lit, h)
	return c
}

// node returns id's code, adding it with first hash h when new.
func (t *dictTxn) node(id int64, h string) uint32 {
	if c, ok := t.d.codes[types.NodeID(id)]; ok {
		return c
	}
	c := uint32(len(t.s.ids)) // #nosec G115 -- bounded by rows sealed
	t.s.ids = append(t.s.ids, id)
	t.s.first = append(t.s.first, t.newHash(h))
	t.d.codes[types.NodeID(id)] = c
	return c
}

// hash returns the code of hash h seen on node code n and whether it is the
// node's first hash (then the rows store nothing for it).
func (t *dictTxn) hash(n uint32, h string) (uint32, bool) {
	fc := t.s.first[n]
	if t.s.sameHash(uint64(fc), h) {
		return fc, true
	}
	k := nodeHashKey{node: n, hash: h}
	if c, ok := t.d.extra[k]; ok {
		return c, false
	}
	c := t.newHash(h)
	t.d.extra[k] = c
	return c, false
}

// StringDict is an append-only dictionary of one declared string column's
// values, shared by the in-RAM segments of one store: a segment stores codes
// into it instead of its own dictionary (a per-segment dictionary repeats
// every value once per segment, and a small segment of a high-cardinality
// column falls back to plain strings). A column whose values grow with the
// rows (more than one new value per 8 rows once the dictionary holds 4,096)
// is not interned: that segment encodes it itself, as without a dictionary,
// so the dictionary never grows with the rows (ADR-0011 §2.2).
type StringDict struct {
	mu    sync.Mutex
	codes map[string]uint32
	rows  int64 // rows encoded through the dictionary (the growth guard)
	snap  atomic.Pointer[[]string]
}

// NewStringDict returns an empty dictionary.
func NewStringDict() *StringDict {
	d := &StringDict{codes: map[string]uint32{}}
	d.snap.Store(&[]string{})
	return d
}

// Len returns the number of distinct values.
func (d *StringDict) Len() int { return len(*d.snap.Load()) }

// Bytes is the size of the values with their string headers (the writer's
// lookup map, which shares the string bytes, is not included).
func (d *StringDict) Bytes() int {
	n := 0
	for _, v := range *d.snap.Load() {
		n += 16 + len(v)
	}
	return n
}

// Guard constants for StringDict.
const (
	stringDictFloor       = 4096
	stringDictRowsPerCode = 8
)

// intern returns codes for vals, or ok=false (nothing interned) when the
// values would grow the dictionary faster than one per stringDictRowsPerCode
// rows.
func (d *StringDict) intern(vals []string) (codes []int64, ok bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	cur := *d.snap.Load()
	fresh := map[string]struct{}{}
	for _, v := range vals {
		if _, known := d.codes[v]; !known {
			fresh[v] = struct{}{}
		}
	}
	limit := max(int64(stringDictFloor), (d.rows+int64(len(vals)))/stringDictRowsPerCode)
	if int64(len(cur)+len(fresh)) > limit {
		return nil, false
	}
	next := cur // appends land beyond every published length
	codes = make([]int64, len(vals))
	for i, v := range vals {
		c, known := d.codes[v]
		if !known {
			c = uint32(len(next)) // #nosec G115 -- bounded by the guard
			v = strings.Clone(v)  // never pin the row that carried it
			next = append(next, v)
			d.codes[v] = c
		}
		codes[i] = int64(c)
	}
	d.rows += int64(len(vals))
	d.snap.Store(&next)
	return codes, true
}

func (d *Dicts) stringDict(col string) *StringDict {
	if d == nil {
		return nil
	}
	return d.Strings[col]
}

func (d *Dicts) nodeDict() *NodeDict {
	if d == nil {
		return nil
	}
	return d.Nodes
}
