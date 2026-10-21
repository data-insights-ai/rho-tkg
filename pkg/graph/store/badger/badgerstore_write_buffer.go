package badger

import "strings"

// writeBucketLen is the width of the write buffer's prefix index: the key tag
// plus the 8-byte entity ID every per-entity keyspace starts with (history
// tag|id|version, adjacency tag|node|type|other|rel, ex-adjacency
// tag|node|dir|rel). A prefix of at least this length names exactly one bucket.
const writeBucketLen = 9

type writeBucket [writeBucketLen]byte

// writeBuf is one generation of the write buffer (pending, or the flushing
// snapshot): ops keyed by string(key), last write wins, plus a per-bucket chain
// of its keys so a per-entity overlay read costs O(the bucket's keys), not
// O(buffer) (row 102). Keys are never removed one by one (a delete is a
// writeOpDelete op), so the index is append-only and moves with the map when
// flush parks pending as flushing. Cost per indexed key: one string header in
// keys, one int32 in next, plus one heads slot per distinct bucket.
type writeBuf struct {
	ops   map[string]writeOp
	heads map[writeBucket]int32 // bucket -> newest key's index+1 in keys
	keys  []string              // indexed keys, insertion order, each once
	next  []int32               // next[i]: previous key's index+1 in keys[i]'s bucket (0 ends)
}

// bucketOf is the bucket of a key or prefix of at least writeBucketLen bytes.
func bucketOf(s string) writeBucket {
	var bk writeBucket
	copy(bk[:], s)
	return bk
}

func newWriteBuf() *writeBuf {
	return &writeBuf{ops: make(map[string]writeOp), heads: make(map[writeBucket]int32)}
}

// put stores op under k (last write wins) and indexes k on first sight.
func (b *writeBuf) put(k string, op writeOp) {
	n := len(b.ops)
	b.ops[k] = op
	if len(b.ops) == n || len(k) < writeBucketLen {
		return // overwrite (already indexed) or too short for any indexed prefix
	}
	bk := bucketOf(k)
	b.next = append(b.next, b.heads[bk])
	b.keys = append(b.keys, k)
	b.heads[bk] = int32(len(b.keys)) // #nosec G115 -- bounded by the op count, far below MaxInt32
}

// size is the number of buffered keys; a nil buffer is empty.
func (b *writeBuf) size() int {
	if b == nil {
		return 0
	}
	return len(b.ops)
}

// rangePrefix calls fn for every buffered key with prefix (len(prefix) >=
// writeBucketLen) and returns how many keys it visited.
func (b *writeBuf) rangePrefix(prefix string, fn func(k string, op writeOp)) int {
	if b == nil {
		return 0
	}
	visited := 0
	for i := b.heads[bucketOf(prefix)]; i != 0; i = b.next[i-1] {
		visited++
		if k := b.keys[i-1]; strings.HasPrefix(k, prefix) {
			fn(k, b.ops[k])
		}
	}
	return visited
}
