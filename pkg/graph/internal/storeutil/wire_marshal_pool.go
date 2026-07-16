package storeutil

import (
	"bytes"
	"sync"

	"github.com/vmihailenco/msgpack/v5"
)

// wireEncBufPool pools the bytes.Buffer used to encode entity-row wires. On the
// ingest hot path a fresh msgpack.Marshal grows its buffer through several
// reallocations per node; reusing a pre-grown buffer removes those growth
// allocations. The buffers are pre-grown to a size that covers a typical small
// node/rel row in one shot.
var wireEncBufPool = sync.Pool{
	New: func() any {
		b := new(bytes.Buffer)
		b.Grow(256)
		return b
	},
}

// marshalWirePooled msgpack-encodes v using a pooled encoder + a pooled,
// pre-grown buffer and returns an EXACT-SIZE, independent copy of the bytes.
//
// The result is BYTE-IDENTICAL to msgpack.Marshal(v): both draw an encoder from
// the same msgpack pool (identical default options) and encode the same value —
// the pooled/pre-grown buffer only changes where the bytes are allocated, never
// their content. This equivalence is what lets the entity-row marshal paths
// (persisted rows, the §4.5 pre-encode, and the change-log put body) share this
// helper without any risk to the content hash, replica byte-exactness, or the
// v1/v2 wire format. The out slice is copied before the buffer returns to the
// pool, so it never aliases pooled memory.
func marshalWirePooled(v interface{}) ([]byte, error) {
	buf := wireEncBufPool.Get().(*bytes.Buffer)
	buf.Reset()
	enc := msgpack.GetEncoder()
	enc.Reset(buf)
	err := enc.Encode(v)
	msgpack.PutEncoder(enc)
	if err != nil {
		wireEncBufPool.Put(buf)
		return nil, err
	}
	out := make([]byte, buf.Len())
	copy(out, buf.Bytes())
	wireEncBufPool.Put(buf)
	return out, nil
}
