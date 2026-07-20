package storeutil

import (
	"bytes"
	"errors"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/vmihailenco/msgpack/v5"
)

// BACKLOG 15m: decodeMapKeyLen had zero direct tests — only indirect
// coverage through the entity wire decoders and fuzzing. Its over-long-key
// path (a fresh allocation instead of pooled scratch) is a genuinely rare
// cold path per its own doc comment ("longest known [key] is 3 bytes"), and
// the allocation is bounded (msgpack str8/str16 caps a single key at 65535
// bytes) and proportional to bytes the caller actually sent (not an
// amplification vector like lesson 48's cases — n bytes claimed always
// requires n real bytes on the wire for ReadFull to succeed), so pooling it
// was investigated and left as-is: adding a pool for a rare, bounded,
// non-amplifying cold path would add sync.Pool complexity (variable-size
// buffers up to 64 KiB are a poor sync.Pool fit — Go's own guidance is to
// avoid pooling widely-variable-sized objects) for negligible real benefit.
// These tests close the actual, valuable gap: direct coverage of the
// function's decode-format and cursor-alignment correctness.

func decodeMapKeyLenTestDecoder(data []byte) *msgpack.Decoder {
	return msgpack.NewDecoder(bytes.NewReader(data))
}

func TestDecodeMapKeyLen_Fixstr(t *testing.T) {
	t.Parallel()
	// fixstr(3) "abc" followed by a marker fixint(0x7f) to prove cursor alignment.
	data := []byte{0xa3, 'a', 'b', 'c', 0x7f}
	dec := decodeMapKeyLenTestDecoder(data)
	kb := make([]byte, wireKeyScratch)
	n, err := decodeMapKeyLen(dec, kb)
	if err != nil {
		t.Fatalf("decodeMapKeyLen: %v", err)
	}
	if n != 3 || string(kb[:n]) != "abc" {
		t.Fatalf("decodeMapKeyLen = (%d, %q), want (3, \"abc\")", n, kb[:n])
	}
	marker, err := dec.DecodeInt()
	if err != nil || marker != 0x7f {
		t.Fatalf("cursor misaligned after fixstr key: marker=%d err=%v", marker, err)
	}
}

func TestDecodeMapKeyLen_Str8(t *testing.T) {
	t.Parallel()
	// str8, length 16 (exactly wireKeyScratch), followed by a marker.
	key := bytes.Repeat([]byte{'k'}, 16)
	data := append([]byte{0xd9, 16}, key...)
	data = append(data, 0x7f)
	dec := decodeMapKeyLenTestDecoder(data)
	kb := make([]byte, wireKeyScratch)
	n, err := decodeMapKeyLen(dec, kb)
	if err != nil {
		t.Fatalf("decodeMapKeyLen: %v", err)
	}
	if n != 16 || string(kb[:n]) != string(key) {
		t.Fatalf("decodeMapKeyLen = (%d, %q), want (16, %q)", n, kb[:n], key)
	}
	marker, err := dec.DecodeInt()
	if err != nil || marker != 0x7f {
		t.Fatalf("cursor misaligned after str8 key: marker=%d err=%v", marker, err)
	}
}

func TestDecodeMapKeyLen_Str16OverLong(t *testing.T) {
	t.Parallel()
	// str16, length 17 (one over wireKeyScratch=16) — the over-long path.
	// Must report n=0 (no match) but still consume exactly the key's bytes
	// so the decoder cursor stays aligned for the next value.
	key := bytes.Repeat([]byte{'x'}, 17)
	data := []byte{0xda, 0x00, 17}
	data = append(data, key...)
	data = append(data, 0x7f)
	dec := decodeMapKeyLenTestDecoder(data)
	kb := make([]byte, wireKeyScratch)
	n, err := decodeMapKeyLen(dec, kb)
	if err != nil {
		t.Fatalf("decodeMapKeyLen: %v", err)
	}
	if n != 0 {
		t.Fatalf("decodeMapKeyLen over-long key n = %d, want 0 (no match)", n)
	}
	marker, err := dec.DecodeInt()
	if err != nil || marker != 0x7f {
		t.Fatalf("cursor misaligned after over-long key: marker=%d err=%v", marker, err)
	}
}

func TestDecodeMapKeyLen_UnexpectedKeyTypeRejected(t *testing.T) {
	t.Parallel()
	// A fixint (0x01) where a string key is expected.
	dec := decodeMapKeyLenTestDecoder([]byte{0x01})
	kb := make([]byte, wireKeyScratch)
	_, err := decodeMapKeyLen(dec, kb)
	if !errors.Is(err, storepkg.ErrCorruptWire) {
		t.Fatalf("decodeMapKeyLen(unexpected type) = %v, want errors.Is ErrCorruptWire", err)
	}
}

func TestDecodeMapKeyLen_TruncatedHeaderRejected(t *testing.T) {
	t.Parallel()
	// str8 tag with no length byte following.
	dec := decodeMapKeyLenTestDecoder([]byte{0xd9})
	kb := make([]byte, wireKeyScratch)
	if _, err := decodeMapKeyLen(dec, kb); err == nil {
		t.Fatal("decodeMapKeyLen(truncated str8 header) = nil error, want a read failure")
	}
}

func TestDecodeMapKeyLen_TruncatedBodyRejected(t *testing.T) {
	t.Parallel()
	// fixstr(3) claims 3 bytes but the stream only has 1.
	dec := decodeMapKeyLenTestDecoder([]byte{0xa3, 'a'})
	kb := make([]byte, wireKeyScratch)
	if _, err := decodeMapKeyLen(dec, kb); err == nil {
		t.Fatal("decodeMapKeyLen(truncated fixstr body) = nil error, want a read failure")
	}
}
