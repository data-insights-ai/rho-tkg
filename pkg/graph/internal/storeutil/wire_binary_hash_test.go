package storeutil

import (
	"bytes"
	"errors"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/vmihailenco/msgpack/v5"
)

// Backlog 61: the binary-hash writer writes integrity hashes as 32 raw bytes
// (fv 4); the hex writer (every older release, and step 1 of the rollout)
// writes 64-hex strings. Every reader decodes both to the same hashes.
func TestWireBinaryHashRoundTripAndLegacyHex(t *testing.T) {
	const (
		h1 = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
		h2 = "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210"
	)
	want := RelWire{FormatVersion: CurrentWireFormatVersion, ID: 5, RelType: 2, StartID: 3, EndID: 4, Version: 1,
		Hash: h1, PrevHash: h2, FromNodeHash: h2, ToNodeHash: h1}
	encode := func(binaryWriter bool, w RelWire) []byte {
		t.Helper()
		defer SetBinaryHashWriterForTest(binaryWriter)()
		b, err := marshalWirePooled(w)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	decode := func(b []byte) RelWire {
		t.Helper()
		var got RelWire
		if err := msgpack.Unmarshal(b, &got); err != nil {
			t.Fatal(err)
		}
		return got
	}

	bin := encode(true, want)
	if bytes.Contains(bin, []byte(h1)) || bytes.Contains(bin, []byte(h2)) {
		t.Fatal("binary writer wrote canonical hashes as hex")
	}
	if got := decode(bin); got.FormatVersion != BinaryHashWireFormatVersion || got.Hash != h1 || got.PrevHash != h2 || got.FromNodeHash != h2 || got.ToNodeHash != h1 {
		t.Fatalf("binary round trip = %+v", got)
	}

	hexRow := encode(false, want)
	if !bytes.Contains(hexRow, []byte(h1)) {
		t.Fatal("hex writer did not write hex")
	}
	if got := decode(hexRow); got.FormatVersion != CurrentWireFormatVersion || got.Hash != h1 || got.PrevHash != h2 || got.FromNodeHash != h2 || got.ToNodeHash != h1 {
		t.Fatalf("hex row decodes to %+v", got)
	}
	if len(hexRow)-len(bin) != 4*32 {
		t.Fatalf("binary row is %d B, hex row %d B: want 32 B saved per hash", len(bin), len(hexRow))
	}

	// A non-canonical hash is kept verbatim (string), and the row keeps fv 2.
	odd := want
	odd.Hash, odd.PrevHash, odd.FromNodeHash, odd.ToNodeHash = "tampered", "", "", ""
	if got := decode(encode(true, odd)); got.Hash != "tampered" || got.FormatVersion != CurrentWireFormatVersion {
		t.Fatalf("non-canonical hash = %+v", got)
	}

	// A binary hash of the wrong length is corruption.
	i := bytes.Index(bin, []byte{0xc4, 0x20})
	bad := append(append(append([]byte{}, bin[:i]...), 0xc4, 0x1f), bin[i+3:]...)
	if err := msgpack.Unmarshal(bad, &RelWire{}); !errors.Is(err, storecontract.ErrCorruptWire) {
		t.Fatalf("31-byte binary hash: err = %v, want ErrCorruptWire", err)
	}

	// A reader that predates fv 4 refuses the binary row instead of reading
	// 32 raw bytes as a hash, and still reads the hex row.
	prev := maxReadableWireFormatVersion
	maxReadableWireFormatVersion = RetractedWireFormatVersion
	defer func() { maxReadableWireFormatVersion = prev }()
	if err := ValidateRelWire(decode(hexRow)); err != nil {
		t.Fatalf("older reader, hex row: %v", err)
	}
	if err := ValidateRelWire(decode(bin)); !errors.Is(err, storecontract.ErrWireFormatVersionUnsupported) {
		t.Fatalf("older reader, binary row: err = %v, want ErrWireFormatVersionUnsupported", err)
	}
}

// The writer of this release: step 1 of the rollout writes hex; step 2 writes
// binary (and a writable open raises the marker to 4).
func TestBinaryHashWriterStep(t *testing.T) {
	w := RelWire{FormatVersion: CurrentWireFormatVersion, ID: 5, RelType: 2, StartID: 3, EndID: 4,
		Hash: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"}
	b, err := marshalWirePooled(w)
	if err != nil {
		t.Fatal(err)
	}
	if writesBinaryHashes == bytes.Contains(b, []byte(w.Hash)) {
		t.Fatalf("writesBinaryHashes = %v but the row holds the hex hash = %v", writesBinaryHashes, bytes.Contains(b, []byte(w.Hash)))
	}
	if WritesBinaryHashes() != writesBinaryHashes {
		t.Fatalf("WritesBinaryHashes() = %v, want %v", WritesBinaryHashes(), writesBinaryHashes)
	}
	// Every reader that opens a marker at or above BinaryHashStoreFormatVersion
	// (an Elements store, 5, may also hold binary rows) reads binary rows.
	if MaxReadableStoreFormatVersion < BinaryHashStoreFormatVersion || maxReadableWireFormatVersion < BinaryHashWireFormatVersion {
		t.Fatalf("reader opens marker %d / row fv %d, below the binary-hash format", MaxReadableStoreFormatVersion, maxReadableWireFormatVersion)
	}
}
