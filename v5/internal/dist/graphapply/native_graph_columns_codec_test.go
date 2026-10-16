package graphapply

import (
	"bytes"
	"errors"
	"testing"
)

func TestNativeColumnChunkAdmissionAndOriginalArena(t *testing.T) {
	p := nativeColumnGuardPolicy()
	g := nativeColumnGeneration{ID: [16]byte{1}, Binding: [32]byte{2}}
	frame := nativeColumnFrame{Partition: 3, Index: 9, Root: [172]byte{1}, Outcome: []byte{}, CDC: []byte{}, Rows: []nativeColumnRow{}}
	wire, err := encodeNativeColumnChunk(g, 0, [32]byte{}, []nativeColumnFrame{frame}, p)
	if err != nil {
		t.Fatal(err)
	}
	frames, err := decodeNativeColumnChunk(g, 0, [32]byte{}, wire, p)
	if err != nil || len(frames) != 1 || frames[0].Outcome == nil || frames[0].CDC == nil || len(frames[0].Rows) != 0 {
		t.Fatal(frames, err)
	}
	clear(wire)
	if frames[0].Root[0] != 1 {
		t.Fatal("borrowed root")
	}
	wire, _ = encodeNativeColumnChunk(g, 0, [32]byte{}, []nativeColumnFrame{frame}, p)
	for _, bad := range [][]byte{nil, wire[:len(wire)-1], append(bytes.Clone(wire), 0)} {
		if out, err := decodeNativeColumnChunk(g, 0, [32]byte{}, bad, p); out != nil || !errors.Is(err, errCorrupt) {
			t.Fatal(out, err)
		}
	}
	foreign := g
	foreign.Binding[0]++
	if _, err := decodeNativeColumnChunk(foreign, 0, [32]byte{}, wire, p); !errors.Is(err, errCorrupt) {
		t.Fatal(err)
	}
	if _, err := encodeNativeColumnChunk(g, 0, [32]byte{}, []nativeColumnFrame{{Outcome: nil, CDC: []byte{}}}, p); !errors.Is(err, errInvalid) {
		t.Fatal("missing original became empty", err)
	}
	tiny := p
	tiny.ChunkBytes = 1024
	frame.Outcome = make([]byte, 1024)
	if _, err := encodeNativeColumnChunk(g, 0, [32]byte{}, []nativeColumnFrame{frame}, tiny); !errors.Is(err, errLimit) {
		t.Fatal(err)
	}
	if nativeColumnAdd(^uint64(0), 1) {
		t.Fatal("overflow")
	}
}
