package storeutil

import (
	"bytes"
	"fmt"
	"sync"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

// TestMarshalWirePooledByteIdentical is the load-bearing invariant: the pooled
// encoder must produce EXACTLY the same bytes as msgpack.Marshal for every wire
// shape, or the content hash / replica byte-exactness / v1-v2 wire format would
// silently diverge. Covers empty, property-bearing, temporal-tail (v1 and v2),
// and integrity-bearing rows for both Node and Rel wires.
func TestMarshalWirePooledByteIdentical(t *testing.T) {
	nodeWires := []NodeWire{
		{ID: 1, PrimaryLabel: 2, Version: 0},
		{ID: 3, PrimaryLabel: 2, Version: 1, ExtraLabels: []int{5, 6}},
		{ID: 7, PrimaryLabel: 2, FormatVersion: 2, TxFrom: 111, TxTo: 0},
		{ID: 8, PrimaryLabel: 2, FormatVersion: 1, TxFrom: 111},
		{ID: 9, PrimaryLabel: 2, HasTemporal: true, ValidFrom: 10, ValidTo: 20, CreatedAt: 5},
		{ID: 10, PrimaryLabel: 2, Hash: "abc", PrevHash: "def", CreatedBy: "u", Version: 4},
		{ID: 11, PrimaryLabel: 2, Properties: []PropertyWire{{Key: "k", Value: int64(1)}, {Key: "s", Value: "v"}}},
	}
	for i, w := range nodeWires {
		want, err := msgpack.Marshal(w)
		if err != nil {
			t.Fatalf("node[%d] Marshal: %v", i, err)
		}
		got, err := marshalWirePooled(w)
		if err != nil {
			t.Fatalf("node[%d] pooled: %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("node[%d] pooled bytes differ from Marshal:\n got=%x\nwant=%x", i, got, want)
		}
	}

	relWires := []RelWire{
		{ID: 1, RelType: 2, StartID: 3, EndID: 4, Version: 0},
		{ID: 5, RelType: 2, StartID: 3, EndID: 4, FormatVersion: 2, TxFrom: 9, TxTo: 0},
		{ID: 6, RelType: 2, StartID: 3, EndID: 4, Properties: []PropertyWire{{Key: "w", Value: int64(2026)}}},
		{ID: 7, RelType: 2, StartID: 3, EndID: 4, Hash: "h", FromNodeHash: "f", ToNodeHash: "t"},
	}
	for i, w := range relWires {
		want, err := msgpack.Marshal(w)
		if err != nil {
			t.Fatalf("rel[%d] Marshal: %v", i, err)
		}
		got, err := marshalWirePooled(w)
		if err != nil {
			t.Fatalf("rel[%d] pooled: %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("rel[%d] pooled bytes differ from Marshal:\n got=%x\nwant=%x", i, got, want)
		}
	}

	// Reuse: the same pooled buffer serves many encodes; a prior larger encode
	// must not leak trailing bytes into a later smaller one.
	big, _ := marshalWirePooled(NodeWire{ID: 99, PrimaryLabel: 1, Properties: []PropertyWire{{Key: "long-key-name", Value: "a fairly long string value to grow the buffer"}}})
	small, _ := marshalWirePooled(NodeWire{ID: 1, PrimaryLabel: 1})
	wantSmall, _ := msgpack.Marshal(NodeWire{ID: 1, PrimaryLabel: 1})
	if !bytes.Equal(small, wantSmall) {
		t.Fatalf("pooled reuse leaked bytes: got=%x want=%x", small, wantSmall)
	}
	_ = big
}

// TestMarshalWirePooledConcurrentSafe is the BACKLOG 15r proof: wireEncBufPool
// is shared across goroutines on the hot ingest write path (sync.Pool's
// concurrent Get/Put is safe by design, but that alone doesn't prove
// marshalWirePooled's OWN discipline — copying out of the buffer before
// returning it to the pool — actually prevents cross-goroutine aliasing).
// Many goroutines concurrently encode DISTINCT, per-goroutine-identifiable
// values in a tight loop and verify every result byte-for-byte against a
// fresh msgpack.Marshal of the SAME value — if a returned []byte ever
// aliased a pooled buffer another goroutine mutated afterward, the observed
// bytes would belong to the wrong node/rel and this comparison would catch
// it. Run under `go test -race` (see the package's `-race` gate) to also
// catch any unsynchronized access to the shared buffer.
func TestMarshalWirePooledConcurrentSafe(t *testing.T) {
	const goroutines = 32
	const iterations = 200

	var wg sync.WaitGroup
	errCh := make(chan error, goroutines)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				// A distinctive, per-call value: any cross-goroutine
				// aliasing would surface as a byte mismatch here.
				w := NodeWire{
					ID:           int64(id)*1_000_000 + int64(i),
					PrimaryLabel: id + 1,
					Version:      i,
					Properties: []PropertyWire{
						{Key: "goroutine", Value: int64(id)},
						{Key: "iter", Value: int64(i)},
					},
				}
				want, err := msgpack.Marshal(w)
				if err != nil {
					errCh <- fmt.Errorf("goroutine %d iter %d: msgpack.Marshal: %w", id, i, err)
					return
				}
				got, err := marshalWirePooled(w)
				if err != nil {
					errCh <- fmt.Errorf("goroutine %d iter %d: marshalWirePooled: %w", id, i, err)
					return
				}
				if !bytes.Equal(got, want) {
					errCh <- fmt.Errorf("goroutine %d iter %d: pooled bytes differ (possible cross-goroutine pool aliasing):\n got=%x\nwant=%x", id, i, got, want)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
}
