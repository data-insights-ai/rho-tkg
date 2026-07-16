package storeutil

import (
	"fmt"
	"reflect"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	"github.com/vmihailenco/msgpack/v5"
)

// nodeWireReflect is a method-less defined type over NodeWire: it keeps the
// msgpack field tags but does NOT inherit NodeWire.DecodeMsgpack, so decoding
// into it forces the pure-reflection path. This lets the benchmark compare
// reflection vs the hand-written custom decoder through the IDENTICAL
// SafeUnmarshal path (same depth guard, same library decoder pooling), isolating
// the field-decode logic as the only variable.
type nodeWireReflect NodeWire

// buildDecodeBenchBytes builds a realistic persisted node row (full temporal
// metadata + 64-hex integrity hashes + `nprops` mixed-type properties, one of
// which is a ~256B string) and returns its marshaled wire bytes.
func buildDecodeBenchBytes(tb testing.TB, nprops int) []byte {
	tb.Helper()
	n := types.NewNode(types.NodeID(snowflake.ID(1<<20)), 1, []uint16{2, 3})
	big := make([]byte, 256)
	for i := range big {
		big[i] = byte('a' + i%26)
	}
	for i := 0; i < nprops; i++ {
		var v any
		switch i % 4 {
		case 0:
			v = fmt.Sprintf("str_value_%d", i)
		case 1:
			v = int64(i * 1000)
		case 2:
			v = float64(i) + 0.5
		case 3:
			v = i%2 == 0
		}
		if i == nprops-1 && nprops > 0 {
			v = string(big)
		}
		if err := n.SetProperty(fmt.Sprintf("key_%d", i), v); err != nil {
			tb.Fatal(err)
		}
	}
	n.SetVersion(7)
	n.SetTemporal(&types.TemporalMetadata{
		ValidFrom: 1_752_000_000_000, ValidTo: 1_800_000_000_000,
		TxFrom: 1_752_000_000_500, TxTo: 1_760_000_000_000,
		CreatedAt: 1_752_000_000_000, UpdatedAt: 1_752_000_000_500,
	})
	h := "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
	n.SetIntegrity(&types.NodeIntegrity{Hash: h, PrevHash: h})
	b, err := marshalWirePooled(NodeToWire(n))
	if err != nil {
		tb.Fatal(err)
	}
	return b
}

// TestHandDecodeMatchesReflection is the correctness gate: the hand-written
// custom decoder must produce a struct equal to the reflection decode.
func TestHandDecodeMatchesReflection(t *testing.T) {
	for _, p := range []int{0, 2, 5, 20} {
		data := buildDecodeBenchBytes(t, p)
		var hand NodeWire
		var refl nodeWireReflect
		if err := SafeUnmarshal(data, &hand); err != nil {
			t.Fatalf("p=%d hand: %v", p, err)
		}
		if err := SafeUnmarshal(data, &refl); err != nil {
			t.Fatalf("p=%d reflection: %v", p, err)
		}
		if !reflect.DeepEqual(NodeWire(refl), hand) {
			t.Fatalf("p=%d mismatch:\n refl=%+v\n hand=%+v", p, NodeWire(refl), hand)
		}
	}
}

func BenchmarkDecode(b *testing.B) {
	for _, p := range []int{2, 5, 20} {
		data := buildDecodeBenchBytes(b, p)
		b.Run(fmt.Sprintf("P%d/reflection", p), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var w nodeWireReflect
				if err := SafeUnmarshal(data, &w); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(fmt.Sprintf("P%d/hand", p), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var w NodeWire
				if err := SafeUnmarshal(data, &w); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkEncodeForRatio gives the encode cost of the same row to compare
// against decode (encode reuses a pooled pre-grown buffer; decode materializes
// the object graph).
func BenchmarkEncodeForRatio(b *testing.B) {
	for _, p := range []int{2, 5, 20} {
		b.Run(fmt.Sprintf("P%d", p), func(b *testing.B) {
			n := types.NewNode(types.NodeID(snowflake.ID(1<<20)), 1, []uint16{2, 3})
			for i := 0; i < p; i++ {
				_ = n.SetProperty(fmt.Sprintf("key_%d", i), fmt.Sprintf("str_value_%d", i))
			}
			n.SetVersion(7)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := marshalWirePooled(NodeToWire(n)); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

var _ = msgpack.Unmarshal
