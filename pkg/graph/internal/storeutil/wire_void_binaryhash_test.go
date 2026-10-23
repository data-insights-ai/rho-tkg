package storeutil

import (
	"errors"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestReadableCaps: this binary reads every row format a feature writes (fv
// 6) and every store marker (9). Catches a merge that keeps one feature's cap
// and refuses the other feature's rows or stores.
func TestReadableCaps(t *testing.T) {
	if maxReadableWireFormatVersion != KeptValidToWireFormatVersion || KeptValidToWireFormatVersion != 6 ||
		VoidWireFormatVersion != 5 || BinaryHashWireFormatVersion != 4 || RetractedWireFormatVersion != 3 {
		t.Fatalf("row cap %d (kept valid-to %d, void %d, binary hash %d, retracted %d); want 6 over 6/5/4/3",
			maxReadableWireFormatVersion, KeptValidToWireFormatVersion, VoidWireFormatVersion, BinaryHashWireFormatVersion, RetractedWireFormatVersion)
	}
	if MaxReadableStoreFormatVersion != StagedFlushStoreFormatVersion || StagedFlushStoreFormatVersion != 9 ||
		KeptValidToStoreFormatVersion != 8 || UntimedUnboundedStoreFormatVersion != 7 || VoidStoreFormatVersion != 6 ||
		ElementsStoreFormatVersion != 5 || BinaryHashStoreFormatVersion != 4 {
		t.Fatalf("store cap %d; want 9 over staged flush 9, kept valid-to 8, untimed unbounded 7, void 6, elements 5, binary hash 4", MaxReadableStoreFormatVersion)
	}
}

// TestVoidRowWithBinaryHash_FormatVersionIsMax: a void piece that also carries
// binary integrity hashes is written at the highest version a feature needs
// (void 5 > binary hash 4 > retracted 3) and a reader capped at 4 refuses it.
// Catches an encoder that returns the first feature's version it checks
// (binary hash 4: a release that reads binary hashes but not void rows would
// read the piece as a valid version), node and relationship.
// Not parallel: it moves the writer and the decode cap.
func TestVoidRowWithBinaryHash_FormatVersionIsMax(t *testing.T) {
	defer SetBinaryHashWriterForTest(true)()
	const canon = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	n := voidNodePiece(t)
	n.SetIntegrity(&types.NodeIntegrity{Hash: canon, PrevHash: canon})
	r := voidRelPiece(t)
	r.SetIntegrity(&types.RelIntegrity{Hash: canon, FromNodeHash: canon, ToNodeHash: canon})
	for _, e := range []struct {
		name string
		raw  []byte
	}{{"node", must(t)(MarshalNodeWire(n))}, {"rel", must(t)(MarshalRelWire(r))}} {
		if _, fv := fvOf(t, e.raw); fv != VoidWireFormatVersion {
			t.Fatalf("%s: fv %d; want %d (void outranks binary hash %d)", e.name, fv, VoidWireFormatVersion, BinaryHashWireFormatVersion)
		}
		restore := SetMaxReadableWireFormatVersionForTest(BinaryHashWireFormatVersion)
		_, _, err := DecodeWireTemporalMeta(e.raw)
		restore()
		if !errors.Is(err, storecontract.ErrWireFormatVersionUnsupported) {
			t.Fatalf("%s: reader capped at %d: %v; want ErrWireFormatVersionUnsupported", e.name, BinaryHashWireFormatVersion, err)
		}
	}
	for _, c := range []struct {
		retracted, bin, void, kept bool
		want                       uint8
	}{
		{false, false, false, false, 2}, {true, false, false, false, 3}, {false, true, false, false, 4}, {true, true, false, false, 4},
		{false, false, true, false, 5}, {false, true, true, false, 5}, {true, true, true, false, 5},
		{false, false, false, true, 6}, {true, true, false, true, 6},
	} {
		if got := emittedRowFormatVersion(CurrentWireFormatVersion, c.retracted, c.bin, c.void, c.kept); got != c.want {
			t.Fatalf("emittedRowFormatVersion(2, rx=%v, bin=%v, vd=%v, dk=%v) = %d; want %d", c.retracted, c.bin, c.void, c.kept, got, c.want)
		}
	}
}
