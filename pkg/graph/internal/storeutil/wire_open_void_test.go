package storeutil

import (
	"errors"
	"math"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestOpenVoidRow_FormatVersion7 (OPENEXCISE): an open-ended void piece is
// written at OpenVoidWireFormatVersion on both valid axes and reads back; a
// reader capped below it refuses the row with ErrWireFormatVersionUnsupported;
// a bounded piece, the literal axis's [vf, 0) with vf < 0 included, keeps
// VoidWireFormatVersion (an excise of a bounded range takes no new one-way
// door); and an open void piece below 7 stays malformed. Catches an encoder
// that writes 5 for an open piece (older binaries reject it as malformed, not
// as newer), one that writes 7 for every void piece, and a decoder that
// accepts an open void piece of an older version.
func TestOpenVoidRow_FormatVersion7(t *testing.T) {
	piece := func(vf, vt types.Instant) []byte {
		n := types.NewNode(types.NodeID(4242), 3, nil)
		n.SetVersion(2)
		n.SetTemporal(&types.TemporalMetadata{ValidFrom: vf, ValidTo: vt, Void: true, TxFrom: 1000, UpdatedAt: 1000})
		return must(t)(MarshalNodeWire(n))
	}
	for _, c := range []struct {
		name   string
		vf, vt types.Instant
		want   byte
	}{
		{"legacy open [500, 0)", 500, 0, OpenVoidWireFormatVersion},
		{"literal open [-100, max)", -100, math.MaxInt64, OpenVoidWireFormatVersion},
		{"bounded [500, 600)", 500, 600, VoidWireFormatVersion},
		{"literal bounded [-100, 0)", -100, 0, VoidWireFormatVersion},
	} {
		raw := piece(c.vf, c.vt)
		if _, fv := fvOf(t, raw); fv != c.want {
			t.Fatalf("%s: fv %d, want %d", c.name, fv, c.want)
		}
		if _, _, err := DecodeWireTemporalMeta(raw); err != nil {
			t.Fatalf("%s: decode: %v", c.name, err)
		}
		if c.want == OpenVoidWireFormatVersion {
			restore := SetMaxReadableWireFormatVersionForTest(KeptValidToWireFormatVersion)
			_, _, err := DecodeWireTemporalMeta(raw)
			restore()
			if !errors.Is(err, storecontract.ErrWireFormatVersionUnsupported) {
				t.Fatalf("%s: reader capped at %d: %v, want ErrWireFormatVersionUnsupported", c.name, KeptValidToWireFormatVersion, err)
			}
		}
	}
	// The same open piece with its fv byte set to 5: refused as malformed.
	raw := piece(500, 0)
	at, _ := fvOf(t, raw)
	raw[at] = VoidWireFormatVersion
	if err := ValidateNodeWire(mustDecodeNodeWire(t, raw)); err == nil {
		t.Fatal("an open void piece at fv 5 was accepted")
	}
	if err := ValidateNodeWire(mustDecodeNodeWire(t, piece(500, 0))); err != nil {
		t.Fatalf("the open void piece at fv 7: %v", err)
	}
	if !IsOpenVoidEnd(500, 0) || !IsOpenVoidEnd(-5, math.MaxInt64) || IsOpenVoidEnd(-100, 0) || IsOpenVoidEnd(500, 600) {
		t.Fatal("IsOpenVoidEnd classifies a piece wrongly")
	}
}
