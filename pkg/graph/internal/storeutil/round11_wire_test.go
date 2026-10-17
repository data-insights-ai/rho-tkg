package storeutil

import (
	"errors"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	"github.com/vmihailenco/msgpack/v5"
)

// TestValidFromZeroWire (round 11 item 2): a row valid from the explicit 0
// round-trips its marker through the full decode, the selection skeleton and
// the scanner, is written at ValidFromZeroWireFormatVersion, and a reader of
// format 5 refuses it (it would read the row as untimed); a plain 0 keeps
// its bytes' format. A marker with a non-zero ValidFrom is refused.
func TestValidFromZeroWire(t *testing.T) {
	n := types.NewNode(types.NodeID(42), 1, nil)
	n.SetTemporal(&types.TemporalMetadata{ValidFrom: 0, ValidFromZero: true, TxFrom: 7})
	w := NodeToWire(n)
	raw, err := msgpack.Marshal(w)
	if err != nil {
		t.Fatal(err)
	}
	var back NodeWire
	if err := SafeUnmarshal(raw, &back); err != nil {
		t.Fatal(err)
	}
	if back.FormatVersion != ValidFromZeroWireFormatVersion || !back.ValidFromZero || back.ValidFrom != 0 {
		t.Fatalf("decoded wire: fv %d vz %v vf %d", back.FormatVersion, back.ValidFromZero, back.ValidFrom)
	}
	if err := ValidateNodeWire(back); err != nil {
		t.Fatalf("ValidateNodeWire: %v", err)
	}
	if _, tm, err := DecodeWireTemporalMeta(raw); err != nil || !tm.HasValidFrom() || !tm.ValidFromZero {
		t.Fatalf("skeleton: %+v %v", tm, err)
	}
	restore := SetMaxReadableWireFormatVersionForTest(VoidWireFormatVersion)
	if _, _, err := DecodeWireTemporalMeta(raw); !errors.Is(err, storepkg.ErrWireFormatVersionUnsupported) {
		t.Errorf("a reader of format 5 decoded the row: %v", err)
	}
	restore()

	plain := types.NewNode(types.NodeID(43), 1, nil)
	plain.SetTemporal(&types.TemporalMetadata{TxFrom: 7})
	pw := NodeToWire(plain)
	praw, err := msgpack.Marshal(pw)
	if err != nil {
		t.Fatal(err)
	}
	var pback NodeWire
	if err := SafeUnmarshal(praw, &pback); err != nil {
		t.Fatal(err)
	}
	if pback.FormatVersion >= ValidFromZeroWireFormatVersion || pback.ValidFromZero {
		t.Errorf("a plain row: fv %d vz %v", pback.FormatVersion, pback.ValidFromZero)
	}

	bad := w
	bad.ValidFrom = 5
	if err := ValidateNodeWire(bad); err == nil {
		t.Error("a valid-from-zero marker with ValidFrom 5 validated")
	}
	rbad := RelWire{FormatVersion: ValidFromZeroWireFormatVersion, ID: 9, RelType: 1, StartID: 1, EndID: 2, HasTemporal: true, ValidFrom: 3, ValidFromZero: true}
	if err := ValidateRelWire(rbad); err == nil {
		t.Error("a rel valid-from-zero marker with ValidFrom 3 validated")
	}
}
