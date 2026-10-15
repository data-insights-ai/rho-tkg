package storeutil

import (
	"errors"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Backlog 64: an excise's void piece (TemporalMetadata.Void, key "vd") is
// written at row format version 5 so a binary that predates it (it skips
// "vd" and would read the piece as a valid version) fails closed; every
// codec door carries the marker; the validators reject a void marker on a
// row that cannot be a piece.

func voidNodePiece(t *testing.T) *types.Node {
	t.Helper()
	n := types.NewNode(types.NodeID(snowflake.ID(5252)), 1, []uint16{2})
	n.SetVersion(4)
	n.SetProperties(mustPropertySlice(t, map[string]any{"s": "v"}))
	n.SetTemporal(&types.TemporalMetadata{ValidFrom: 200, ValidTo: 300, TxFrom: 500, UpdatedAt: 500, Void: true})
	n.SetIntegrity(&types.NodeIntegrity{Hash: "abc123", PrevHash: "def456"})
	return n
}

func voidRelPiece(t *testing.T) *types.Relationship {
	t.Helper()
	r := types.NewRelationship(types.RelID(snowflake.ID(5253)), 1, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(12)))
	r.SetVersion(3)
	r.SetProperties(mustPropertySlice(t, map[string]any{"s": "v"}))
	r.SetTemporal(&types.TemporalMetadata{ValidFrom: 200, ValidTo: 300, TxFrom: 500, UpdatedAt: 500, Void: true})
	r.SetIntegrity(&types.RelIntegrity{Hash: "abc123"})
	return r
}

// voidPath is one codec door: encode, decode back to the Void marker.
type voidPath struct {
	name string
	enc  func(t *testing.T, n *types.Node, r *types.Relationship) []byte
	dec  func(isNode bool, raw []byte) (bool, error)
}

func checkedVoid(isNode bool, nw NodeWire, rw RelWire) (bool, error) {
	if isNode {
		n, err := WireToNodeChecked(nw)
		if err != nil {
			return false, err
		}
		return n.Temporal().Void, nil
	}
	r, err := WireToRelChecked(rw)
	if err != nil {
		return false, err
	}
	return r.Temporal().Void, nil
}

func voidPaths() []voidPath {
	full := func(t *testing.T, n *types.Node, r *types.Relationship) []byte {
		if n != nil {
			return must(t)(MarshalNodeWire(n))
		}
		return must(t)(MarshalRelWire(r))
	}
	return []voidPath{
		{"full checked", full, func(isNode bool, raw []byte) (bool, error) {
			var nw NodeWire
			var rw RelWire
			if err := SafeUnmarshal(raw, map[bool]any{true: &nw, false: &rw}[isNode]); err != nil {
				return false, err
			}
			return checkedVoid(isNode, nw, rw)
		}},
		{"partial scanner", full, func(_ bool, raw []byte) (bool, error) {
			_, tm, err := DecodeWireTemporalMeta(raw)
			return err == nil && tm.Void, err
		}},
		{"partial slow", full, func(_ bool, raw []byte) (bool, error) {
			w, err := decodeWireTemporalMetaSlow(raw)
			if err == nil {
				err = checkRowFormatVersion("partial", w.FormatVersion)
			}
			return w.Void, err
		}},
		{"change feed put", func(t *testing.T, n *types.Node, r *types.Relationship) []byte {
			if n != nil {
				return must(t)(NodePutPayload(n, true))
			}
			return must(t)(RelPutPayload(r, true))
		}, func(isNode bool, raw []byte) (bool, error) {
			if isNode {
				b, err := DecodeNodePut(raw)
				if err != nil {
					return false, err
				}
				return checkedVoid(true, b.Wire, RelWire{})
			}
			b, err := DecodeRelPut(raw)
			if err != nil {
				return false, err
			}
			return checkedVoid(false, NodeWire{}, b.Wire)
		}},
		{"history delta", func(t *testing.T, n *types.Node, r *types.Relationship) []byte {
			if n != nil {
				tw, _ := NodeToWireChecked(n)
				an := tw
				an.Void, an.ValidTo = false, 0
				return must(t)(EncodeNodeHistoryDelta(DiffNodeHistory(an, tw)))
			}
			tw, _ := RelToWireChecked(r)
			ar := tw
			ar.Void, ar.ValidTo = false, 0
			return must(t)(EncodeRelHistoryDelta(DiffRelHistory(ar, tw)))
		}, func(isNode bool, raw []byte) (bool, error) {
			if isNode {
				d, err := DecodeNodeHistoryDelta(raw)
				if err != nil {
					return false, err
				}
				an := d.Meta
				an.Void = false
				return checkedVoid(true, ApplyNodeHistory(an, d), RelWire{})
			}
			d, err := DecodeRelHistoryDelta(raw)
			if err != nil {
				return false, err
			}
			ar := d.Meta
			ar.Void = false
			return checkedVoid(false, NodeWire{}, ApplyRelHistory(ar, d))
		}},
	}
}

// TestVoidRow_FormatVersion5 catches an encoder that drops "vd" or keeps fv 2
// (an older binary reads the piece as a valid version), a decoder or the
// selection-scope scanner that loses the marker (a reopened store answers
// inside an excised span), and a door that does not fail closed for a reader
// capped below fv 5. Not parallel: it moves the package's decode cap.
func TestVoidRow_FormatVersion5(t *testing.T) {
	type row struct {
		name string
		n    *types.Node
		r    *types.Relationship
	}
	rows := []row{{"node", voidNodePiece(t), nil}, {"rel", nil, voidRelPiece(t)}}
	for _, e := range rows {
		for _, p := range voidPaths() {
			raw := p.enc(t, e.n, e.r)
			if _, fv := fvOf(t, raw); fv != VoidWireFormatVersion {
				t.Fatalf("%s %s: fv %d; want %d", e.name, p.name, fv, VoidWireFormatVersion)
			}
			got, err := p.dec(e.n != nil, raw)
			if err != nil || !got {
				t.Fatalf("%s %s: decoded void=%v err=%v; want the marker", e.name, p.name, got, err)
			}
			restore := SetMaxReadableWireFormatVersionForTest(BinaryHashOrRetractedCap())
			_, err = p.dec(e.n != nil, raw)
			restore()
			if !errors.Is(err, storecontract.ErrWireFormatVersionUnsupported) {
				t.Fatalf("%s %s: older reader err = %v; want ErrWireFormatVersionUnsupported", e.name, p.name, err)
			}
		}
	}
}

// BinaryHashOrRetractedCap is the highest row format version a binary before
// backlog 64 reads (4 once backlog 61 lands, 3 before it): one below void.
func BinaryHashOrRetractedCap() uint8 { return VoidWireFormatVersion - 1 }

// TestVoidRow_ValidationRejectsImpossiblePieces: a void marker on an open,
// empty, deleted or retracted row is corruption (an excise writes none).
func TestVoidRow_ValidationRejectsImpossiblePieces(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		mut  func(tm *types.TemporalMetadata)
	}{
		{"open", func(tm *types.TemporalMetadata) { tm.ValidTo = 0 }},
		{"inverted", func(tm *types.TemporalMetadata) { tm.ValidFrom = tm.ValidTo }},
		{"deleted", func(tm *types.TemporalMetadata) { tm.DeletedAt, tm.TxTo = 600, 600 }},
		{"retracted", func(tm *types.TemporalMetadata) { tm.DeletedAt, tm.TxTo, tm.Retracted = 600, 600, true }},
	} {
		n := voidNodePiece(t)
		tc.mut(n.Temporal())
		nw, _ := NodeToWireChecked(n)
		if err := ValidateNodeWire(nw); err == nil {
			t.Fatalf("node %s: void row accepted", tc.name)
		}
		r := voidRelPiece(t)
		tc.mut(r.Temporal())
		rw, _ := RelToWireChecked(r)
		if err := ValidateRelWire(rw); err == nil {
			t.Fatalf("rel %s: void row accepted", tc.name)
		}
	}
	nw, _ := NodeToWireChecked(voidNodePiece(t))
	if err := ValidateNodeWire(nw); err != nil {
		t.Fatalf("valid void node piece rejected: %v", err)
	}
	rw, _ := RelToWireChecked(voidRelPiece(t))
	if err := ValidateRelWire(rw); err != nil {
		t.Fatalf("valid void rel piece rejected: %v", err)
	}
}

// TestSelectAsOf_SkipsVoidRows: the as-of rule never answers a void piece and
// reads the chain as if it were absent — the current arm (a piece above the
// current version) and the history arm (the newest row a piece).
func TestSelectAsOf_SkipsVoidRows(t *testing.T) {
	t.Parallel()
	row := func(v uint32, vf, vt, tf types.Instant, void bool) *types.Node {
		n := types.NewNode(types.NodeID(snowflake.ID(77)), 1, nil)
		n.SetVersion(v)
		n.SetTemporal(&types.TemporalMetadata{ValidFrom: vf, ValidTo: vt, TxFrom: tf, Void: void})
		return n
	}
	cur := row(0, 10, 50, 100, false)
	piece := row(1, 20, 30, 200, true)
	if got, ok := SelectAsOfWithCurrent([]*types.Node{piece}, cur, true, 300); !ok || got != cur {
		t.Fatalf("current arm with a void piece above: %v %v; want the current row", got, ok)
	}
	resumption := row(1, 30, 50, 200, false)
	piece2 := row(2, 20, 30, 200, true)
	if got, ok := SelectAsOfWithCurrent([]*types.Node{resumption, piece2}, cur, true, 300); !ok || got != resumption {
		t.Fatalf("current arm with resumption + piece: %v %v; want the resumption", got, ok)
	}
	if got, ok := SelectAsOf([]*types.Node{cur, piece}, 300); !ok || got != cur {
		t.Fatalf("history arm, newest row void: %v %v; want the row below", got, ok)
	}
	if _, ok := SelectAsOf([]*types.Node{piece}, 300); ok {
		t.Fatal("a chain of only a void piece answered")
	}
}
