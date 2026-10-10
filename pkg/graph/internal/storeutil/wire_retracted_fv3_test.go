package storeutil

import (
	"bytes"
	"encoding/hex"
	"errors"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Backlog 43 follow-up (tasks/evidence/retraction-fv3/): a retraction
// tombstone is written at row format version 3 so a binary that predates the
// marker (it reads fv <= 2 and skips the unknown key "rx") fails closed with
// ErrWireFormatVersionUnsupported instead of answering the row as a plain
// Delete. Every other row keeps fv=2 byte for byte, and a v4.49.1 row (fv=2 +
// rx) still decodes as a retraction.

// fv3Row is one entity under test: a node or a relationship.
type fv3Row struct {
	name      string
	n         *types.Node
	r         *types.Relationship
	retracted bool
	golden    string // v4.49.1 full-wire bytes of a plain row (hex)
}

func fv3Rows(t *testing.T) []fv3Row {
	t.Helper()
	plainN := retractedNodeTombstone(t)
	plainN.Temporal().Retracted = false
	liveN := retractedNodeTombstone(t)
	liveN.Temporal().Retracted = false
	liveN.Temporal().DeletedAt, liveN.Temporal().TxTo, liveN.Temporal().ValidTo = 0, 0, 0
	plainR := retractedRelTombstone(t)
	plainR.Temporal().Retracted = false
	return []fv3Row{
		{name: "node retracted", n: retractedNodeTombstone(t), retracted: true},
		{name: "rel retracted", r: retractedRelTombstone(t), retracted: true},
		{name: "node plain delete", n: plainN, golden: goldenPlainNodeDelete},
		{name: "node live", n: liveN, golden: goldenLiveNode},
		{name: "rel plain delete", r: plainR, golden: goldenPlainRelDelete},
	}
}

// anchorWire is the row's previous version (live, no marker): the delta anchor.
func (e fv3Row) anchorWire() (NodeWire, RelWire) {
	if e.n != nil {
		nw, _ := NodeToWireChecked(e.n)
		nw.Retracted, nw.DeletedAt, nw.TxTo, nw.ValidTo = false, 0, 0, 0
		return nw, RelWire{}
	}
	rw, _ := RelToWireChecked(e.r)
	rw.Retracted, rw.DeletedAt, rw.TxTo, rw.ValidTo = false, 0, 0, 0
	return NodeWire{}, rw
}

func must(t *testing.T) func([]byte, error) []byte {
	return func(buf []byte, err error) []byte {
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		return buf
	}
}

func fullWire(t *testing.T, e fv3Row) []byte {
	if e.n != nil {
		return must(t)(MarshalNodeWire(e.n))
	}
	return must(t)(MarshalRelWire(e.r))
}

func deltaWire(t *testing.T, e fv3Row) []byte {
	an, ar := e.anchorWire()
	if e.n != nil {
		tw, _ := NodeToWireChecked(e.n)
		return must(t)(EncodeNodeHistoryDelta(DiffNodeHistory(an, tw)))
	}
	tw, _ := RelToWireChecked(e.r)
	return must(t)(EncodeRelHistoryDelta(DiffRelHistory(ar, tw)))
}

func checkedRx(isNode bool, nw NodeWire, rw RelWire) (bool, error) {
	if isNode {
		n, err := WireToNodeChecked(nw)
		if err != nil {
			return false, err
		}
		return n.Temporal().Retracted, nil
	}
	r, err := WireToRelChecked(rw)
	if err != nil {
		return false, err
	}
	return r.Temporal().Retracted, nil
}

// fv3Path is one codec door: encode the row, decode the bytes back to the marker.
type fv3Path struct {
	name   string
	seesRx bool // the door reports the marker (a stamps-only door does not)
	enc    func(t *testing.T, e fv3Row) []byte
	dec    func(e fv3Row, raw []byte) (retracted bool, err error)
}

func fv3Paths() []fv3Path {
	return []fv3Path{
		{"full checked", true, fullWire, func(e fv3Row, raw []byte) (bool, error) {
			var nw NodeWire
			var rw RelWire
			if err := SafeUnmarshal(raw, map[bool]any{true: &nw, false: &rw}[e.n != nil]); err != nil {
				return false, err
			}
			return checkedRx(e.n != nil, nw, rw)
		}},
		{"partial scanner", true, fullWire, func(_ fv3Row, raw []byte) (bool, error) {
			_, tm, err := DecodeWireTemporalMeta(raw)
			return err == nil && tm.Retracted, err
		}},
		{"partial slow", true, fullWire, func(_ fv3Row, raw []byte) (bool, error) {
			w, err := decodeWireTemporalMetaSlow(raw)
			if err == nil {
				err = checkRowFormatVersion("partial", w.FormatVersion)
			}
			return w.Retracted, err
		}},
		{"tx stamps", false, fullWire, func(_ fv3Row, raw []byte) (bool, error) {
			_, _, _, err := DecodeWireTxStamps(raw)
			return false, err
		}},
		{"change feed put", true, func(t *testing.T, e fv3Row) []byte {
			if e.n != nil {
				return must(t)(NodePutPayload(e.n, true))
			}
			return must(t)(RelPutPayload(e.r, true))
		}, func(e fv3Row, raw []byte) (bool, error) {
			if e.n != nil {
				b, err := DecodeNodePut(raw)
				if err != nil {
					return false, err
				}
				return checkedRx(true, b.Wire, RelWire{})
			}
			b, err := DecodeRelPut(raw)
			if err != nil {
				return false, err
			}
			return checkedRx(false, NodeWire{}, b.Wire)
		}},
		{"history delta", true, deltaWire, func(e fv3Row, raw []byte) (bool, error) {
			an, ar := e.anchorWire()
			if e.n != nil {
				d, err := DecodeNodeHistoryDelta(raw)
				if err != nil {
					return false, err
				}
				return checkedRx(true, ApplyNodeHistory(an, d), RelWire{})
			}
			d, err := DecodeRelHistoryDelta(raw)
			if err != nil {
				return false, err
			}
			return checkedRx(false, NodeWire{}, ApplyRelHistory(ar, d))
		}},
		{"history delta stamps", false, deltaWire, func(e fv3Row, raw []byte) (bool, error) {
			_, _, _, err := HistoryValueTxStamps(raw, e.n != nil)
			return false, err
		}},
	}
}

// fvOf returns the offset and value of the first row format version byte in
// raw (the "fv" key's uint8 value, 0xcc NN; for a nested body, the first wire's).
func fvOf(t *testing.T, raw []byte) (int, byte) {
	t.Helper()
	i := bytes.Index(raw, []byte{0xa2, 'f', 'v'})
	if i < 0 || i+4 >= len(raw) || raw[i+3] != 0xcc {
		t.Fatalf("no fv key in %x", raw)
	}
	return i + 4, raw[i+4]
}

// TestRetractedRow_FormatVersion3 catches: fv always 2 (an old binary reads a
// retraction as a plain Delete), fv 3 on every row (an old binary refuses
// stores that hold no retraction), a decoder that rejects fv 3 (this binary
// refuses its own retractions), a decoder that drops v4.49.1's fv=2 + rx, and
// a door that does not fail closed for a reader capped at fv <= 2.
// Not parallel: it moves the package's decode cap (restored before return).
func TestRetractedRow_FormatVersion3(t *testing.T) {
	for _, e := range fv3Rows(t) {
		for _, p := range fv3Paths() {
			raw := p.enc(t, e)
			off, fv := fvOf(t, raw)
			want := byte(CurrentWireFormatVersion)
			if e.retracted {
				want = RetractedWireFormatVersion
			}
			if fv != want {
				t.Errorf("%s / %s: fv=%d, want %d", e.name, p.name, fv, want)
			}
			if p.name == "full checked" && !e.retracted && hex.EncodeToString(raw) != e.golden {
				t.Errorf("%s: plain row bytes differ from v4.49.1:\n got %x\nwant %s", e.name, raw, e.golden)
			}
			if rx, err := p.dec(e, raw); err != nil || (p.seesRx && rx != e.retracted) {
				t.Errorf("%s / %s: decode rx=%v err=%v, want rx=%v", e.name, p.name, rx, err, e.retracted)
			}
			if e.retracted { // the same row as v4.49.1 wrote it: fv=2 + rx
				old := bytes.Clone(raw)
				old[off] = CurrentWireFormatVersion
				if rx, err := p.dec(e, old); err != nil || (p.seesRx && !rx) {
					t.Errorf("%s / %s: v4.49.1 row (fv=2 + rx) rx=%v err=%v, want rx=true", e.name, p.name, rx, err)
				}
			}
			maxReadableWireFormatVersion = CurrentWireFormatVersion // a v4.49.0 reader
			rx, err := p.dec(e, raw)
			maxReadableWireFormatVersion = RetractedWireFormatVersion
			if e.retracted && !errors.Is(err, storecontract.ErrWireFormatVersionUnsupported) {
				t.Errorf("%s / %s: fv<=2 reader got rx=%v err=%v, want ErrWireFormatVersionUnsupported", e.name, p.name, rx, err)
			}
			if !e.retracted && err != nil {
				t.Errorf("%s / %s: fv<=2 reader rejected a plain row: %v", e.name, p.name, err)
			}
		}
	}
}

// v4.49.1 encodings (generated at 7d6e55c) of the plain rows above.
const (
	goldenPlainNodeDelete = "8fa26676cc02a26964d30000000000001092a2706c01a2656c9102a1709283a16ba16ea176d30000000000000007a174cc0683a16ba46e616d65a176ac77726f6e67207265636f7264a174cc0ea17603a26874c3a27666d30000000000000064a27674d30000000000000384a27561d3000000000000012ca26461d30000000000000384a168a6616263313233a27068a6646566343536a27466d3000000000000012ca27474d30000000000000384"
	goldenLiveNode        = "8da26676cc02a26964d30000000000001092a2706c01a2656c9102a1709283a16ba16ea176d30000000000000007a174cc0683a16ba46e616d65a176ac77726f6e67207265636f7264a174cc0ea17603a26874c3a27666d30000000000000064a27561d3000000000000012ca168a6616263313233a27068a6646566343536a27466d3000000000000012ca27474d30000000000000000"
	goldenPlainRelDelete  = "8ea26676cc02a26964d30000000000001093a2727401a173d3000000000000000aa165d3000000000000000ca1709183a16ba177a176d30000000000000001a174cc06a17602a26874c3a27666d30000000000000064a27674d30000000000000384a26461d30000000000000384a168a6616263313233a27466d3000000000000012ca27474d30000000000000384"
)
