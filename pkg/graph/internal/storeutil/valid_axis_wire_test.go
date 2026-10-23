package storeutil

import (
	"encoding/hex"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	"github.com/vmihailenco/msgpack/v5"
)

// TestRound11RowBytesUnchanged (round 11, zero is an instant): no flag, no
// field and no format version records how a row reads its valid time — the
// store's valid axis does (types.ValidAxis). A row's wire bytes are those
// v4.62.0 wrote for the same stored values, with and without a claim, and a
// literal-axis row whose start or end is the instant 0 is byte for byte the
// v4.62.0 row of those values (the golden hex below was captured from
// v4.62.0's NodeToWire / RelToWire). The full decode, the selection skeleton
// and the scanner read the values back unchanged; only the axis says what a
// 0 means.
func TestRound11RowBytesUnchanged(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		name      string
		tm        types.TemporalMetadata
		node, rel string // v4.62.0 bytes
	}{
		{"no-claim-legacy", types.TemporalMetadata{TxFrom: 1792000000000}, "88a26676cc02a26964d30b60f990fa29a800a2706c03a1709183a16ba16ba176d30000000000000007a174cc06a17600a26874c3a27466d3000001a13b860000a27474d30000000000000000", "89a26676cc02a26964d30b60f990fa29a801a2727402a173d30b60f990fa29a800a165d30b60f990fa29a802a17600a26874c3a27466d3000001a13b860000a27474d30000000000000000"},                                                                                                                                                                 // a legacy row without a claim (ValidFrom 0 = no claim, ValidTo 0 = open)
		{"unbounded-open-legacy", types.TemporalMetadata{ValidFrom: types.UnboundedValidFrom, TxFrom: 1792000000000}, "89a26676cc02a26964d30b60f990fa29a800a2706c03a1709183a16ba16ba176d30000000000000007a174cc06a17600a26874c3a27666d38000000000000000a27466d3000001a13b860000a27474d30000000000000000", "8aa26676cc02a26964d30b60f990fa29a801a2727402a173d30b60f990fa29a800a165d30b60f990fa29a802a17600a26874c3a27666d38000000000000000a27466d3000001a13b860000a27474d30000000000000000"},                                                                      // an UntimedUnbounded untimed first version
		{"claim", types.TemporalMetadata{ValidFrom: 5, ValidTo: 10, TxFrom: 1792000000000}, "8aa26676cc02a26964d30b60f990fa29a800a2706c03a1709183a16ba16ba176d30000000000000007a174cc06a17600a26874c3a27666d30000000000000005a27674d3000000000000000aa27466d3000001a13b860000a27474d30000000000000000", "8ba26676cc02a26964d30b60f990fa29a801a2727402a173d30b60f990fa29a800a165d30b60f990fa29a802a17600a26874c3a27666d30000000000000005a27674d3000000000000000aa27466d3000001a13b860000a27474d30000000000000000"},                                                // a row with a claim [5, 10)
		{"zero-start", types.TemporalMetadata{ValidFrom: 0, ValidTo: 10, TxFrom: 1792000000000}, "89a26676cc02a26964d30b60f990fa29a800a2706c03a1709183a16ba16ba176d30000000000000007a174cc06a17600a26874c3a27674d3000000000000000aa27466d3000001a13b860000a27474d30000000000000000", "8aa26676cc02a26964d30b60f990fa29a801a2727402a173d30b60f990fa29a800a165d30b60f990fa29a802a17600a26874c3a27674d3000000000000000aa27466d3000001a13b860000a27474d30000000000000000"},                                                                                           // a literal-axis row valid from the instant 0 to 10
		{"zero-end", types.TemporalMetadata{ValidFrom: -10, ValidTo: 0, TxFrom: 1792000000000}, "89a26676cc02a26964d30b60f990fa29a800a2706c03a1709183a16ba16ba176d30000000000000007a174cc06a17600a26874c3a27666d3fffffffffffffff6a27466d3000001a13b860000a27474d30000000000000000", "8aa26676cc02a26964d30b60f990fa29a801a2727402a173d30b60f990fa29a800a165d30b60f990fa29a802a17600a26874c3a27666d3fffffffffffffff6a27466d3000001a13b860000a27474d30000000000000000"},                                                                                            // a literal-axis row valid from -10 to the instant 0
		{"whole-axis", types.TemporalMetadata{ValidFrom: types.UnboundedValidFrom, ValidTo: types.InfiniteValidTo, TxFrom: 1792000000000}, "8aa26676cc02a26964d30b60f990fa29a800a2706c03a1709183a16ba16ba176d30000000000000007a174cc06a17600a26874c3a27666d38000000000000000a27674d37fffffffffffffffa27466d3000001a13b860000a27474d30000000000000000", "8ba26676cc02a26964d30b60f990fa29a801a2727402a173d30b60f990fa29a800a165d30b60f990fa29a802a17600a26874c3a27666d38000000000000000a27674d37fffffffffffffffa27466d3000001a13b860000a27474d30000000000000000"}, // a literal-axis untimed row (the whole axis)
	} {
		t.Run(c.name, func(t *testing.T) {
			n := types.NewNode(types.NodeID(819929533249071104), 3, nil)
			if err := n.SetProperty("k", int64(7)); err != nil {
				t.Fatal(err)
			}
			tm := c.tm
			n.SetTemporal(&tm)
			raw, err := msgpack.Marshal(NodeToWire(n))
			if err != nil {
				t.Fatal(err)
			}
			if got := hex.EncodeToString(raw); got != c.node {
				t.Errorf("node bytes changed:\n got %s\nwant %s", got, c.node)
			}
			rel := types.NewRelationship(types.RelID(819929533249071105), 2, types.NodeID(819929533249071104), types.NodeID(819929533249071106))
			rtm := c.tm
			rel.SetTemporal(&rtm)
			rraw, err := msgpack.Marshal(RelToWire(rel))
			if err != nil {
				t.Fatal(err)
			}
			if got := hex.EncodeToString(rraw); got != c.rel {
				t.Errorf("relationship bytes changed:\n got %s\nwant %s", got, c.rel)
			}
			back, err := WireToNodeChecked(mustDecodeNodeWire(t, raw))
			if err != nil {
				t.Fatal(err)
			}
			if got := back.Temporal(); got.ValidFrom != c.tm.ValidFrom || got.ValidTo != c.tm.ValidTo {
				t.Errorf("full decode: [%d, %d), want [%d, %d)", got.ValidFrom, got.ValidTo, c.tm.ValidFrom, c.tm.ValidTo)
			}
			_, sel, err := DecodeWireTemporalMeta(raw)
			if err != nil {
				t.Fatal(err)
			}
			if sel.ValidFrom != c.tm.ValidFrom || sel.ValidTo != c.tm.ValidTo {
				t.Errorf("selection decode: [%d, %d), want [%d, %d)", sel.ValidFrom, sel.ValidTo, c.tm.ValidFrom, c.tm.ValidTo)
			}
		})
	}
}

func mustDecodeNodeWire(t *testing.T, raw []byte) NodeWire {
	t.Helper()
	var w NodeWire
	if err := SafeUnmarshal(raw, &w); err != nil {
		t.Fatal(err)
	}
	return w
}

// TestRound11AxisReadsStoredValues: the two axes read the same stored values
// differently, and only through their two parameters (the unclaimed start
// and the open end): a stored 0 is "no claim" / "open" on the legacy axis and
// the instant 0 on the literal axis; the literal axis's open end is
// types.InfiniteValidTo and its untimed start types.UnboundedValidFrom.
func TestRound11AxisReadsStoredValues(t *testing.T) {
	t.Parallel()
	leg, lit := types.LegacyValidAxis, types.LiteralValidAxis
	tm := &types.TemporalMetadata{ValidFrom: 0, ValidTo: 0, TxFrom: 77}
	if got := EntityValidFrom(leg, 0, tm); got != 77 {
		t.Errorf("legacy start of a stored 0: %d, want the TxFrom 77", got)
	}
	if got := EntityValidFrom(lit, 0, tm); got != 0 {
		t.Errorf("literal start of a stored 0: %d, want 0", got)
	}
	if !leg.Open(0) || lit.Open(0) || !lit.Open(types.InfiniteValidTo) || leg.Open(types.InfiniteValidTo) {
		t.Error("Open: the legacy axis's open end is 0, the literal axis's InfiniteValidTo")
	}
	if leg.Claimed(0) || !lit.Claimed(0) || !leg.Claimed(types.UnboundedValidFrom) {
		t.Error("Claimed: a stored 0 is a claim on the literal axis only")
	}
	if leg.NoClaimStart() != 0 || lit.NoClaimStart() != types.UnboundedValidFrom {
		t.Error("NoClaimStart: legacy 0, literal -inf")
	}
	if leg.Literal() || !lit.Literal() || (types.ValidAxis{}) != leg {
		t.Error("the zero ValidAxis is the legacy axis")
	}
	// The point and overlap rules: a literal row ending at 0 does not hold at
	// 0, a legacy row with a stored end of 0 is open.
	if !IntervalCovers(leg, -10, 0, 5) || IntervalCovers(lit, -10, 0, 0) || !IntervalCovers(lit, -10, 0, -1) {
		t.Error("IntervalCovers reads the end on the axis")
	}
	if IntervalMeets(lit, -10, 0, 0, 10) || !IntervalMeets(leg, -10, 0, 0, 10) {
		t.Error("IntervalMeets reads the end on the axis")
	}
	// Reopening a delete-written end yields the axis's open end.
	del := &types.TemporalMetadata{ValidFrom: 0, ValidTo: 50, DeletedAt: 50, TxFrom: 10, TxTo: 50}
	cp := *del
	NormalizeTemporalVisibleAtTxTime(lit, &cp, 20)
	if cp.ValidTo != types.InfiniteValidTo {
		t.Errorf("literal reopen: ValidTo %d, want InfiniteValidTo", cp.ValidTo)
	}
	cp = *del
	NormalizeTemporalVisibleAtTxTime(leg, &cp, 20)
	if cp.ValidTo != 0 {
		t.Errorf("legacy reopen: ValidTo %d, want 0", cp.ValidTo)
	}
	if !OwnOpenAtTxTime(lit, del, 20) || OwnOpenAtTxTime(lit, del, 60) {
		t.Error("OwnOpenAtTxTime on the literal axis")
	}
}
