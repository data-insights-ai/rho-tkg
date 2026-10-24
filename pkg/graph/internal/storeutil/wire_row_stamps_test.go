package storeutil

import (
	"errors"
	"math/rand"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// DecodeWireRowStamps (backlog 42) reads a stored current row's TxFrom, TxTo,
// DeletedAt and version without the full decode. It must equal the full
// checked decode for every row form the badger store holds: the legacy v1
// layout (omitempty tail), the v2 fixed tail, a pre-encoded row whose tail the
// ingest applier patched, rows without a temporal block (zero stamps, the
// version still read), node and relationship rows. A newer format version, a
// negative version and corrupt bytes fail closed instead of answering zero
// stamps. Faults it names: zero stamps for the patched-tail or the v1 form, the
// version not read, a decode error swallowed into zeros.
func TestDecodeWireRowStamps(t *testing.T) {
	fullNode := func(raw []byte) (storepkg.RowStamps, error) {
		var w NodeWire
		if err := SafeUnmarshal(raw, &w); err != nil {
			return storepkg.RowStamps{}, err
		}
		n, err := WireToNodeChecked(w)
		if err != nil {
			return storepkg.RowStamps{}, err
		}
		return storepkg.NodeRowStamps(n), nil
	}
	fullRel := func(raw []byte) (storepkg.RowStamps, error) {
		var w RelWire
		if err := SafeUnmarshal(raw, &w); err != nil {
			return storepkg.RowStamps{}, err
		}
		r, err := WireToRelChecked(w)
		if err != nil {
			return storepkg.RowStamps{}, err
		}
		return storepkg.RelRowStamps(r), nil
	}
	agree := func(name string, raw []byte, node bool) storepkg.RowStamps {
		t.Helper()
		full := fullRel
		if node {
			full = fullNode
		}
		want, err := full(raw)
		if err != nil {
			t.Fatalf("%s: full decode: %v", name, err)
		}
		got, err := DecodeWireRowStamps(raw)
		if err != nil || got != want {
			t.Fatalf("%s: DecodeWireRowStamps = (%+v, %v), the full decode reads %+v", name, got, err, want)
		}
		return got
	}

	for name, hexStr := range map[string]string{
		"goldenV1NodeMinimal": goldenV1NodeMinimal,
		"goldenV1NodeFull":    goldenV1NodeFull,
		"goldenV1NodeLegacy":  goldenV1NodeLegacy,
	} {
		agree(name, mustHex(t, hexStr), true)
	}
	for name, hexStr := range map[string]string{
		"goldenV1RelMinimal": goldenV1RelMinimal,
		"goldenV1RelFull":    goldenV1RelFull,
		"goldenV1RelLegacy":  goldenV1RelLegacy,
	} {
		agree(name, mustHex(t, hexStr), false)
	}
	if got := agree("v1 full node", mustHex(t, goldenV1NodeFull), true); got.TxFrom == 0 || got.TxTo == 0 || got.DeletedAt == 0 {
		t.Fatalf("v1 golden full node reads zero stamps %+v: the fixture no longer exercises the tail", got)
	}

	tm := &types.TemporalMetadata{ValidFrom: 1, ValidTo: 2, TxFrom: 30, TxTo: 40, DeletedAt: 50, UpdatedAt: 60}
	n := types.NewNode(types.NodeID(snowflake.ID(7)), 1, nil)
	n.SetVersion(4)
	n.SetTemporal(tm)
	raw, err := MarshalNodeWire(n)
	if err != nil {
		t.Fatal(err)
	}
	want := storepkg.RowStamps{TxFrom: 30, TxTo: 40, DeletedAt: 50, Version: 4}
	if got := agree("v2 node", raw, true); got != want {
		t.Fatalf("v2 node = %+v, want %+v", got, want)
	}
	r := types.NewRelationship(types.RelID(snowflake.ID(9)), 2, types.NodeID(1), types.NodeID(2))
	r.SetVersion(6)
	r.SetTemporal(tm)
	relRaw, err := MarshalRelWire(r)
	if err != nil {
		t.Fatal(err)
	}
	if got := agree("v2 rel", relRaw, false); got != (storepkg.RowStamps{TxFrom: 30, TxTo: 40, DeletedAt: 50, Version: 6}) {
		t.Fatalf("v2 rel = %+v", got)
	}

	// A row pre-encoded with a zero tail and patched in place (the ingest
	// applier's form): the stamps are only in the patched slot.
	pre, err := PreEncodeNodeWireV2(NodeToWire(n))
	if err != nil {
		t.Fatal(err)
	}
	if err := PatchWireTemporalTail(pre, 777, 888); err != nil {
		t.Fatal(err)
	}
	if got := agree("patched tail", pre, true); got.TxFrom != 777 || got.TxTo != 888 {
		t.Fatalf("patched tail = %+v, want TxFrom 777, TxTo 888", got)
	}

	bare := types.NewNode(types.NodeID(snowflake.ID(8)), 1, nil)
	bare.SetVersion(3)
	bareRaw, err := MarshalNodeWire(bare)
	if err != nil {
		t.Fatal(err)
	}
	if got := agree("no temporal block", bareRaw, true); got != (storepkg.RowStamps{Version: 3}) {
		t.Fatalf("no temporal block = %+v, want version 3 and zero stamps", got)
	}

	rng := rand.New(rand.NewSource(42)) // #nosec G404 -- deterministic test corpus
	for i := 0; i < 300; i++ {
		rn := types.NewNode(types.NodeID(snowflake.ID(rng.Int63n(1<<40)+1)), uint16(rng.Intn(50)+1), nil) // #nosec G115 -- bounded
		rn.SetVersion(uint32(rng.Intn(1 << 20)))                                                          // #nosec G115 -- bounded
		if rng.Intn(5) != 0 {
			rn.SetTemporal(&types.TemporalMetadata{
				ValidFrom: types.Instant(rng.Int63n(1 << 40)),
				TxFrom:    types.Instant(rng.Int63n(1 << 40)),
				TxTo:      types.Instant(rng.Int63n(2) * rng.Int63n(1<<40)),
				DeletedAt: types.Instant(rng.Int63n(2) * rng.Int63n(1<<40)),
				CreatedBy: "c",
			})
		}
		if rng.Intn(2) == 0 {
			ps, err := types.NewPropertySlice(map[string]any{"k": rng.Int63(), "s": "some value", "l": []any{int64(1), "x"}})
			if err != nil {
				t.Fatal(err)
			}
			rn.SetProperties(ps)
		}
		raw, err := MarshalNodeWire(rn)
		if err != nil {
			t.Fatal(err)
		}
		agree("random node", raw, true)
	}

	if a := testing.AllocsPerRun(50, func() { _, _ = DecodeWireRowStamps(raw) }); a != 0 {
		t.Fatalf("DecodeWireRowStamps allocates %.0f per row", a)
	}

	newer := NodeToWire(n)
	newer.FormatVersion = OpenVoidWireFormatVersion + 1
	newerRaw, err := MarshalNodeWireStruct(newer)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := DecodeWireRowStamps(newerRaw); !errors.Is(err, storepkg.ErrWireFormatVersionUnsupported) {
		t.Fatalf("newer format = (%+v, %v), want ErrWireFormatVersionUnsupported", got, err)
	}
	negative := NodeToWire(n)
	negative.Version = -1
	negRaw, err := MarshalNodeWireStruct(negative)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := DecodeWireRowStamps(negRaw); !errors.Is(err, storepkg.ErrCorruptWire) {
		t.Fatalf("negative version = (%+v, %v), want ErrCorruptWire", got, err)
	}
	// A version above uint32 is corrupt for the full decode too; the stamps
	// read (and DecodeWireTxStamps, which delegates) must not truncate it.
	wide := NodeToWire(n)
	wide.Version = 1 << 32
	wideRaw, err := MarshalNodeWireStruct(wide)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := DecodeWireRowStamps(wideRaw); !errors.Is(err, storepkg.ErrCorruptWire) {
		t.Fatalf("version 1<<32 = (%+v, %v), want ErrCorruptWire", got, err)
	}
	if _, _, _, err := DecodeWireTxStamps(wideRaw); !errors.Is(err, storepkg.ErrCorruptWire) {
		t.Fatalf("DecodeWireTxStamps version 1<<32 = %v, want ErrCorruptWire", err)
	}
	if _, err := fullNode(wideRaw); err == nil {
		t.Fatal("scenario wrong: the full decode accepts version 1<<32")
	}
	for _, bad := range [][]byte{nil, {0xc1}, raw[:len(raw)/2]} {
		if got, err := DecodeWireRowStamps(bad); err == nil {
			t.Fatalf("corrupt row %x decoded to %+v", bad, got)
		}
	}
	// DecodeWireTxStamps is the same read without the version.
	if f, to, da, err := DecodeWireTxStamps(relRaw); err != nil || f != 30 || to != 40 || da != 50 {
		t.Fatalf("DecodeWireTxStamps = (%d, %d, %d, %v)", f, to, da, err)
	}
}
