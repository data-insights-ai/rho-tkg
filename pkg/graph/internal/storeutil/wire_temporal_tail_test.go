package storeutil

import (
	"bytes"
	"encoding/hex"
	"errors"
	"math"
	"math/rand"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	"github.com/vmihailenco/msgpack/v5"
)

// --- STAGE A: v1 golden fixtures decode identically under v2 code ---
//
// These hex strings were produced by the CURRENT (v1) encoder BEFORE the wire
// format was bumped to v2 (tf/tt omitempty mid-map). New (v2) code MUST still
// decode them byte-identically, proving the migration is backward compatible.
const (
	goldenV1NodeMinimal = "84a26676cc01a26964d30000000000001092a2706c03a17607"
	goldenV1NodeFull    = "de0013a26676cc01a26964d300000000000003e9a2706c01a2656c920203a1709283a16ba3616765a176d3000000000000001ea174cc0683a16ba46e616d65a176a5416c696365a174cc0ea17605a26874c3a27666d30000000000000064a27674d300000000000000c8a27466d3000000000000012ca27474d30000000000000190a26361d300000000000001f4a27561d30000000000000258a26461d300000000000002bca26362a561646d696ea27562a673797374656da26265d300000000000003e7a168a6616263313233a27068a6646566343536"
	goldenV1NodeLegacy  = "83a26964d30000000000001092a2706c03a17607"
	goldenV1RelMinimal  = "86a26676cc01a26964d3000000000000004da2727405a173d3000000000000000aa165d30000000000000014a17600"
	goldenV1RelFull     = "de0010a26676cc01a26964d300000000000001f4a2727403a173d30000000000000064a165d300000000000000c8a1709183a16ba6776569676874a176cb3ff8000000000000a174cc0da17602a26874c3a27666d3000000000000000aa27674d30000000000000014a27466d3000000000000001ea27474d30000000000000028a26362a474657374a168a872656c2d68617368a3666e68a26668a3746e68a27468"
	goldenV1RelLegacy   = "85a26964d3000000000000004da2727405a173d3000000000000000aa165d30000000000000014a17601"
)

func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatalf("bad golden hex: %v", err)
	}
	return b
}

func TestV1GoldenNodesDecodeUnderV2(t *testing.T) {
	t.Parallel()

	// nodeMinimal: fv=1, id=4242, pl=3, v=7
	var w NodeWire
	if err := SafeUnmarshal(mustHex(t, goldenV1NodeMinimal), &w); err != nil {
		t.Fatalf("decode v1 minimal: %v", err)
	}
	if w.FormatVersion != 1 || w.ID != 4242 || w.PrimaryLabel != 3 || w.Version != 7 {
		t.Fatalf("v1 minimal mutated: %+v", w)
	}
	n, err := WireToNodeChecked(w)
	if err != nil {
		t.Fatalf("WireToNodeChecked v1 minimal: %v", err)
	}
	if int64(n.ID().SnowflakeID()) != 4242 || n.Version() != 7 {
		t.Fatalf("v1 minimal round-trip mismatch")
	}

	// nodeFull: exercises temporal (tf=300, tt=400) + integrity + props.
	var wf NodeWire
	if err := SafeUnmarshal(mustHex(t, goldenV1NodeFull), &wf); err != nil {
		t.Fatalf("decode v1 full: %v", err)
	}
	if wf.FormatVersion != 1 || wf.TxFrom != 300 || wf.TxTo != 400 || wf.ValidFrom != 100 || wf.ValidTo != 200 {
		t.Fatalf("v1 full temporal mismatch: %+v", wf)
	}
	if wf.Hash != "abc123" || wf.PrevHash != "def456" || wf.BaseEntityID != 999 {
		t.Fatalf("v1 full integrity mismatch: %+v", wf)
	}
	nf, err := WireToNodeChecked(wf)
	if err != nil {
		t.Fatalf("WireToNodeChecked v1 full: %v", err)
	}
	if nf.Temporal().TxFrom != 300 || nf.Temporal().TxTo != 400 {
		t.Fatalf("v1 full tx round-trip mismatch")
	}
	if v, ok := nf.GetProperty("name"); !ok || v != "Alice" {
		t.Fatalf("v1 full prop mismatch")
	}

	// nodeLegacy: no fv key at all (FormatVersion == 0).
	var wl NodeWire
	if err := SafeUnmarshal(mustHex(t, goldenV1NodeLegacy), &wl); err != nil {
		t.Fatalf("decode legacy: %v", err)
	}
	if wl.FormatVersion != 0 || wl.ID != 4242 || wl.Version != 7 {
		t.Fatalf("legacy mismatch: %+v", wl)
	}
	if _, err := WireToNodeChecked(wl); err != nil {
		t.Fatalf("WireToNodeChecked legacy: %v", err)
	}
}

func TestV1GoldenRelsDecodeUnderV2(t *testing.T) {
	t.Parallel()

	var w RelWire
	if err := SafeUnmarshal(mustHex(t, goldenV1RelMinimal), &w); err != nil {
		t.Fatalf("decode v1 rel minimal: %v", err)
	}
	if w.FormatVersion != 1 || w.ID != 77 || w.RelType != 5 || w.StartID != 10 || w.EndID != 20 {
		t.Fatalf("v1 rel minimal mismatch: %+v", w)
	}

	var wf RelWire
	if err := SafeUnmarshal(mustHex(t, goldenV1RelFull), &wf); err != nil {
		t.Fatalf("decode v1 rel full: %v", err)
	}
	if wf.FormatVersion != 1 || wf.TxFrom != 30 || wf.TxTo != 40 || wf.FromNodeHash != "fh" || wf.ToNodeHash != "th" {
		t.Fatalf("v1 rel full mismatch: %+v", wf)
	}
	rf, err := WireToRelChecked(wf)
	if err != nil {
		t.Fatalf("WireToRelChecked v1 rel full: %v", err)
	}
	if rf.Temporal().TxFrom != 30 || rf.Integrity().FromNodeHash != "fh" {
		t.Fatalf("v1 rel full round-trip mismatch")
	}

	var wl RelWire
	if err := SafeUnmarshal(mustHex(t, goldenV1RelLegacy), &wl); err != nil {
		t.Fatalf("decode rel legacy: %v", err)
	}
	if wl.FormatVersion != 0 || wl.ID != 77 {
		t.Fatalf("rel legacy mismatch: %+v", wl)
	}
}

// --- STAGE A: v2 encoding emits the fixed tail and stamps fv=2 ---

func TestV2EncodingEmitsFixedTail(t *testing.T) {
	t.Parallel()

	n := goldenNodeFullForTail(t)
	b, err := MarshalNodeWire(n)
	if err != nil {
		t.Fatalf("MarshalNodeWire: %v", err)
	}
	if !HasWireTemporalTail(b) {
		t.Fatalf("v2 node has no fixed tail: %s", hex.EncodeToString(b))
	}
	// fv must be 2.
	var w NodeWire
	if err := SafeUnmarshal(b, &w); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if w.FormatVersion != CurrentWireFormatVersion {
		t.Fatalf("fv = %d, want %d", w.FormatVersion, CurrentWireFormatVersion)
	}
	if w.TxFrom != 300 || w.TxTo != 400 {
		t.Fatalf("v2 tail values wrong: tf=%d tt=%d", w.TxFrom, w.TxTo)
	}

	// A zero-temporal node still emits the tail (non-omitempty) as tf=0/tt=0.
	nz := types.NewNode(types.NodeID(snowflake.ID(5)), 1, nil)
	nz.SetTemporal(&types.TemporalMetadata{}) // HasTemporal but zero tx
	bz, err := MarshalNodeWire(nz)
	if err != nil {
		t.Fatalf("MarshalNodeWire zero: %v", err)
	}
	if !HasWireTemporalTail(bz) {
		t.Fatalf("zero-temporal node missing fixed tail: %s", hex.EncodeToString(bz))
	}
}

// --- STAGE A: mixed v1 + v2 rows both decode through every path ---

func TestMixedV1V2Decode(t *testing.T) {
	t.Parallel()

	v1 := mustHex(t, goldenV1NodeFull)
	n := goldenNodeFullForTail(t)
	v2, err := MarshalNodeWire(n)
	if err != nil {
		t.Fatalf("marshal v2: %v", err)
	}

	// The two byte streams differ (v1 mid-map omitempty vs v2 fixed tail)...
	if bytes.Equal(v1, v2) {
		t.Fatalf("v1 and v2 encodings unexpectedly identical")
	}
	// ...but decode to the same temporal content.
	var w1, w2 NodeWire
	if err := SafeUnmarshal(v1, &w1); err != nil {
		t.Fatalf("decode v1: %v", err)
	}
	if err := SafeUnmarshal(v2, &w2); err != nil {
		t.Fatalf("decode v2: %v", err)
	}
	if w1.TxFrom != w2.TxFrom || w1.TxTo != w2.TxTo || w1.ValidFrom != w2.ValidFrom || w1.Hash != w2.Hash {
		t.Fatalf("v1 vs v2 semantic mismatch: %+v vs %+v", w1, w2)
	}
	if w1.FormatVersion != 1 || w2.FormatVersion != 2 {
		t.Fatalf("format versions wrong: %d %d", w1.FormatVersion, w2.FormatVersion)
	}
}

// --- STAGE A: a FUTURE format version fails closed (old-reader probe) ---

func TestFutureVersionFailsClosedTail(t *testing.T) {
	t.Parallel()

	// Simulate an older reader: a v2 binary reading a v3 row must reject.
	future := NodeWire{FormatVersion: CurrentWireFormatVersion + 1, ID: 1, PrimaryLabel: 1}
	data, err := msgpack.Marshal(future)
	if err != nil {
		t.Fatalf("marshal future: %v", err)
	}
	var w NodeWire
	if err := SafeUnmarshal(data, &w); err != nil {
		t.Fatalf("unmarshal future: %v", err)
	}
	if _, err := WireToNodeChecked(w); !errors.Is(err, storepkg.ErrWireFormatVersionUnsupported) {
		t.Fatalf("future row = %v, want ErrWireFormatVersionUnsupported", err)
	}
}

// --- STAGE B: the CROWN equivalence property ---
//
// For randomized entities E and timestamps T:
//
//	PatchWireTemporalTail(PreEncodeV2(E, zero tail), T) == EncodeV2(E with TxFrom/TxTo=T)
//
// byte-for-byte. This is what makes the pipeline patch path correct by
// construction. Exercised for node AND rel, over unicode/empty/max-size/zero
// entities and boundary timestamps.

func TestCrownEquivalenceNode(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewSource(0xC0FFEE))
	for i := 0; i < 400; i++ {
		w := randomNodeWire(rng)
		txFrom := randomTimestamp(rng)
		txTo := randomTimestamp(rng)

		pre, err := PreEncodeNodeWireV2(w)
		if err != nil {
			t.Fatalf("pre-encode[%d]: %v", i, err)
		}
		if !HasWireTemporalTail(pre) {
			t.Fatalf("pre-encode[%d] has no tail", i)
		}
		patched := append([]byte(nil), pre...)
		if err := PatchWireTemporalTail(patched, txFrom, txTo); err != nil {
			t.Fatalf("patch[%d]: %v", i, err)
		}

		w2 := w
		w2.FormatVersion = CurrentWireFormatVersion
		w2.TxFrom = txFrom
		w2.TxTo = txTo
		direct, err := msgpack.Marshal(w2)
		if err != nil {
			t.Fatalf("direct[%d]: %v", i, err)
		}
		if !bytes.Equal(patched, direct) {
			t.Fatalf("crown mismatch node[%d]:\n patched=%s\n direct =%s",
				i, hex.EncodeToString(patched), hex.EncodeToString(direct))
		}
		// And it decodes back to the patched timestamps.
		var back NodeWire
		if err := SafeUnmarshal(patched, &back); err != nil {
			t.Fatalf("decode patched[%d]: %v", i, err)
		}
		if back.TxFrom != txFrom || back.TxTo != txTo {
			t.Fatalf("patched decode[%d]: tf=%d tt=%d want %d %d", i, back.TxFrom, back.TxTo, txFrom, txTo)
		}
	}
}

func TestCrownEquivalenceRel(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewSource(0xBEEF))
	for i := 0; i < 400; i++ {
		w := randomRelWire(rng)
		txFrom := randomTimestamp(rng)
		txTo := randomTimestamp(rng)

		pre, err := PreEncodeRelWireV2(w)
		if err != nil {
			t.Fatalf("pre-encode rel[%d]: %v", i, err)
		}
		patched := append([]byte(nil), pre...)
		if err := PatchWireTemporalTail(patched, txFrom, txTo); err != nil {
			t.Fatalf("patch rel[%d]: %v", i, err)
		}

		w2 := w
		w2.FormatVersion = CurrentWireFormatVersion
		w2.TxFrom = txFrom
		w2.TxTo = txTo
		direct, err := msgpack.Marshal(w2)
		if err != nil {
			t.Fatalf("direct rel[%d]: %v", i, err)
		}
		if !bytes.Equal(patched, direct) {
			t.Fatalf("crown mismatch rel[%d]:\n patched=%s\n direct =%s",
				i, hex.EncodeToString(patched), hex.EncodeToString(direct))
		}
	}
}

// --- STAGE B: PatchWireTemporalTail fails closed on bad buffers ---

func TestPatchFailsClosed(t *testing.T) {
	t.Parallel()

	// Too short.
	if err := PatchWireTemporalTail([]byte{1, 2, 3}, 1, 2); !errors.Is(err, storepkg.ErrCorruptWire) {
		t.Fatalf("short buffer = %v, want ErrCorruptWire", err)
	}

	// A v1 buffer (mid-map omitempty tail) must NOT be patchable — its trailing
	// bytes are not the fixed slot markers.
	v1 := mustHex(t, goldenV1NodeFull)
	if HasWireTemporalTail(v1) {
		t.Fatalf("v1 buffer wrongly reported as having a fixed tail")
	}
	err := PatchWireTemporalTail(v1, 1, 2)
	if !errors.Is(err, storepkg.ErrCorruptWire) {
		t.Fatalf("v1 patch = %v, want ErrCorruptWire", err)
	}

	// A valid v2 buffer with its last byte flipped fails the tt marker check
	// (or, if the flip lands in the value bytes, still must not silently be
	// treated as a mismatched marker — construct a marker corruption explicitly).
	n := goldenNodeFullForTail(t)
	good, _ := MarshalNodeWire(n)
	corrupt := append([]byte(nil), good...)
	corrupt[len(corrupt)-12] ^= 0xFF // flip a byte inside the "tt" key marker
	if err := PatchWireTemporalTail(corrupt, 1, 2); !errors.Is(err, storepkg.ErrCorruptWire) {
		t.Fatalf("corrupt tt marker = %v, want ErrCorruptWire", err)
	}

	// The good buffer patches cleanly and the rest of the row is untouched.
	before := append([]byte(nil), good...)
	if err := PatchWireTemporalTail(good, 111, 222); err != nil {
		t.Fatalf("good patch: %v", err)
	}
	// everything except the two 8-byte value windows is identical.
	L := len(good)
	tfStart := L - wireTxFromValueStartFromEnd
	tfEnd := L - wireTxFromValueEndFromEnd
	ttStart := L - wireTxToValueStartFromEnd
	for i := range good {
		inTf := i >= tfStart && i < tfEnd
		inTt := i >= ttStart && i < L
		if inTf || inTt {
			continue
		}
		if good[i] != before[i] {
			t.Fatalf("patch mutated byte %d outside the slots", i)
		}
	}
}

// --- helpers ---

func goldenNodeFullForTail(t *testing.T) *types.Node {
	t.Helper()
	n := types.NewNode(types.NodeID(snowflake.ID(1001)), 1, []uint16{2, 3})
	n.SetVersion(5)
	n.SetProperties(mustPropertySlice(t, map[string]any{"name": "Alice", "age": int64(30)}))
	n.SetTemporal(&types.TemporalMetadata{
		ValidFrom: 100, ValidTo: 200, TxFrom: 300, TxTo: 400,
		CreatedAt: 500, UpdatedAt: 600, DeletedAt: 700,
		CreatedBy: "admin", UpdatedBy: "system",
	})
	n.Temporal().SetBaseEntityID(types.EntityID(999))
	n.SetIntegrity(&types.NodeIntegrity{Hash: "abc123", PrevHash: "def456"})
	return n
}

func randomTimestamp(rng *rand.Rand) int64 {
	switch rng.Intn(5) {
	case 0:
		return 0
	case 1:
		return math.MaxInt64
	case 2:
		return 1
	default:
		return rng.Int63()
	}
}

var unicodeSamples = []string{"", "Alice", "名前", "café", "🚀emoji", "a\x00b", "tab\tnewline\n"}

func randomNodeWire(rng *rand.Rand) NodeWire {
	w := NodeWire{
		ID:           rng.Int63n(1 << 40),
		PrimaryLabel: rng.Intn(60000) + 1,
		Version:      rng.Intn(1 << 20),
		HasTemporal:  true,
	}
	if w.ID == 0 {
		w.ID = 1
	}
	// extra labels
	nExtra := rng.Intn(4)
	seen := map[int]bool{w.PrimaryLabel: true}
	for len(w.ExtraLabels) < nExtra {
		tok := rng.Intn(60000) + 1
		if seen[tok] {
			continue
		}
		seen[tok] = true
		w.ExtraLabels = append(w.ExtraLabels, tok)
	}
	// properties (sorted, unicode keys/values)
	w.Properties = randomProps(rng)
	// temporal (except tf/tt which are the slot)
	w.ValidFrom = int64(rng.Intn(1000))
	if rng.Intn(2) == 0 {
		w.ValidTo = w.ValidFrom + 1 + int64(rng.Intn(1000))
	}
	w.CreatedAt = int64(rng.Intn(1 << 30))
	w.UpdatedAt = int64(rng.Intn(1 << 30))
	if rng.Intn(3) == 0 {
		w.DeletedAt = int64(rng.Intn(1 << 30))
	}
	w.CreatedBy = unicodeSamples[rng.Intn(len(unicodeSamples))]
	w.UpdatedBy = unicodeSamples[rng.Intn(len(unicodeSamples))]
	if rng.Intn(2) == 0 {
		w.BaseEntityID = rng.Int63n(1 << 40)
	}
	w.Hash = unicodeSamples[rng.Intn(len(unicodeSamples))]
	w.PrevHash = unicodeSamples[rng.Intn(len(unicodeSamples))]
	w.AuthorID = unicodeSamples[rng.Intn(len(unicodeSamples))]
	if rng.Intn(2) == 0 {
		w.Signature = []byte{byte(rng.Intn(256)), 0, byte(rng.Intn(256))}
	}
	w.AuthorizedBy = unicodeSamples[rng.Intn(len(unicodeSamples))]
	w.AuthorizationLevel = uint8(rng.Intn(256))
	return w
}

func randomRelWire(rng *rand.Rand) RelWire {
	w := RelWire{
		ID:           rng.Int63n(1 << 40),
		RelType:      rng.Intn(60000) + 1,
		StartID:      rng.Int63n(1<<40) + 1,
		EndID:        rng.Int63n(1<<40) + 1,
		Version:      rng.Intn(1 << 20),
		HasTemporal:  true,
		Properties:   randomProps(rng),
		ValidFrom:    int64(rng.Intn(1000)),
		CreatedAt:    int64(rng.Intn(1 << 30)),
		UpdatedAt:    int64(rng.Intn(1 << 30)),
		CreatedBy:    unicodeSamples[rng.Intn(len(unicodeSamples))],
		UpdatedBy:    unicodeSamples[rng.Intn(len(unicodeSamples))],
		Hash:         unicodeSamples[rng.Intn(len(unicodeSamples))],
		PrevHash:     unicodeSamples[rng.Intn(len(unicodeSamples))],
		FromNodeHash: unicodeSamples[rng.Intn(len(unicodeSamples))],
		ToNodeHash:   unicodeSamples[rng.Intn(len(unicodeSamples))],
	}
	if w.ID == 0 {
		w.ID = 1
	}
	if rng.Intn(2) == 0 {
		w.ValidTo = w.ValidFrom + 1 + int64(rng.Intn(1000))
	}
	w.AuthorizationLevel = uint8(rng.Intn(256))
	return w
}

var propKeys = []string{"a", "b", "name", "名前", "z_last", "café"}

func randomProps(rng *rand.Rand) []PropertyWire {
	n := rng.Intn(4)
	if n == 0 {
		return nil
	}
	chosen := map[string]bool{}
	var keys []string
	for len(keys) < n {
		k := propKeys[rng.Intn(len(propKeys))]
		if chosen[k] {
			continue
		}
		chosen[k] = true
		keys = append(keys, k)
	}
	// PropertyWire slice must be in strict sorted order by key.
	sortStrings(keys)
	out := make([]PropertyWire, 0, len(keys))
	for _, k := range keys {
		out = append(out, PropertyWire{Key: k, Value: int64(rng.Intn(1000)), Type: ptInt64})
	}
	return out
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j-1] > s[j]; j-- {
			s[j-1], s[j] = s[j], s[j-1]
		}
	}
}
