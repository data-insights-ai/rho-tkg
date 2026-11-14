package storeutil

import (
	"errors"
	"math/rand"
	"slices"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// fullDecodeLabels is the full decoder's answer for a node row: its label
// tokens, primary first (WireToNodeChecked's node), or its error.
func fullDecodeLabels(raw []byte) ([]uint16, error) {
	var w NodeWire
	if err := SafeUnmarshal(raw, &w); err != nil {
		return nil, err
	}
	n, err := WireToNodeChecked(w)
	if err != nil {
		return nil, err
	}
	out := make([]uint16, 0, n.LabelTokenCount())
	for i := 0; i < n.LabelTokenCount(); i++ {
		out = append(out, n.LabelTokenRawAt(i))
	}
	return out, nil
}

// referenceDecodeTemporalMetaLabels is the scanner-free arm of
// DecodeWireNodeTemporalMetaLabels.
func referenceDecodeTemporalMetaLabels(raw []byte) (wireTemporalMetaLabelsPartial, error) {
	var w wireTemporalMetaLabelsPartial
	err := SafeUnmarshal(raw, &w)
	return w, err
}

// TestDecodeValidEndNodeValue_LabelsMatchFullDecode (R2-2 VEATTRIB, design
// risk 4): the valid-end first call reads a node's current labels with the
// row's temporal block in one decode, the wire scanner capturing "pl" and
// "el". On every input class the scanner's label capture either equals the
// SafeUnmarshal arm (selection fields and labels) or declines; and the
// tokens DecodeValidEndNodeValue returns equal the full decoder's (primary
// first) on random rows with 1 to 6 labels, golden v1 rows and node deltas.
// Rows the full decoder rejects for their labels (token 0, past uint16, a
// repeat) fail closed with ErrCorruptWire, from the scanner and the fallback.
func TestDecodeValidEndNodeValue_LabelsMatchFullDecode(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewSource(0x7AB1E5)) // #nosec G404 — deterministic test fuzz
	sameAsReference := func(name string, raw []byte, wantScanOK bool) {
		t.Helper()
		var lab wireRowTokens
		got, ok := scanWireTemporalMetaInto(raw, &lab)
		if !ok {
			if wantScanOK {
				t.Fatalf("%s: scanner declined a well-formed row", name)
			}
			return
		}
		want, err := referenceDecodeTemporalMetaLabels(raw)
		if err != nil {
			t.Fatalf("%s: scanner accepted input SafeUnmarshal rejects: %v", name, err)
		}
		if got != want.wireTemporalMetaPartial || lab.Primary != want.Primary || !slices.Equal(lab.Extra, want.Extra) || lab.RelType != want.RelType {
			t.Fatalf("%s: scanner diverges: got %+v %+v, want %+v", name, got, lab, want)
		}
		// Without a label sink the scanner answers as before.
		if plain, ok := scanWireTemporalMeta(raw); !ok || plain != got {
			t.Fatalf("%s: the label capture changed the plain scan: %+v %v, want %+v", name, plain, ok, got)
		}
	}
	sameAsFull := func(name string, raw []byte) {
		t.Helper()
		want, err := fullDecodeLabels(raw)
		if err != nil {
			t.Fatalf("%s: full decode: %v", name, err)
		}
		tm, got, err := DecodeValidEndNodeValue(raw)
		if err != nil || !slices.Equal(got, want) {
			t.Fatalf("%s: labels %v, %v; the full decoder's %v", name, got, err, want)
		}
		wantTM, err := DecodeValidEndValue(raw, true)
		if err != nil || (tm == nil) != (wantTM == nil) || (tm != nil && *tm != *wantTM) {
			t.Fatalf("%s: temporal %+v; DecodeValidEndValue's %+v, %v", name, tm, wantTM, err)
		}
	}

	for name, hexStr := range map[string]string{
		"goldenV1NodeMinimal": goldenV1NodeMinimal,
		"goldenV1NodeFull":    goldenV1NodeFull,
		"goldenV1NodeLegacy":  goldenV1NodeLegacy,
	} {
		raw := mustHex(t, hexStr)
		sameAsReference(name, raw, true)
		sameAsFull(name, raw)
	}

	for i := 0; i < 400; i++ {
		k := 1 + rng.Intn(6)
		toks := make([]uint16, 0, k)
		for len(toks) < k {
			tok := uint16(1 + rng.Intn(1<<16-1)) // #nosec G115 — bounded
			if i%3 == 0 {
				tok = uint16(1 + rng.Intn(127)) // #nosec G115 — fixint tokens
			}
			if !slices.Contains(toks, tok) {
				toks = append(toks, tok)
			}
		}
		n := types.NewNode(types.NodeID(rng.Int63n(1<<40)+1), toks[0], toks[1:])
		n.SetVersion(uint32(rng.Intn(1 << 20))) // #nosec G115 — bounded
		if rng.Intn(3) != 0 {
			if err := n.SetProperty("p", rng.Int63()); err != nil {
				t.Fatal(err)
			}
		}
		if rng.Intn(4) != 0 {
			n.SetTemporal(&types.TemporalMetadata{TxFrom: types.Instant(randomTimestamp(rng)), DeletedAt: types.Instant(randomTimestamp(rng))})
		}
		raw, err := MarshalNodeWire(n)
		if err != nil {
			continue
		}
		sameAsReference("random node", raw, true)
		sameAsFull("random node", raw)
		if i < 40 {
			for cut := 0; cut < len(raw); cut += 1 + rng.Intn(3) {
				sameAsReference("truncated", raw[:cut], false)
			}
			flipped := append([]byte(nil), raw...)
			flipped[rng.Intn(len(flipped))] ^= 0xFF
			var lab wireRowTokens
			if got, ok := scanWireTemporalMetaInto(flipped, &lab); ok {
				if want, err := referenceDecodeTemporalMetaLabels(flipped); err == nil && (got != want.wireTemporalMetaPartial || lab.Primary != want.Primary || !slices.Equal(lab.Extra, want.Extra) || lab.RelType != want.RelType) {
					t.Fatalf("bit-flip divergence: got %+v %+v, want %+v", got, lab, want)
				}
			}
		}
		if i < 20 { // a node delta's labels come from its Meta
			nw := NodeToWire(n)
			anchor := nw
			anchor.Properties = nil
			delta, err := EncodeNodeHistoryDelta(DiffNodeHistory(anchor, nw))
			if err != nil {
				t.Fatal(err)
			}
			if _, got, err := DecodeValidEndNodeValue(delta); err != nil || !slices.Equal(got, toks) {
				t.Fatalf("node delta: labels %v, %v; want %v", got, err, toks)
			}
		}
	}

	// Hand-built rows: fixmap{"pl": x, "el": y}.
	row := func(pl []byte, el []byte) []byte {
		out := []byte{0x82, 0xa2, 'p', 'l'}
		out = append(out, pl...)
		out = append(out, 0xa2, 'e', 'l')
		return append(out, el...)
	}
	for _, c := range []struct {
		name    string
		raw     []byte
		want    []uint16
		corrupt bool // fails closed with ErrCorruptWire
	}{
		{"el nil", row([]byte{0x05}, []byte{0xc0}), []uint16{5}, false},
		{"el empty", row([]byte{0x05}, []byte{0x90}), []uint16{5}, false},
		{"el array16", row([]byte{0xcd, 0x01, 0x00}, []byte{0xdc, 0x00, 0x02, 0x07, 0xcd, 0xff, 0xff}), []uint16{256, 7, 65535}, false},
		{"primary 0", row([]byte{0x00}, []byte{0xc0}), nil, true},
		{"primary past uint16", row([]byte{0xce, 0x00, 0x01, 0x00, 0x00}, []byte{0xc0}), nil, true},
		{"negative extra", row([]byte{0x05}, []byte{0x91, 0xff}), nil, true},
		{"extra repeats primary", row([]byte{0x05}, []byte{0x91, 0x05}), nil, true},
		{"extra repeats extra", row([]byte{0x05}, []byte{0x92, 0x06, 0x06}), nil, true},
		{"el a string", row([]byte{0x05}, []byte{0xa1, 'x'}), nil, false},
		{"el array of strings", row([]byte{0x05}, []byte{0x91, 0xa1, 'x'}), nil, false},
		{"el claims more than the row holds", row([]byte{0x05}, []byte{0xdd, 0x7f, 0xff, 0xff, 0xff, 0x01}), nil, false},
	} {
		sameAsReference(c.name, c.raw, false)
		_, got, err := DecodeValidEndNodeValue(c.raw)
		switch {
		case c.corrupt && !errors.Is(err, storepkg.ErrCorruptWire):
			t.Fatalf("%s: %v, %v; want ErrCorruptWire", c.name, got, err)
		case !c.corrupt && c.want != nil && (err != nil || !slices.Equal(got, c.want)):
			t.Fatalf("%s: %v, %v; want %v", c.name, got, err, c.want)
		case !c.corrupt && c.want == nil && err == nil:
			t.Fatalf("%s: decoded %v; want an error", c.name, got)
		}
	}
	// Relationship rows: the type token comes with the temporal block, from a
	// full row and a delta, equal to the relationship's; a type token outside
	// 1..65535 fails closed.
	for i := 0; i < 200; i++ {
		tok := uint16(1 + rng.Intn(1<<16-1)) // #nosec G115 — bounded
		r := types.NewRelationship(types.RelID(rng.Int63n(1<<40)+1), tok, types.NodeID(1+rng.Int63n(1<<30)), types.NodeID(1+rng.Int63n(1<<30)))
		if rng.Intn(4) != 0 {
			r.SetTemporal(&types.TemporalMetadata{TxFrom: types.Instant(randomTimestamp(rng)), DeletedAt: types.Instant(randomTimestamp(rng))})
		}
		if rng.Intn(2) == 0 {
			if err := r.SetProperty("p", rng.Int63()); err != nil {
				t.Fatal(err)
			}
		}
		raw, err := MarshalRelWire(r)
		if err != nil {
			continue
		}
		sameAsReference("random rel", raw, true)
		tm, got, err := DecodeValidEndRelValue(raw)
		wantTM, werr := DecodeValidEndValue(raw, false)
		if err != nil || got != tok || werr != nil || (tm == nil) != (wantTM == nil) || (tm != nil && *tm != *wantTM) {
			t.Fatalf("random rel: type %d, %+v, %v; want %d, %+v", got, tm, err, tok, wantTM)
		}
		if i < 20 {
			rw := RelToWire(r)
			anchor := rw
			anchor.Properties = nil
			delta, err := EncodeRelHistoryDelta(DiffRelHistory(anchor, rw))
			if err != nil {
				t.Fatal(err)
			}
			if _, got, err := DecodeValidEndRelValue(delta); err != nil || got != tok {
				t.Fatalf("rel delta: type %d, %v; want %d", got, err, tok)
			}
		}
	}
	for name, raw := range map[string][]byte{
		"rt 0":            {0x81, 0xa2, 'r', 't', 0x00},
		"rt past uint16":  {0x81, 0xa2, 'r', 't', 0xce, 0x00, 0x01, 0x00, 0x00},
		"rt missing":      {0x81, 0xa1, 'v', 0x01},
		"rt negative":     {0x81, 0xa2, 'r', 't', 0xff},
		"rt not a number": {0x81, 0xa2, 'r', 't', 0xa1, 'x'},
	} {
		sameAsReference(name, raw, false)
		if _, got, err := DecodeValidEndRelValue(raw); err == nil {
			t.Fatalf("%s: decoded type %d; want an error", name, got)
		}
	}

	// Duplicate keys: the last "el" wins in both arms.
	dup := append(row([]byte{0x05}, []byte{0x91, 0x06}), 0xa2, 'e', 'l', 0x91, 0x07)
	dup[0] = 0x83
	sameAsReference("duplicate el", dup, true)
	if _, got, err := DecodeValidEndNodeValue(dup); err != nil || !slices.Equal(got, []uint16{5, 7}) {
		t.Fatalf("duplicate el: %v, %v; want [5 7]", got, err)
	}
}
