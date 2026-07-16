package storeutil

import (
	"bytes"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

// propSetBytes marshals each property to its canonical wire bytes keyed by
// identity — an order-independent, msgpack-normalized property-set fingerprint.
func propSetBytes(t *testing.T, props []PropertyWire) map[propKeyRef][]byte {
	t.Helper()
	m := make(map[propKeyRef][]byte, len(props))
	for i := range props {
		b, err := msgpack.Marshal(props[i])
		if err != nil {
			t.Fatalf("marshal prop %+v: %v", props[i], err)
		}
		m[propRefOf(props[i])] = b
	}
	return m
}

func metaBytes(t *testing.T, w NodeWire) []byte {
	t.Helper()
	w.Properties = nil
	b, err := msgpack.Marshal(w)
	if err != nil {
		t.Fatalf("marshal meta: %v", err)
	}
	return b
}

func assertPropSetsEqual(t *testing.T, want, got []PropertyWire, ctx string) {
	t.Helper()
	wm, gm := propSetBytes(t, want), propSetBytes(t, got)
	if len(wm) != len(gm) {
		t.Fatalf("%s: property count = %d, want %d", ctx, len(gm), len(wm))
	}
	for ref, wb := range wm {
		gb, ok := gm[ref]
		if !ok {
			t.Fatalf("%s: reconstructed missing property %+v", ctx, ref)
		}
		if !bytes.Equal(wb, gb) {
			t.Fatalf("%s: property %+v differs after reconstruction", ctx, ref)
		}
	}
}

// tokProps builds a tokenized (v2) property slice.
func tokProps(entries ...PropertyWire) []PropertyWire { return entries }

func p(token uint16, value any, typ uint8) PropertyWire {
	return PropertyWire{KeyToken: token, Value: value, Type: typ}
}

const bigBlob = "Enterprise customer onboarded via partner channel; contract renewed 2026, " +
	"net-90 terms, primary contact prefers email, escalation path documented in CRM, " +
	"and a great deal more unchanging text that must never be duplicated per version."

// nodePair returns an (anchor, target) NodeWire pair sharing a large blob but
// differing in the requested ways.
func baseNode(ver int, props []PropertyWire) NodeWire {
	return NodeWire{
		FormatVersion: 2,
		ID:            123456789,
		PrimaryLabel:  4,
		ExtraLabels:   []int{7, 9},
		Version:       ver,
		HasTemporal:   true,
		ValidFrom:     1_700_000_000_000,
		CreatedAt:     1_700_000_000_000,
		UpdatedAt:     1_700_000_000_000 + int64(ver)*3600_000,
		Hash:          "hash-v" + string(rune('0'+ver)),
		PrevHash:      "hash-v" + string(rune('0'+ver-1)),
		Properties:    props,
	}
}

func TestNodeHistoryDeltaReconstructs(t *testing.T) {
	// blob prop (token 1) unchanged; status (token 2) changed; counter (token 3)
	// changed; extra (token 4) only on anchor (removed); added (token 5) only on
	// target (added).
	anchor := baseNode(0, tokProps(
		p(1, bigBlob, 6),
		p(2, "active", 6),
		p(3, int64(0), 2),
		p(4, "legacy", 6),
	))
	target := baseNode(1, tokProps(
		p(1, bigBlob, 6),      // unchanged
		p(2, "suspended", 6),  // changed
		p(3, int64(7), 2),     // changed
		p(5, float64(3.5), 5), // added
	)) // token 4 removed

	delta := DiffNodeHistory(anchor, target)

	// ps must contain exactly {2,3,5}; pr exactly {4}. The unchanged blob (1) must
	// NOT appear in ps — that is the whole point.
	psRefs := map[uint16]bool{}
	for _, pw := range delta.PS {
		psRefs[pw.KeyToken] = true
	}
	if !psRefs[2] || !psRefs[3] || !psRefs[5] || psRefs[1] || psRefs[4] {
		t.Fatalf("delta.PS tokens = %v, want {2,3,5} without 1/4", psRefs)
	}
	if len(delta.PR) != 1 || delta.PR[0].Token != 4 {
		t.Fatalf("delta.PR = %+v, want [{4}]", delta.PR)
	}

	// Reconstruct in-memory and after a full codec round-trip.
	got := ApplyNodeHistory(anchor, delta)
	if !bytes.Equal(metaBytes(t, target), metaBytes(t, got)) {
		t.Fatalf("reconstructed meta fields differ from target")
	}
	assertPropSetsEqual(t, target.Properties, got.Properties, "in-memory apply")

	enc, err := EncodeNodeHistoryDelta(delta)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	dec, err := DecodeNodeHistoryDelta(enc)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	got2 := ApplyNodeHistory(anchor, dec)
	if !bytes.Equal(metaBytes(t, target), metaBytes(t, got2)) {
		t.Fatalf("post-codec reconstructed meta differs from target")
	}
	assertPropSetsEqual(t, target.Properties, got2.Properties, "post-codec apply")
}

func TestNodeHistoryDeltaDeterministicAndIdempotent(t *testing.T) {
	anchor := baseNode(0, tokProps(p(1, bigBlob, 6), p(2, "active", 6), p(3, int64(0), 2)))
	target := baseNode(1, tokProps(p(1, bigBlob, 6), p(2, "pending", 6), p(3, int64(9), 2)))

	b1, err := EncodeNodeHistoryDelta(DiffNodeHistory(anchor, target))
	if err != nil {
		t.Fatalf("encode1: %v", err)
	}
	b2, err := EncodeNodeHistoryDelta(DiffNodeHistory(anchor, target))
	if err != nil {
		t.Fatalf("encode2: %v", err)
	}
	if !bytes.Equal(b1, b2) {
		t.Fatalf("Diff+Encode not deterministic — breaks replica byte-exactness")
	}
	// Codec idempotence: decode then re-encode must reproduce the same bytes.
	dec, err := DecodeNodeHistoryDelta(b1)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	b3, err := EncodeNodeHistoryDelta(dec)
	if err != nil {
		t.Fatalf("re-encode: %v", err)
	}
	if !bytes.Equal(b1, b3) {
		t.Fatalf("Encode(Decode(x)) != x — codec not stable")
	}
}

func TestHistoryValueKindOf(t *testing.T) {
	full := baseNode(3, tokProps(p(1, "x", 6)))
	fb, err := msgpack.Marshal(full)
	if err != nil {
		t.Fatalf("marshal full: %v", err)
	}
	if k := HistoryValueKindOf(fb); k != HistoryFull {
		t.Fatalf("full row classified as %d, want HistoryFull; first byte 0x%02x", k, fb[0])
	}
	db, err := EncodeNodeHistoryDelta(DiffNodeHistory(full, baseNode(4, tokProps(p(1, "y", 6)))))
	if err != nil {
		t.Fatalf("encode delta: %v", err)
	}
	if k := HistoryValueKindOf(db); k != HistoryDelta {
		t.Fatalf("delta row classified as %d, want HistoryDelta", k)
	}
	if HistoryValueKindOf(nil) != HistoryFull {
		t.Fatalf("empty value must classify as HistoryFull")
	}
	// A non-delta must be rejected by the delta decoder.
	if _, err := DecodeNodeHistoryDelta(fb); err == nil {
		t.Fatalf("DecodeNodeHistoryDelta accepted a full row")
	}
}

// TestHistoryDeltaFailsClosedForDeltaUnawareDecoder locks the safety property
// that guards a downgrade: a delta value ('D'-tagged) fed to the plain
// full-snapshot decode path (what a delta-UNAWARE binary does — SafeUnmarshal
// straight into a NodeWire) must FAIL, never silently produce a bogus entity.
// The tag byte 0x44 is a msgpack positive-fixint, not a map header, so the
// struct decode rejects it. This is why enabling HistoryDeltaEncoding is
// fail-closed for an older reader even without a wire-format-version bump.
func TestHistoryDeltaFailsClosedForDeltaUnawareDecoder(t *testing.T) {
	anchor := baseNode(0, tokProps(p(1, bigBlob, 6), p(2, "active", 6)))
	target := baseNode(1, tokProps(p(1, bigBlob, 6), p(2, "closed", 6)))
	enc, err := EncodeNodeHistoryDelta(DiffNodeHistory(anchor, target))
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var w NodeWire
	if err := SafeUnmarshal(enc, &w); err == nil {
		t.Fatalf("delta value decoded as a plain NodeWire without error — NOT fail-closed (silent misread risk)")
	}
}

func TestNodeHistoryDeltaSizeWin(t *testing.T) {
	// Wide entity, big blob, only one scalar changes: the delta must be far
	// smaller than a full snapshot.
	props := []PropertyWire{p(1, bigBlob, 6)}
	for tok := uint16(2); tok <= 12; tok++ {
		props = append(props, p(tok, "value-"+string(rune('a'+tok)), 6))
	}
	anchor := baseNode(0, props)
	changed := append([]PropertyWire(nil), props...)
	changed[1] = p(2, "CHANGED", 6)
	target := baseNode(1, changed)

	full, err := msgpack.Marshal(target)
	if err != nil {
		t.Fatalf("marshal target: %v", err)
	}
	delta, err := EncodeNodeHistoryDelta(DiffNodeHistory(anchor, target))
	if err != nil {
		t.Fatalf("encode delta: %v", err)
	}
	if len(delta) >= len(full) {
		t.Fatalf("delta (%d B) not smaller than full snapshot (%d B)", len(delta), len(full))
	}
	t.Logf("size win: full=%d B  delta=%d B  (%.0f%% smaller)", len(full), len(delta),
		100*float64(len(full)-len(delta))/float64(len(full)))
}

// --- Relationship mirror (Testing Rule 2 parity) ---

func baseRel(ver int, props []PropertyWire) RelWire {
	return RelWire{
		FormatVersion: 2,
		ID:            99887766,
		RelType:       3,
		StartID:       111,
		EndID:         222,
		Version:       ver,
		HasTemporal:   true,
		ValidFrom:     1_700_000_000_000,
		CreatedAt:     1_700_000_000_000,
		UpdatedAt:     1_700_000_000_000 + int64(ver)*3600_000,
		Hash:          "rh-v" + string(rune('0'+ver)),
		PrevHash:      "rh-v" + string(rune('0'+ver-1)),
		FromNodeHash:  "fnh",
		ToNodeHash:    "tnh",
		Properties:    props,
	}
}

func TestRelHistoryDeltaReconstructs(t *testing.T) {
	anchor := baseRel(0, tokProps(p(1, bigBlob, 6), p(2, "active", 6), p(4, "legacy", 6)))
	target := baseRel(1, tokProps(p(1, bigBlob, 6), p(2, "closed", 6), p(5, int64(1), 2)))

	delta := DiffRelHistory(anchor, target)
	enc, err := EncodeRelHistoryDelta(delta)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if HistoryValueKindOf(enc) != HistoryDelta {
		t.Fatalf("rel delta misclassified")
	}
	dec, err := DecodeRelHistoryDelta(enc)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	got := ApplyRelHistory(anchor, dec)

	var tMeta, gMeta RelWire = target, got
	tMeta.Properties, gMeta.Properties = nil, nil
	tb, _ := msgpack.Marshal(tMeta)
	gb, _ := msgpack.Marshal(gMeta)
	if !bytes.Equal(tb, gb) {
		t.Fatalf("rel reconstructed meta differs from target")
	}
	// Byte-compare property sets via the node helper's marshaling logic.
	wm := propSetBytes(t, target.Properties)
	gm := propSetBytes(t, got.Properties)
	if len(wm) != len(gm) {
		t.Fatalf("rel prop count = %d, want %d", len(gm), len(wm))
	}
	for ref, wb := range wm {
		if !bytes.Equal(wb, gm[ref]) {
			t.Fatalf("rel property %+v differs after reconstruction", ref)
		}
	}
}
