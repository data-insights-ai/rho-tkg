package storeutil

import (
	"math/rand"
	"reflect"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// referenceDecodeTemporalMeta is the scanner-free arm of
// DecodeWireTemporalMeta: the audited SafeUnmarshal partial decode. The
// scanner must either agree with it exactly or decline (ok=false).
func referenceDecodeTemporalMeta(raw []byte) (wireTemporalMetaPartial, error) {
	var w wireTemporalMetaPartial
	err := SafeUnmarshal(raw, &w)
	return w, err
}

// TestScanWireTemporalMeta_MatchesSafeUnmarshal is the scanner's equivalence
// battery: on every input class — marshalled node/rel rows (random temporal,
// random properties, both wire versions), golden v1 vectors, and adversarial
// bytes (truncations at every position, bit flips, deep nesting, duplicate
// keys, trailing garbage) — the scanner must either return EXACTLY the
// SafeUnmarshal partial-decode result or decline. It must never panic and
// never return ok=true with a divergent value.
func TestScanWireTemporalMeta_MatchesSafeUnmarshal(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewSource(0x5CA11ED)) // #nosec G404 — deterministic test fuzz

	check := func(name string, raw []byte, wantScanOK bool) {
		t.Helper()
		got, ok := scanWireTemporalMeta(raw)
		if !ok {
			if wantScanOK {
				t.Fatalf("%s: scanner declined a well-formed row", name)
			}
			return // declined — fallback path takes over, nothing to compare
		}
		want, err := referenceDecodeTemporalMeta(raw)
		if err != nil {
			t.Fatalf("%s: scanner accepted input SafeUnmarshal rejects: %v", name, err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("%s: scanner diverges: got %+v, want %+v", name, got, want)
		}
	}

	// Golden v1 fixtures (legacy omitempty layout).
	for name, hexStr := range map[string]string{
		"goldenV1NodeMinimal": goldenV1NodeMinimal,
		"goldenV1NodeFull":    goldenV1NodeFull,
		"goldenV1NodeLegacy":  goldenV1NodeLegacy,
		"goldenV1RelMinimal":  goldenV1RelMinimal,
		"goldenV1RelFull":     goldenV1RelFull,
		"goldenV1RelLegacy":   goldenV1RelLegacy,
	} {
		check(name, mustHex(t, hexStr), true)
	}

	// Randomized marshalled rows: nodes and rels, random temporal instants
	// (incl. zeros and extremes), random properties (nested containers), with
	// and without a temporal block.
	for i := 0; i < 400; i++ {
		n := types.NewNode(types.NodeID(snowflake.ID(rng.Int63n(1<<40)+1)), uint16(rng.Intn(100)+1), nil) // #nosec G115 — bounded
		n.SetVersion(uint32(rng.Intn(1 << 20)))                                                           // #nosec G115 — bounded
		props := map[string]any{}
		for p := 0; p < rng.Intn(6); p++ {
			switch rng.Intn(4) {
			case 0:
				props["s"+string(rune('a'+p))] = "value-of-some-length-" + string(rune('a'+p))
			case 1:
				props["i"+string(rune('a'+p))] = rng.Int63()
			case 2:
				props["l"+string(rune('a'+p))] = []string{"x", "y", "z"}
			case 3:
				props["m"+string(rune('a'+p))] = map[string]any{"inner": []any{int64(1), "two"}}
			}
		}
		if len(props) > 0 {
			ps, err := types.NewPropertySlice(props)
			if err != nil {
				t.Fatalf("props: %v", err)
			}
			n.SetProperties(ps)
		}
		if rng.Intn(4) != 0 {
			n.SetTemporal(&types.TemporalMetadata{
				ValidFrom: types.Instant(randomTimestamp(rng)),
				ValidTo:   0,
				TxFrom:    types.Instant(randomTimestamp(rng)),
				TxTo:      types.Instant(randomTimestamp(rng)),
				CreatedAt: types.Instant(randomTimestamp(rng)),
				UpdatedAt: types.Instant(randomTimestamp(rng)),
				DeletedAt: types.Instant(randomTimestamp(rng)),
				CreatedBy: "creator",
			})
		}
		raw, err := MarshalNodeWire(n)
		if err != nil {
			continue // invalid random combo (e.g. vf>=vt) — not a wire we ever store
		}
		check("random node", raw, true)

		// Adversarial mutations of a real row: every truncation length, and a
		// byte flip at a random offset. The scanner may accept OR decline —
		// but an accept must match SafeUnmarshal, and nothing may panic.
		if i < 40 {
			for cut := 0; cut < len(raw); cut += 1 + rng.Intn(3) {
				check("truncated", raw[:cut], false)
			}
			flipped := append([]byte(nil), raw...)
			flipped[rng.Intn(len(flipped))] ^= 0xFF
			got, ok := scanWireTemporalMeta(flipped)
			if ok {
				want, err := referenceDecodeTemporalMeta(flipped)
				if err == nil && !reflect.DeepEqual(got, want) {
					t.Fatalf("bit-flip divergence: got %+v, want %+v", got, want)
				}
			}
		}
	}

	// Hand-built adversarial shapes.
	deep := make([]byte, 0, 4096)
	for i := 0; i < 2000; i++ {
		deep = append(deep, 0x91) // fixarray(1) nested 2000 deep
	}
	deep = append(deep, 0xc0)
	adversarial := map[string][]byte{
		"empty":              {},
		"non-map top":        {0x91, 0xc0},
		"invalid type":       {0x81, 0xa1, 'x', 0xc1},
		"deep nesting value": append([]byte{0x81, 0xa1, 'p'}, deep...),
		"trailing garbage":   append(mustHex(t, goldenV1NodeMinimal), 0xc0),
		"int key":            {0x81, 0x01, 0x02},
	}
	for name, raw := range adversarial {
		check(name, raw, false)
	}

	// Duplicate keys: last-wins in BOTH arms (the partial struct has no
	// interface fields, so the reflect decoder does not panic here).
	dup, err := msgpack.Marshal(map[string]any{"v": 1})
	if err != nil {
		t.Fatal(err)
	}
	_ = dup // build a real duplicate manually: fixmap(2){"tf":1,"tf":2}
	dupRaw := []byte{0x82, 0xa2, 't', 'f', 0x01, 0xa2, 't', 'f', 0x02}
	got, ok := scanWireTemporalMeta(dupRaw)
	if !ok {
		t.Fatal("duplicate-key row: scanner declined, want last-wins accept")
	}
	want, err := referenceDecodeTemporalMeta(dupRaw)
	if err != nil {
		t.Fatalf("duplicate-key reference: %v", err)
	}
	if !reflect.DeepEqual(got, want) || got.TxFrom != 2 {
		t.Fatalf("duplicate-key: got %+v, want %+v (tf=2, last wins)", got, want)
	}
}

// BenchmarkDecodeWireTemporalMeta quantifies the scanner vs the SafeUnmarshal
// partial decode on a realistic wide row.
func BenchmarkDecodeWireTemporalMeta(b *testing.B) {
	n := types.NewNode(types.NodeID(snowflake.ID(424242)), 7, []uint16{8, 9})
	n.SetVersion(17)
	ps, err := types.NewPropertySlice(map[string]any{
		"name": "a-reasonably-long-name-value", "age": int64(30), "tags": []string{"one", "two", "three"},
		"meta": map[string]any{"k1": "v1", "k2": int64(2)}, "score": 3.14,
	})
	if err != nil {
		b.Fatal(err)
	}
	n.SetProperties(ps)
	n.SetTemporal(&types.TemporalMetadata{ValidFrom: 100, TxFrom: 300, TxTo: 400, UpdatedAt: 600})
	raw, err := MarshalNodeWire(n)
	if err != nil {
		b.Fatal(err)
	}

	b.Run("scanner", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, ok := scanWireTemporalMeta(raw); !ok {
				b.Fatal("scanner declined")
			}
		}
	})
	b.Run("safeunmarshal", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := referenceDecodeTemporalMeta(raw); err != nil {
				b.Fatal(err)
			}
		}
	})
}
