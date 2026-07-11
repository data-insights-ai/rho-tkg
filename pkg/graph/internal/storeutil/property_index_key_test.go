package storeutil

import (
	"bytes"
	"math"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func vk(v any) string { return types.IndexablePropertyValueKey(v) }

func TestPropertyIndexValueBytes_EmptyNotIndexable(t *testing.T) {
	t.Parallel()
	if _, ok := PropertyIndexValueBytes(""); ok {
		t.Fatal("expected ok=false for empty value key")
	}
	// A map/slice value has no canonical value key.
	if got := vk(map[string]int{"a": 1}); got != "" {
		t.Fatalf("expected non-indexable value to produce empty vk, got %q", got)
	}
}

// TestPropertyIndexValueBytes_MalformedColonLess directly exercises the
// defensive "no colon separator" branch (colon < 0) — every REAL vk produced
// by types.IndexablePropertyValueKey has a "tag:payload" shape, so this input
// class can only arise from a corrupt/forged value key (e.g. hand-built by a
// caller bypassing the canonical constructor, or a future encoding bug). The
// function must fail closed (ok=false, nil payload) rather than panic or
// mis-parse.
func TestPropertyIndexValueBytes_MalformedColonLess(t *testing.T) {
	t.Parallel()
	cases := []string{
		"malformed",     // plausible-looking string, no colon at all
		"i64",           // looks like a numeric tag prefix but missing its ":payload"
		"nocolonhere42", // digits present but still no separator
		" ",             // single non-colon character
	}
	for _, c := range cases {
		payload, ok := PropertyIndexValueBytes(c)
		if ok {
			t.Fatalf("valueKey %q: expected ok=false for a colon-less (malformed) value key, got ok=true payload=%x", c, payload)
		}
		if payload != nil {
			t.Fatalf("valueKey %q: expected nil payload on the malformed fallback, got %x", c, payload)
		}
	}
}

// TestPropertyIndexValueBytes_MalformedNumericTagBadPayload exercises the
// sibling defensive path one level deeper: the colon IS present and the tag
// before it matches a known numeric subtype, but the payload after the colon
// fails to parse as that subtype (e.g. corrupted bytes, or a future encoder
// bug that emits a non-numeric payload under a numeric tag). Must fail closed
// exactly like the colon-less case, never panic.
func TestPropertyIndexValueBytes_MalformedNumericTagBadPayload(t *testing.T) {
	t.Parallel()
	cases := []string{
		"i64:notanumber",
		"u64:-5", // unsigned tag, negative payload — ParseUint rejects it
		"f64:notafloat",
		"i:", // numeric tag, empty payload
	}
	for _, c := range cases {
		payload, ok := PropertyIndexValueBytes(c)
		if ok {
			t.Fatalf("valueKey %q: expected ok=false for an unparseable numeric payload, got ok=true payload=%x", c, payload)
		}
		if payload != nil {
			t.Fatalf("valueKey %q: expected nil payload on the malformed fallback, got %x", c, payload)
		}
	}
}

func TestPropertyIndexValueBytes_Deterministic(t *testing.T) {
	t.Parallel()
	a, ok := PropertyIndexValueBytes(vk(int64(42)))
	if !ok {
		t.Fatal("expected ok=true")
	}
	b, ok := PropertyIndexValueBytes(vk(int64(42)))
	if !ok {
		t.Fatal("expected ok=true")
	}
	if !bytes.Equal(a, b) {
		t.Fatalf("encoding not deterministic: %x != %x", a, b)
	}
}

func TestPropertyIndexValueBytes_NumericFixedLength(t *testing.T) {
	t.Parallel()
	cases := []any{
		int(1), int8(1), int16(1), int32(1), int64(1),
		uint(1), uint8(1), uint16(1), uint32(1), uint64(1),
		float32(1.5), float64(1.5),
	}
	for _, c := range cases {
		payload, ok := PropertyIndexValueBytes(vk(c))
		if !ok {
			t.Fatalf("%T: expected ok=true", c)
		}
		if len(payload) != PropIdxNumericPayloadLen {
			t.Fatalf("%T: expected fixed payload length %d, got %d", c, PropIdxNumericPayloadLen, len(payload))
		}
		if payload[0] != PropIdxDomainNumeric {
			t.Fatalf("%T: expected numeric domain tag, got 0x%02X", c, payload[0])
		}
	}
}

func TestPropertyIndexValueBytes_CrossTypeSharedSortKeyDistinctFullKey(t *testing.T) {
	t.Parallel()
	// int64(5), uint64(5) and float64(5.0) share the same numeric MAGNITUDE, so
	// a range scan must see them at the same ordinal position (shared sort-key
	// prefix) — but they are logically distinct stored values (equality must
	// not conflate them), so the full payload (with the subtype+bits trailer)
	// must differ.
	pi, _ := PropertyIndexValueBytes(vk(int64(5)))
	pu, _ := PropertyIndexValueBytes(vk(uint64(5)))
	pf, _ := PropertyIndexValueBytes(vk(float64(5.0)))

	sortKeyLen := 1 + 8 // domain tag + sortKey
	if !bytes.Equal(pi[:sortKeyLen], pu[:sortKeyLen]) {
		t.Fatalf("int64(5) and uint64(5) should share sort-key bytes: %x vs %x", pi[:sortKeyLen], pu[:sortKeyLen])
	}
	if !bytes.Equal(pi[:sortKeyLen], pf[:sortKeyLen]) {
		t.Fatalf("int64(5) and float64(5.0) should share sort-key bytes: %x vs %x", pi[:sortKeyLen], pf[:sortKeyLen])
	}
	if bytes.Equal(pi, pu) {
		t.Fatal("int64(5) and uint64(5) must NOT produce identical full payloads (distinct stored values)")
	}
	if bytes.Equal(pi, pf) {
		t.Fatal("int64(5) and float64(5.0) must NOT produce identical full payloads (distinct stored values)")
	}
}

func TestPropertyIndexValueBytes_OrderPreserving(t *testing.T) {
	t.Parallel()
	values := []float64{-1e18, -1000.5, -1, -0.0001, 0, 0.0001, 1, 1000.5, 1e18}
	var prevPrefix []byte
	for i, v := range values {
		p, ok := PropertyIndexValueBytes(vk(v))
		if !ok {
			t.Fatalf("value %v: expected ok=true", v)
		}
		prefix := p[:9] // domain + sortKey
		if i > 0 && bytes.Compare(prevPrefix, prefix) >= 0 {
			t.Fatalf("order violation at index %d (value %v): prev=%x cur=%x", i, v, prevPrefix, prefix)
		}
		prevPrefix = prefix
	}
}

func TestPropertyIndexValueBytes_NegativeZeroCollapsesWithPositiveZero(t *testing.T) {
	t.Parallel()
	// types.IndexablePropertyValueKey canonicalizes +0/-0 to the same vk
	// BEFORE the codec ever sees it (Go's == treats them equal), so the codec
	// receives identical input either way.
	if vk(0.0) != vk(math.Copysign(0, -1)) {
		t.Fatal("expected +0 and -0 to canonicalize to the same value key upstream")
	}
}

func TestPropertyIndexValueBytes_NaNDeterministicAndSortsLast(t *testing.T) {
	t.Parallel()
	nan64a, ok := PropertyIndexValueBytes(vk(math.NaN()))
	if !ok {
		t.Fatal("expected ok=true for NaN (equality lookups must still work)")
	}
	nan64b, ok := PropertyIndexValueBytes(vk(math.NaN()))
	if !ok {
		t.Fatal("expected ok=true for NaN")
	}
	if !bytes.Equal(nan64a, nan64b) {
		t.Fatalf("NaN encoding not deterministic: %x != %x", nan64a, nan64b)
	}

	large, ok := PropertyIndexValueBytes(vk(1e300))
	if !ok {
		t.Fatal("expected ok=true")
	}
	if bytes.Compare(large[:9], nan64a[:9]) >= 0 {
		t.Fatal("expected NaN's sort-key prefix to sort after every finite value")
	}
}

func TestPropertyIndexValueBytes_RawDomainStringsBoolsTemporal(t *testing.T) {
	t.Parallel()
	cases := []any{"hello", true, false}
	for _, c := range cases {
		p, ok := PropertyIndexValueBytes(vk(c))
		if !ok {
			t.Fatalf("%v: expected ok=true", c)
		}
		if p[0] != PropIdxDomainRaw {
			t.Fatalf("%v: expected raw domain tag, got 0x%02X", c, p[0])
		}
		if string(p[1:]) != vk(c) {
			t.Fatalf("%v: expected raw payload to be the verbatim value key, got %q want %q", c, string(p[1:]), vk(c))
		}
	}
}

func TestPropertyIndexValueBytes_StringVsNumberNoCollision(t *testing.T) {
	t.Parallel()
	numeric, _ := PropertyIndexValueBytes(vk(int64(5)))
	str, _ := PropertyIndexValueBytes(vk("5"))
	if bytes.Equal(numeric, str) {
		t.Fatal("numeric 5 and string \"5\" must not collide")
	}
	if numeric[0] == str[0] {
		t.Fatal("numeric and raw domains must use distinct domain tags")
	}
}

func TestPropertyIndexEntryKeyAndPrefixes(t *testing.T) {
	t.Parallel()
	payload, ok := PropertyIndexValueBytes(vk(int64(42)))
	if !ok {
		t.Fatal("expected ok=true")
	}
	const tok uint16 = 7
	const nid = 123456789
	full := PropertyIndexEntryKey(tok, payload, nid)

	if full[0] != KeyPropertyIndex {
		t.Fatalf("expected prefix 0x%02X, got 0x%02X", KeyPropertyIndex, full[0])
	}
	if len(full) != 1+2+len(payload)+8 {
		t.Fatalf("unexpected key length: got %d", len(full))
	}
	if got := PropertyIndexNodeIDFromKey(full); got != nid {
		t.Fatalf("expected nodeID %d, got %d", nid, got)
	}

	valPrefix := PropertyIndexValuePrefix(tok, payload)
	if !bytes.HasPrefix(full, valPrefix) {
		t.Fatalf("full key must have the value prefix: %x does not have prefix %x", full, valPrefix)
	}

	tokPrefix := PropertyIndexTokenPrefix(tok)
	if !bytes.HasPrefix(full, tokPrefix) {
		t.Fatalf("full key must have the token prefix: %x does not have prefix %x", full, tokPrefix)
	}
	otherTokPrefix := PropertyIndexTokenPrefix(tok + 1)
	if bytes.HasPrefix(full, otherTokPrefix) {
		t.Fatal("key must not match a different token's prefix")
	}
}

func TestPropertyIndexNumericRangeBounds(t *testing.T) {
	t.Parallel()
	const tok uint16 = 3
	lo, hi := PropertyIndexNumericRangeBounds(tok, 10, 20)
	if bytes.Compare(lo, hi) >= 0 {
		t.Fatalf("expected lo < hi, got lo=%x hi=%x", lo, hi)
	}

	// A value inside [10,20] must fall within [lo,hi] on its sort-key prefix.
	payload, _ := PropertyIndexValueBytes(vk(int64(15)))
	key := PropertyIndexEntryKey(tok, payload, 1)
	boundLen := len(lo)
	if bytes.Compare(key[:boundLen], lo) < 0 || bytes.Compare(key[:boundLen], hi) > 0 {
		t.Fatalf("value 15 should be within widened bounds [%x,%x], got %x", lo, hi, key[:boundLen])
	}

	// A value outside the range must fall outside [lo,hi].
	payloadOut, _ := PropertyIndexValueBytes(vk(int64(100)))
	keyOut := PropertyIndexEntryKey(tok, payloadOut, 1)
	if bytes.Compare(keyOut[:boundLen], lo) >= 0 && bytes.Compare(keyOut[:boundLen], hi) <= 0 {
		t.Fatal("value 100 should be outside bounds [10,20]")
	}
}

func TestPropertyIndexNumericDomainPrefixExcludesRawDomain(t *testing.T) {
	t.Parallel()
	const tok uint16 = 9
	numPrefix := PropertyIndexNumericDomainPrefix(tok)
	rawPayload, _ := PropertyIndexValueBytes(vk("x"))
	rawKey := PropertyIndexEntryKey(tok, rawPayload, 1)
	if bytes.HasPrefix(rawKey, numPrefix) {
		t.Fatal("a raw-domain key must not match the numeric-domain prefix")
	}
}
