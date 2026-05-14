package storeutil

import (
	"bytes"
	"math"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
)

func TestNodeKeyLength(t *testing.T) {
	t.Parallel()
	key := NodeKey(12345)
	if len(key) != 9 {
		t.Fatalf("expected 9 bytes, got %d", len(key))
	}
	if key[0] != KeyNode {
		t.Fatalf("expected prefix 0x%02X, got 0x%02X", KeyNode, key[0])
	}
}

func TestRelKeyLength(t *testing.T) {
	t.Parallel()
	key := RelKey(12345)
	if len(key) != 9 {
		t.Fatalf("expected 9 bytes, got %d", len(key))
	}
	if key[0] != KeyRel {
		t.Fatalf("expected prefix 0x%02X, got 0x%02X", KeyRel, key[0])
	}
}

func TestLabelIndexKeyLength(t *testing.T) {
	t.Parallel()
	key := LabelIndexKey(42, 12345)
	if len(key) != 11 {
		t.Fatalf("expected 11 bytes, got %d", len(key))
	}
	if key[0] != KeyLabel {
		t.Fatalf("expected prefix 0x%02X, got 0x%02X", KeyLabel, key[0])
	}
}

func TestRelTypeIndexKeyLength(t *testing.T) {
	t.Parallel()
	key := RelTypeIndexKey(42, 12345)
	if len(key) != 11 {
		t.Fatalf("expected 11 bytes, got %d", len(key))
	}
	if key[0] != KeyRelType {
		t.Fatalf("expected prefix 0x%02X, got 0x%02X", KeyRelType, key[0])
	}
}

func TestOutKeyLength(t *testing.T) {
	t.Parallel()
	key := OutKey(1, 2, 3, 4)
	if len(key) != 27 {
		t.Fatalf("expected 27 bytes, got %d", len(key))
	}
	if key[0] != KeyOut {
		t.Fatalf("expected prefix 0x%02X, got 0x%02X", KeyOut, key[0])
	}
}

func TestInKeyLength(t *testing.T) {
	t.Parallel()
	key := InKey(1, 2, 3, 4)
	if len(key) != 27 {
		t.Fatalf("expected 27 bytes, got %d", len(key))
	}
	if key[0] != KeyIn {
		t.Fatalf("expected prefix 0x%02X, got 0x%02X", KeyIn, key[0])
	}
}

func TestHistKeyLength(t *testing.T) {
	t.Parallel()
	nk := HistNodeKey(100, 1)
	if len(nk) != 17 {
		t.Fatalf("expected 17 bytes, got %d", len(nk))
	}
	rk := HistRelKey(200, 2)
	if len(rk) != 17 {
		t.Fatalf("expected 17 bytes, got %d", len(rk))
	}
}

func TestHistPrefixContainment(t *testing.T) {
	t.Parallel()

	nodeID := snowflake.ID(100)
	nodePrefix := HistNodePrefix(nodeID)
	nodeKey := HistNodeKey(nodeID, 7)
	if len(nodePrefix) != SizeNodeKey {
		t.Fatalf("HistNodePrefix length = %d, want %d", len(nodePrefix), SizeNodeKey)
	}
	if !bytes.HasPrefix(nodeKey, nodePrefix) {
		t.Fatal("HistNodeKey should have HistNodePrefix as prefix")
	}
	if bytes.HasPrefix(nodeKey, HistNodePrefix(nodeID+1)) {
		t.Fatal("HistNodeKey should not match prefix for a different node ID")
	}

	relID := snowflake.ID(200)
	relPrefix := HistRelPrefix(relID)
	relKey := HistRelKey(relID, 9)
	if len(relPrefix) != SizeRelKey {
		t.Fatalf("HistRelPrefix length = %d, want %d", len(relPrefix), SizeRelKey)
	}
	if !bytes.HasPrefix(relKey, relPrefix) {
		t.Fatal("HistRelKey should have HistRelPrefix as prefix")
	}
	if bytes.HasPrefix(relKey, HistRelPrefix(relID+1)) {
		t.Fatal("HistRelKey should not match prefix for a different relationship ID")
	}
}

func TestTempIdxKeyLength(t *testing.T) {
	t.Parallel()
	nk := tempNodeKey(1000, 100)
	if len(nk) != 17 {
		t.Fatalf("expected 17 bytes, got %d", len(nk))
	}
	rk := tempRelKey(2000, 200)
	if len(rk) != 17 {
		t.Fatalf("expected 17 bytes, got %d", len(rk))
	}
}

func TestMetaKeyLength(t *testing.T) {
	t.Parallel()
	key := MetaKey("label_tokens")
	if len(key) != 1+len("label_tokens") {
		t.Fatalf("expected %d bytes, got %d", 1+len("label_tokens"), len(key))
	}
	if key[0] != KeyMeta {
		t.Fatalf("expected prefix 0x%02X, got 0x%02X", KeyMeta, key[0])
	}
}

func TestNodeKeyRoundTrip(t *testing.T) {
	t.Parallel()
	ids := []snowflake.ID{0, 1, 42, snowflake.ID(math.MaxInt64)}
	for _, id := range ids {
		key := NodeKey(id)
		got := ParseIDFromKey(key, 1)
		if got != id {
			t.Errorf("NodeKey round-trip: want %d, got %d", id, got)
		}
	}
}

func TestRelKeyRoundTrip(t *testing.T) {
	t.Parallel()
	ids := []snowflake.ID{0, 1, 999, snowflake.ID(math.MaxInt64)}
	for _, id := range ids {
		key := RelKey(id)
		got := ParseIDFromKey(key, 1)
		if got != id {
			t.Errorf("RelKey round-trip: want %d, got %d", id, got)
		}
	}
}

func TestAdjacencyKeyRoundTrip(t *testing.T) {
	t.Parallel()
	start := snowflake.ID(100)
	end := snowflake.ID(200)
	rel := snowflake.ID(300)
	rType := uint16(5)

	ok := OutKey(start, rType, end, rel)
	if ParseIDFromKey(ok, 1) != start {
		t.Fatal("OutKey: start mismatch")
	}
	if ParseRelIDFromAdjKey(ok) != rel {
		t.Fatal("OutKey: relID mismatch")
	}

	ik := InKey(end, rType, start, rel)
	if ParseIDFromKey(ik, 1) != end {
		t.Fatal("InKey: end mismatch")
	}
	if ParseRelIDFromAdjKey(ik) != rel {
		t.Fatal("InKey: relID mismatch")
	}
}

func TestLabelIdxRoundTrip(t *testing.T) {
	t.Parallel()
	nid := snowflake.ID(42)
	tok := uint16(7)
	key := LabelIndexKey(tok, nid)
	got := parseNodeIDFromLabelIdx(key)
	if got != nid {
		t.Fatalf("label idx round-trip: want %d, got %d", nid, got)
	}
}

func TestRelTypeIdxRoundTrip(t *testing.T) {
	t.Parallel()
	rid := snowflake.ID(99)
	tok := uint16(3)
	key := RelTypeIndexKey(tok, rid)
	got := parseRelIDFromTypeIdx(key)
	if got != rid {
		t.Fatalf("reltype idx round-trip: want %d, got %d", rid, got)
	}
}

func TestBigEndianSortOrder(t *testing.T) {
	t.Parallel()
	k1 := NodeKey(1)
	k2 := NodeKey(2)
	kMax := NodeKey(math.MaxInt64)

	if bytes.Compare(k1, k2) >= 0 {
		t.Fatal("NodeKey(1) should sort before NodeKey(2)")
	}
	if bytes.Compare(k2, kMax) >= 0 {
		t.Fatal("NodeKey(2) should sort before NodeKey(maxInt64)")
	}
}

func TestLabelPrefixContainment(t *testing.T) {
	t.Parallel()
	tok := uint16(42)
	prefix := labelIndexPrefix(tok)
	full := LabelIndexKey(tok, 99999)

	if !bytes.HasPrefix(full, prefix) {
		t.Fatal("LabelIndexKey should have labelIndexPrefix as prefix")
	}
}

func TestRelTypePrefixContainment(t *testing.T) {
	t.Parallel()
	tok := uint16(7)
	prefix := relTypeIndexPrefix(tok)
	full := RelTypeIndexKey(tok, 55555)

	if !bytes.HasPrefix(full, prefix) {
		t.Fatal("RelTypeIndexKey should have relTypeIndexPrefix as prefix")
	}
}

func TestOutPrefixContainment(t *testing.T) {
	t.Parallel()
	start := snowflake.ID(100)
	rType := uint16(5)

	full := OutKey(start, rType, 200, 300)

	// outPrefix(start) is prefix of OutKey(start, *, *, *)
	p1 := outPrefix(start)
	if !bytes.HasPrefix(full, p1) {
		t.Fatal("OutKey should have outPrefix as prefix")
	}

	// outTypedPrefix(start, rType) is prefix of OutKey(start, rType, *, *)
	p2 := outTypedPrefix(start, rType)
	if !bytes.HasPrefix(full, p2) {
		t.Fatal("OutKey should have outTypedPrefix as prefix")
	}
}

func TestInPrefixContainment(t *testing.T) {
	t.Parallel()
	end := snowflake.ID(200)
	rType := uint16(5)

	full := InKey(end, rType, 100, 300)

	p1 := inPrefix(end)
	if !bytes.HasPrefix(full, p1) {
		t.Fatal("InKey should have inPrefix as prefix")
	}

	p2 := inTypedPrefix(end, rType)
	if !bytes.HasPrefix(full, p2) {
		t.Fatal("InKey should have inTypedPrefix as prefix")
	}
}

func TestNegativeIDEncoding(t *testing.T) {
	t.Parallel()
	// Negative IDs are valid snowflake values (int64 cast to uint64).
	id := snowflake.ID(-1)
	key := NodeKey(id)
	got := ParseIDFromKey(key, 1)
	if got != id {
		t.Fatalf("negative ID round-trip: want %d, got %d", id, got)
	}
}

func TestKeyPrefixesNonOverlapping(t *testing.T) {
	t.Parallel()
	// Verify that different key types never share a prefix byte.
	prefixes := []byte{KeyNode, KeyRel, KeyLabel, KeyRelType, KeyOut, KeyIn,
		KeyHistNode, KeyHistRel, keyTempNode, keyTempRel, KeyMeta}
	seen := make(map[byte]bool)
	for _, p := range prefixes {
		if seen[p] {
			t.Fatalf("duplicate prefix: 0x%02X", p)
		}
		seen[p] = true
	}
}

func TestOutPrefixExcludesDifferentStart(t *testing.T) {
	t.Parallel()
	key := OutKey(100, 5, 200, 300)
	wrongPrefix := outPrefix(999)
	if bytes.HasPrefix(key, wrongPrefix) {
		t.Fatal("OutKey for start=100 should not match outPrefix for start=999")
	}
}

func TestInPrefixExcludesDifferentEnd(t *testing.T) {
	t.Parallel()
	key := InKey(200, 5, 100, 300)
	wrongPrefix := inPrefix(999)
	if bytes.HasPrefix(key, wrongPrefix) {
		t.Fatal("InKey for end=200 should not match inPrefix for end=999")
	}
}
