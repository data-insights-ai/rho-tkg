package graph

import (
	"bytes"
	"math"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
)

func TestNodeKeyLength(t *testing.T) {
	t.Parallel()
	key := nodeKey(12345)
	if len(key) != 9 {
		t.Fatalf("expected 9 bytes, got %d", len(key))
	}
	if key[0] != keyNode {
		t.Fatalf("expected prefix 0x%02X, got 0x%02X", keyNode, key[0])
	}
}

func TestRelKeyLength(t *testing.T) {
	t.Parallel()
	key := relKey(12345)
	if len(key) != 9 {
		t.Fatalf("expected 9 bytes, got %d", len(key))
	}
	if key[0] != keyRel {
		t.Fatalf("expected prefix 0x%02X, got 0x%02X", keyRel, key[0])
	}
}

func TestLabelIndexKeyLength(t *testing.T) {
	t.Parallel()
	key := labelIndexKey(42, 12345)
	if len(key) != 11 {
		t.Fatalf("expected 11 bytes, got %d", len(key))
	}
	if key[0] != keyLabel {
		t.Fatalf("expected prefix 0x%02X, got 0x%02X", keyLabel, key[0])
	}
}

func TestRelTypeIndexKeyLength(t *testing.T) {
	t.Parallel()
	key := relTypeIndexKey(42, 12345)
	if len(key) != 11 {
		t.Fatalf("expected 11 bytes, got %d", len(key))
	}
	if key[0] != keyRelType {
		t.Fatalf("expected prefix 0x%02X, got 0x%02X", keyRelType, key[0])
	}
}

func TestOutKeyLength(t *testing.T) {
	t.Parallel()
	key := outKey(1, 2, 3, 4)
	if len(key) != 27 {
		t.Fatalf("expected 27 bytes, got %d", len(key))
	}
	if key[0] != keyOut {
		t.Fatalf("expected prefix 0x%02X, got 0x%02X", keyOut, key[0])
	}
}

func TestInKeyLength(t *testing.T) {
	t.Parallel()
	key := inKey(1, 2, 3, 4)
	if len(key) != 27 {
		t.Fatalf("expected 27 bytes, got %d", len(key))
	}
	if key[0] != keyIn {
		t.Fatalf("expected prefix 0x%02X, got 0x%02X", keyIn, key[0])
	}
}

func TestHistKeyLength(t *testing.T) {
	t.Parallel()
	nk := histNodeKey(100, 1)
	if len(nk) != 17 {
		t.Fatalf("expected 17 bytes, got %d", len(nk))
	}
	rk := histRelKey(200, 2)
	if len(rk) != 17 {
		t.Fatalf("expected 17 bytes, got %d", len(rk))
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
	key := metaKey("label_tokens")
	if len(key) != 1+len("label_tokens") {
		t.Fatalf("expected %d bytes, got %d", 1+len("label_tokens"), len(key))
	}
	if key[0] != keyMeta {
		t.Fatalf("expected prefix 0x%02X, got 0x%02X", keyMeta, key[0])
	}
}

func TestNodeKeyRoundTrip(t *testing.T) {
	t.Parallel()
	ids := []snowflake.ID{0, 1, 42, snowflake.ID(math.MaxInt64)}
	for _, id := range ids {
		key := nodeKey(id)
		got := parseIDFromKey(key, 1)
		if got != id {
			t.Errorf("nodeKey round-trip: want %d, got %d", id, got)
		}
	}
}

func TestRelKeyRoundTrip(t *testing.T) {
	t.Parallel()
	ids := []snowflake.ID{0, 1, 999, snowflake.ID(math.MaxInt64)}
	for _, id := range ids {
		key := relKey(id)
		got := parseIDFromKey(key, 1)
		if got != id {
			t.Errorf("relKey round-trip: want %d, got %d", id, got)
		}
	}
}

func TestAdjacencyKeyRoundTrip(t *testing.T) {
	t.Parallel()
	start := snowflake.ID(100)
	end := snowflake.ID(200)
	rel := snowflake.ID(300)
	rType := uint16(5)

	ok := outKey(start, rType, end, rel)
	if parseIDFromKey(ok, 1) != start {
		t.Fatal("outKey: start mismatch")
	}
	if parseRelIDFromAdjKey(ok) != rel {
		t.Fatal("outKey: relID mismatch")
	}

	ik := inKey(end, rType, start, rel)
	if parseIDFromKey(ik, 1) != end {
		t.Fatal("inKey: end mismatch")
	}
	if parseRelIDFromAdjKey(ik) != rel {
		t.Fatal("inKey: relID mismatch")
	}
}

func TestLabelIdxRoundTrip(t *testing.T) {
	t.Parallel()
	nid := snowflake.ID(42)
	tok := uint16(7)
	key := labelIndexKey(tok, nid)
	got := parseNodeIDFromLabelIdx(key)
	if got != nid {
		t.Fatalf("label idx round-trip: want %d, got %d", nid, got)
	}
}

func TestRelTypeIdxRoundTrip(t *testing.T) {
	t.Parallel()
	rid := snowflake.ID(99)
	tok := uint16(3)
	key := relTypeIndexKey(tok, rid)
	got := parseRelIDFromTypeIdx(key)
	if got != rid {
		t.Fatalf("reltype idx round-trip: want %d, got %d", rid, got)
	}
}

func TestBigEndianSortOrder(t *testing.T) {
	t.Parallel()
	k1 := nodeKey(1)
	k2 := nodeKey(2)
	kMax := nodeKey(math.MaxInt64)

	if bytes.Compare(k1, k2) >= 0 {
		t.Fatal("nodeKey(1) should sort before nodeKey(2)")
	}
	if bytes.Compare(k2, kMax) >= 0 {
		t.Fatal("nodeKey(2) should sort before nodeKey(maxInt64)")
	}
}

func TestLabelPrefixContainment(t *testing.T) {
	t.Parallel()
	tok := uint16(42)
	prefix := labelIndexPrefix(tok)
	full := labelIndexKey(tok, 99999)

	if !bytes.HasPrefix(full, prefix) {
		t.Fatal("labelIndexKey should have labelIndexPrefix as prefix")
	}
}

func TestRelTypePrefixContainment(t *testing.T) {
	t.Parallel()
	tok := uint16(7)
	prefix := relTypeIndexPrefix(tok)
	full := relTypeIndexKey(tok, 55555)

	if !bytes.HasPrefix(full, prefix) {
		t.Fatal("relTypeIndexKey should have relTypeIndexPrefix as prefix")
	}
}

func TestOutPrefixContainment(t *testing.T) {
	t.Parallel()
	start := snowflake.ID(100)
	rType := uint16(5)

	full := outKey(start, rType, 200, 300)

	// outPrefix(start) is prefix of outKey(start, *, *, *)
	p1 := outPrefix(start)
	if !bytes.HasPrefix(full, p1) {
		t.Fatal("outKey should have outPrefix as prefix")
	}

	// outTypedPrefix(start, rType) is prefix of outKey(start, rType, *, *)
	p2 := outTypedPrefix(start, rType)
	if !bytes.HasPrefix(full, p2) {
		t.Fatal("outKey should have outTypedPrefix as prefix")
	}
}

func TestInPrefixContainment(t *testing.T) {
	t.Parallel()
	end := snowflake.ID(200)
	rType := uint16(5)

	full := inKey(end, rType, 100, 300)

	p1 := inPrefix(end)
	if !bytes.HasPrefix(full, p1) {
		t.Fatal("inKey should have inPrefix as prefix")
	}

	p2 := inTypedPrefix(end, rType)
	if !bytes.HasPrefix(full, p2) {
		t.Fatal("inKey should have inTypedPrefix as prefix")
	}
}

func TestNegativeIDEncoding(t *testing.T) {
	t.Parallel()
	// Negative IDs are valid snowflake values (int64 cast to uint64).
	id := snowflake.ID(-1)
	key := nodeKey(id)
	got := parseIDFromKey(key, 1)
	if got != id {
		t.Fatalf("negative ID round-trip: want %d, got %d", id, got)
	}
}

func TestKeyPrefixesNonOverlapping(t *testing.T) {
	t.Parallel()
	// Verify that different key types never share a prefix byte.
	prefixes := []byte{keyNode, keyRel, keyLabel, keyRelType, keyOut, keyIn,
		keyHistNode, keyHistRel, keyTempNode, keyTempRel, keyMeta}
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
	key := outKey(100, 5, 200, 300)
	wrongPrefix := outPrefix(999)
	if bytes.HasPrefix(key, wrongPrefix) {
		t.Fatal("outKey for start=100 should not match outPrefix for start=999")
	}
}

func TestInPrefixExcludesDifferentEnd(t *testing.T) {
	t.Parallel()
	key := inKey(200, 5, 100, 300)
	wrongPrefix := inPrefix(999)
	if bytes.HasPrefix(key, wrongPrefix) {
		t.Fatal("inKey for end=200 should not match inPrefix for end=999")
	}
}
