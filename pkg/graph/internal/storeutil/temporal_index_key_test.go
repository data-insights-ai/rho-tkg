package storeutil

import (
	"bytes"
	"sort"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestTemporalIndexEntryKey_RoundTrip proves the (labelToken, from, nodeID)
// tuple embedded in a key survives encode-then-decode exactly, across
// negative, zero, and positive `from` values (Instant is a signed int64 —
// negative values are an edge case the higher-level equivalence tests never
// exercise, since effective valid-from is always derived from a positive
// snowflake timestamp or a caller-asserted value in practice).
func TestTemporalIndexEntryKey_RoundTrip(t *testing.T) {
	t.Parallel()
	cases := []struct {
		label uint16
		from  types.Instant
		id    snowflake.ID
	}{
		{0, 0, 0},
		{1, 1, 1},
		{65535, types.Instant(-1), snowflake.ID(1<<62 - 1)},
		{42, types.Instant(-1000000), 42},
		{7, types.Instant(1<<62 - 1), snowflake.ID(9999999999)},
		{100, types.Instant(-(1 << 62)), 1},
	}
	for _, c := range cases {
		key := TemporalIndexEntryKey(c.label, c.from, c.id)
		if len(key) != SizeTemporalIndexEntryKey {
			t.Fatalf("TemporalIndexEntryKey(%d,%d,%d): len=%d, want %d",
				c.label, c.from, c.id, len(key), SizeTemporalIndexEntryKey)
		}
		if key[0] != KeyTemporalIndex {
			t.Fatalf("TemporalIndexEntryKey(%d,%d,%d): prefix byte = 0x%02x, want 0x%02x",
				c.label, c.from, c.id, key[0], KeyTemporalIndex)
		}
		gotID := TemporalIndexNodeIDFromKey(key)
		if gotID != c.id {
			t.Fatalf("TemporalIndexNodeIDFromKey: got %d, want %d", gotID, c.id)
		}
		gotFrom := TemporalIndexFromFromKey(key)
		if gotFrom != c.from {
			t.Fatalf("TemporalIndexFromFromKey: got %d, want %d", gotFrom, c.from)
		}
	}
}

// TestTemporalIndexEntryValue_RoundTrip proves the TO instant survives
// encode-then-decode exactly, including negative and zero (open-ended).
func TestTemporalIndexEntryValue_RoundTrip(t *testing.T) {
	t.Parallel()
	for _, to := range []types.Instant{0, 1, -1, types.Instant(1<<62 - 1), types.Instant(-(1 << 62))} {
		val := TemporalIndexEntryValue(to)
		if len(val) != 8 {
			t.Fatalf("TemporalIndexEntryValue(%d): len=%d, want 8", to, len(val))
		}
		got := TemporalIndexEntryValueDecode(val)
		if got != to {
			t.Fatalf("TemporalIndexEntryValueDecode(TemporalIndexEntryValue(%d)) = %d, want %d", to, got, to)
		}
	}
}

// TestTemporalIndexEntryValueDecode_ShortInput proves the defensive
// too-short-input guard fails soft (returns 0) instead of panicking — the
// keyspace is internal/trusted, but a truncated/corrupt row must not crash
// the reader.
func TestTemporalIndexEntryValueDecode_ShortInput(t *testing.T) {
	t.Parallel()
	for _, val := range [][]byte{nil, {}, {1, 2, 3}, {1, 2, 3, 4, 5, 6, 7}} {
		if got := TemporalIndexEntryValueDecode(val); got != 0 {
			t.Fatalf("TemporalIndexEntryValueDecode(%v) = %d, want 0", val, got)
		}
	}
}

// TestTemporalIndexEntryKey_OrderPreservesFromThenID proves the key's byte
// ordering matches (From ASC, ID ASC) — the invariant loadIndexesScan's fast
// path depends on to stream entries directly into TemporalIndex.Entries
// without a separate sort, across negative, zero, and positive from values.
func TestTemporalIndexEntryKey_OrderPreservesFromThenID(t *testing.T) {
	t.Parallel()
	type entry struct {
		from types.Instant
		id   snowflake.ID
	}
	entries := []entry{
		{-500, 1}, {-500, 2}, {-1, 100}, {0, 1}, {0, 2}, {1, 1}, {100, 5}, {100, 6}, {100000, 1},
	}
	const label = uint16(7)

	keys := make([][]byte, len(entries))
	for i, e := range entries {
		keys[i] = TemporalIndexEntryKey(label, e.from, e.id)
	}

	// Shuffle-independent check: sort a COPY of the keys lexicographically and
	// verify the resulting order matches (From ASC, ID ASC) over `entries`.
	sortedIdx := make([]int, len(entries))
	for i := range sortedIdx {
		sortedIdx[i] = i
	}
	sort.Slice(sortedIdx, func(i, j int) bool {
		return bytes.Compare(keys[sortedIdx[i]], keys[sortedIdx[j]]) < 0
	})
	wantOrder := make([]int, len(entries))
	for i := range wantOrder {
		wantOrder[i] = i
	}
	sort.Slice(wantOrder, func(i, j int) bool {
		a, b := entries[wantOrder[i]], entries[wantOrder[j]]
		if a.from != b.from {
			return a.from < b.from
		}
		return a.id < b.id
	})
	for i := range entries {
		if sortedIdx[i] != wantOrder[i] {
			t.Fatalf("byte-order index %d = entry %d (from=%d,id=%d), want entry %d (from=%d,id=%d)",
				i, sortedIdx[i], entries[sortedIdx[i]].from, entries[sortedIdx[i]].id,
				wantOrder[i], entries[wantOrder[i]].from, entries[wantOrder[i]].id)
		}
	}
}

// TestTemporalIndexTokenPrefix_IsKeyPrefix proves TemporalIndexTokenPrefix is
// always a true byte prefix of TemporalIndexEntryKey for the SAME label
// token, and NOT a prefix for a DIFFERENT one — the invariant a prefix
// iteration (loadIndexesScan's fast path, the corruption-purge scan, and the
// drop-index purge) depends on to visit exactly one label's rows.
func TestTemporalIndexTokenPrefix_IsKeyPrefix(t *testing.T) {
	t.Parallel()
	key := TemporalIndexEntryKey(42, 1000, snowflake.ID(7))
	prefix42 := TemporalIndexTokenPrefix(42)
	if !bytes.HasPrefix(key, prefix42) {
		t.Fatalf("key for label 42 does not have TemporalIndexTokenPrefix(42) as a prefix: key=%x prefix=%x", key, prefix42)
	}
	prefix43 := TemporalIndexTokenPrefix(43)
	if bytes.HasPrefix(key, prefix43) {
		t.Fatalf("key for label 42 unexpectedly has TemporalIndexTokenPrefix(43) as a prefix: key=%x prefix=%x", key, prefix43)
	}
}

// TestOrderPreservingInt64_RoundTripAndOrder directly exercises the sign-flip
// helpers' contract: round-trip exactness and unsigned-comparison ordering
// matching signed int64 ordering across the negative/positive boundary.
func TestOrderPreservingInt64_RoundTripAndOrder(t *testing.T) {
	t.Parallel()
	values := []int64{
		-1 << 63, -1 << 62, -1000000, -1, 0, 1, 1000000, 1<<62 - 1, 1<<63 - 1,
	}
	bits := make([]uint64, len(values))
	for i, v := range values {
		bits[i] = orderPreservingInt64Bits(v)
		if got := orderPreservingInt64Value(bits[i]); got != v {
			t.Fatalf("orderPreservingInt64Value(orderPreservingInt64Bits(%d)) = %d, want %d", v, got, v)
		}
	}
	// values is already ascending; bits must be ascending too (unsigned).
	for i := 1; i < len(bits); i++ {
		if bits[i-1] >= bits[i] {
			t.Fatalf("order-preserving bits not ascending: bits[%d]=%d (v=%d) >= bits[%d]=%d (v=%d)",
				i-1, bits[i-1], values[i-1], i, bits[i], values[i])
		}
	}
}
