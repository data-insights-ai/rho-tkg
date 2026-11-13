package storeutil

import (
	"bytes"
	"math"
	"slices"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
)

// TestEntityIndexTimeKey_RoundTripAndOrder (RELDISKWIN option B): a
// time-ordered entry round-trips every field; the late axis sorts by lateTo
// ascending, the recent axis by last descending, negative and open instants
// included; a seek key sorts before every entry of its instant; the tail is 37
// bytes for both kinds and EntityIndexEntryOf reads the ID and transaction
// time of one.
func TestEntityIndexTimeKey_RoundTripAndOrder(t *testing.T) {
	head := AppendEntityIndexHead(nil, EntityIndexRelTime, 3, 4)
	p, ok := AppendEntityIndexValue(head, "string:a")
	if !ok {
		t.Fatal("no payload")
	}
	if EntityIndexTailLen(EntityIndexRelTime) != 37 || EntityIndexTailLen(EntityIndexNodeTime) != 37 {
		t.Fatalf("tail %d/%d, want 37", EntityIndexTailLen(EntityIndexRelTime), EntityIndexTailLen(EntityIndexNodeTime))
	}
	instants := []int64{math.MinInt64, -5, 0, 1, 1000, math.MaxInt64 - 1, math.MaxInt64}
	for _, axis := range []byte{EntityIndexAxisLate, EntityIndexAxisRecent} {
		var keys [][]byte
		for _, at := range instants {
			k := AppendEntityIndexTimeEntry(slices.Clone(p), axis, at, -7, snowflake.ID(42), 99, 3)
			if len(k) != len(p)+37 {
				t.Fatalf("key length %d, want %d", len(k), len(p)+37)
			}
			gAxis, gAt, gFrom, gID, gTx, gV, ok := EntityIndexTimeEntryOf(k)
			if !ok || gAxis != axis || gAt != at || gFrom != -7 || gID != 42 || gTx != 99 || gV != 3 {
				t.Fatalf("axis %d at %d: got %d %d %d %d %d %d %t", axis, at, gAxis, gAt, gFrom, gID, gTx, gV, ok)
			}
			if _, id, tx, ok := EntityIndexEntryOf(k, EntityIndexRelTime); !ok || id != 42 || tx != 99 {
				t.Fatalf("EntityIndexEntryOf: %d %d %t", id, tx, ok)
			}
			if !bytes.HasPrefix(k, append(slices.Clone(p), axis)) {
				t.Fatal("entry not under prefix|axis")
			}
			seek := AppendEntityIndexTimeSeek(slices.Clone(p), axis, at)
			if bytes.Compare(seek, k) >= 0 {
				t.Fatalf("seek of %d sorts at or after its entry", at)
			}
			keys = append(keys, k)
		}
		sorted := slices.SortedFunc(slices.Values(keys), bytes.Compare)
		want := keys
		if axis == EntityIndexAxisRecent {
			want = slices.Clone(keys)
			slices.Reverse(want)
		}
		for i := range want {
			if !bytes.Equal(sorted[i], want[i]) {
				t.Fatalf("axis %d: key order differs at %d", axis, i)
			}
		}
	}
	// A longer raw value's entries are not under the shorter value's prefix|axis.
	longer, _ := AppendEntityIndexValue(slices.Clone(head), "string:a\x00b")
	k := AppendEntityIndexTimeEntry(longer, EntityIndexAxisLate, 5, 0, 1, 1, 0)
	if bytes.HasPrefix(k, append(slices.Clone(p), EntityIndexAxisLate)) {
		t.Fatal("a longer raw value's entry falls under the value's late axis")
	}
	if !EntityIndexIsTime(EntityIndexRelTime) || !EntityIndexIsTime(EntityIndexNodeTime) || EntityIndexIsTime(EntityIndexRelSpan) {
		t.Fatal("EntityIndexIsTime")
	}
	if EntityIndexTimeKind(EntityIndexRelSpan) != EntityIndexRelTime || EntityIndexTimeKind(EntityIndexNodeSpan) != EntityIndexNodeTime {
		t.Fatal("EntityIndexTimeKind")
	}
	if !EntityIndexIsNodeMember(EntityIndexNodeTime) || EntityIndexIsNodeMember(EntityIndexRelTime) {
		t.Fatal("EntityIndexIsNodeMember")
	}
}
