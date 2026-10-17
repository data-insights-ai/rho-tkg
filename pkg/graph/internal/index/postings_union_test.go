package index

import (
	"slices"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
)

// TestPostingsUnion: the union of posting lists reads as one sorted set with
// each ID once, through AppendAfter (any limit, any cursor) and After (more
// IDs than one batch), and PostingsOfKeys returns the list itself for one key,
// an empty set for none, a union for several (repeated keys once).
func TestPostingsUnion(t *testing.T) {
	pi := NewPropertyIndex()
	want := map[snowflake.ID]bool{}
	for i := 1; i <= 400; i++ {
		id := snowflake.ID(i)
		switch i % 3 {
		case 0:
			pi.AddKey(id, "i64:5")
			want[id] = true
		case 1:
			pi.AddKey(id, "f64:5")
			want[id] = true
		default:
			pi.AddKey(id, "i64:6")
		}
		if i%7 == 0 { // an Elements-like row under two keys
			pi.AddKey(id, "f32:5")
			want[id] = true
		}
	}
	var all []snowflake.ID
	for id := range want {
		all = append(all, id)
	}
	slices.Sort(all)
	u := pi.PostingsOfKeys([]string{"i64:5", "f64:5", "f32:5", "i64:5", "s:none"})
	if _, ok := u.(PostingsUnion); !ok || u.Len() < len(all) {
		t.Fatalf("PostingsOfKeys: %T len %d, want a union of at least %d", u, u.Len(), len(all))
	}
	var got []snowflake.ID
	for id := range u.After(0) {
		got = append(got, id)
	}
	if !slices.Equal(got, all) {
		t.Fatalf("After(0): %d IDs, want %d sorted distinct", len(got), len(all))
	}
	for _, after := range []snowflake.ID{0, 1, 7, 150, 399, 400} {
		for _, limit := range []int{0, 1, 2, 64, 65, 1000} {
			page := u.AppendAfter([]snowflake.ID{42}, after, limit)
			if page[0] != 42 {
				t.Fatalf("AppendAfter overwrote dst")
			}
			var exp []snowflake.ID
			for _, id := range all {
				if id > after && (limit <= 0 || len(exp) < limit) {
					exp = append(exp, id)
				}
			}
			if !slices.Equal(page[1:], exp) {
				t.Errorf("AppendAfter(after %d, limit %d) = %v, want %v", after, limit, page[1:], exp)
			}
		}
	}
	early := 0
	for range u.After(0) {
		if early++; early == 3 {
			break
		}
	}
	// Skewed lists: every ID of one list below every ID of the other, so a
	// page must take all its IDs from the first list.
	skew := NewPropertyIndex()
	for i := 1; i <= 300; i++ {
		skew.AddKey(snowflake.ID(i), "i64:1")
		skew.AddKey(snowflake.ID(1000+i), "f64:1")
	}
	su := skew.PostingsOfKeys([]string{"f64:1", "i64:1"})
	for _, limit := range []int{1, 2, 10, 64} {
		page := su.AppendAfter(nil, 0, limit)
		if len(page) != limit || page[limit-1] != snowflake.ID(limit) {
			t.Errorf("skewed AppendAfter(0, %d) = %v, want 1..%d", limit, page, limit)
		}
	}
	n, prev := 0, snowflake.ID(0)
	for id := range su.After(0) {
		if id <= prev {
			t.Fatalf("skewed After: %d after %d", id, prev)
		}
		prev = id
		n++
	}
	if n != 600 {
		t.Errorf("skewed After: %d IDs, want 600", n)
	}
	if one := pi.PostingsOfKeys([]string{"s:none", "i64:6"}); one != Postings(pi.Entries["i64:6"]) {
		t.Errorf("one key: %T, want the list itself", one)
	}
	if none := pi.PostingsOfKeys([]string{"s:none"}); none.Len() != 0 {
		t.Errorf("no holder: len %d", none.Len())
	}
	var nilIdx *PropertyIndex
	if none := nilIdx.PostingsOfKeys([]string{"i64:5"}); none.Len() != 0 {
		t.Errorf("nil index: len %d", none.Len())
	}
}
