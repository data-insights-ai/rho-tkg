package index

import (
	"math/rand"
	"slices"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
)

// Backlog 72: IDPostings is the one sorted posting list of every equality
// index key (property, rel property, composite, the numeric buckets). Against a
// map oracle over random adds and removes — duplicates, absent removes,
// ascending and descending runs, chunk splits and merges — every After(cursor)
// read must give the oracle's sorted IDs above the cursor.
func TestIDPostingsAgainstOracle(t *testing.T) {
	for seed := int64(1); seed <= 6; seed++ {
		rng := rand.New(rand.NewSource(seed)) // #nosec G404 -- deterministic test input
		var p IDPostings
		oracle := map[snowflake.ID]struct{}{}
		span := int64(50 + rng.Intn(4*orderedKeyChunk))
		for op := 0; op < 6000; op++ {
			id := snowflake.ID(1 + rng.Int63n(span))
			switch r := rng.Intn(10); {
			case r < 5:
				_, had := oracle[id]
				if added := p.add(id); added == had {
					t.Fatalf("seed %d: add(%d) = %v with member %v", seed, id, added, had)
				}
				oracle[id] = struct{}{}
			case r < 8:
				_, had := oracle[id]
				if removed := p.remove(id); removed != had {
					t.Fatalf("seed %d: remove(%d) = %v with member %v", seed, id, removed, had)
				}
				delete(oracle, id)
			case r < 9: // an ascending run past every member (the snowflake write path)
				base := snowflake.ID(span + 1)
				span += int64(1 + rng.Intn(orderedKeyChunk))
				for x := base; x <= snowflake.ID(span); x++ {
					p.add(x)
					oracle[x] = struct{}{}
				}
			default: // a descending run (every insert lands at the front of its chunk)
				for x := snowflake.ID(1 + rng.Int63n(span)); x > 0 && rng.Intn(64) != 0; x-- {
					p.add(x)
					oracle[x] = struct{}{}
				}
			}
			if op%97 == 0 {
				checkPostings(t, &p, oracle, span)
			}
		}
		checkPostings(t, &p, oracle, span)
	}
}

func checkPostings(t *testing.T, p *IDPostings, oracle map[snowflake.ID]struct{}, span int64) {
	t.Helper()
	want := make([]snowflake.ID, 0, len(oracle))
	for id := range oracle {
		want = append(want, id)
	}
	slices.Sort(want)
	if p.Len() != len(want) {
		t.Fatalf("Len %d, oracle %d", p.Len(), len(want))
	}
	// Cursors: 0, every boundary member, a gap, past the end.
	cursors := []snowflake.ID{0, snowflake.ID(span + 1)}
	if len(want) > 0 {
		cursors = append(cursors, want[0], want[len(want)-1], want[len(want)/2], want[len(want)/2]+1)
	}
	for _, after := range cursors {
		i, _ := slices.BinarySearch(want, after+1)
		exp := want[i:]
		got := slices.Collect(p.After(after))
		if !slices.Equal(got, exp) {
			t.Fatalf("After(%d): %d IDs, want %d (first got %v want %v)", after, len(got), len(exp), head(got), head(exp))
		}
		for _, limit := range []int{1, 3, orderedKeyChunk + 1} {
			page := p.AppendAfter(nil, after, limit)
			if !slices.Equal(page, exp[:min(limit, len(exp))]) {
				t.Fatalf("AppendAfter(%d, %d) = %v, want %v", after, limit, head(page), head(exp[:min(limit, len(exp))]))
			}
		}
		if all := p.AppendAfter(nil, after, 0); !slices.Equal(all, exp) {
			t.Fatalf("AppendAfter(%d, 0): %d IDs, want %d", after, len(all), len(exp))
		}
	}
	for _, id := range append(want[:min(len(want), 50)], snowflake.ID(span+7)) {
		_, member := oracle[id]
		if p.Has(id) != member {
			t.Fatalf("Has(%d) = %v, member %v", id, !member, member)
		}
	}
	// Chunk invariants: ascending across chunks, no empty chunk.
	var prev snowflake.ID
	for ci, c := range p.s.chunks {
		if len(c) == 0 {
			t.Fatalf("chunk %d empty", ci)
		}
		if len(c) > 2*orderedKeyChunk {
			t.Fatalf("chunk %d holds %d keys, above the split cap", ci, len(c))
		}
		for _, id := range c {
			if id <= prev {
				t.Fatalf("chunk %d out of order: %d after %d", ci, id, prev)
			}
			prev = id
		}
	}
}

func head(ids []snowflake.ID) []snowflake.ID { return ids[:min(len(ids), 4)] }

// A range-counted index that never held a key counts 0: its counts are
// allocated at the first key (backlog 72) and a read before must not touch
// them. Found by the sharded RangeCardinality tests.
func TestRangeCardinalityEmptyCountedIndex(t *testing.T) {
	pi := NewPropertyIndexWith(true, false)
	if n, ok := pi.RangeCardinality(1, 10, true, true); n != 0 || !ok {
		t.Fatalf("empty counted index: %d, %v", n, ok)
	}
	pi.AddKey(1, "i64:5")
	pi.removeKey(1, "i64:5")
	if n, ok := pi.RangeCardinality(1, 10, true, true); n != 0 || !ok {
		t.Fatalf("emptied counted index: %d, %v", n, ok)
	}
}

// PostingsBuild hands back each key's IDs sorted and distinct whatever the
// add order (shuffled, duplicated, empty key ignored), and an index filled
// from it in that order holds exact 512-ID chunks.
func TestPostingsBuildSortedDistinctExactChunks(t *testing.T) {
	b := NewPostingsBuild(false)
	rng := rand.New(rand.NewSource(3)) // #nosec G404 -- deterministic test input
	const n = 3 * orderedKeyChunk
	for _, i := range rng.Perm(n) {
		b.AddKey(snowflake.ID(i+1), "k")
		if i%7 == 0 {
			b.AddKey(snowflake.ID(i+1), "k") // duplicate
		}
	}
	b.AddKey(99, "")
	ci := NewCompositePropertyIndex([]string{"a", "b"})
	keys := 0
	b.Range(func(vk string, ids []snowflake.ID) bool {
		keys++
		if vk != "k" || len(ids) != n || !slices.IsSorted(ids) {
			t.Fatalf("key %q: %d IDs, sorted %v", vk, len(ids), slices.IsSorted(ids))
		}
		for _, id := range ids {
			ci.AddKey(id, vk)
		}
		return true
	})
	if keys != 1 {
		t.Fatalf("%d keys, want 1 (empty key recorded)", keys)
	}
	for i, c := range ci.many["k"].s.chunks {
		if len(c) != orderedKeyChunk || cap(c) != orderedKeyChunk {
			t.Fatalf("chunk %d: len %d cap %d, want exact %d", i, len(c), cap(c), orderedKeyChunk)
		}
	}
}

// A nil posting list (a value key nobody holds) reads as empty.
func TestIDPostingsNil(t *testing.T) {
	var p *IDPostings
	if p.Len() != 0 || p.Has(1) {
		t.Fatal("nil postings not empty")
	}
	if got := slices.Collect(p.After(0)); len(got) != 0 {
		t.Fatalf("nil After = %v", got)
	}
	if got := p.AppendAfter(nil, 0, 5); len(got) != 0 {
		t.Fatalf("nil AppendAfter = %v", got)
	}
}

// The ordered pages of the numeric and string views break ties by ID from the
// sorted buckets: a value held by many IDs pages in ID order across the cursor,
// in both directions, without the bucket being collected and sorted.
func TestOrderedPagesTieOrderFromSortedBuckets(t *testing.T) {
	pi := NewPropertyIndex()
	rng := rand.New(rand.NewSource(7)) // #nosec G404 -- deterministic test input
	ids := rng.Perm(3000)
	for _, i := range ids {
		id := snowflake.ID(i + 1)
		switch i % 3 {
		case 0:
			pi.AddKey(id, "i64:5")
		case 1:
			pi.AddKey(id, "f64:5") // same sort key, other type: one numeric bucket
		default:
			pi.AddKey(id, "s:cat")
		}
	}
	for _, desc := range []bool{false, true} {
		var got []snowflake.ID
		var cur RangeOrderedCursor
		for {
			page, next, done, ok := pi.RangeOrderedPage(5, 5, desc, cur, 7)
			if !ok {
				t.Fatal("unsupported")
			}
			got = append(got, page...)
			cur = next
			if done {
				break
			}
		}
		if len(got) != 2000 || !slices.IsSorted(got) {
			t.Fatalf("numeric desc=%v: %d IDs, sorted %v", desc, len(got), slices.IsSorted(got))
		}
		var sgot []snowflake.ID
		var scur StrOrderedCursor
		for {
			page, next, done, ok := pi.PrefixOrderedPage("ca", desc, scur, 11)
			if !ok {
				t.Fatal("unsupported")
			}
			sgot = append(sgot, page...)
			scur = next
			if done {
				break
			}
		}
		if len(sgot) != 1000 || !slices.IsSorted(sgot) {
			t.Fatalf("string desc=%v: %d IDs, sorted %v", desc, len(sgot), slices.IsSorted(sgot))
		}
	}
}
