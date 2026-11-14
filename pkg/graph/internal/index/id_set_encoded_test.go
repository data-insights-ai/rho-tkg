package index

import (
	"encoding/binary"
	"math"
	"math/rand/v2"
	"slices"
	"testing"
)

// encodeIDs is the image's encoding of ascending ids: uvarint gaps, the first
// from 0.
func encodeIDs(ids []int64) []byte {
	var b []byte
	prev := int64(0)
	for _, id := range ids {
		b = binary.AppendUvarint(b, uint64(id-prev))
		prev = id
	}
	return b
}

// idPattern returns n ascending positive IDs with gaps drawn by gap.
func idPattern(n int, first int64, gap func(i int) int64) []int64 {
	ids := make([]int64, n)
	id := first
	for i := range ids {
		if i > 0 {
			id += gap(i)
		}
		ids[i] = id
	}
	return ids
}

func collect(s *IDSet) []int64 {
	var out []int64
	s.Ascend(func(id int64) bool { out = append(out, id); return true })
	return out
}

// TestIDSetEncodedMatchesChunks: a set wrapped over the image's encoding answers
// Has, Len, Ascend, All and Clone as the set built by Add does, for every size
// around an anchor and a chunk boundary and for dense, sparse and huge gaps,
// probing each member, both of its neighbours, the ends of the ID range and
// random IDs; and the first Add or Remove builds the chunks and the set keeps
// answering as the reference does through a run of writes. Catches an anchor at
// the wrong offset, a probe that stops a gap early or late, a Len that
// forgets the stream and a write that loses an ID.
func TestIDSetEncodedMatchesChunks(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewPCG(1, 2))
	patterns := map[string]func(i int) int64{
		"dense":  func(int) int64 { return 1 },
		"random": func(int) int64 { return 1 + rng.Int64N(1000) },
		"huge":   func(int) int64 { return 1 + rng.Int64N(1<<40) },
		"mixed": func(i int) int64 {
			if i%97 == 0 {
				return 1 << 33
			}
			return 1 + rng.Int64N(5)
		},
	}
	sizes := []int{1, 2, 3, 63, 64, 65, 127, 128, 129, 511, 512, 513, 1000, 5000}
	for name, gap := range patterns {
		for _, n := range sizes {
			first := int64(1)
			if name == "huge" {
				first = 1 << 50
			}
			ids := idPattern(n, first, gap)
			set, used, err := NewIDSetEncoded(append(encodeIDs(ids), 0xEE, 0xEE), n) // bytes after the set are not its own
			if err != nil {
				t.Fatalf("%s/%d: %v", name, n, err)
			}
			if want := len(encodeIDs(ids)); used != want {
				t.Fatalf("%s/%d: used %d bytes, want %d", name, n, used, want)
			}
			if !set.Encoded() || set.Len() != n {
				t.Fatalf("%s/%d: encoded %v, Len %d", name, n, set.Encoded(), set.Len())
			}
			member := make(map[int64]bool, n)
			for _, id := range ids {
				member[id] = true
			}
			probe := func(id int64) {
				t.Helper()
				if got := set.Has(id); got != member[id] {
					t.Fatalf("%s/%d: Has(%d) = %v, want %v", name, n, id, got, member[id])
				}
			}
			for _, id := range ids {
				probe(id)
				probe(id - 1)
				probe(id + 1)
			}
			for _, id := range []int64{math.MinInt64, -1, 0, 1, ids[0] - 1, ids[n-1] + 1, math.MaxInt64} {
				probe(id)
			}
			for range 300 {
				probe(1 + rng.Int64N(ids[n-1]+10))
			}
			if got := collect(&set); !slices.Equal(got, ids) {
				t.Fatalf("%s/%d: Ascend gave %d IDs, not the %d stored", name, n, len(got), n)
			}
			var seq []int64
			for id := range set.All() {
				seq = append(seq, id)
			}
			if !slices.Equal(seq, ids) {
				t.Fatalf("%s/%d: All differs from the stored IDs", name, n)
			}
			stopAfter, seen := n/2+1, 0
			set.Ascend(func(int64) bool { seen++; return seen < stopAfter })
			if seen != stopAfter {
				t.Fatalf("%s/%d: Ascend visited %d IDs after fn stopped it at %d", name, n, seen, stopAfter)
			}
			clone := set.Clone()
			if !set.Encoded() || clone.Encoded() || !slices.Equal(collect(&clone), ids) {
				t.Fatalf("%s/%d: Clone encoded %v (source %v) or differs", name, n, clone.Encoded(), set.Encoded())
			}

			// Writes build the chunks and answer as a reference set does.
			ref := NewIDSetFrom(ids)
			for range 40 {
				var id int64
				switch rng.IntN(3) {
				case 0:
					id = ids[rng.IntN(n)] // a member
				case 1:
					id = ids[n-1] + 1 + rng.Int64N(100) // above all
				default:
					id = 1 + rng.Int64N(ids[n-1]) // anywhere
				}
				if rng.IntN(2) == 0 {
					if got, want := set.Add(id), ref.Add(id); got != want {
						t.Fatalf("%s/%d: Add(%d) = %v, reference %v", name, n, id, got, want)
					}
				} else if got, want := set.Remove(id), ref.Remove(id); got != want {
					t.Fatalf("%s/%d: Remove(%d) = %v, reference %v", name, n, id, got, want)
				}
				if set.Encoded() {
					t.Fatalf("%s/%d: still encoded after a write", name, n)
				}
				if set.Len() != ref.Len() || !slices.Equal(collect(&set), collect(&ref)) {
					t.Fatalf("%s/%d: after a write the set has %d IDs, reference %d", name, n, set.Len(), ref.Len())
				}
			}
		}
	}
}

// TestIDSetEncodedRefusesWhatADecodeRefuses: NewIDSetEncoded refuses a zero gap,
// an overflowing one, a truncated varint, a count beyond the bytes and a
// negative count, and takes n = 0 as the empty set with no bytes used.
func TestIDSetEncodedRefusesWhatADecodeRefuses(t *testing.T) {
	t.Parallel()
	gaps := func(gs ...uint64) []byte {
		var b []byte
		for _, g := range gs {
			b = binary.AppendUvarint(b, g)
		}
		return b
	}
	for name, c := range map[string]struct {
		b []byte
		n int
	}{
		"zero gap":         {gaps(5, 0), 2},
		"first gap zero":   {gaps(0, 5), 2},
		"overflow":         {gaps(1<<62, 1<<63-1), 2},
		"truncated":        {[]byte{1, 0x80}, 2},
		"count beyond":     {gaps(5, 3), 3},
		"count beyond 1e9": {gaps(5), 1 << 30},
		"negative count":   {gaps(5), -1},
		"gap above int64":  {gaps(1 << 63), 1},
	} {
		if _, _, err := NewIDSetEncoded(c.b, c.n); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	set, used, err := NewIDSetEncoded([]byte{9, 9}, 0)
	if err != nil || used != 0 || set.Encoded() || set.Len() != 0 || set.Has(9) {
		t.Fatalf("n = 0: %v, used %d, encoded %v, Len %d", err, used, set.Encoded(), set.Len())
	}
	if _, _, err := NewIDSetEncoded(gaps(1<<62, 1<<62-1), 2); err != nil {
		t.Errorf("the largest ID (2^63-1) refused: %v", err)
	}
}

// TestIDSetEncodedAnchorsAreSparse: the anchors cost a sixteenth of an ID's
// bytes per ID or less, so an encoded set stays far under the 8 B per ID of the
// chunks (the bytes it keeps are the image's own).
func TestIDSetEncodedAnchorsAreSparse(t *testing.T) {
	t.Parallel()
	ids := idPattern(50000, 1, func(int) int64 { return 1 })
	set, _, err := NewIDSetEncoded(encodeIDs(ids), len(ids))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(set.enc.anchors), (len(ids)+idAnchorStride-1)/idAnchorStride; got != want {
		t.Fatalf("%d anchors for %d IDs, want %d", got, len(ids), want)
	}
}
