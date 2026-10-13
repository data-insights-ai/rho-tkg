package badger

import (
	"fmt"
	"math"
	"math/rand"
	"slices"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// The packed table against a map oracle over random puts, overwrites,
// removes and ascending appends across many blocks: ascending runs, inserts
// in front of block 0, into full blocks (split), past a full block's end,
// removes that empty a block or shrink it, and the table emptied and refilled.
// After every phase the table must hold exactly the oracle (get, contains,
// Len, ascending IDs) and its blocks must stay sorted, non-empty, routed by
// their first ID (directory included) and within idTableBlock. IDs are dense
// or skewed like snowflakes (bursts of consecutive IDs, gaps growing by the
// cube of the burst number: the interpolation guess and the directory buckets
// are far off).
func TestIDTable_RandomizedAgainstMap(t *testing.T) {
	for seed := int64(1); seed <= 8; seed++ {
		skewed := seed%2 == 0
		t.Run(fmt.Sprintf("seed=%d/skewed=%v", seed, skewed), func(t *testing.T) {
			r := rand.New(rand.NewSource(seed))  // #nosec G404 -- deterministic test sequence
			idAt := func(x int64) snowflake.ID { // monotonic in x
				if !skewed {
					return snowflake.ID(x)
				}
				g := x / 64
				return snowflake.ID(1 + g*g*g*1_000_003 + g*64 + x%64)
			}
			var tab idTable[stampPair]
			oracle := map[snowflake.ID]stampEntry{}
			span := int64(1 + r.Intn(40_000))
			check := func(phase string) {
				t.Helper()
				verifyIDTable(t, phase, &tab, oracle)
				for k := 0; k < 300; k++ {
					id := idAt(1 + r.Int63n(span+2))
					top, v, ok := tab.get(id)
					want, wok := oracle[id]
					if ok != wok || tab.contains(id) != wok || (ok && (top != want.top || v.from != want.from || v.to != want.to)) {
						t.Fatalf("%s: get(%d) = %d %+v %v, oracle %+v %v", phase, id, top, v, ok, want, wok)
					}
				}
			}
			put := func(id snowflake.ID) {
				e := stampEntry{from: types.Instant(r.Int63()), to: types.Instant(r.Int63()), top: r.Uint32()}
				tab.put(id, e.top, stampPair{from: e.from, to: e.to})
				oracle[id] = e
			}
			// Ascending build, as a scan streams it.
			for x := int64(1); x <= span; x += 1 + r.Int63n(3) {
				id := idAt(x)
				e := stampEntry{from: types.Instant(id), to: types.Instant(id + 1), top: uint32(x)}
				tab.appendAscending(id, e.top, stampPair{from: e.from, to: e.to})
				oracle[id] = e
			}
			check("built")
			for round := 0; round < 4; round++ {
				for k := 0; k < 3000; k++ {
					switch op := r.Intn(10); {
					case op < 5:
						put(idAt(1 + r.Int63n(span+2)))
					case op < 6:
						put(idAt(span + 3 + int64(k))) // ascending past the end
					case op < 7 && len(oracle) > 0:
						tab.appendAscending(idAt(1), 9, stampPair{}) // not above the last: an insert
						oracle[idAt(1)] = stampEntry{top: 9}
					default:
						id := idAt(1 + r.Int63n(span+2))
						_, wok := oracle[id]
						if got := tab.remove(id); got != wok {
							t.Fatalf("round %d: remove(%d) = %v, oracle has it %v", round, id, got, wok)
						}
						delete(oracle, id)
					}
				}
				check(fmt.Sprintf("round %d", round))
			}
			// Remove everything in random order, then refill.
			ids := tab.appendIDs(nil)
			r.Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })
			for i, id := range ids {
				if !tab.remove(id) {
					t.Fatalf("remove(%d) of a present ID = false", id)
				}
				delete(oracle, id)
				if i == len(ids)/2 {
					check("half removed")
				}
			}
			check("emptied")
			if tab.remove(7) || len(tab.blocks) != 0 {
				t.Fatalf("emptied table: remove = true or %d blocks left", len(tab.blocks))
			}
			for k := 0; k < 2000; k++ {
				put(idAt(1 + r.Int63n(span+2)))
			}
			put(math.MaxInt64) // the extremes of the ID range
			put(1)
			check("extremes")
			check("refilled")
		})
	}
}

// An insert into a full block at every boundary position (front, either side
// of the split point, the last slot, past the end with and without room in
// the next block) lands in order and leaves valid blocks.
func TestIDTable_InsertIntoFullBlockAtEveryBoundary(t *testing.T) {
	const half = idTableBlock / 2
	for _, pos := range []int{0, 1, half - 1, half, half + 1, half + 2, idTableBlock - 1, idTableBlock} {
		for _, nextFull := range []bool{false, true} {
			t.Run(fmt.Sprintf("pos=%d/nextFull=%v", pos, nextFull), func(t *testing.T) {
				var tab idTable[stampPair]
				oracle := map[snowflake.ID]stampEntry{}
				add := func(id snowflake.ID) {
					tab.appendAscending(id, uint32(id), stampPair{from: types.Instant(id)})
					oracle[id] = stampEntry{from: types.Instant(id), top: uint32(id)}
				}
				// Block 0: IDs 10, 20, ..., full; block 1 directory buckets
				// above it (an insert at its front moves its first ID down
				// across buckets), then enough full blocks that the directory
				// routes.
				for k := 1; k <= idTableBlock; k++ {
					add(snowflake.ID(10 * k))
				}
				for k := 0; k < idTableBlock; k++ {
					add(snowflake.ID(500_000_000 + k))
				}
				for k := 0; k < idDirMinBlocks*idTableBlock; k++ {
					add(snowflake.ID(1_000_000_000 + 1_000*k))
				}
				if !nextFull { // room in block 1
					for k := half; k < idTableBlock; k++ {
						tab.remove(snowflake.ID(500_000_000 + k))
						delete(oracle, snowflake.ID(500_000_000+k))
					}
				}
				verifyIDTable(t, "before insert", &tab, oracle)
				id := snowflake.ID(10*pos + 5) // after the pos-th ID of block 0
				tab.put(id, 1, stampPair{from: 2, to: 3})
				oracle[id] = stampEntry{from: 2, to: 3, top: 1}
				verifyIDTable(t, "after insert", &tab, oracle)
			})
		}
	}
}

// BenchmarkIDTableGet is the table's lookup beside the Go map it replaced,
// over 10 K / 100 K / 1 M IDs, probing 4096 IDs spread over the table (hit)
// and their neighbours (miss). IDs are evenly spaced, or snowflake-like
// (bursts of 1-512 IDs minted in one millisecond, exponential gaps of about a
// second between bursts, one gap in a hundred an hour long: the
// interpolation guess and the directory buckets meet skew).
//
//	go test ./pkg/graph/store/badger/ -run '^$' -bench BenchmarkIDTableGet
func BenchmarkIDTableGet(b *testing.B) {
	for _, n := range []int{10_000, 100_000, 1_000_000} {
		for _, shape := range []string{"even", "snowflake"} {
			ids := sidecarRAMIDs(n)
			if shape == "snowflake" {
				ids = snowflakeLikeIDs(n)
			}
			var tab idTable[stampPair]
			m := make(map[snowflake.ID]stampEntry, n)
			for _, id := range ids {
				tab.appendAscending(snowflake.ID(id), 1, stampPair{from: 1})
				m[snowflake.ID(id)] = stampEntry{from: 1, top: 1}
			}
			probe := make([]snowflake.ID, 4096)
			for i := range probe {
				probe[i] = snowflake.ID(ids[(i*7919)%n])
			}
			for _, miss := range []snowflake.ID{0, 1} {
				name := fmt.Sprintf("ids=%d/%s/hit", n, shape)
				if miss == 1 {
					name = fmt.Sprintf("ids=%d/%s/miss", n, shape)
				}
				b.Run(name+"/table", func(b *testing.B) {
					found := 0
					for i := 0; i < b.N; i++ {
						if _, _, ok := tab.get(probe[i&4095] + miss); ok {
							found++
						}
					}
					benchSink = found
				})
				b.Run(name+"/map", func(b *testing.B) {
					found := 0
					for i := 0; i < b.N; i++ {
						if _, ok := m[probe[i&4095]+miss]; ok {
							found++
						}
					}
					benchSink = found
				})
			}
		}
	}
}

var benchSink int

// snowflakeLikeIDs returns n ascending IDs shaped like minted snowflakes.
func snowflakeLikeIDs(n int) []int64 {
	r := rand.New(rand.NewSource(41)) // #nosec G404 -- deterministic benchmark data
	ids := make([]int64, 0, n)
	ms := int64(1 << 20)
	for len(ids) < n {
		gap := int64(r.ExpFloat64() * 1000)
		if r.Intn(100) == 0 {
			gap *= 3600 // an idle stretch
		}
		ms += 1 + gap
		burst := 1 + r.Intn(512)
		for s := 0; s < burst && len(ids) < n; s++ {
			ids = append(ids, ms<<22|int64(s))
		}
	}
	return ids
}

func verifyIDTable(t *testing.T, phase string, tab *idTable[stampPair], oracle map[snowflake.ID]stampEntry) {
	t.Helper()
	if tab.Len() != len(oracle) {
		t.Fatalf("%s: Len = %d, oracle %d", phase, tab.Len(), len(oracle))
	}
	want := make([]snowflake.ID, 0, len(oracle))
	for id := range oracle {
		want = append(want, id)
	}
	slices.Sort(want)
	if got := tab.appendIDs(nil); !slices.Equal(got, want) {
		t.Fatalf("%s: ascending IDs differ from the oracle (%d vs %d)", phase, len(got), len(want))
	}
	if len(tab.firsts) != len(tab.blocks) {
		t.Fatalf("%s: %d firsts for %d blocks", phase, len(tab.firsts), len(tab.blocks))
	}
	for i, b := range tab.blocks {
		if len(b.ids) == 0 || len(b.ids) > idTableBlock || cap(b.ids) > idTableBlock || len(b.tops) != len(b.ids) || len(b.vals) != len(b.ids) {
			t.Fatalf("%s: block %d len %d cap %d tops %d vals %d", phase, i, len(b.ids), cap(b.ids), len(b.tops), len(b.vals))
		}
		if tab.firsts[i] != b.ids[0] {
			t.Fatalf("%s: firsts[%d] = %d, block starts at %d", phase, i, tab.firsts[i], b.ids[0])
		}
		if got := tab.blockFor(b.ids[0] - 1); i > 0 && got != i-1 {
			t.Fatalf("%s: the ID below block %d's first routes to block %d", phase, i, got)
		}
		for j := range b.keys {
			want := snowflake.ID(math.MaxInt64)
			if k := j * idSegment; k < len(b.ids) {
				want = b.ids[k]
			}
			if b.keys[j] != want {
				t.Fatalf("%s: block %d summary[%d] = %d, want %d", phase, i, j, b.keys[j], want)
			}
		}
		for j, id := range b.ids {
			if got := tab.blockFor(id); got != i {
				t.Fatalf("%s: ID %d of block %d routes to block %d", phase, id, i, got)
			}
			e := oracle[id]
			if b.tops[j] != e.top || b.vals[j].from != e.from || b.vals[j].to != e.to {
				t.Fatalf("%s: block %d holds %d = %d %+v, oracle %+v", phase, i, id, b.tops[j], b.vals[j], e)
			}
		}
	}
}
