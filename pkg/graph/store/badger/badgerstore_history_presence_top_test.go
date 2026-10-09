package badger

import (
	"math"
	"math/rand/v2"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Unit tests for the per-ID top version of the history presence set. Written
// with the code that adds the field (they reference it, so they could not run
// red first); the behavioural red tests are TestBulkAsOfPresence_* and the
// mutants in tasks/evidence/bulk-asof-presence/mutants-2.txt cover each rule.

func newPresenceForTest() *historyPresence {
	return &historyPresence{tracking: true, has: map[snowflake.ID]uint32{}, unknown: map[snowflake.ID]uint64{}}
}

func TestHistoryPresenceNoteTop(t *testing.T) {
	t.Parallel()
	const id = snowflake.ID(42)
	type op struct {
		version uint64
		del     bool
	}
	cases := []struct {
		name        string
		ops         []op
		wantTop     uint32
		wantHas     bool
		wantUnknown bool
	}{
		{"first set", []op{{3, false}}, 3, true, false},
		{"set raises", []op{{3, false}, {7, false}}, 7, true, false},
		{"lower set keeps", []op{{7, false}, {3, false}}, 7, true, false},
		{"equal set", []op{{5, false}, {5, false}}, 5, true, false},
		{"set version zero", []op{{0, false}}, 0, true, false},
		{"delete leaves unknown", []op{{7, false}, {7, true}}, 0, false, true},
		{"set into unknown stays unknown", []op{{7, false}, {7, true}, {2, false}}, 0, false, true},
		{"delete of unseen id", []op{{7, true}}, 0, false, true},
		{"huge version is unbounded", []op{{math.MaxUint32, false}}, topUnbounded, true, false},
		{"above uint32 is unbounded", []op{{math.MaxUint32 + 10, false}}, topUnbounded, true, false},
		{"unbounded stays", []op{{math.MaxUint32, false}, {1, false}}, topUnbounded, true, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := newPresenceForTest()
			for _, o := range tc.ops {
				p.note(id, o.version, o.del)
			}
			top, has := p.has[id]
			_, unknown := p.unknown[id]
			if has != tc.wantHas || unknown != tc.wantUnknown || (has && top != tc.wantTop) {
				t.Fatalf("has=%v top=%d unknown=%v; want has=%v top=%d unknown=%v", has, top, unknown, tc.wantHas, tc.wantTop, tc.wantUnknown)
			}
		})
	}
}

// A SET into an unknown ID must move the stamp so a probe in flight (which read
// the stamp before and the rows before the SET) does not install its top.
func TestHistoryPresenceSetIntoUnknownMovesStamp(t *testing.T) {
	t.Parallel()
	p := newPresenceForTest()
	const id = snowflake.ID(7)
	p.note(id, 4, true)
	before := p.unknown[id]
	p.note(id, 2, false)
	if after := p.unknown[id]; after == 0 || after == before {
		t.Fatalf("stamp %d -> %d: an in-flight probe could install a stale top", before, after)
	}
}

func TestBulkPresenceBound(t *testing.T) {
	t.Parallel()
	p := newPresenceForTest()
	p.has[1] = 5
	p.has[2] = topUnbounded
	p.unknown[3] = 1
	p.built.Store(true)
	bs := &Store{}
	b := bs.beginBulkPresence(p)
	for _, tc := range []struct {
		id    snowflake.ID
		limit int64
		ok    bool
	}{{1, 5, true}, {2, 0, false}, {3, 0, false}, {4, -1, true}} {
		if limit, ok := b.bound(tc.id); limit != tc.limit || ok != tc.ok {
			t.Fatalf("bound(%d) = (%d, %v), want (%d, %v)", tc.id, limit, ok, tc.limit, tc.ok)
		}
	}
	p.note(1, 9, true) // a delete since the scan began withdraws every answer
	if _, ok := b.bound(4); ok {
		t.Fatal("bound answered after a history delete since the scan began")
	}
	var zero bulkPresence
	if _, ok := zero.bound(4); ok {
		t.Fatal("the zero bulkPresence answered")
	}
	unbuilt := newPresenceForTest()
	if z := bs.beginBulkPresence(unbuilt); z.p != nil {
		t.Fatal("an unbuilt set was used")
	}
	bs.historyPresenceProbeOnly = true
	if z := bs.beginBulkPresence(p); z.p != nil {
		t.Fatal("a probe-only store used the set")
	}
}

// scanHistoryTops and probeHistoryPresence(wantTop) against the rows
// (GetNodeHistory / GetRelHistory) over randomized data: committed, pending,
// trimmed in the buffer and on disk.
func TestScanHistoryTopsAndProbeMatchHistory(t *testing.T) {
	t.Parallel()
	for _, k := range bulkPresenceKinds() {
		t.Run(k.name, func(t *testing.T) {
			t.Parallel()
			rng := rand.New(rand.NewPCG(0x70B, 0x51))
			bs := openBulkPresenceStore(t, t.TempDir(), false)
			t.Cleanup(func() { _ = bs.Close() })
			putBulkPresenceEndpoints(t, bs)
			kindByte := storepkg.KeyHistNode
			if k.name == "rel" {
				kindByte = storepkg.KeyHistRel
			}
			const n = 120
			ids := make([]int64, n)
			for e := range ids {
				ids[e] = k.id(e)
				rows := rng.IntN(6) // 0 rows: no history; 1 row: single-row path
				if e%9 == 0 {
					rows = historyForwardRows + 1 + rng.IntN(8) // deeper than the forward walk: reverse-seek path
				}
				for v := 0; v < rows; v++ {
					k.put(t, bs, ids[e], uint32(v), types.Instant(10+v), false)
				}
			}
			if err := bs.Flush(); err != nil {
				t.Fatalf("Flush: %v", err)
			}
			for e, id := range ids {
				switch rng.IntN(5) {
				case 0: // pending rows above the committed ones (the highest version may be pending)
					k.put(t, bs, id, 7+uint32(rng.IntN(3)), 50, false)
				case 1: // pending trim (masks committed keys, may empty the ID)
					if err := k.trim(bs, id, uint32(rng.IntN(4))); err != nil {
						t.Fatalf("trim %d: %v", e, err)
					}
				}
			}
			// Pass 0 sees pending ops in the buffer, pass 1 after they are committed.
			for pass := 0; pass < 2; pass++ {
				want := map[snowflake.ID]uint32{}
				for _, id := range ids {
					if top, ok := historyTopFromRows(t, k, bs, id); ok {
						want[snowflake.ID(id)] = top
					}
				}
				tops, err := bs.scanHistoryTops(kindByte)
				if err != nil {
					t.Fatalf("scan: %v", err)
				}
				got := map[snowflake.ID]uint32{}
				for _, e := range tops {
					if _, dup := got[e.id]; dup {
						t.Fatalf("pass %d: ID %d scanned twice", pass, e.id)
					}
					got[e.id] = e.top
				}
				if len(got) != len(want) {
					t.Fatalf("pass %d: scan returned %d IDs, rows say %d", pass, len(got), len(want))
				}
				for id, top := range want {
					if got[id] != top {
						t.Fatalf("pass %d: scan top of %d = %d, rows say %d", pass, id, got[id], top)
					}
					present, ptop, err := bs.probeHistoryPresence(kindByte, id, true)
					if err != nil || !present || ptop != top {
						t.Fatalf("pass %d: probe(%d) = (%v, %d, %v), rows say top %d", pass, id, present, ptop, err, top)
					}
				}
				for _, id := range ids {
					if _, ok := want[snowflake.ID(id)]; ok {
						continue
					}
					if present, _, err := bs.probeHistoryPresence(kindByte, snowflake.ID(id), true); err != nil || present {
						t.Fatalf("pass %d: probe(%d) = (%v, %v) for an ID without rows", pass, id, present, err)
					}
				}
				if err := bs.Flush(); err != nil {
					t.Fatalf("Flush: %v", err)
				}
			}
		})
	}
}

func historyTopFromRows(t *testing.T, k bulkPresenceKind, bs *Store, id int64) (uint32, bool) {
	t.Helper()
	var top uint32
	var n int
	if k.name == "node" {
		hist, err := bs.GetNodeHistory(types.NodeID(snowflake.ID(id)))
		if err != nil {
			t.Fatalf("history: %v", err)
		}
		for _, h := range hist {
			top, n = max(top, h.Version()), n+1
		}
	} else {
		hist, err := bs.GetRelHistory(types.RelID(snowflake.ID(id)))
		if err != nil {
			t.Fatalf("history: %v", err)
		}
		for _, h := range hist {
			top, n = max(top, h.Version()), n+1
		}
	}
	return top, n > 0
}
