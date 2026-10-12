package index

import (
	"math/rand/v2"
	"os"
	"runtime"
	"slices"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestKeyOrderedIDs_MatchesSortedReference — random inserts (duplicates
// included) and removes (absent ones included) across many chunk splits and
// merges; after each step ascendFrom at random thresholds equals the sorted
// reference filtered, and fn's stop is honoured.
func TestKeyOrderedIDs_MatchesSortedReference(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewPCG(51, 51))
	var s keyOrderedIDs
	ref := map[veKey]bool{}
	for step := 0; step < 20_000; step++ {
		x := veKey{k: rng.Int64N(400) - 200, id: rng.Int64N(50)}
		if rng.IntN(3) == 0 {
			s.remove(x)
			delete(ref, x)
		} else {
			s.insert(x)
			ref[x] = true
		}
		if step%97 != 0 {
			continue
		}
		from := rng.Int64N(500) - 250
		var want []veKey
		for k := range ref {
			if k.k >= from {
				want = append(want, k)
			}
		}
		slices.SortFunc(want, cmpVEKey)
		var got []veKey
		s.ascendFrom(from, func(k veKey) bool { got = append(got, k); return true })
		if !slices.Equal(got, want) || s.len() != len(ref) {
			t.Fatalf("step %d from %d: got %d entries, want %d (len %d vs %d)", step, from, len(got), len(want), s.len(), len(ref))
		}
		if len(want) > 1 {
			n := 0
			s.ascendFrom(from, func(veKey) bool { n++; return false })
			if n != 1 {
				t.Fatalf("stop ignored: %d calls", n)
			}
		}
	}
}

const (
	open = storepkg.ValidEndOpen
	none = storepkg.ValidEndNone
)

func cur(txFrom, validTo, deletedAt types.Instant, toks ...uint16) ValidEndRow {
	return ValidEndRow{TxFrom: txFrom, ValidTo: validTo, DeletedAt: deletedAt, Tokens: toks}
}

func hist(v uint32, txFrom, txTo, validTo, deletedAt types.Instant, toks ...uint16) ValidEndRow {
	return ValidEndRow{Hist: true, Version: v, TxFrom: txFrom, TxTo: txTo, ValidTo: validTo, DeletedAt: deletedAt, Tokens: toks}
}

func builtIndex() *ValidEndIndex {
	ix := &ValidEndIndex{}
	ix.BeginBuild()
	ix.EndBuild()
	return ix
}

// state returns id's bound and stamp, or unknown.
func state(ix *ValidEndIndex, id int64) (bound, stamp int64, unknown bool) {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	if _, u := ix.unknown[id]; u {
		return 0, 0, true
	}
	e := ix.ents[id]
	return e.bound, e.stamp, false
}

// TestValidEndIndex_Fold — the bound each row sequence folds to, written as
// the lifecycle that produces it. The delete cap applies only when every
// earlier stamp is at or below the delete (out of order: the bound stays, a
// looser but sound answer); a later life reopens it.
func TestValidEndIndex_Fold(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		name   string
		writes []ValidEndRow // the notes, in write order
		stored []ValidEndRow // the rows left (a delete removes the current row)
		bound  int64
		stamp  int64
	}{
		{"untimed live", []ValidEndRow{cur(10, 0, 0, 1)}, []ValidEndRow{cur(10, 0, 0, 1)}, int64(open), 10},
		{"closed (own ValidTo)", []ValidEndRow{cur(10, 50, 0, 1)}, []ValidEndRow{cur(10, 50, 0, 1)}, 50, 10},
		{"deleted in order",
			[]ValidEndRow{cur(10, 0, 0, 1), hist(0, 10, 30, 30, 30, 1)},
			[]ValidEndRow{hist(0, 10, 30, 30, 30, 1)}, 30, 30},
		{"updated then deleted",
			[]ValidEndRow{cur(10, 0, 0, 1), hist(0, 10, 20, 0, 0, 1), cur(20, 0, 0, 1), hist(1, 20, 30, 0, 30, 1)},
			[]ValidEndRow{hist(0, 10, 20, 0, 0, 1), hist(1, 20, 30, 0, 30, 1)}, 30, 30},
		{"delete noted after a later row (out of order): no cap",
			[]ValidEndRow{cur(40, 0, 0, 1), hist(0, 10, 30, 0, 30, 1)},
			[]ValidEndRow{hist(0, 10, 30, 0, 30, 1), cur(40, 0, 0, 1)}, int64(open), 40},
		{"tombstone recorded after its delete: no cap",
			[]ValidEndRow{hist(0, 35, 0, 0, 30, 1)},
			[]ValidEndRow{hist(0, 35, 0, 0, 30, 1)}, int64(open), 35},
		{"re-imported after a delete",
			[]ValidEndRow{cur(10, 0, 0, 1), hist(0, 10, 30, 0, 30, 1), cur(40, 0, 0, 1)},
			[]ValidEndRow{hist(0, 10, 30, 0, 30, 1), cur(40, 0, 0, 1)}, int64(open), 40},
		{"closed then deleted later: the earlier end stays",
			[]ValidEndRow{cur(10, 50, 0, 1), hist(0, 10, 90, 50, 90, 1)},
			[]ValidEndRow{hist(0, 10, 90, 50, 90, 1)}, 50, 90},
		{"second life deleted",
			[]ValidEndRow{hist(0, 10, 30, 0, 30, 1), cur(40, 0, 0, 1), hist(1, 40, 60, 0, 60, 1)},
			[]ValidEndRow{hist(0, 10, 30, 0, 30, 1), hist(1, 40, 60, 0, 60, 1)}, 60, 60},
	} {
		ix := builtIndex()
		for _, r := range c.writes {
			ix.NoteRow(7, r)
		}
		bound, stamp, unknown := state(ix, 7)
		if unknown || bound != c.bound || stamp != c.stamp {
			t.Errorf("%s: notes fold to bound %d stamp %d unknown %v, want bound %d stamp %d", c.name, bound, stamp, unknown, c.bound, c.stamp)
		}
		// Install (a build or a re-read) folds the stored rows, history by
		// version then the current row, to the same answer.
		ix2 := builtIndex()
		ix2.Install(7, c.stored)
		if b2, s2, _ := state(ix2, 7); b2 != c.bound || s2 != c.stamp {
			t.Errorf("%s: Install folds to bound %d stamp %d, want %d %d", c.name, b2, s2, c.bound, c.stamp)
		}
	}
}

// TestValidEndIndex_UnknownRules — the writes the fold cannot absorb move the
// entity to unknown (listed for every token, never refined) until a re-read:
// an overwrite of a history version at or below the highest tombstone, a
// current row replaced or removed while it carried a delete, any write after a
// Refine, any write during a build. A removal or overwrite that cannot have
// dropped a tombstone keeps the fold (and advances the generation).
func TestValidEndIndex_UnknownRules(t *testing.T) {
	t.Parallel()
	deleted := []ValidEndRow{cur(10, 0, 0, 1), hist(0, 10, 20, 0, 0, 1), cur(20, 0, 0, 1), hist(1, 20, 30, 0, 30, 1)}
	for _, c := range []struct {
		name    string
		setup   []ValidEndRow
		act     func(ix *ValidEndIndex)
		unknown bool
	}{
		{"history overwrite at the tombstone version", deleted, func(ix *ValidEndIndex) { ix.NoteRow(7, hist(1, 20, 0, 0, 0, 1)) }, true},
		{"history overwrite below the tombstone version", deleted, func(ix *ValidEndIndex) { ix.NoteRow(7, hist(0, 10, 20, 0, 0, 1)) }, true},
		{"history write above the tombstone version", deleted, func(ix *ValidEndIndex) { ix.NoteRow(7, hist(2, 40, 0, 0, 0, 1)) }, false},
		{"history removed", deleted, func(ix *ValidEndIndex) { ix.MarkUnknown(7) }, true},
		{"current removed without a delete", []ValidEndRow{cur(10, 0, 0, 1)}, func(ix *ValidEndIndex) { ix.NoteCurrentDelete(7) }, false},
		{"current carrying a delete removed", []ValidEndRow{cur(10, 0, 20, 1)}, func(ix *ValidEndIndex) { ix.NoteCurrentDelete(7) }, true},
		{"current carrying a delete replaced", []ValidEndRow{cur(10, 0, 20, 1)}, func(ix *ValidEndIndex) { ix.NoteRow(7, cur(30, 0, 0, 1)) }, true},
		{"write after a refine", []ValidEndRow{cur(10, 0, 0, 1), hist(0, 10, 20, 0, 0, 1), cur(20, 30, 0, 1)}, func(ix *ValidEndIndex) {
			ix.Refine(7, gen(ix, 7), 30)
			ix.NoteRow(7, hist(1, 20, 40, 30, 0, 1))
		}, true},
	} {
		ix := builtIndex()
		for _, r := range c.setup {
			ix.NoteRow(7, r)
		}
		g0 := gen(ix, 7)
		c.act(ix)
		_, _, unknown := state(ix, 7)
		if unknown != c.unknown {
			t.Errorf("%s: unknown %v, want %v", c.name, unknown, c.unknown)
		}
		if !unknown && gen(ix, 7) == g0 {
			t.Errorf("%s: generation did not advance", c.name)
		}
		if unknown {
			got := ix.Candidates([]uint16{99}, 1000, 1000) // a token it never had
			if len(got) != 1 || got[0].ID != 7 || got[0].Stamp != open || got[0].Bound != open {
				t.Errorf("%s: unknown entity not listed for every token, unrefinable: %+v", c.name, got)
			}
		}
	}
	// During a build every write moves its entity to unknown, and the scan
	// then skips it.
	ix := &ValidEndIndex{}
	ix.BeginBuild()
	ix.NoteRow(7, cur(10, 0, 0, 1))
	ix.BuildRow(7, cur(5, 0, 0, 1))
	ix.BuildRow(8, cur(5, 0, 0, 1))
	ix.EndBuild()
	if _, _, unknown := state(ix, 7); !unknown {
		t.Error("write during the build: not unknown")
	}
	if b, _, unknown := state(ix, 8); unknown || b != int64(open) {
		t.Errorf("scanned entity: bound %d unknown %v", b, unknown)
	}
}

func gen(ix *ValidEndIndex, id int64) uint64 {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	return uint64(ix.ents[id].gen)
}

func candIDs(cs []storepkg.ValidEndCandidate) []int64 {
	var ids []int64
	for _, c := range cs {
		ids = append(ids, c.ID)
	}
	slices.Sort(ids)
	return ids
}

// TestValidEndIndex_Candidates — per token: the entities whose bound is at or
// above validFrom (the bound itself included) or whose stamp is above pin;
// each once (a node with several queried labels); a label acquired later
// lists the node under it; tokens never carried list nothing.
func TestValidEndIndex_Candidates(t *testing.T) {
	t.Parallel()
	ix := builtIndex()
	ix.NoteRow(1, cur(10, 0, 0, 5))          // open
	ix.NoteRow(2, cur(10, 0, 0, 5))          //
	ix.NoteRow(2, hist(0, 10, 30, 0, 30, 5)) // deleted at 30
	ix.NoteRow(3, cur(10, 50, 0, 5, 6))      // closed at 50, two labels
	ix.NoteRow(4, cur(80, 20, 0, 5))         // closed at 20, written at 80
	ix.NoteRow(5, cur(10, 0, 0, 6))          // other token
	ix.NoteRow(6, cur(10, 0, 0, 6))          //
	ix.NoteRow(6, cur(15, 0, 0, 5, 6))       // acquires 5 later
	for _, c := range []struct {
		toks      []uint16
		pin, from types.Instant
		want      []int64
	}{
		{[]uint16{5}, 100, 0, []int64{1, 2, 3, 4, 6}},
		{[]uint16{5}, 100, 30, []int64{1, 2, 3, 6}},
		{[]uint16{5}, 100, 31, []int64{1, 3, 6}},
		{[]uint16{5}, 100, 51, []int64{1, 6}},
		{[]uint16{5}, 79, 51, []int64{1, 4, 6}}, // 4 written after the pin
		{[]uint16{5, 6}, 100, 40, []int64{1, 3, 5, 6}},
		{[]uint16{9}, 100, 0, nil},
	} {
		if got := candIDs(ix.Candidates(c.toks, c.pin, c.from)); !slices.Equal(got, c.want) {
			t.Errorf("toks %v pin %d from %d: %v, want %v", c.toks, c.pin, c.from, got, c.want)
		}
	}
	// Refine: a stale generation, a raise and an unknown entity are ignored;
	// a lower exact end drops the entity from the suffix.
	ix.Refine(1, gen(ix, 1)+1, 20)
	ix.Refine(3, gen(ix, 3), 60)
	if got := candIDs(ix.Candidates([]uint16{5}, 100, 51)); !slices.Equal(got, []int64{1, 6}) {
		t.Errorf("stale or raising refine applied: %v", got)
	}
	ix.Refine(1, gen(ix, 1), 20)
	ix.Refine(6, gen(ix, 6), none)
	if got := candIDs(ix.Candidates([]uint16{5}, 100, 21)); !slices.Equal(got, []int64{2, 3}) {
		t.Errorf("refine not applied: %v", got)
	}
	if got := candIDs(ix.Candidates([]uint16{5}, 100, 20)); !slices.Equal(got, []int64{1, 2, 3, 4}) {
		t.Errorf("refined end not at or above from: %v", got)
	}
	// Reset: inactive, no answer, notes ignored.
	ix.Reset()
	ix.NoteRow(1, cur(200, 0, 0, 5))
	if ix.Active() || ix.Built() || ix.Candidates([]uint16{5}, 1000, 0) != nil {
		t.Error("reset index still answers")
	}
}

// TestValidEndIndex_ResolveUnknown — a re-read installs the rows' fold; an
// entity without rows disappears; a write landing during the re-read keeps the
// entity unknown (listed), and a read error is returned.
func TestValidEndIndex_ResolveUnknown(t *testing.T) {
	t.Parallel()
	ix := builtIndex()
	for _, id := range []int64{1, 2, 3} {
		ix.MarkUnknown(id)
	}
	rows := map[int64][]ValidEndRow{1: {cur(10, 40, 0, 5)}, 3: {cur(10, 0, 0, 5)}}
	err := ix.ResolveUnknown(func(id int64) ([]ValidEndRow, error) {
		if id == 3 {
			ix.NoteRow(3, cur(20, 0, 0, 5)) // a write during the read
		}
		return rows[id], nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if b, _, u := state(ix, 1); u || b != 40 {
		t.Errorf("entity 1: bound %d unknown %v, want 40", b, u)
	}
	if _, ok := ix.ents[2]; ok || len(ix.unknown) != 1 {
		t.Errorf("entity 2 (no rows) still present, unknown %v", ix.unknown)
	}
	if _, _, u := state(ix, 3); !u {
		t.Error("entity 3 written during the re-read: not unknown")
	}
	wantErr := storepkg.ErrRelNotFound
	if err := ix.ResolveUnknown(func(int64) ([]ValidEndRow, error) { return nil, wantErr }); err != wantErr {
		t.Fatalf("read error: %v", err)
	}
}

// TestValidEndIndex_RAMPerEntity measures the index's heap per entity (one
// type token per entity, as relationships; half deleted, half open, stamps
// rising as a stream writes them) and checks the Stats estimate against it.
// VALID_END_RAM=1 logs 10 K, 100 K and 1 M entities.
func TestValidEndIndex_RAMPerEntity(t *testing.T) {
	sizes := []int{100_000}
	if testing.Short() {
		sizes = []int{20_000}
	}
	if os.Getenv("VALID_END_RAM") != "" {
		sizes = []int{10_000, 100_000, 1_000_000}
	}
	heap := func() uint64 {
		runtime.GC()
		runtime.GC()
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		return m.HeapAlloc
	}
	for _, n := range sizes {
		base := heap()
		ix := builtIndex()
		for i := 0; i < n; i++ {
			id := int64(i)*4194304 + 7
			tx := types.Instant(1_000 + 2*i)
			ix.NoteRow(id, cur(tx, 0, 0, 3))
			if i%2 == 0 {
				ix.NoteRow(id, hist(0, tx, tx+1, 0, tx+1, 3))
				ix.NoteCurrentDelete(id)
			}
		}
		used := heap() - base
		st := ix.Stats()
		perEntity := float64(used) / float64(n)
		t.Logf("entities=%d heap %.1f B/entity (Stats estimate %.1f B/entity, %d ordered entries)", n, perEntity, float64(st.Bytes)/float64(n), st.Entries)
		if perEntity > 160 {
			t.Errorf("%.1f B/entity, want at most 160", perEntity)
		}
		runtime.KeepAlive(ix)
	}
}
