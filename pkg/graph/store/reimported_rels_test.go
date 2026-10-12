package store_test

import (
	"errors"
	"slices"
	"testing"
	"time"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Backlog 50: store.ReimportedRelCapability on every backend. The set holds
// each noted ID once, in any order, until Clear; a nil callback, an invalid ID
// and a closed store fail with their sentinels; fn returning false stops; on
// badger the set survives a reopen and Clear wipes it on disk too.

func reimportedSet(t *testing.T, caps storecontract.ReimportedRelCapability) []types.RelID {
	t.Helper()
	var ids []types.RelID
	if err := caps.ForEachReimportedRelID(func(id types.RelID) bool { ids = append(ids, id); return true }); err != nil {
		t.Fatalf("ForEachReimportedRelID: %v", err)
	}
	slices.Sort(ids)
	return ids
}

func TestReimportedRelSet(t *testing.T) {
	a, b := currentRelID(1), currentRelID(2)
	for _, be := range currentBackends() {
		t.Run(be.name, func(t *testing.T) {
			st := be.open(t)
			caps, ok := st.(storecontract.ReimportedRelCapability)
			if !ok {
				t.Fatalf("%T lacks ReimportedRelCapability", st)
			}
			if got := reimportedSet(t, caps); len(got) != 0 {
				t.Fatalf("fresh store set = %v, want empty", got)
			}
			for _, id := range []types.RelID{a, b, a} {
				if err := caps.NoteReimportedRel(id); err != nil {
					t.Fatalf("NoteReimportedRel(%d): %v", id, err)
				}
			}
			if got, want := reimportedSet(t, caps), []types.RelID{a, b}; !slices.Equal(got, want) {
				t.Fatalf("set = %v, want %v (each noted ID once)", got, want)
			}
			calls := 0
			if err := caps.ForEachReimportedRelID(func(types.RelID) bool { calls++; return false }); err != nil || calls != 1 {
				t.Fatalf("fn false: %d calls, %v; want 1 call", calls, err)
			}
			if err := caps.ForEachReimportedRelID(nil); !errors.Is(err, storecontract.ErrInvalidStoreMutation) {
				t.Fatalf("nil fn = %v, want ErrInvalidStoreMutation", err)
			}
			for _, bad := range []types.RelID{0, -1} {
				if err := caps.NoteReimportedRel(bad); !errors.Is(err, storecontract.ErrInvalidStoreMutation) {
					t.Fatalf("NoteReimportedRel(%d) = %v, want ErrInvalidStoreMutation", bad, err)
				}
			}
			if got, want := reimportedSet(t, caps), []types.RelID{a, b}; !slices.Equal(got, want) {
				t.Fatalf("set after rejected notes = %v, want %v", got, want)
			}
			if err := st.Clear(); err != nil {
				t.Fatal(err)
			}
			if got := reimportedSet(t, caps); len(got) != 0 {
				t.Fatalf("set after Clear = %v, want empty", got)
			}
			if err := caps.NoteReimportedRel(b); err != nil {
				t.Fatal(err)
			}
			if got, want := reimportedSet(t, caps), []types.RelID{b}; !slices.Equal(got, want) {
				t.Fatalf("set after Clear and a note = %v, want %v", got, want)
			}
			if err := st.Close(); err != nil {
				t.Fatal(err)
			}
			if err := caps.NoteReimportedRel(a); !errors.Is(err, storecontract.ErrStoreClosed) {
				t.Fatalf("closed NoteReimportedRel = %v, want ErrStoreClosed", err)
			}
			if err := caps.ForEachReimportedRelID(func(types.RelID) bool { return true }); !errors.Is(err, storecontract.ErrStoreClosed) {
				t.Fatalf("closed ForEachReimportedRelID = %v, want ErrStoreClosed", err)
			}
		})
	}
}

// TestReimportedRelSetSurvivesReopen: badger keeps the set on disk (a note
// made before a reopen is listed after it, and noting it again is a no-op),
// and Clear removes the keys, not only the RAM copy.
func TestReimportedRelSetSurvivesReopen(t *testing.T) {
	a, b := currentRelID(1), currentRelID(2)
	dir := t.TempDir()
	open := func() *badger.Store {
		t.Helper()
		bs, err := badger.New(badger.Config{Dir: dir, FlushInterval: time.Hour})
		if err != nil {
			t.Fatalf("badger.New: %v", err)
		}
		return bs
	}
	bs := open()
	for _, id := range []types.RelID{a, b} {
		if err := bs.NoteReimportedRel(id); err != nil {
			t.Fatal(err)
		}
	}
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}
	bs = open()
	if got, want := reimportedSet(t, bs), []types.RelID{a, b}; !slices.Equal(got, want) {
		t.Fatalf("set after reopen = %v, want %v", got, want)
	}
	if err := bs.NoteReimportedRel(a); err != nil {
		t.Fatal(err)
	}
	if err := bs.Clear(); err != nil {
		t.Fatal(err)
	}
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}
	bs = open()
	defer bs.Close()
	if got := reimportedSet(t, bs); len(got) != 0 {
		t.Fatalf("set after Clear and reopen = %v, want empty", got)
	}
}
