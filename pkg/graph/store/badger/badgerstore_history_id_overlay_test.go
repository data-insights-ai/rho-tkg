package badger

import (
	"fmt"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// idOverlayKind adds the ID-walk doors to presenceKind.
type idOverlayKind struct {
	presenceKind
	truncate func(bs *Store, id int64, keep int) error
	allIDs   func(bs *Store) ([]int64, error)
	count    func(bs *Store) (int, error)
}

func idOverlayKinds() []idOverlayKind {
	pk := presenceKinds()
	return []idOverlayKind{
		{
			presenceKind: pk[0],
			truncate: func(bs *Store, id int64, keep int) error {
				return bs.TruncateNodeHistory(types.NodeID(snowflake.ID(id)), keep)
			},
			allIDs: func(bs *Store) ([]int64, error) {
				ids, err := bs.AllNodeHistoryIDs()
				out := make([]int64, 0, len(ids))
				for _, id := range ids {
					out = append(out, int64(id))
				}
				return out, err
			},
			count: func(bs *Store) (int, error) { return bs.NodeHistoryCount() },
		},
		{
			presenceKind: pk[1],
			truncate: func(bs *Store, id int64, keep int) error {
				return bs.TruncateRelHistory(types.RelID(snowflake.ID(id)), keep)
			},
			allIDs: func(bs *Store) ([]int64, error) {
				ids, err := bs.AllRelHistoryIDs()
				out := make([]int64, 0, len(ids))
				for _, id := range ids {
					out = append(out, int64(id))
				}
				return out, err
			},
			count: func(bs *Store) (int, error) { return bs.RelHistoryCount() },
		},
	}
}

// TestHistoryIDOverlay_SetAndDeleteOfDifferentVersionsSameID: the write buffer
// holds a SET of one history version and a DELETE of ANOTHER version of the
// same ID (what Truncate*History(id, 1) after a fresh version leaves, and what
// replica apply of a truncate record or import-merge truncate-then-rewrite
// produce). The ID still has history, so the ID walk, the history count and
// the first (building) HasHistory call must all report it. An overlay that
// resolves set-vs-delete per ID instead of per key drops it. Shapes: both ops
// pending; SET pending with the DELETE parked in `flushing`; the reverse; and
// the counter case, a DELETE of the same key after its SET, which must still
// mask. Twenty fresh stores each (map iteration order varies).
func TestHistoryIDOverlay_SetAndDeleteOfDifferentVersionsSameID(t *testing.T) {
	const id = 77
	shapes := []struct {
		name string
		want bool
		prep func(t *testing.T, k idOverlayKind, bs *Store)
	}{
		{"both pending", true, func(t *testing.T, k idOverlayKind, bs *Store) {
			mustDo(t, k.put(bs, id, 1))
			mustDo(t, bs.Flush())
			mustDo(t, k.put(bs, id, 2))
			mustDo(t, k.truncate(bs, id, 1)) // DELETE v1 (pending), SET v2 (pending)
		}},
		{"set pending, delete flushing", true, func(t *testing.T, k idOverlayKind, bs *Store) {
			mustDo(t, k.put(bs, id, 1))
			mustDo(t, bs.Flush())
			mustDo(t, k.trimFrom(bs, id, 1)) // DELETE v1
			parkPendingIntoFlushing(t, bs)
			mustDo(t, k.put(bs, id, 2)) // SET v2 (pending)
		}},
		{"set flushing, delete pending", true, func(t *testing.T, k idOverlayKind, bs *Store) {
			mustDo(t, k.put(bs, id, 1))
			mustDo(t, bs.Flush())
			mustDo(t, k.put(bs, id, 2))
			parkPendingIntoFlushing(t, bs)   // SET v2 (flushing)
			mustDo(t, k.truncate(bs, id, 1)) // DELETE v1 (pending)
		}},
		{"delete of the same key after its set masks", false, func(t *testing.T, k idOverlayKind, bs *Store) {
			mustDo(t, k.put(bs, id, 1))
			mustDo(t, bs.Flush())
			mustDo(t, k.put(bs, id, 2))
			mustDo(t, k.truncate(bs, id, 0)) // DELETE v1 and v2
		}},
	}
	for _, k := range idOverlayKinds() {
		for _, sh := range shapes {
			t.Run(fmt.Sprintf("%s/%s", k.name, sh.name), func(t *testing.T) {
				wrong := 0
				for iter := 0; iter < 20; iter++ {
					bs := newFlushParkStore(t, nil)
					sh.prep(t, k, bs)
					rows, err := k.rows(bs, id)
					mustDo(t, err)
					if (rows > 0) != sh.want {
						t.Fatalf("setup: history has %d rows, want presence %v", rows, sh.want)
					}
					has, err := k.has(bs, id)
					mustDo(t, err)
					ids, err := k.allIDs(bs)
					mustDo(t, err)
					n, err := k.count(bs)
					mustDo(t, err)
					listed := len(ids) == 1 && ids[0] == id
					wantN := 0
					if sh.want {
						wantN = 1
					}
					if has != sh.want || listed != sh.want || (!sh.want && len(ids) != 0) || n != wantN {
						wrong++
						if wrong == 1 {
							t.Errorf("iter %d: first HasHistory=%v, AllHistoryIDs=%v, HistoryCount=%d; want %v / [%d]? / %d",
								iter, has, ids, n, sh.want, id, wantN)
						}
					}
					requeueParked(bs)
					mustDo(t, bs.Close())
				}
				if wrong > 0 {
					t.Errorf("wrong in %d of 20 fresh stores", wrong)
				}
			})
		}
	}
}

func mustDo(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
