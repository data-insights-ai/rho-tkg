package badger

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Round 5 request 4 (sigma-tkgd C4r/C4s): one property write to a member of a
// label made the next column read rebuild the label's whole snapshot, 4.5 s on
// 1,000,000 badger nodes. An in-place update of a member is now patched into
// the cached snapshot.
//
// Faulty implementations caught:
//   - the rebuild after an update (TestColumnPatch_UpdateIsPatched counts it);
//   - a patch that keeps a stale value, a deleted or relabelled node's row, a
//     wrong type class, or misses a row a write made (the random oracle reads
//     every member back from the store);
//   - a patch recorded on a label nobody reads columns of (the lazy rule).

// TestColumnPatch_UpdateIsPatched: an update of a member after the snapshot
// was built is served by a patch, not a rebuild, for a single label and for
// an intersection (each on its own store: a refresh consumes the member
// labels' records, so reading both kinds alternately rebuilds the other, as
// for appends), and reads the new value.
func TestColumnPatch_UpdateIsPatched(t *testing.T) {
	for _, multi := range []bool{false, true} {
		t.Run(fmt.Sprintf("multi=%v", multi), func(t *testing.T) {
			bs := adStore(t)
			for i := int64(1); i <= 6; i++ {
				adPutMulti(t, bs, i, i*100, i%2 == 0)
			}
			read := func() map[int64]int64 {
				if multi {
					return adReadMulti(t, bs)
				}
				return adRead(t, bs)
			}
			read()
			rebuilds := bs.ColumnRebuildCount()

			n := types.NewNode(types.NodeID(4), adLabel, []uint16{adLabelB})
			_ = n.SetProperty("qty", int64(4444))
			n.SetTemporal(&types.TemporalMetadata{ValidFrom: 40})
			if err := bs.ReplaceNode(n); err != nil {
				t.Fatal(err)
			}
			want := map[int64]int64{1: 100, 2: 200, 3: 300, 4: 4444, 5: 500, 6: 600}
			if multi {
				want = map[int64]int64{2: 200, 4: 4444, 6: 600}
			}
			adWant(t, read(), want)
			if got := bs.ColumnRebuildCount(); got != rebuilds {
				t.Fatalf("an in-place update rebuilt %d snapshot(s); want a patch", got-rebuilds)
			}
			if got := bs.ColumnPatchCount(); got != 1 {
				t.Fatalf("ColumnPatchCount = %d, want 1", got)
			}
		})
	}
}

// TestColumnPatch_UnreadLabelStaysPoisoned: a label no column snapshot was
// built for records no update (the removal seam poisons it, as before), so
// a write-only label pays nothing new per write.
func TestColumnPatch_UnreadLabelStaysPoisoned(t *testing.T) {
	bs := adStore(t)
	for i := int64(1); i <= 3; i++ {
		adPut(t, bs, i, i)
	}
	adUpdate(t, bs, 2, 22)
	bs.appendMu.Lock()
	d := bs.appendDeltas[adLabel]
	bs.appendMu.Unlock()
	if d == nil || !d.poisoned || len(d.changed) != 0 {
		t.Fatalf("delta on an unread label = %+v, want poisoned with nothing recorded", d)
	}
	if bs.ColumnPatchCount() != 0 {
		t.Fatal("patched without a snapshot")
	}
}

// TestColumnPatch_RandomWorkloadMatchesTheStore: random inserts, updates
// (int, float, string, absent values, validity changes), deletes and label
// changes, with reads after some writes and none after others, single label
// and intersection; every read equals the members' current rows read back
// from the store, and the patch path ran.
func TestColumnPatch_RandomWorkloadMatchesTheStore(t *testing.T) {
	for seed := int64(1); seed <= 6; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			r := rand.New(rand.NewSource(seed)) // #nosec G404 -- deterministic test sequence
			bs := adStore(t)
			const ids = 40
			alive := map[int64]bool{}
			labelsOf := map[int64][]uint16{}
			numeric := seed%2 == 0 // the column's class stays fixed so patches can apply
			val := func() any {
				switch k := r.Intn(8); {
				case k == 0:
					return nil
				case !numeric:
					return string(rune('a' + r.Intn(5)))
				case k < 3:
					return float64(r.Intn(9)) + 0.25
				default:
					return int64(r.Intn(9))
				}
			}
			write := func(id int64, replace bool) {
				labels := []uint16{adLabel}
				if replace {
					labels = labelsOf[id] // a replace keeps the labels
				} else if r.Intn(2) == 0 {
					labels = append(labels, adLabelB)
				}
				n := types.NewNode(types.NodeID(id), labels[0], labels[1:])
				if v := val(); v != nil {
					_ = n.SetProperty("qty", v)
				}
				n.SetTemporal(&types.TemporalMetadata{ValidFrom: types.Instant(id*10 + int64(r.Intn(3)))})
				var err error
				if replace {
					err = bs.ReplaceNode(n)
				} else {
					err = bs.PutNode(n)
				}
				if err != nil {
					t.Fatal(err)
				}
				alive[id], labelsOf[id] = true, labels
			}
			read := func(step string, multi bool) {
				t.Helper()
				toks := []uint16{adLabel}
				if multi {
					toks = append(toks, adLabelB)
				}
				got := map[int64]string{}
				fn := func(id types.NodeID, vals []any, present []bool) bool {
					s := "absent"
					if present[0] {
						s = fmt.Sprintf("%T:%v", vals[0], vals[0])
					}
					got[int64(id)] = s
					return true
				}
				var ok bool
				var err error
				if multi {
					_, ok, err = bs.ForEachDocValuesMulti(toks, []string{"qty"}, fn)
				} else {
					_, ok, err = bs.ForEachDocValues(adLabel, []string{"qty"}, fn)
				}
				if err != nil {
					t.Fatal(err)
				}
				want := map[int64]string{}
				for id := int64(1); id <= ids; id++ {
					if !alive[id] {
						continue
					}
					member := true
					for _, tok := range toks {
						has := false
						for _, l := range labelsOf[id] {
							has = has || l == tok
						}
						member = member && has
					}
					if !member {
						continue
					}
					n, err := bs.GetNode(types.NodeID(id))
					if err != nil {
						t.Fatal(err)
					}
					s := "absent"
					if v, has := n.GetProperty("qty"); has {
						s = fmt.Sprintf("%T:%v", v, v)
					}
					want[id] = s
				}
				if !ok {
					if len(want) == 0 {
						return // an empty label or intersection declines
					}
					t.Fatalf("%s: declined with %d members", step, len(want))
				}
				if fmt.Sprint(sortedRows(got)) != fmt.Sprint(sortedRows(want)) {
					t.Fatalf("%s (multi=%v):\n got  %v\n want %v", step, multi, sortedRows(got), sortedRows(want))
				}
			}
			for id := int64(1); id <= 10; id++ {
				write(id, false)
			}
			read("seed", false)
			read("seed", true)
			for step := range 300 {
				id := int64(1 + r.Intn(ids))
				switch op := r.Intn(10); {
				case op < 2 && !alive[id]:
					write(id, false)
				case op < 7 && alive[id]:
					write(id, true)
				case op < 8 && alive[id]:
					if err := bs.DeleteNode(types.NodeID(id)); err != nil {
						t.Fatal(err)
					}
					alive[id] = false
				case op < 10 && alive[id]:
					// Toggle the intersection's second label.
					cur, err := bs.GetNode(types.NodeID(id))
					if err != nil {
						t.Fatal(err)
					}
					if len(labelsOf[id]) == 2 {
						if !cur.RemoveLabelTokenRaw(adLabelB) {
							t.Fatal("RemoveLabelTokenRaw")
						}
						err = bs.RemoveNodeLabelToken(types.NodeID(id), adLabelB, cur)
						labelsOf[id] = []uint16{adLabel}
					} else {
						if !cur.AddLabelTokenRaw(adLabelB) {
							t.Fatal("AddLabelTokenRaw")
						}
						err = bs.AddNodeLabelToken(types.NodeID(id), adLabelB, cur)
						labelsOf[id] = []uint16{adLabel, adLabelB}
					}
					if err != nil {
						t.Fatal(err)
					}
				}
				if r.Intn(3) == 0 {
					read(fmt.Sprintf("step %d", step), r.Intn(2) == 0)
				}
			}
			read("final", false)
			read("final", true)
			if bs.ColumnPatchCount() == 0 {
				t.Fatal("no refresh used the patch path; the oracle tested the rebuild only")
			}
		})
	}
}

func sortedRows(m map[int64]string) []string {
	out := make([]string, 0, len(m))
	for id, s := range m {
		out = append(out, fmt.Sprintf("%d=%s", id, s))
	}
	sort.Strings(out)
	return out
}

// TestColumnPatch_ConcurrentUpdatesAndReads: a writer updates members while
// readers refresh the snapshot; every exposed row keeps the writer's
// invariant (qty / 1000 == id) and the final read equals the last writes.
// Run under -race.
func TestColumnPatch_ConcurrentUpdatesAndReads(t *testing.T) {
	bs := adStore(t)
	for i := int64(1); i <= 20; i++ {
		adPut(t, bs, i, i*1000)
	}
	adRead(t, bs)
	done := make(chan struct{})
	last := map[int64]int64{}
	go func() {
		defer close(done)
		for step := int64(1); step <= 300; step++ {
			id := 1 + step%20
			if err := bs.ReplaceNode(adNode(id, id*1000+step%1000)); err != nil {
				t.Error(err)
				return
			}
			last[id] = id*1000 + step%1000
		}
	}()
	for range 100 {
		for id, q := range adRead(t, bs) {
			if q/1000 != id {
				t.Fatalf("id %d exposed qty %d — a torn patch", id, q)
			}
		}
	}
	<-done
	want := map[int64]int64{}
	for id := int64(1); id <= 20; id++ {
		want[id] = id * 1000
		if q, ok := last[id]; ok {
			want[id] = q
		}
	}
	adWant(t, adRead(t, bs), want)
}
