package badger

import (
	"fmt"
	"math"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// RELDISKWIN option B: the time-ordered member entries (0x0C kinds 7 and 8)
// and the windowed read that seeks them.

// timeEntries counts the time-ordered entries of the seat definitions, rel and
// node, by axis.
func (w *spanWorld) timeEntries(bs *Store) (late, recent int) {
	w.t.Helper()
	for _, c := range []struct {
		kind  byte
		scope uint16
	}{{storepkg.EntityIndexRelTime, w.r}, {storepkg.EntityIndexNodeTime, w.d}} {
		for k := range spanEntries(w.t, bs, c.kind, c.scope) {
			axis, _, _, _, _, _, ok := storepkg.EntityIndexTimeEntryOf([]byte(k))
			switch {
			case !ok:
				w.t.Fatalf("a malformed time-ordered key %x", k)
			case axis == storepkg.EntityIndexAxisLate:
				late++
			case axis == storepkg.EntityIndexAxisRecent:
				recent++
			default:
				w.t.Fatalf("a time-ordered key on axis %d", axis)
			}
		}
	}
	return late, recent
}

// memberEntries counts every member entry of the seat definitions: span
// kinds, time-ordered kinds, and the legacy kinds.
func (w *spanWorld) memberEntries(bs *Store) int {
	w.t.Helper()
	n := 0
	for _, kind := range []byte{storepkg.EntityIndexRelSpan, storepkg.EntityIndexRelTime, storepkg.EntityIndexRelMember} {
		n += len(spanEntries(w.t, bs, kind, w.r))
	}
	for _, kind := range []byte{storepkg.EntityIndexNodeSpan, storepkg.EntityIndexNodeTime, storepkg.EntityIndexNodeMember} {
		n += len(spanEntries(w.t, bs, kind, w.d))
	}
	return n
}

// TestMemberSpan_TimeEntriesPerRow: after scenarios A to C every row that
// carries the value has one late-axis entry and every widened row (an open
// row an update demoted, a tombstone whose delete wrote ValidTo) one
// recent-axis entry, exactly: a move (a demotion, a delete) leaves no entry at
// the row's former end. Widened rows, by hand from the scenarios: 11 v0, 12,
// 14, 15 v0 and 18 v0, per kind (10).
func TestMemberSpan_TimeEntriesPerRow(t *testing.T) {
	t.Parallel()
	w := newSpanWorld(t)
	bs := w.open("", true, false)
	w.create(bs)
	w.scenarioA(bs)
	w.scenarioB(bs)
	w.scenarioC(bs)
	spans := len(spanEntries(t, bs, storepkg.EntityIndexRelSpan, w.r)) + len(spanEntries(t, bs, storepkg.EntityIndexNodeSpan, w.d))
	late, recent := w.timeEntries(bs)
	if late != spans || recent != 10 {
		t.Fatalf("%d span entries, %d late-axis and %d recent-axis entries; want one late-axis entry per span entry and 10 recent-axis entries", spans, late, recent)
	}
	t.Logf("%d span entries, %d late-axis entries, %d recent-axis entries", spans, late, recent)
}

// TestMemberSpan_PurgeErasureDropLeaveNoTimeEntries: after scenarios A to C
// (creates, with-history replacements, history-door versions, deletes that
// write and keep ValidTo) a retention purge or an exact erasure leaves no
// member entry of any kind, time-ordered ones included, of the entities it
// removes, and a drop of the definitions leaves none at all. The purge doors
// remove the live entities (11, 13, 15, 17, 18, 19 and their node twins): a
// deleted entity (12, 14, 16) is selected by neither PurgeNodesByLabelBefore
// nor, by its history, PurgeRelationshipByInfo, and keeps its span entries as
// before option B.
func TestMemberSpan_PurgeErasureDropLeaveNoTimeEntries(t *testing.T) {
	t.Parallel()
	for _, how := range []string{"purge", "erasure", "drop"} {
		t.Run(how, func(t *testing.T) {
			t.Parallel()
			w := newSpanWorld(t)
			bs := w.open("", true, false)
			w.create(bs)
			w.scenarioA(bs)
			w.scenarioB(bs)
			w.scenarioC(bs)
			if late, recent := w.timeEntries(bs); late == 0 || recent == 0 {
				t.Fatalf("the scenarios wrote %d late-axis and %d recent-axis entries", late, recent)
			}
			removed := map[int64]bool{}
			switch how {
			case "purge":
				for _, id := range []int64{11, 13, 15, 17, 18, 19} {
					must(t, "purge rel", bs.PurgeRelationshipByInfo(storecontract.PurgedRel{ID: types.RelID(snowflake.ID(id)), TypeToken: w.r, StartID: 1, EndID: 2}))
					removed[id], removed[id+nodeBase] = true, true
				}
				if _, err := bs.PurgeNodesByLabelBefore(w.d, math.MaxInt64, 0); err != nil {
					t.Fatal(err)
				}
			case "erasure":
				req := storecontract.ExactErasureRequest{Bounds: badgerExactErasureBounds}
				for id := int64(11); id <= 19; id++ {
					req.RelIDs = append(req.RelIDs, types.RelID(snowflake.ID(id)))
					req.NodeIDs = append(req.NodeIDs, types.NodeID(snowflake.ID(id+nodeBase)))
					removed[id], removed[id+nodeBase] = true, true
				}
				if _, err := bs.ExactErase(req); err != nil {
					t.Fatal(err)
				}
			case "drop":
				must(t, "drop rel", bs.DropRelPropertyIndex(w.r, "seat"))
				must(t, "drop node", bs.DropPropertyIndex(w.d, "seat"))
				if n := w.memberEntries(bs); n != 0 {
					t.Fatalf("the drop left %d member entries", n)
				}
				return
			}
			left := 0
			for _, kind := range []byte{storepkg.EntityIndexRelSpan, storepkg.EntityIndexRelTime, storepkg.EntityIndexNodeSpan, storepkg.EntityIndexNodeTime} {
				scope := w.r
				if storepkg.EntityIndexIsNodeMember(kind) {
					scope = w.d
				}
				for k := range spanEntries(t, bs, kind, scope) {
					left++
					if _, id, _, ok := storepkg.EntityIndexEntryOf([]byte(k), kind); !ok || removed[int64(id)] {
						t.Errorf("%s left an entry of kind %d of removed entity %d", how, kind, id)
					}
				}
			}
			t.Logf("%s: %d member entries left, none of a removed entity", how, left)
		})
	}
}

// TestMemberSpan_SeekShapes measures the shapes the seek does not bound
// (design 1: the rows that start after the window's end, the widened rows
// written after an old pin, the open rows that start before the window's end),
// each with n = 8, 32 and 128 such rows beside the two targets, flushed: the
// steps grow with n, the members handed on do not (the first two) or are the
// rows themselves (the third, genuine candidates). A fourth shape it does
// bound: rows that end exactly at the window's start are never visited (the
// seek is past the start).
func TestMemberSpan_SeekShapes(t *testing.T) {
	t.Parallel()
	for _, shape := range []string{"future", "old pin", "open", "ends at start"} {
		for _, n := range unrelatedHistory {
			t.Run(fmt.Sprint(shape, " n=", n), func(t *testing.T) {
				t.Parallel()
				w := newSpanWorld(t)
				bs := w.open("", true, false)
				w.create(bs)
				must(t, "target 1", bs.PutRelationship(w.rel(1, 1, 0, 100, 0, 1000, 1010)))
				must(t, "target 2", bs.PutRelationship(w.rel(2, 1, 0, 100, 0, 1005, 1006)))
				pin := types.Instant(0)
				for i := int64(0); i < int64(n); i++ {
					id := 100 + i
					switch shape {
					case "future": // valid after the window
						must(t, "put", bs.PutRelationship(w.rel(id, 1, 0, 100, 0, 5000+types.Instant(i), 5010+types.Instant(i))))
					case "old pin": // open at 100, replaced at 200 by a row far before the window: widened after pin 150
						must(t, "put", bs.PutRelationship(w.rel(id, 1, 0, 100, 0, 5, 0)))
						must(t, "replace", bs.ReplaceRelWithHistory(w.rel(id, 2, 1, 200, 0, 6, 7), 0, w.rel(id, 1, 0, 100, 200, 5, 0)))
						pin = 150
					case "ends at start": // valid up to the window's start, not into it
						must(t, "put", bs.PutRelationship(w.rel(id, 1, 0, 100, 0, 5+types.Instant(i), 1000)))
					case "open": // open since before the window: each meets it
						must(t, "put", bs.PutRelationship(w.rel(id, 1, 0, 100, 0, 5+types.Instant(i), 0)))
					}
				}
				must(t, "flush", bs.Flush())
				work, err := bs.DiagnoseRelPropertyTxMembersDuring(w.r, "seat", w.vk, 1000, 1010, pin, func(types.RelID, types.Instant) bool { return true })
				must(t, "diagnose", err)
				wantMembers := uint64(2)
				if shape == "old pin" || shape == "open" {
					wantMembers += uint64(n) // an old pin sees each open row, as an open row meets the window
				}
				wantSteps := uint64(2 + n)
				if shape == "ends at start" {
					wantSteps = 2
				}
				if shape == "old pin" {
					// The demotion moved each row's late-axis entry from MaxInt64 to 6:
					// its delete stays a badger version at MaxInt64, past every window's
					// start, until compaction drops it (docs/badger.md rule 12).
					wantSteps += uint64(n)
				}
				if work.MembersEmitted != wantMembers || work.IteratorSteps != wantSteps {
					t.Errorf("%s n=%d: %d members, %d steps; want %d and %d", shape, n, work.MembersEmitted, work.IteratorSteps, wantMembers, wantSteps)
				}
				t.Logf("%s n=%d window [1000,1010)@%d: iterator steps %d, seeks %d, disk entries %d, members %d", shape, n, pin, work.IteratorSteps, work.IteratorSeeks, work.DiskEntriesEmitted, work.MembersEmitted)
			})
		}
	}
}

// TestRowMeets_IsOneEntryMember: index.RowMeets answers what a DiskMember with
// that one entry answers, at every window and pin of a grid.
func TestRowMeets_IsOneEntryMember(t *testing.T) {
	t.Parallel()
	vals := []types.Instant{types.UnboundedValidFrom, 0, 5, 10, 20, math.MaxInt64}
	for _, from := range vals {
		for _, lateTo := range vals {
			for _, allTo := range vals {
				if allTo < lateTo {
					continue
				}
				for _, last := range []types.Instant{1, 10, 30} {
					m := indexpkg.NewDiskMember()
					m.AddEntry(1, from, allTo, lateTo, last)
					for _, s := range vals {
						for _, e := range vals {
							for _, pin := range []types.Instant{0, 5, 10, 20, math.MaxInt64} {
								if got, want := indexpkg.RowMeets(from, allTo, lateTo, last, s, e, pin), m.Meets(s, e, pin); got != want {
									t.Fatalf("row [%d %d %d %d] window [%d,%d)@%d: RowMeets %t, DiskMember %t", from, allTo, lateTo, last, s, e, pin, got, want)
								}
							}
						}
					}
				}
			}
		}
	}
}
