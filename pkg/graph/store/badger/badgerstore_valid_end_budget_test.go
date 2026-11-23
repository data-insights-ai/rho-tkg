package badger

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/internal/testtier"
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestValidEndCandidates_AdmissionBeforeCopy(t *testing.T) {
	bs, err := New(Config{InMemory: true, MaxPendingWrites: -1, FlushInterval: 1<<63 - 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	for id := int64(1); id <= 6; id++ {
		tok := uint16(1)
		if id > 3 {
			tok = 2
		}
		if err := bs.PutNode(types.NewNode(types.NodeID(id), tok, nil)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := bs.NodeValidEndCandidates([]uint16{1, 2}, 100, 0); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		budget storecontract.ReadBudget
		limit  string
		copies int64
	}{
		{"control", storecontract.ReadBudget{}, "", 6},
		{"candidates", storecontract.ReadBudget{Candidates: 4}, "candidates", 4},
		{"buffer", storecontract.ReadBudget{BufferBytes: 1}, "buffer bytes", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, m := storecontract.WithReadBudget(t.Context(), tc.budget)
			copied, stop := indexpkg.CountValidEndCandidatesCopiedForTest()
			defer stop()
			got, err := bs.NodeValidEndCandidatesMetered([]uint16{1, 2}, 100, 0, m.Tally("probe"))
			if tc.limit == "" {
				var ids []int64
				for _, c := range got {
					ids = append(ids, c.ID)
				}
				slices.Sort(ids)
				if err != nil || !slices.Equal(ids, []int64{1, 2, 3, 4, 5, 6}) {
					t.Fatalf("control: %v, %v", ids, err)
				}
			} else {
				var refusal *storecontract.ReadBudgetError
				if !errors.As(err, &refusal) || refusal.Limit != tc.limit || len(got) != 0 {
					t.Fatalf("bounded: %v, %v", got, err)
				}
			}
			if copied() != tc.copies {
				t.Fatalf("native copies %d; want %d", copied(), tc.copies)
			}
		})
	}
}

func TestValidEndRows_PendingScope(t *testing.T) {
	bs, err := New(Config{InMemory: true, MaxPendingWrites: -1, FlushInterval: 1<<63 - 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	for id := int64(1); id <= 6; id++ {
		if err := bs.PutNode(types.NewNode(types.NodeID(id), 1, nil)); err != nil {
			t.Fatal(err)
		}
	}
	if err := bs.ensureLabelTxMembersBuilt(); err != nil {
		t.Fatal(err)
	}
	for id := int64(101); id <= 164; id++ {
		if err := bs.PutNode(types.NewNode(types.NodeID(id), 2, nil)); err != nil {
			t.Fatal(err)
		}
	}
	visited, stop := CountPendingKeysVisitedForTest()
	defer stop()
	_, m := storecontract.WithReadBudget(t.Context(), storecontract.ReadBudget{Candidates: 48})
	got, err := bs.NodeValidEndCandidatesMetered([]uint16{1}, 100, 0, m.Tally("probe"))
	var ids []int64
	for _, c := range got {
		ids = append(ids, c.ID)
	}
	slices.Sort(ids)
	if err != nil || !slices.Equal(ids, []int64{1, 2, 3, 4, 5, 6}) || visited() != 0 {
		t.Fatalf("ids %v, pending walk %d, work %+v, err %v; want exact six IDs without a pending-key walk", ids, visited(), m.Work(), err)
	}
}

func TestValidEndRows_ForwardStepsAdmitted(t *testing.T) {
	for _, limit := range []uint64{0, 8} {
		bs, err := New(Config{InMemory: true, MaxPendingWrites: -1, FlushInterval: 1<<63 - 1})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = bs.Close() })
		for id := int64(1); id <= 66; id++ {
			tok := uint16(2)
			if id == 1 || id == 66 {
				tok = 1
			}
			if err := bs.PutNode(types.NewNode(types.NodeID(id), tok, nil)); err != nil {
				t.Fatal(err)
			}
		}
		if err := bs.Flush(); err != nil {
			t.Fatal(err)
		}
		if err := bs.ensureLabelTxMembersBuilt(); err != nil {
			t.Fatal(err)
		}
		_, m := storecontract.WithReadBudget(t.Context(), storecontract.ReadBudget{Candidates: limit})
		cs, err := bs.NodeValidEndCandidatesMetered([]uint16{1}, 100, 0, m.Tally("probe"))
		if limit != 0 {
			if !errors.Is(err, storecontract.ErrReadBudget) || len(cs) != 0 || bs.ValidEndIteratorStepsForTest() > 2 || bs.ValidEndRowDecodesForTest() > 1 {
				t.Fatalf("bounded: %v, %v, steps %d, rows %d", cs, err, bs.ValidEndIteratorStepsForTest(), bs.ValidEndRowDecodesForTest())
			}
		} else {
			var ids []int64
			for _, c := range cs {
				ids = append(ids, c.ID)
			}
			slices.Sort(ids)
			if err != nil || !slices.Equal(ids, []int64{1, 66}) || bs.ValidEndIteratorStepsForTest() != 17 || bs.ValidEndRowDecodesForTest() != 2 {
				t.Fatalf("control: %v, %v, steps %d, rows %d", ids, err, bs.ValidEndIteratorStepsForTest(), bs.ValidEndRowDecodesForTest())
			}
		}
	}
}

// TestValidEndOverlayRows_ChargeEachHistoryRowBeforeItsDecode (R2 late counts,
// ai-soc review 14 Nov 3.3): an entity with buffered writes is read by
// validEndOverlayRows, which decoded its whole history, committed rows and
// buffered rows alike, and only then counted the history versions (and the
// stored bytes not at all). Node 1 has 30 history versions, 15 flushed and 15
// buffered, and a buffered current row. Capturing the 15 pending rows and
// decoding all 30 versions are separate history examinations. A history limit
// of 3 refuses during capture, before any decode. A decode limit of 1 admits
// that capture and the first committed examination, then refuses the decoder.
func TestValidEndOverlayRows_ChargeEachHistoryRowBeforeItsDecode(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	const versions = 30
	open := func(t *testing.T) *Store {
		bs, err := New(Config{Dir: t.TempDir(), FlushInterval: time.Hour})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = bs.Close() })
		must := func(err error) {
			t.Helper()
			if err != nil {
				t.Fatal(err)
			}
		}
		must(bs.PutNode(veNode(1, 0, 10, 0, 0)))
		for v := uint32(1); v <= versions; v++ {
			must(bs.PutNodeVersion(types.NodeID(1), v, veNode(1, v, types.Instant(10+v), 0, 0)))
			if v == versions/2 {
				must(bs.Flush())
			}
		}
		must(bs.ReplaceNode(veNode(1, versions+1, 100, 0, 0))) // a buffered current row: the entity is read from the overlay
		// This test measures valid-end row reads after membership construction.
		must(bs.ensureLabelTxMembersBuilt())
		return bs
	}
	call := func(bs *Store, b storecontract.ReadBudget) (*storecontract.ReadMeter, error) {
		_, m := storecontract.WithReadBudget(context.Background(), b)
		_, err := bs.NodeValidEndCandidatesMetered([]uint16{1}, 1_000, 0, m.Tally("probe"))
		return m, err
	}

	bs := open(t)
	m, err := call(bs, storecontract.ReadBudget{})
	if w, decodes := m.Work(), bs.HistoryMetaDecodesForTest(); err != nil || w.HistoryExamined != versions+versions/2 || w.DecodedBytes == 0 || w.Candidates != 4 || decodes != versions {
		t.Fatalf("unlimited = %+v, %v with %d history rows decoded; want %d examinations, 4 candidates and %d native decodes", w, err, decodes, versions+versions/2, versions)
	}

	for _, c := range []struct {
		limit   string
		b       storecontract.ReadBudget
		counted uint64 // history versions counted at the refusal
		decoded int64  // history rows decoded at the refusal
	}{
		{"history examined", storecontract.ReadBudget{HistoryExamined: 3}, 4, 0},
		{"decoded bytes", storecontract.ReadBudget{DecodedBytes: 1}, versions/2 + 1, 0},
	} {
		t.Run(c.limit, func(t *testing.T) {
			bs := open(t)
			m, err := call(bs, c.b)
			var rb *storecontract.ReadBudgetError
			if !errors.As(err, &rb) || rb.Limit != c.limit {
				t.Fatalf("%s: %v; want ErrReadBudget at %q", c.limit, err, c.limit)
			}
			if got, decodes := m.Work().HistoryExamined, bs.HistoryMetaDecodesForTest(); got != c.counted || decodes != c.decoded {
				t.Fatalf("%s: %d history versions counted (%+v), %d rows decoded at the refusal; want %d and %d: the history was decoded before it was counted", c.limit, got, m.Work(), decodes, c.counted, c.decoded)
			}
		})
	}
}

// TestValidEndFirstCall_RefusesBeforeTheEnlistment (R2 late counts): the first
// call per token enlisted every ever-member of the token as unknown (a map entry
// each) and only then counted one candidate per entity, so a Candidates limit
// below the members refused with the whole index built. It now refuses before
// anything is enlisted, nodes and relationships, committed and buffered: the
// token is not tracked, no entity is unknown and no row was decoded (the native
// counters, which the meter does not keep), the refusal counts the members the
// count walk saw before it stopped (the first past the room), and a retry with
// room for all answers what the control (no limit) answers, decoding each
// committed row once.
func TestValidEndFirstCall_RefusesBeforeTheEnlistment(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	const members, room = 50, 10
	for _, kind := range []string{"nodes", "rels"} {
		for _, flushed := range []bool{true, false} {
			name := kind + map[bool]string{true: "/flushed", false: "/pending"}[flushed]
			t.Run(name, func(t *testing.T) {
				open := func() *Store {
					bs, err := New(Config{Dir: t.TempDir(), FlushInterval: time.Hour})
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = bs.Close() })
					if kind == "rels" { // the endpoints, in a label of their own
						for _, id := range []int64{1001, 1002} {
							if err := bs.PutNode(types.NewNode(types.NodeID(id), 2, nil)); err != nil {
								t.Fatal(err)
							}
						}
					}
					for i := int64(1); i <= members; i++ {
						var err error
						if kind == "nodes" {
							err = bs.PutNode(veNode(i, 0, 10, 0, 0))
						} else {
							err = bs.PutRelationship(types.NewRelationship(types.RelID(i), 1, 1001, 1002))
						}
						if err != nil {
							t.Fatal(err)
						}
					}
					if flushed {
						if err := bs.Flush(); err != nil {
							t.Fatal(err)
						}
					}
					if err := bs.ensureLabelTxMembersBuilt(); err != nil {
						t.Fatal(err)
					}
					if err := bs.ensureRelTypeTxMembersBuilt(); err != nil {
						t.Fatal(err)
					}
					return bs
				}
				call := func(bs *Store, m *storecontract.ReadMeter) ([]int64, error) {
					var cands []storecontract.ValidEndCandidate
					var err error
					if kind == "nodes" {
						cands, err = bs.NodeValidEndCandidatesMetered([]uint16{1}, 1_000, 0, m.Tally("probe"))
					} else {
						cands, err = bs.RelValidEndCandidatesMetered(1, 1_000, 0, m.Tally("probe"))
					}
					var ids []int64
					for _, c := range cands {
						ids = append(ids, c.ID)
					}
					slices.Sort(ids)
					return ids, err
				}
				stats := func(bs *Store) storecontract.ValidEndStats {
					rels, nodes := bs.ValidEndStats()
					if kind == "nodes" {
						return nodes
					}
					return rels
				}
				// Rows decoded by the first call: a committed row is decoded once, a
				// buffered one is read from the overlay and the entity cache.
				wantDecodes := int64(0)
				if flushed {
					wantDecodes = members
				}

				// The control: no limit, every member, one count each, the rows decoded once.
				wantCandidates := uint64(4 * members)
				if flushed {
					wantCandidates += members - 1
				}
				ctl := open()
				_, free := storecontract.WithReadBudget(context.Background(), storecontract.ReadBudget{})
				want, err := call(ctl, free)
				if err != nil || len(want) != members || free.Work().Candidates != wantCandidates || ctl.ValidEndRowDecodesForTest() != wantDecodes {
					t.Fatalf("unlimited = %d members, %v, %+v, %d rows decoded; want %d members, %d candidates, %d rows decoded", len(want), err, free.Work(), ctl.ValidEndRowDecodesForTest(), members, wantCandidates, wantDecodes)
				}
				if st := stats(ctl); st.Tokens != 1 || st.Unknown != 0 {
					t.Fatalf("after the control: %+v; want the token tracked and nothing unknown", st)
				}

				bs := open()
				_, m := storecontract.WithReadBudget(context.Background(), storecontract.ReadBudget{Candidates: room})
				_, err = call(bs, m)
				var rb *storecontract.ReadBudgetError
				if !errors.As(err, &rb) || rb.Limit != "candidates" {
					t.Fatalf("room %d for %d members: %v; want ErrReadBudget at candidates", room, members, err)
				}
				if st, decodes := stats(bs), bs.ValidEndRowDecodesForTest(); st.Tokens != 0 || st.Unknown != 0 || st.Entities != 0 || decodes != 0 {
					t.Fatalf("after the refusal: %+v, %d rows decoded; want the token untracked, nothing enlisted and nothing decoded before the first entity was counted", st, decodes)
				}
				if got := m.Work().Candidates; got != room+1 {
					t.Fatalf("the refusal counted %d candidates, want the %d members the count walk saw before it stopped", got, room+1)
				}
				// Room for exactly the members: the control's answer, and the refused call left the index exact.
				_, exact := storecontract.WithReadBudget(context.Background(), storecontract.ReadBudget{Candidates: wantCandidates})
				if got, err := call(bs, exact); err != nil || !slices.Equal(got, want) || exact.Work().Candidates != wantCandidates || bs.ValidEndRowDecodesForTest() != wantDecodes {
					t.Fatalf("room %d = %d members, %v, %+v, %d rows decoded; want the control's %d members, %d candidates and %d rows decoded", wantCandidates, len(got), err, exact.Work(), bs.ValidEndRowDecodesForTest(), members, wantCandidates, wantDecodes)
				}
			})
		}
	}
}
