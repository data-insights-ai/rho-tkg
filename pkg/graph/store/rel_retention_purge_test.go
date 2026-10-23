package store_test

import (
	"errors"
	"slices"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Backlog 21: store.RelRetentionPurgeCapability on every backend, driven
// directly (the graph-level tests cover the door). Relationships of type 5
// minted before the boundary go in every lifecycle — live, live with history,
// deleted (history only), history rows without a current row — with their
// rows, legs, ex-adjacency and history; type 6 and a later type-5
// relationship stay; chunks report More; a rerun, a non-positive boundary and
// type token 0 are no-ops.
func TestPurgeRelsByTypeBefore(t *testing.T) {
	t.Parallel()
	const evType, otherType = uint16(5), uint16(6)
	a, b := currentNodeID(1), currentNodeID(2)
	live, liveHist, deleted, histOnly, other := currentRelID(10), currentRelID(11), currentRelID(12), currentRelID(13), currentRelID(14)
	late := types.RelID(snowflake.ID(2*stampsIDBase + 15))
	purged := []types.RelID{live, liveHist, deleted, histOnly}
	before := late.MintInstant()
	if storeutil.SnowflakeInstant(histOnly.SnowflakeID()) >= before {
		t.Fatal("fixture: old IDs not below the boundary")
	}
	rel := func(id types.RelID, typ uint16, ver uint32, s, e types.NodeID) *types.Relationship {
		r := types.NewRelationship(id, typ, s, e)
		r.SetVersion(ver)
		return r
	}
	for _, be := range stampsBackends() {
		t.Run(be.name, func(t *testing.T) {
			st := be.open(t)
			p, ok := st.(storecontract.RelRetentionPurgeCapability)
			if !ok {
				t.Fatalf("%T lacks RelRetentionPurgeCapability", st)
			}
			for _, n := range []types.NodeID{a, b} {
				if err := st.PutNode(types.NewNode(n, 1, nil)); err != nil {
					t.Fatal(err)
				}
			}
			for _, r := range []*types.Relationship{
				rel(live, evType, 0, a, b), rel(liveHist, evType, 1, a, b), rel(deleted, evType, 1, b, a),
				rel(other, otherType, 0, a, b), rel(late, evType, 0, a, b),
			} {
				if err := st.PutRelationship(r); err != nil {
					t.Fatal(err)
				}
			}
			for _, r := range []*types.Relationship{rel(liveHist, evType, 0, a, b), rel(deleted, evType, 0, b, a), rel(histOnly, evType, 0, b, a)} {
				if err := st.PutRelVersion(r.ID(), r.Version(), r); err != nil {
					t.Fatal(err)
				}
			}
			if err := st.DeleteRelationship(deleted); err != nil {
				t.Fatal(err)
			}

			// Break cases first: nothing moves.
			for _, tc := range []struct {
				name   string
				tok    uint16
				before types.Instant
			}{{"zero before", evType, 0}, {"negative before", evType, -5}, {"type token 0", 0, before}, {"unknown type", 99, before}} {
				res, err := p.PurgeRelsByTypeBefore(tc.tok, tc.before, 0)
				if err != nil || res.RelsPurged != 0 || res.More {
					t.Fatalf("%s: %+v, %v; want a no-op", tc.name, res, err)
				}
			}

			total, calls := 0, 0
			for {
				res, err := p.PurgeRelsByTypeBefore(evType, before, 2)
				if err != nil {
					t.Fatal(err)
				}
				if res.RelsPurged > 2 {
					t.Fatalf("chunk of 2 purged %d", res.RelsPurged)
				}
				total += res.RelsPurged
				calls++
				if !res.More {
					break
				}
				if calls > 10 {
					t.Fatal("More never cleared")
				}
			}
			if total != len(purged) || calls < 2 {
				t.Fatalf("purged %d in %d calls, want %d in at least 2", total, calls, len(purged))
			}
			if res, err := p.PurgeRelsByTypeBefore(evType, before, 0); err != nil || res.RelsPurged != 0 {
				t.Fatalf("rerun = %+v, %v; want a no-op", res, err)
			}

			ex, _ := st.(storecontract.ExAdjacencyCapability)
			for _, id := range purged {
				if _, err := st.GetRelationship(id); !errors.Is(err, storecontract.ErrRelNotFound) {
					t.Fatalf("rel %d: GetRelationship = %v, want ErrRelNotFound", id, err)
				}
				if h, err := st.GetRelHistory(id); err != nil || len(h) != 0 {
					t.Fatalf("rel %d: %d history rows, %v", id, len(h), err)
				}
				if ex != nil {
					for _, n := range []types.NodeID{a, b} {
						for _, in := range []bool{false, true} {
							if slices.Contains(exAdjOf(t, ex, n, in), id) {
								t.Fatalf("rel %d still in the ex-adjacency of %d (incoming=%v)", id, n, in)
							}
						}
					}
				}
			}
			for _, id := range []types.RelID{other, late} {
				if _, err := st.GetRelationship(id); err != nil {
					t.Fatalf("survivor %d: %v", id, err)
				}
			}
			out, err := st.OutgoingRelationships(a, 0)
			if err != nil {
				t.Fatal(err)
			}
			var outIDs []types.RelID
			for _, r := range out {
				outIDs = append(outIDs, r.ID())
			}
			slices.Sort(outIDs)
			if want := []types.RelID{other, late}; !slices.Equal(outIDs, want) {
				t.Fatalf("Outgoing(a) = %v, want %v", outIDs, want)
			}
			if in, err := st.IncomingRelationships(a, 0); err != nil || len(in) != 0 {
				t.Fatalf("Incoming(a) = %d rels, %v; want none", len(in), err)
			}
			if sc, ok := st.(storecontract.StatsCapability); ok {
				if n, err := sc.RelCountByType(evType); err != nil || n != 1 {
					t.Fatalf("RelCountByType(5) = %d, %v; want 1", n, err)
				}
			}
			if l, ok := st.(storecontract.RelRangePurgeLogCapability); !ok {
				t.Fatalf("%T lacks RelRangePurgeLogCapability", st)
			} else if err := l.LogRelRangePurge(evType, before, 0); err != nil {
				t.Fatalf("LogRelRangePurge without a change-log: %v", err)
			}
		})
	}
}

// TestLogRelRangePurge: with a change-log the record is tag 14 carrying the
// predicate (memory and badger; sharded and tiered route to them and are
// covered by the graph feed test).
func TestLogRelRangePurge(t *testing.T) {
	bs, err := badger.New(badger.Config{InMemory: true, ChangeLog: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	for _, st := range []storecontract.Store{memory.New(memory.WithChangeLog()), bs} {
		l := st.(storecontract.RelRangePurgeLogCapability)
		if err := l.LogRelRangePurge(7, 12345, 0); err != nil {
			t.Fatal(err)
		}
		var got []storecontract.ChangeRecord
		if err := st.(storecontract.ChangeFeedCapability).ForEachChange(0, func(r storecontract.ChangeRecord) bool {
			got = append(got, r)
			return true
		}); err != nil {
			t.Fatal(err)
		}
		if len(got) != 1 || got[0].Tag != storecontract.ChangeRelRangePurge {
			t.Fatalf("%T: records %+v, want one tag-14 record", st, got)
		}
		body, err := storeutil.DecodeRelRangePurge(got[0].Payload)
		if err != nil || body != (storeutil.RelRangePurgeBody{TypeToken: 7, Before: 12345}) {
			t.Fatalf("%T: body %+v, %v", st, body, err)
		}
	}
	closed := memory.New()
	_ = closed.Close()
	if err := closed.LogRelRangePurge(1, 1, 0); !errors.Is(err, storecontract.ErrStoreClosed) {
		t.Fatalf("closed memory store: %v", err)
	}
	if _, err := closed.PurgeRelsByTypeBefore(1, 1, 0); !errors.Is(err, storecontract.ErrStoreClosed) {
		t.Fatalf("closed memory store purge: %v", err)
	}
}
