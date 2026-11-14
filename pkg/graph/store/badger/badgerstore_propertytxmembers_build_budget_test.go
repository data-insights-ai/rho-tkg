package badger

import (
	"errors"
	"math"
	"slices"
	"strings"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestPropertyMembershipFirstBuildBudget(t *testing.T) {
	for _, rel := range []bool{false, true} {
		for _, pending := range []bool{false, true} {
			for _, limit := range []string{"candidates", "decoded bytes", "buffer bytes", "history examined"} {
				t.Run(map[bool]string{false: "node", true: "rel"}[rel]+"/"+map[bool]string{false: "committed", true: "pending"}[pending]+"/"+limit, func(t *testing.T) {
					bs, err := New(Config{InMemory: true, PropertyKeyRegistry: registrypkg.NewPropertyKeyRegistry(), MaxPendingWrites: -1})
					if err != nil {
						t.Fatal(err)
					}
					defer bs.Close()
					must := func(err error) {
						t.Helper()
						if err != nil {
							t.Fatal(err)
						}
					}
					if rel {
						must(bs.CreateRelPropertyIndex(1, "k"))
						must(bs.PutNode(types.NewNode(1, 2, []uint16{2})))
						must(bs.PutNode(types.NewNode(2, 2, []uint16{2})))
					} else {
						must(bs.CreatePropertyIndex(1, "k"))
					}
					var want []int64
					for i := range 6 {
						id := int64(101 + i)
						want = append(want, id)
						if rel {
							r := types.NewRelationship(types.RelID(id), 1, 1, 2)
							must(r.SetProperty("k", "v"))
							r.SetTemporal(&types.TemporalMetadata{TxFrom: 5, ValidFrom: 5})
							if limit == "history examined" {
								must(bs.PutRelVersion(r.ID(), 0, r))
							} else {
								must(bs.PutRelationship(r))
							}
						} else {
							n := types.NewNode(types.NodeID(id), 1, []uint16{1})
							must(n.SetProperty("k", "v"))
							n.SetTemporal(&types.TemporalMetadata{TxFrom: 5, ValidFrom: 5})
							if limit == "history examined" {
								must(bs.PutNodeVersion(n.ID(), 0, n))
							} else {
								must(bs.PutNode(n))
							}
						}
					}
					if !pending {
						must(bs.Flush())
					}
					read := func(b storecontract.ReadBudget) ([]int64, int64, error) {
						_, m := storecontract.WithReadBudget(t.Context(), b)
						decoded, stop := CountPropertyMemberDecodesForTest()
						defer stop()
						copied, stopCopies := CountPropertyMemberBytesCopiedForTest()
						defer stopCopies()
						var got []int64
						keys := []string{types.IndexablePropertyValueKey("v")}
						var err error
						if rel {
							err = bs.ForEachRelPropertyTxMemberMetered(1, "k", keys, nil, m.Tally("build"), func(id types.RelID, _ types.Instant) bool { got = append(got, int64(id)); return true })
						} else {
							err = bs.ForEachNodePropertyTxMemberMetered(1, "k", keys, nil, m.Tally("build"), func(id types.NodeID, _ types.Instant) bool { got = append(got, int64(id)); return true })
						}
						slices.Sort(got)
						if b.BufferBytes == 1 && copied() != 0 {
							t.Errorf("copied %d native bytes before buffer refusal", copied())
						}
						return got, decoded(), err
					}
					budget := map[string]storecontract.ReadBudget{"candidates": {Candidates: 3}, "decoded bytes": {DecodedBytes: 1}, "buffer bytes": {BufferBytes: 1}, "history examined": {HistoryExamined: 1}}[limit]
					got, decodes, err := read(budget)
					var refusal *storecontract.ReadBudgetError
					stats, statErr := bs.PropertyTxMembershipStats()
					if !errors.As(err, &refusal) || refusal.Limit != limit || len(got) != 0 || stats.Builds != 0 || statErr != nil {
						t.Errorf("limited: ids %v, decodes %d, stats %+v, err %v / %v; want %s refusal before installation", got, decodes, stats, err, statErr, limit)
					}
					if limit == "decoded bytes" && decodes != 0 {
						t.Errorf("decoded %d rows before refusal", decodes)
					}
					got, decodes, err = read(storecontract.ReadBudget{})
					if err != nil || !slices.Equal(got, want) || decodes != 6 {
						t.Fatalf("retry: ids %v, decodes %d, err %v; want %v and six native decodes", got, decodes, err, want)
					}
				})
			}
		}
	}
}

func TestPropertyMembershipDeltaAnchorAdmission(t *testing.T) {
	bs, err := New(Config{InMemory: true, PropertyKeyRegistry: registrypkg.NewPropertyKeyRegistry(), MaxPendingWrites: -1, HistoryDeltaEncoding: true, HistoryAnchorInterval: 8})
	if err != nil {
		t.Fatal(err)
	}
	defer bs.Close()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	for version := range uint32(2) {
		n := types.NewNode(101, 1, []uint16{1})
		n.SetVersion(version)
		n.SetTemporal(&types.TemporalMetadata{TxFrom: types.Instant(version + 1), ValidFrom: 1})
		must(n.SetProperty("k", "v"))
		must(n.SetProperty("payload", strings.Repeat("x", 8192)))
		must(bs.PutNodeVersion(n.ID(), version, n))
		if version == 0 {
			must(bs.Flush())
		}
	}
	anchorRaw, err := bs.readHistoryNodeRaw(101, 0)
	must(err)
	deltaRaw, err := bs.readHistoryNodeRaw(101, 1)
	must(err)
	if storepkg.HistoryValueKindOf(deltaRaw) != storepkg.HistoryDelta {
		t.Fatal("fixture did not produce a history delta")
	}
	read := func(b storecontract.ReadBudget) ([]uint64, int64, int64, storecontract.ReadWork, error) {
		_, m := storecontract.WithReadBudget(t.Context(), b)
		decoded, stop := CountPropertyMemberDecodesForTest()
		defer stop()
		copied, stopCopies := CountPropertyMemberBytesCopiedForTest()
		defer stopCopies()
		var versions []uint64
		err := bs.scanPropTxRowsUnder(storepkg.KeyNode, storepkg.KeyHistNode, m.Tally("scan"), func(id snowflake.ID, hist bool, version uint64, raw []byte, local propertyAnchorFunc) error {
			n, err := bs.decodeScannedNodeUnder(func(w *storepkg.NodeWire) bool { return w.PrimaryLabel == 1 }, id, hist, version, raw, local, m.Tally("decode"))
			if err == nil && n != nil {
				versions = append(versions, version)
			}
			return err
		})
		slices.Sort(versions)
		return versions, decoded(), copied(), m.Work(), err
	}
	want := []uint64{0, 1}
	got, decodes, copies, work, err := read(storecontract.ReadBudget{})
	if err != nil || !slices.Equal(got, want) || decodes != 3 || copies != int64(len(deltaRaw)+2*len(anchorRaw)) || work.HistoryExamined != 4 {
		t.Fatalf("control: versions %v, decodes %d, copies %d, work %+v, err %v", got, decodes, copies, work, err)
	}
	// A pending delta must read its committed anchor again after the committed
	// scan. The second anchor copy is refused before allocation, not translated
	// into a cache miss and retried through an unmetered read.
	budget := uint64(72 + len(deltaRaw) + 32 + len(anchorRaw) + len(anchorRaw) - 1)
	got, decodes, copies, _, err = read(storecontract.ReadBudget{BufferBytes: budget})
	var refusal *storecontract.ReadBudgetError
	if !errors.As(err, &refusal) || refusal.Limit != "buffer bytes" || !slices.Equal(got, []uint64{0}) || decodes != 2 || copies != int64(len(deltaRaw)+len(anchorRaw)) {
		t.Fatalf("anchor copy: versions %v, decodes %d, copies %d, err %v", got, decodes, copies, err)
	}
	got, decodes, _, _, err = read(storecontract.ReadBudget{DecodedBytes: work.DecodedBytes - 1})
	if !errors.As(err, &refusal) || refusal.Limit != "decoded bytes" || !slices.Equal(got, []uint64{0}) || decodes != 2 {
		t.Fatalf("anchor decode: versions %v, decodes %d, err %v", got, decodes, err)
	}
	got, decodes, _, _, err = read(storecontract.ReadBudget{HistoryExamined: work.HistoryExamined - 1})
	if !errors.As(err, &refusal) || refusal.Limit != "history examined" || !slices.Equal(got, []uint64{0}) || decodes != 2 {
		t.Fatalf("anchor history: versions %v, decodes %d, err %v", got, decodes, err)
	}
}

func TestRAMWindowBuildAdmissionCumulativeAcrossKeys(t *testing.T) {
	for _, limited := range []bool{false, true} {
		t.Run(map[bool]string{false: "unlimited", true: "limited"}[limited], func(t *testing.T) {
			bs, err := New(Config{InMemory: true, PropertyKeyRegistry: registrypkg.NewPropertyKeyRegistry(), MaxPendingWrites: -1})
			if err != nil {
				t.Fatal(err)
			}
			defer bs.Close()
			if err = bs.CreatePropertyIndex(1, "k"); err != nil {
				t.Fatal(err)
			}
			keys := []string{types.IndexablePropertyValueKey("a"), types.IndexablePropertyValueKey("b")}
			var want []types.NodeID
			for i := range 8 {
				n := types.NewNode(types.NodeID(i+1), 1, []uint16{1})
				v := "a"
				if i >= 4 {
					v = "b"
				}
				_ = n.SetProperty("k", v)
				n.SetTemporal(&types.TemporalMetadata{TxFrom: 1, ValidFrom: 100, ValidTo: 200})
				if err = bs.PutNode(n); err != nil {
					t.Fatal(err)
				}
				want = append(want, n.ID())
			}
			if err = bs.Flush(); err != nil {
				t.Fatal(err)
			}
			if err = bs.ForEachNodePropertyTxMember(1, "k", keys[0], func(types.NodeID, types.Instant) bool { return true }); err != nil {
				t.Fatal(err)
			}
			budget := storecontract.ReadBudget{}
			if limited {
				budget.Candidates = 9
			}
			_, m := storecontract.WithReadBudget(t.Context(), budget)
			var got []types.NodeID
			err = bs.ForEachNodePropertyTxMemberMetered(1, "k", keys, &storecontract.PropertyTxMemberWindow{ValidStart: 100, ValidEnd: 101, TxAt: math.MaxInt64}, m.Tally("window"), func(id types.NodeID, _ types.Instant) bool { got = append(got, id); return true })
			slices.Sort(got)
			bs.idxMu.RLock()
			sc := bs.nodePropTx[indexpkg.PropertyIndexKey{LabelToken: 1, PropertyKey: "k"}]
			_, first := sc.members.MembersDuring(keys[0], 100, 101, math.MaxInt64)
			_, second := sc.members.MembersDuring(keys[1], 100, 101, math.MaxInt64)
			bs.idxMu.RUnlock()
			if limited {
				var refusal *storecontract.ReadBudgetError
				if !errors.As(err, &refusal) || refusal.Limit != "candidates" || !slices.Equal(got, want[:4]) || !first || second || m.Work().Candidates != 12 {
					t.Fatalf("limited: ids %v, views %v/%v, work %+v, err %v; want first key only and refusal before second view build", got, first, second, m.Work(), err)
				}
			} else if err != nil || !slices.Equal(got, want) || !first || !second || m.Work().Candidates != 16 {
				t.Fatalf("control: ids %v, views %v/%v, work %+v, err %v; want eight exact IDs, eight builds plus eight walk visits", got, first, second, m.Work(), err)
			}
		})
	}
}
