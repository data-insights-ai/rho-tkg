package core

import (
	"slices"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// historyEnumCounter observes the native enumeration boundary independently
// of the read meter. The embedded store still performs the actual walk.
type historyEnumCounter struct {
	storepkg.MandatoryStore
	calls, ids int
}

func (s *historyEnumCounter) ForEachNodeHistoryID(fn func(types.NodeID) bool) error {
	s.calls++
	return s.MandatoryStore.ForEachNodeHistoryID(func(id types.NodeID) bool {
		s.ids++
		return fn(id)
	})
}

func (s *historyEnumCounter) ForEachRelHistoryID(fn func(types.RelID) bool) error {
	s.calls++
	return s.MandatoryStore.ForEachRelHistoryID(func(id types.RelID) bool {
		s.ids++
		return fn(id)
	})
}

// These fallback helpers collect their history union before handing out an
// ID. Without native admission, even an unlimited meter must refuse before
// entering that enumeration; charging the completed union is too late.
func TestReadBudget_UnmeteredHistoryUnionRefused(t *testing.T) {
	t.Parallel()
	for _, be := range rbdBackends() {
		if be.name != "sharded" {
			continue
		}
		g, shut := be.open(t, t.TempDir())
		defer shut()
		w := sbBuild(t, g, 4)
		counter := &historyEnumCounter{MandatoryStore: g.store}
		g.store = counter
		// Capability caches must match the wrapper's deliberately restricted
		// surface, rather than the native store discovered at construction.
		g.labelTxMembers = labelTxMembershipCapability(counter)
		g.relTypeTxMembers = relTypeTxMembershipCapability(counter)
		label, _ := g.labels.Lookup(sbLabel)
		other, _ := g.labels.Lookup("Ev")
		relType, _ := g.relTypes.Lookup(sbType)
		nodes := make([]int64, 0, len(w.nodes))
		rels := make([]int64, 0, len(w.rels))
		for _, id := range w.nodes {
			nodes = append(nodes, int64(id))
		}
		for _, id := range w.rels {
			rels = append(rels, int64(id))
		}
		multi := append(slices.Clone(nodes), int64(w.end))
		slices.Sort(multi)
		for _, tc := range []struct {
			name string
			want []int64
			run  func(storepkg.QueryOpts) ([]int64, error)
		}{
			{"node property", nodes, func(opts storepkg.QueryOpts) ([]int64, error) {
				var got []int64
				err := g.forEachNodePropertyCandidateID(label, nil, func() ([]types.NodeID, error) { return nil, nil }, opts, func(id types.NodeID) error {
					got = append(got, int64(id))
					return nil
				})
				return got, err
			}},
			{"rel property", rels, func(opts storepkg.QueryOpts) ([]int64, error) {
				var got []int64
				err := g.forEachRelPropertyCandidateID(relType, "missing", equalKeys{types.IndexablePropertyValueKey(int64(1))}, func() ([]types.RelID, error) { return nil, nil }, opts, func(id types.RelID) error {
					got = append(got, int64(id))
					return nil
				})
				return got, err
			}},
			{"multi-label", multi, func(opts storepkg.QueryOpts) ([]int64, error) {
				ids, err := g.nodeLabelsCandidatesAtOpts([]uint16{label, other}, opts)
				var got []int64
				for _, id := range ids {
					got = append(got, int64(id))
				}
				return got, err
			}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				opts := storepkg.QueryOpts{TxPin: w.pin}
				counter.calls, counter.ids = 0, 0
				got, err := tc.run(opts)
				slices.Sort(got)
				if err != nil || !slices.Equal(got, tc.want) || counter.calls == 0 || counter.ids == 0 {
					t.Fatalf("plain = %v, %v, %d native enum calls/%d IDs; want %v and a real history fold", got, err, counter.calls, counter.ids, tc.want)
				}
				for _, budget := range []storepkg.ReadBudget{{Candidates: 10000}, {}} {
					counter.calls, counter.ids = 0, 0
					ctx, outer := storepkg.WithReadBudget(t.Context(), budget)
					_, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
					got, err := tc.run(opts.WithReadMeter(meter, "history union"))
					rb := rbdBudgetErr(t, err, "unsupported")
					if len(got) != 0 || counter.calls != 0 || counter.ids != 0 || rb.Door != "history union" || outer.Exceeded() == nil || meter.Exceeded() == nil {
						t.Fatalf("budget %+v = %v, %v, %d native enum calls/%d IDs; want shared refusal before the native enumeration", budget, got, err, counter.calls, counter.ids)
					}
				}
			})
		}
	}
}
