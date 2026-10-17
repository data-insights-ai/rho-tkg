package temporal

import (
	"fmt"
	"sort"

	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Interval is one half-open valid-time interval [From, To) of a Window.
// Both bounds are real instants: types.UnboundedValidFrom (math.MinInt64)
// is the far past, math.MaxInt64 the far future, 0 the instant 0 (no value
// stands for "unset").
type Interval struct {
	From, To types.Instant
}

// Window is a valid-time window of the windowed effective reads
// (ForEachNodeEffectiveByLabelsWindow, ForEachRelEffectiveByTypeWindow,
// ForEachRelEffectiveAtNodesWindow, ScanRelEffectiveColumnsAtNodes): the
// union of disjoint half-open intervals, ascending (each From < To, each
// interval's To <= the next one's From; adjacent intervals may touch). A
// segment [ValidFrom, ValidTo) of a timeline (ValidTo open by the store's
// valid axis: 0 on the legacy axis, types.InfiniteValidTo on the literal one) is read when
// it meets the window: it overlaps at least one interval
// (ValidFrom < To and ValidTo open or > From, the overlap rule of every
// valid-time interval filter). The empty window meets nothing; "any valid
// time" is Window{{types.UnboundedValidFrom, math.MaxInt64}} (it reads every
// segment that starts before math.MaxInt64).
type Window []Interval

// Validate returns store.ErrInvalidTimeRange for an interval with From >= To
// or intervals out of order or overlapping.
func (w Window) Validate() error {
	for i, iv := range w {
		if iv.From >= iv.To {
			return fmt.Errorf("%w: window interval %d [%d, %d) is empty", storepkg.ErrInvalidTimeRange, i, iv.From, iv.To)
		}
		if i > 0 && w[i-1].To > iv.From {
			return fmt.Errorf("%w: window interval %d [%d, %d) overlaps or precedes [%d, %d)", storepkg.ErrInvalidTimeRange,
				i, iv.From, iv.To, w[i-1].From, w[i-1].To)
		}
	}
	return nil
}

// Meets reports whether the valid span [from, to) (to 0: open, the legacy
// valid axis) overlaps an interval of the validated window w: the first
// interval ending after from decides (the later ones start later still).
func (w Window) Meets(from, to types.Instant) bool {
	return w.MeetsOn(types.LegacyValidAxis, from, to)
}

// MeetsOn is Meets for a span stored on the valid axis a (round 11): on the
// literal axis to is an instant (types.InfiniteValidTo open, 0 the instant
// 0).
func (w Window) MeetsOn(a types.ValidAxis, from, to types.Instant) bool {
	i := sort.Search(len(w), func(i int) bool { return w[i].To > from })
	return i < len(w) && storeutil.IntervalMeets(a, from, to, w[i].From, w[i].To)
}
