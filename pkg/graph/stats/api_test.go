package stats

import (
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/grapherr"
)

func TestAPINilReceiversReturnErrNilGraph(t *testing.T) {
	t.Parallel()

	var nilAPI *API
	for _, check := range []struct {
		name string
		fn   func() (int, error)
	}{
		{name: "NodeCount", fn: nilAPI.NodeCount},
		{name: "RelCount", fn: nilAPI.RelCount},
		{name: "NodeCountByLabel", fn: func() (int, error) { return nilAPI.NodeCountByLabel("Person") }},
		{name: "NodeCountByLabelAndPropertyKey", fn: func() (int, error) { return nilAPI.NodeCountByLabelAndPropertyKey("Person", "id") }},
		{name: "RelCountByType", fn: func() (int, error) { return nilAPI.RelCountByType("KNOWS") }},
	} {
		got, err := check.fn()
		if got != 0 {
			t.Fatalf("%s nil count = %d, want 0", check.name, got)
		}
		if !errors.Is(err, grapherr.ErrNilGraph) {
			t.Fatalf("%s nil error = %v, want ErrNilGraph", check.name, err)
		}
	}
	if got, err := nilAPI.Get(); got != (GraphStats{}) || !errors.Is(err, grapherr.ErrNilGraph) {
		t.Fatalf("nil Get = (%+v, %v), want (zero, ErrNilGraph)", got, err)
	}
	if got, err := nilAPI.AllLabelCounts(); got != nil || !errors.Is(err, grapherr.ErrNilGraph) {
		t.Fatalf("nil AllLabelCounts = (%v, %v), want (nil, ErrNilGraph)", got, err)
	}
	if got, err := nilAPI.AllRelTypeCounts(); got != nil || !errors.Is(err, grapherr.ErrNilGraph) {
		t.Fatalf("nil AllRelTypeCounts = (%v, %v), want (nil, ErrNilGraph)", got, err)
	}

	api := New((*statsOpsSpy)(nil))
	if got, err := api.NodeCount(); got != 0 || !errors.Is(err, grapherr.ErrNilGraph) {
		t.Fatalf("typed-nil NodeCount = (%d, %v), want (0, ErrNilGraph)", got, err)
	}
	if got, err := api.Get(); got != (GraphStats{}) || !errors.Is(err, grapherr.ErrNilGraph) {
		t.Fatalf("typed-nil Get = (%+v, %v), want (zero, ErrNilGraph)", got, err)
	}
}

// TestAPIGetPropagatesSnapshotError pins the close-error contract: when the
// backing ops report a snapshot error (e.g. ErrGraphClosed from a closed
// graph), API.Get must surface it. Pre-fix, SnapshotCounters silently
// dropped the error and Get reported nil — breaking the documented
// fail-closed shape that every other Stats method honours.
func TestAPIGetPropagatesSnapshotError(t *testing.T) {
	t.Parallel()
	want := errors.New("graph: graph is closed (test sentinel)")
	api := New(&statsOpsSpy{
		snapshot:    [12]int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12},
		snapshotErr: want,
	})
	got, err := api.Get()
	if !errors.Is(err, want) {
		t.Fatalf("Get error = %v, want propagated snapshotErr", err)
	}
	// Counter snapshot must still be populated so callers can observe final state.
	if got.NodesAdded != 1 || got.RelCacheMisses != 12 {
		t.Fatalf("Get counters partially zero on snapshot error: %+v", got)
	}
}

func TestAPIForwardsMethodsAndMapsSnapshotCounters(t *testing.T) {
	t.Parallel()

	ops := &statsOpsSpy{
		nodeCount:        10,
		relCount:         20,
		nodeCountByLabel: 3,
		nodePropKeyCount: 2,
		relCountByType:   4,
		labelCounts:      map[string]int{"Person": 3},
		relTypeCounts:    map[string]int{"KNOWS": 4},
		snapshot:         [12]int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12},
	}
	api := New(ops)

	if got, err := api.NodeCount(); got != 10 || err != nil {
		t.Fatalf("NodeCount = (%d, %v), want (10, nil)", got, err)
	}
	if got, err := api.RelCount(); got != 20 || err != nil {
		t.Fatalf("RelCount = (%d, %v), want (20, nil)", got, err)
	}
	if got, err := api.NodeCountByLabel("Person"); got != 3 || err != nil {
		t.Fatalf("NodeCountByLabel = (%d, %v), want (3, nil)", got, err)
	}
	if got, err := api.NodeCountByLabelAndPropertyKey("Person", "id"); got != 2 || err != nil {
		t.Fatalf("NodeCountByLabelAndPropertyKey = (%d, %v), want (2, nil)", got, err)
	}
	if got, err := api.RelCountByType("KNOWS"); got != 4 || err != nil {
		t.Fatalf("RelCountByType = (%d, %v), want (4, nil)", got, err)
	}
	labelCounts, err := api.AllLabelCounts()
	if err != nil || labelCounts["Person"] != 3 {
		t.Fatalf("AllLabelCounts = (%v, %v), want Person=3 nil", labelCounts, err)
	}
	relTypeCounts, err := api.AllRelTypeCounts()
	if err != nil || relTypeCounts["KNOWS"] != 4 {
		t.Fatalf("AllRelTypeCounts = (%v, %v), want KNOWS=4 nil", relTypeCounts, err)
	}

	if got, err := api.Get(); err != nil || got != (GraphStats{
		NodesAdded:      1,
		NodesRead:       2,
		NodesUpdated:    3,
		NodesDeleted:    4,
		RelsAdded:       5,
		RelsRead:        6,
		RelsUpdated:     7,
		RelsDeleted:     8,
		NodeCacheHits:   9,
		NodeCacheMisses: 10,
		RelCacheHits:    11,
		RelCacheMisses:  12,
	}) {
		t.Fatalf("Get = (%+v, %v), want mapped snapshot", got, err)
	}

	if ops.nodeLabelArg != "Person" || ops.relTypeArg != "KNOWS" {
		t.Fatalf("forwarded args = label %q type %q", ops.nodeLabelArg, ops.relTypeArg)
	}
	if ops.nodeCountCalls != 1 || ops.relCountCalls != 1 || ops.nodeCountByLabelCalls != 1 ||
		ops.relCountByTypeCalls != 1 || ops.allLabelCountsCalls != 1 ||
		ops.allRelTypeCountsCalls != 1 || ops.snapshotCountersCalls != 1 {
		t.Fatalf("unexpected call counts: %+v", ops)
	}
}

func TestAPIAllCountsReturnIndependentMaps(t *testing.T) {
	t.Parallel()

	ops := &statsOpsSpy{
		labelCounts:   map[string]int{"Person": 3},
		relTypeCounts: map[string]int{"KNOWS": 4},
	}
	api := New(ops)

	labelCounts, err := api.AllLabelCounts()
	if err != nil {
		t.Fatalf("AllLabelCounts: %v", err)
	}
	labelCounts["Person"] = 99
	labelCounts["Injected"] = 1
	if ops.labelCounts["Person"] != 3 {
		t.Fatalf("mutating AllLabelCounts result changed ops map: %v", ops.labelCounts)
	}
	if _, ok := ops.labelCounts["Injected"]; ok {
		t.Fatalf("mutating AllLabelCounts result inserted into ops map: %v", ops.labelCounts)
	}

	relTypeCounts, err := api.AllRelTypeCounts()
	if err != nil {
		t.Fatalf("AllRelTypeCounts: %v", err)
	}
	relTypeCounts["KNOWS"] = 99
	relTypeCounts["INJECTED"] = 1
	if ops.relTypeCounts["KNOWS"] != 4 {
		t.Fatalf("mutating AllRelTypeCounts result changed ops map: %v", ops.relTypeCounts)
	}
	if _, ok := ops.relTypeCounts["INJECTED"]; ok {
		t.Fatalf("mutating AllRelTypeCounts result inserted into ops map: %v", ops.relTypeCounts)
	}
}

func TestAPIPropagatesOpsErrors(t *testing.T) {
	t.Parallel()

	opErr := errors.New("stats op failed")
	api := New(&statsOpsSpy{
		nodeCountErr:        opErr,
		relCountErr:         opErr,
		nodeCountByLabelErr: opErr,
		nodePropKeyCountErr: opErr,
		relCountByTypeErr:   opErr,
		allLabelCountsErr:   opErr,
		allRelTypeCountsErr: opErr,
	})

	for _, check := range []struct {
		name string
		err  error
	}{
		{name: "NodeCount", err: mustErr(api.NodeCount())},
		{name: "RelCount", err: mustErr(api.RelCount())},
		{name: "NodeCountByLabel", err: mustErr(api.NodeCountByLabel("Person"))},
		{name: "NodeCountByLabelAndPropertyKey", err: mustErr(api.NodeCountByLabelAndPropertyKey("Person", "id"))},
		{name: "RelCountByType", err: mustErr(api.RelCountByType("KNOWS"))},
	} {
		if !errors.Is(check.err, opErr) {
			t.Fatalf("%s error = %v, want %v", check.name, check.err, opErr)
		}
	}
	if _, err := api.AllLabelCounts(); !errors.Is(err, opErr) {
		t.Fatalf("AllLabelCounts error = %v, want %v", err, opErr)
	}
	if _, err := api.AllRelTypeCounts(); !errors.Is(err, opErr) {
		t.Fatalf("AllRelTypeCounts error = %v, want %v", err, opErr)
	}
}

func mustErr(_ int, err error) error { return err }

type statsOpsSpy struct {
	nodeCount        int
	relCount         int
	nodeCountByLabel int
	nodePropKeyCount int
	relCountByType   int
	labelCounts      map[string]int
	relTypeCounts    map[string]int
	snapshot         [12]int64

	nodeCountErr        error
	relCountErr         error
	nodeCountByLabelErr error
	nodePropKeyCountErr error
	relCountByTypeErr   error
	allLabelCountsErr   error
	allRelTypeCountsErr error
	snapshotErr         error

	nodeLabelArg   string
	nodePropLabel  string
	nodePropKeyArg string
	relTypeArg     string

	nodeCountCalls        int
	relCountCalls         int
	nodeCountByLabelCalls int
	nodePropKeyCountCalls int
	relCountByTypeCalls   int
	allLabelCountsCalls   int
	allRelTypeCountsCalls int
	snapshotCountersCalls int
}

func (s *statsOpsSpy) NodeCount() (int, error) {
	s.nodeCountCalls++
	return s.nodeCount, s.nodeCountErr
}

func (s *statsOpsSpy) RelCount() (int, error) {
	s.relCountCalls++
	return s.relCount, s.relCountErr
}

func (s *statsOpsSpy) NodeCountByLabel(label string) (int, error) {
	s.nodeCountByLabelCalls++
	s.nodeLabelArg = label
	return s.nodeCountByLabel, s.nodeCountByLabelErr
}

func (s *statsOpsSpy) NodeCountByLabelAndPropertyKey(label, propertyKey string) (int, error) {
	s.nodePropKeyCountCalls++
	s.nodePropLabel = label
	s.nodePropKeyArg = propertyKey
	return s.nodePropKeyCount, s.nodePropKeyCountErr
}

func (s *statsOpsSpy) RelCountByType(typeName string) (int, error) {
	s.relCountByTypeCalls++
	s.relTypeArg = typeName
	return s.relCountByType, s.relCountByTypeErr
}

func (s *statsOpsSpy) AllLabelCounts() (map[string]int, error) {
	s.allLabelCountsCalls++
	return s.labelCounts, s.allLabelCountsErr
}

func (s *statsOpsSpy) AllRelTypeCounts() (map[string]int, error) {
	s.allRelTypeCountsCalls++
	return s.relTypeCounts, s.allRelTypeCountsErr
}

func (s *statsOpsSpy) SnapshotCounters() (
	nodesAdded, nodesRead, nodesUpdated, nodesDeleted int64,
	relsAdded, relsRead, relsUpdated, relsDeleted int64,
	nodeCacheHits, nodeCacheMisses, relCacheHits, relCacheMisses int64,
	err error,
) {
	s.snapshotCountersCalls++
	return s.snapshot[0], s.snapshot[1], s.snapshot[2], s.snapshot[3],
		s.snapshot[4], s.snapshot[5], s.snapshot[6], s.snapshot[7],
		s.snapshot[8], s.snapshot[9], s.snapshot[10], s.snapshot[11],
		s.snapshotErr
}
