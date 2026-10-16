package nodes

import (
	"errors"
	"strings"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/grapherr"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func (s *nodeOpsSpy) ValidHullByLabel(label string, pin types.Instant, opts storepkg.CountTimelineOpts) (storepkg.ValidHull, bool, error) {
	s.record("ValidHullByLabel:" + label)
	return storepkg.ValidHull{From: 3, To: 9}, true, s.err
}

func (s *nodeOpsSpy) DocValuesSnapshotMulti(labels []string, propKeys []string) (types.NodeColumnReader, uint64, bool, error) {
	s.record("DocValuesSnapshotMulti:" + strings.Join(labels, "+"))
	return nil, 4, true, s.err
}

func (s *nodeOpsSpy) DocValuesSnapshotOrdinal(label string, propKeys []string) (types.NodeColumnChunkReader, uint64, bool, error) {
	s.record("DocValuesSnapshotOrdinal:" + label)
	return nil, 5, true, s.err
}

func (s *nodeOpsSpy) DocValuesSnapshotMultiOrdinal(labels []string, propKeys []string) (types.NodeColumnChunkReader, uint64, bool, error) {
	s.record("DocValuesSnapshotMultiOrdinal:" + strings.Join(labels, "+"))
	return nil, 6, true, s.err
}

// TestRound10DoorsForward: the round 10 node doors forward their arguments
// and the Ops' answer and error; a nil API reports the nil graph (rule 1).
func TestRound10DoorsForward(t *testing.T) {
	wantErr := errors.New("ops failed")
	spy := &nodeOpsSpy{err: wantErr}
	a := New(spy)
	if h, ok, err := a.ValidHullByLabel("X", 5, storepkg.CountTimelineOpts{}); !errors.Is(err, wantErr) || !ok || h.From != 3 || h.To != 9 {
		t.Fatalf("ValidHullByLabel = %+v %v %v", h, ok, err)
	}
	if _, gen, ok, err := a.DocValuesSnapshotMulti([]string{"A", "B"}, nil); !errors.Is(err, wantErr) || !ok || gen != 4 {
		t.Fatalf("DocValuesSnapshotMulti = %v %v %v", gen, ok, err)
	}
	if _, gen, ok, err := a.DocValuesSnapshotOrdinal("A", nil); !errors.Is(err, wantErr) || !ok || gen != 5 {
		t.Fatalf("DocValuesSnapshotOrdinal = %v %v %v", gen, ok, err)
	}
	if _, gen, ok, err := a.DocValuesSnapshotMultiOrdinal([]string{"A", "C"}, nil); !errors.Is(err, wantErr) || !ok || gen != 6 {
		t.Fatalf("DocValuesSnapshotMultiOrdinal = %v %v %v", gen, ok, err)
	}
	if got := strings.Join(spy.calls, ","); got != "ValidHullByLabel:X,DocValuesSnapshotMulti:A+B,DocValuesSnapshotOrdinal:A,DocValuesSnapshotMultiOrdinal:A+C" {
		t.Fatalf("calls %s", got)
	}
	var nilAPI *API
	if _, _, err := nilAPI.ValidHullByLabel("X", 5, storepkg.CountTimelineOpts{}); !errors.Is(err, grapherr.ErrNilGraph) {
		t.Fatalf("nil ValidHullByLabel: %v", err)
	}
	if _, _, _, err := nilAPI.DocValuesSnapshotMulti([]string{"A"}, nil); !errors.Is(err, grapherr.ErrNilGraph) {
		t.Fatalf("nil DocValuesSnapshotMulti: %v", err)
	}
	if _, _, _, err := nilAPI.DocValuesSnapshotOrdinal("A", nil); !errors.Is(err, grapherr.ErrNilGraph) {
		t.Fatalf("nil DocValuesSnapshotOrdinal: %v", err)
	}
	if _, _, _, err := nilAPI.DocValuesSnapshotMultiOrdinal([]string{"A"}, nil); !errors.Is(err, grapherr.ErrNilGraph) {
		t.Fatalf("nil DocValuesSnapshotMultiOrdinal: %v", err)
	}
}
