package badger

import (
	"errors"
	"testing"

	store "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestColumnTypedBoundsDeclineAfterAppendAndReopen(t *testing.T) {
	dir := t.TempDir()
	s, err := New(Config{Dir: dir, SyncWrites: true, ColumnsOnDisk: true})
	if err != nil {
		t.Fatal(err)
	}
	n := types.NewNode(1001, 7, nil)
	if err := n.SetProperty("n", int64(1)); err != nil {
		t.Fatal(err)
	}
	if err := s.PutNode(n); err != nil {
		t.Fatal(err)
	}
	calls := 0
	if err := s.ScanNodeColumns(7, []string{"n"}, store.QueryOpts{}, func(*store.ColumnBatch) bool { calls++; return true }); err != nil || calls != 1 {
		t.Fatal("ordinary columns unavailable", calls, err)
	}
	typed := types.NewNode(1002, 7, nil)
	tm := &types.TemporalMetadata{}
	if err := tm.SetValidBounds(types.TemporalRange{From: types.TemporalEndpoint{Value: -1, Finite: true}, To: types.TemporalEndpoint{Finite: true}}); err != nil {
		t.Fatal(err)
	}
	typed.SetTemporal(tm)
	if err := typed.SetProperty("n", int64(2)); err != nil {
		t.Fatal(err)
	}
	if err := s.PutNode(typed); err != nil {
		t.Fatal(err)
	}
	for pass := range 2 {
		calls = 0
		if err := s.ScanNodeColumns(7, []string{"n"}, store.QueryOpts{}, func(*store.ColumnBatch) bool { calls++; return true }); !errors.Is(err, store.ErrCapabilityNotSupported) || calls != 0 {
			t.Fatal("ambiguous scalar batch emitted", pass, calls, err)
		}
		if pass == 0 {
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			s, err = New(Config{Dir: dir, SyncWrites: true, ColumnsOnDisk: true})
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	defer s.Close()
	col, _, ok, err := s.DocValuesSnapshot(7, []string{"n"})
	if err != nil || !ok || col == nil {
		t.Fatal("property-only snapshot needlessly disabled", err)
	}
}
