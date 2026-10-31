package badger

import (
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestHoldsNoRows: meta alone counts as empty; an unflushed row, a flushed row
// and a history row left by a delete do not.
func TestHoldsNoRows(t *testing.T) {
	t.Parallel()
	bs, err := New(Config{Dir: t.TempDir(), FlushInterval: 1<<63 - 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	want := func(step string, none bool) {
		t.Helper()
		got, err := bs.HoldsNoRows()
		if err != nil {
			t.Fatal(err)
		}
		if got != none {
			t.Errorf("%s: HoldsNoRows = %v, want %v", step, got, none)
		}
	}
	if err := bs.MetaSetDurable("some", []byte{1}); err != nil {
		t.Fatal(err)
	}
	want("meta only", true)
	const id = types.NodeID(1 << 20)
	if err := bs.PutNodeVersion(id, 0, types.NewNode(id, 1, nil)); err != nil {
		t.Fatal(err)
	}
	want("history row, unflushed", false)
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	want("history row, flushed", false)
}
