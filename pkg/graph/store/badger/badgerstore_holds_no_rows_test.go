package badger

import (
	"testing"

	badgerv4 "github.com/dgraph-io/badger/v4"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
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

// TestHoldsNoRows_ChangeLogOnly: a change-log record is a row, since a shard
// skipped as empty would drop it from a change-log read.
func TestHoldsNoRows_ChangeLogOnly(t *testing.T) {
	t.Parallel()
	bs, err := New(Config{Dir: t.TempDir(), FlushInterval: 1<<63 - 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	if err := bs.db.Update(func(txn *badgerv4.Txn) error {
		return txn.Set([]byte{storepkg.KeyChangeLog, 0, 0, 0, 0, 0, 0, 0, 1}, []byte{1})
	}); err != nil {
		t.Fatal(err)
	}
	none, err := bs.HoldsNoRows()
	if err != nil {
		t.Fatal(err)
	}
	if none {
		t.Fatal("a shard holding only a change-log record counts as empty")
	}
}
