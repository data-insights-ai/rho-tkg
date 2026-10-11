package badger

import (
	"testing"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestCompletedDiskPropertyIndexReopenDoesNotDecodeRowsPerDefinition(t *testing.T) {
	dir := t.TempDir()
	bs, err := New(Config{Dir: dir, PropertyIndexOnDisk: true, LabelIndexOnDisk: true, PropertyKeyRegistry: registrypkg.NewPropertyKeyRegistry()})
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"value", "absent1", "absent2", "absent3"} {
		if err := bs.CreatePropertyIndex(1, key); err != nil {
			t.Fatal(err)
		}
	}
	for i := range 20 {
		n := types.NewNode(types.NodeID(i+1), 1, nil)
		if err := n.SetProperty("value", int64(i)); err != nil {
			t.Fatal(err)
		}
		if err := bs.PutNode(n); err != nil {
			t.Fatal(err)
		}
	}
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}
	for _, readOnly := range []bool{false, true} {
		bs, err = New(Config{Dir: dir, PropertyIndexOnDisk: true, LabelIndexOnDisk: true, ReadOnly: readOnly})
		if err != nil {
			t.Fatal(err)
		}
		if got := bs.IndexRebuildStats().PropertyRowsRead; got != 0 {
			t.Fatal("completed disk index scanned rows per definition", got)
		}
		for i := range 20 {
			got := nodeIDsVia(t, bs, 1, "value", int64(i))
			if len(got) != 1 || got[0] != int64(i+1) {
				t.Fatal("disk index lost exact value", i, got)
			}
		}
		if got := nodeIDsVia(t, bs, 1, "absent1", int64(1)); len(got) != 0 {
			t.Fatal("empty definition manufactured membership", got)
		}
		if err := bs.Flush(); err != nil {
			t.Fatal(err)
		}
		if err := bs.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestEnablingDiskPropertyIndexStillBackfillsBeforeFastReopen(t *testing.T) {
	dir := t.TempDir()
	bs, err := New(Config{Dir: dir, PropertyKeyRegistry: registrypkg.NewPropertyKeyRegistry()})
	if err != nil {
		t.Fatal(err)
	}
	if err := bs.CreatePropertyIndex(1, "value"); err != nil {
		t.Fatal(err)
	}
	n := types.NewNode(1, 1, nil)
	if err := n.SetProperty("value", int64(7)); err != nil {
		t.Fatal(err)
	}
	if err := bs.PutNode(n); err != nil {
		t.Fatal(err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}
	bs, err = New(Config{Dir: dir, PropertyIndexOnDisk: true})
	if err != nil {
		t.Fatal(err)
	}
	if got := bs.IndexRebuildStats().PropertyRowsRead; got != 1 {
		t.Fatal("required one-time backfill skipped", got)
	}
	if got := nodeIDsVia(t, bs, 1, "value", int64(7)); len(got) != 1 || got[0] != 1 {
		t.Fatal("backfill lost existing value", got)
	}
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}
	bs, err = New(Config{Dir: dir, PropertyIndexOnDisk: true})
	if err != nil {
		t.Fatal(err)
	}
	defer bs.Close()
	if got := bs.IndexRebuildStats().PropertyRowsRead; got != 0 {
		t.Fatal("backfill repeated despite durable marker", got)
	}
	if got := nodeIDsVia(t, bs, 1, "value", int64(7)); len(got) != 1 || got[0] != 1 {
		t.Fatal("persisted backfill changed", got)
	}
}
