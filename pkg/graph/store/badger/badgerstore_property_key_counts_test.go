package badger

import (
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestBadgerStoreNodeCountByLabelAndPropertyKey(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	n1 := types.NewNode(types.NodeID(snowflake.ID(101)), 1, []uint16{2})
	if err := n1.SetProperty("id", int64(1)); err != nil {
		t.Fatalf("SetProperty n1.id: %v", err)
	}
	if err := bs.PutNode(n1); err != nil {
		t.Fatalf("PutNode n1: %v", err)
	}
	n2 := types.NewNode(types.NodeID(snowflake.ID(102)), 1, nil)
	if err := n2.SetProperty("id", int64(2)); err != nil {
		t.Fatalf("SetProperty n2.id: %v", err)
	}
	if err := bs.PutNode(n2); err != nil {
		t.Fatalf("PutNode n2: %v", err)
	}
	n3 := types.NewNode(types.NodeID(snowflake.ID(103)), 3, nil)
	if err := n3.SetProperty("tags", []string{"not", "indexable"}); err != nil {
		t.Fatalf("SetProperty n3.tags: %v", err)
	}
	if err := bs.PutNode(n3); err != nil {
		t.Fatalf("PutNode n3: %v", err)
	}

	if got, err := bs.NodeCountByLabelAndPropertyKey(1, "id"); err != nil || got != 2 {
		t.Fatalf("label 1 id count = (%d, %v), want (2, nil)", got, err)
	}
	if got, err := bs.NodeCountByLabelAndPropertyKey(2, "id"); err != nil || got != 1 {
		t.Fatalf("label 2 id count = (%d, %v), want (1, nil)", got, err)
	}
	if got, err := bs.NodeCountByLabelAndPropertyKey(3, "tags"); err != nil || got != 0 {
		t.Fatalf("label 3 tags count = (%d, %v), want (0, nil)", got, err)
	}

	updated := n2.DeepCopy()
	if _, err := updated.DeleteProperty("id"); err != nil {
		t.Fatalf("DeleteProperty updated.id: %v", err)
	}
	if err := bs.ReplaceNode(updated); err != nil {
		t.Fatalf("ReplaceNode updated: %v", err)
	}
	if got, err := bs.NodeCountByLabelAndPropertyKey(1, "id"); err != nil || got != 1 {
		t.Fatalf("label 1 id after replace = (%d, %v), want (1, nil)", got, err)
	}

	if err := bs.DeleteNode(n1.ID()); err != nil {
		t.Fatalf("DeleteNode n1: %v", err)
	}
	if got, err := bs.NodeCountByLabelAndPropertyKey(1, "id"); err != nil || got != 0 {
		t.Fatalf("label 1 id after delete = (%d, %v), want (0, nil)", got, err)
	}
	if got, err := bs.NodeCountByLabelAndPropertyKey(2, "id"); err != nil || got != 0 {
		t.Fatalf("label 2 id after delete = (%d, %v), want (0, nil)", got, err)
	}
}

func TestBadgerStoreNodeCountByLabelAndPropertyKeyRebuildsOnOpen(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	bs1, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("New bs1: %v", err)
	}
	n := types.NewNode(types.NodeID(snowflake.ID(201)), 7, []uint16{8})
	if err := n.SetProperty("id", int64(1)); err != nil {
		t.Fatalf("SetProperty n.id: %v", err)
	}
	if err := bs1.PutNode(n); err != nil {
		t.Fatalf("PutNode n: %v", err)
	}
	withoutID := types.NewNode(types.NodeID(snowflake.ID(202)), 9, nil)
	if err := withoutID.SetProperty("name", "no-id"); err != nil {
		t.Fatalf("SetProperty withoutID.name: %v", err)
	}
	if err := bs1.PutNode(withoutID); err != nil {
		t.Fatalf("PutNode withoutID: %v", err)
	}
	if err := bs1.Flush(); err != nil {
		t.Fatalf("Flush bs1: %v", err)
	}
	if err := bs1.Close(); err != nil {
		t.Fatalf("Close bs1: %v", err)
	}

	bs2, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("New bs2: %v", err)
	}
	defer bs2.Close()

	if got, err := bs2.NodeCountByLabelAndPropertyKey(7, "id"); err != nil || got != 1 {
		t.Fatalf("label 7 id after reopen = (%d, %v), want (1, nil)", got, err)
	}
	if got, err := bs2.NodeCountByLabelAndPropertyKey(8, "id"); err != nil || got != 1 {
		t.Fatalf("label 8 id after reopen = (%d, %v), want (1, nil)", got, err)
	}
	if got, err := bs2.NodeCountByLabelAndPropertyKey(9, "id"); err != nil || got != 0 {
		t.Fatalf("label 9 id after reopen = (%d, %v), want (0, nil)", got, err)
	}
}
