package memory

import (
	"errors"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestMemoryStoreNodeCountByLabelAndPropertyKey(t *testing.T) {
	t.Parallel()
	ms := New()
	t.Cleanup(func() { _ = ms.Close() })

	n1 := types.NewNode(types.NodeID(snowflake.ID(101)), 1, []uint16{2})
	if err := n1.SetProperty("id", int64(1)); err != nil {
		t.Fatalf("SetProperty n1.id: %v", err)
	}
	if err := ms.PutNode(n1); err != nil {
		t.Fatalf("PutNode n1: %v", err)
	}
	n2 := types.NewNode(types.NodeID(snowflake.ID(102)), 1, nil)
	if err := n2.SetProperty("id", int64(2)); err != nil {
		t.Fatalf("SetProperty n2.id: %v", err)
	}
	if err := ms.PutNode(n2); err != nil {
		t.Fatalf("PutNode n2: %v", err)
	}
	n3 := types.NewNode(types.NodeID(snowflake.ID(103)), 3, nil)
	if err := n3.SetProperty("tags", []string{"not", "indexable"}); err != nil {
		t.Fatalf("SetProperty n3.tags: %v", err)
	}
	if err := ms.PutNode(n3); err != nil {
		t.Fatalf("PutNode n3: %v", err)
	}

	if got, err := ms.NodeCountByLabelAndPropertyKey(1, "id"); err != nil || got != 2 {
		t.Fatalf("label 1 id count = (%d, %v), want (2, nil)", got, err)
	}
	if got, err := ms.NodeCountByLabelAndPropertyKey(2, "id"); err != nil || got != 1 {
		t.Fatalf("label 2 id count = (%d, %v), want (1, nil)", got, err)
	}
	if got, err := ms.NodeCountByLabelAndPropertyKey(3, "tags"); err != nil || got != 0 {
		t.Fatalf("label 3 tags count = (%d, %v), want (0, nil)", got, err)
	}
	// A label with no counts at all returns 0 via the nil-map branch.
	if got, err := ms.NodeCountByLabelAndPropertyKey(99, "id"); err != nil || got != 0 {
		t.Fatalf("missing label count = (%d, %v), want (0, nil)", got, err)
	}

	// ReplaceNode removes the old property contribution.
	updated := n2.DeepCopy()
	if _, err := updated.DeleteProperty("id"); err != nil {
		t.Fatalf("DeleteProperty updated.id: %v", err)
	}
	if err := ms.ReplaceNode(updated); err != nil {
		t.Fatalf("ReplaceNode updated: %v", err)
	}
	if got, err := ms.NodeCountByLabelAndPropertyKey(1, "id"); err != nil || got != 1 {
		t.Fatalf("label 1 id after replace = (%d, %v), want (1, nil)", got, err)
	}

	// DeleteNode drops the last contributor; the label key is pruned.
	if err := ms.DeleteNode(n1.ID()); err != nil {
		t.Fatalf("DeleteNode n1: %v", err)
	}
	if got, err := ms.NodeCountByLabelAndPropertyKey(1, "id"); err != nil || got != 0 {
		t.Fatalf("label 1 id after delete = (%d, %v), want (0, nil)", got, err)
	}
	if got, err := ms.NodeCountByLabelAndPropertyKey(2, "id"); err != nil || got != 0 {
		t.Fatalf("label 2 id after delete = (%d, %v), want (0, nil)", got, err)
	}
}

func TestMemoryStoreNodeCountByLabelAndPropertyKeyErrors(t *testing.T) {
	t.Parallel()

	var nilStore *Store
	if _, err := nilStore.NodeCountByLabelAndPropertyKey(1, "id"); !errors.Is(err, ErrNilStore) {
		t.Fatalf("nil store = %v, want ErrNilStore", err)
	}

	ms := New()
	if _, err := ms.NodeCountByLabelAndPropertyKey(0, "id"); err == nil {
		t.Fatal("token 0 should be rejected")
	}
	if _, err := ms.NodeCountByLabelAndPropertyKey(1, "tkg_version"); err == nil {
		t.Fatal("shadow property key should be rejected")
	}
	if err := ms.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := ms.NodeCountByLabelAndPropertyKey(1, "id"); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("closed store = %v, want ErrStoreClosed", err)
	}
}
