package store

import (
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Backlog 43: the retraction marker qualifies a delete tombstone. A store write
// carrying it without DeletedAt (a current row, a history row, a replacement
// marked "retracted") is no door's output; the Store boundary refuses it
// (ErrInvalidStoreMutation) so no reader ever meets an orphan marker. Catches
// a write boundary that admits the marker anywhere.
func TestStoreWritesRejectOrphanRetractionMarker(t *testing.T) {
	t.Parallel()
	orphan := &types.TemporalMetadata{ValidFrom: 10, TxFrom: 20, Retracted: true}
	tomb := &types.TemporalMetadata{ValidFrom: 10, ValidTo: 30, TxFrom: 20, TxTo: 30, DeletedAt: 30, Retracted: true}

	n := types.NewNode(types.NodeID(5), 1, nil)
	n.SetTemporal(orphan)
	if err := ValidateNodeWrite(n); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("ValidateNodeWrite(orphan marker) = %v, want ErrInvalidStoreMutation", err)
	}
	if err := ValidateNodeHistorySnapshot(n.ID(), n); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("ValidateNodeHistorySnapshot(orphan marker) = %v, want ErrInvalidStoreMutation", err)
	}
	n.SetTemporal(tomb)
	if err := ValidateNodeHistorySnapshot(n.ID(), n); err != nil {
		t.Fatalf("counterpart: a retraction tombstone is a valid history row: %v", err)
	}

	r := types.NewRelationship(types.RelID(7), 1, types.NodeID(5), types.NodeID(6))
	r.SetTemporal(orphan)
	if err := ValidateRelationshipWrite(r); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("ValidateRelationshipWrite(orphan marker) = %v, want ErrInvalidStoreMutation", err)
	}
	if err := ValidateRelationshipHistorySnapshot(r.ID(), r); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("ValidateRelationshipHistorySnapshot(orphan marker) = %v, want ErrInvalidStoreMutation", err)
	}
	r.SetTemporal(tomb)
	if err := ValidateRelationshipHistorySnapshot(r.ID(), r); err != nil {
		t.Fatalf("counterpart: a retraction tombstone is a valid relationship history row: %v", err)
	}
}
