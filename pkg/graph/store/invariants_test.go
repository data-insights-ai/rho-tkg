package store

import (
	"errors"
	"testing"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

func TestValidateQueryOptsRejectsInvalidActiveInterval(t *testing.T) {
	t.Parallel()
	for _, opts := range []QueryOpts{
		{ValidStart: types.Instant(10), ValidEnd: types.Instant(10)},
		{ValidStart: types.Instant(20), ValidEnd: types.Instant(10)},
	} {
		if err := ValidateQueryOpts(opts); !errors.Is(err, ErrInvalidTimeRange) {
			t.Fatalf("ValidateQueryOpts(%+v) = %v, want ErrInvalidTimeRange", opts, err)
		}
	}
}

func TestValidateQueryOptsRejectsNegativeLimit(t *testing.T) {
	t.Parallel()
	err := ValidateQueryOpts(QueryOpts{Limit: -1})
	if !errors.Is(err, ErrInvalidQueryLimit) {
		t.Fatalf("ValidateQueryOpts negative limit = %v, want ErrInvalidQueryLimit", err)
	}
}

func TestValidateQueryOptsRejectsNegativeCursor(t *testing.T) {
	t.Parallel()
	err := ValidateQueryOpts(QueryOpts{After: types.EntityID(-1)})
	if !errors.Is(err, ErrInvalidQueryCursor) {
		t.Fatalf("ValidateQueryOpts negative cursor = %v, want ErrInvalidQueryCursor", err)
	}
}

func TestValidatePaginationRejectsNegativeLimitAndCursor(t *testing.T) {
	t.Parallel()
	if err := ValidatePagination(0, -1); !errors.Is(err, ErrInvalidQueryLimit) {
		t.Fatalf("ValidatePagination negative limit = %v, want ErrInvalidQueryLimit", err)
	}
	if err := ValidatePagination(types.EntityID(-1), 0); !errors.Is(err, ErrInvalidQueryCursor) {
		t.Fatalf("ValidatePagination negative cursor = %v, want ErrInvalidQueryCursor", err)
	}
}

func TestValidateHistoryRetentionRejectsNegativeOnly(t *testing.T) {
	t.Parallel()
	if err := ValidateHistoryRetention(-1); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("ValidateHistoryRetention(-1) = %v, want ErrInvalidStoreMutation", err)
	}
	if err := ValidateHistoryRetention(0); err != nil {
		t.Fatalf("ValidateHistoryRetention(0): %v", err)
	}
}

func TestValidateEntityIDsRejectZeroAndNegative(t *testing.T) {
	t.Parallel()
	for _, check := range []struct {
		name string
		err  error
	}{
		{name: "zero node", err: ValidateNodeID(0)},
		{name: "negative node", err: ValidateNodeID(types.NodeID(-1))},
		{name: "zero rel", err: ValidateRelID(0)},
		{name: "negative rel", err: ValidateRelID(types.RelID(-1))},
	} {
		if !errors.Is(check.err, ErrInvalidStoreMutation) {
			t.Fatalf("%s = %v, want ErrInvalidStoreMutation", check.name, check.err)
		}
	}
	if err := ValidateNodeID(types.NodeID(1)); err != nil {
		t.Fatalf("ValidateNodeID(1): %v", err)
	}
	if err := ValidateRelID(types.RelID(1)); err != nil {
		t.Fatalf("ValidateRelID(1): %v", err)
	}
}

func TestValidateSnapshotAndRelationshipIndexRejectNegativeIDs(t *testing.T) {
	t.Parallel()
	negNode := types.NewNode(types.NodeID(-1), 1, nil)
	if err := ValidateNodeSnapshotKey(negNode.ID(), negNode); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("ValidateNodeSnapshotKey negative ID = %v, want ErrInvalidStoreMutation", err)
	}

	negRel := types.NewRelationship(types.RelID(-1), 1, types.NodeID(1), types.NodeID(2))
	if err := ValidateRelSnapshotKey(negRel.ID(), negRel); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("ValidateRelSnapshotKey negative ID = %v, want ErrInvalidStoreMutation", err)
	}

	for _, check := range []struct {
		name string
		err  error
	}{
		{
			name: "negative rel",
			err:  ValidateRelationshipIndexEntry(types.NodeID(1), types.NodeID(2), 1, types.RelID(-1)),
		},
		{
			name: "negative start",
			err:  ValidateRelationshipIndexEntry(types.NodeID(-1), types.NodeID(2), 1, types.RelID(3)),
		},
		{
			name: "negative end",
			err:  ValidateRelationshipIndexEntry(types.NodeID(1), types.NodeID(-2), 1, types.RelID(3)),
		},
	} {
		if !errors.Is(check.err, ErrInvalidStoreMutation) {
			t.Fatalf("%s = %v, want ErrInvalidStoreMutation", check.name, check.err)
		}
	}
}

func TestValidateNodeWriteRejectsReservedLabelTokens(t *testing.T) {
	t.Parallel()

	zeroPrimary := types.NewNode(types.NodeID(1), 0, nil)
	if err := ValidateNodeWrite(zeroPrimary); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("ValidateNodeWrite zero primary = %v, want ErrInvalidStoreMutation", err)
	}

	zeroExtra := types.NewNode(types.NodeID(2), 1, []uint16{2, 0})
	if err := ValidateNodeWrite(zeroExtra); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("ValidateNodeWrite zero extra = %v, want ErrInvalidStoreMutation", err)
	}

	zeroReplacementOld := types.NewNode(types.NodeID(3), 0, nil)
	zeroReplacementCurrent := zeroReplacementOld.DeepCopy()
	if err := ValidateNodeReplacement(zeroReplacementOld, zeroReplacementCurrent); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("ValidateNodeReplacement zero label = %v, want ErrInvalidStoreMutation", err)
	}

	addOld := types.NewNode(types.NodeID(4), 1, []uint16{0})
	addCurrent := types.NewNode(types.NodeID(4), 1, []uint16{0, 2})
	if err := ValidateNodeLabelAddition(addOld, addCurrent, 2); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("ValidateNodeLabelAddition with preserved zero label = %v, want ErrInvalidStoreMutation", err)
	}

	removeOld := types.NewNode(types.NodeID(5), 1, []uint16{0, 2})
	removeCurrent := types.NewNode(types.NodeID(5), 1, []uint16{0})
	if err := ValidateNodeLabelRemoval(removeOld, removeCurrent, 2); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("ValidateNodeLabelRemoval with preserved zero label = %v, want ErrInvalidStoreMutation", err)
	}
}

func TestValidateNodeWriteRejectsInvalidExplicitTemporalRange(t *testing.T) {
	t.Parallel()

	node := types.NewNode(types.NodeID(10), 1, nil)
	node.SetTemporal(&types.TemporalMetadata{ValidFrom: 20, ValidTo: 20})
	if err := ValidateNodeWrite(node); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("ValidateNodeWrite empty temporal range = %v, want ErrInvalidStoreMutation", err)
	}

	old := types.NewNode(types.NodeID(11), 1, nil)
	current := old.DeepCopy()
	current.SetTemporal(&types.TemporalMetadata{ValidFrom: 30, ValidTo: 10})
	if err := ValidateNodeReplacement(old, current); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("ValidateNodeReplacement reversed temporal range = %v, want ErrInvalidStoreMutation", err)
	}

	removeOld := types.NewNode(types.NodeID(12), 1, []uint16{2})
	removeCurrent := types.NewNode(types.NodeID(12), 1, nil)
	removeCurrent.SetTemporal(&types.TemporalMetadata{ValidFrom: 7, ValidTo: 6})
	if err := ValidateNodeLabelRemoval(removeOld, removeCurrent, 2); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("ValidateNodeLabelRemoval invalid temporal range = %v, want ErrInvalidStoreMutation", err)
	}

	addOld := types.NewNode(types.NodeID(13), 1, nil)
	addCurrent := types.NewNode(types.NodeID(13), 1, []uint16{2})
	addCurrent.SetTemporal(&types.TemporalMetadata{ValidFrom: 9, ValidTo: 8})
	if err := ValidateNodeLabelAddition(addOld, addCurrent, 2); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("ValidateNodeLabelAddition invalid temporal range = %v, want ErrInvalidStoreMutation", err)
	}

	openEnded := types.NewNode(types.NodeID(14), 1, nil)
	openEnded.SetTemporal(&types.TemporalMetadata{ValidFrom: 20, ValidTo: 0})
	if err := ValidateNodeWrite(openEnded); err != nil {
		t.Fatalf("ValidateNodeWrite open-ended temporal range: %v", err)
	}

	derivedStart := types.NewNode(types.NodeID(15), 1, nil)
	derivedStart.SetTemporal(&types.TemporalMetadata{ValidFrom: 0, ValidTo: 20})
	if err := ValidateNodeWrite(derivedStart); err != nil {
		t.Fatalf("ValidateNodeWrite derived-start temporal range: %v", err)
	}
}

func TestValidateRelationshipWriteRejectsReservedTypeToken(t *testing.T) {
	t.Parallel()

	rel := types.NewRelationship(types.RelID(1), 0, types.NodeID(1), types.NodeID(2))
	if err := ValidateRelationshipWrite(rel); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("ValidateRelationshipWrite zero type = %v, want ErrInvalidStoreMutation", err)
	}

	replacementOld := types.NewRelationship(types.RelID(2), 0, types.NodeID(1), types.NodeID(2))
	replacementCurrent := replacementOld.DeepCopy()
	if err := ValidateRelationshipReplacement(replacementOld, replacementCurrent); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("ValidateRelationshipReplacement zero type = %v, want ErrInvalidStoreMutation", err)
	}

	zeroEndpointOld := types.NewRelationship(types.RelID(3), 1, 0, types.NodeID(2))
	zeroEndpointCurrent := zeroEndpointOld.DeepCopy()
	if err := ValidateRelationshipReplacement(zeroEndpointOld, zeroEndpointCurrent); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("ValidateRelationshipReplacement zero endpoint = %v, want ErrInvalidStoreMutation", err)
	}
}

func TestValidateRelationshipWriteRejectsInvalidExplicitTemporalRange(t *testing.T) {
	t.Parallel()

	rel := types.NewRelationship(types.RelID(20), 1, types.NodeID(1), types.NodeID(2))
	rel.SetTemporal(&types.TemporalMetadata{ValidFrom: 20, ValidTo: 20})
	if err := ValidateRelationshipWrite(rel); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("ValidateRelationshipWrite empty temporal range = %v, want ErrInvalidStoreMutation", err)
	}

	old := types.NewRelationship(types.RelID(21), 1, types.NodeID(1), types.NodeID(2))
	current := old.DeepCopy()
	current.SetTemporal(&types.TemporalMetadata{ValidFrom: 30, ValidTo: 10})
	if err := ValidateRelationshipReplacement(old, current); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("ValidateRelationshipReplacement reversed temporal range = %v, want ErrInvalidStoreMutation", err)
	}

	openEnded := types.NewRelationship(types.RelID(22), 1, types.NodeID(1), types.NodeID(2))
	openEnded.SetTemporal(&types.TemporalMetadata{ValidFrom: 20, ValidTo: 0})
	if err := ValidateRelationshipWrite(openEnded); err != nil {
		t.Fatalf("ValidateRelationshipWrite open-ended temporal range: %v", err)
	}

	derivedStart := types.NewRelationship(types.RelID(23), 1, types.NodeID(1), types.NodeID(2))
	derivedStart.SetTemporal(&types.TemporalMetadata{ValidFrom: 0, ValidTo: 20})
	if err := ValidateRelationshipWrite(derivedStart); err != nil {
		t.Fatalf("ValidateRelationshipWrite derived-start temporal range: %v", err)
	}
}

func TestValidateQueryOptsValidAtTakesPrecedenceOverInterval(t *testing.T) {
	t.Parallel()
	opts := QueryOpts{ValidAt: 5, ValidStart: 20, ValidEnd: 10}
	if err := ValidateQueryOpts(opts); err != nil {
		t.Fatalf("ValidateQueryOpts with ValidAt precedence: %v", err)
	}
}
