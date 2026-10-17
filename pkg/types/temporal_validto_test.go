package types

import "testing"

// TestValidToAtTxTime: a row's own end as believed at a pin, the rule
// consumers and storeutil.OwnOpenAtTxTime read (row 73). Break cases: the pin one before and at the delete,
// a kept close, the kept open end, a legacy tombstone.
func TestTemporalMetadataValidToAtTxTime(t *testing.T) {
	t.Parallel()
	kept := &TemporalMetadata{ValidTo: 30, DeletedAt: 20, DeleteKeptValidTo: true}
	keptOpen := &TemporalMetadata{ValidFrom: 50, DeletedAt: 20, DeleteKeptValidTo: true}
	legacy := &TemporalMetadata{ValidTo: 20, DeletedAt: 20}
	closed := &TemporalMetadata{ValidTo: 15, DeletedAt: 20}
	for _, tc := range []struct {
		name string
		tm   *TemporalMetadata
		pin  Instant
		want Instant
	}{
		{"nil", nil, 10, 0},
		{"live", &TemporalMetadata{ValidTo: 7}, 10, 7},
		{"kept before delete", kept, 19, 30},
		{"kept at delete", kept, 20, 20},
		{"kept open before delete", keptOpen, 19, 0},
		{"kept open at delete", keptOpen, 20, 20},
		{"legacy before delete", legacy, 19, 0},
		{"legacy at delete", legacy, 20, 20},
		{"earlier close before delete", closed, 19, 15},
		{"earlier close at delete", closed, 20, 15},
	} {
		if got := tc.tm.ValidToAtTxTime(tc.pin); got != tc.want {
			t.Errorf("%s: ValidToAtTxTime = %d, want %d", tc.name, got, tc.want)
		}
	}
	if kept.DeleteWroteValidTo() || closed.DeleteWroteValidTo() || !legacy.DeleteWroteValidTo() {
		t.Error("DeleteWroteValidTo: want only the legacy shape")
	}
}
