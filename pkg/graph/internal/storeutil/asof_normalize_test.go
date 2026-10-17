package storeutil

import (
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestNormalizeTemporalVisibleAtTxTime: the one rewind (core as-of and pinned
// doors, memory store pinned reads). A ValidTo the delete wrote reopens before
// the delete; a ValidTo the delete kept (DeleteKeptValidTo, backlogs 4/7/25)
// or a close recorded before it stays; the markers go with their delete.
func TestNormalizeTemporalVisibleAtTxTime(t *testing.T) {
	t.Parallel()
	NormalizeTemporalVisibleAtTxTime(types.LegacyValidAxis, nil, 10)
	for _, tc := range []struct {
		name string
		in   types.TemporalMetadata
		want types.TemporalMetadata
	}{
		{"future TxTo hidden", types.TemporalMetadata{TxTo: 20}, types.TemporalMetadata{}},
		{"past TxTo kept", types.TemporalMetadata{TxTo: 10}, types.TemporalMetadata{TxTo: 10}},
		{"future delete-written ValidTo reopens", types.TemporalMetadata{ValidTo: 20, DeletedAt: 20, TxTo: 20}, types.TemporalMetadata{}},
		{"future delete keeps an earlier close", types.TemporalMetadata{ValidTo: 15, DeletedAt: 20}, types.TemporalMetadata{ValidTo: 15}},
		{"future delete keeps a later close", types.TemporalMetadata{ValidTo: 30, DeletedAt: 20, DeleteKeptValidTo: true}, types.TemporalMetadata{ValidTo: 30}},
		{"future delete keeps a close at its instant", types.TemporalMetadata{ValidTo: 20, DeletedAt: 20, DeleteKeptValidTo: true}, types.TemporalMetadata{ValidTo: 20}},
		{"future delete keeps the open end", types.TemporalMetadata{ValidFrom: 50, DeletedAt: 20, DeleteKeptValidTo: true}, types.TemporalMetadata{ValidFrom: 50}},
		{"future retraction hidden", types.TemporalMetadata{ValidTo: 20, DeletedAt: 20, Retracted: true}, types.TemporalMetadata{}},
		{"past delete kept", types.TemporalMetadata{ValidTo: 10, DeletedAt: 10}, types.TemporalMetadata{ValidTo: 10, DeletedAt: 10}},
		{"past kept delete kept", types.TemporalMetadata{ValidTo: 30, DeletedAt: 10, DeleteKeptValidTo: true}, types.TemporalMetadata{ValidTo: 30, DeletedAt: 10, DeleteKeptValidTo: true}},
	} {
		tm := tc.in
		NormalizeTemporalVisibleAtTxTime(types.LegacyValidAxis, &tm, 10)
		if tm != tc.want {
			t.Errorf("%s: got %+v, want %+v", tc.name, tm, tc.want)
		}
	}
}
