package store

import (
	"errors"
	"testing"
)

func TestQueryOptsSignedRangePresence(t *testing.T) {
	for _, tc := range []struct {
		q       QueryOpts
		present bool
	}{{QueryOpts{}, false}, {QueryOpts{ValidStart: -10}, false}, {QueryOpts{ValidEnd: -1}, false}, {QueryOpts{ValidStart: -10, ValidEnd: -1}, true}, {QueryOpts{ValidStart: -10, ValidEnd: 10}, true}, {QueryOpts{ValidStart: 1, ValidEnd: 10}, true}} {
		if got := tc.q.HasValidRange(); got != tc.present {
			t.Fatalf("presence %+v: %v want%v", tc.q, got, tc.present)
		}
		if err := ValidateQueryOpts(tc.q); err != nil {
			t.Fatal(tc.q, err)
		}
	}
	for _, q := range []QueryOpts{{ValidStart: -1, ValidEnd: -10}, {ValidStart: -1, ValidEnd: -1}, {ValidStart: 10, ValidEnd: -10}} {
		if err := ValidateQueryOpts(q); !errors.Is(err, ErrInvalidTimeRange) {
			t.Fatal(q, err)
		}
	}
}
