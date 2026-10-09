package index

import (
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestRelCoveredInTemporalIndexes(t *testing.T) {
	r := types.NewRelationship(7, 3, 1, 2)
	if RelCoveredInTemporalIndexes(nil, r, 7) {
		t.Error("nil index map reports coverage")
	}
	idxs := map[uint16]*TemporalIndex{3: NewTemporalIndex()}
	if RelCoveredInTemporalIndexes(idxs, r, 7) {
		t.Error("empty index reports coverage")
	}
	idxs[3].Extend(snowflake.ID(7), 10, 20)
	if !RelCoveredInTemporalIndexes(idxs, r, 7) {
		t.Error("covered id not reported")
	}
	if RelCoveredInTemporalIndexes(idxs, r, 8) {
		t.Error("another id reported as covered")
	}
	other := types.NewRelationship(7, 4, 1, 2) // same id, a type without an index
	if RelCoveredInTemporalIndexes(idxs, other, 7) {
		t.Error("a type without an index reports coverage")
	}
}
