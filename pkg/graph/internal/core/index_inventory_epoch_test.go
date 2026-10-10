package core

import (
	"errors"
	"fmt"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	tieredpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
)

// indexDDL advances the epoch on success and on a failure that may have left
// partial state, and not on a refusal that changed nothing (wrapped or not).
func TestIndexDDLAdvancesExceptOnRefusals(t *testing.T) {
	c := &Core{}
	ops := &IndexOps{c: c}
	want := uint64(0)
	check := func(err error, advances bool) {
		t.Helper()
		if got := c.indexDDL(err); got != err {
			t.Fatalf("indexDDL changed the error: %v -> %v", err, got)
		}
		if advances {
			want++
		}
		if got := ops.InventoryEpoch(); got != want {
			t.Fatalf("after %v: epoch %d, want %d", err, got, want)
		}
	}
	check(nil, true)
	check(errors.New("backfill failed"), true)
	check(fmt.Errorf("shard 2: %w", storepkg.ErrStoreClosed), true)
	for _, refused := range []error{
		storepkg.ErrIndexExists, storepkg.ErrIndexNotFound,
		storepkg.ErrTemporalIndexExists, storepkg.ErrTemporalIndexNotFound,
		storepkg.ErrVectorIndexExists, storepkg.ErrVectorIndexNotFound,
		storepkg.ErrRelPropertyIndexUnsupported, storepkg.ErrCapabilityNotSupported,
		tieredpkg.ErrEventPropertyIndex,
		fmt.Errorf("wrapped: %w", storepkg.ErrIndexExists),
	} {
		check(refused, false)
	}
}
