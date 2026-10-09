package storeutil

import (
	"math"
	"math/rand"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	snowflakepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/snowflake"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// refCreatedAtMillis is the pre-item-36 formula of EntityValidFrom /
// SnowflakeInstant, kept here only as the oracle for positive IDs.
func refCreatedAtMillis(id snowflake.ID) types.Instant {
	return types.Instant(snowflakepkg.Layout.CreatedAt(id).UnixMilli())
}

// The resolver (EntityValidFrom without metadata), the retention boundary
// (SnowflakeInstant) and the public doors (NodeID/RelID.MintInstant) are ONE
// function: they must agree for every ID (single source of truth, item 36).
func TestMintInstant_ResolverAndPublicDoorsAgreeForAllIDs(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewSource(36))
	check := func(id snowflake.ID) {
		t.Helper()
		pub := types.NodeID(id).MintInstant()
		if got := types.RelID(id).MintInstant(); got != pub {
			t.Fatalf("id %d: RelID %d != NodeID %d", int64(id), got, pub)
		}
		if got := EntityValidFrom(id, nil); got != pub {
			t.Fatalf("id %d: EntityValidFrom(nil) %d != MintInstant %d", int64(id), got, pub)
		}
		if got := EntityValidFrom(id, &types.TemporalMetadata{}); got != pub {
			t.Fatalf("id %d: EntityValidFrom(zero ValidFrom) %d != MintInstant %d", int64(id), got, pub)
		}
		if got := SnowflakeInstant(id); got != pub {
			t.Fatalf("id %d: SnowflakeInstant %d != MintInstant %d", int64(id), got, pub)
		}
		if id > 0 {
			if want := refCreatedAtMillis(id); pub != want {
				t.Fatalf("id %d: MintInstant %d != Layout.CreatedAt.UnixMilli %d", int64(id), pub, want)
			}
		}
	}
	for _, id := range []snowflake.ID{0, -1, 1, 1 << 15, math.MaxInt64, math.MinInt64} {
		check(id)
	}
	for i := 0; i < 200_000; i++ {
		check(snowflake.ID(rng.Int63()))
	}
	for i := 0; i < 1000; i++ {
		check(-snowflake.ID(rng.Int63()))
	}
}

// Non-positive IDs are never minted: the derived start is 0 (unset), not the
// epoch and not a pre-epoch instant, and nothing panics.
func TestMintInstant_NonPositiveIDsDeriveZero(t *testing.T) {
	t.Parallel()
	for _, id := range []snowflake.ID{0, -1, -(1 << 15), math.MinInt64} {
		if got := EntityValidFrom(id, nil); got != 0 {
			t.Fatalf("EntityValidFrom(%d, nil) = %d, want 0", int64(id), got)
		}
		if got := SnowflakeInstant(id); got != 0 {
			t.Fatalf("SnowflakeInstant(%d) = %d, want 0", int64(id), got)
		}
	}
}

// An explicit ValidFrom still wins over the derived start.
func TestMintInstant_ExplicitValidFromStillWins(t *testing.T) {
	t.Parallel()
	id := snowflake.ID(5_000_000<<15 | 2<<10 | 1)
	tm := &types.TemporalMetadata{ValidFrom: 42}
	if got := EntityValidFrom(id, tm); got != 42 {
		t.Fatalf("EntityValidFrom with explicit ValidFrom = %d, want 42", got)
	}
}

// The canonical predicates use the derived start: a row without ValidFrom is
// valid exactly from its mint instant (the sigma-tkgd rule).
func TestMintInstant_PredicateStartsAtMintInstant(t *testing.T) {
	t.Parallel()
	id := snowflake.ID(86_400_000_000<<15 | 6<<10 | 3)
	mint := types.NodeID(id).MintInstant()
	if MatchesPointInTime(id, nil, mint-1) {
		t.Fatal("valid one ms before the mint instant")
	}
	if !MatchesPointInTime(id, nil, mint) {
		t.Fatal("not valid at the mint instant")
	}
}
