package types

import (
	"testing"

	snowflake "gitlab2024.bds421-cloud.com/bds421/rho/snowflake-2026"
)

func TestEntityIDSnowflakeIDRoundTrip(t *testing.T) {
	t.Parallel()

	tm := &TemporalMetadata{}
	tm.SetBaseEntityID(snowflake.ID(42))
	got := tm.BaseEntityID().SnowflakeID()
	if got != snowflake.ID(42) {
		t.Errorf("entityID.SnowflakeID() = %d, want 42", got)
	}
}

func TestEntityIDSnowflakeIDZeroValue(t *testing.T) {
	t.Parallel()

	tm := &TemporalMetadata{}
	got := tm.BaseEntityID().SnowflakeID()
	if got != snowflake.ID(0) {
		t.Errorf("zero entityID.SnowflakeID() = %d, want 0", got)
	}
}
