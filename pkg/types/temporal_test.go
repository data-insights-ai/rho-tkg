package types

import (
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
)

func TestEntityIDSnowflakeIDRoundTrip(t *testing.T) {
	t.Parallel()

	tm := &TemporalMetadata{}
	tm.SetBaseEntityID(EntityID(42))
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

func TestTemporalMetadataNilReceiverMethodsFailClosed(t *testing.T) {
	t.Parallel()

	var tm *TemporalMetadata
	if got := tm.BaseEntityID(); got != 0 {
		t.Fatalf("BaseEntityID() on nil receiver = %d, want 0", got)
	}
	tm.SetBaseEntityID(EntityID(42))
}
