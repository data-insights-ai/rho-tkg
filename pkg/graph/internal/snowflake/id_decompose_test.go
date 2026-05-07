package snowflake

import (
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
)

// TestEpoch_IsJanuary2026 anchors the package-level epoch so a future change
// silently shifting the timestamp baseline triggers a test failure.
func TestEpoch_IsJanuary2026(t *testing.T) {
	t.Parallel()
	want := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if !Epoch.Equal(want) {
		t.Fatalf("Epoch = %v, want %v", Epoch, want)
	}
}

// TestLayout_Decompose_KnownID confirms the package Layout uses the
// production-shaped 48/5/10 microsecond format. We synthesize an ID with a
// snowflake.Node configured against the same Layout and check that
// DecomposeID round-trips the node and step fields.
func TestLayout_Decompose_KnownID(t *testing.T) {
	t.Parallel()
	gen, err := snowflake.NewNode(7,
		snowflake.WithEpoch(Epoch),
		snowflake.WithMicroseconds(),
		snowflake.WithNodeBits(5),
		snowflake.WithStepBits(10),
	)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	id := gen.Generate()
	c := DecomposeID(id)
	if c.NodeID != 7 {
		t.Errorf("NodeID = %d, want 7", c.NodeID)
	}
	if c.Sequence < 0 || c.Sequence > 1023 {
		t.Errorf("Sequence = %d, want 0..1023", c.Sequence)
	}
	// CreatedAt must be no earlier than the epoch.
	if c.CreatedAt.Before(Epoch) {
		t.Errorf("CreatedAt = %v, expected >= Epoch %v", c.CreatedAt, Epoch)
	}
}

// TestDecomposeID_ConsistentWithLayout ensures DecomposeID's CreatedAt comes
// from the same Layout.CreatedAt the rest of the codebase reads (no second
// independent decoding path).
func TestDecomposeID_ConsistentWithLayout(t *testing.T) {
	t.Parallel()
	gen, err := snowflake.NewNode(0,
		snowflake.WithEpoch(Epoch),
		snowflake.WithMicroseconds(),
		snowflake.WithNodeBits(5),
		snowflake.WithStepBits(10),
	)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	id := gen.Generate()
	c := DecomposeID(id)
	want := Layout.CreatedAt(id)
	if !c.CreatedAt.Equal(want) {
		t.Errorf("DecomposeID.CreatedAt = %v, Layout.CreatedAt = %v", c.CreatedAt, want)
	}
}
