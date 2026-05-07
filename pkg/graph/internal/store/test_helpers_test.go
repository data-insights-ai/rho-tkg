package store

import (
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
)

// newTestGen creates a snowflake generator for testing.
// Mirrors the production layout (5 node bits, 10 step bits, microsecond precision).
func newTestGen(t *testing.T, nodeID int64) *snowflake.Node {
	t.Helper()
	gen, err := snowflake.NewNode(nodeID,
		snowflake.WithEpoch(SnowflakeEpoch),
		snowflake.WithMicroseconds(),
		snowflake.WithNodeBits(5),
		snowflake.WithStepBits(10),
	)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	return gen
}
