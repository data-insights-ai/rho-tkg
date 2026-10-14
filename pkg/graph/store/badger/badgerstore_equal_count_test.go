package badger

import (
	"errors"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestBadgerStoreEqualCount (round 7 request 4): counts from the RAM node and
// relationship property indexes, declines without one, fails closed after
// Close.
func TestBadgerStoreEqualCount(t *testing.T) {
	bs := newTestBadgerStoreInMemory(t)
	if err := bs.CreatePropertyIndex(1, "v"); err != nil {
		t.Fatal(err)
	}
	if err := bs.CreateRelPropertyIndex(3, "w"); err != nil {
		t.Fatal(err)
	}
	for i, v := range []any{int64(3), 3.0, "3"} {
		n := types.NewNode(types.NodeID(snowflake.ID(i+1)), 1, nil)
		_ = n.SetProperty("v", v)
		if err := bs.PutNode(n); err != nil {
			t.Fatal(err)
		}
	}
	r := types.NewRelationship(types.RelID(snowflake.ID(50)), 3, 1, 2)
	_ = r.SetProperty("w", true)
	if err := bs.PutRelationship(r); err != nil {
		t.Fatal(err)
	}
	if c, exact, err := bs.NodeEqualCount(1, "v", uint8(3)); err != nil || !exact || c != 2 {
		t.Fatalf("NodeEqualCount = %d, %v, %v; want 2", c, exact, err)
	}
	if c, exact, err := bs.RelEqualCount(3, "w", true); err != nil || !exact || c != 1 {
		t.Fatalf("RelEqualCount = %d, %v, %v; want 1", c, exact, err)
	}
	if _, exact, err := bs.RelEqualCount(3, "nope", true); err != nil || exact {
		t.Fatalf("no index: exact=%v err=%v", exact, err)
	}
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := bs.NodeEqualCount(1, "v", int64(3)); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("after Close: %v", err)
	}
	if _, _, err := bs.RelEqualCount(3, "w", true); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("rel after Close: %v", err)
	}
}
