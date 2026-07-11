package memory

import (
	"errors"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// The lesson-62 retraction case at the memory-store seam: a chain whose decisive
// newest belief (highest version) was retracted by the pin must report the entity
// ABSENT — the selector must NOT fall through to an older still-open genesis row.
// This is the exact regression the WP's mutation gate reintroduces inside the
// shared storeutil.SelectAsOf; without a direct memory-level assertion the memory
// suite would stay green under that mutation (only the core oracle caught it).

func mkNodeVersion(nid types.NodeID, version uint32, txFrom, txTo types.Instant) *types.Node {
	n := types.NewNode(nid, 1, nil)
	n.SetVersion(version)
	n.SetTemporal(&types.TemporalMetadata{TxFrom: txFrom, TxTo: txTo})
	return n
}

func mkRelVersion(rid types.RelID, version uint32, txFrom, txTo types.Instant) *types.Relationship {
	r := types.NewRelationship(rid, 1, types.NodeID(snowflake.ID(2)), types.NodeID(snowflake.ID(4)))
	r.SetVersion(version)
	r.SetTemporal(&types.TemporalMetadata{TxFrom: txFrom, TxTo: txTo})
	return r
}

func TestMemoryNodeAsOfRetractedBeliefIsAbsent(t *testing.T) {
	t.Parallel()
	ms := New()
	t.Cleanup(func() { _ = ms.Close() })

	nid := types.NodeID(snowflake.ID(100))
	// v0: OPEN genesis (an append-only cascade demoted it to history WITHOUT
	//     stamping TxTo). v1: corrected tile recorded at tx=30, retracted at tx=50.
	if err := ms.PutNodeVersion(nid, 0, mkNodeVersion(nid, 0, 10, 0)); err != nil {
		t.Fatalf("PutNodeVersion v0: %v", err)
	}
	if err := ms.PutNodeVersion(nid, 1, mkNodeVersion(nid, 1, 30, 50)); err != nil {
		t.Fatalf("PutNodeVersion v1: %v", err)
	}

	// Pin 60: after the retraction. The decisive newest belief (v1) is retracted;
	// the entity is absent — v0 must NOT resurrect.
	if _, err := ms.NodeAsOf(nid, 60); !errors.Is(err, ErrVersionNotFound) {
		t.Fatalf("pin 60: want ErrVersionNotFound (absent), got %v", err)
	}

	// Pin 40: before the retraction. v1 is the visible belief.
	got, err := ms.NodeAsOf(nid, 40)
	if err != nil {
		t.Fatalf("pin 40: %v", err)
	}
	if got.Version() != 1 {
		t.Fatalf("pin 40: want v1, got v%d", got.Version())
	}

	// Pin 20: before v1 was recorded. v0 is the visible belief.
	got, err = ms.NodeAsOf(nid, 20)
	if err != nil {
		t.Fatalf("pin 20: %v", err)
	}
	if got.Version() != 0 {
		t.Fatalf("pin 20: want v0, got v%d", got.Version())
	}
}

func TestMemoryRelAsOfRetractedBeliefIsAbsent(t *testing.T) {
	t.Parallel()
	ms := New()
	t.Cleanup(func() { _ = ms.Close() })

	rid := types.RelID(snowflake.ID(101))
	if err := ms.PutRelVersion(rid, 0, mkRelVersion(rid, 0, 10, 0)); err != nil {
		t.Fatalf("PutRelVersion v0: %v", err)
	}
	if err := ms.PutRelVersion(rid, 1, mkRelVersion(rid, 1, 30, 50)); err != nil {
		t.Fatalf("PutRelVersion v1: %v", err)
	}

	if _, err := ms.RelAsOf(rid, 60); !errors.Is(err, ErrVersionNotFound) {
		t.Fatalf("pin 60: want ErrVersionNotFound (absent), got %v", err)
	}

	got, err := ms.RelAsOf(rid, 40)
	if err != nil {
		t.Fatalf("pin 40: %v", err)
	}
	if got.Version() != 1 {
		t.Fatalf("pin 40: want v1, got v%d", got.Version())
	}

	got, err = ms.RelAsOf(rid, 20)
	if err != nil {
		t.Fatalf("pin 20: %v", err)
	}
	if got.Version() != 0 {
		t.Fatalf("pin 20: want v0, got v%d", got.Version())
	}
}
