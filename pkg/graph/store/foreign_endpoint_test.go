package store

import (
	"errors"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BACKLOG 9i: ForeignEndpoint/ForeignIncomingEdge's doc comments used to read
// as promising a staleness-window CHECK ("a cross-validation aid whose
// staleness window is made explicit by AttestTx"), but Validate() only ever
// checked AttestTx for presence (non-zero), never for age relative to local
// commit time. The doc comments were rewritten to state this precisely:
// rho-tkg enforces NO staleness bound — bounding acceptable staleness is the
// caller's/orchestrator's responsibility, enforced before
// calling these doors. These tests pin the now-accurately-documented
// behavior: Validate accepts ANY positive AttestTx, no matter how old, and
// still rejects a zero AttestTx (the one check that DOES exist).

func TestForeignEndpointValidate_AcceptsArbitrarilyOldAttestTx(t *testing.T) {
	fe := ForeignEndpoint{
		NodeID:   types.NodeID(snowflake.ID(1)),
		Hash:     "deadbeef",
		AttestTx: types.Instant(1), // as old as a positive Instant can be
	}
	if err := fe.Validate(); err != nil {
		t.Fatalf("Validate with an arbitrarily old (but positive) AttestTx = %v, want nil — BACKLOG 9i: no staleness bound is enforced here by design", err)
	}
}

func TestForeignEndpointValidate_RejectsZeroAttestTx(t *testing.T) {
	fe := ForeignEndpoint{
		NodeID:   types.NodeID(snowflake.ID(1)),
		Hash:     "deadbeef",
		AttestTx: 0,
	}
	if err := fe.Validate(); !errors.Is(err, ErrInvalidForeignEndpoint) {
		t.Fatalf("Validate with zero AttestTx = %v, want ErrInvalidForeignEndpoint", err)
	}
}

func TestForeignEndpointValidate_RejectsZeroNodeIDAndEmptyHash(t *testing.T) {
	valid := ForeignEndpoint{NodeID: types.NodeID(snowflake.ID(1)), Hash: "deadbeef", AttestTx: 1}

	zeroID := valid
	zeroID.NodeID = 0
	if err := zeroID.Validate(); !errors.Is(err, ErrInvalidForeignEndpoint) {
		t.Fatalf("Validate with zero NodeID = %v, want ErrInvalidForeignEndpoint", err)
	}

	emptyHash := valid
	emptyHash.Hash = ""
	if err := emptyHash.Validate(); !errors.Is(err, ErrInvalidForeignEndpoint) {
		t.Fatalf("Validate with empty Hash = %v, want ErrInvalidForeignEndpoint", err)
	}
}

func TestForeignIncomingEdgeValidate_AcceptsArbitrarilyOldAttestTx(t *testing.T) {
	e := ForeignIncomingEdge{
		RelID:    types.RelID(snowflake.ID(2)),
		TypeName: "KNOWS",
		StartID:  types.NodeID(snowflake.ID(3)),
		EndID:    types.NodeID(snowflake.ID(4)),
		AttestTx: types.Instant(1),
	}
	if err := e.Validate(); err != nil {
		t.Fatalf("Validate with an arbitrarily old (but positive) AttestTx = %v, want nil — BACKLOG 9i: no staleness bound is enforced here by design", err)
	}
}

func TestForeignIncomingEdgeValidate_RejectsZeroAttestTx(t *testing.T) {
	e := ForeignIncomingEdge{
		RelID:    types.RelID(snowflake.ID(2)),
		TypeName: "KNOWS",
		StartID:  types.NodeID(snowflake.ID(3)),
		EndID:    types.NodeID(snowflake.ID(4)),
		AttestTx: 0,
	}
	if err := e.Validate(); !errors.Is(err, ErrInvalidForeignIncoming) {
		t.Fatalf("Validate with zero AttestTx = %v, want ErrInvalidForeignIncoming", err)
	}
}

func TestForeignIncomingEdgeValidate_RejectsZeroIDsAndEmptyTypeName(t *testing.T) {
	valid := ForeignIncomingEdge{
		RelID:    types.RelID(snowflake.ID(2)),
		TypeName: "KNOWS",
		StartID:  types.NodeID(snowflake.ID(3)),
		EndID:    types.NodeID(snowflake.ID(4)),
		AttestTx: 1,
	}

	zeroRel := valid
	zeroRel.RelID = 0
	if err := zeroRel.Validate(); !errors.Is(err, ErrInvalidForeignIncoming) {
		t.Fatalf("Validate with zero RelID = %v, want ErrInvalidForeignIncoming", err)
	}

	emptyType := valid
	emptyType.TypeName = ""
	if err := emptyType.Validate(); !errors.Is(err, ErrInvalidForeignIncoming) {
		t.Fatalf("Validate with empty TypeName = %v, want ErrInvalidForeignIncoming", err)
	}

	zeroStart := valid
	zeroStart.StartID = 0
	if err := zeroStart.Validate(); !errors.Is(err, ErrInvalidForeignIncoming) {
		t.Fatalf("Validate with zero StartID = %v, want ErrInvalidForeignIncoming", err)
	}

	zeroEnd := valid
	zeroEnd.EndID = 0
	if err := zeroEnd.Validate(); !errors.Is(err, ErrInvalidForeignIncoming) {
		t.Fatalf("Validate with zero EndID = %v, want ErrInvalidForeignIncoming", err)
	}
}
