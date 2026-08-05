package store

import (
	"errors"
	"fmt"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// ErrInvalidForeignEndpoint reports a malformed ForeignEndpoint descriptor: a
// zero node ID, an empty attested hash, or a missing attest-time provenance.
var ErrInvalidForeignEndpoint = errors.New("graph: invalid foreign endpoint descriptor")

// ForeignEndpoint describes a relationship endpoint whose node lives on a
// FOREIGN partition — a slot owned by a different machine, not present in the
// local store (ADR-0010). The local machine cannot fetch the foreign node's
// integrity hash or confirm its existence under a local entity lock, so a
// caller that has resolved the foreign endpoint out-of-band (an RPC to the
// owning machine) supplies here what the local endpoint-hash ladder would
// otherwise read from the store.
//
// The descriptor is caller-ATTESTED, not locally verified: Hash is taken as
// authoritative for the relationship's tkg_to_hash and the node's existence is
// trusted. tkg_from_hash/tkg_to_hash are deliberately NOT part of the
// relationship content hash (they never were — see the relationship create
// kernel), so an attested hash does not weaken Verify*Chain or replication
// byte-exactness. AttestTx is REQUIRED PROVENANCE (Validate rejects a zero
// value) recording WHEN Hash was read, so the attest-time-vs-commit-time gap
// is always knowable in the persisted data — but rho-tkg itself enforces NO
// staleness bound on that gap: any positive AttestTx passes, no matter how
// old relative to local commit time. Bounding acceptable staleness (a
// deployment policy — how large a gap is tolerable, clock-skew allowance,
// whether to reject vs. warn) is the caller's/orchestrator's responsibility
// (ADR-0010 §4.1; a deployment-policy concern),
// enforced BEFORE calling AddByIDForeignEnd, not inside it. The tkg_to_hash
// therefore reflects the foreign node's state at attest time, not at
// local-commit time, and callers needing a bound must check AttestTx against
// their own clock before this door is invoked.
type ForeignEndpoint struct {
	// NodeID is the foreign node's ID. Its snowflake slot is owned by another
	// machine and is not claimed by the local (sharded) store.
	NodeID types.NodeID
	// Hash is the foreign node's integrity hash, captured on the owning machine
	// under that machine's entity lock at AttestTx.
	Hash string
	// AttestTx is the owning machine's transaction time when Hash was read —
	// required provenance (Validate rejects zero) for the attest-time-vs-
	// commit-time gap. NOT staleness-bounded here; see the type doc comment.
	AttestTx types.Instant
}

// Validate rejects a malformed descriptor. A foreign create must carry real
// provenance: a non-zero node ID, a non-empty attested hash, and a non-zero
// attest-time. Validate does NOT check how OLD AttestTx is — any positive
// value passes; see the type doc comment for why that bound is intentionally
// left to the caller.
func (fe ForeignEndpoint) Validate() error {
	if fe.NodeID.SnowflakeID() == 0 {
		return fmt.Errorf("%w: zero node ID", ErrInvalidForeignEndpoint)
	}
	if fe.Hash == "" {
		return fmt.Errorf("%w: empty attested hash for node %d", ErrInvalidForeignEndpoint, fe.NodeID.SnowflakeID())
	}
	if fe.AttestTx == 0 {
		return fmt.Errorf("%w: missing attest-time provenance for node %d", ErrInvalidForeignEndpoint, fe.NodeID.SnowflakeID())
	}
	return nil
}

// ErrInvalidForeignIncoming reports a malformed ForeignIncomingEdge descriptor.
var ErrInvalidForeignIncoming = errors.New("graph: invalid foreign incoming edge descriptor")

// ForeignIncomingEdge describes a cross-machine edge to be recorded as an incoming
// half-edge STUB on the END node's machine (ADR-0010 Model A). It is the mirror of
// ForeignEndpoint, sent the OTHER direction: after the authoritative edge is
// created on the START node's machine (via AddByIDForeignEnd), the caller (the caller)
// extracts these fields from the created relationship and RPCs them to the END
// node's machine, which reconstructs a faithful stub (same rel-ID, endpoints,
// properties, type NAME, integrity, and temporal) so IncomingRelationships(END) is
// locally complete. The type is carried by NAME (re-tokenized in the end machine's
// own registry); the content hash is RECOMPUTED from the same inputs, so the stub's
// tkg_hash is byte-identical to the authoritative edge's.
type ForeignIncomingEdge struct {
	RelID      types.RelID
	TypeName   string
	StartID    types.NodeID // foreign — the edge's authoritative start
	EndID      types.NodeID // local — hosts the stub
	Properties map[string]any
	FromHash   string // tkg_from_hash of the authoritative edge (verbatim)
	ToHash     string // tkg_to_hash of the authoritative edge (verbatim)
	PrevHash   string
	ValidFrom  types.Instant
	ValidTo    types.Instant
	CreatedAt  types.Instant
	TxFrom     types.Instant
	Version    uint32
	// AttestTx is required non-zero provenance (§4.1) for the attest-time-vs-
	// commit-time gap. Like ForeignEndpoint.AttestTx, rho-tkg enforces NO
	// staleness bound on it — any positive value passes Validate; bounding
	// acceptable staleness is the caller's/orchestrator's responsibility.
	AttestTx types.Instant
}

// Validate rejects a malformed descriptor. Validate does NOT check how OLD
// AttestTx is — any positive value passes; see ForeignEndpoint's type doc
// comment for why that bound is intentionally left to the caller.
func (e ForeignIncomingEdge) Validate() error {
	if e.RelID.SnowflakeID() == 0 {
		return fmt.Errorf("%w: zero rel ID", ErrInvalidForeignIncoming)
	}
	if e.TypeName == "" {
		return fmt.Errorf("%w: empty type name", ErrInvalidForeignIncoming)
	}
	if e.StartID.SnowflakeID() == 0 || e.EndID.SnowflakeID() == 0 {
		return fmt.Errorf("%w: zero endpoint ID", ErrInvalidForeignIncoming)
	}
	if e.AttestTx == 0 {
		return fmt.Errorf("%w: missing attest-time provenance", ErrInvalidForeignIncoming)
	}
	return nil
}
