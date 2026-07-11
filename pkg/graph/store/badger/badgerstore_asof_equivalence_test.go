package badger

import (
	"errors"
	"math/rand/v2"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// The badger native reverse-scan (NodeAsOf / RelAsOf) is an OPTIMIZATION of the
// canonical as-of selection rule that now lives once in storeutil.SelectAsOf: it
// visits history newest-version-first and stops at the first version recorded by
// the pin, which is exactly SelectAsOf's "newest belief by version + retraction"
// verdict. These tests prove that equivalence over randomized version chains
// rather than re-deriving the rule — a divergence (e.g. the commit-window drop or
// the as-of version-order divergence) surfaces as a mismatch here.
//
// The one invariant a real chain always satisfies, which the generator preserves,
// is that a hard delete stamps TxTo == DeletedAt in place (DeletedAt is never set
// without an equal TxTo); the native classifier checks only TxTo, so the two
// agree exactly under that invariant (see storeutil.retractedAtTxTime's note).

// asofVersion is one generated version of an entity's chain.
type asofVersion struct {
	version   uint32
	txFrom    types.Instant
	txTo      types.Instant
	deletedAt types.Instant
	current   bool // stored as the live current row (PutNode/PutRelationship), not history
}

// genAsofChain produces a random but invariant-respecting version chain: strictly
// increasing version numbers, optionally non-monotonic TxFrom (to exercise the
// lesson-62 version-vs-TxFrom inversion), a terminal version that is either an
// open live current or a retracted/deleted tombstone, and DeletedAt only ever set
// alongside an equal TxTo.
func genAsofChain(rng *rand.Rand) []asofVersion {
	n := 1 + rng.IntN(5)
	chain := make([]asofVersion, 0, n)
	for i := 0; i < n; i++ {
		v := asofVersion{version: uint32(i), txFrom: types.Instant(1 + rng.IntN(200))}
		chain = append(chain, v)
	}
	// Non-terminal versions are always superseded: give them a TxTo.
	for i := 0; i < n-1; i++ {
		chain[i].txTo = types.Instant(1 + rng.IntN(200))
	}
	// Terminal version: open-current, superseded-open (retracted), or deleted.
	switch rng.IntN(3) {
	case 0: // open live current
		chain[n-1].current = true
	case 1: // retracted, no current
		chain[n-1].txTo = types.Instant(1 + rng.IntN(200))
	default: // hard-deleted tombstone: TxTo == DeletedAt
		d := types.Instant(1 + rng.IntN(200))
		chain[n-1].txTo = d
		chain[n-1].deletedAt = d
	}
	// Occasionally force the exact lesson-62 shape: an OPEN genesis (TxTo unset)
	// beneath a retracted higher version, which used to resurrect the genesis.
	if n >= 2 && rng.IntN(4) == 0 {
		chain[0].txTo = 0
		chain[0].deletedAt = 0
		if !chain[n-1].current {
			// keep the terminal retracted so there is a decisive retracted belief
			if chain[n-1].txTo == 0 {
				chain[n-1].txTo = types.Instant(1 + rng.IntN(200))
			}
		} else {
			// demote the current to a retracted terminal so the shape holds
			chain[n-1].current = false
			chain[n-1].txTo = types.Instant(1 + rng.IntN(200))
		}
	}
	return chain
}

func buildNode(id types.NodeID, v asofVersion) *types.Node {
	n := types.NewNode(id, 1, nil)
	n.SetVersion(v.version)
	n.SetTemporal(&types.TemporalMetadata{TxFrom: v.txFrom, TxTo: v.txTo, DeletedAt: v.deletedAt})
	return n
}

func buildRel(id types.RelID, v asofVersion) *types.Relationship {
	r := types.NewRelationship(id, 1, types.NodeID(snowflake.ID(7)), types.NodeID(snowflake.ID(9)))
	r.SetVersion(v.version)
	r.SetTemporal(&types.TemporalMetadata{TxFrom: v.txFrom, TxTo: v.txTo, DeletedAt: v.deletedAt})
	return r
}

func TestBadgerNodeAsOfEquivalentToSelectAsOf(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)
	rng := rand.New(rand.NewPCG(0xA50F, 0x51CE))

	const entities = 300
	for e := 0; e < entities; e++ {
		nid := types.NodeID(snowflake.ID(1000 + e*2)) // even => node domain
		chain := genAsofChain(rng)

		full := make([]*types.Node, 0, len(chain))
		for _, v := range chain {
			node := buildNode(nid, v)
			full = append(full, node)
			if v.current {
				if err := bs.PutNode(node); err != nil {
					t.Fatalf("entity %d PutNode: %v", e, err)
				}
			} else {
				if err := bs.PutNodeVersion(nid, v.version, node); err != nil {
					t.Fatalf("entity %d PutNodeVersion v%d: %v", e, v.version, err)
				}
			}
		}

		for pin := types.Instant(0); pin <= 210; pin += 7 {
			wantV, wantOK := storeutil.SelectAsOf(full, pin)
			got, err := bs.NodeAsOf(nid, pin)
			if !wantOK {
				if !errors.Is(err, ErrVersionNotFound) {
					t.Fatalf("entity %d pin %d: SelectAsOf absent but native returned (%v, %v)", e, pin, versionOrNil(got), err)
				}
				continue
			}
			if err != nil {
				t.Fatalf("entity %d pin %d: SelectAsOf picked v%d but native errored: %v", e, pin, wantV.Version(), err)
			}
			if got.Version() != wantV.Version() {
				t.Fatalf("entity %d pin %d: native v%d != SelectAsOf v%d", e, pin, got.Version(), wantV.Version())
			}
		}
	}
}

func TestBadgerRelAsOfEquivalentToSelectAsOf(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)
	rng := rand.New(rand.NewPCG(0xB61E, 0x62DF))

	// Endpoint nodes must exist for PutRelationship to accept the current row.
	for _, endpoint := range []types.NodeID{types.NodeID(snowflake.ID(7)), types.NodeID(snowflake.ID(9))} {
		ep := types.NewNode(endpoint, 1, nil)
		if err := bs.PutNode(ep); err != nil {
			t.Fatalf("PutNode endpoint %d: %v", endpoint, err)
		}
	}

	const entities = 300
	for e := 0; e < entities; e++ {
		rid := types.RelID(snowflake.ID(1001 + e*2)) // odd => rel domain
		chain := genAsofChain(rng)

		full := make([]*types.Relationship, 0, len(chain))
		for _, v := range chain {
			rel := buildRel(rid, v)
			full = append(full, rel)
			if v.current {
				if err := bs.PutRelationship(rel); err != nil {
					t.Fatalf("entity %d PutRelationship: %v", e, err)
				}
			} else {
				if err := bs.PutRelVersion(rid, v.version, rel); err != nil {
					t.Fatalf("entity %d PutRelVersion v%d: %v", e, v.version, err)
				}
			}
		}

		for pin := types.Instant(0); pin <= 210; pin += 7 {
			wantV, wantOK := storeutil.SelectAsOf(full, pin)
			got, err := bs.RelAsOf(rid, pin)
			if !wantOK {
				if !errors.Is(err, ErrVersionNotFound) {
					t.Fatalf("entity %d pin %d: SelectAsOf absent but native returned (%v, %v)", e, pin, relVersionOrNil(got), err)
				}
				continue
			}
			if err != nil {
				t.Fatalf("entity %d pin %d: SelectAsOf picked v%d but native errored: %v", e, pin, wantV.Version(), err)
			}
			if got.Version() != wantV.Version() {
				t.Fatalf("entity %d pin %d: native v%d != SelectAsOf v%d", e, pin, got.Version(), wantV.Version())
			}
		}
	}
}

func versionOrNil(n *types.Node) any {
	if n == nil {
		return nil
	}
	return n.Version()
}

func relVersionOrNil(r *types.Relationship) any {
	if r == nil {
		return nil
	}
	return r.Version()
}
