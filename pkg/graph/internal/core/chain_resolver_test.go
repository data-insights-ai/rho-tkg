package core

import (
	"errors"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// These are DIRECT unit tests of the single resolution seam (resolveNodeChain /
// resolveRelChain). They build version chains by hand with explicit temporal
// metadata (so the snowflake valid-from fallback never fires) and assert
// hand-computed selections, covering each of the six semantic rules the funnel
// concentrates. A bare &Core{bitemporalMigrated: true} is sufficient: the
// selection cores touch only c.bitemporalMigrated plus pure package helpers,
// never the store.

func resolverTestCore() *Core { return &Core{bitemporalMigrated: true} }

func rcNode(id uint64, version uint32, tm *types.TemporalMetadata) *types.Node {
	n := types.NewNode(types.NodeID(id), 1, nil)
	n.SetVersion(version)
	n.SetTemporal(tm)
	return n
}

func rcRel(id uint64, version uint32, tm *types.TemporalMetadata) *types.Relationship {
	r := types.NewRelationship(types.RelID(id), 1, types.NodeID(10), types.NodeID(20))
	r.SetVersion(version)
	r.SetTemporal(tm)
	return r
}

// --- Node: point (probePoint) ---------------------------------------------

func TestResolveNodeChain_Point(t *testing.T) {
	c := resolverTestCore()
	// Monotonic tiled chain: v0 [1000,2000), v1 [2000, open).
	build := func() []*types.Node {
		return []*types.Node{
			rcNode(1, 0, &types.TemporalMetadata{ValidFrom: 1000, TxFrom: 1000}),
			rcNode(1, 1, &types.TemporalMetadata{ValidFrom: 2000, UpdatedAt: 2000, TxFrom: 2000}),
		}
	}
	cases := []struct {
		name       string
		validAt    types.Instant
		txAt       types.Instant
		wantVer    uint32
		wantErr    error
		wantTxToNZ bool // expect TxTo cleared to 0 (normalization)
	}{
		{name: "covers v0", validAt: 1500, wantVer: 0},
		{name: "covers v1", validAt: 2500, wantVer: 1},
		{name: "before genesis", validAt: 500, wantErr: storepkg.ErrNoVersionValidAt},
		// TX filter: v1 recorded at 2000; pin at 1500 hides it, so the then-open
		// v0 answers even a validAt in v1's later slot (lesson 43 belief rebuild).
		{name: "txAt hides v1", validAt: 2500, txAt: 1500, wantVer: 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := c.resolveNodeChain(build(), chainProbe{kind: probePoint, validAt: tc.validAt, tx: tc.txAt}, nil)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("err = %v, want %v", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			if got.Version() != tc.wantVer {
				t.Fatalf("version = %d, want %d", got.Version(), tc.wantVer)
			}
		})
	}
}

// TestResolveChainRelating_WhiteBox exercises the probeRelate seam directly,
// including the defense-in-depth branches the door short-circuits away from: the
// empty-set guard (the door rejects rels==0 before the resolver, but a future
// direct caller must be safe), the eclipsed 1-instant tombstone-tile skip, and
// the RelateOpen open-end classification. Query interval b = [100,200).
func TestResolveChainRelating_WhiteBox(t *testing.T) {
	c := resolverTestCore()

	// Chain: v0 closed [10,50) (Before b), then an ECLIPSED 1-instant tile at 50
	// (must be skipped), then v1 open [60,∞) (Contains b). Tiling: v0's end comes
	// from the eclipsed tile's ValidFrom — but the resolver skips eclipsed rows for
	// vEnd derivation too, so v0 tiles to v1's ValidFrom 60. That still leaves v0
	// [10,60) Before b, so both {Before} and {Contains} have a distinct match.
	build := func() []*types.Node {
		return []*types.Node{
			rcNode(1, 0, &types.TemporalMetadata{ValidFrom: 10, TxFrom: 10}),
			rcNode(1, 1, &types.TemporalMetadata{ValidFrom: 50, UpdatedAt: 50, ValidTo: 51, TxFrom: 50}), // eclipsed
			rcNode(1, 2, &types.TemporalMetadata{ValidFrom: 60, UpdatedAt: 60, TxFrom: 60}),
		}
	}

	// Empty set → ErrNoVersionValidAt (defense-in-depth guard).
	if _, err := c.resolveNodeChain(build(), chainProbe{kind: probeRelate, validStart: 100, validEnd: 200, rels: 0}, nil); !errors.Is(err, storepkg.ErrNoVersionValidAt) {
		t.Fatalf("empty set: err = %v, want ErrNoVersionValidAt", err)
	}

	// {Contains}: the open head [60,∞) envelopes [100,200); the eclipsed tile is
	// skipped, proving the skip does not spuriously match.
	got, err := c.resolveNodeChain(build(), chainProbe{kind: probeRelate, validStart: 100, validEnd: 200, rels: types.Contains.Set()}, nil)
	if err != nil {
		t.Fatalf("{Contains}: unexpected err %v", err)
	}
	if got.Version() != 2 {
		t.Fatalf("{Contains}: version = %d, want 2 (open head)", got.Version())
	}

	// {Before}: only the older tile [10,60) qualifies (predicate-anywhere).
	got, err = c.resolveNodeChain(build(), chainProbe{kind: probeRelate, validStart: 100, validEnd: 200, rels: types.Before.Set()}, nil)
	if err != nil {
		t.Fatalf("{Before}: unexpected err %v", err)
	}
	if got.Version() != 0 {
		t.Fatalf("{Before}: version = %d, want 0 (older tile)", got.Version())
	}

	// {During}: no version is inside b → ErrNoVersionValidAt.
	if _, err := c.resolveNodeChain(build(), chainProbe{kind: probeRelate, validStart: 100, validEnd: 200, rels: types.During.Set()}, nil); !errors.Is(err, storepkg.ErrNoVersionValidAt) {
		t.Fatalf("{During}: err = %v, want ErrNoVersionValidAt", err)
	}

	// Relationship mirror (rule 2): same shape, {Before} finds the older tile.
	buildR := func() []*types.Relationship {
		return []*types.Relationship{
			rcRel(1, 0, &types.TemporalMetadata{ValidFrom: 10, TxFrom: 10}),
			rcRel(1, 1, &types.TemporalMetadata{ValidFrom: 50, UpdatedAt: 50, ValidTo: 51, TxFrom: 50}), // eclipsed
			rcRel(1, 2, &types.TemporalMetadata{ValidFrom: 60, UpdatedAt: 60, TxFrom: 60}),
		}
	}
	if _, err := c.resolveRelChain(buildR(), chainProbe{kind: probeRelate, validStart: 100, validEnd: 200, rels: 0}, nil); !errors.Is(err, storepkg.ErrNoVersionValidAt) {
		t.Fatalf("rel empty set: err = %v, want ErrNoVersionValidAt", err)
	}
	gotR, err := c.resolveRelChain(buildR(), chainProbe{kind: probeRelate, validStart: 100, validEnd: 200, rels: types.Before.Set()}, nil)
	if err != nil {
		t.Fatalf("rel {Before}: unexpected err %v", err)
	}
	if gotR.Version() != 0 {
		t.Fatalf("rel {Before}: version = %d, want 0", gotR.Version())
	}
	gotR, err = c.resolveRelChain(buildR(), chainProbe{kind: probeRelate, validStart: 100, validEnd: 200, rels: types.Contains.Set()}, nil)
	if err != nil {
		t.Fatalf("rel {Contains}: unexpected err %v", err)
	}
	if gotR.Version() != 2 {
		t.Fatalf("rel {Contains}: version = %d, want 2", gotR.Version())
	}
}

// TestResolveNodeChain_TombstoneNormalizedAtPreDeletePin exercises lesson 60: a
// hard-deleted row queried at a pin BEFORE the delete is normalized to its live
// belief (ValidTo/DeletedAt cleared) rather than silently excluded.
func TestResolveNodeChain_TombstoneNormalizedAtPreDeletePin(t *testing.T) {
	c := resolverTestCore()
	build := func() []*types.Node {
		return []*types.Node{
			rcNode(1, 0, &types.TemporalMetadata{ValidFrom: 1000, TxFrom: 1000, ValidTo: 3000, DeletedAt: 3000, TxTo: 3000}),
		}
	}
	// Pin 2000 is before the delete at 3000 → row survives, normalized.
	got, err := c.resolveNodeChain(build(), chainProbe{kind: probePoint, validAt: 1500, tx: 2000}, nil)
	if err != nil {
		t.Fatalf("pre-delete pin: unexpected err %v", err)
	}
	if tm := got.Temporal(); tm.ValidTo != 0 || tm.DeletedAt != 0 || tm.TxTo != 0 {
		t.Fatalf("pre-delete pin not normalized: %+v", tm)
	}
	// The store row was NOT mutated (filter deep-copies tombstones): re-resolving
	// at a pin after the delete still sees the closed interval.
	got2, err := c.resolveNodeChain(build(), chainProbe{kind: probePoint, validAt: 1500, tx: 3500}, nil)
	if err != nil {
		t.Fatalf("post-delete pin (validAt in interval): unexpected err %v", err)
	}
	if got2.Temporal().ValidTo != 3000 {
		t.Fatalf("post-delete pin: ValidTo = %d, want 3000", got2.Temporal().ValidTo)
	}
}

// --- Node: interval (probeInterval) — predicate-anywhere (rule 16) ---------

func TestResolveNodeChain_IntervalPredicateAnywhere(t *testing.T) {
	c := resolverTestCore()
	build := func() []*types.Node {
		return []*types.Node{
			rcNode(1, 0, &types.TemporalMetadata{ValidFrom: 1000, TxFrom: 1000}),
			rcNode(1, 1, &types.TemporalMetadata{ValidFrom: 2000, UpdatedAt: 2000, TxFrom: 2000}),
		}
	}
	// pred matches ONLY the older version. The newest overlapping version (v1)
	// fails it, so the resolver must keep scanning and return v0.
	predOld := func(n *types.Node) bool { return n.Version() == 0 }
	got, err := c.resolveNodeChain(build(), chainProbe{kind: probeInterval, validStart: 1000, validEnd: 3000}, predOld)
	if err != nil {
		t.Fatalf("predicate-anywhere: unexpected err %v", err)
	}
	if got.Version() != 0 {
		t.Fatalf("version = %d, want 0 (older matching version)", got.Version())
	}
	// No overlap → ErrNoVersionValidAt.
	if _, err := c.resolveNodeChain(build(), chainProbe{kind: probeInterval, validStart: 100, validEnd: 500}, nil); !errors.Is(err, storepkg.ErrNoVersionValidAt) {
		t.Fatalf("no-overlap err = %v, want ErrNoVersionValidAt", err)
	}
	// pred never satisfied → ErrNoVersionValidAt.
	predNone := func(n *types.Node) bool { return false }
	if _, err := c.resolveNodeChain(build(), chainProbe{kind: probeInterval, validStart: 1000, validEnd: 3000}, predNone); !errors.Is(err, storepkg.ErrNoVersionValidAt) {
		t.Fatalf("pred-none err = %v, want ErrNoVersionValidAt", err)
	}
}

// --- Node: as-of (probeAsOf) — belief selection + retraction (lesson 62) ---

func TestResolveNodeChain_AsOf(t *testing.T) {
	c := resolverTestCore()
	// v0 superseded by v1 at tx 2000.
	buildSupersede := func() []*types.Node {
		return []*types.Node{
			rcNode(1, 0, &types.TemporalMetadata{ValidFrom: 1000, TxFrom: 1000, TxTo: 2000}),
			rcNode(1, 1, &types.TemporalMetadata{ValidFrom: 1000, TxFrom: 2000}),
		}
	}
	// Pin between the two beliefs → the older v0, TxTo normalized away.
	got, err := c.resolveNodeChain(buildSupersede(), chainProbe{kind: probeAsOf, tx: 1500}, nil)
	if err != nil {
		t.Fatalf("asOf 1500: unexpected err %v", err)
	}
	if got.Version() != 0 || got.Temporal().TxTo != 0 {
		t.Fatalf("asOf 1500: version=%d TxTo=%d, want 0/0", got.Version(), got.Temporal().TxTo)
	}
	// Pin after v1 recorded → newest belief v1.
	if got, err := c.resolveNodeChain(buildSupersede(), chainProbe{kind: probeAsOf, tx: 2500}, nil); err != nil || got.Version() != 1 {
		t.Fatalf("asOf 2500: version=%v err=%v, want 1/nil", got, err)
	}
	// Pin before any belief recorded → absent.
	if _, err := c.resolveNodeChain(buildSupersede(), chainProbe{kind: probeAsOf, tx: 500}, nil); !errors.Is(err, ErrNoVersionAsOf) {
		t.Fatalf("asOf 500 err = %v, want ErrNoVersionAsOf", err)
	}

	// Retraction (lesson 62): newest belief v1 retracted by 2500; the resolver
	// must NOT fall through to older v0 whose TxTo is still open past the pin.
	buildRetract := func() []*types.Node {
		return []*types.Node{
			rcNode(1, 0, &types.TemporalMetadata{ValidFrom: 1000, TxFrom: 1000, TxTo: 3000}),
			rcNode(1, 1, &types.TemporalMetadata{ValidFrom: 1000, TxFrom: 2000, TxTo: 2500}),
		}
	}
	if _, err := c.resolveNodeChain(buildRetract(), chainProbe{kind: probeAsOf, tx: 2600}, nil); !errors.Is(err, ErrNoVersionAsOf) {
		t.Fatalf("retracted-newest err = %v, want ErrNoVersionAsOf", err)
	}
}

// --- Relationship parity (rule 2) -----------------------------------------

func TestResolveRelChain_Point(t *testing.T) {
	c := resolverTestCore()
	build := func() []*types.Relationship {
		return []*types.Relationship{
			rcRel(1, 0, &types.TemporalMetadata{ValidFrom: 1000, TxFrom: 1000}),
			rcRel(1, 1, &types.TemporalMetadata{ValidFrom: 2000, UpdatedAt: 2000, TxFrom: 2000}),
		}
	}
	if got, err := c.resolveRelChain(build(), chainProbe{kind: probePoint, validAt: 1500}, nil); err != nil || got.Version() != 0 {
		t.Fatalf("rel point 1500: version=%v err=%v, want 0/nil", got, err)
	}
	if got, err := c.resolveRelChain(build(), chainProbe{kind: probePoint, validAt: 2500, tx: 1500}, nil); err != nil || got.Version() != 0 {
		t.Fatalf("rel point txAt-hides-v1: version=%v err=%v, want 0/nil", got, err)
	}
	if _, err := c.resolveRelChain(build(), chainProbe{kind: probePoint, validAt: 500}, nil); !errors.Is(err, storepkg.ErrNoVersionValidAt) {
		t.Fatalf("rel point before-genesis err = %v, want ErrNoVersionValidAt", err)
	}
}

func TestResolveRelChain_IntervalPredicateAnywhere(t *testing.T) {
	c := resolverTestCore()
	build := func() []*types.Relationship {
		return []*types.Relationship{
			rcRel(1, 0, &types.TemporalMetadata{ValidFrom: 1000, TxFrom: 1000}),
			rcRel(1, 1, &types.TemporalMetadata{ValidFrom: 2000, UpdatedAt: 2000, TxFrom: 2000}),
		}
	}
	predOld := func(r *types.Relationship) bool { return r.Version() == 0 }
	if got, err := c.resolveRelChain(build(), chainProbe{kind: probeInterval, validStart: 1000, validEnd: 3000}, predOld); err != nil || got.Version() != 0 {
		t.Fatalf("rel predicate-anywhere: version=%v err=%v, want 0/nil", got, err)
	}
}

func TestResolveRelChain_AsOf(t *testing.T) {
	c := resolverTestCore()
	build := func() []*types.Relationship {
		return []*types.Relationship{
			rcRel(1, 0, &types.TemporalMetadata{ValidFrom: 1000, TxFrom: 1000, TxTo: 3000}),
			rcRel(1, 1, &types.TemporalMetadata{ValidFrom: 1000, TxFrom: 2000, TxTo: 2500}),
		}
	}
	// Retraction: newest belief v1 retracted by 2600 → absent, no fall-through.
	if _, err := c.resolveRelChain(build(), chainProbe{kind: probeAsOf, tx: 2600}, nil); !errors.Is(err, ErrNoVersionAsOf) {
		t.Fatalf("rel retracted-newest err = %v, want ErrNoVersionAsOf", err)
	}
	// Pin between beliefs → older v0.
	if got, err := c.resolveRelChain(build(), chainProbe{kind: probeAsOf, tx: 1500}, nil); err != nil || got.Version() != 0 {
		t.Fatalf("rel asOf 1500: version=%v err=%v, want 0/nil", got, err)
	}
}
