package core

import (
	"bytes"
	"context"
	"errors"
	"math"
	"strings"
	"testing"

	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// ── Break-round hardening pins (v4.11.x transaction-time surface). Each asserts
// a property the adversarial campaign found unpinned; each can FAIL if the
// implementation silently did the wrong thing (no happy-path). The corrupt-blob
// and non-positive-instant cases already live in asof_tags_test.go — these add
// the NOVEL boundaries: the byte cap, multibyte, verbatim storage, MaxInt, and
// snapshot isolation of the returned map.

func newAsOfTestGraph(t *testing.T) *Core {
	t.Helper()
	g, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	return g
}

// §4.2 as-of tag name is bounded by RAW BYTE length at exactly 256 (accept) / 257
// (reject); a multibyte name is bounded by bytes, not runes; names are stored
// VERBATIM (no trim); and a MaxInt instant round-trips losslessly.
func TestAsOfTag_NameAndInstantBoundaries(t *testing.T) {
	t.Parallel()
	g := newAsOfTestGraph(t)

	// Exactly 256 bytes accepted; 257 rejected. (Existing tests probe only 257.)
	if err := g.Temporal.TagAsOf(strings.Repeat("a", 256), 5); err != nil {
		t.Fatalf("256-byte name rejected: %v (the cap is `len > 256`, so 256 must pass)", err)
	}
	if err := g.Temporal.TagAsOf(strings.Repeat("a", 257), 5); !errors.Is(err, ErrInvalidAsOfTag) {
		t.Fatalf("257-byte name = %v, want ErrInvalidAsOfTag", err)
	}

	// Multibyte byte-cap: 80 runes of "€" = 240 bytes accepted and resolvable;
	// 100 runes = 300 bytes rejected (rune count would wrongly accept it).
	m80 := strings.Repeat("€", 80) // 240 bytes, 80 runes
	if err := g.Temporal.TagAsOf(m80, 7); err != nil {
		t.Fatalf("80-rune/240-byte multibyte name rejected: %v", err)
	}
	if v, ok, err := g.Temporal.ResolveAsOf(m80); err != nil || !ok || v != 7 {
		t.Fatalf("resolve multibyte name = (%d, %v, %v), want (7, true, nil)", v, ok, err)
	}
	m100 := strings.Repeat("€", 100) // 300 bytes, 100 runes
	if err := g.Temporal.TagAsOf(m100, 7); !errors.Is(err, ErrInvalidAsOfTag) {
		t.Fatalf("100-rune/300-byte name = %v, want ErrInvalidAsOfTag (byte cap, not rune cap)", err)
	}

	// Verbatim: a padded name is a DISTINCT key from its trimmed form (no
	// normalization — else two documented marks would silently collide).
	if err := g.Temporal.TagAsOf("  release  ", 11); err != nil {
		t.Fatalf("padded name: %v", err)
	}
	if _, ok, _ := g.Temporal.ResolveAsOf("release"); ok {
		t.Fatal("padded as-of tag name resolved under its trimmed form — names must be stored verbatim")
	}
	if v, ok, _ := g.Temporal.ResolveAsOf("  release  "); !ok || v != 11 {
		t.Fatalf("verbatim padded name = (%d, %v), want (11, true)", v, ok)
	}

	// MaxInt instant round-trips losslessly (int64 alias, no truncation).
	if err := g.Temporal.TagAsOf("max", types.Instant(math.MaxInt64)); err != nil {
		t.Fatalf("MaxInt instant: %v", err)
	}
	if v, ok, _ := g.Temporal.ResolveAsOf("max"); !ok || v != types.Instant(math.MaxInt64) {
		t.Fatalf("MaxInt instant round-trip = %d, want %d", v, int64(math.MaxInt64))
	}
}

// AsOfTags returns an INDEPENDENT snapshot: mutating the returned map must not
// corrupt the durable registry, and a stale snapshot must not shadow a later write.
func TestAsOfTag_SnapshotIsolation(t *testing.T) {
	t.Parallel()
	g := newAsOfTestGraph(t)
	if err := g.Temporal.TagAsOf("keep", 100); err != nil {
		t.Fatalf("TagAsOf: %v", err)
	}
	snap, err := g.Temporal.AsOfTags()
	if err != nil {
		t.Fatalf("AsOfTags: %v", err)
	}
	snap["injected"] = 999 // mutate the returned map
	delete(snap, "keep")

	snap2, err := g.Temporal.AsOfTags()
	if err != nil {
		t.Fatalf("AsOfTags 2: %v", err)
	}
	if _, ok := snap2["injected"]; ok {
		t.Fatal("mutating the AsOfTags() result injected a key into the registry — the returned map is not an independent copy")
	}
	if v, ok := snap2["keep"]; !ok || v != 100 {
		t.Fatalf("deleting from the AsOfTags() result dropped the registered mark: keep=(%d,%v)", v, ok)
	}
}

// §4.1 backfill: tkg_tx_from is a RESERVED key — after a backfilled create it must
// be the entity's stamped TxFrom (echoed by the shadow resolver as the BACKFILLED
// instant, not the system clock) yet must NOT leak into the user-property surface
// (Properties / PropertiesMap). Node + relationship parity.
func TestBackfill_ReservedTxFromNotLeakedIntoUserProps(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	g, err := New(Config{Store: memory.New(), AllowTxBackfill: true})
	if err != nil {
		t.Fatalf("New(AllowTxBackfill): %v", err)
	}
	defer g.Close()

	erk := types.Instant(1_600_000_000_000) // a documented PAST knowledge time
	n, err := g.Nodes.AddWithTx(ctx, []string{"N"}, map[string]any{"w": int64(2)}, erk)
	if err != nil {
		t.Fatalf("AddWithTx: %v", err)
	}

	assertNoReservedLeak := func(what string, props map[string]any) {
		if _, ok := props["tkg_tx_from"]; ok {
			t.Fatalf("tkg_tx_from leaked into %s", what)
		}
	}
	if _, ok := n.Properties().Get("tkg_tx_from"); ok {
		t.Fatal("tkg_tx_from leaked into node Properties()")
	}
	assertNoReservedLeak("node PropertiesMap()", n.PropertiesMap())
	// The real user prop survived the reserved-key extraction.
	if v, ok := n.Properties().Get("w"); !ok || v != int64(2) {
		t.Fatalf("user prop w = (%v, %v), want (2, true)", v, ok)
	}
	// Stamped as the backfilled TxFrom, echoed verbatim by the shadow resolver.
	if n.Temporal().TxFrom != erk {
		t.Fatalf("node TxFrom = %d, want backfilled %d", n.Temporal().TxFrom, erk)
	}
	if v, ok := g.Resolve.NodeProperty(n, "tkg_tx_from"); !ok || v != erk {
		t.Fatalf("shadow tkg_tx_from = (%v, %v), want backfilled %d (not the system clock)", v, ok, erk)
	}

	// Relationship mirror.
	nA, err := g.Nodes.Add(ctx, []string{"N"}, nil)
	if err != nil {
		t.Fatalf("add nA: %v", err)
	}
	nB, err := g.Nodes.Add(ctx, []string{"N"}, nil)
	if err != nil {
		t.Fatalf("add nB: %v", err)
	}
	r, err := g.Rels.AddWithTx(ctx, "R", nA, nB, map[string]any{"w": int64(3)}, erk)
	if err != nil {
		t.Fatalf("Rels.AddWithTx: %v", err)
	}
	if _, ok := r.Properties().Get("tkg_tx_from"); ok {
		t.Fatal("tkg_tx_from leaked into rel Properties()")
	}
	assertNoReservedLeak("rel PropertiesMap()", r.PropertiesMap())
	if r.Temporal().TxFrom != erk {
		t.Fatalf("rel TxFrom = %d, want backfilled %d", r.Temporal().TxFrom, erk)
	}
	if v, ok := g.Resolve.RelProperty(r, "tkg_tx_from"); !ok || v != erk {
		t.Fatalf("shadow rel tkg_tx_from = (%v, %v), want backfilled %d", v, ok, erk)
	}
}

// §4.1 backfill durability across Export→Import: a backfilled TxFrom (documented
// Erkenntniszeit) must reconstruct VERBATIM on a FRESH importer whose commit clock
// starts over (TxFrom is not hashed, so a hash oracle is blind to a re-stamp), and
// stay correctly addressable through the generic TxAt door — a pin BEFORE the
// Erkenntniszeit excludes it (not yet "known"); a pin AT/after includes it.
func TestBackfill_ExportImportTxFromVerbatimAndTxAtAddressable(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	src, err := New(Config{Store: memory.New(), AllowTxBackfill: true, SnowflakeNodeID: 1})
	if err != nil {
		t.Fatalf("src New: %v", err)
	}
	defer src.Close()

	erk := types.Instant(1_600_000_000_000) // documented past knowledge time
	// Explicit PAST world-time so the row is valid at the probe instant.
	a, err := src.Nodes.AddWithTx(ctx, []string{"K"}, map[string]any{"tkg_valid_from": types.Instant(1), "v": "x"}, erk)
	if err != nil {
		t.Fatalf("AddWithTx: %v", err)
	}

	var buf bytes.Buffer
	if err := src.IO.Export(&buf); err != nil {
		t.Fatalf("Export: %v", err)
	}

	// Import into a fresh graph — a different SnowflakeNodeID, a commit clock that
	// knows nothing of erk.
	dst, err := New(Config{Store: memory.New(), SnowflakeNodeID: 2})
	if err != nil {
		t.Fatalf("dst New: %v", err)
	}
	defer dst.Close()
	if err := dst.IO.Import(&buf, tkgio.ImportOptions{}); err != nil {
		t.Fatalf("Import: %v", err)
	}

	got, err := dst.Nodes.Get(ctx, a.ID())
	if err != nil {
		t.Fatalf("dst Get: %v", err)
	}
	if got.Temporal().TxFrom != erk {
		t.Fatalf("imported TxFrom = %d, want backfilled %d verbatim (not re-stamped to import time)", got.Temporal().TxFrom, erk)
	}

	containsA := func(rows []*types.Node) bool {
		for _, r := range rows {
			if r.ID() == a.ID() {
				return true
			}
		}
		return false
	}

	// Pin the tx clock BEFORE the Erkenntniszeit — the fact was not yet "known".
	before, err := dst.Nodes.ByLabel("K", storepkg.QueryOpts{TxAt: erk - 1, ValidAt: 1})
	if err != nil {
		t.Fatalf("ByLabel(TxAt<erk): %v", err)
	}
	if containsA(before) {
		t.Fatal("backfilled node visible at a TxAt pin BEFORE its Erkenntniszeit — recorded-by-then violated")
	}
	// Pin AT the Erkenntniszeit — the fact is now recorded.
	at, err := dst.Nodes.ByLabel("K", storepkg.QueryOpts{TxAt: erk, ValidAt: 1})
	if err != nil {
		t.Fatalf("ByLabel(TxAt=erk): %v", err)
	}
	if !containsA(at) {
		t.Fatal("backfilled node NOT visible at a TxAt pin AT its Erkenntniszeit — addressability lost across import")
	}
}
