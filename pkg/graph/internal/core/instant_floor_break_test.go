package core

import (
	"context"
	"encoding/binary"
	"math"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Break-round coverage for the commit-clock floor (lesson 71). Every case here
// is an attack on the floor's inputs, not a demonstration that it works.

// A malformed durable watermark must be treated as ABSENT — never parsed into a
// floor, never fatal to New — and the next Close must REPLACE the garbage.
//
// Two independent guards carry this and they reject different shapes:
// instant_floor.go's `len(v) != 8` drops wrong-length blobs before parsing, and
// advanceInstantFloor's `inst <= 0` drops values that parse to zero or negative.
// A refactor collapsing either one would silently start feeding garbage into the
// commit clock, and no test covered these shapes.
func TestInstantFloor_MalformedWatermarkIsAbsentAndSelfHeals(t *testing.T) {
	be := func(v uint64) []byte {
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, v)
		return b
	}

	cases := []struct {
		name string
		blob []byte
		why  string
	}{
		{"seven bytes", []byte{1, 2, 3, 4, 5, 6, 7}, "short blob must not be parsed as a truncated instant"},
		{"nine bytes", []byte{1, 2, 3, 4, 5, 6, 7, 8, 9}, "long blob must not be parsed by silently taking the first 8"},
		{"empty", []byte{}, "an empty value must read as absent"},
		{"all zero", be(0), "zero is 'no floor', not a floor at the epoch"},
		{"all 0xFF (int64 -1)", be(math.MaxUint64), "a negative instant must never become a floor"},
		{"MinInt64", be(1 << 63), "the most-negative instant must never become a floor"},
		{"MaxInt64", be(uint64(math.MaxInt64)), "control: the skew bound rejects it, so there is no one-bit cliff next to 0xFF*8"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			ctx := context.Background()

			g, err := New(Config{BadgerDir: dir})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			mk, ok := g.store.(storepkg.MetaKVCapability)
			if !ok {
				t.Skip("backend without MetaKV")
			}
			if err := mk.MetaSet(instantFloorMeta, tc.blob); err != nil {
				t.Fatalf("MetaSet: %v", err)
			}

			// What New() does on open, now against a malformed value.
			g.seedInstantFloor()

			pin, err := g.Temporal.NowTx()
			if err != nil {
				t.Fatalf("NowTx: %v", err)
			}
			wall := int64(nowInstant())
			if pin <= 0 {
				t.Fatalf("%s: NowTx = %d — %s", tc.name, pin, tc.why)
			}
			if int64(pin) > wall+maxClockAdvanceSkewMillis {
				t.Fatalf("%s: NowTx = %d is implausibly far past wall %d — %s", tc.name, pin, wall, tc.why)
			}

			// A write must still be stamped with sane, positive transaction time.
			n, err := g.Nodes.Add(ctx, []string{"T"}, map[string]any{"i": 1})
			if err != nil {
				t.Fatalf("add: %v", err)
			}
			if tx := n.Temporal().TxFrom; tx <= 0 {
				t.Fatalf("%s: a write was stamped TxFrom=%d — the garbage watermark reached the clock", tc.name, tx)
			}

			// Self-heal: Close must REPLACE the garbage with a well-formed value.
			if err := g.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}
			g2, err := New(Config{BadgerDir: dir})
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			defer g2.Close()
			mk2 := g2.store.(storepkg.MetaKVCapability)
			got, err := mk2.MetaGet(instantFloorMeta)
			if err != nil {
				t.Fatalf("MetaGet: %v", err)
			}
			if len(got) != 8 {
				t.Fatalf("%s: after Close the watermark is still %d bytes — garbage was not replaced, "+
					"so it will be re-read on every future open", tc.name, len(got))
			}
		})
	}
}

// Caller-asserted WORLD time must never pull the commit clock.
//
// ValidFrom/ValidTo are asserted by the caller and may legitimately lie in the
// future; CreatedAt is not a transaction-time stamp either. Only the four
// SYSTEM-minted stamps may raise the floor. The wire structs carry all seven
// fields adjacent to each other, so this is one word away from regressing, and
// the regression would be silent: a caller asserting next year's validity would
// drag the commit clock a year forward and every subsequent TxFrom with it.
//
// Non-vacuous by construction: the future used here is INSIDE the ~10y skew
// bound, so a naive max-over-all-fields implementation is not rescued by the
// plausibility guard — it really would move the clock.
func TestCommitStamp_ValidTimeAndCreatedAtNeverFeedTheCommitClock(t *testing.T) {
	t.Parallel()
	vf := time.Now().Add(365 * 24 * time.Hour).UnixMilli() // ~1y out, inside the 10y bound
	vt := vf + 86_400_000
	ca := vf - 1

	if got := maxCommitStamp(&types.TemporalMetadata{
		ValidFrom: types.Instant(vf), ValidTo: types.Instant(vt), CreatedAt: types.Instant(ca),
	}); got != 0 {
		t.Fatalf("maxCommitStamp(valid-time/CreatedAt only) = %d, want 0 — caller-asserted world "+
			"time is feeding the commit clock, so asserting a future validity would drag every "+
			"later TxFrom with it", got)
	}
	if got := nodeWireCommitStamp(storeutil.NodeWire{ValidFrom: vf, ValidTo: vt, CreatedAt: ca}); got != 0 {
		t.Fatalf("nodeWireCommitStamp(valid-time/CreatedAt only) = %d, want 0", got)
	}
	if got := relWireCommitStamp(storeutil.RelWire{ValidFrom: vf, ValidTo: vt, CreatedAt: ca}); got != 0 {
		t.Fatalf("relWireCommitStamp(valid-time/CreatedAt only) = %d, want 0", got)
	}

	// Control: a genuine system-minted stamp on the SAME struct must still count,
	// so the test cannot pass by the functions returning 0 unconditionally.
	sys := types.Instant(time.Now().UnixMilli())
	if got := maxCommitStamp(&types.TemporalMetadata{
		ValidFrom: types.Instant(vf), ValidTo: types.Instant(vt), TxFrom: sys,
	}); got != sys {
		t.Fatalf("maxCommitStamp with a real TxFrom = %d, want %d — the system-minted stamp must "+
			"still raise the floor (and valid time must not out-rank it)", got, sys)
	}
}

// maxWireStamp must never return a negative, and a negative field must never
// mask a genuine positive one. A negative reaching advanceInstantFloor is
// rejected there, but only by a second guard — this pins the first.
func TestMaxWireStamp_NegativesClampAndNeverMaskAPositive(t *testing.T) {
	t.Parallel()
	base := time.Now().UnixMilli()

	if got := maxWireStamp(math.MinInt64, -3, 0, base+7); got != types.Instant(base+7) {
		t.Fatalf("maxWireStamp(MinInt64,-3,0,base+7) = %d, want %d — a negative field masked the "+
			"real stamp, so the floor would not advance to a committed TxFrom", got, base+7)
	}
	if got := maxWireStamp(-9, -3, -7, -1); got != 0 {
		t.Fatalf("maxWireStamp(all negative) = %d, want 0 — a negative must clamp, never propagate "+
			"into the commit clock", got)
	}
}

// ChangeNodeDelete carries a node tombstone that may be NIL plus N cascaded
// relationship tombstones. The record's stamp is the max across all of them, so
// a nil node tombstone must not abort the fold and the cascade's maximum must
// still win — otherwise a cascade delete's transaction time escapes the floor on
// a replica, which is the exact class of hole lesson 71 exists to close.
func TestRecordCommitStamp_NodeDeleteFoldsCascadedRelTombstones(t *testing.T) {
	t.Parallel()
	base := time.Now().UnixMilli()
	want := types.Instant(base + 9999)

	rec := func(b storeutil.NodeDeleteBody) storepkg.ChangeRecord {
		p, err := storeutil.MarshalChangeBody(b)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		return storepkg.ChangeRecord{Tag: storepkg.ChangeNodeDelete, Payload: p}
	}

	// (a) NIL node tombstone; the maximum lives in a cascaded rel tombstone.
	got := recordCommitStamp(rec(storeutil.NodeDeleteBody{
		ID: 1, WithHistory: true,
		RelTombstones: []storeutil.RelWire{
			{TxFrom: base + 1},
			{TxFrom: base + 2, DeletedAt: int64(want)},
		},
	}))
	if got != want {
		t.Fatalf("nil node tombstone + cascade: recordCommitStamp = %d, want %d — a nil tombstone "+
			"aborted the fold, so a cascade delete's stamp escapes the commit-clock floor", got, want)
	}

	// (b) Node tombstone present but LOWER than the cascade's maximum.
	got = recordCommitStamp(rec(storeutil.NodeDeleteBody{
		ID: 2, WithHistory: true,
		Tombstone:     &storeutil.NodeWire{TxFrom: base + 5},
		RelTombstones: []storeutil.RelWire{{TxFrom: int64(want)}},
	}))
	if got != want {
		t.Fatalf("node tombstone below cascade: recordCommitStamp = %d, want %d — the fold stopped "+
			"at the node tombstone instead of taking the max", got, want)
	}

	// (c) No wires at all must be 0, not a panic and not a stale value.
	if got := recordCommitStamp(rec(storeutil.NodeDeleteBody{ID: 3})); got != 0 {
		t.Fatalf("empty node-delete body: recordCommitStamp = %d, want 0", got)
	}

}
