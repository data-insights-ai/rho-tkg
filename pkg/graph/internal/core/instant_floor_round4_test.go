package core

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"math"
	"testing"
	"time"

	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// The tests below cover fixes that shipped in v4.24.6/7 without one.

// A watermark REFUSED by the plausibility bound must not then be overwritten.
//
// The bound compares against this host's wall clock, so on a machine whose clock
// is decades behind — a dead RTC, a pre-NTP boot, a VM restored from an old
// snapshot — it refuses a perfectly valid watermark. That refusal is a statement
// about the CLOCK, and the watermark is precisely the wall-independent defence
// that should have covered it. Overwriting it at Close makes the loss durable.
//
// A refused value is unvalidated in exactly the way an unreadable one is; only a
// MALFORMED blob (wrong length, or <= 0, which cannot be a real instant)
// justifies replacing it.
func TestInstantFloor_RefusedSeedMustNotDestroyTheWatermark(t *testing.T) {
	ctx := context.Background()
	base := memory.New()

	// Session 1, healthy clock: a >1 write/ms burst under a frozen wall pushes
	// the floor above the wall, and Close persists it.
	g1, err := New(Config{Store: &floorFailStore{Store: base}})
	if err != nil {
		t.Fatalf("New g1: %v", err)
	}
	frozen := time.Now().Add(time.Second)
	g1.SetClockForTest(t, func() time.Time { return frozen })
	var want types.Instant
	for i := 0; i < 300; i++ {
		n, err := g1.Nodes.Add(ctx, []string{"T"}, map[string]any{"i": i})
		if err != nil {
			t.Fatalf("add %d: %v", i, err)
		}
		if tx := n.Temporal().TxFrom; tx > want {
			want = tx
		}
	}
	if err := g1.Close(); err != nil {
		t.Fatalf("close g1: %v", err)
	}
	if v, err := base.MetaGet(instantFloorMeta); err != nil || len(v) != 8 {
		t.Fatalf("precondition: watermark not persisted (%v, %d bytes)", err, len(v))
	}

	// Session 2, clock regressed 30 years: the seed refuses the (valid) watermark.
	g2, err := New(Config{Store: &floorFailStore{Store: base}})
	if err != nil {
		t.Fatalf("New g2: %v", err)
	}
	g2.SetClockForTest(t, func() time.Time { return time.Now().AddDate(-30, 0, 0) })
	g2.lastInstant.Store(0)
	g2.floorSeedUnreadable = false
	g2.seedInstantFloor() // refuses: want > wall(-30y) + 10y

	if got := g2.lastInstant.Load(); got >= int64(want) {
		t.Fatalf("precondition: the seed did NOT refuse (floor=%d, watermark=%d)", got, want)
	}
	if _, err := g2.Nodes.Add(ctx, []string{"T"}, map[string]any{"post": true}); err != nil {
		t.Fatalf("g2 add: %v", err)
	}
	if err := g2.Close(); err != nil {
		t.Fatalf("close g2: %v", err)
	}

	v, err := base.MetaGet(instantFloorMeta)
	if err != nil || len(v) != 8 {
		t.Fatalf("watermark unreadable after g2: %v, %d bytes", err, len(v))
	}
	if got := types.Instant(binary.BigEndian.Uint64(v)); got < want {
		t.Fatalf("WATERMARK DESTROYED BY A REFUSED SEED: %d < %d. The seed refused a watermark it "+
			"could not validate against a regressed wall clock, then Close overwrote it with this "+
			"session's lower floor. A refused value is unvalidated exactly as an unreadable one is; "+
			"only a malformed blob may be replaced.", got, want)
	}
}

// The plausibility bound must be TWO-SIDED. An upper bound alone treats a
// negative stamp as "safely in the past" — but RecordForeignIncoming stores
// edge.TxFrom verbatim, so the row lands with negative transaction time that no
// AS-OF pin can ever reach, and TxFrom is outside the integrity hash so the
// chain verifiers still report the store healthy.
func TestForeignStamp_NegativeIsRejected(t *testing.T) {
	g, endID := newForeignIncomingTestGraph(t, ValidationLimits{})

	edge := baseForeignEdge(endID)
	edge.TxFrom = -1
	edge.AttestTx = 1

	err := g.Rels.RecordForeignIncoming(context.Background(), edge)
	if !errors.Is(err, ErrForeignStampImplausible) {
		t.Fatalf("RecordForeignIncoming(TxFrom=-1) = %v, want ErrForeignStampImplausible — a negative "+
			"foreign stamp is not 'safely in the past'; it is stored verbatim and no pin can reach it", err)
	}
	if in, err := g.Rels.Incoming(endID, ""); err != nil || len(in) != 0 {
		t.Fatalf("a stub with a negative TxFrom was stored anyway: in=%d err=%v", len(in), err)
	}

	// Control: a sane stamp is still accepted, so the bound is not refusing
	// everything.
	ok := baseForeignEdge(endID)
	ok.TxFrom = types.Instant(time.Now().UnixMilli())
	ok.AttestTx = ok.TxFrom
	if err := g.Rels.RecordForeignIncoming(context.Background(), ok); err != nil {
		t.Fatalf("RecordForeignIncoming with a sane stamp = %v, want nil", err)
	}
}

// After Reset re-wrote the watermark, floorSeedUnreadable is stale: this session
// now KNOWS the durable value because it wrote it. Leaving the flag set makes
// the following Close decline to persist a strictly higher floor, stranding
// every post-Reset write above the durable watermark.
func TestReset_ClearsTheStaleSeedFlagSoCloseStillPersists(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	g, err := New(Config{BadgerDir: dir, AllowReset: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := g.Nodes.Add(ctx, []string{"T"}, map[string]any{"i": 1}); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	g.floorSeedUnreadable = true // this session could not READ the watermark

	if err := g.Admin.Reset(); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	if g.floorSeedUnreadable {
		t.Fatal("floorSeedUnreadable is still set after Reset re-wrote the watermark — the following " +
			"Close will decline to persist a floor this session already wrote, so every post-Reset " +
			"write is stranded above the durable watermark")
	}

	// Post-Reset writes push the floor higher; Close must persist that.
	for i := 0; i < 50; i++ {
		if _, err := g.Nodes.Add(ctx, []string{"T"}, map[string]any{"post": i}); err != nil {
			t.Fatalf("post-reset add: %v", err)
		}
	}
	floor := g.lastInstant.Load()
	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	g2, err := New(Config{BadgerDir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer g2.Close()
	mk := g2.store.(storepkg.MetaKVCapability)
	v, err := mk.MetaGet(instantFloorMeta)
	if err != nil || len(v) != 8 {
		t.Fatalf("watermark absent after Close: %v, %d bytes", err, len(v))
	}
	if got := types.Instant(binary.BigEndian.Uint64(v)); int64(got) < floor {
		t.Fatalf("persisted watermark %d < the live post-Reset floor %d", got, floor)
	}
}

// Import must bound a wire's transaction stamp BEFORE the row reaches the store.
//
// Bounding it afterwards meant a stamp Import itself classifies as corruption
// was already stored and — with the change log on — already published to the
// change feed, escaping to replicas whose apply door advances the clock
// unconditionally and correctly, having no way to know the record was about to
// be rejected upstream. Rollback unwinds the local store; it cannot unpublish.
func TestImport_ImplausibleStampIsRejectedBeforeTheRowIsStored(t *testing.T) {
	// The change log must be ON: publication is the harm rollback cannot undo.
	st := memory.New(memory.WithChangeLog())
	g, err := New(Config{Store: st})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	var stream bytes.Buffer
	writeImportMsgpackRecord(t, &stream, exportTagHeader, exportHeader{Version: exportFormatVersion})
	writeImportMsgpackRecord(t, &stream, exportTagRegistry, tiered.RegistryFileData{
		Labels: []string{"", "Person"}, RelTypes: []string{""},
	})
	writeImportMsgpackRecord(t, &stream, exportTagNode, mustHashedNodeWire(t, storeutil.NodeWire{
		ID: 4242, PrimaryLabel: 1, Version: 1, HasTemporal: true, TxFrom: math.MaxInt64,
	}, []string{"Person"}))

	if err := g.IO.Import(bytes.NewReader(stream.Bytes()), tkgio.ImportOptions{}); !errors.Is(err, ErrCorruptExport) {
		t.Fatalf("Import(TxFrom=MaxInt64) = %v, want ErrCorruptExport", err)
	}
	if _, err := g.store.GetNode(types.NodeID(4242)); !errors.Is(err, storepkg.ErrNodeNotFound) {
		t.Fatalf("the node is still in the store after a rejected import (GetNode = %v)", err)
	}

	// The decisive assertion. Rollback removes the row from the STORE either
	// way, so the store alone cannot distinguish bounding-before from
	// bounding-after. What rollback cannot undo is PUBLICATION: once the record
	// is on the change feed it has escaped to replicas, whose apply door
	// advances the commit clock unconditionally and correctly, having no way to
	// know the record was about to be rejected upstream.
	recs, err := st.ChangeFeed(0, 1000)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	for _, rec := range recs {
		if stamp := recordCommitStamp(rec); stamp == math.MaxInt64 {
			t.Fatalf("a change record carrying the rejected TxFrom=%d was PUBLISHED to the change "+
				"feed (tag %v). Import classified this stamp as corruption, but bounded it only "+
				"AFTER writing the row, so the record reached the feed first. Rollback unwinds the "+
				"local store; it cannot unpublish, and a replica applying this advances its commit "+
				"clock to MaxInt64.", stamp, rec.Tag)
		}
	}
}
